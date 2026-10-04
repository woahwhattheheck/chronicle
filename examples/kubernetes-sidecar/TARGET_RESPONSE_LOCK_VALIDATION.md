# Target response writes do not block target updates

The `/targets` handler held `targetsMu.RLock` until JSON response encoding had finished writing to the client. A slow response therefore blocked `AddTarget` and `RemoveTarget`; once a writer waited, later scrape target snapshots also waited on the same lock.

The handler now clones the target slice while holding the read lock, releases the lock, and writes the captured response. `slices.Clone` preserves order and the existing distinction between a nil target list (`null`) and a nonnil empty list (`[]`). Target registration, removal, scraping, and stored point behavior are unchanged.

## Actual selected regression

The normal Chronicle root package compiled and ran exactly one selected regression on Go 1.24.4, Linux amd64, CGO enabled. The original source failed; the candidate passed. No source-isolation fallback was used.

`TestK8sSidecarTargetResponseLock` invokes the actual handler with a response writer held inside `Write`. It concurrently invokes the actual `AddTarget` and `RemoveTarget` methods, then checks their progression before releasing that write. It also checks the exact original JSON snapshot and content type, the final replacement target, and both nil/empty JSON controls.

| Operation | Completed while Write held, before | Completed while Write held, after | Before elapsed | After elapsed |
| --- | --- | --- | ---: | ---: |
| AddTarget | No | Yes | 1000.122162 ms | 0.025839 ms |
| RemoveTarget | No | Yes | 1000.113229 ms | 0.000921 ms |

These are single controlled observations. The original update calls waited until the one-second observation deadline released the writer. The candidate calls completed while it remained held. Channel ordering establishes the behavior; the elapsed samples do not establish Kubernetes or production-network throughput. The regression exercises handler and target registry code, without database writes, deployment, a full test suite, or a benchmark matrix.

## Source and execution receipt

- Original contribution: [PR105](https://github.com/josedab/chronicle/pull/105).
- Original source commit: `3bff9d255e45c27f1ad66b0e0571850969c8e8ac`.
- Original `k8s_sidecar.go` blob: `ce5aa1aaeecd1d592b642f3e0eac71872338ec39`.
- Executed candidate blob: `c5e976b9a564a43cdf5d0414166276f47ddffe81`.
- Executed regression blob: `ea46bc246bfa381339a2e5f1818cb2df13fd9508`.
- [One completed successful run](https://github.com/woahwhattheheck/chronicle/actions/runs/37201362080), job `111433551017`.
- Isolated execution-controller commit: `858e0ba536e17a14a67c0305541530fd1a5d8fd8`.
- Artifact `11302946986`, 17,615 bytes, SHA-256 `9e28bebe5cdca58d8bd5c4fad63748b09d71b301baef75577ae4050176b5fc08`.
- The downloaded artifact's production and regression bytes match the files submitted with this report. Its complete recorded selected-test events are retained in [TARGET_RESPONSE_LOCK_RESULTS.json](TARGET_RESPONSE_LOCK_RESULTS.json).

Reproduce the maintained regression from the repository root:

```bash
go test -count=1 -timeout=30s -run '^TestK8sSidecarTargetResponseLock$' .
```

The existing parser, target-index, startup, image and Kubernetes Service reports retain their original execution scopes and source pins.
