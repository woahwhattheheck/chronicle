# Native Windows OTel ingestion and shutdown

This continuation of the Windows compatibility contribution repairs an actual ingestion/shutdown panic and removes a clock-resolution assumption from the existing uptime test. It does not change production uptime calculation.

## Shutdown race

`PushMetrics` checked `pipeline.running` and attempted a nonblocking channel send without the mutex used by `stopPipeline` to close that channel. A producer that had passed the check could therefore panic with `send on closed channel` while shutdown closed the queue.

The running check and nonblocking send now share the existing pipeline mutex with shutdown. A producer admitted before shutdown finishes its enqueue/drop decision before the channel closes. A producer arriving after shutdown does not increment the received or dropped counters. Queue capacity, nonblocking behavior, existing drop accounting and queued contents are preserved. This change concerns the pipeline send/close boundary; it is not a general receiver-lifecycle redesign.

## Uptime fixture

The initial metrics read may occur within the constructor's clock tick. Zero initial elapsed time is valid; negative elapsed time remains rejected. The maintained `TestDistroMetrics` now additionally records a start time one second earlier, under the existing metrics mutex, and checks the reported start and elapsed interval. There are no sleeps, clock replacements or production timestamp changes.

## Actual execution

[Native Windows run 37192306478](https://github.com/woahwhattheheck/chronicle/actions/runs/37192306478), job 111406894231, completed successfully on Windows/amd64 with Go 1.24.13, `CGO_ENABLED=0`, `GOTOOLCHAIN=local`, and `GOMAXPROCS=2`. The validation controller is `c109ba31f6b970dfffd9939ee0a78285b2f9cacb`; its workflow remains outside the original contribution branch.

Baseline source: `7b8512b6fcbe6ac1942b73cfe7911fcdad63660d`. Tested candidate: `a2a6bbbe17ca25fa5906418782fd123b9b862af5`, tree `20a0d2ec9248cdaa0737c1afc9976f871695e809`.

| Selected case | Baseline | Repaired |
| --- | --- | --- |
| `TestPushMetricsDuringStop` | Failed: 71 recovered producer panics; first was `send on closed channel` | Passed: no producer panic across 100 shutdown rounds with eight producers each |
| `TestDistroMetrics` | Failed: expected positive uptime immediately after construction | Passed: nonnegative initial uptime and explicit elapsed-time fixture |

Both executions ran the actual complete package source with only these two test names selected. The same new shutdown regression was present on both versions. It also checks that queued data remains available after channel closure and that subsequent pushes do not change admission/drop counters. The baseline retained its existing uptime assertion. Exit codes were 1 before and 0 after; the candidate has two top-level passes and no selected skips or failures.

```powershell
$env:CGO_ENABLED = '0'
$env:GOTOOLCHAIN = 'local'
$env:GOMAXPROCS = '2'
go test -short -count=1 -timeout=60s -json -run '^(TestDistroMetrics|TestPushMetricsDuringStop)$' ./internal/oteldistro
```

[Evidence artifact 11299557260](https://github.com/woahwhattheheck/chronicle/actions/runs/37192306478/artifacts/11299557260) contains raw before/after events, stderr, source bytes, patch and `receipt.json`. Archive SHA-256 was independently recomputed after download: `449123916e024bf1492be777b5e9b7ee28db429f141bb48fb00ef44c458b4f80`.

Published tested blobs:

- `chronicle_o_tel_distro.go`: `a722823c50f2a1b33870d61e775631527318407e`
- `oteldistro_test.go`: `e5be6d864ee63619d87e581475ea38da6c723a9a`
- `metrics_lifecycle_test.go`: `7269bf1e9ac5561d22140db04417dbaee8fd562b`

All three downloaded source files matched their recorded Git blob hashes. They are composed unchanged onto the current original branch after checking that intervening notebook and alert changes did not touch this package.

This is selected native execution, not a full package/repository suite, race-detector run, live OTLP receiver exercise, performance benchmark, upstream approval, bounty award or payout. The 34.094-second baseline command and 2.110-second candidate command include differing compilation/cache costs and are not presented as a speedup. Other findings in [the Windows report](WINDOWS.md) retain their independent status and owners.
