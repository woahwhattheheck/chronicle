# Windows digitaltwin cleanup follow-up

This is a focused follow-up to [the Windows compatibility report](WINDOWS.md) for the existing PR #104 / testing bounty #9. It does not replace the report's repository-wide failure inventory.

## Finding and repair

The ten digitaltwin cases that use `setupTestDB` opened a Chronicle database without registering its close. `DigitalTwinEngine.Close` cancels its work and disconnects adapters; the supplied database remains caller-owned. On native Windows, the open database prevented `t.TempDir` cleanup from removing `test.db`.

Five added lines register a checked `t.Cleanup` callback immediately after a successful database open. Because the temporary directory's cleanup was registered first, the database closes before directory removal. The existing deferred engine shutdowns finish before the test cleanup callbacks. A failed database close is reported rather than ignored.

No production code, existing assertions, test selectors or dependency versions changed.

## Controlled native result — October 4, 2026

- Before: `d6e5f9144e9a376a3dee2ca3327617c8012fa84b`, test file blob `fb7988a1af936592608420f6abde4a5d726603ef`.
- After: `230e6ef8e4162618d2a567ad5d865821313dd92a`, test file blob `0de863ef59fa864a98b30521e36b915ef90dfff8`.
- Environment: Windows Server 2025, version 10.0.26100; Go 1.24.13 windows/amd64; `CGO_ENABLED=0`.
- [One native before/after run](https://github.com/woahwhattheheck/chronicle/actions/runs/37190315632), job `111400919734`: succeeded.

The identical ten existing top-level tests failed before the repair and passed afterward. All baseline failures included the open-file directory-cleanup error. The after run had no failed test events and a terminal package success. `TestPlatformCreation` also produced four passing subtest events; those are not four additional independent top-level cases.

Recorded package elapsed times were **16.635 seconds before** and **0.207 seconds after**. These are the two observed test-run durations, not a repeated benchmark or a claim about application throughput; the baseline repeatedly retried file deletion. Dependency download/compilation occurred before the first execution and is not presented as a performance improvement.

Reproduce the selected scope on Windows:

```powershell
$env:CGO_ENABLED = '0'
go test -short -count=1 -p=2 -timeout=120s -json -run '^(TestDigitalTwinEngine|TestAddConnection|TestRemoveConnection|TestAddMapping|TestMappingWithInvalidConnection|TestRemoveMapping|TestPushMetric|TestPushMetricWithTransform|TestTwinCallbacks|TestPlatformCreation)$' ./internal/digitaltwin
```

The [isolated validation workflow](https://github.com/woahwhattheheck/chronicle/blob/06cf45f7fea6823d8b99d53ff73c77e4320973db/.github/workflows/verify-digitaltwin-759b.yml) checks the exact original source blob, reproduces the retained ten failures, inserts only the five cleanup lines, and executes the identical selection. It also verifies formatting and the one-file diff before retaining a code-only commit. The workflow is not included in the sponsor PR.

## Retained evidence and limits

[Artifact `11299011070`](https://github.com/woahwhattheheck/chronicle/actions/runs/37190315632/artifacts/11299011070), `chronicle104-digitaltwin-759b`, contains the raw before/after JSON test events, exit codes, stderr, source diff, command, runtime information and commit/blob identities. ZIP SHA-256: `f52c4642dc53ee8a490bb7ad66e95b2e0dad3edaf77f3bc2a6ac5bf2f64a17db`. Its downloaded bytes were checked against the provider's digest. The hosted artifact expires October 18, 2026; the source pins, reproduction command and result above remain in this document.

The ten-test cleanup slice is resolved by the measured run. The repository-wide suite was not rerun, and the separate root, continuousquery, oteldistro and raft failures in `WINDOWS.md` remain unresolved by this change. No full-suite compatibility, upstream approval, bounty assignment or payment is asserted.
