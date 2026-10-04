# Windows Compatibility Report

Bounty #9 (`docs/BOUNTY_PROGRAM.md`): run Chronicle on Windows and document compatibility issues.

## Current contribution and result index — October 4, 2026

The original [PR #104](https://github.com/josedab/chronicle/pull/104) now includes
the native WAL repair and the follow-up changes below. Product checkpoint:
`592c97242cfe63188c0d73040026c414ba2efd79`. This index changes documentation only.

Each linked report or native-run record identifies its executed source, command, platform, raw results
and coverage limits. The focused results were produced on separate source
revisions; they must not be added together or described as a passing execution
of the final composed branch.

| Area | Change now in the contribution | Existing execution and scope |
|---|---|---|
| Windows disk space and WAL | Portable disk lookup and checked reset through a separate writable handle | Eight selected native groups passed at `006bc101`; the original record follows below |
| [Raft resource lifecycle](https://github.com/woahwhattheheck/chronicle/commit/f1c21406fbf6b6ba69ef4a71235c1b03e810a5cd) | Close log handles after failed construction and pre-start shutdown; make Stop idempotent and reject restart after shutdown | [Native run 37192245119](https://github.com/woahwhattheheck/chronicle/actions/runs/37192245119) compared `cf57d7c` with `f1c21406`: three lifecycle failures plus one passing normal-shutdown control became four passing subcases; the three executed blobs are retained in `efb80aef` |
| [Digital Twin cleanup](WINDOWS_DIGITALTWIN.md) | Close the caller-owned database before temporary-directory cleanup | The same ten selected native cases failed before and passed after the fixture repair |
| [Continuous-query registration](WINDOWS_CONTINUOUSQUERY.md) | Unique engine-local handles and capacity checks under the registry lock | Four selected native groups passed; query retention and concurrent admission are covered |
| [OTel lifecycle](WINDOWS_OTEL_LIFECYCLE.md) | Synchronize enqueue with shutdown; remove an invalid positive-initial-uptime assumption | Two selected native groups passed; the send/close panic was reproduced before the fix |
| [Streaming SQL registration](WINDOWS_STREAMING_SQL.md) | Preserve distinct query handles and enforce the concurrent capacity limit | Four selected native groups passed, including both capacity subcases |
| [Flight SQL statement tickets](WINDOWS_FLIGHTSQL.md) | Allocate random statement and prepared-statement IDs under the registry lock; place query-fixture rows inside the exclusive time range | Two selected native groups passed, preserving SQL bindings, closed-ticket invalidation and fresh-instance separation; the report retains the failing baseline and final source pins |
| [Export paths](WINDOWS_EXPORT_PATH.md) | Apply the existing sensitive-directory policy using Windows path rules | Four selected native groups passed, including real CSV creation and the Windows path table |
| [Configuration migration](WINDOWS_CONFIG_MIGRATION.md) | Initialize valid unrelated storage defaults in the maintained fixture | All 16 migration subcases passed natively; production validation remains unchanged |
| [Multimodal identities](WINDOWS_MULTIMODAL_IDS.md) | Preserve distinct generated log/span IDs on repeated clock ticks | The same four selected native cases failed before and passed after; explicit producer IDs remain supported |
| [Notebook parser and identity composition](WINDOWS_NOTEBOOKS.md) | Retain prose after fenced queries and preserve the separately integrated ID implementation | The parser and its assertion match a 15-group native candidate; that candidate used a different ID implementation, so the combined branch has no aggregate notebook pass claim |
| [Other clock and delta-sync repairs](WINDOWS_CLOCK_FIXES.md) | Preserve notebook/cell, alert, batch and explanation identities; retain request lifetime and queued work | A focused local notebook replay is recorded. Alert, delta-sync and explanation changes have no new native execution result |
| Time-series diff timing assertion | Accept a legitimate zero elapsed measurement from `time.Since` on timer-limited platforms and verify `ComputedAt` instead; production diff timing and aggregate statistics are unchanged | Native run [37205348415](https://github.com/woahwhattheheck/chronicle/actions/runs/37205348415) at `6660c179` observed zero and failed only this assertion in the selected group. Focused Windows run [37211147854](https://github.com/woahwhattheheck/chronicle/actions/runs/37211147854) / job `111462438471` passed `TestTSDiffMerge_DiffBranches` on Go 1.24.13 windows/amd64 with CGO disabled; the executed test blob `316109430ddf3fa8e9954879f08d0b616ebe93fe` is identical to the published PR104 blob |

The repository-wide failure inventory below is the historical observation at
`006bc101`, before these follow-ups. It remains evidence of that run; it is not
a freshly measured list of failures at the current product checkpoint. No
repository-wide rerun of the composed branch, upstream CI approval, bounty
acceptance or payment is established by this index. Existing reproduction
commands and coverage limits remain in the linked reports.

## Historical native execution on October 4, 2026

| Item | Recorded value |
|---|---|
| Repository / PR | `josedab/chronicle`, PR #104 |
| Tested source after the runtime fix | `006bc101c1f2079b04696dfc497f5587328fcc46` |
| Reproduced failing source before the runtime fix | `d539d1f6b9ed731fa39d5bb5403c3ea490064e54` |
| Operating system | Microsoft Windows Server 2025 Datacenter, `10.0.26100`, amd64 |
| Go | `go1.24.13 windows/amd64` |
| C interoperability | `CGO_ENABLED=0` |
| Execution type | Actual native Windows execution, not cross-compilation |

**The focused compatibility checks pass. The repository-wide short suite does not pass.** These are separate results; the failing suite is retained and summarized below. The tested commit pins the Go source and tests; this report is a documentation-only follow-up.

## Findings fixed in this PR

| Issue | Observed impact | Fix |
|---|---|---|
| `health_check.go` used `syscall.Statfs` / `Statfs_t` | The package did not compile on Windows | Separate Unix `Statfs` and Windows `GetDiskFreeSpaceEx` implementations |
| Disk-space lookup received the database filename | A not-yet-created database was not a suitable Windows directory target | Query the existing parent directory using an absolute UTF-16 path |
| C FFI test files lacked `cgo` build constraints | Non-cgo builds referenced cgo-only symbols | Add `//go:build cgo` to those test files |
| `WAL.Reset` truncated an append-only Windows handle | Native write/flush/close failed with `truncate ...reopen.db.wal: Access is denied.` | Use a separate writable handle for Windows truncation, verify that it identifies the same file, and preserve the append handle |

The final issue was reproduced by writing two points into a database under a Unicode directory, flushing, and checking the error from `db.Close()`. It was not a compile failure. Go's [Windows open implementation at the tested toolchain revision](https://github.com/golang/go/blob/go1.24.13/src/syscall/syscall_windows.go#L350-L380) removes generic write access from an `O_APPEND` handle without `O_TRUNC`.

The reset repair changes one line in `wal.go` to call a platform-specific helper. On Windows, the helper opens a writable handle without append mode, checks `os.SameFile`, and truncates while the existing WAL mutex is held. The original append handle remains in use. Non-Windows platforms retain the previous `file.Truncate(0)` behavior. `TestWALResetWindowsPreservesAppend` checks that reset empties the log and subsequent append/replay cycles retain the expected records.

## Focused result: pass

[Before-fix run](https://github.com/woahwhattheheck/chronicle/actions/runs/37188380717) reproduced the close failure. [After-fix run](https://github.com/woahwhattheheck/chronicle/actions/runs/37188938611) passed **all eight top-level tests**, with **32 test/subtest pass events** and no skipped selected cases:

- Five existing groups: `TestHealthCheckEngine`, `TestHealthCheckMemoryAndDisk`, `TestHealthCheckHTTPEndpoints`, `TestDBWriteAndQuery`, and `TestDBWriteBatch`.
- The committed regression: `TestWALResetWindowsPreservesAppend`.
- Two additive native acceptance probes: `TestNativeWindowsDiskUsageAcceptance` and `TestNativeWindowsPersistenceAcceptance`.

The disk probe covers an existing Unicode parent with a nonexistent database, a relative parent, a missing parent, and an embedded NUL. The persistence probe verifies the exact timestamp, value, and tag of both records after a checked close and reopen.

The additive probes were not replacements for production code or existing tests. Their bytes are identical to the first failing run, verified before execution with SHA-256 `349c75126aecf5a6dd9407bfe8e72be106b6ae214c1cb24979dc75725dbdebbb`. The [retained probe source](https://github.com/woahwhattheheck/chronicle/blob/4ca19243e8d0244e76a7d825317e1bce42002adf/validation/native_windows_probe_test.go) and the [workflow](https://github.com/woahwhattheheck/chronicle/blob/402ece476179a5aca337df9b5d7529dc6dd25ecd/.github/workflows/verify-windows-104.yml) reproduce the complete eight-test run.

To run the six checked-in groups without adding the external acceptance probes:

```powershell
$env:CGO_ENABLED = '0'
go version
go test -count=1 -short -p=2 -timeout=4m -run '^(TestHealthCheckEngine|TestHealthCheckMemoryAndDisk|TestHealthCheckHTTPEndpoints|TestDBWriteAndQuery|TestDBWriteBatch|TestWALResetWindowsPreservesAppend)$' .
```

The [passing evidence artifact](https://github.com/woahwhattheheck/chronicle/actions/runs/37188938611/artifacts/11298228278) contains the exact command, complete JSON test events, runtime and source provenance, probe source, and an empty tracked-source diff. Artifact ZIP SHA-256: `3f9fedfb3a4b6589a7e122066b3e170f29e9274d8254fb7f4549eabf6523ec64`.

## Repository-wide native short suite: failed, fully recorded

The [separate native suite run](https://github.com/woahwhattheheck/chronicle/actions/runs/37189099263) executed this command against the same source commit, without adding the external probes or modifying tracked files:

```powershell
$env:CGO_ENABLED = '0'
go test -short -count=1 -p=2 -timeout=120s -json ./...
```

**Exit code: 1.** The 600-second process bound did not expire, and the captured test output contains no package-timeout panic. The command completed with these results:

| Result | Count |
|---|---:|
| Passing packages | 23 |
| Failing packages | 5 |
| Package with no test files (`cmd/chronicle-collector`) | 1 |
| Passing top-level test events | 4,166 |
| Passing subtest events | 1,963 |
| Failing top-level test events | 57 |
| Failing subtest events | 40 |
| Skipped top-level test events | 117 |
| Skipped subtest events | 1 |

Parent tests and their subtests are counted separately in the event totals; these are not counts of independent test cases.

| Failing package | Failed top-level tests | Observed examples |
|---|---:|---|
| Root `chronicle` | 37 | Rule/query count assertions, positive-duration assertions, configuration normalization, HTTP/webhook policy expectations, and POSIX-oriented export-path expectations |
| `internal/continuousquery` | 2 | Query-list/limit and statistics assertions |
| `internal/digitaltwin` | 10 | Temporary-directory cleanup reporting that a database file is still in use |
| `internal/oteldistro` | 1 | Uptime expected to be greater than zero |
| `internal/raft` | 7 | Open-file cleanup and leadership/snapshot assertions |

These are observed failures on this Windows run, not a claim that all 57 originate in Windows-specific product code or in this PR. No before/after full-suite comparison was performed. In particular, an HTTP-only test webhook being rejected by an HTTPS requirement is not a reason to weaken production enforcement. The focused WAL repair above has its own controlled failing/passing reproduction.

<details>
<summary>Complete failed top-level test inventory</summary>

Root package:

```text
TestAlertBuilder_ListRules
TestAlertBuilder_ExportImport
TestAlertBuilder_MaxRulesLimit
TestAlertManager_Webhook
TestAnomalyExplainabilityStats
TestFlightSQLServer_HandleStatementQuery
TestAutoMLTrainingTime
TestBenchRunSuiteWrite
TestBenchRunSuiteQuery
TestChaosRecoveryEngine
TestConfig_DeprecationNormalization
TestDeltaSyncManager_Batching
TestDuckDBBackendEngine
TestEmbeddedCluster_Stats
TestValidateExportPath
TestExporter_SensitivePathRejection
TestOnlineServing
TestGRPCIngestionEngine
TestGRPCHandleQueryWithAggregation
TestHWAccelParallelScan
TestAutoMLSelector
TestMLInferencePipeline_TrainAndScore
TestModelRegistry
TestQueryLogsFullTextSearch
TestQuerySpans
TestMultiRegionReplicationStats
TestNotebookEngineGetAndList
TestProfilePartitionStore_Correlation
TestQueryProfilerEngine
TestStreamDSLV2Stats
TestStreamingSQLEngine_MaxConcurrentQueries
TestStreamingSQLEngine_ListQueries
TestBranchDiff
TestManualMergeConflict
TestTSDiffMerge_DiffBranches
TestHTTPSchemas
TestHTTPEmptyBody
```

`internal/continuousquery`:

```text
TestContinuousQueryEngine
TestContinuousQueryEngine_Stats
```

`internal/digitaltwin`:

```text
TestDigitalTwinEngine
TestAddConnection
TestRemoveConnection
TestAddMapping
TestMappingWithInvalidConnection
TestRemoveMapping
TestPushMetric
TestPushMetricWithTransform
TestTwinCallbacks
TestPlatformCreation
```

`internal/oteldistro`:

```text
TestDistroMetrics
```

`internal/raft`:

```text
TestRaftPersistentState
TestRaftSnapshot
TestLogCompactor_Compact
TestLogCompactor_NewInstance
TestMaybeSnapshot_BelowThreshold
TestMaybeSnapshot_AboveThreshold
TestConfirmLeadership_ExpiredLease
```

</details>

The [full-suite evidence artifact](https://github.com/woahwhattheheck/chronicle/actions/runs/37189099263/artifacts/11297698322) retains every raw test event and failure message, stderr, command/environment provenance, the summary, and an empty tracked-source diff. ZIP SHA-256: `8f36853d49bbdf505e93fa75afe917b4de0edda6b0170947d926409cd35e1221`. The hosted artifacts are retained for 14 days from the run; the failure inventory and source/run pins above remain in this report.

## Coverage limits

The results cover native Windows Server 2025 amd64 with Go 1.24.13 and non-cgo operation. C FFI, the race detector, tests skipped by `-short`, UNC/network shares, other Windows releases, and hardware-specific eBPF/TEE paths were not exercised. Cross-compilation is not substituted for runtime evidence.

The repository's existing CI matrix includes Windows, but the fork executions above are not upstream CI approval. This report establishes the executed results and remaining failures, not full-suite compatibility, maintainer acceptance, bounty assignment, or payment.
