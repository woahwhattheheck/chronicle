# Native Windows continuous-query registration

The two previously reported continuous-query test groups now pass with the focused registration repair. Native execution reproduced the original query-list, maximum-query and statistics failures before the change, as well as the two new regressions. All four selected top-level tests pass after the change.

## Cause and change

`CreateQuery` used only the wall-clock nanosecond timestamp as its registry key. Windows returned the same timestamp for successive real calls, so a later accepted query replaced an earlier query. A separate admission race let two concurrent requests pass the same available slot before either registered.

The engine now appends an engine-local sequence while holding the query-registry lock. The sequence survives deletion of all queries. Registration checks the capacity again under that same lock and cancels an unregistered query's context when capacity has already been consumed. Successful-create metrics retain their existing placement after registration. The ID remains an opaque string with the `cq-` prefix; this is an engine-local uniqueness guarantee, not persistent or cross-engine identity.

## Executed evidence

- [Successful native run 37191639409](https://github.com/woahwhattheheck/chronicle/actions/runs/37191639409), [job 111404878986](https://github.com/woahwhattheheck/chronicle/actions/runs/37191639409/job/111404878986), attempt 1.
- Actual Windows Server 2025, amd64; Go 1.24.13; `CGO_ENABLED=0`, `GOTOOLCHAIN=local`, `GOPROXY=off`.
- Baseline: `cf57d7c658acfb91f23b4e1ccc66794187607ce0`.
- Tested candidate: `03d145b67a9743e064d633ccc902955b96f3d4b7`.
- [Validation-only workflow](https://github.com/woahwhattheheck/chronicle/blob/dc5b84f9d585f5c1901c8417e18509ff8fb56475/.github/workflows/ci.yml). This workflow does not enter the original contribution branch.
- [Raw evidence artifact 11298434373](https://github.com/woahwhattheheck/chronicle/actions/runs/37191639409/artifacts/11298434373). GitHub reported archive SHA-256 `907e81da6e21f3bac5486d224a4ee754259a8eee8b7312eb0f85d9474c3ad509`. Raw test events and summary also appear in the job log.

| Check | Native baseline | Native candidate |
|---|---|---|
| Existing `ListQueries` assertion | Expected at least 3 queries; received 2 | Pass |
| Existing `MaxQueriesLimit` assertion | Third query incorrectly accepted at limit 2 | Pass |
| Existing `TestContinuousQueryEngine_Stats` | Expected at least 2 queries; received 1 | Pass |
| Rapid create/delete/recreate regression | Reused ID `cq-1791105451362565800` | 512 distinct accepted handles retained across two batches |
| Deterministic concurrent-capacity regression | Limit 1 accepted 2 requests; one retained entry, created metric 2 | Exactly one accepted and retained; created metric 1 |

The new regressions live in the existing `continuous_query_test.go`. Both executions used identical candidate test bytes: SHA-256 `52976ffa4dae94e233e87af5565135c69a27d26bab377bfe028cc05893d92201`. The baseline received only the additive test-file change; its production files remained pinned to the baseline commit. The candidate checkout remained unchanged and passed `gofmt -l`.

The rapid-create case uses the real clock, with no sleep or clock replacement. The concurrency case uses the existing optimizer rule interface to hold both requests after the initial capacity check, then releases them together. No production test hook was added.

## Reproduction

```powershell
$env:CGO_ENABLED = '0'
$env:GOTOOLCHAIN = 'local'
$env:GOPROXY = 'off'
go test -short -count=1 -timeout=60s -json -run '^(TestContinuousQueryEngine|TestContinuousQueryEngine_Stats|TestContinuousQueryEngine_RapidCreatesRetainQueries|TestContinuousQueryEngine_ConcurrentCreateLimit)$' ./internal/continuousquery
```

Candidate result: exit 0; four top-level passes and five subtest passes; no selected skips or failures. Baseline result: exit 1 with the original failures and both new regressions reproduced. Parent and subtest events are not independent-case counts.

The validated source blobs are engine `2d9c4d4e3f62aab7983bab22832294486fa9a176`, types `650fbdc0b9efc963088ce1d8e2958406947efbb2`, and tests `647b9a426bcbbb27359aa7af6aa586be80939ef4`. The contribution composes those exact blobs onto the current original branch, preserving intervening peer changes.

This closes the selected continuous-query failures recorded in [the earlier Windows report](WINDOWS.md). It does not claim a full-package or repository-wide suite run, race-detector coverage, a throughput benchmark, upstream CI approval, bounty acceptance or payment. Other reported package failures retain their separate owners and evidence.
