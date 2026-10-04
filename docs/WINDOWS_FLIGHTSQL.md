# Windows Flight SQL ticket identity and query fixture

This continuation belongs to the original [Windows testing PR #104](https://github.com/josedab/chronicle/pull/104), issue #54. It fixes ticket identity loss reproduced in the actual Windows implementation and resolves one separately identified fixture timing failure. It does not replace the existing Windows compatibility reports or establish a complete Windows-suite pass.

## What changed

`GetFlightInfoStatement` and `CreatePreparedStatement` previously generated their registry keys solely from `time.Now().UnixNano()`. On the tested Windows clock, consecutive calls returned the same timestamp. Inserting the later SQL statement replaced the earlier statement under a ticket already returned to its caller. Closing and immediately creating another statement could also rebind the closed ticket.

Both APIs now allocate their opaque IDs while holding the existing `sessionMu` and register the statement in that same critical section. The `stmt-` and `prepared-` prefixes remain. The ID uses the standard-library [`crypto/rand.Text`](https://pkg.go.dev/crypto/rand#Text), available since Go 1.24, which is already the module's minimum. A live-map check preserves an already occupied ID before insertion. No module, lock, query parser, database, or wire-format change is required.

A local sequence was the first tested candidate. Review identified that a sequence starting at one would deterministically repeat across newly constructed servers. The final random-ID candidate removes that new alias. The first candidate is retained in the validation history and was not published to the original contribution branch. The original baseline was not rerun.

The existing `TestFlightSQLServer_HandleStatementQuery/valid_query` failure has a separate cause. Its rows were timestamped at `now` and `now + 1ns`, while the default SQL query uses an **exclusive** upper bound of `now`. A coarse clock can leave both rows outside that range. `WriteBatch` already persists synchronously. The fixture now timestamps those same rows one minute in the past, inside the existing last-hour range; production query semantics and all original assertions remain unchanged.

## Native Windows evidence

Baseline: original PR head `0c6cb51397f4c564e6c8595139796b04bb144b95`, production blob `4b9a1c31b8060d35f30a9ad2d95cd3c39458f528`. Only the same added public-API identity regression was overlaid for the baseline; the existing query fixture retained its original timestamps. The baseline and the first candidate executed in [run 37200445799](https://github.com/woahwhattheheck/chronicle/actions/runs/37200445799).

Final candidate: immutable controller `c8923af5f4239f15795c8d1f43d130a0dfb65f87`, executed once in [run 37200781892](https://github.com/woahwhattheheck/chronicle/actions/runs/37200781892). Exact final product blob `8dd540d79dcb328e768ded41b2d4c87e873e31ab` and test blob `7cdae1b81fb625f00c5cfadd3267c7c1c8368bb8` match the downloaded executed bytes. The final artifact SHA-256 is `454d2f2b628089aefdb28e1de5ce16c1ea4aaa3247f6e3cdcc6d43d3cb67997a`. The [machine-readable result](WINDOWS_FLIGHTSQL_RESULTS.json) preserves the raw test output, separate candidate history, commands, versions, and archive digests. Publication preserves the later documentation-only parent `1b297a1bc7ece0ee03f1367b78a778b274308fdb`.

| Public API and creation mode | Requests | Baseline retained IDs | Baseline changed SQL bindings | Final retained IDs | Final changed SQL bindings |
| --- | ---: | ---: | ---: | ---: | ---: |
| Statement ticket, sequential | 1,024 | 3 | 1,021 | 1,024 | 0 |
| Statement ticket, 16 concurrent workers | 1,024 | 4 | 1,020 | 1,024 | 0 |
| Prepared statement, sequential | 1,024 | 1 | 1,023 | 1,024 | 0 |
| Prepared statement, 16 concurrent workers | 1,024 | 2 | 1,022 | 1,024 | 0 |

The baseline has **2 passing and 7 failing subcases** across the two selected groups. The final candidate passes **all 11 subcases**, including the two added fresh-instance controls, with **zero failures or skips**. Counting parent-group events gives 13 passing events; those are not 13 independent test cases. Across the four creation groups, all **4,096** returned IDs retain their original SQL binding; the baseline retained only **10** unique IDs and changed **4,086** bindings.

The original valid-query fixture returned zero rows on the native baseline and passes after the fixture correction. SQL-disabled and invalid-SQL controls pass throughout. The final selection additionally checks both factories against a fresh server instance, preservation of existing client-provided records, and invalidation of a closed generated ticket. The fixed `prefix-1` record is a preservation control; it does not force a random collision or prove that retry branch was executed.

Both runs use Windows Server 2025, Go 1.24.13, amd64, and `CGO_ENABLED=0`. The selected command is:

```sh
go test -short -count=1 -timeout=90s \
  -run '^TestFlightSQLServer_(HandleStatementQuery|StatementIdentity)$' -json .
```

The final two Go files pass `gofmt`. The retained [initial runner](https://github.com/woahwhattheheck/chronicle/blob/c9b149031c4d09c8f7541e0f0055bfdbad65f54f/work/validation/chronicle104-flightsql-relay17/run.py) and [final runner](https://github.com/woahwhattheheck/chronicle/blob/c8923af5f4239f15795c8d1f43d130a0dfb65f87/work/validation/chronicle104-flightsql-relay17/run_final.py) record the exact inputs, commands, outputs, and source identities. Runner workflows remain on isolated validation history, outside the original PR's product changes.

## Scope of the result

The query test uses the real database, write path, translator, and query result. Identity tests execute the real public allocation APIs and inspect the actual stored SQL bindings; they do not execute 4,096 database queries or claim networked BI-client acceptance. No clock replacement, mocks, skipped selected tests, or sleep-based fixture repair is used.

This is two focused root-package groups, not the full repository suite, a race-detector run, or a performance benchmark. Random identifiers avoid deterministic clock/reset aliases; this is not a permanent tombstone or global uniqueness promise for arbitrary client-chosen IDs. Existing source and submission ownership are preserved. The advertised Windows-testing bounty remains subject to maintainer acceptance and payment.
