# Native Windows: multimodal identity collisions

This is a focused follow-up to [the Windows compatibility report](WINDOWS.md) and the existing PR #104, not a repository-wide test result.

## Observed failure and repair

The original log and span helpers hashed only `time.Now().UnixNano()`. On the native Windows runner, consecutive calls received equal clock readings. Log IDs therefore joined unrelated messages in the full-text index, and a later span overwrote an earlier one in the span map.

The helpers now share a process-monotonic atomic sequence and retain the existing 16-character hexadecimal shape. Caller-supplied log/span IDs, event timestamps, query predicates and storage behavior are unchanged. The sequence prevents same-process clock-resolution, rollback and concurrent-generation collisions; it is not a distributed identity protocol or a cryptographic token.

## Actual execution

Both executions used Microsoft Windows Server 2025 amd64, Go 1.24.13, `CGO_ENABLED=0`, the repository's real package/dependencies and unchanged `go.mod`/`go.sum`.

- Product parent: `f07e64289e5cc38ad569c108d7b9167beb8f2d97`.
- Tested two-file candidate: `92d9e5800281c94215cd05582996684cdce3db3b`.
- Production blob: `659442b68abcb13e6d5cf0ed21df18e8f996bb57`.
- Regression blob: `994ab0171ed4eb58fc2e04f307863cc62212a6fa`.

| Selected case | Original implementation | Repaired implementation |
| --- | --- | --- |
| `TestQueryLogsFullTextSearch` | Returned 3 logs instead of 2 | PASS |
| `TestQuerySpans` | Returned 0 matching spans instead of 1 | PASS |
| `TestMultiModalGeneratedIDUniqueness` | Generated a duplicate ID during concurrent generation | PASS: 4,096 unique log/span IDs |
| `TestMultiModalGeneratedIDsRetainRecords` | Returned all 32 logs instead of the 16 matching messages | PASS: matching log membership, all 32 spans, filtered spans and explicit-ID preservation |

[Original-source run 37193885461](https://github.com/woahwhattheheck/chronicle/actions/runs/37193885461) executed all four cases; all four failed. [Candidate run 37194168811](https://github.com/woahwhattheheck/chronicle/actions/runs/37194168811) executed the identical selection with the exact repaired production blob; all four passed, none skipped. The candidate package reported 0.056 seconds of test execution; this excludes compilation and is not a performance benchmark.

```sh
CGO_ENABLED=0 go test -short -count=1 -p=2 -timeout=120s -json \
  -run '^(TestQueryLogsFullTextSearch|TestQuerySpans|TestMultiModalGeneratedIDUniqueness|TestMultiModalGeneratedIDsRetainRecords)$' .
```

The original two tests use their existing real database fixtures. The new record-retention case exercises the actual in-memory multimodal store with no database collaborator because that path does not call the database. No replacement implementation or clock stub was used.

## Retained artifacts and limits

Original artifact `11299598906`: ZIP SHA-256 `08ebd7cbd87b16cfb9d40956f07a8c33bfdd2618878dd611c523c7be9dcac151`.
Candidate artifact `11299803968`: ZIP SHA-256 `aaf244b2bbc10d1aebfb81d6a0c65ca90df1b36df54ff92d54900400d6d5ed40`.
Both contain the raw JSON test events and runtime/source provenance; the candidate also retains the executed source and regression bytes. Artifacts expire seven days after October 4, 2026; the run logs and this report retain the result and source identities.

The first attempt, run `37193725425`, stopped at patch application because of Windows checkout line endings; no tests ran. The next attempt recorded the four intended original-source failures and exited before the candidate step because the shell inherited `errexit`. The final run reused those original results and executed only the candidate, rather than repeating the baseline. These runner-only corrections did not change the product patch.

Final integration preserves the newer `88f803f8eda57f4a6ccdee8f84df96c37ce43fac` source and the tested candidate ancestry. The two changed source/test blobs were unchanged in that newer parent. No full-suite, race-detector, multi-process identity, live deployment, maintainer acceptance or bounty-payment result is claimed. The temporary runner workflow is not part of the product branch.
