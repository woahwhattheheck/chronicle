# Native Windows streaming SQL registration

This report follows the original Windows Testing Bounty #9 ($50 advertised),
[issue #54 claim](https://github.com/josedab/chronicle/issues/54#issuecomment-5820789518)
and [PR #104](https://github.com/josedab/chronicle/pull/104). Sponsor acceptance and payment are not established by these test results.

## Repair

The root StreamingSQLEngine previously keyed accepted queries only by UnixNano.
Repeated Windows clock ticks could replace an earlier accepted query in the registry.
Capacity admission also unlocked before insertion, allowing concurrent callers to
accept more queries than the configured limit. The repair adds an engine-local
sequence, keeps admission and insertion under the existing queryMu, and launches
execution after unlocking. IDs retain the ssql- prefix; the sequence remains after
StopQuery empties the registry.

Production source and maintained tests:
- Baseline: 6be8f377886cfb8a7586c270ab466a52279244c1.
- Tested candidate: 42c2ecacaf15df14b9c04e1b340627c442a1ea4c.
- streaming_sql.go: 41522a21386cbb39a07a2ec92d1415b91133d61f.
- streaming_sql_test.go: 8ab75957e06516786243293a0eb243da8348d6dd.

## Native execution

[Run 37192645752](https://github.com/woahwhattheheck/chronicle/actions/runs/37192645752)
uses isolated validation commit b61679f1f4565d70ad6e33f9b51a3373384a87cd.
The workflow is not part of the original PR. It executes the actual root package
on native Windows with Go 1.24.13 and CGO disabled. No production substitutes,
clock mock, sleep, or new production scheduling hook are used.

The candidate's exact complete test file is copied into the baseline; all baseline
production files remain unchanged. Both use:

```sh
go test -short -count=1 -timeout=60s -json -run '^(TestStreamingSQLEngine_MaxConcurrentQueries|TestStreamingSQLEngine_ListQueries|TestStreamingSQLEngine_RapidCreatesRetainQueries|TestStreamingSQLEngine_ConcurrentAdmission)$' .
```

Observed native Windows Server 2025 build 10.0.26100, amd64, Go 1.24.13:
baseline exit 1; candidate exit 0. The candidate passed all four selected top-level
tests and both concurrency subcases, with no failed or skipped events.

| Selected case | Observed baseline | Candidate |
| --- | --- | --- |
| Existing maximum-concurrent test | Incorrectly accepted third query at limit 2 | Pass |
| Existing list test | Retained 1 query instead of 3 | Pass |
| Rapid creation and replacement lifecycle | Second create reused ssql-1791106639897463500 | Pass |
| 32 concurrent callers, limit 1 | Pass | Pass |
| 32 concurrent callers, limit 4 | Accepted 32, registry retained 1 | Pass |

Shared complete test SHA256:
3cef09f768c388ca48a97cef304844fdc6827306381b8f84eeac3134db2a572a.
[Job 111407902726](https://github.com/woahwhattheheck/chronicle/actions/runs/37192645752/job/111407902726)
completed successfully on 2026-10-04 at 09:37:48Z.
Artifact 11299432974 is 3507 bytes; GitHub reports ZIP SHA256
ff807cc91822418336049fbd28df9be496003b1765a09749c155aba0b894b5c8.
This report was checked against the full returned native job log; the ZIP was not
downloaded for a separate digest check.

The new lifecycle case retains 64 queries, verifies exact returned pointers and
list/stat counts, stops them, creates a replacement, and verifies that stale IDs
cannot affect it. Concurrent cases release 32 callers at limits 1 and 4. Cleanup
cancels the engine and observes every accepted query's Results channel closing.
Baseline timing/interleaving failures are reported as observed; they are not
assumed to occur on every schedule.

The job artifact retains command, platform/Go environment, original Git blobs,
shared test SHA256, raw JSON events, stderr, and exit/pass/fail/skip summaries.
Only these selected groups ran; this is not full-root-suite, full-repository,
race-detector, load/performance, maintainer-acceptance or payment evidence.
The tested source/test blobs are composed unchanged onto original-branch parent
a5fa154af8f883d351e6a312a6bd9479f8bc7fc9, preserving intervening peer edits.
Those other edits were not re-executed by this job.
