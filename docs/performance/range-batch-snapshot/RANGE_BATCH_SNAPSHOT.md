# Range-partition batch snapshot

## Change

`RangePartitioner.PartitionPoints` now snapshots the boundary slice once under
the existing read lock and uses that immutable generation for every row.
`SetBoundaries` already copies, sorts, and replaces its slice instead of mutating
an installed slice, so no extra copy or long-held batch lock is needed.
Single-point partitioning, boundary updates, public types, stable within-bucket
ordering, missing-tag behavior, and non-nil empty buckets remain unchanged.

Previously the batch fixed its bucket count, unlocked, and reread the current
boundaries once per point. Concurrent updates could split identical-tag points
between different boundary generations. A stale-count clamp prevented some
out-of-range indices but did not make the result consistent. The clamp is no
longer necessary because the search and bucket count use the same snapshot.

## Executed scope

2026-10-04, Go 1.23.2, Linux amd64, `GOMAXPROCS=2`. The complete production
`RangePartitioner` declaration block and original `Point` declaration were
compiled in an isolated standard-library package. No fake partitioner, mocked
lock, or alternate implementation was used. The isolated imports/module wrapper
are not a complete repository build; Chronicle's project minimum is Go 1.24.

Parent: `eaee58a171945db46b0721c5b06d2daa4324d7b8`.

| Input | Git blob |
|---|---|
| Original complete partition_advanced.go | `97612b29707417b38a6fd6df192497254a783206` |
| Published complete partition_advanced.go | `2879da5f7a91caf6b5a5e17acf56b56257b6551e` |
| Original point.go containing the selected Point declaration | `c5e5cd60adb6426e9fcb472ca2bdff45fa924424` |
| Published focused regression and benchmark | `adda1ef61283bce7576c83a02dc3a60c61387eb5` |

The original compatibility check passed, but the concurrent-update regression
failed on batch 0: identical-tag points split **9,985 / 15** between buckets.
The repaired compatibility and concurrent-update checks both passed. A separate
`go test -race -count=1 -v .` of the same isolated declarations also passed both
checks. The updated test runs 32 bounded batch iterations under one live setter;
there are two test functions, not a repository-wide test run.

## Measured cost

The existing complete batch path was measured at 1,000 and 100,000 points with
16 fixed boundaries. Fixture creation is excluded; result allocation, tag
lookup, sorting-search, locking, and bucket append are included. Seven paired
process executions alternated before/after order, using the Go benchmark's
100 ms calibration and allocation reporting. There was no concurrent setter in
these timing samples; correctness under updates is the separate regression.

| Points per batch | Before median | After median | Median change | Bytes/op, both | Allocs/op, both |
|---|---:|---:|---:|---:|---:|
| 1,000 | 54.388 us | 50.980 us | -6.27% | 94,400 | 118 |
| 100,000 | 11.184286 ms | 10.993604 ms | -1.70% | 16,255,552 | 284 |

The source removes per-point lock reacquisition: a batch of N points goes from
N+1 read-lock/unlock pairs to one. This is a source-operation count, not an
instrumented provider or fleet metric. The wall-time differences are modest and
samples are noisy; especially the 100,000-point result is not a reliable broad
speedup claim. No reduction in result allocation was observed. A replaced
boundary backing array remains alive until any batch using it completes.

All raw paired timing results below are nanoseconds/op:

| Pair | Execution order | 1k before | 1k after | 100k before | 100k after |
|---|---|---:|---:|---:|---:|
| 1 | before / after | 54388 | 58572 | 11424683 | 10890257 |
| 2 | after / before | 52805 | 55215 | 11184286 | 12031065 |
| 3 | before / after | 50940 | 49393 | 10748595 | 12196442 |
| 4 | after / before | 55071 | 50980 | 10385058 | 10028998 |
| 5 | before / after | 56099 | 47472 | 11196549 | 10200362 |
| 6 | after / before | 52539 | 53391 | 13163325 | 10993604 |
| 7 | before / after | 58365 | 49718 | 10908532 | 12747218 |

## Reproduction

From a normal complete checkout with the repository's required toolchain:

```sh
go test -run '^TestRangePartitionPointsSnapshot' -count=1 .
go test -race -run '^TestRangePartitionPointsSnapshot' -count=1 .
go test -run '^$' -bench '^BenchmarkRangePartitionPointsSnapshot$' -benchtime=100ms -benchmem .
```

The recorded isolated execution used the same test/benchmark source and the
unaltered declarations between `// --- Range Partitioning ---` and
`// --- Dynamic Partition Split/Merge ---`, plus the original Point declaration.
Its module contained only those declarations, standard imports `sort`/`sync`,
and the focused test file. Both revisions were compiled once, then their test
binaries ran with `-test.run=^$ -test.bench=^BenchmarkRangePartitionPointsSnapshot$
-test.benchtime=100ms -test.count=1 -test.benchmem` for each timed sample.

The existing [ARM execution report](../../ARM.md) remains unchanged and belongs
to its recorded source. This continuation did not rerun ARM emulation, physical
hardware, database I/O, project-wide tests, lint, or hosted CI. It does not
establish sponsor acceptance or payment.
