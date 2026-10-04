# Windows gRPC aggregation and overlapping partition selection

This continuation belongs to the original [Windows testing PR #104](https://github.com/josedab/chronicle/pull/104), issue #54. It repairs two product failures observed on native Windows. Neither defect is asserted to be Windows-specific.

## Product changes

The gRPC ingestion handler constructed an `Aggregation` with a reducer but no window. Both existing aggregate-query tests therefore failed with `aggregation window must be positive`. The handler now uses the same one-second default as [the existing SQL parser](https://github.com/woahwhattheheck/chronicle/blob/efb80aefe3af26183e765607ea013ca2d3ab2b1b/internal/query/parser.go). The request has no window field. Unsupported reducers remain errors, and the existing error/success counter behavior is preserved. Core positive-window validation and the request/response schema remain unchanged.

The new value regression exposed a second failure: even a raw query returned zero points when its start fell inside the containing partition. `Index.FindPartitions` passed the query interval to `BTree.Range`, whose contract selects **partition start timestamps** in that interval. A containing partition beginning earlier was omitted.

Partition selection now scans the authoritative, ordered partition slice and applies the existing half-open overlap conditions. It stops once a partition begins at the exclusive query end. Empty/reversed bounded ranges remain empty, and zero start/end values retain the query engine's unbounded meaning, including pre-epoch data. Insertions sort the slice; removals preserve its order; index serialization and recovery preserve that same sequence. The prior B-tree range traversal already visited every node, so this is not a claimed asymptotic speedup or benchmark result.

## Native evidence

The [initial comparison](https://github.com/woahwhattheheck/chronicle/actions/runs/37202462701) used baseline `efb80aefe3af26183e765607ea013ca2d3ab2b1b` and identical test bytes before/after the gRPC-only patch. Baseline production blob `ce7a61a3734742714bcb3a77764f0a88e8ff30ef` failed both original maintained aggregation cases. The gRPC-only candidate fixed those original cases, but all nine new bounded value/raw cases still returned zero points. The unsupported-reducer control passed in both versions.

That initial run is **failed**, not a passing final result. Its raw output was subsequently [inspected without rerunning tests](https://github.com/woahwhattheheck/chronicle/actions/runs/37202749969), using retained artifact `11302983717`. This isolated the partition-selection failure. The original baseline was not replayed.

The [final candidate-only Windows run](https://github.com/woahwhattheheck/chronicle/actions/runs/37203434883) passed: **14 selected top-level test groups and 28 subcases; 42 test pass events, zero failures and zero skips** (39 independent leaf cases; parent group events are not additional independent tests). It reran the unchanged gRPC value assertions together with the maintained index cases needed for the observed selection defect. Controller commit: `183ceeb1f9e29d685882c24d755500e8d1b5d813`.

The exact executed Go blobs are also the published source postimages:

| File | Git blob |
| --- | --- |
| `grpc_ingestion.go` | `e78b4845364bc04c90c6993bdf6c42837aadd513` |
| `grpc_ingestion_write_test.go` | `484c4ddcbd1a292d2ecb9e1c7d4de9a6bc051668` |
| `index.go` | `232944e3986d8997716088488ef8feb916de7295` |
| `index_test.go` | `140e0bf728092063014a2ac1cd88dc0cd80f6c05` |

Final artifact `11303603724` retains the raw JSONL, summary and source files. GitHub reports archive digest `sha256:fd355406e9392b41651b0d63790e512d67e6e8de27a40a9308624f93e0d187e9`; this is provider metadata, not an independently downloaded archive hash. The committed JSON result also retains the final raw events and the earlier failed observations, so the acceptance evidence does not depend on artifact retention.

Publication is based on original-branch parent `f47fac34f6de45d648b8c86f2cec6f00f88176c9`. The only intervening change since executed product baseline `efb80aefe3af26183e765607ea013ca2d3ab2b1b` is the existing Raft result index in `docs/WINDOWS.md`; it is preserved.

The value cases assert two precise one-second buckets:

| Reducer | First bucket | Second bucket |
| --- | ---: | ---: |
| Sum | 40 | 120 |
| Average / mean | 20 | 60 |
| Minimum | 10 | 50 |
| Maximum | 30 | 70 |
| Count | 2 | 2 |
| Rate | 25 | 40 |

The fixture includes a different-host record and a record exactly at the exclusive end; neither enters the result. The raw control preserves all four original timestamps/values. The limit control returns one aggregate bucket. Index coverage asserts exact overlapping partition identities, including a query wholly inside a partition, exact boundaries, an earlier long overlap, unbounded end, empty/reversed ranges, pre-epoch unbounded-start selection, out-of-order insertion followed by real encode/decode recovery, and the existing removal controls.

## Reproduction and scope

The comparison and final run use Windows Server 2025, Go 1.24.13, amd64, and `CGO_ENABLED=0`. The final selected command is:

```sh
go test -short -count=1 -timeout=90s -run '^(TestGRPCIngestionEngine|TestGRPCHandleQuery(WithAggregation|AggregateBuckets|ReturnsResults|NilRequest)|TestIndex_(GetOrCreatePartition|FindPartitions(_Empty)?|RemovePartitionsBefore(_Empty)?|RemoveOldestPartition(_Empty)?|RemovePartitionByID(_NotFound)?))$' -json .
```

The original gRPC test fixture and its new value assertions remain unchanged between the first and final candidates. The final four Go files pass `gofmt`. The public validation controller retains commands, raw events and exact source identities; the [machine-readable result](WINDOWS_GRPC_AGGREGATION_RESULTS.json) preserves the separate initial and final observations.

Execution covers the real request handler, database, aggregation and index paths. It does not establish a networked gRPC/HTTP client result, a full repository suite, race-detector coverage, or a performance benchmark. Validation workflows remain on isolated history. All unrelated original-PR source and documentation changes are preserved; maintainer acceptance and bounty payment remain separate.

