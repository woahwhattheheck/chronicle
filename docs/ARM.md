# 32-bit ARM execution report

This report records the 2026-10-03 continuation of Bounty #10 and PR #108.
The core tests, internal packages, and database workloads were executed as
32-bit ARM binaries under QEMU user-mode emulation. Physical-board coverage is
listed under limitations.

## Environment

| Item | Observed value |
|---|---|
| Host | Linux/x86_64, kernel 6.18.44 |
| Compiler | Go 1.27.1, normal compiler optimizations |
| Emulator | QEMU 8.2.2 |
| Target | `GOOS=linux GOARCH=arm GOARM=7 CGO_ENABLED=0` |
| Executable | Statically linked ELF32 ARM/EABI5 |
| Core execution | `GOMAXPROCS=2`, short mode, one parallel test at a time |
| Comparison revision | `cd90a7dc11563980eb43860f80deb901cd9070e7`, before these runtime repairs |

The final default core executable had SHA256
`b0b02d214ff925d144190035bb37e40269d2c53829ef00df0387446be532eb61`.
Its source hashes were checked before and after execution. Compilation was
serialized with `-p=1`, `GOMAXPROCS=1`, `GOGC=10`, and `GOMEMLIMIT=384MiB`
to reduce pressure on the shared build host.

## Runtime defects repaired

**64-bit atomic alignment.** Existing ARM tests panicked with
`unaligned 64-bit atomic operation` in fault injection, relay accounting,
hardware-query counters, natural-language dashboards, query planning, ETL
snapshots, WASM plugin statistics, and internal protocol, hardware, and
telemetry components. A grouped layout check also found the same problem in
connector sink statistics, whose previous tests did not execute a database
flush.

Private counters now use aligned typed atomics where appropriate. Public
statistics retain their scalar field types and numeric JSON values; relay,
planner, connector, and telemetry updates use the same mutex as their snapshots.
The ETL backpressure field is read under its existing snapshot lock, and the
fault-injection counter is the first field of its allocated object.
Connector getters return detached scalar statistics and status snapshots.

The regressions exercise actual operations: connector delivery, failed writes
and retry, checkpoint advancement, concurrent query planning, streaming queries,
dashboard generation, relay enqueueing, and hardware API paths. They check exact
totals, point membership, and snapshot/JSON behavior.

**Signed index overflow.** `HashPartitioner` converted an unsigned FNV hash to
`int` before taking its remainder. High-bit hashes produced negative buckets on
ARM, and `PartitionPoints` panicked. Taking the unsigned remainder first keeps
indices in range and preserves the existing 64-bit bucket mapping, including
large positive partition counts. Fixed hash vectors and bucket membership are
covered by the tests.

Anomaly sampling, edge gossip, and experimental local training also converted
nanosecond timestamps to `int` before selecting an index. Their selection now
uses bounded random indices. Gossip coverage uses an in-memory HTTP transport;
training coverage checks order-independent weight updates and preserved sample
membership.

The earlier duration-arithmetic corrections and `cgo` build constraints on C FFI
tests remain in this PR. The experimental `TestExtractFeatures` was renamed to
`TestFederatedLearningExtractFeatures` to resolve its collision with the default
data-lifecycle test and make the tagged trainer check buildable.

## Observed verification

| Scope | Result |
|---|---|
| Complete default short core run | Completed without a panic: 3,377 top-level tests/examples passed, 115 skipped, 10 failed as classified below. |
| Three disk-health cases after capacity recovery | All passed on the same unchanged executable. Across the complete run and this targeted rerun, 3,380 top-level tests/examples passed. |
| `go test -short -count=1 ./internal/...` under QEMU | All 25 packages passed; 982 passing test/subtest results and two skipped results. |
| Independent exact counter/planner/database/WAL checks | All 11 passed on the final executable, including six previously demonstrated alignment panics. |
| Experimental local trainer on ARM | The ordinary tagged command passed, including all five sample-count subcases (0, 1, 2, 7, and 32); no source overlay was used. |
| Original revision, native amd64 comparison | All seven remaining failure families reproduced. Both original hash-partitioner tests passed natively, while they failed on ARM before the repair. |
| Native `go vet ./...` | Passed. |

`make validate` exited with status 2 after its native vet step passed.
The Makefile selected `golangci-lint` v1.64.8, whose type checker could not read
the Go 1.27 export data: `export data version 4 is greater than maximum supported
version 2`. Its subsequent type-check diagnostics do not establish a successful
lint run. The later test and generation steps in that command were not reached;
the independently executed tests are recorded above.

The complete core run encountered a shared volume with zero free space. Its
three additional failures were `TestHealthCheckEngine`,
`TestHealthCheck_RealChecks`, and `TestHTTPHealth`; the disk component correctly
reported that free space was below its 100 MB threshold. All three passed after
capacity was recovered, with no code change.

The default core suite retains these seven pre-existing failure families on
both architectures:

| Test | Observed cause |
|---|---|
| `TestAlertManager_Webhook` | Its HTTP fixture is rejected by the existing HTTPS-only webhook validation. |
| `TestConfig_DeprecationNormalization` | Fixtures omit required positive storage partition duration and buffer size values. |
| `TestGRPCIngestionEngine` | Its aggregation subtest omits a positive aggregation window. |
| `TestGRPCHandleQueryWithAggregation` | The same aggregation-window validation rejects its fixture. |
| `TestAutoMLSelector` | The default build cannot train its experimental models. |
| `TestMLInferencePipeline_TrainAndScore` | Training requires the experimental build tag; the missing trained model follows that failure. |
| `TestModelRegistry` | Its model-saving path requires the experimental build tag. |

## Bounded database workloads

The final ARM executable passed the existing error-checking
`BenchmarkWriteBatch` and `BenchmarkQueryAggregate` workloads. Each ran three
times with 20 operations per run, `GOMAXPROCS=2`, and normal compiler
optimizations.

| Workload | Median time/op | Median bytes/op | Median allocations/op |
|---|---:|---:|---:|
| Write a 1,000-point batch across 10 hosts | 58.147 ms | 3,292,114 | 59,809 |
| Grouped one-second mean query over 10,000 seeded points / 50 hosts | 27.117 ms | 264,522 | 17,225 |

`WriteBatch` reuses one prepared payload and its timestamps on every operation;
the aggregation benchmark repeats one query against the same seeded database.
Setup and seeding are excluded by `ResetTimer`, while deferred `db.Close` is
amortized into each 20-operation sample and its returned error is ignored.
Open, write, and query errors are fatal to the benchmarks. Query rows are
discarded, with result correctness covered by the separate database tests.

These are observed costs of buffered database operations under emulation on a
shared host. Physical Pi performance, durable-storage throughput, and a
before/after speedup were not measured.

## Reproduction

Use Go 1.24 or later and `qemu-arm-static` on the host. The recorded run used the
versions above. Choose a normal temporary directory whose volume has more than
100 MB free for the built-in health checks. Set both `TMPDIR` and `GOTMPDIR`:
the tested Go version's `testing.TempDir` also consults `GOTMPDIR`.

```bash
CHRONICLE_ARM_TMP=$(mktemp -d /tmp/chronicle-arm.XXXXXX)
export TMPDIR="$CHRONICLE_ARM_TMP" GOTMPDIR="$CHRONICLE_ARM_TMP"
export GOTOOLCHAIN=local CGO_ENABLED=0 GOOS=linux GOARCH=arm GOARM=7

GOMAXPROCS=1 GOGC=10 GOMEMLIMIT=384MiB \
  go test -p=1 -c -o "$CHRONICLE_ARM_TMP/core.test" .

GOMAXPROCS=2 qemu-arm-static "$CHRONICLE_ARM_TMP/core.test" \
  -test.short -test.count=1 -test.parallel=1 -test.timeout=10m -test.v

GOMAXPROCS=1 go test -p=1 -exec qemu-arm-static \
  -short -count=1 -parallel=1 ./internal/...

GOMAXPROCS=2 qemu-arm-static "$CHRONICLE_ARM_TMP/core.test" \
  -test.run='^$' \
  -test.bench='^(BenchmarkWriteBatch|BenchmarkQueryAggregate)$' \
  -test.benchtime=20x -test.count=3 -test.benchmem -test.timeout=2m

GOMAXPROCS=1 go test -p=1 -tags experimental -exec qemu-arm-static \
  -short -count=1 -run '^TestLocalTrainerTrainEpochSampleMembership$' .
```

The default core command currently exits unsuccessfully because of the seven
failure families above. The internal-package, bounded benchmark, and focused
experimental trainer commands passed in the recorded run.

## Platform and verification limits

- Physical Pi execution, board memory limits, power use, storage endurance, and
  soak behavior remain unmeasured.
- This continuation executed ARMv7. ARM64 was not revalidated.
- `CGO_ENABLED=0` excludes C FFI; an ARM C toolchain and separate execution are
  needed for that path.
- eBPF, TEE, GPU, and FPGA hardware were not exercised. Hardware API and fallback
  tests run as software under emulation.
- The complete race-detector suite was not run. A native race run of the changed
  internal telemetry package passed; broader native race compilation earlier
  exceeded the shared host's memory budget.
- The default suite results include the explicit skips and existing failures
  above. They do not establish an entirely green repository-wide CI run.
