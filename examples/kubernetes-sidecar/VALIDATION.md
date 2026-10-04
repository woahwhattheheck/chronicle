# Kubernetes sidecar validation

## Executed source and result

A bounded hosted Linux job completed successfully on 2026-10-04 at 08:04:28 UTC:
[workflow run 37187277094](https://github.com/woahwhattheheck/chronicle/actions/runs/37187277094),
job `health-lifecycle` (111391793163).

The job checked out **2052a9fa68ecad868e82f894c2e75fa3c33679c7**, not a moving branch.
Its source tree was `1157ba43eb4410188e8dfe76c57918fdede5e580` and its
`k8s_sidecar.go` blob was `a2aeeb25388c7552d7310016eca5ed7a015a3ee9`.
The runner installed Go 1.24.0; module execution selected the declared Go 1.24.4
toolchain. Compilation concurrency was bounded with `GOMAXPROCS=2` and
`GOFLAGS=-p=2`.

The check compiled the actual root package and its dependencies. It did not
substitute database or point types. The focused health-lifecycle test used no
database and disabled discovery, with a one-hour scrape interval, so storage
and scraping were not exercised by that test.

| Check | Observed result |
| --- | --- |
| Previous `k8s_sidecar.go` from parent `9904db19571e9d52d1bb8a434ff8ab50bbdbe7dc` | Exit 1: `Start accepted an occupied health port` |
| Repaired startup with an occupied health port | Returned an error; running state rolled back; retry context remained usable |
| Retry after releasing the port | Started successfully and served all five checked routes with HTTP 200 |
| Duplicate `Start`, `Stop`, repeated `Stop` | Duplicate start rejected without stopping the server; shutdown canceled the context; repeated stop succeeded |
| Listener cleanup | The health port could be rebound after shutdown |
| Example test package | All four top-level tests passed, including ten invalid-configuration subcases |
| Example build | Exit 0 |

The five routes were `/health`, `/api/v1/sidecar/health`, `/stats`, `/targets`,
and `/api/v1/sidecar/metrics`. The existing example tests covered mode parsing,
configuration rejection, serving application metrics and releasing its port,
and preserving Downward API environment values.

## Reproducing the pinned check

The validation-only commit
[`3cfc5bacb73f040e43c76162d438c3c7343882f5`](https://github.com/woahwhattheheck/chronicle/commit/3cfc5bacb73f040e43c76162d438c3c7343882f5)
contains the workflow and the temporary
`k8s_sidecar_health_lifecycle_test.go` fixture. It is not part of this example's
implementation. From a checkout containing both commits:

```sh
git checkout --detach 2052a9fa68ecad868e82f894c2e75fa3c33679c7
git show 3cfc5bacb73f040e43c76162d438c3c7343882f5:k8s_sidecar_health_lifecycle_test.go \
  > k8s_sidecar_health_lifecycle_test.go

# Repaired source: expect success.
go test -v -run '^TestK8sSidecarHealthBindFailureAndRetry$' -count=1 -timeout=60s .

# Previous source: expect failure for the occupied-port assertion specifically.
git show 9904db19571e9d52d1bb8a434ff8ab50bbdbe7dc:k8s_sidecar.go > k8s_sidecar.go
go test -v -run '^TestK8sSidecarHealthBindFailureAndRetry$' -count=1 -timeout=60s .

# Restore the tested repair before checking the example.
git restore --source=2052a9fa68ecad868e82f894c2e75fa3c33679c7 -- k8s_sidecar.go
(cd examples/kubernetes-sidecar && go test -v -count=1 -timeout=60s . && go build .)
```

The workflow additionally requires the baseline's specific occupied-port
failure text, verifies the restored source blob, and propagates command
failures through Bash `pipefail`.

## Retained evidence and limits

The [five-file evidence archive](https://github.com/woahwhattheheck/chronicle/actions/runs/37187277094/artifacts/11297692099)
contains `source.txt`, `before.log`, `before.exit`, `after.log`, and `example.log`.
Artifact ID: `11297692099`. The downloaded 2,122-byte ZIP was independently
verified with SHA-256:

```text
da9aaf01d3f82f78a6f9470e323abcadf68008c30afeb9dec4854211fb9b54bc
```

Artifact retention is 14 days from upload; the workflow and this source-bound
result remain in Git history. This run does **not** assert all root-package
tests, race-detector coverage, database ingestion/query behavior, a Kubernetes
deployment, or execution of subsequent commits. In particular, the later
startup-test addition, example changes, and label-regexp reuse through
`fa7800904fcfa45f1da08ffcf30dbb5933088628` were not the checkout used by this run.
The later `k8s_sidecar_startup_test.go` remains the branch's regression coverage;
the temporary validation fixture need not be added to the branch.

## Prometheus parsing and throughput (2026-10-04)

Commit `bd05cac196e8d6fd39fb473d84c933003902ee95` preserves quoted braces,
escaped quotes/backslashes/newlines, UTF-8 label values, and tab-separated
samples. On parent `584818a0609718005e2de36384bd723d5d4df153`, the actual parser
failed 11 of the 13 valid-label cases and the malformed-line recovery case.
All 14 pass after the repair in
[run 37190090089](https://github.com/woahwhattheheck/chronicle/actions/runs/37190090089),
job `111400272397`. This first correctness change increased the same-run
plain-label benchmark median from 126,030 to 134,558 ns per 100 samples,
with 1,209 allocations unchanged. Those measurements used an Intel Xeon
6973P-C and are separate from the following numeric-conversion comparison.

Commit `354a543814752ba8fc59e9ef8230443f9e3836bb` replaces both numeric
`fmt.Sscanf` calls with `strconv.ParseFloat`. Invalid values such as
`7garbage` and `1.2.3` no longer silently become their numeric prefixes.
The actual root package compiled and all 40 focused cases passed: 13 valid
label cases, one recovery/target-label-precedence case, 18 numeric cases,
and eight invalid-value cases. Numeric checks include signed zero, scientific
notation, infinities, and NaN. The label repairs remain included.

[Run 37190380248](https://github.com/woahwhattheheck/chronicle/actions/runs/37190380248)
compared the quote-corrected parent `bd05cac196e8d6fd39fb473d84c933003902ee95`
with this numeric follow-through on the same AMD EPYC 9V74 runner, using
Go 1.24.4, `GOMAXPROCS=2`, and `GOFLAGS=-p=2`. Three 100 ms repetitions of the
identical 100-sample plain-label workload produced these medians:

| Per 100 samples | Quote-corrected parent | Numeric follow-through |
| --- | ---: | ---: |
| Time | 139,142 ns | 104,206 ns |
| Allocations | 1,209 | 909 |
| Allocated bytes | 108,471 | 97,238 |

That is 25.1% less parser time and 24.8% fewer allocations for this workload.
It is not an end-to-end ingestion, Kubernetes, or fleet throughput result.
The two separate runners' timings must not be combined into a same-machine
comparison against the original uncorrected version.

The executed final source blob is `5e134dfe3d72294c384bf0d80cc213b5eb5b62e0`;
the numeric test blob is `19ac80741c03b026a8d6302bba04ac908ed3de39`.
The [ten-file evidence archive](https://github.com/woahwhattheheck/chronicle/actions/runs/37190380248/artifacts/11298916242)
contains the pinned source identities, before/after logs and raw benchmark
repetitions, patch, and executed source/test files. Its downloaded ZIP SHA-256 is
`337e9982092304cc5a4a92533cba262e374cb4ee28a1e3cc187dc44b7fbee119`.

To repeat on the final source commit from the repository root:

```sh
go test -v -run '^TestK8sSidecarPrometheus' -count=1 -timeout=60s .
go test -run '^$' -bench '^BenchmarkK8sSidecarPrometheusPlainLabels$' \
  -benchmem -benchtime=100ms -count=3 -timeout=60s .
```

No dependencies or earlier health/example implementations changed in these
parser commits. These runs did not repeat the full repository suite,
race detection, storage ingestion, or Kubernetes deployment. The temporary
runner workflows are not part of the product branch.
