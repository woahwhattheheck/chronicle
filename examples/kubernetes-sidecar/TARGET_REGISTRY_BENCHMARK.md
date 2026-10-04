# Target registration scaling — 4 October 2026

`AddTarget` preserves first-registration-wins, exact address/port identity and
insertion order. Registries below 32 targets retain the linear scan; larger
registries create a private lookup index under the existing mutex. Removal
updates the index, and removing all targets releases it. The ordered slice is
still the output/scrape interface. Removal remains linear.

## Measured scope and tradeoff

One operation constructs a registry, adds N unique synthetic endpoints, then
replays those same N registrations. Endpoint construction is outside timing.
The complete production `k8s_sidecar.go` file and maintained test/benchmark file
were compiled with Go 1.23.2 on Linux amd64, AMD EPYC 9V74, GOMAXPROCS=2. Five
fresh benchmark processes per variant ran in alternating order, 100 ms minimum
per sub-benchmark. The slow 8,192-target baseline performed one operation in
each sample; these are five observations, not a longer statistical study.

| Targets | Baseline median ns/op | Candidate median ns/op | Baseline B/op | Candidate B/op |
| ---: | ---: | ---: | ---: | ---: |
| 1 | 159.0 | 162.8 | 512 | 512 |
| 16 | 2,403 | 2,436 | 3,344 | 3,344 |
| 1,024 | 5,564,468 | 241,277 | 303,376 | 424,000 |
| 8,192 | 337,408,891 | 3,483,944 | 3,580,176 | 4,553,143 |

The larger workloads were 23.1x and 96.8x faster for this operation. The index
trades memory for registration time: allocated bytes per operation increased
39.8% and 27.2% respectively. B/op is cumulative allocation, not peak or
retained RAM. Small workloads had unchanged allocations; +3.8 ns/+33 ns median
changes are not a stable small-workload performance claim. The stress cases exercise much larger registries than the default
single-target configuration. No scrape, database,
Kubernetes discovery, network, startup or whole-application speedup is claimed.

The standalone measurement package supplies compile-only DB/Point definitions
for unrelated parts of the complete source. Its DB write method panics if
called; no benchmark/test uses DB or metric parsing. This is not a full Chronicle
package build or integration run. No dependencies or new runner were installed.

All four focused test functions passed before/after (the first also covers six
sizes across the map threshold). Candidate race execution passed. Cases cover
first-wins/order, exact identity, constructor seed, removal/re-add, seeded
duplicates and concurrent registration/readout. This is a performance change;
these behavioral tests correctly pass on the baseline too.

Baseline commit: `87d28f7ee80960f0d70e29e2bb73e47048771771`.
Baseline production blob: `5e134dfe3d72294c384bf0d80cc213b5eb5b62e0`.
Candidate production blob: `ce5aa1aaeecd1d592b642f3e0eac71872338ec39`.
Maintained test blob: `95304317cea95b9afea7c89e2ffcbe420a85d5cd`.
The existing label and numeric parser improvements are retained unchanged.

## Reproduce the isolated measurement

From the repository root, prepare two temporary standalone packages. The
scaffold below is only for isolated measurement; never add it to the real
Chronicle package, which already defines DB/Point.

```bash
before=$(mktemp -d)
after=$(mktemp -d)
git show 87d28f7ee80960f0d70e29e2bb73e47048771771:k8s_sidecar.go > "$before/k8s_sidecar.go"
cp k8s_sidecar.go "$after/"
for dir in "$before" "$after"; do
  cp k8s_sidecar_target_registry_test.go "$dir/"
  printf 'module local.invalid/sidecar-registry-measurement\n\ngo 1.23\n' > "$dir/go.mod"
  cat > "$dir/standalone_scaffold_test.go" <<'GO'
package chronicle
type DB struct{}
func (*DB) WriteBatch([]Point) error { panic("outside registry measurement") }
type Point struct { Metric string; Tags map[string]string; Value float64; Timestamp int64 }
func (p *Point) ensureTags() { if p.Tags == nil { p.Tags = make(map[string]string) } }
GO
  (cd "$dir" && GOTOOLCHAIN=local GOPROXY=off GOSUMDB=off GOMAXPROCS=2 \
    go test -run '^TestK8sSidecarTargetRegistry' -count=1 .)
done
# Repeat five times, alternating before/after order each round:
(cd "$before" && GOTOOLCHAIN=local GOPROXY=off GOSUMDB=off GOMAXPROCS=2 \
  go test -run '^$' -bench '^BenchmarkK8sSidecarTargetRegistry$' -benchmem -benchtime=100ms -count=1 .)
(cd "$after" && GOTOOLCHAIN=local GOPROXY=off GOSUMDB=off GOMAXPROCS=2 \
  go test -run '^$' -bench '^BenchmarkK8sSidecarTargetRegistry$' -benchmem -benchtime=100ms -count=1 .)
(cd "$after" && GOTOOLCHAIN=local GOPROXY=off GOSUMDB=off GOMAXPROCS=2 \
  go test -race -run '^TestK8sSidecarTargetRegistry' -count=1 .)
```

For a normal installed Chronicle checkout the maintained benchmark/test can run
with `go test .` and the same selection flags, without any scaffold. That
full-package variant was not run for the measurements above.

## Raw observations

Each row is the original Go benchmark observation in execution order. No
outlier trimming; medians above include all five samples for each case/variant.

```csv
repeat,variant,targets,iterations,ns_per_op,bytes_per_op,allocs_per_op
0,baseline,1,742647,162.3,512,2
0,baseline,16,47340,2422.0,3344,6
0,baseline,1024,20,5775067.0,303376,13
0,baseline,8192,1,368437455.0,3580176,20
0,candidate,1,576921,185.4,512,2
0,candidate,16,43360,2783.0,3344,6
0,candidate,1024,499,248158.0,424001,50
0,candidate,8192,39,3839836.0,4553009,297
1,candidate,1,649876,165.9,512,2
1,candidate,16,46741,2540.0,3344,6
1,candidate,1024,511,249118.0,424000,50
1,candidate,8192,36,3315651.0,4552818,296
1,baseline,1,777603,157.4,512,2
1,baseline,16,48666,2635.0,3344,6
1,baseline,1024,20,5675156.0,303376,13
1,baseline,8192,1,348159371.0,3580176,20
2,baseline,1,703154,159.4,512,2
2,baseline,16,46419,2397.0,3344,6
2,baseline,1024,20,5564468.0,303376,13
2,baseline,8192,1,324582108.0,3580176,20
2,candidate,1,688720,162.8,512,2
2,candidate,16,43008,2418.0,3344,6
2,candidate,1024,528,221846.0,423997,50
2,candidate,8192,38,3353324.0,4553143,298
3,candidate,1,764626,154.8,512,2
3,candidate,16,47818,2392.0,3344,6
3,candidate,1024,519,241277.0,423995,50
3,candidate,8192,32,3483944.0,4553402,299
3,baseline,1,744837,159.0,512,2
3,baseline,16,46740,2360.0,3344,6
3,baseline,1024,19,5404561.0,303376,13
3,baseline,8192,1,337408891.0,3580176,20
4,baseline,1,789445,156.3,512,2
4,baseline,16,49222,2403.0,3344,6
4,baseline,1024,21,5408001.0,303376,13
4,baseline,8192,1,337080089.0,3580176,20
4,candidate,1,704590,155.8,512,2
4,candidate,16,49935,2436.0,3344,6
4,candidate,1024,530,240295.0,424034,50
4,candidate,8192,30,3535892.0,4553714,301
```
