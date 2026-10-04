# Windows configuration migration test fixture

Existing submission: [Windows testing PR #104](https://github.com/josedab/chronicle/pull/104).

## Why these cases failed

The retained Windows run [37189099263](https://github.com/woahwhattheheck/chronicle/actions/runs/37189099263/job/111397350481)
reported all 16 subcases of `TestConfig_DeprecationNormalization` failing at
source `006bc101c1f2079b04696dfc497f5587328fcc46`. Its log establishes the
failure cluster; the following cause was diagnosed in the current source.

Each case constructed a configuration with only `Path`, changed the field
under test, and called `Validate()` before checking migration. Validation
requires positive `Storage.PartitionDuration` and `Storage.BufferSize`.
At least one remained zero in every case, so the fixture failed validation
before the intended migration assertion.

## Change

The shared fixture now initializes a one-hour partition duration and a
10,000-point buffer. These are the existing documented storage defaults.
The two cases specifically testing duration and buffer migration still
explicitly zero their own target fields before setting the legacy value.

All 16 case setups and checks are unchanged, including the nested-field
precedence case. Production configuration normalization and validation are
unchanged. This repairs the maintained fixture rather than relaxing validation
to accept an invalid configuration.

## Focused Windows result

**16 subcases passed, 0 failed, 0 skipped**, with the parent group and package
also passing. The group took 0.02 seconds and the package test execution took
0.043 seconds; these are test-reported durations and exclude runner setup and
compilation. The changed fixture also passed `gofmt -l config_test.go`.

- [Focused run 37193189289](https://github.com/woahwhattheheck/chronicle/actions/runs/37193189289)
  and [job 111409505498](https://github.com/woahwhattheheck/chronicle/actions/runs/37193189289/job/111409505498):
  completed successfully on 2026-10-04.
- Runtime: Go 1.24.13, Windows/amd64, Windows Server 2025 Datacenter,
  `CGO_ENABLED=0`.
- Exact executed source:
  [`fc0ee34bca493768cb48ed0922311b1601a5ae62`](https://github.com/woahwhattheheck/chronicle/commit/fc0ee34bca493768cb48ed0922311b1601a5ae62),
  based on `2b9080f674a5d7c837291b750e825951ee8b8478`.
- Unchanged production `config.go` blob:
  `4ce80754bba6cadd70f3272963410a72beaabd7b`.
- Executed `config_test.go` blob:
  `08f7cd54a8c806974bcff3e8ca84d34afd3b4c9c`.
- [Retained execution artifact 11300236023](https://github.com/woahwhattheheck/chronicle/actions/runs/37193189289/artifacts/11300236023):
  2,444 bytes; GitHub-reported SHA-256
  `ab54d573072c36fd63d2b418fc3c3103d4521f44c1d4e91f223b76fe860546ed`.
  It contains source/runtime provenance, the raw test JSON and exit status.
  The artifact is retained until 2026-10-11; source and this result note remain
  in Git history.

The isolated workflow controller is
`a8f45bc97a4e95b1f5f17d0df75f5825013cad5c`; it checked out the exact source
commit above. The original submission subsequently received this same tested
fixture blob while preserving newer unrelated contributions. The focused
result is bound to the listed source and configuration, not to an unrun full
suite at a later composed head.

## Reproduce

From the recorded source with Go 1.24.13 on Windows:

```powershell
$env:CGO_ENABLED = '0'
go test -json -count=1 -timeout 120s -run '^TestConfig_DeprecationNormalization$' .
```

Only the maintained migration group was selected. This result does not establish
a passing full repository suite, cross-platform coverage, sponsor acceptance,
or payment. The isolated validation workflow is not part of the original
submission branch. Other Windows repairs and their evidence remain under
their own source revisions.
