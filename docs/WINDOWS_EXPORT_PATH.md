# Windows export path guard

## Result

On October 4, 2026, the four selected export tests passed on native Windows:
`TestValidateExportPath`, `TestExporter_SensitivePathRejection`,
`TestExporter_CSV`, and `TestValidateExportPathWindows`.
The run includes 40 passing subtests, with zero failures and zero skips.

This report supplements [the Windows compatibility report](WINDOWS.md) for
[josedab/chronicle#104](https://github.com/josedab/chronicle/pull/104).

## Repair

The existing guard resolves an absolute output path and rejects a fixed list of
sensitive root directories. On Windows, the resolved drive prefix and backslash
separators prevented those checks from matching.

The repair derives a comparison path using Windows separator, case, and volume
rules, then applies the existing directory-boundary checks. Extended UNC paths
are converted to ordinary UNC form for this comparison so that the complete
server/share volume is removed. The actual returned absolute destination keeps
its original spelling. Non-Windows comparisons and the protected-directory list
are unchanged.

The Windows regression table covers drive roots, other drives, case and separator
variants, root-relative paths, dot segments, ordinary and extended UNC paths,
extended and device drive paths, sibling prefixes, nested directories, relative
paths, and a UNC share whose name is itself `etc`. Allowed paths must equal the
original cleaned absolute destination.

The old traversal test assumed `../../../etc/passwd` was allowed, although that
depends on checkout depth. It now explicitly tests both traversal within a
temporary directory and traversal to a protected root.

## Native execution

| Item | Recorded value |
|---|---|
| Environment | Windows Server 2025, build 10.0.26100, amd64 |
| Go | `go1.24.13 windows/amd64` |
| Configuration | `CGO_ENABLED=0`, `GOMAXPROCS=2` |
| Validation commit | `36f0377c33a6b014220dc7065da33fcdbd4ece33` |
| Contribution tree used by validation | `2b9080f674a5d7c837291b750e825951ee8b8478` |
| Candidate run | [37193112435](https://github.com/woahwhattheheck/chronicle/actions/runs/37193112435) |
| Candidate job | [111409283620](https://github.com/woahwhattheheck/chronicle/actions/runs/37193112435/job/111409283620) |
| Exit code | `0` |
| Formatting | `gofmt -l` returned no changed files |

```sh
go test -short -count=1 -p=2 -timeout=120s -json \
  -run '^(TestValidateExportPath|TestExporter_SensitivePathRejection|TestExporter_CSV|TestValidateExportPathWindows)$' .
```

| Selected test | Result |
|---|---|
| `TestValidateExportPath` | Passed, 16 subtests |
| `TestExporter_SensitivePathRejection` | Passed, 4 subtests exercising the exporter |
| `TestExporter_CSV` | Passed, including actual CSV file creation |
| `TestValidateExportPathWindows` | Passed, 20 subtests |

The exact source blobs executed were:

| File | Git blob |
|---|---|
| `export.go` | `f53054e9acd77ae7032496f52730edbd71cfe5b9` |
| `export_test.go` | `16d5b70922c0e5ffb1a8cd1d29457366da7cac56` |
| `export_path_windows_test.go` | `d2930311cf061cfc924dd629822a1f584aa154ad` |

## Retained evidence

[Candidate artifact](https://github.com/woahwhattheheck/chronicle/actions/runs/37193112435/artifacts/11299308699)
contains JSON test events, stderr, provenance, and the result summary.
Artifact ID: `11299308699`; SHA-256:
`ed3737dd289e84b4ca73719360d185487065d17c085d0f376b096a100ea86e57`.
The configured artifact retention ends on October 18, 2026.

The failing baseline was retained in
[run 37192442319](https://github.com/woahwhattheheck/chronicle/actions/runs/37192442319).
It used export source from `cf57d7c658acfb91f23b4e1ccc66794187607ce0`
(blob `dc9171b5667f245ecc636e5fc3ed849a1909a372`) with the same test files.
The existing guard and exporter rejection checks failed there while the CSV
control passed. That run also exposed the extended UNC case in the first repair.
The final run executes only the corrected candidate, reusing the retained
baseline evidence.

Baseline artifact ID: `11298968302`; SHA-256:
`1155bb2288bce85cab7263378638cdbde5e38d339b9fc50b0109ce16cb7a499e`.

## Scope

These results cover the selected export paths and actual exporter execution for
the identified source blobs. The guard remains a lexical check against its
existing directory list; filesystem aliases and symlink resolution are outside
this repair. Broader Windows acceptance is tracked in the main compatibility
report. This focused execution does not establish a full-suite pass or a
performance speedup.
