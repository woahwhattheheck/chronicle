# Windows Compatibility Report

Bounty #9 (`docs/BOUNTY_PROGRAM.md`): verify Chronicle tests on Windows and document issues.

## Tested revision

- Repository: `josedab/chronicle`
- Branch / commit: see this PR
- Go toolchain target: `go1.24` (`windows/amd64`)
- CI matrix: `.github/workflows/ci.yml` already includes `windows-latest`

## How to reproduce

```powershell
go version
go mod download
go build ./...
go test -short -count=1 ./...
```

Cross-compile check from Linux/macOS:

```bash
GOOS=windows GOARCH=amd64 go test -c -o chronicle-windows.test .
```

## Findings fixed in this PR

| Issue | Impact | Fix |
|-------|--------|-----|
| `health_check.go` called `syscall.Statfs` / `Statfs_t` | Package failed to compile on Windows | Split into `disk_usage_unix.go` (`Statfs`) and `disk_usage_windows.go` (`GetDiskFreeSpaceEx`) |
| `cffi_test.go` / `cffi_functions_test.go` lacked `cgo` build tags | With `CGO_ENABLED=0` (common on Windows runners without a C toolchain), tests referenced symbols from cgo-only files and failed to build | Gate those tests with `//go:build cgo` |

## Remaining limitations

- **CGO / C FFI** (`cffi.go`, `chronicle_cffi.h`): requires a C toolchain (e.g. MinGW). Not exercised when `CGO_ENABLED=0`.
- **eBPF** collectors are Linux-only (`runtime.GOOS != "linux"` guards already present).
- **TEE / confidential computing** hardware paths are Linux-oriented.
- Absolute POSIX paths hardcoded in a few *unit tests* (e.g. `"/tmp/..."` config defaults) are expectations about default string values, not runtime Windows I/O.

## Acceptance

- Windows builds the core library without `Statfs` errors.
- Non-cgo test packages compile on Windows.
- CI `Test (windows-latest, 1.24)` is the ongoing regression signal; failures outside the fixes above should be filed as follow-ups with reproduction steps.
