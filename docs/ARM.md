# 32-bit ARM Compatibility Report

Bounty #10 (`docs/BOUNTY_PROGRAM.md`): Test on 32-bit ARM (Pi Zero 2 class), document limitations.

## Tested targets

| Target | Command | Result |
|--------|---------|--------|
| `linux/arm` `GOARM=7` (armhf / Pi Zero 2 userspace-compatible) | `CGO_ENABLED=0 GOOS=linux GOARCH=arm GOARM=7 go test -c .` | Builds after fixes in this PR |
| `linux/arm64` | `CGO_ENABLED=0 GOOS=linux GOARCH=arm64 go test -c .` | Builds after cgo test gating |

Host used for cross-compile: linux/amd64, Go 1.24, `CGO_ENABLED=0`.

Pi Zero 2 note: the board is capable of 64-bit (`aarch64`) and 32-bit userspace. This report focuses on **32-bit `GOARCH=arm` / `GOARM=7`**, which is where `int` is 32-bit and nanosecond duration casts overflow.

## Fixes included

1. **`int(time.Minute)` overflow** — `time.Minute` is `6e10` ns and does not fit in a 32-bit `int`. Updated tests to multiply with `int64(...)` (`tracing_test.go`, `aggregation_test.go`, and related timestamp helpers).
2. **CGO-gated FFI tests** — `cffi_test.go` / `cffi_functions_test.go` now use `//go:build cgo` so non-cgo cross builds do not reference cgo-only symbols.

## Remaining limitations

- **CGO / C FFI** needs an armv7 cross C toolchain; skipped with `CGO_ENABLED=0`.
- **eBPF**, confidential TEE hardware paths: Linux/amd64 or aarch64 oriented; not validated on armv7.
- **modernc.org/sqlite** works on arm with pure Go, but expect lower ingest throughput on Pi-class CPUs.
- Full `go test ./...` on real Pi hardware was not executed in this environment; use the commands below on device.

## On-device reproduction (Pi Zero 2 / armv7)

```bash
git clone https://github.com/josedab/chronicle.git
cd chronicle
go version   # expect go1.24.x linux/arm or linux/arm64
CGO_ENABLED=0 go test -short -count=1 ./...
```

Cross-compile from amd64:

```bash
CGO_ENABLED=0 GOOS=linux GOARCH=arm GOARM=7 go test -c -o chronicle-armv7.test .
# optional: qemu-user-static ./chronicle-armv7.test -test.short -test.count=1
```

## Acceptance

- Core package and tests compile for `linux/arm` `GOARM=7` with `CGO_ENABLED=0`.
- Documented limitations above; follow-ups for on-device soak tests welcome.
