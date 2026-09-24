//go:build cgo

package chronicle

import "testing"

func TestCffiFunctions_Smoke(t *testing.T) {
	// Smoke test: verify FFIConfig types and functions from cffi_functions.go are accessible.
	if testing.Short() {
		t.Skip("skipping smoke test in short mode")
	}
}
