//go:build windows

package chronicle

import (
	"os"
	"path/filepath"
	"runtime"
	"testing"
	"time"
)

// These additive acceptance probes exercise the actual submitted package.
// No production or existing test file is replaced in the validation checkout.
func TestNativeWindowsDiskUsageAcceptance(t *testing.T) {
	if runtime.GOOS != "windows" {
		t.Fatal("native Windows execution is required")
	}
	t.Run("nonexistent_database_in_unicode_parent", func(t *testing.T) {
		parent := filepath.Join(t.TempDir(), "chronicle-資料")
		if err := os.Mkdir(parent, 0700); err != nil {
			t.Fatal(err)
		}
		free, total, err := diskUsage(filepath.Join(parent, "not-created.db"))
		if err != nil || total == 0 || free > total {
			t.Fatalf("diskUsage: free=%d total=%d err=%v", free, total, err)
		}
		t.Logf("native GetDiskFreeSpaceEx: free=%d total=%d", free, total)
	})
	t.Run("relative_parent", func(t *testing.T) {
		parent, err := os.MkdirTemp(".", ".native-windows-disk-")
		if err != nil {
			t.Fatal(err)
		}
		t.Cleanup(func() { _ = os.RemoveAll(parent) })
		free, total, err := diskUsage(filepath.Join(parent, "not-created.db"))
		if err != nil || total == 0 || free > total {
			t.Fatalf("relative diskUsage: free=%d total=%d err=%v", free, total, err)
		}
	})
	t.Run("missing_parent_returns_error", func(t *testing.T) {
		_, _, err := diskUsage(filepath.Join(t.TempDir(), "not-created", "db"))
		if err == nil {
			t.Fatal("missing parent unexpectedly succeeded")
		}
	})
	t.Run("embedded_nul_returns_error", func(t *testing.T) {
		_, _, err := diskUsage(filepath.Join(t.TempDir(), "bad\x00parent", "db"))
		if err == nil {
			t.Fatal("embedded NUL unexpectedly succeeded")
		}
	})
}

func TestNativeWindowsPersistenceAcceptance(t *testing.T) {
	if runtime.GOOS != "windows" {
		t.Fatal("native Windows execution is required")
	}
	parent := filepath.Join(t.TempDir(), "chronicle-資料")
	if err := os.Mkdir(parent, 0700); err != nil {
		t.Fatal(err)
	}
	path := filepath.Join(parent, "reopen.db")
	cfg := DefaultConfig(path)
	db, err := Open(path, cfg)
	if err != nil {
		t.Fatal(err)
	}
	defer func() {
		if db != nil {
			_ = db.Close()
		}
	}()
	now := time.Now().UnixNano()
	points := []Point{
		{Metric: "windows_reopen_probe", Tags: map[string]string{"source": "native"}, Timestamp: now, Value: 1.25},
		{Metric: "windows_reopen_probe", Tags: map[string]string{"source": "native"}, Timestamp: now + 1, Value: 2.5},
	}
	if err := db.WriteBatch(points); err != nil {
		t.Fatal(err)
	}
	if err := db.Flush(); err != nil {
		t.Fatal(err)
	}
	if err := db.Close(); err != nil {
		t.Fatal(err)
	}
	db = nil
	reopened, err := Open(path, cfg)
	if err != nil {
		t.Fatal(err)
	}
	defer reopened.Close()
	result, err := reopened.Execute(&Query{Metric: "windows_reopen_probe"})
	if err != nil {
		t.Fatal(err)
	}
	if len(result.Points) != len(points) {
		t.Fatalf("got %d persisted points, want %d", len(result.Points), len(points))
	}
	want := map[int64]float64{now: 1.25, now + 1: 2.5}
	for _, point := range result.Points {
		value, ok := want[point.Timestamp]
		if !ok || point.Value != value || point.Tags["source"] != "native" {
			t.Fatalf("unexpected persisted point: %+v", point)
		}
		delete(want, point.Timestamp)
	}
	if len(want) != 0 {
		t.Fatal("a persisted timestamp is missing")
	}
	t.Log("two exact timestamp/value/tag records survived native Windows close and reopen")
}
