//go:build windows

package chronicle

import (
	"path/filepath"
	"testing"
)

func TestWALResetWindowsPreservesAppend(t *testing.T) {
	w, err := NewWAL(filepath.Join(t.TempDir(), "reset.wal"), 0, 0, 0)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if err := w.Close(); err != nil {
			t.Error(err)
		}
	})
	if err := w.Write([]Point{{Metric: "discarded", Timestamp: 1, Value: 1}}); err != nil {
		t.Fatal(err)
	}
	if w.Position() == 0 {
		t.Fatal("expected a nonempty WAL before reset")
	}
	if err := w.Reset(); err != nil {
		t.Fatal(err)
	}
	if w.Position() != 0 {
		t.Fatal("WAL was not truncated")
	}
	for _, value := range []float64{2, 3} {
		if err := w.Write([]Point{{Metric: "retained", Timestamp: int64(value), Value: value}}); err != nil {
			t.Fatal(err)
		}
		w.Position() // Flush buffered writes before replaying them.
		points, err := w.ReadAll()
		if err != nil {
			t.Fatal(err)
		}
		if len(points) != int(value)-1 {
			t.Fatalf("expected %d retained records, got %d", int(value)-1, len(points))
		}
		for i, point := range points {
			if point.Metric != "retained" || point.Value != float64(i+2) || point.Timestamp != int64(i+2) {
				t.Fatalf("unexpected post-reset record: %+v", point)
			}
		}
	}
}
