package chronicle

import (
	"testing"
	"time"
)

func TestDBWriteAndQuery(t *testing.T) {
	tests := []struct {
		name       string
		metric     string
		numPoints  int
		batchWrite bool
	}{
		{"single_writes_100", "cpu", 100, false},
		{"batch_write_500", "mem", 500, true},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			dir := t.TempDir()
			cfg := DefaultConfig(dir + "/test.db")
			db, err := Open(cfg.Path, cfg)
			if err != nil {
				t.Fatalf("open: %v", err)
			}
			defer db.Close()

			now := time.Now().UnixNano()
			if tc.batchWrite {
				points := make([]Point, tc.numPoints)
				for i := range points {
					points[i] = Point{
						Metric:    tc.metric,
						Tags:      map[string]string{"host": "server1"},
						Value:     float64(i),
						Timestamp: now + int64(i),
					}
				}
				if err := db.WriteBatch(points); err != nil {
					t.Fatalf("write batch: %v", err)
				}
			} else {
				for i := 0; i < tc.numPoints; i++ {
					err := db.Write(Point{
						Metric:    tc.metric,
						Tags:      map[string]string{"host": "server1"},
						Value:     float64(i),
						Timestamp: now + int64(i)*int64(time.Second),
					})
					if err != nil {
						t.Fatalf("write: %v", err)
					}
				}
			}

			if err := db.Flush(); err != nil {
				t.Fatalf("flush: %v", err)
			}

			result, err := db.Execute(&Query{Metric: tc.metric})
			if err != nil {
				t.Fatalf("query: %v", err)
			}

			if len(result.Points) != tc.numPoints {
				t.Errorf("expected %d points, got %d", tc.numPoints, len(result.Points))
			}
		})
	}
}

func TestDBWriteBatch(t *testing.T) {
	t.Run("standard_batch", func(t *testing.T) {
		dir := t.TempDir()
		cfg := DefaultConfig(dir + "/test.db")
		db, err := Open(cfg.Path, cfg)
		if err != nil {
			t.Fatalf("open: %v", err)
		}
		defer db.Close()

		now := time.Now().UnixNano()
		points := make([]Point, 500)
		for i := range points {
			points[i] = Point{
				Metric:    "mem",
				Tags:      map[string]string{"host": "server1"},
				Value:     float64(i),
				Timestamp: now + int64(i),
			}
		}

		if err := db.WriteBatch(points); err != nil {
			t.Fatalf("write batch: %v", err)
		}

		result, err := db.Execute(&Query{Metric: "mem"})
		if err != nil {
			t.Fatalf("query: %v", err)
		}

		if len(result.Points) != 500 {
			t.Errorf("expected 500 points, got %d", len(result.Points))
		}
	})
}

func TestDBQueryFilters(t *testing.T) {
	t.Run("tag_filter", func(t *testing.T) {
		dir := t.TempDir()
		cfg := DefaultConfig(dir + "/test.db")
		db, err := Open(cfg.Path, cfg)
		if err != nil {
			t.Fatalf("open: %v", err)
		}
		defer db.Close()

		now := time.Now().UnixNano()
		points := []Point{
			{Metric: "cpu", Tags: map[string]string{"host": "a"}, Value: 1.0, Timestamp: now},
			{Metric: "cpu", Tags: map[string]string{"host": "b"}, Value: 2.0, Timestamp: now},
			{Metric: "cpu", Tags: map[string]string{"host": "a"}, Value: 3.0, Timestamp: now + 1},
		}

		if err := db.WriteBatch(points); err != nil {
			t.Fatalf("write batch: %v", err)
		}

		result, err := db.Execute(&Query{
			Metric: "cpu",
			Tags:   map[string]string{"host": "a"},
		})
		if err != nil {
			t.Fatalf("query: %v", err)
		}

		if len(result.Points) != 2 {
			t.Errorf("expected 2 points for host=a, got %d", len(result.Points))
		}
	})

	t.Run("time_range", func(t *testing.T) {
		dir := t.TempDir()
		cfg := DefaultConfig(dir + "/test.db")
		db, err := Open(cfg.Path, cfg)
		if err != nil {
			t.Fatalf("open: %v", err)
		}
		defer db.Close()

		now := time.Now().UnixNano()
		points := []Point{
			{Metric: "cpu", Value: 1.0, Timestamp: now - int64(2*time.Hour)},
			{Metric: "cpu", Value: 2.0, Timestamp: now - int64(time.Hour)},
			{Metric: "cpu", Value: 3.0, Timestamp: now},
		}

		if err := db.WriteBatch(points); err != nil {
			t.Fatalf("write batch: %v", err)
		}

		result, err := db.Execute(&Query{
			Metric: "cpu",
		})
		if err != nil {
			t.Fatalf("query: %v", err)
		}

		if len(result.Points) != 3 {
			t.Errorf("expected 3 points total, got %d", len(result.Points))
		}
	})

	t.Run("aggregation", func(t *testing.T) {
		dir := t.TempDir()
		cfg := DefaultConfig(dir + "/test.db")
		db, err := Open(cfg.Path, cfg)
		if err != nil {
			t.Fatalf("open: %v", err)
		}
		defer db.Close()

		now := time.Now().UnixNano()
		points := []Point{
			{Metric: "temp", Value: 20.0, Timestamp: now},
			{Metric: "temp", Value: 22.0, Timestamp: now + 1},
			{Metric: "temp", Value: 24.0, Timestamp: now + 2},
		}

		if err := db.WriteBatch(points); err != nil {
			t.Fatalf("write batch: %v", err)
		}

		result, err := db.Execute(&Query{
			Metric: "temp",
			Aggregation: &Aggregation{
				Function: AggMean,
				Window:   time.Hour,
			},
		})
		if err != nil {
			t.Fatalf("query: %v", err)
		}

		if len(result.Points) == 0 {
			t.Error("expected aggregated result")
		}
	})

	t.Run("multiple_metrics", func(t *testing.T) {
		dir := t.TempDir()
		cfg := DefaultConfig(dir + "/test.db")
		db, err := Open(cfg.Path, cfg)
		if err != nil {
			t.Fatalf("open: %v", err)
		}
		defer db.Close()

		now := time.Now().UnixNano()
		points := []Point{
			{Metric: "cpu", Value: 1.0, Timestamp: now},
			{Metric: "mem", Value: 2.0, Timestamp: now},
			{Metric: "disk", Value: 3.0, Timestamp: now},
		}

		if err := db.WriteBatch(points); err != nil {
			t.Fatalf("write batch: %v", err)
		}

		for _, metric := range []string{"cpu", "mem", "disk"} {
			result, err := db.Execute(&Query{Metric: metric})
			if err != nil {
				t.Fatalf("query %s: %v", metric, err)
			}
			if len(result.Points) != 1 {
				t.Errorf("expected 1 point for %s, got %d", metric, len(result.Points))
			}
		}
	})
}

func TestDBEdgeCases(t *testing.T) {
	t.Run("reopen", func(t *testing.T) {
		dir := t.TempDir()
		path := dir + "/test.db"
		cfg := DefaultConfig(path)

		db, err := Open(path, cfg)
		if err != nil {
			t.Fatalf("open: %v", err)
		}

		now := time.Now().UnixNano()
		if err := db.Write(Point{Metric: "cpu", Value: 42.0, Timestamp: now}); err != nil {
			t.Fatalf("write: %v", err)
		}
		if err := db.Flush(); err != nil {
			t.Fatalf("flush: %v", err)
		}
		if err := db.Close(); err != nil {
			t.Fatalf("close: %v", err)
		}

		db2, err := Open(path, cfg)
		if err != nil {
			t.Fatalf("reopen: %v", err)
		}
		defer db2.Close()

		result, err := db2.Execute(&Query{Metric: "cpu"})
		if err != nil {
			t.Fatalf("query: %v", err)
		}

		if len(result.Points) != 1 {
			t.Errorf("expected 1 point after reopen, got %d", len(result.Points))
		}
		if result.Points[0].Value != 42.0 {
			t.Errorf("expected value 42.0, got %f", result.Points[0].Value)
		}
	})

	t.Run("empty_write_batch", func(t *testing.T) {
		dir := t.TempDir()
		cfg := DefaultConfig(dir + "/test.db")
		db, err := Open(cfg.Path, cfg)
		if err != nil {
			t.Fatalf("open: %v", err)
		}
		defer db.Close()

		if err := db.WriteBatch(nil); err != nil {
			t.Errorf("unexpected error: %v", err)
		}
		if err := db.WriteBatch([]Point{}); err != nil {
			t.Errorf("unexpected error: %v", err)
		}
	})

	t.Run("default_timestamp", func(t *testing.T) {
		dir := t.TempDir()
		cfg := DefaultConfig(dir + "/test.db")
		db, err := Open(cfg.Path, cfg)
		if err != nil {
			t.Fatalf("open: %v", err)
		}
		defer db.Close()

		before := time.Now().UnixNano()
		if err := db.Write(Point{Metric: "cpu", Value: 1.0}); err != nil {
			t.Fatalf("write: %v", err)
		}
		after := time.Now().UnixNano()

		if err := db.Flush(); err != nil {
			t.Fatalf("flush: %v", err)
		}

		result, err := db.Execute(&Query{Metric: "cpu"})
		if err != nil {
			t.Fatalf("query: %v", err)
		}

		if len(result.Points) != 1 {
			t.Fatalf("expected 1 point, got %d", len(result.Points))
		}

		ts := result.Points[0].Timestamp
		if ts < before || ts > after {
			t.Error("timestamp should be auto-generated within test window")
		}
	})
}

func TestDefaultConfig(t *testing.T) {
	cfg := DefaultConfig("/tmp/test.db")
	cfg.normalize()
	cfg.syncLegacyFields()

	if cfg.Path != "/tmp/test.db" {
		t.Errorf("expected path /tmp/test.db, got %s", cfg.Path)
	}
	if cfg.MaxMemory != 64*1024*1024 {
		t.Errorf("expected 64MB max memory, got %d", cfg.MaxMemory)
	}
	if cfg.Storage.MaxMemory != 64*1024*1024 {
		t.Errorf("expected 64MB storage max memory, got %d", cfg.Storage.MaxMemory)
	}
	if cfg.PartitionDuration != time.Hour {
		t.Errorf("expected 1h partition duration, got %v", cfg.PartitionDuration)
	}
	if cfg.Storage.PartitionDuration != time.Hour {
		t.Errorf("expected 1h storage partition duration, got %v", cfg.Storage.PartitionDuration)
	}
	if cfg.BufferSize != 10_000 {
		t.Errorf("expected buffer size 10000, got %d", cfg.BufferSize)
	}
	if cfg.Storage.BufferSize != 10_000 {
		t.Errorf("expected storage buffer size 10000, got %d", cfg.Storage.BufferSize)
	}
	if cfg.HTTPEnabled {
		t.Error("expected HTTP disabled by default")
	}
	if cfg.HTTP.HTTPEnabled {
		t.Error("expected HTTP HTTPEnabled disabled by default")
	}
}
