package chronicle

import (
	"encoding/json"
	"math"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
)

func TestNewHWAcceleratedQueryEngine(t *testing.T) {
	config := DefaultHWAcceleratedQueryConfig()
	engine := NewHWAcceleratedQueryEngine(nil, config)
	if engine == nil {
		t.Fatal("expected non-nil engine")
	}
	if engine.config.BatchSize != 1024 {
		t.Errorf("expected batch size 1024, got %d", engine.config.BatchSize)
	}
	if engine.profile.Architecture == "" {
		t.Error("expected architecture to be detected")
	}
}

func TestHWDetectCapabilities(t *testing.T) {
	engine := NewHWAcceleratedQueryEngine(nil, DefaultHWAcceleratedQueryConfig())
	profile := engine.DetectCapabilities()

	if profile.Architecture == "" {
		t.Error("expected non-empty architecture")
	}
	if len(profile.Capabilities) == 0 {
		t.Error("expected at least one capability")
	}
	if profile.NumCores <= 0 {
		t.Errorf("expected positive cores, got %d", profile.NumCores)
	}
	if profile.DetectedAt.IsZero() {
		t.Error("expected non-zero detection time")
	}
	if profile.CacheLineBytes <= 0 {
		t.Errorf("expected positive cache line bytes, got %d", profile.CacheLineBytes)
	}
}

func TestHWAccelSumFloat64(t *testing.T) {
	engine := NewHWAcceleratedQueryEngine(nil, DefaultHWAcceleratedQueryConfig())

	tests := []struct {
		name string
		data []float64
		want float64
	}{
		{"basic", []float64{1, 2, 3, 4, 5}, 15},
		{"empty", nil, 0},
		{"single", []float64{42}, 42},
		{"large", make10k(), 49995000},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := engine.SumFloat64(tt.data)
			if math.Abs(got-tt.want) > 1e-6 {
				t.Errorf("SumFloat64 = %f, want %f", got, tt.want)
			}
		})
	}
}

func TestHWAccelMinFloat64(t *testing.T) {
	engine := NewHWAcceleratedQueryEngine(nil, DefaultHWAcceleratedQueryConfig())

	got := engine.MinFloat64([]float64{5, 3, 7, 1, 9})
	if got != 1 {
		t.Errorf("MinFloat64 = %f, want 1", got)
	}

	got = engine.MinFloat64(nil)
	if !math.IsNaN(got) {
		t.Error("expected NaN for empty slice")
	}

	got = engine.MinFloat64([]float64{-3, -1, -7})
	if got != -7 {
		t.Errorf("MinFloat64 = %f, want -7", got)
	}
}

func TestHWAccelMaxFloat64(t *testing.T) {
	engine := NewHWAcceleratedQueryEngine(nil, DefaultHWAcceleratedQueryConfig())

	got := engine.MaxFloat64([]float64{5, 3, 7, 1, 9})
	if got != 9 {
		t.Errorf("MaxFloat64 = %f, want 9", got)
	}

	got = engine.MaxFloat64(nil)
	if !math.IsNaN(got) {
		t.Error("expected NaN for empty slice")
	}
}

func TestHWAccelMeanFloat64(t *testing.T) {
	engine := NewHWAcceleratedQueryEngine(nil, DefaultHWAcceleratedQueryConfig())

	got := engine.MeanFloat64([]float64{10, 20, 30})
	if got != 20 {
		t.Errorf("MeanFloat64 = %f, want 20", got)
	}

	got = engine.MeanFloat64(nil)
	if !math.IsNaN(got) {
		t.Error("expected NaN for empty slice")
	}
}

func TestHWAccelCountWhere(t *testing.T) {
	engine := NewHWAcceleratedQueryEngine(nil, DefaultHWAcceleratedQueryConfig())

	data := []float64{1, 2, 3, 4, 5, 6, 7, 8, 9, 10}
	got := engine.CountWhere(data, func(v float64) bool { return v > 5 })
	if got != 5 {
		t.Errorf("CountWhere(>5) = %d, want 5", got)
	}

	got = engine.CountWhere(nil, func(v float64) bool { return true })
	if got != 0 {
		t.Errorf("CountWhere(nil) = %d, want 0", got)
	}
}

func TestHWAccelVarianceFloat64(t *testing.T) {
	engine := NewHWAcceleratedQueryEngine(nil, DefaultHWAcceleratedQueryConfig())

	data := []float64{2, 4, 4, 4, 5, 5, 7, 9}
	got := engine.VarianceFloat64(data)
	want := 4.0
	if math.Abs(got-want) > 1e-6 {
		t.Errorf("VarianceFloat64 = %f, want %f", got, want)
	}

	got = engine.VarianceFloat64(nil)
	if !math.IsNaN(got) {
		t.Error("expected NaN for empty slice")
	}
}

func TestHWAccelBatchAggregate(t *testing.T) {
	engine := NewHWAcceleratedQueryEngine(nil, DefaultHWAcceleratedQueryConfig())

	batches := [][]float64{
		{1, 2, 3},
		{4, 5, 6},
		{7, 8, 9},
	}
	results := engine.BatchAggregate(OpSum, batches)
	if len(results) != 3 {
		t.Fatalf("expected 3 results, got %d", len(results))
	}
	expected := []float64{6, 15, 24}
	for i, want := range expected {
		if math.Abs(results[i]-want) > 1e-6 {
			t.Errorf("batch[%d] = %f, want %f", i, results[i], want)
		}
	}

	// Test with OpMax
	results = engine.BatchAggregate(OpMax, batches)
	expectedMax := []float64{3, 6, 9}
	for i, want := range expectedMax {
		if results[i] != want {
			t.Errorf("batch max[%d] = %f, want %f", i, results[i], want)
		}
	}
}

func TestHWAccelParallelScan(t *testing.T) {
	engine := NewHWAcceleratedQueryEngine(nil, DefaultHWAcceleratedQueryConfig())

	data := make([]float64, 10000)
	for i := range data {
		data[i] = float64(i)
	}

	result := engine.ParallelScan(data, OpSum)
	want := 49995000.0
	if math.Abs(result.Value-want) > 1e-6 {
		t.Errorf("ParallelScan Sum = %f, want %f", result.Value, want)
	}
	if result.Count != 10000 {
		t.Errorf("count = %d, want 10000", result.Count)
	}
	if result.Duration <= 0 {
		t.Error("expected positive duration")
	}

	result = engine.ParallelScan(data, OpMin)
	if result.Value != 0 {
		t.Errorf("ParallelScan Min = %f, want 0", result.Value)
	}

	result = engine.ParallelScan(data, OpMax)
	if result.Value != 9999 {
		t.Errorf("ParallelScan Max = %f, want 9999", result.Value)
	}
}

func TestHWAccelPlanAcceleration(t *testing.T) {
	engine := NewHWAcceleratedQueryEngine(nil, DefaultHWAcceleratedQueryConfig())

	plan := engine.PlanAcceleration(OpSum, 10000)
	if plan.Operation != OpSum {
		t.Errorf("expected OpSum, got %v", plan.Operation)
	}
	if plan.InputSize != 10000 {
		t.Errorf("expected input size 10000, got %d", plan.InputSize)
	}
	if plan.BatchCount < 1 {
		t.Errorf("expected batch count >= 1, got %d", plan.BatchCount)
	}
	if plan.EstimatedSpeedup < 1.0 {
		t.Errorf("expected speedup >= 1.0, got %f", plan.EstimatedSpeedup)
	}

	// Test with software fallback
	cfg := DefaultHWAcceleratedQueryConfig()
	cfg.ForceSoftwareFallback = true
	fallback := NewHWAcceleratedQueryEngine(nil, cfg)
	plan = fallback.PlanAcceleration(OpSum, 100)
	if plan.UseVectorized {
		t.Error("expected vectorized disabled with software fallback")
	}
}

func TestHWAccelBenchmark(t *testing.T) {
	engine := NewHWAcceleratedQueryEngine(nil, DefaultHWAcceleratedQueryConfig())

	results := engine.Benchmark(1000)
	if len(results) == 0 {
		t.Fatal("expected benchmark results")
	}

	ops := []AccelOperation{OpSum, OpMin, OpMax, OpMean, OpCount, OpVariance}
	for _, op := range ops {
		r, ok := results[op]
		if !ok {
			t.Errorf("missing result for %s", op)
			continue
		}
		if r.Duration < 0 {
			t.Errorf("%s: expected non-negative duration", op)
		}
		if r.MethodUsed != "benchmark" {
			t.Errorf("%s: expected method 'benchmark', got %q", op, r.MethodUsed)
		}
	}
}

func TestHWAccelStats(t *testing.T) {
	engine := NewHWAcceleratedQueryEngine(nil, DefaultHWAcceleratedQueryConfig())

	// Run some operations
	engine.SumFloat64([]float64{1, 2, 3})
	engine.MinFloat64([]float64{1, 2, 3})
	engine.MaxFloat64([]float64{1, 2, 3})

	stats := engine.Stats()
	if stats.QueriesAccelerated != 3 {
		t.Errorf("expected 3 queries accelerated, got %d", stats.QueriesAccelerated)
	}
	if stats.TotalDataProcessed != 9 {
		t.Errorf("expected 9 data points processed, got %d", stats.TotalDataProcessed)
	}
	if stats.HardwareProfile.Architecture == "" {
		t.Error("expected architecture in stats")
	}
	if len(stats.OperationsByType) != 3 {
		t.Errorf("expected 3 operation types, got %d", len(stats.OperationsByType))
	}
}

func TestHWAccelConcurrentStats(t *testing.T) {
	engine := NewHWAcceleratedQueryEngine(nil, DefaultHWAcceleratedQueryConfig())
	const workers, iterations = 8, 100
	data := []float64{1, 2, 3, 4}
	predicates := []ScanPredicate{{Type: PredicateValueRange, ValueMin: 2, ValueMax: 3}}
	start := make(chan struct{})
	var wg sync.WaitGroup
	for range workers {
		wg.Add(1)
		go func() {
			defer wg.Done()
			<-start
			var previous int64
			for range iterations {
				if got := engine.SumFloat64(data); got != 10 {
					t.Errorf("SumFloat64 = %v, want 10", got)
				}
				result := engine.VectorizedScan(data, nil, predicates)
				if result.RowCount != 2 || result.BytesRead != 32 {
					t.Errorf("VectorizedScan = %+v, want 2 rows and 32 bytes read", result)
				}
				stats := engine.Stats()
				if stats.QueriesAccelerated < previous {
					t.Errorf("query count decreased from %d to %d", previous, stats.QueriesAccelerated)
				}
				previous = stats.QueriesAccelerated
			}
		}()
	}
	close(start)
	wg.Wait()

	stats := engine.Stats()
	const calls = workers * iterations
	if stats.QueriesAccelerated != 2*calls || stats.TotalDataProcessed != 8*calls {
		t.Errorf("unexpected concurrent totals: %+v", stats)
	}
	if stats.OperationsByType["Sum"] != calls {
		t.Errorf("Sum count = %d, want %d", stats.OperationsByType["Sum"], calls)
	}
	if stats.AvgSpeedup != 1 {
		t.Errorf("AvgSpeedup = %v, want 1 without a benchmark baseline", stats.AvgSpeedup)
	}
}

func TestHWAccelHTTPHandlers(t *testing.T) {
	engine := NewHWAcceleratedQueryEngine(nil, DefaultHWAcceleratedQueryConfig())
	mux := http.NewServeMux()
	engine.RegisterHTTPHandlers(mux)

	// Test capabilities endpoint
	t.Run("capabilities", func(t *testing.T) {
		req := httptest.NewRequest(http.MethodGet, "/api/v1/hwaccel/capabilities", nil)
		w := httptest.NewRecorder()
		mux.ServeHTTP(w, req)

		if w.Code != http.StatusOK {
			t.Errorf("expected 200, got %d", w.Code)
		}
		var profile HWProfile
		if err := json.NewDecoder(w.Body).Decode(&profile); err != nil {
			t.Fatalf("decode: %v", err)
		}
		if profile.Architecture == "" {
			t.Error("expected non-empty architecture")
		}
	})

	// Test benchmark endpoint
	t.Run("benchmark", func(t *testing.T) {
		body := `{"data_size":100}`
		req := httptest.NewRequest(http.MethodPost, "/api/v1/hwaccel/benchmark", strings.NewReader(body))
		w := httptest.NewRecorder()
		mux.ServeHTTP(w, req)

		if w.Code != http.StatusOK {
			t.Errorf("expected 200, got %d", w.Code)
		}
	})

	// Test stats endpoint
	t.Run("stats", func(t *testing.T) {
		req := httptest.NewRequest(http.MethodGet, "/api/v1/hwaccel/stats", nil)
		w := httptest.NewRecorder()
		mux.ServeHTTP(w, req)

		if w.Code != http.StatusOK {
			t.Errorf("expected 200, got %d", w.Code)
		}
		var stats HWAcceleratedQueryStats
		if err := json.NewDecoder(w.Body).Decode(&stats); err != nil {
			t.Fatalf("decode: %v", err)
		}
	})

	// Test method not allowed
	t.Run("method_not_allowed", func(t *testing.T) {
		req := httptest.NewRequest(http.MethodPost, "/api/v1/hwaccel/capabilities", nil)
		w := httptest.NewRecorder()
		mux.ServeHTTP(w, req)

		if w.Code != http.StatusMethodNotAllowed {
			t.Errorf("expected 405, got %d", w.Code)
		}
	})
}

// make10k returns a slice [0..9999] for testing.
func make10k() []float64 {
	data := make([]float64, 10000)
	for i := range data {
		data[i] = float64(i)
	}
	return data
}
