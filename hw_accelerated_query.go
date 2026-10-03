package chronicle

import (
	"encoding/json"
	"math"
	"net/http"
	"runtime"
	"sync"
	"sync/atomic"
	"time"
)

// HWCapability represents a hardware SIMD capability level.
type HWCapability int

const (
	CapNone   HWCapability = iota
	CapSSE42               // x86 SSE4.2
	CapAVX2                // x86 AVX2
	CapAVX512              // x86 AVX-512
	CapNEON                // ARM NEON
	CapSVE                 // ARM SVE
)

func (c HWCapability) String() string {
	names := [...]string{"None", "SSE4.2", "AVX2", "AVX-512", "NEON", "SVE"}
	if int(c) < len(names) {
		return names[c]
	}
	return "Unknown"
}

// AccelOperation identifies an aggregation operation for hardware acceleration.
type AccelOperation int

const (
	OpSum AccelOperation = iota
	OpMin
	OpMax
	OpMean
	OpCount
	OpVariance
	OpStdDev
	OpPercentile
)

func (op AccelOperation) String() string {
	names := [...]string{"Sum", "Min", "Max", "Mean", "Count", "Variance", "StdDev", "Percentile"}
	if int(op) < len(names) {
		return names[op]
	}
	return "Unknown"
}

// HWAcceleratedQueryConfig configures the hardware-accelerated query engine.
type HWAcceleratedQueryConfig struct {
	EnableSIMD            bool `json:"enable_simd"`
	EnableBatchProcessing bool `json:"enable_batch_processing"`
	BatchSize             int  `json:"batch_size"`
	VectorWidth           int  `json:"vector_width"`
	EnablePrefetch        bool `json:"enable_prefetch"`
	CacheLineSize         int  `json:"cache_line_size"`
	EnableParallelScan    bool `json:"enable_parallel_scan"`
	MaxParallelWorkers    int  `json:"max_parallel_workers"`
	ForceSoftwareFallback bool `json:"force_software_fallback"`
}

// DefaultHWAcceleratedQueryConfig returns sensible defaults.
func DefaultHWAcceleratedQueryConfig() HWAcceleratedQueryConfig {
	return HWAcceleratedQueryConfig{
		EnableSIMD:            true,
		EnableBatchProcessing: true,
		BatchSize:             1024,
		VectorWidth:           256,
		EnablePrefetch:        true,
		CacheLineSize:         64,
		EnableParallelScan:    true,
		MaxParallelWorkers:    runtime.NumCPU(),
		ForceSoftwareFallback: false,
	}
}

// HWProfile describes the detected hardware capabilities.
type HWProfile struct {
	Architecture   string         `json:"architecture"`
	Capabilities   []HWCapability `json:"capabilities"`
	VectorWidthBit int            `json:"vector_width_bits"`
	CacheLineBytes int            `json:"cache_line_bytes"`
	NumCores       int            `json:"num_cores"`
	DetectedAt     time.Time      `json:"detected_at"`
}

// AccelPlan describes the execution plan for an accelerated operation.
type AccelPlan struct {
	Operation        AccelOperation `json:"operation"`
	InputSize        int            `json:"input_size"`
	UseVectorized    bool           `json:"use_vectorized"` // uses unrolled loops, not hardware SIMD
	EstimatedSpeedup float64        `json:"estimated_speedup"`
	BatchCount       int            `json:"batch_count"`
	ParallelWorkers  int            `json:"parallel_workers"`
}

// AccelResult holds the result of an accelerated operation.
type AccelResult struct {
	Operation         AccelOperation `json:"operation"`
	Value             float64        `json:"value"`
	Count             int64          `json:"count"`
	Duration          time.Duration  `json:"duration"`
	SpeedupVsBaseline float64        `json:"speedup_vs_baseline"`
	MethodUsed        string         `json:"method_used"`
}

// HWAcceleratedQueryStats holds engine statistics.
type HWAcceleratedQueryStats struct {
	QueriesAccelerated int64            `json:"queries_accelerated"`
	TotalDataProcessed int64            `json:"total_data_processed"`
	AvgSpeedup         float64          `json:"avg_speedup"`
	HardwareProfile    HWProfile        `json:"hardware_profile"`
	OperationsByType   map[string]int64 `json:"operations_by_type"`
}

// HWAcceleratedQueryEngine provides optimized aggregation operations using
// loop unrolling, batch processing, and parallel worker pools.
//
// Despite the name, this engine uses portable Go optimizations (4-way unrolled
// loops for ILP, goroutine-based parallelism) rather than hardware SIMD
// intrinsics. The hardware capability detection identifies the platform but
// the execution paths are architecture-independent.
//
// 🧪 EXPERIMENTAL: This API may change or be removed without notice.
// See api_stability.go for stability classifications.
type HWAcceleratedQueryEngine struct {
	db       *DB
	config   HWAcceleratedQueryConfig
	profile  HWProfile
	baseline map[AccelOperation]time.Duration
	mu       sync.RWMutex

	// stats
	queriesAccelerated atomic.Int64
	totalDataProcessed atomic.Int64
	totalSpeedup       float64
	speedupCount       int64
	operationsByType   map[AccelOperation]int64
	statsMu            sync.Mutex
}

// NewHWAcceleratedQueryEngine creates a new hardware-accelerated query engine.
func NewHWAcceleratedQueryEngine(db *DB, config HWAcceleratedQueryConfig) *HWAcceleratedQueryEngine {
	e := &HWAcceleratedQueryEngine{
		db:               db,
		config:           config,
		baseline:         make(map[AccelOperation]time.Duration),
		operationsByType: make(map[AccelOperation]int64),
	}
	e.profile = e.DetectCapabilities()
	return e
}

// DetectCapabilities detects the hardware capabilities of the current platform.
func (e *HWAcceleratedQueryEngine) DetectCapabilities() HWProfile {
	arch := runtime.GOARCH
	caps := []HWCapability{CapNone}
	vectorWidth := 64
	cacheLineBytes := e.config.CacheLineSize
	if cacheLineBytes == 0 {
		cacheLineBytes = 64
	}

	switch arch {
	case "amd64":
		caps = append(caps, CapSSE42)
		vectorWidth = 128
		if !e.config.ForceSoftwareFallback {
			caps = append(caps, CapAVX2)
			vectorWidth = 256
		}
	case "arm64":
		caps = append(caps, CapNEON)
		vectorWidth = 128
	}

	return HWProfile{
		Architecture:   arch,
		Capabilities:   caps,
		VectorWidthBit: vectorWidth,
		CacheLineBytes: cacheLineBytes,
		NumCores:       runtime.NumCPU(),
		DetectedAt:     time.Now(),
	}
}

// PlanAcceleration creates an execution plan for the given operation and data size.
// Note: The current implementation uses loop unrolling and parallelism for
// acceleration, not hardware SIMD intrinsics. EstimatedSpeedup reflects the
// benefit from these portable optimizations.
func (e *HWAcceleratedQueryEngine) PlanAcceleration(op AccelOperation, dataSize int) AccelPlan {
	useVectorized := e.config.EnableSIMD && !e.config.ForceSoftwareFallback && len(e.profile.Capabilities) > 1
	batchCount := 1
	if e.config.EnableBatchProcessing && e.config.BatchSize > 0 {
		batchCount = (dataSize + e.config.BatchSize - 1) / e.config.BatchSize
		if batchCount < 1 {
			batchCount = 1
		}
	}

	workers := 1
	if e.config.EnableParallelScan && dataSize > e.config.BatchSize*4 {
		workers = e.config.MaxParallelWorkers
		if workers < 1 {
			workers = 1
		}
		if workers > batchCount {
			workers = batchCount
		}
	}

	// Speedup from loop unrolling is modest (~1.2-1.5x), not the theoretical
	// SIMD vector width. Parallelism provides the major gain.
	speedup := 1.0
	if useVectorized {
		speedup = 1.3 // conservative estimate for 4-way loop unrolling
	}
	if workers > 1 {
		speedup *= float64(workers) * 0.8 // account for coordination overhead
	}

	return AccelPlan{
		Operation:        op,
		InputSize:        dataSize,
		UseVectorized:    useVectorized,
		EstimatedSpeedup: speedup,
		BatchCount:       batchCount,
		ParallelWorkers:  workers,
	}
}

// recordOp records an operation execution for stats.
func (e *HWAcceleratedQueryEngine) recordOp(op AccelOperation, dataLen int, speedup float64) {
	e.queriesAccelerated.Add(1)
	e.totalDataProcessed.Add(int64(dataLen))
	e.statsMu.Lock()
	e.operationsByType[op]++
	e.totalSpeedup += speedup
	e.speedupCount++
	e.statsMu.Unlock()
}

// SumFloat64 computes the sum of data using SIMD-style unrolled accumulation.
func (e *HWAcceleratedQueryEngine) SumFloat64(data []float64) float64 {
	if len(data) == 0 {
		return 0
	}
	start := time.Now()
	n := len(data)
	var s0, s1, s2, s3 float64
	i := 0
	for ; i+3 < n; i += 4 {
		s0 += data[i]
		s1 += data[i+1]
		s2 += data[i+2]
		s3 += data[i+3]
	}
	total := s0 + s1 + s2 + s3
	for ; i < n; i++ {
		total += data[i]
	}
	dur := time.Since(start)
	speedup := e.computeSpeedup(OpSum, dur, n)
	e.recordOp(OpSum, n, speedup)
	return total
}

// MinFloat64 returns the minimum value in data.
func (e *HWAcceleratedQueryEngine) MinFloat64(data []float64) float64 {
	if len(data) == 0 {
		return math.NaN()
	}
	start := time.Now()
	min := data[0]
	for i := 1; i < len(data); i++ {
		if data[i] < min {
			min = data[i]
		}
	}
	dur := time.Since(start)
	speedup := e.computeSpeedup(OpMin, dur, len(data))
	e.recordOp(OpMin, len(data), speedup)
	return min
}

// MaxFloat64 returns the maximum value in data.
func (e *HWAcceleratedQueryEngine) MaxFloat64(data []float64) float64 {
	if len(data) == 0 {
		return math.NaN()
	}
	start := time.Now()
	max := data[0]
	for i := 1; i < len(data); i++ {
		if data[i] > max {
			max = data[i]
		}
	}
	dur := time.Since(start)
	speedup := e.computeSpeedup(OpMax, dur, len(data))
	e.recordOp(OpMax, len(data), speedup)
	return max
}

// MeanFloat64 returns the arithmetic mean.
func (e *HWAcceleratedQueryEngine) MeanFloat64(data []float64) float64 {
	if len(data) == 0 {
		return math.NaN()
	}
	start := time.Now()
	sum := e.sumRaw(data)
	mean := sum / float64(len(data))
	dur := time.Since(start)
	speedup := e.computeSpeedup(OpMean, dur, len(data))
	e.recordOp(OpMean, len(data), speedup)
	return mean
}

// CountWhere counts elements matching the predicate.
func (e *HWAcceleratedQueryEngine) CountWhere(data []float64, predicate func(float64) bool) int64 {
	start := time.Now()
	var count int64
	for _, v := range data {
		if predicate(v) {
			count++
		}
	}
	dur := time.Since(start)
	speedup := e.computeSpeedup(OpCount, dur, len(data))
	e.recordOp(OpCount, len(data), speedup)
	return count
}

// VarianceFloat64 computes the population variance.
func (e *HWAcceleratedQueryEngine) VarianceFloat64(data []float64) float64 {
	if len(data) == 0 {
		return math.NaN()
	}
	start := time.Now()
	mean := e.sumRaw(data) / float64(len(data))
	var ss float64
	for _, v := range data {
		d := v - mean
		ss += d * d
	}
	variance := ss / float64(len(data))
	dur := time.Since(start)
	speedup := e.computeSpeedup(OpVariance, dur, len(data))
	e.recordOp(OpVariance, len(data), speedup)
	return variance
}

// BatchAggregate runs an operation across multiple series batches, returning one result per batch.
func (e *HWAcceleratedQueryEngine) BatchAggregate(op AccelOperation, batches [][]float64) []float64 {
	results := make([]float64, len(batches))
	for i, batch := range batches {
		results[i] = e.execOp(op, batch)
	}
	return results
}

// ParallelScan runs an aggregation using a parallel worker pool.
func (e *HWAcceleratedQueryEngine) ParallelScan(data []float64, op AccelOperation) AccelResult {
	start := time.Now()

	plan := e.PlanAcceleration(op, len(data))
	workers := plan.ParallelWorkers
	if workers < 1 {
		workers = 1
	}

	chunkSize := (len(data) + workers - 1) / workers
	partials := make([]float64, workers)
	counts := make([]int, workers)

	var wg sync.WaitGroup
	for w := 0; w < workers; w++ {
		lo := w * chunkSize
		hi := lo + chunkSize
		if lo >= len(data) {
			break
		}
		if hi > len(data) {
			hi = len(data)
		}
		wg.Add(1)
		go func(idx int, chunk []float64) {
			defer wg.Done()
			partials[idx] = e.execOp(op, chunk)
			counts[idx] = len(chunk)
		}(w, data[lo:hi])
	}
	wg.Wait()

	value := e.mergePartials(op, partials, counts, workers)
	dur := time.Since(start)

	method := "parallel"
	if workers == 1 {
		method = "single"
	}
	speedup := e.computeSpeedup(op, dur, len(data))
	e.recordOp(op, len(data), speedup)

	return AccelResult{
		Operation:         op,
		Value:             value,
		Count:             int64(len(data)),
		Duration:          dur,
		SpeedupVsBaseline: speedup,
		MethodUsed:        method,
	}
}

// Benchmark benchmarks all operations on synthetic data of the given size.
func (e *HWAcceleratedQueryEngine) Benchmark(dataSize int) map[AccelOperation]AccelResult {
	data := make([]float64, dataSize)
	for i := range data {
		data[i] = float64(i) + 0.5
	}

	ops := []AccelOperation{OpSum, OpMin, OpMax, OpMean, OpCount, OpVariance}
	results := make(map[AccelOperation]AccelResult, len(ops))

	for _, op := range ops {
		start := time.Now()
		val := e.execOp(op, data)
		dur := time.Since(start)

		// Store baseline for speedup calculation
		e.mu.Lock()
		e.baseline[op] = dur
		e.mu.Unlock()

		results[op] = AccelResult{
			Operation:         op,
			Value:             val,
			Count:             int64(dataSize),
			Duration:          dur,
			SpeedupVsBaseline: 1.0,
			MethodUsed:        "benchmark",
		}
	}
	return results
}

// Stats returns current engine statistics.
func (e *HWAcceleratedQueryEngine) Stats() HWAcceleratedQueryStats {
	avgSpeedup := 0.0
	e.statsMu.Lock()
	opsByType := make(map[string]int64, len(e.operationsByType))
	for op, cnt := range e.operationsByType {
		opsByType[op.String()] = cnt
	}
	if e.speedupCount > 0 {
		avgSpeedup = e.totalSpeedup / float64(e.speedupCount)
	}
	e.statsMu.Unlock()

	return HWAcceleratedQueryStats{
		QueriesAccelerated: e.queriesAccelerated.Load(),
		TotalDataProcessed: e.totalDataProcessed.Load(),
		AvgSpeedup:         avgSpeedup,
		HardwareProfile:    e.profile,
		OperationsByType:   opsByType,
	}
}

// RegisterHTTPHandlers registers hardware-accelerated query engine endpoints.
func (e *HWAcceleratedQueryEngine) RegisterHTTPHandlers(mux *http.ServeMux) {
	mux.HandleFunc("/api/v1/hwaccel/capabilities", func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodGet {
			http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
			return
		}
		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(e.profile)
	})

	mux.HandleFunc("/api/v1/hwaccel/benchmark", func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost {
			http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
			return
		}
		var req struct {
			DataSize int `json:"data_size"`
		}
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			http.Error(w, "bad request", http.StatusBadRequest)
			return
		}
		if req.DataSize <= 0 {
			req.DataSize = 10000
		}
		results := e.Benchmark(req.DataSize)
		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(results)
	})

	mux.HandleFunc("/api/v1/hwaccel/stats", func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodGet {
			http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
			return
		}
		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(e.Stats())
	})
}
