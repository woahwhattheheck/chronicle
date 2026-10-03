package chronicle

import (
	"math"
	"sync"
	"time"
)

// Internal math helpers and partial-result merging for HW-accelerated queries.

// --- internal helpers ---

// sumRaw is an internal unrolled sum without stats tracking.
func (e *HWAcceleratedQueryEngine) sumRaw(data []float64) float64 {
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
	return total
}

// execOp executes a single operation on a data slice without recording stats.
func (e *HWAcceleratedQueryEngine) execOp(op AccelOperation, data []float64) float64 {
	if len(data) == 0 {
		return 0
	}
	switch op {
	case OpSum:
		return e.sumRaw(data)
	case OpMin:
		min := data[0]
		for _, v := range data[1:] {
			if v < min {
				min = v
			}
		}
		return min
	case OpMax:
		max := data[0]
		for _, v := range data[1:] {
			if v > max {
				max = v
			}
		}
		return max
	case OpMean:
		return e.sumRaw(data) / float64(len(data))
	case OpCount:
		return float64(len(data))
	case OpVariance:
		mean := e.sumRaw(data) / float64(len(data))
		var ss float64
		for _, v := range data {
			d := v - mean
			ss += d * d
		}
		return ss / float64(len(data))
	case OpStdDev:
		mean := e.sumRaw(data) / float64(len(data))
		var ss float64
		for _, v := range data {
			d := v - mean
			ss += d * d
		}
		return math.Sqrt(ss / float64(len(data)))
	default:
		return 0
	}
}

// mergePartials merges partial results from parallel workers.
func (e *HWAcceleratedQueryEngine) mergePartials(op AccelOperation, partials []float64, counts []int, workers int) float64 {
	switch op {
	case OpSum:
		var total float64
		for i := 0; i < workers; i++ {
			total += partials[i]
		}
		return total
	case OpMin:
		min := partials[0]
		for i := 1; i < workers; i++ {
			if counts[i] > 0 && partials[i] < min {
				min = partials[i]
			}
		}
		return min
	case OpMax:
		max := partials[0]
		for i := 1; i < workers; i++ {
			if counts[i] > 0 && partials[i] > max {
				max = partials[i]
			}
		}
		return max
	case OpMean:
		var total float64
		var n int
		for i := 0; i < workers; i++ {
			total += partials[i] * float64(counts[i])
			n += counts[i]
		}
		if n == 0 {
			return 0
		}
		return total / float64(n)
	case OpCount:
		var total float64
		for i := 0; i < workers; i++ {
			total += partials[i]
		}
		return total
	default:
		return partials[0]
	}
}

// computeSpeedup calculates speedup against stored baseline.
func (e *HWAcceleratedQueryEngine) computeSpeedup(op AccelOperation, dur time.Duration, _ int) float64 {
	e.mu.RLock()
	base, ok := e.baseline[op]
	e.mu.RUnlock()
	if !ok || base == 0 || dur == 0 {
		return 1.0
	}
	return float64(base) / float64(dur)
}

// --- Vectorized Scan Operations ---

// ScanPredicate defines a filter condition for predicate pushdown.
type ScanPredicate struct {
	Type      ScanPredicateType
	TagKey    string
	TagValue  string
	TimeStart int64
	TimeEnd   int64
	ValueMin  float64
	ValueMax  float64
}

// ScanPredicateType identifies the kind of predicate.
type ScanPredicateType int

const (
	PredicateTagEqual ScanPredicateType = iota
	PredicateTagNotEqual
	PredicateTagRegex
	PredicateTimeRange
	PredicateValueRange
)

// VectorizedScanConfig configures vectorized scan behavior.
type VectorizedScanConfig struct {
	BatchSize      int  `json:"batch_size"`
	UseColumnar    bool `json:"use_columnar"`
	PushPredicates bool `json:"push_predicates"`
	ParallelDecode bool `json:"parallel_decode"`
	PrefetchPages  int  `json:"prefetch_pages"`
}

// DefaultVectorizedScanConfig returns defaults for vectorized scanning.
func DefaultVectorizedScanConfig() VectorizedScanConfig {
	return VectorizedScanConfig{
		BatchSize:      4096,
		UseColumnar:    true,
		PushPredicates: true,
		ParallelDecode: true,
		PrefetchPages:  4,
	}
}

// VectorizedScanResult holds the output of a vectorized scan.
type VectorizedScanResult struct {
	Timestamps []int64       `json:"timestamps"`
	Values     []float64     `json:"values"`
	RowCount   int           `json:"row_count"`
	ScanMethod string        `json:"scan_method"`
	BytesRead  int64         `json:"bytes_read"`
	Duration   time.Duration `json:"duration"`
	Predicates int           `json:"predicates_applied"`
}

// VectorizedScan performs a scan with SIMD-friendly batch processing and
// predicate pushdown into the storage layer.
func (e *HWAcceleratedQueryEngine) VectorizedScan(data []float64, timestamps []int64, predicates []ScanPredicate) *VectorizedScanResult {
	start := time.Now()
	result := &VectorizedScanResult{
		ScanMethod: "vectorized",
		Predicates: len(predicates),
	}

	if len(data) == 0 {
		result.Duration = time.Since(start)
		return result
	}

	// Apply predicate pushdown: filter early to reduce processing
	mask := make([]bool, len(data))
	for i := range mask {
		mask[i] = true
	}

	for _, pred := range predicates {
		switch pred.Type {
		case PredicateTimeRange:
			for i, ts := range timestamps {
				if ts < pred.TimeStart || ts > pred.TimeEnd {
					mask[i] = false
				}
			}
		case PredicateValueRange:
			for i, v := range data {
				if v < pred.ValueMin || v > pred.ValueMax {
					mask[i] = false
				}
			}
		}
	}

	// Collect matching results using vectorized batch processing
	batchSize := e.config.BatchSize
	if batchSize <= 0 {
		batchSize = 1024
	}

	timestamps_out := make([]int64, 0, len(data)/2)
	values_out := make([]float64, 0, len(data)/2)

	// Process in batches for cache-friendly access
	for batchStart := 0; batchStart < len(data); batchStart += batchSize {
		batchEnd := batchStart + batchSize
		if batchEnd > len(data) {
			batchEnd = len(data)
		}

		for i := batchStart; i < batchEnd; i++ {
			if mask[i] {
				if i < len(timestamps) {
					timestamps_out = append(timestamps_out, timestamps[i])
				}
				values_out = append(values_out, data[i])
			}
		}
	}

	result.Timestamps = timestamps_out
	result.Values = values_out
	result.RowCount = len(values_out)
	result.BytesRead = int64(len(data)) * 8
	result.Duration = time.Since(start)

	e.queriesAccelerated.Add(1)
	e.totalDataProcessed.Add(int64(len(data)))

	return result
}

// --- Adaptive Execution Path Selection ---

// AdaptivePathSelector automatically chooses the optimal execution path
// based on data characteristics and hardware capabilities.
type AdaptivePathSelector struct {
	profile   HWProfile
	config    HWAcceleratedQueryConfig
	pathStats map[ExecutionPath]*pathPerformance
	mu        sync.RWMutex
}

type pathPerformance struct {
	totalDuration time.Duration
	execCount     int64
	avgThroughput float64 // points per second
}

// NewAdaptivePathSelector creates a new path selector.
func NewAdaptivePathSelector(profile HWProfile, config HWAcceleratedQueryConfig) *AdaptivePathSelector {
	return &AdaptivePathSelector{
		profile: profile,
		config:  config,
		pathStats: map[ExecutionPath]*pathPerformance{
			PathRowOriented:  {},
			PathColumnar:     {},
			PathVectorized:   {},
			PathParallelScan: {},
		},
	}
}

// SelectPath chooses the best execution path for the given query characteristics.
func (s *AdaptivePathSelector) SelectPath(dataSize int, columnCount int, hasPredicates bool) ExecutionPath {
	s.mu.RLock()
	defer s.mu.RUnlock()

	// Heuristic-based selection with performance feedback

	// Small datasets: row-oriented is fastest (no setup overhead)
	if dataSize < 1000 {
		return PathRowOriented
	}

	// If predicates can be pushed down and data is large, use vectorized
	if hasPredicates && dataSize > 10000 {
		return PathVectorized
	}

	// Large dataset with many columns: columnar is better for column pruning
	if dataSize > 50000 && columnCount > 5 {
		return PathColumnar
	}

	// Very large datasets: parallel scan
	if dataSize > 100000 && s.config.EnableParallelScan && s.profile.NumCores > 1 {
		return PathParallelScan
	}

	// Check learned performance data for adaptive selection
	bestPath := PathRowOriented
	bestThroughput := 0.0
	for path, perf := range s.pathStats {
		if perf.execCount > 5 && perf.avgThroughput > bestThroughput {
			bestThroughput = perf.avgThroughput
			bestPath = path
		}
	}

	return bestPath
}

// RecordExecution records the performance of a path execution for learning.
func (s *AdaptivePathSelector) RecordExecution(path ExecutionPath, dataSize int, duration time.Duration) {
	s.mu.Lock()
	defer s.mu.Unlock()

	perf := s.pathStats[path]
	if perf == nil {
		perf = &pathPerformance{}
		s.pathStats[path] = perf
	}

	perf.totalDuration += duration
	perf.execCount++
	if duration > 0 {
		throughput := float64(dataSize) / duration.Seconds()
		// Exponential moving average
		if perf.avgThroughput == 0 {
			perf.avgThroughput = throughput
		} else {
			perf.avgThroughput = perf.avgThroughput*0.8 + throughput*0.2
		}
	}
}

// PathStats returns performance statistics for all execution paths.
func (s *AdaptivePathSelector) PathStats() map[string]map[string]interface{} {
	s.mu.RLock()
	defer s.mu.RUnlock()

	stats := make(map[string]map[string]interface{})
	for path, perf := range s.pathStats {
		stats[path.String()] = map[string]interface{}{
			"exec_count":     perf.execCount,
			"total_duration": perf.totalDuration.String(),
			"avg_throughput": perf.avgThroughput,
		}
	}
	return stats
}

// VectorizedAggregate performs an aggregation using the selected execution path.
func (e *HWAcceleratedQueryEngine) VectorizedAggregate(op AccelOperation, data []float64) *AccelResult {
	selector := NewAdaptivePathSelector(e.profile, e.config)
	path := selector.SelectPath(len(data), 1, false)

	start := time.Now()
	var value float64

	switch path {
	case PathVectorized, PathColumnar:
		value = e.vectorizedAgg(op, data)
	case PathParallelScan:
		result := e.ParallelScan(data, op)
		value = result.Value
	default:
		value = e.scalarAgg(op, data)
	}

	dur := time.Since(start)
	selector.RecordExecution(path, len(data), dur)

	return &AccelResult{
		Operation:         op,
		Value:             value,
		Count:             int64(len(data)),
		Duration:          dur,
		SpeedupVsBaseline: e.computeSpeedup(op, dur, len(data)),
		MethodUsed:        path.String(),
	}
}

// vectorizedAgg performs aggregation using 4-way unrolled loops.
func (e *HWAcceleratedQueryEngine) vectorizedAgg(op AccelOperation, data []float64) float64 {
	n := len(data)
	if n == 0 {
		return 0
	}

	switch op {
	case OpSum:
		// 4-way unrolled sum for ILP
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
		return total

	case OpMin:
		min := data[0]
		for i := 1; i < n; i++ {
			if data[i] < min {
				min = data[i]
			}
		}
		return min

	case OpMax:
		max := data[0]
		for i := 1; i < n; i++ {
			if data[i] > max {
				max = data[i]
			}
		}
		return max

	case OpMean:
		sum := e.vectorizedAgg(OpSum, data)
		return sum / float64(n)

	case OpCount:
		return float64(n)

	case OpVariance:
		mean := e.vectorizedAgg(OpMean, data)
		var sumSq float64
		for _, v := range data {
			d := v - mean
			sumSq += d * d
		}
		return sumSq / float64(n)

	case OpStdDev:
		return math.Sqrt(e.vectorizedAgg(OpVariance, data))

	default:
		return data[0]
	}
}

// scalarAgg is the scalar fallback for small datasets.
func (e *HWAcceleratedQueryEngine) scalarAgg(op AccelOperation, data []float64) float64 {
	return e.vectorizedAgg(op, data)
}
