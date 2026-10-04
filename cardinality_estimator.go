package chronicle

import (
	"context"
	"fmt"
	"hash/fnv"
	"math"
	"math/bits"
	"sort"
	"sync"
	"time"
)

// CardinalityEstimatorConfig configures the adaptive cardinality estimator.
type CardinalityEstimatorConfig struct {
	Enabled              bool          `json:"enabled"`
	HLLPrecision         uint8         `json:"hll_precision"`
	HistogramBuckets     int           `json:"histogram_buckets"`
	AutoCollectOnCompact bool          `json:"auto_collect_on_compact"`
	StaleThreshold       time.Duration `json:"stale_threshold"`
	SampleRate           float64       `json:"sample_rate"`
	FeedbackEnabled      bool          `json:"feedback_enabled"`
	FeedbackWindowSize   int           `json:"feedback_window_size"`
}

// DefaultCardinalityEstimatorConfig returns sensible defaults.
func DefaultCardinalityEstimatorConfig() CardinalityEstimatorConfig {
	return CardinalityEstimatorConfig{
		Enabled:              true,
		HLLPrecision:         14,
		HistogramBuckets:     64,
		AutoCollectOnCompact: true,
		StaleThreshold:       1 * time.Hour,
		SampleRate:           1.0,
		FeedbackEnabled:      true,
		FeedbackWindowSize:   100,
	}
}

// HyperLogLogSketch implements a HyperLogLog cardinality estimator.
type HyperLogLogSketch struct {
	precision  uint8
	m          uint32 // number of registers (2^precision)
	registers  []uint8
}

// NewHyperLogLogSketch creates a new HLL sketch with the given precision (4-18).
func NewHyperLogLogSketch(precision uint8) *HyperLogLogSketch {
	if precision < 4 {
		precision = 4
	}
	if precision > 18 {
		precision = 18
	}
	m := uint32(1) << precision
	return &HyperLogLogSketch{
		precision: precision,
		m:         m,
		registers: make([]uint8, m),
	}
}

// hllHash computes a 64-bit hash using FNV-1a with extra mixing.
func hllHash(data []byte) uint64 {
	h := fnv.New64a()
	h.Write(data)
	v := h.Sum64()
	// Avalanche mixing for better distribution
	v ^= v >> 33
	v *= 0xff51afd7ed558ccd
	v ^= v >> 33
	v *= 0xc4ceb9fe1a85ec53
	v ^= v >> 33
	return v
}

// Add inserts a value into the sketch.
func (h *HyperLogLogSketch) Add(value []byte) {
	x := hllHash(value)

	idx := x >> (64 - h.precision)
	// Count leading zeros in the remaining bits
	remaining := x<<h.precision | 1 // guard bit to bound the count
	rho := uint8(bits.LeadingZeros64(remaining)) + 1

	if rho > h.registers[idx] {
		h.registers[idx] = rho
	}
}

// AddString inserts a string value into the sketch.
func (h *HyperLogLogSketch) AddString(value string) {
	h.Add([]byte(value))
}

// Estimate returns the estimated cardinality.
func (h *HyperLogLogSketch) Estimate() uint64 {
	m := float64(h.m)
	var sum float64
	zeros := 0
	for _, v := range h.registers {
		sum += 1.0 / float64(uint64(1)<<v)
		if v == 0 {
			zeros++
		}
	}

	alpha := 0.7213 / (1.0 + 1.079/m)
	estimate := alpha * m * m / sum

	// Small range correction
	if estimate <= 2.5*m && zeros > 0 {
		estimate = m * math.Log(m/float64(zeros))
	}
	// Large range correction
	if estimate > (1.0/30.0)*math.Pow(2, 64) {
		estimate = -(math.Pow(2, 64)) * math.Log(1.0-estimate/math.Pow(2, 64))
	}

	return uint64(estimate)
}

// Merge combines another HLL sketch into this one.
func (h *HyperLogLogSketch) Merge(other *HyperLogLogSketch) {
	if other == nil || h.precision != other.precision {
		return
	}
	for i := range h.registers {
		if other.registers[i] > h.registers[i] {
			h.registers[i] = other.registers[i]
		}
	}
}

// EquiDepthHistogram implements an equi-depth histogram for value distribution estimation.
type EquiDepthHistogram struct {
	Buckets    []HistogramBucket `json:"buckets"`
	TotalCount int64             `json:"total_count"`
	NumBuckets int               `json:"num_buckets"`
}

// NewEquiDepthHistogram builds an equi-depth histogram from sorted values.
func NewEquiDepthHistogram(values []float64, numBuckets int) *EquiDepthHistogram {
	if len(values) == 0 || numBuckets <= 0 {
		return &EquiDepthHistogram{NumBuckets: numBuckets}
	}

	sorted := make([]float64, len(values))
	copy(sorted, values)
	sort.Float64s(sorted)

	if numBuckets > len(sorted) {
		numBuckets = len(sorted)
	}

	buckets := make([]HistogramBucket, 0, numBuckets)
	bucketSize := len(sorted) / numBuckets

	for i := 0; i < numBuckets; i++ {
		start := i * bucketSize
		end := start + bucketSize
		if i == numBuckets-1 {
			end = len(sorted)
		}
		if start >= len(sorted) {
			break
		}

		segment := sorted[start:end]
		distinct := make(map[float64]struct{})
		for _, v := range segment {
			distinct[v] = struct{}{}
		}

		buckets = append(buckets, HistogramBucket{
			LowerBound:     segment[0],
			UpperBound:     segment[len(segment)-1],
			Count:          int64(len(segment)),
			DistinctValues: int64(len(distinct)),
		})
	}

	return &EquiDepthHistogram{
		Buckets:    buckets,
		TotalCount: int64(len(values)),
		NumBuckets: numBuckets,
	}
}

// EstimateRangeCount estimates rows in [low, high].
func (h *EquiDepthHistogram) EstimateRangeCount(low, high float64) int64 {
	if h == nil || len(h.Buckets) == 0 {
		return 0
	}
	var count int64
	for _, b := range h.Buckets {
		if b.UpperBound < low || b.LowerBound > high {
			continue
		}
		bRange := b.UpperBound - b.LowerBound
		if bRange <= 0 {
			count += b.Count
			continue
		}
		overlapLow := math.Max(b.LowerBound, low)
		overlapHigh := math.Min(b.UpperBound, high)
		fraction := (overlapHigh - overlapLow) / bRange
		count += int64(float64(b.Count) * fraction)
	}
	return count
}

// EstimateSelectivity returns the fraction of rows in [low, high].
func (h *EquiDepthHistogram) EstimateSelectivity(low, high float64) float64 {
	if h == nil || h.TotalCount == 0 {
		return 0.5
	}
	return float64(h.EstimateRangeCount(low, high)) / float64(h.TotalCount)
}

// MetricCardinalityStats holds per-metric cardinality statistics with HLL sketches.
type MetricCardinalityStats struct {
	Metric           string                        `json:"metric"`
	RowCount         int64                         `json:"row_count"`
	TagSketches      map[string]*HyperLogLogSketch `json:"-"`
	TagCardinalities map[string]uint64             `json:"tag_cardinalities"`
	ValueHistogram   *EquiDepthHistogram           `json:"value_histogram,omitempty"`
	TimeHistogram    *EquiDepthHistogram           `json:"time_histogram,omitempty"`
	MinTimestamp     int64                         `json:"min_timestamp"`
	MaxTimestamp     int64                         `json:"max_timestamp"`
	LastCollected    time.Time                     `json:"last_collected"`
	PartitionsScanned int                          `json:"partitions_scanned"`
}

// FeedbackEntry records the accuracy of a past cardinality estimate.
type FeedbackEntry struct {
	Metric       string    `json:"metric"`
	Estimated    int64     `json:"estimated"`
	Actual       int64     `json:"actual"`
	ErrorRatio   float64   `json:"error_ratio"`
	Timestamp    time.Time `json:"timestamp"`
}

// CardinalityEstimator maintains per-metric/tag statistics and feeds
// estimates into the cost-based optimizer for accurate plan costing.
type CardinalityEstimator struct {
	config   CardinalityEstimatorConfig
	db       *DB
	mu       sync.RWMutex
	stats    map[string]*MetricCardinalityStats
	feedback []FeedbackEntry

	// Runtime stats
	totalCollections int64
	totalEstimates   int64
	avgErrorRatio    float64
}

// NewCardinalityEstimator creates a new cardinality estimator.
func NewCardinalityEstimator(db *DB, config CardinalityEstimatorConfig) *CardinalityEstimator {
	return &CardinalityEstimator{
		config:   config,
		db:       db,
		stats:    make(map[string]*MetricCardinalityStats),
		feedback: make([]FeedbackEntry, 0, config.FeedbackWindowSize),
	}
}

// CollectStats collects cardinality statistics for a metric by scanning its data.
func (ce *CardinalityEstimator) CollectStats(ctx context.Context, metric string) (*MetricCardinalityStats, error) {
	if metric == "" {
		return nil, fmt.Errorf("cardinality estimator: empty metric name")
	}

	q := &Query{Metric: metric}
	result, err := ce.db.Execute(q)
	if err != nil {
		return nil, fmt.Errorf("cardinality estimator: collect %q: %w", metric, err)
	}

	mcs := &MetricCardinalityStats{
		Metric:           metric,
		RowCount:         int64(len(result.Points)),
		TagSketches:      make(map[string]*HyperLogLogSketch),
		TagCardinalities: make(map[string]uint64),
		LastCollected:    time.Now(),
	}

	if len(result.Points) == 0 {
		ce.mu.Lock()
		ce.stats[metric] = mcs
		ce.totalCollections++
		ce.mu.Unlock()
		return mcs, nil
	}

	values := make([]float64, 0, len(result.Points))
	timestamps := make([]float64, 0, len(result.Points))

	for i := range result.Points {
		p := &result.Points[i]
		values = append(values, p.Value)
		timestamps = append(timestamps, float64(p.Timestamp))

		if mcs.MinTimestamp == 0 || p.Timestamp < mcs.MinTimestamp {
			mcs.MinTimestamp = p.Timestamp
		}
		if p.Timestamp > mcs.MaxTimestamp {
			mcs.MaxTimestamp = p.Timestamp
		}

		for k, v := range p.Tags {
			sketch, ok := mcs.TagSketches[k]
			if !ok {
				sketch = NewHyperLogLogSketch(ce.config.HLLPrecision)
				mcs.TagSketches[k] = sketch
			}
			sketch.AddString(v)
		}
	}

	// Build histograms
	mcs.ValueHistogram = NewEquiDepthHistogram(values, ce.config.HistogramBuckets)
	mcs.TimeHistogram = NewEquiDepthHistogram(timestamps, ce.config.HistogramBuckets)

	// Extract cardinalities from HLL sketches
	for k, sketch := range mcs.TagSketches {
		mcs.TagCardinalities[k] = sketch.Estimate()
	}

	ce.mu.Lock()
	ce.stats[metric] = mcs
	ce.totalCollections++
	ce.mu.Unlock()

	return mcs, nil
}

// CollectAllStats collects statistics for all known metrics.
func (ce *CardinalityEstimator) CollectAllStats(ctx context.Context) error {
	metrics := ce.db.Metrics()
	for _, m := range metrics {
		select {
		case <-ctx.Done():
			return ctx.Err()
		default:
		}
		if _, err := ce.CollectStats(ctx, m); err != nil {
			return err
		}
	}
	return nil
}

// CollectFromPartition collects stats from a single partition during compaction.
func (ce *CardinalityEstimator) CollectFromPartition(partition *Partition) {
	if partition == nil {
		return
	}
	partition.mu.RLock()
	defer partition.mu.RUnlock()

	for _, sd := range partition.series {
		ce.mu.Lock()
		mcs, ok := ce.stats[sd.Series.Metric]
		if !ok {
			mcs = &MetricCardinalityStats{
				Metric:           sd.Series.Metric,
				TagSketches:      make(map[string]*HyperLogLogSketch),
				TagCardinalities: make(map[string]uint64),
				LastCollected:    time.Now(),
			}
			ce.stats[sd.Series.Metric] = mcs
		}

		mcs.RowCount += int64(len(sd.Timestamps))
		mcs.PartitionsScanned++

		if len(sd.Timestamps) > 0 {
			if mcs.MinTimestamp == 0 || sd.MinTime < mcs.MinTimestamp {
				mcs.MinTimestamp = sd.MinTime
			}
			if sd.MaxTime > mcs.MaxTimestamp {
				mcs.MaxTimestamp = sd.MaxTime
			}
		}

		for k, v := range sd.Series.Tags {
			sketch, exists := mcs.TagSketches[k]
			if !exists {
				sketch = NewHyperLogLogSketch(ce.config.HLLPrecision)
				mcs.TagSketches[k] = sketch
			}
			sketch.AddString(v)
			mcs.TagCardinalities[k] = sketch.Estimate()
		}

		mcs.LastCollected = time.Now()
		ce.mu.Unlock()
	}
}

// distinctTagFilterValueCount returns the number of distinct values in a
// membership filter. IN and NOT IN use set semantics, so repeated literals
// must not change selectivity estimates.
func distinctTagFilterValueCount(values []string) int {
	if len(values) < 2 {
		return len(values)
	}
	unique := make(map[string]struct{}, len(values))
	for _, value := range values {
		unique[value] = struct{}{}
	}
	return len(unique)
}

// EstimateCardinality returns estimated row count for a query.
func (ce *CardinalityEstimator) EstimateCardinality(q *Query) int64 {
	if q == nil || q.Metric == "" {
		return 0
	}

	ce.mu.Lock()
	mcs := ce.stats[q.Metric]
	ce.totalEstimates++
	ce.mu.Unlock()

	if mcs == nil {
		return 0
	}

	estimate := float64(mcs.RowCount)

	// Apply time range selectivity
	if q.Start > 0 || q.End > 0 {
		totalRange := float64(mcs.MaxTimestamp - mcs.MinTimestamp)
		if totalRange > 0 {
			qStart := float64(q.Start)
			qEnd := float64(q.End)
			if qStart < float64(mcs.MinTimestamp) {
				qStart = float64(mcs.MinTimestamp)
			}
			if qEnd <= 0 || qEnd > float64(mcs.MaxTimestamp) {
				qEnd = float64(mcs.MaxTimestamp)
			}
			if qEnd > qStart {
				sel := (qEnd - qStart) / totalRange
				if sel > 1.0 {
					sel = 1.0
				}
				estimate *= sel
			}
		}
	}

	// Apply tag equality selectivity using HLL estimates
	for tagKey := range q.Tags {
		card, ok := mcs.TagCardinalities[tagKey]
		if ok && card > 0 {
			estimate /= float64(card)
		}
	}

	// Apply TagFilter selectivity
	for _, tf := range q.TagFilters {
		card, ok := mcs.TagCardinalities[tf.Key]
		if !ok || card == 0 {
			continue
		}
		switch tf.Op {
		case TagOpEq:
			estimate /= float64(card)
		case TagOpNotEq:
			estimate *= (1.0 - 1.0/float64(card))
		case TagOpIn:
			sel := float64(len(tf.Values)) / float64(card)
			if sel > 1.0 {
				sel = 1.0
			}
			estimate *= sel
		case TagOpNotIn:
			sel := 1.0 - (float64(distinctTagFilterValueCount(tf.Values)) / float64(card))
			if sel < 0.0 {
				sel = 0.0
			}
			estimate *= sel
		case TagOpRegex, TagOpNotRegex:
			estimate *= 0.25 // conservative estimate for regex
		}
	}

	if estimate < 1 {
		estimate = 1
	}
	return int64(math.Ceil(estimate))
}

// IsStale returns true if stats for the given metric are older than the stale threshold.
func (ce *CardinalityEstimator) IsStale(metric string) bool {
	ce.mu.RLock()
	defer ce.mu.RUnlock()
	mcs, ok := ce.stats[metric]
	if !ok {
		return true
	}
	return time.Since(mcs.LastCollected) > ce.config.StaleThreshold
}

// FeedEstimateToOptimizer pushes cardinality stats into the CostBasedOptimizer.
func (ce *CardinalityEstimator) FeedEstimateToOptimizer(optimizer *CostBasedOptimizer) {
	if optimizer == nil {
		return
	}

	ce.mu.RLock()
	defer ce.mu.RUnlock()

	for metric, mcs := range ce.stats {
		ts := &TableStatistics{
			Metric:           metric,
			RowCount:         mcs.RowCount,
			DistinctTagCount: make(map[string]int64),
			MinTimestamp:      mcs.MinTimestamp,
			MaxTimestamp:      mcs.MaxTimestamp,
			Columns:          make(map[string]*CBOColumnStats),
			LastAnalyzed:     mcs.LastCollected,
		}

		for k, card := range mcs.TagCardinalities {
			ts.DistinctTagCount[k] = int64(card)
		}

		if mcs.ValueHistogram != nil {
			valCol := &CBOColumnStats{
				Name:      "value",
				Histogram: mcs.ValueHistogram.Buckets,
			}
			if len(mcs.ValueHistogram.Buckets) > 0 {
				valCol.MinValue = mcs.ValueHistogram.Buckets[0].LowerBound
				valCol.MaxValue = mcs.ValueHistogram.Buckets[len(mcs.ValueHistogram.Buckets)-1].UpperBound
				var distinctSum int64
				for _, b := range mcs.ValueHistogram.Buckets {
					distinctSum += b.DistinctValues
				}
				valCol.DistinctCount = distinctSum
			}
			ts.Columns["value"] = valCol
		}

		optimizer.mu.Lock()
		optimizer.tableStats[metric] = ts
		optimizer.mu.Unlock()
	}
}

// RecordFeedback records the accuracy of a past estimate for runtime feedback.
func (ce *CardinalityEstimator) RecordFeedback(metric string, estimated, actual int64) {
	if actual <= 0 {
		return
	}
	errorRatio := math.Abs(float64(estimated)-float64(actual)) / float64(actual)

	ce.mu.Lock()
	defer ce.mu.Unlock()

	entry := FeedbackEntry{
		Metric:     metric,
		Estimated:  estimated,
		Actual:     actual,
		ErrorRatio: errorRatio,
		Timestamp:  time.Now(),
	}

	if len(ce.feedback) >= ce.config.FeedbackWindowSize {
		ce.feedback = ce.feedback[1:]
	}
	ce.feedback = append(ce.feedback, entry)

	// Update running average error ratio
	var totalError float64
	for _, f := range ce.feedback {
		totalError += f.ErrorRatio
	}
	ce.avgErrorRatio = totalError / float64(len(ce.feedback))
}

// GetStats returns the current statistics for a metric.
func (ce *CardinalityEstimator) GetStats(metric string) *MetricCardinalityStats {
	ce.mu.RLock()
	defer ce.mu.RUnlock()
	return ce.stats[metric]
}

// GetAllStats returns all collected statistics.
func (ce *CardinalityEstimator) GetAllStats() map[string]*MetricCardinalityStats {
	ce.mu.RLock()
	defer ce.mu.RUnlock()
	out := make(map[string]*MetricCardinalityStats, len(ce.stats))
	for k, v := range ce.stats {
		out[k] = v
	}
	return out
}

// GetFeedbackSummary returns accuracy feedback statistics.
func (ce *CardinalityEstimator) GetFeedbackSummary() map[string]interface{} {
	ce.mu.RLock()
	defer ce.mu.RUnlock()
	return map[string]interface{}{
		"total_collections":   ce.totalCollections,
		"total_estimates":     ce.totalEstimates,
		"avg_error_ratio":     ce.avgErrorRatio,
		"feedback_entries":    len(ce.feedback),
		"metrics_tracked":     len(ce.stats),
	}
}

// MergeEquiDepthHistograms merges two equi-depth histograms into a single histogram.
func MergeEquiDepthHistograms(a, b *EquiDepthHistogram) *EquiDepthHistogram {
	if a == nil || len(a.Buckets) == 0 {
		return b
	}
	if b == nil || len(b.Buckets) == 0 {
		return a
	}

	// Merge all buckets and rebuild
	merged := make([]HistogramBucket, 0, len(a.Buckets)+len(b.Buckets))
	merged = append(merged, a.Buckets...)
	merged = append(merged, b.Buckets...)

	// Sort by lower bound
	sort.Slice(merged, func(i, j int) bool {
		return merged[i].LowerBound < merged[j].LowerBound
	})

	// Coalesce overlapping buckets
	numTarget := a.NumBuckets
	if b.NumBuckets > numTarget {
		numTarget = b.NumBuckets
	}

	if len(merged) <= numTarget {
		return &EquiDepthHistogram{
			Buckets:    merged,
			TotalCount: a.TotalCount + b.TotalCount,
			NumBuckets: len(merged),
		}
	}

	// Merge smallest adjacent pairs until we reach target count
	for len(merged) > numTarget {
		minGap := math.Inf(1)
		minIdx := 0
		for i := 0; i < len(merged)-1; i++ {
			gap := merged[i+1].LowerBound - merged[i].UpperBound
			if gap < minGap {
				minGap = gap
				minIdx = i
			}
		}
		// Merge bucket minIdx and minIdx+1
		merged[minIdx] = HistogramBucket{
			LowerBound:     merged[minIdx].LowerBound,
			UpperBound:     merged[minIdx+1].UpperBound,
			Count:          merged[minIdx].Count + merged[minIdx+1].Count,
			DistinctValues: merged[minIdx].DistinctValues + merged[minIdx+1].DistinctValues,
		}
		merged = append(merged[:minIdx+1], merged[minIdx+2:]...)
	}

	return &EquiDepthHistogram{
		Buckets:    merged,
		TotalCount: a.TotalCount + b.TotalCount,
		NumBuckets: len(merged),
	}
}

// AdaptiveHistogramRebuild rebuilds a histogram with adaptive bucket count based on
// data distribution. Regions with high density get more buckets.
func AdaptiveHistogramRebuild(hist *EquiDepthHistogram, maxBuckets int) *EquiDepthHistogram {
	if hist == nil || len(hist.Buckets) == 0 {
		return hist
	}

	// Calculate density per bucket
	type bucketDensity struct {
		bucket  HistogramBucket
		density float64
	}

	densities := make([]bucketDensity, len(hist.Buckets))
	for i, b := range hist.Buckets {
		bRange := b.UpperBound - b.LowerBound
		density := float64(b.Count)
		if bRange > 0 {
			density = float64(b.Count) / bRange
		}
		densities[i] = bucketDensity{bucket: b, density: density}
	}

	// Sort by density descending - split high-density buckets
	sort.Slice(densities, func(i, j int) bool {
		return densities[i].density > densities[j].density
	})

	// High-density buckets get more allocation
	newBuckets := make([]HistogramBucket, 0, maxBuckets)
	for _, d := range densities {
		if len(newBuckets) >= maxBuckets {
			break
		}
		newBuckets = append(newBuckets, d.bucket)
	}

	// Sort back by lower bound
	sort.Slice(newBuckets, func(i, j int) bool {
		return newBuckets[i].LowerBound < newBuckets[j].LowerBound
	})

	return &EquiDepthHistogram{
		Buckets:    newBuckets,
		TotalCount: hist.TotalCount,
		NumBuckets: len(newBuckets),
	}
}

// ApplyFeedbackToCBO applies accumulated feedback to the cost-based optimizer
// using exponential moving average to auto-adjust selectivity estimates.
func (ce *CardinalityEstimator) ApplyFeedbackToCBO(optimizer *CostBasedOptimizer) {
	ce.mu.RLock()
	defer ce.mu.RUnlock()

	if len(ce.feedback) == 0 || optimizer == nil {
		return
	}

	// Calculate per-metric adjustment factors from feedback
	metricErrors := make(map[string][]float64)
	for _, f := range ce.feedback {
		if f.Actual > 0 {
			ratio := float64(f.Estimated) / float64(f.Actual)
			metricErrors[f.Metric] = append(metricErrors[f.Metric], ratio)
		}
	}

	// Apply EMA-based correction
	const alpha = 0.3 // EMA smoothing factor
	for metric, ratios := range metricErrors {
		if len(ratios) == 0 {
			continue
		}

		// Compute EMA of error ratios
		ema := ratios[0]
		for i := 1; i < len(ratios); i++ {
			ema = alpha*ratios[i] + (1-alpha)*ema
		}

		// Adjust the statistics in the optimizer
		optimizer.mu.Lock()
		if ts, ok := optimizer.tableStats[metric]; ok {
			// If we consistently overestimate (ema > 1), reduce row count estimate
			// If we underestimate (ema < 1), increase it
			adjustedRows := int64(float64(ts.RowCount) / ema)
			if adjustedRows > 0 {
				ts.RowCount = adjustedRows
			}
		}
		optimizer.mu.Unlock()
	}
}
