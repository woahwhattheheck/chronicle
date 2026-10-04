package chronicle

import (
	"context"
	"fmt"
	"math"
	"sync"
	"time"
)

// CostBasedOptimizerConfig configures the cost-based query optimizer.
type CostBasedOptimizerConfig struct {
	Enabled              bool          `json:"enabled"`
	SeqScanCostPerRow    float64       `json:"seq_scan_cost_per_row"`
	IndexScanCostPerRow  float64       `json:"index_scan_cost_per_row"`
	SortCostPerRow       float64       `json:"sort_cost_per_row"`
	AggCostPerRow        float64       `json:"agg_cost_per_row"`
	NetworkCostPerRow    float64       `json:"network_cost_per_row"`
	DefaultSelectivity   float64       `json:"default_selectivity"`
	RegexSelectivity     float64       `json:"regex_selectivity"`
	HistogramBuckets     int           `json:"histogram_buckets"`
	StaleStatsThreshold  time.Duration `json:"stale_stats_threshold"`
	MaxAnalyzeSampleSize int           `json:"max_analyze_sample_size"`
}

// DefaultCostBasedOptimizerConfig returns sensible defaults.
func DefaultCostBasedOptimizerConfig() CostBasedOptimizerConfig {
	return CostBasedOptimizerConfig{
		Enabled:              true,
		SeqScanCostPerRow:    1.0,
		IndexScanCostPerRow:  0.5,
		SortCostPerRow:       2.0,
		AggCostPerRow:        0.1,
		NetworkCostPerRow:    0.05,
		DefaultSelectivity:   0.5,
		RegexSelectivity:     0.25,
		HistogramBuckets:     64,
		StaleStatsThreshold:  1 * time.Hour,
		MaxAnalyzeSampleSize: 1_000_000,
	}
}

// HistogramBucket represents a single bucket in an equi-depth histogram.
type HistogramBucket struct {
	LowerBound     float64 `json:"lower_bound"`
	UpperBound     float64 `json:"upper_bound"`
	Count          int64   `json:"count"`
	DistinctValues int64   `json:"distinct_values"`
}

// CBOColumnStats holds per-column statistics with histogram support.
type CBOColumnStats struct {
	Name          string            `json:"name"`
	NullCount     int64             `json:"null_count"`
	DistinctCount int64             `json:"distinct_count"`
	MinValue      float64           `json:"min_value"`
	MaxValue      float64           `json:"max_value"`
	AvgValue      float64           `json:"avg_value"`
	Histogram     []HistogramBucket `json:"histogram,omitempty"`
}

// TableStatistics holds per-metric cardinality statistics.
type TableStatistics struct {
	Metric           string                     `json:"metric"`
	RowCount         int64                      `json:"row_count"`
	DistinctTagCount map[string]int64           `json:"distinct_tag_count"`
	MinTimestamp     int64                      `json:"min_timestamp"`
	MaxTimestamp     int64                      `json:"max_timestamp"`
	Columns          map[string]*CBOColumnStats `json:"columns"`
	LastAnalyzed     time.Time                  `json:"last_analyzed"`
}

// SelectivityEstimator estimates filter selectivity using table statistics.
type SelectivityEstimator struct {
	config CostBasedOptimizerConfig
}

// EstimateTimeRangeSelectivity computes selectivity for a time range filter.
func (se *SelectivityEstimator) EstimateTimeRangeSelectivity(stats *TableStatistics, start, end int64) float64 {
	if stats == nil || stats.RowCount == 0 {
		return se.config.DefaultSelectivity
	}
	totalRange := float64(stats.MaxTimestamp - stats.MinTimestamp)
	if totalRange <= 0 {
		return 1.0
	}

	qStart := start
	qEnd := end
	if qStart < stats.MinTimestamp {
		qStart = stats.MinTimestamp
	}
	if qEnd <= 0 || qEnd > stats.MaxTimestamp {
		qEnd = stats.MaxTimestamp
	}
	if qEnd <= qStart {
		return 0.0
	}

	sel := float64(qEnd-qStart) / totalRange
	if sel > 1.0 {
		sel = 1.0
	}
	return sel
}

// EstimateTagEqualitySelectivity estimates selectivity for tag = 'value' filters.
func (se *SelectivityEstimator) EstimateTagEqualitySelectivity(stats *TableStatistics, tagKey string) float64 {
	if stats == nil || stats.RowCount == 0 {
		return se.config.DefaultSelectivity
	}
	distinct, ok := stats.DistinctTagCount[tagKey]
	if !ok || distinct <= 0 {
		return se.config.DefaultSelectivity
	}
	return 1.0 / float64(distinct)
}

// EstimateTagInSelectivity estimates selectivity for tag IN (...) filters.
func (se *SelectivityEstimator) EstimateTagInSelectivity(stats *TableStatistics, tagKey string, numValues int) float64 {
	eqSel := se.EstimateTagEqualitySelectivity(stats, tagKey)
	sel := eqSel * float64(numValues)
	if sel > 1.0 {
		sel = 1.0
	}
	return sel
}

// EstimateTagRegexSelectivity returns a conservative selectivity for regex filters.
func (se *SelectivityEstimator) EstimateTagRegexSelectivity(stats *TableStatistics, tagKey, pattern string) float64 {
	if stats == nil || stats.RowCount == 0 {
		return se.config.RegexSelectivity
	}
	distinct, ok := stats.DistinctTagCount[tagKey]
	if !ok || distinct <= 0 {
		return se.config.RegexSelectivity
	}
	// Anchored prefixes are more selective
	if len(pattern) > 0 && pattern[0] == '^' {
		return math.Max(1.0/float64(distinct), se.config.RegexSelectivity*0.5)
	}
	return se.config.RegexSelectivity
}

// EstimateValueRangeSelectivity estimates selectivity using histogram data.
func (se *SelectivityEstimator) EstimateValueRangeSelectivity(col *CBOColumnStats, low, high float64) float64 {
	if col == nil || len(col.Histogram) == 0 {
		return se.config.DefaultSelectivity
	}
	var matchCount int64
	var totalCount int64
	for _, b := range col.Histogram {
		totalCount += b.Count
		if b.UpperBound < low || b.LowerBound > high {
			continue
		}
		// Partial overlap: estimate proportionally
		bRange := b.UpperBound - b.LowerBound
		if bRange <= 0 {
			matchCount += b.Count
			continue
		}
		overlapLow := math.Max(b.LowerBound, low)
		overlapHigh := math.Min(b.UpperBound, high)
		fraction := (overlapHigh - overlapLow) / bRange
		matchCount += int64(float64(b.Count) * fraction)
	}
	if totalCount == 0 {
		return se.config.DefaultSelectivity
	}
	return float64(matchCount) / float64(totalCount)
}

// CombinedSelectivity computes combined selectivity for a query using table stats.
func (se *SelectivityEstimator) CombinedSelectivity(stats *TableStatistics, q *Query) float64 {
	sel := 1.0

	// Time range selectivity
	if q.Start > 0 || q.End > 0 {
		timeSel := se.EstimateTimeRangeSelectivity(stats, q.Start, q.End)
		sel *= timeSel
	}

	// Exact tag match selectivity
	for tagKey := range q.Tags {
		tagSel := se.EstimateTagEqualitySelectivity(stats, tagKey)
		sel *= tagSel
	}

	// TagFilter selectivity
	for _, tf := range q.TagFilters {
		switch tf.Op {
		case TagOpEq:
			sel *= se.EstimateTagEqualitySelectivity(stats, tf.Key)
		case TagOpNotEq:
			eqSel := se.EstimateTagEqualitySelectivity(stats, tf.Key)
			sel *= (1.0 - eqSel)
		case TagOpIn:
			sel *= se.EstimateTagInSelectivity(stats, tf.Key, len(tf.Values))
		case TagOpNotIn:
			inSel := se.EstimateTagInSelectivity(stats, tf.Key, distinctTagFilterValueCount(tf.Values))
			sel *= (1.0 - inSel)
		case TagOpRegex:
			pattern := ""
			if len(tf.Values) > 0 {
				pattern = tf.Values[0]
			}
			sel *= se.EstimateTagRegexSelectivity(stats, tf.Key, pattern)
		case TagOpNotRegex:
			pattern := ""
			if len(tf.Values) > 0 {
				pattern = tf.Values[0]
			}
			regSel := se.EstimateTagRegexSelectivity(stats, tf.Key, pattern)
			sel *= (1.0 - regSel)
		}
	}

	if sel < 0 {
		sel = 0
	}
	if sel > 1.0 {
		sel = 1.0
	}
	return sel
}

// CBOPlanType indicates the query execution strategy.
type CBOPlanType string

const (
	PlanSeqScan        CBOPlanType = "seq_scan"
	PlanIndexScan      CBOPlanType = "index_scan"
	PlanIndexOnly      CBOPlanType = "index_only"
	PlanColumnarBatch  CBOPlanType = "columnar_batch"
)

// ColumnarBatchThreshold is the minimum row count to prefer columnar execution.
const ColumnarBatchThreshold = 10000

// CBOPlanCandidate represents a candidate execution plan with its estimated cost.
type CBOPlanCandidate struct {
	Plan          CBOPlanType `json:"plan"`
	EstimatedCost float64     `json:"estimated_cost"`
	EstimatedRows int64       `json:"estimated_rows"`
	Selectivity   float64     `json:"selectivity"`
	ScanCost      float64     `json:"scan_cost"`
	SortCost      float64     `json:"sort_cost"`
	AggCost       float64     `json:"agg_cost"`
	NetworkCost   float64     `json:"network_cost"`
	Notes         string      `json:"notes,omitempty"`
}

// CostBasedOptimizerStats holds engine-level statistics.
type CostBasedOptimizerStats struct {
	TotalAnalyzes    int64 `json:"total_analyzes"`
	TotalEstimates   int64 `json:"total_estimates"`
	TotalPlans       int64 `json:"total_plans"`
	MetricsTracked   int   `json:"metrics_tracked"`
	StaleMetrics     int   `json:"stale_metrics"`
	IndexPlansChosen    int64 `json:"index_plans_chosen"`
	SeqPlansChosen      int64 `json:"seq_plans_chosen"`
	ColumnarPlansChosen int64 `json:"columnar_plans_chosen"`
}

// CostBasedOptimizer collects real statistics and uses them to choose query plans.
type CostBasedOptimizer struct {
	db      *DB
	config  CostBasedOptimizerConfig
	mu      sync.RWMutex
	running bool
	stopCh  chan struct{}

	stats      CostBasedOptimizerStats
	tableStats map[string]*TableStatistics
	estimator  *SelectivityEstimator
}

// NewCostBasedOptimizer creates a new cost-based query optimizer.
func NewCostBasedOptimizer(db *DB, cfg CostBasedOptimizerConfig) *CostBasedOptimizer {
	return &CostBasedOptimizer{
		db:         db,
		config:     cfg,
		stopCh:     make(chan struct{}),
		tableStats: make(map[string]*TableStatistics),
		estimator:  &SelectivityEstimator{config: cfg},
	}
}

// Start starts the cost-based optimizer engine.
func (o *CostBasedOptimizer) Start() error {
	o.mu.Lock()
	defer o.mu.Unlock()
	if o.running {
		return nil
	}
	o.running = true
	o.stopCh = make(chan struct{})
	return nil
}

// Stop stops the cost-based optimizer engine.
func (o *CostBasedOptimizer) Stop() error {
	o.mu.Lock()
	defer o.mu.Unlock()
	if !o.running {
		return nil
	}
	o.running = false
	close(o.stopCh)
	return nil
}

// Analyze collects real statistics for a single metric by scanning its data.
func (o *CostBasedOptimizer) Analyze(ctx context.Context, metric string) (*TableStatistics, error) {
	if metric == "" {
		return nil, fmt.Errorf("cost-based optimizer: %w: empty metric name", ErrInvalidQuery)
	}

	o.mu.RLock()
	if !o.running {
		o.mu.RUnlock()
		return nil, fmt.Errorf("cost-based optimizer: %w", ErrClosed)
	}
	o.mu.RUnlock()

	// Query all points for the metric (bounded by sample size).
	q := &Query{
		Metric: metric,
		Limit:  o.config.MaxAnalyzeSampleSize,
	}

	result, err := o.db.Execute(q)
	if err != nil {
		return nil, fmt.Errorf("cost-based optimizer: analyze %q: %w", metric, err)
	}

	points := result.Points
	if len(points) == 0 {
		ts := &TableStatistics{
			Metric:           metric,
			RowCount:         0,
			DistinctTagCount: make(map[string]int64),
			Columns:          make(map[string]*CBOColumnStats),
			LastAnalyzed:     time.Now(),
		}
		o.mu.Lock()
		o.tableStats[metric] = ts
		o.stats.TotalAnalyzes++
		o.mu.Unlock()
		return ts, nil
	}

	ts := o.computeStatistics(metric, points)

	o.mu.Lock()
	o.tableStats[metric] = ts
	o.stats.TotalAnalyzes++
	o.mu.Unlock()

	return ts, nil
}

// computeStatistics builds TableStatistics from a slice of points.
func (o *CostBasedOptimizer) computeStatistics(metric string, points []Point) *TableStatistics {
	ts := &TableStatistics{
		Metric:           metric,
		RowCount:         int64(len(points)),
		DistinctTagCount: make(map[string]int64),
		MinTimestamp:     points[0].Timestamp,
		MaxTimestamp:     points[0].Timestamp,
		Columns:          make(map[string]*CBOColumnStats),
		LastAnalyzed:     time.Now(),
	}

	// Track distinct tag values per key.
	tagValueSets := make(map[string]map[string]struct{})

	var valueSum float64
	minVal := math.Inf(1)
	maxVal := math.Inf(-1)
	distinctVals := make(map[float64]struct{})

	for i := range points {
		p := &points[i]

		// Timestamp bounds
		if p.Timestamp < ts.MinTimestamp {
			ts.MinTimestamp = p.Timestamp
		}
		if p.Timestamp > ts.MaxTimestamp {
			ts.MaxTimestamp = p.Timestamp
		}

		// Value stats
		valueSum += p.Value
		if p.Value < minVal {
			minVal = p.Value
		}
		if p.Value > maxVal {
			maxVal = p.Value
		}
		distinctVals[p.Value] = struct{}{}

		// Tag cardinality
		for k, v := range p.Tags {
			if tagValueSets[k] == nil {
				tagValueSets[k] = make(map[string]struct{})
			}
			tagValueSets[k][v] = struct{}{}
		}
	}

	for k, vs := range tagValueSets {
		ts.DistinctTagCount[k] = int64(len(vs))
	}

	// Build column stats for the value column.
	valCol := &CBOColumnStats{
		Name:          "value",
		NullCount:     0,
		DistinctCount: int64(len(distinctVals)),
		MinValue:      minVal,
		MaxValue:      maxVal,
		AvgValue:      valueSum / float64(len(points)),
	}
	valCol.Histogram = o.buildHistogram(points)
	ts.Columns["value"] = valCol

	// Build column stats for the timestamp column.
	tsCol := &CBOColumnStats{
		Name:          "timestamp",
		NullCount:     0,
		DistinctCount: int64(len(points)), // Approximate: each point typically has a unique timestamp.
		MinValue:      float64(ts.MinTimestamp),
		MaxValue:      float64(ts.MaxTimestamp),
		AvgValue:      float64(ts.MinTimestamp+ts.MaxTimestamp) / 2.0,
	}
	ts.Columns["timestamp"] = tsCol

	return ts
}
