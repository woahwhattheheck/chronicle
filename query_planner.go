package chronicle

import (
	"context"
	"fmt"
	"math"
	"sort"
	"strings"
	"sync"
	"sync/atomic"
	"time"
)

// QQPlanNodeType represents the type of a query plan node.
type QQPlanNodeType string

const (
	QPlanScan          QQPlanNodeType = "scan"
	QPlanFilter        QQPlanNodeType = "filter"
	QPlanAggregate     QQPlanNodeType = "aggregate"
	QPlanSort          QQPlanNodeType = "sort"
	QPlanLimit         QQPlanNodeType = "limit"
	QPlanMerge         QQPlanNodeType = "merge"
	QPlanPartitionScan QQPlanNodeType = "partition_scan"
)

// QueryPlannerConfig configures the adaptive query planner.
type QueryPlannerConfig struct {
	Enabled                 bool          `json:"enabled"`
	StatsRefreshInterval    time.Duration `json:"stats_refresh_interval"`
	MaxParallelScans        int           `json:"max_parallel_scans"`
	EnablePartitionPruning  bool          `json:"enable_partition_pruning"`
	EnablePredicatePushdown bool          `json:"enable_predicate_pushdown"`
	EnableParallelExec      bool          `json:"enable_parallel_exec"`
	CostModelWeight         float64       `json:"cost_model_weight"`
	MaxStatsAge             time.Duration `json:"max_stats_age"`
	MinRowsForParallel      int           `json:"min_rows_for_parallel"`
	StaleStatsThreshold     float64       `json:"stale_stats_threshold"`
}

// DefaultQueryPlannerConfig returns sensible defaults.
func DefaultQueryPlannerConfig() QueryPlannerConfig {
	return QueryPlannerConfig{
		Enabled:                 true,
		StatsRefreshInterval:    5 * time.Minute,
		MaxParallelScans:        4,
		EnablePartitionPruning:  true,
		EnablePredicatePushdown: true,
		EnableParallelExec:      true,
		CostModelWeight:         1.0,
		MaxStatsAge:             10 * time.Minute,
		MinRowsForParallel:      10000,
		StaleStatsThreshold:     0.2,
	}
}

// PartitionStats holds runtime statistics for a single partition.
type PartitionStats struct {
	ID             string    `json:"id"`
	MinTime        int64     `json:"min_time"`
	MaxTime        int64     `json:"max_time"`
	RowCount       int64     `json:"row_count"`
	ByteSize       int64     `json:"byte_size"`
	Metrics        []string  `json:"metrics"`
	CardinalityEst int       `json:"cardinality_est"`
	LastUpdated    time.Time `json:"last_updated"`
}

// QueryStats aggregates statistics about the query engine's data.
type QueryStats struct {
	TotalPartitions   int              `json:"total_partitions"`
	TotalRows         int64            `json:"total_rows"`
	TotalBytes        int64            `json:"total_bytes"`
	PartitionStats    []PartitionStats `json:"partition_stats"`
	MetricCardinality map[string]int   `json:"metric_cardinality"`
	CollectedAt       time.Time        `json:"collected_at"`
}

// QPlanNode represents a single node in the query execution plan.
type QPlanNode struct {
	Type          QQPlanNodeType `json:"type"`
	Description   string         `json:"description"`
	EstimatedRows int64          `json:"estimated_rows"`
	EstimatedCost float64        `json:"estimated_cost"`
	Children      []*QPlanNode   `json:"children,omitempty"`
	Properties    map[string]any `json:"properties,omitempty"`
}

// CostQueryPlan represents the full execution plan for a query.
type CostQueryPlan struct {
	Root             *QPlanNode `json:"root"`
	TotalCost        float64    `json:"total_cost"`
	EstimatedRows    int64      `json:"estimated_rows"`
	PartitionsUsed   int        `json:"partitions_used"`
	PartitionsPruned int        `json:"partitions_pruned"`
	ParallelDegree   int        `json:"parallel_degree"`
	Optimizations    []string   `json:"optimizations"`
	PlanningTime     string     `json:"planning_time"`
}

// PlannerStats tracks query planner usage statistics.
type PlannerStats struct {
	QueriesPlanned    uint64  `json:"queries_planned"`
	PartitionsPruned  uint64  `json:"partitions_pruned"`
	PredicatesPushed  uint64  `json:"predicates_pushed"`
	ParallelQueries   uint64  `json:"parallel_queries"`
	AvgPlanningTimeUs float64 `json:"avg_planning_time_us"`
	CacheHits         uint64  `json:"cache_hits"`
	StatsRefreshes    uint64  `json:"stats_refreshes"`
}

// QueryPlanner provides cost-based query optimization with runtime statistics.
type QueryPlanner struct {
	config     QueryPlannerConfig
	db         *DB
	queryStats atomic.Value // stores *QueryStats
	// Usage counters and their planning-time total are protected by mu.
	plannerStats    PlannerStats
	totalPlanTimeUs int64

	mu   sync.RWMutex
	done chan struct{}
}

// NewQueryPlanner creates a new adaptive query planner.
func NewQueryPlanner(db *DB, config QueryPlannerConfig) *QueryPlanner {
	qp := &QueryPlanner{
		config: config,
		db:     db,
		done:   make(chan struct{}),
	}
	qp.queryStats.Store(&QueryStats{})
	return qp
}

// Start begins the background statistics collection loop.
func (qp *QueryPlanner) Start() {
	if !qp.config.Enabled {
		return
	}
	qp.RefreshStats()
	go qp.statsLoop()
}

// Stop halts the background statistics collection.
func (qp *QueryPlanner) Stop() {
	select {
	case <-qp.done:
	default:
		close(qp.done)
	}
}

// Plan generates an optimized execution plan for a query.
func (qp *QueryPlanner) Plan(ctx context.Context, q *Query) (*CostQueryPlan, error) {
	if q == nil {
		return nil, fmt.Errorf("nil query")
	}
	start := time.Now()
	defer func() {
		elapsed := time.Since(start).Microseconds()
		qp.mu.Lock()
		qp.totalPlanTimeUs += elapsed
		qp.plannerStats.QueriesPlanned++
		total := qp.plannerStats.QueriesPlanned
		if total > 0 {
			qp.plannerStats.AvgPlanningTimeUs = float64(qp.totalPlanTimeUs) / float64(total)
		}
		qp.mu.Unlock()
	}()

	stats := qp.GetStats()
	plan := &CostQueryPlan{
		Optimizations: make([]string, 0),
	}

	// Phase 1: Identify eligible partitions
	eligible := qp.findEligiblePartitions(q, stats)
	totalPartitions := len(stats.PartitionStats)
	plan.PartitionsUsed = len(eligible)
	plan.PartitionsPruned = totalPartitions - len(eligible)

	if plan.PartitionsPruned > 0 && qp.config.EnablePartitionPruning {
		plan.Optimizations = append(plan.Optimizations, fmt.Sprintf("partition_pruning: %d/%d pruned", plan.PartitionsPruned, totalPartitions))
		qp.mu.Lock()
		qp.plannerStats.PartitionsPruned += uint64(plan.PartitionsPruned)
		qp.mu.Unlock()
	}

	// Phase 2: Build plan tree
	var scanNodes []*QPlanNode
	for _, ps := range eligible {
		scanNodes = append(scanNodes, &QPlanNode{
			Type:          QPlanPartitionScan,
			Description:   fmt.Sprintf("Scan partition %s [%d rows]", ps.ID, ps.RowCount),
			EstimatedRows: ps.RowCount,
			EstimatedCost: float64(ps.RowCount) * qp.config.CostModelWeight * 0.001,
			Properties:    map[string]any{"partition_id": ps.ID, "byte_size": ps.ByteSize},
		})
	}

	var root *QPlanNode
	if len(scanNodes) > 1 {
		root = &QPlanNode{
			Type:        QPlanMerge,
			Description: fmt.Sprintf("Merge %d partitions", len(scanNodes)),
			Children:    scanNodes,
		}
	} else if len(scanNodes) == 1 {
		root = scanNodes[0]
	} else {
		root = &QPlanNode{Type: QPlanScan, Description: "Empty scan", EstimatedRows: 0}
	}

	// Phase 3: Apply predicate pushdown
	if qp.config.EnablePredicatePushdown && q.Metric != "" {
		filterNode := &QPlanNode{
			Type:        QPlanFilter,
			Description: fmt.Sprintf("Filter metric=%s", q.Metric),
			Children:    []*QPlanNode{root},
			Properties:  map[string]any{"predicate": "metric=" + q.Metric},
		}
		root = filterNode
		plan.Optimizations = append(plan.Optimizations, "predicate_pushdown: metric filter")
		qp.mu.Lock()
		qp.plannerStats.PredicatesPushed++
		qp.mu.Unlock()
	}

	if len(q.Tags) > 0 && qp.config.EnablePredicatePushdown {
		tagFilters := make([]string, 0, len(q.Tags))
		for k, v := range q.Tags {
			tagFilters = append(tagFilters, fmt.Sprintf("%s=%s", k, v))
		}
		filterNode := &QPlanNode{
			Type:        QPlanFilter,
			Description: fmt.Sprintf("Filter tags: %s", strings.Join(tagFilters, ", ")),
			Children:    []*QPlanNode{root},
			Properties:  map[string]any{"tag_filters": tagFilters},
		}
		root = filterNode
		plan.Optimizations = append(plan.Optimizations, "predicate_pushdown: tag filters")
		qp.mu.Lock()
		qp.plannerStats.PredicatesPushed++
		qp.mu.Unlock()
	}

	// Phase 4: Aggregation
	if q.Aggregation != nil {
		aggNode := &QPlanNode{
			Type:        QPlanAggregate,
			Description: fmt.Sprintf("Aggregate func=%d window=%s", q.Aggregation.Function, q.Aggregation.Window),
			Children:    []*QPlanNode{root},
			Properties:  map[string]any{"function": q.Aggregation.Function, "window": q.Aggregation.Window.String()},
		}
		root = aggNode
	}

	// Phase 5: Limit
	if q.Limit > 0 {
		limitNode := &QPlanNode{
			Type:        QPlanLimit,
			Description: fmt.Sprintf("Limit %d", q.Limit),
			Children:    []*QPlanNode{root},
			Properties:  map[string]any{"limit": q.Limit},
		}
		root = limitNode
	}

	// Phase 6: Determine parallelism
	totalRows := qp.estimateTotalRows(eligible)
	if qp.config.EnableParallelExec && totalRows >= int64(qp.config.MinRowsForParallel) && len(eligible) > 1 {
		plan.ParallelDegree = int(math.Min(float64(qp.config.MaxParallelScans), float64(len(eligible))))
		plan.Optimizations = append(plan.Optimizations, fmt.Sprintf("parallel_scan: degree=%d", plan.ParallelDegree))
		qp.mu.Lock()
		qp.plannerStats.ParallelQueries++
		qp.mu.Unlock()
	} else {
		plan.ParallelDegree = 1
	}

	// Calculate total cost
	plan.Root = root
	plan.TotalCost = qp.calculatePlanCost(root)
	plan.EstimatedRows = totalRows
	plan.PlanningTime = time.Since(start).String()

	return plan, nil
}

// Explain returns a human-readable query plan explanation.
func (qp *QueryPlanner) Explain(ctx context.Context, q *Query) (string, error) {
	plan, err := qp.Plan(ctx, q)
	if err != nil {
		return "", err
	}
	var sb strings.Builder
	sb.WriteString("Query Plan:\n")
	sb.WriteString(fmt.Sprintf("  Total Cost: %.2f\n", plan.TotalCost))
	sb.WriteString(fmt.Sprintf("  Estimated Rows: %d\n", plan.EstimatedRows))
	sb.WriteString(fmt.Sprintf("  Partitions Used: %d (pruned %d)\n", plan.PartitionsUsed, plan.PartitionsPruned))
	sb.WriteString(fmt.Sprintf("  Parallel Degree: %d\n", plan.ParallelDegree))
	sb.WriteString(fmt.Sprintf("  Planning Time: %s\n", plan.PlanningTime))
	if len(plan.Optimizations) > 0 {
		sb.WriteString("  Optimizations:\n")
		for _, opt := range plan.Optimizations {
			sb.WriteString(fmt.Sprintf("    - %s\n", opt))
		}
	}
	sb.WriteString("\nExecution Tree:\n")
	qp.formatQPlanNode(&sb, plan.Root, 0)
	return sb.String(), nil
}

// RefreshStats collects current statistics from the database.
func (qp *QueryPlanner) RefreshStats() {
	stats := &QueryStats{
		PartitionStats:    make([]PartitionStats, 0),
		MetricCardinality: make(map[string]int),
		CollectedAt:       time.Now(),
	}

	if qp.db != nil && qp.db.index != nil {
		qp.db.index.mu.RLock()
		partitions := qp.db.index.partitions
		for i, p := range partitions {
			ps := PartitionStats{
				ID:          fmt.Sprintf("p%d", i),
				MinTime:     p.MinTime(),
				MaxTime:     p.MaxTime(),
				RowCount:    p.PointCount(),
				LastUpdated: time.Now(),
			}
			ps.ByteSize = ps.RowCount * 24 // approximate: 8 bytes timestamp + 8 bytes value + 8 bytes overhead
			metrics := make(map[string]bool)
			for key := range p.SeriesData() {
				// Series key includes metric name as prefix before tag separator
				parts := strings.SplitN(key, ",", 2)
				if len(parts) > 0 {
					metrics[parts[0]] = true
				}
			}
			for m := range metrics {
				ps.Metrics = append(ps.Metrics, m)
				stats.MetricCardinality[m]++
			}
			ps.CardinalityEst = len(metrics)
			stats.TotalRows += ps.RowCount
			stats.TotalBytes += ps.ByteSize
			stats.PartitionStats = append(stats.PartitionStats, ps)
		}
		stats.TotalPartitions = len(partitions)
		qp.db.index.mu.RUnlock()
	}

	qp.queryStats.Store(stats)
	qp.mu.Lock()
	qp.plannerStats.StatsRefreshes++
	qp.mu.Unlock()
}

// GetStats returns the current query statistics.
func (qp *QueryPlanner) GetStats() *QueryStats {
	if v, ok := qp.queryStats.Load().(*QueryStats); ok {
		return v
	}
	return &QueryStats{}
}

// GetPlannerStats returns planner usage statistics.
func (qp *QueryPlanner) GetPlannerStats() PlannerStats {
	qp.mu.RLock()
	defer qp.mu.RUnlock()
	return qp.plannerStats
}

func (qp *QueryPlanner) statsLoop() {
	ticker := time.NewTicker(qp.config.StatsRefreshInterval)
	defer ticker.Stop()
	for {
		select {
		case <-qp.done:
			return
		case <-ticker.C:
			qp.RefreshStats()
		}
	}
}

func (qp *QueryPlanner) findEligiblePartitions(q *Query, stats *QueryStats) []PartitionStats {
	if !qp.config.EnablePartitionPruning {
		return stats.PartitionStats
	}

	eligible := make([]PartitionStats, 0, len(stats.PartitionStats))
	for _, ps := range stats.PartitionStats {
		// Time-based pruning
		if q.Start > 0 && ps.MaxTime < q.Start {
			continue
		}
		if q.End > 0 && ps.MinTime > q.End {
			continue
		}

		// Metric-based pruning
		if q.Metric != "" && len(ps.Metrics) > 0 {
			found := false
			for _, m := range ps.Metrics {
				if m == q.Metric {
					found = true
					break
				}
			}
			if !found {
				continue
			}
		}

		eligible = append(eligible, ps)
	}

	// Sort by time order
	sort.Slice(eligible, func(i, j int) bool {
		return eligible[i].MinTime < eligible[j].MinTime
	})

	return eligible
}

func (qp *QueryPlanner) estimateTotalRows(partitions []PartitionStats) int64 {
	var total int64
	for _, p := range partitions {
		total += p.RowCount
	}
	return total
}

func (qp *QueryPlanner) calculatePlanCost(node *QPlanNode) float64 {
	if node == nil {
		return 0
	}
	cost := node.EstimatedCost
	for _, child := range node.Children {
		cost += qp.calculatePlanCost(child)
	}
	return cost
}

func (qp *QueryPlanner) formatQPlanNode(sb *strings.Builder, node *QPlanNode, depth int) {
	if node == nil {
		return
	}
	indent := strings.Repeat("  ", depth)
	sb.WriteString(fmt.Sprintf("%s-> %s: %s (est. rows=%d, cost=%.2f)\n", indent, node.Type, node.Description, node.EstimatedRows, node.EstimatedCost))
	for _, child := range node.Children {
		qp.formatQPlanNode(sb, child, depth+1)
	}
}
