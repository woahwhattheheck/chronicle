package continuousquery

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"sort"
	"strings"
	"sync"
	"sync/atomic"
	"time"
)

// Start starts the continuous query engine.
func (e *ContinuousQueryEngine) Start() error {

	e.wg.Add(1)
	go e.checkpointLoop()

	if e.config.EnableWatermarks {
		e.wg.Add(1)
		go e.watermarkLoop()
	}

	if e.config.MetricsEnabled {
		e.wg.Add(1)
		go e.metricsLoop()
	}

	return nil
}

// Stop stops the continuous query engine.
func (e *ContinuousQueryEngine) Stop() error {
	e.cancel()

	e.queryMu.Lock()
	for _, q := range e.queries {
		q.cancel()
	}
	e.queryMu.Unlock()

	e.wg.Wait()
	return nil
}

func (e *ContinuousQueryEngine) checkpointLoop() {
	defer e.wg.Done()

	ticker := time.NewTicker(e.config.CheckpointInterval)
	defer ticker.Stop()

	for {
		select {
		case <-e.ctx.Done():
			return
		case <-ticker.C:
			e.triggerCheckpoint()
		}
	}
}

func (e *ContinuousQueryEngine) triggerCheckpoint() {
	e.queryMu.RLock()
	queries := make([]*ContinuousQueryV2, 0, len(e.queries))
	for _, q := range e.queries {
		if q.State == CQStateRunning {
			queries = append(queries, q)
		}
	}
	e.queryMu.RUnlock()

	for _, q := range queries {
		if err := e.checkpointQuery(q); err != nil {
			atomic.AddInt64(&e.metrics.CheckpointsFailed, 1)
		} else {
			atomic.AddInt64(&e.metrics.CheckpointsTotal, 1)
		}
	}
}

func (e *ContinuousQueryEngine) checkpointQuery(q *ContinuousQueryV2) error {
	q.mu.Lock()
	defer q.mu.Unlock()

	if q.State != CQStateRunning {
		return nil
	}

	q.State = CQStateCheckpointing
	defer func() { q.State = CQStateRunning }()

	// Collect state from all operators
	var states [][]byte
	for _, op := range q.operators {
		state, err := op.GetState()
		if err != nil {
			return err
		}
		states = append(states, state)
	}

	checkpoint := &QueryCheckpoint{
		QueryID:     q.ID,
		Timestamp:   time.Now().UnixNano(),
		Watermark:   q.Stats.Watermark,
		States:      states,
		InputOffset: q.Stats.InputRecords,
	}

	data, err := json.Marshal(checkpoint)
	if err != nil {
		return err
	}

	e.stateManager.mu.Lock()
	e.stateManager.states[q.ID] = data
	e.stateManager.mu.Unlock()

	q.LastCheckpoint = time.Now()
	atomic.AddInt64(&q.Stats.CheckpointCount, 1)

	return nil
}

func (e *ContinuousQueryEngine) watermarkLoop() {
	defer e.wg.Done()

	ticker := time.NewTicker(time.Second)
	defer ticker.Stop()

	for {
		select {
		case <-e.ctx.Done():
			return
		case <-ticker.C:
			e.advanceWatermarks()
		}
	}
}

func (e *ContinuousQueryEngine) advanceWatermarks() {
	now := time.Now().UnixNano()
	watermark := now - int64(e.config.WatermarkDelay)

	e.watermarkTracker.mu.Lock()
	for source := range e.watermarkTracker.watermarks {
		e.watermarkTracker.watermarks[source] = watermark
	}
	e.watermarkTracker.mu.Unlock()
}

func (e *ContinuousQueryEngine) metricsLoop() {
	defer e.wg.Done()

	ticker := time.NewTicker(10 * time.Second)
	defer ticker.Stop()

	for {
		select {
		case <-e.ctx.Done():
			return
		case <-ticker.C:
			e.collectMetrics()
		}
	}
}

func (e *ContinuousQueryEngine) collectMetrics() {
	e.queryMu.RLock()
	running := 0
	for _, q := range e.queries {
		if q.State == CQStateRunning {
			running++
		}
	}
	e.queryMu.RUnlock()

	e.metrics.mu.Lock()
	e.metrics.QueriesRunning = int64(running)
	e.metrics.mu.Unlock()
}

// CreateQuery creates a new continuous query.
func (e *ContinuousQueryEngine) CreateQuery(name, sql string, config CQConfig) (*ContinuousQueryV2, error) {
	e.queryMu.Lock()
	if len(e.queries) >= e.config.MaxQueries {
		e.queryMu.Unlock()
		return nil, errors.New("max queries reached")
	}
	e.queryMu.Unlock()

	plan, err := e.parseAndPlan(sql)
	if err != nil {
		return nil, fmt.Errorf("parse error: %w", err)
	}

	if e.config.EnableQueryOptimizer {
		plan = e.optimizer.Optimize(plan)
	}

	ctx, cancel := context.WithCancel(e.ctx)

	query := &ContinuousQueryV2{
		Name:    name,
		SQL:     sql,
		Plan:    plan,
		State:   CQStateCreated,
		Config:  config,
		Created: time.Now(),
		ctx:     ctx,
		cancel:  cancel,
	}

	query.operators, err = e.buildOperators(plan)
	if err != nil {
		cancel()
		return nil, fmt.Errorf("build operators: %w", err)
	}

	e.queryMu.Lock()
	// Planning runs outside the registry lock. Recheck capacity at insertion so
	// simultaneous creates cannot both consume the same remaining slot.
	if len(e.queries) >= e.config.MaxQueries {
		e.queryMu.Unlock()
		cancel()
		return nil, errors.New("max queries reached")
	}
	// Windows clock resolution can give successive creates the same timestamp.
	// Keep IDs monotonic for this engine, including after deletion or clock drift.
	nextID := time.Now().UnixNano()
	if nextID <= e.lastQueryID {
		nextID = e.lastQueryID + 1
	}
	e.lastQueryID = nextID
	query.ID = fmt.Sprintf("cq-%d", nextID)
	e.queries[query.ID] = query
	e.queryMu.Unlock()

	atomic.AddInt64(&e.metrics.QueriesCreated, 1)

	return query, nil
}

// StartQuery starts a continuous query.
func (e *ContinuousQueryEngine) StartQuery(queryID string) error {
	e.queryMu.RLock()
	query, ok := e.queries[queryID]
	e.queryMu.RUnlock()

	if !ok {
		return errors.New("query not found")
	}

	query.mu.Lock()
	if query.State == CQStateRunning {
		query.mu.Unlock()
		return nil
	}
	query.State = CQStateRunning
	query.mu.Unlock()

	for _, op := range query.operators {
		if err := op.Open(query.ctx); err != nil {
			return err
		}
	}

	e.wg.Add(1)
	go e.runQuery(query)

	return nil
}

// StopQuery stops a continuous query.
func (e *ContinuousQueryEngine) StopQuery(queryID string) error {
	e.queryMu.RLock()
	query, ok := e.queries[queryID]
	e.queryMu.RUnlock()

	if !ok {
		return errors.New("query not found")
	}

	query.cancel()
	query.mu.Lock()
	query.State = CQStateStopped
	query.mu.Unlock()

	for _, op := range query.operators {
		op.Close()
	}

	return nil
}

// DeleteQuery deletes a continuous query.
func (e *ContinuousQueryEngine) DeleteQuery(queryID string) error {
	if err := e.StopQuery(queryID); err != nil && err.Error() != "query not found" {
		return err
	}

	e.queryMu.Lock()
	delete(e.queries, queryID)
	e.queryMu.Unlock()

	e.stateManager.mu.Lock()
	delete(e.stateManager.states, queryID)
	e.stateManager.mu.Unlock()

	return nil
}

func (e *ContinuousQueryEngine) runQuery(query *ContinuousQueryV2) {
	defer e.wg.Done()

	sources := query.Plan.Sources
	if len(sources) == 0 {
		return
	}

	if e.bus == nil {
		query.mu.Lock()
		query.State = CQStateFailed
		query.mu.Unlock()
		return
	}

	sub := e.bus.Subscribe(sources[0], nil)
	if sub == nil {
		query.mu.Lock()
		query.State = CQStateFailed
		query.mu.Unlock()
		return
	}
	defer sub.Close()

	// Trigger ticker
	var triggerTicker *time.Ticker
	triggerInterval := time.Second
	if query.Config.Trigger.Interval > 0 {
		triggerInterval = query.Config.Trigger.Interval
	}
	triggerTicker = time.NewTicker(triggerInterval)
	defer triggerTicker.Stop()

	// Pending results buffer
	var pendingResults []*Record
	var pendingMu sync.Mutex

	for {
		select {
		case <-query.ctx.Done():
			return

		case point, ok := <-sub.C():
			if !ok {
				return
			}

			record := &Record{
				Key:       point.Metric,
				Value:     make(map[string]any),
				Timestamp: point.Timestamp,
			}
			record.Value["value"] = point.Value
			record.Value["metric"] = point.Metric
			for k, v := range point.Tags {
				record.Value[k] = v
			}

			results := []*Record{record}
			for _, op := range query.operators {
				var newResults []*Record
				for _, r := range results {
					output, err := op.Process(r)
					if err != nil {
						atomic.AddInt64(&query.Stats.FailureCount, 1)
						continue
					}
					newResults = append(newResults, output...)
				}
				results = newResults
			}

			pendingMu.Lock()
			pendingResults = append(pendingResults, results...)
			pendingMu.Unlock()

			atomic.AddInt64(&query.Stats.InputRecords, 1)
			atomic.AddInt64(&e.metrics.RecordsProcessed, 1)

		case <-triggerTicker.C:

			pendingMu.Lock()
			toEmit := pendingResults
			pendingResults = nil
			pendingMu.Unlock()

			for _, result := range toEmit {
				e.emitResult(query, result)
			}

			query.mu.Lock()
			query.Stats.LastTriggerTime = time.Now()
			query.mu.Unlock()
		}
	}
}

func (e *ContinuousQueryEngine) emitResult(query *ContinuousQueryV2, record *Record) {

	for _, sink := range query.Config.Sinks {
		switch sink.Type {
		case "stream":
			e.emitToStream(sink.Name, record)
		case "view":
			e.updateView(sink.Name, record)
		case "chronicle":
			e.emitToChronicle(record)
		}
	}

	atomic.AddInt64(&query.Stats.OutputRecords, 1)
	atomic.AddInt64(&e.metrics.RecordsEmitted, 1)
}

func (e *ContinuousQueryEngine) emitToStream(streamName string, record *Record) {
	if e.bus == nil {
		return
	}

	point := Point{
		Metric:    streamName,
		Timestamp: record.Timestamp,
		Tags:      make(map[string]string),
	}

	if v, ok := record.Value["value"].(float64); ok {
		point.Value = v
	}

	for k, v := range record.Value {
		if s, ok := v.(string); ok {
			point.Tags[k] = s
		}
	}

	point.Metric = streamName
	e.bus.Publish(point)
}

func (e *ContinuousQueryEngine) updateView(viewName string, record *Record) {
	e.viewMu.RLock()
	view, ok := e.views[viewName]
	e.viewMu.RUnlock()

	if !ok {
		return
	}

	view.mu.Lock()
	defer view.mu.Unlock()

	if view.Data == nil {
		view.Data = &ViewData{
			Rows: make([]map[string]any, 0),
		}
	}

	if view.RefreshMode == RefreshModeIncremental {
		view.Data.Rows = append(view.Data.Rows, record.Value)
		view.Data.RowCount = len(view.Data.Rows)
	}

	view.Data.UpdatedAt = time.Now()
	view.Updated = time.Now()
}

func (e *ContinuousQueryEngine) emitToChronicle(record *Record) {
	if e.pw == nil {
		return
	}

	point := Point{
		Metric:    record.Key,
		Timestamp: record.Timestamp,
		Tags:      make(map[string]string),
	}

	if v, ok := record.Value["value"].(float64); ok {
		point.Value = v
	}

	for k, v := range record.Value {
		if s, ok := v.(string); ok {
			point.Tags[k] = s
		}
	}

	e.pw.WritePoint(point)
}

// parseAndPlan parses SQL and creates an execution plan.
func (e *ContinuousQueryEngine) parseAndPlan(sql string) (*QueryPlan, error) {
	upper := strings.ToUpper(sql)

	plan := &QueryPlan{
		Partitions: e.config.ParallelismHint,
	}

	if fromIdx := strings.Index(upper, "FROM"); fromIdx != -1 {
		remaining := sql[fromIdx+4:]
		parts := strings.Fields(remaining)
		if len(parts) > 0 {
			source := strings.Trim(parts[0], " \t\n,;")
			plan.Sources = []string{source}
		}
	}

	plan.Root = &PlanNode{
		ID:   "sink-0",
		Type: PlanNodeSink,
		Children: []*PlanNode{
			e.buildPlanFromSQL(sql),
		},
	}

	plan.Cost = e.estimateCost(plan)

	return plan, nil
}

func (e *ContinuousQueryEngine) buildPlanFromSQL(sql string) *PlanNode {
	upper := strings.ToUpper(sql)
	var nodes []*PlanNode

	hasAgg := strings.Contains(upper, "SUM(") || strings.Contains(upper, "AVG(") ||
		strings.Contains(upper, "COUNT(") || strings.Contains(upper, "MIN(") ||
		strings.Contains(upper, "MAX(")

	scanNode := &PlanNode{
		ID:         "scan-0",
		Type:       PlanNodeScan,
		Properties: make(map[string]any),
	}
	nodes = append(nodes, scanNode)

	if strings.Contains(upper, "WHERE") {
		filterNode := &PlanNode{
			ID:         "filter-0",
			Type:       PlanNodeFilter,
			Properties: make(map[string]any),
			Children:   []*PlanNode{nodes[len(nodes)-1]},
		}
		nodes = append(nodes, filterNode)
	}

	if strings.Contains(upper, "WINDOW") || strings.Contains(upper, "GROUP BY") {
		windowNode := &PlanNode{
			ID:         "window-0",
			Type:       PlanNodeWindow,
			Properties: make(map[string]any),
			Children:   []*PlanNode{nodes[len(nodes)-1]},
		}
		nodes = append(nodes, windowNode)
	}

	if hasAgg {
		aggNode := &PlanNode{
			ID:         "agg-0",
			Type:       PlanNodeAggregate,
			Properties: make(map[string]any),
			Children:   []*PlanNode{nodes[len(nodes)-1]},
		}
		nodes = append(nodes, aggNode)
	}

	projectNode := &PlanNode{
		ID:         "project-0",
		Type:       PlanNodeProject,
		Properties: make(map[string]any),
		Children:   []*PlanNode{nodes[len(nodes)-1]},
	}
	nodes = append(nodes, projectNode)

	return nodes[len(nodes)-1]
}

func (e *ContinuousQueryEngine) estimateCost(plan *QueryPlan) float64 {
	var cost float64

	var walkPlan func(*PlanNode)
	walkPlan = func(node *PlanNode) {
		if node == nil {
			return
		}

		switch node.Type {
		case PlanNodeScan:
			cost += 1.0
		case PlanNodeFilter:
			cost += 0.5
		case PlanNodeProject:
			cost += 0.3
		case PlanNodeAggregate:
			cost += 2.0
		case PlanNodeJoin:
			cost += 5.0
		case PlanNodeWindow:
			cost += 1.5
		}

		for _, child := range node.Children {
			walkPlan(child)
		}
	}

	walkPlan(plan.Root)
	return cost
}

// buildOperators creates operators from a query plan.
func (e *ContinuousQueryEngine) buildOperators(plan *QueryPlan) ([]CQOperator, error) {
	var operators []CQOperator

	var buildFromNode func(*PlanNode) error
	buildFromNode = func(node *PlanNode) error {
		if node == nil {
			return nil
		}

		for _, child := range node.Children {
			if err := buildFromNode(child); err != nil {
				return err
			}
		}

		// Create operator for this node
		var op CQOperator
		switch node.Type {
		case PlanNodeScan:
			op = &ScanOperator{id: node.ID}
		case PlanNodeFilter:
			op = &FilterOperator{id: node.ID}
		case PlanNodeProject:
			op = &ProjectOperator{id: node.ID}
		case PlanNodeAggregate:
			op = &AggregateOperator{id: node.ID}
		case PlanNodeWindow:
			op = &WindowOperator{id: node.ID}
		case PlanNodeSink:
			op = &SinkOperator{id: node.ID}
		default:
			op = &PassthroughOperator{id: node.ID}
		}

		operators = append(operators, op)
		return nil
	}

	if err := buildFromNode(plan.Root); err != nil {
		return nil, err
	}

	return operators, nil
}

// CreateMaterializedView creates a materialized view.
func (e *ContinuousQueryEngine) CreateMaterializedView(name, sql string, refreshMode RefreshMode) (*MaterializedView, error) {

	config := CQConfig{
		Parallelism: e.config.ParallelismHint,
		OutputMode:  OutputModeUpdate,
		Sinks: []SinkConfig{
			{Type: "view", Name: name},
		},
	}

	query, err := e.CreateQuery(name+"_query", sql, config)
	if err != nil {
		return nil, err
	}

	view := &MaterializedView{
		Name:        name,
		Query:       sql,
		RefreshMode: refreshMode,
		QueryID:     query.ID,
		Created:     time.Now(),
		Data: &ViewData{
			Rows: make([]map[string]any, 0),
		},
	}

	e.viewMu.Lock()
	e.views[name] = view
	e.viewMu.Unlock()

	if err := e.StartQuery(query.ID); err != nil {
		return nil, err
	}

	return view, nil
}

// GetView returns a materialized view by name.
func (e *ContinuousQueryEngine) GetView(name string) (*MaterializedView, bool) {
	e.viewMu.RLock()
	defer e.viewMu.RUnlock()
	v, ok := e.views[name]
	return v, ok
}

// QueryView queries a materialized view.
func (e *ContinuousQueryEngine) QueryView(name string, filter func(map[string]any) bool) ([]map[string]any, error) {
	e.viewMu.RLock()
	view, ok := e.views[name]
	e.viewMu.RUnlock()

	if !ok {
		return nil, errors.New("view not found")
	}

	view.mu.RLock()
	defer view.mu.RUnlock()

	if view.Data == nil {
		return nil, nil
	}

	if filter == nil {
		return view.Data.Rows, nil
	}

	var result []map[string]any
	for _, row := range view.Data.Rows {
		if filter(row) {
			result = append(result, row)
		}
	}

	return result, nil
}

// DropView drops a materialized view.
func (e *ContinuousQueryEngine) DropView(name string) error {
	e.viewMu.Lock()
	view, ok := e.views[name]
	if !ok {
		e.viewMu.Unlock()
		return errors.New("view not found")
	}
	delete(e.views, name)
	e.viewMu.Unlock()

	return e.DeleteQuery(view.QueryID)
}

// GetQuery returns a query by ID.
func (e *ContinuousQueryEngine) GetQuery(queryID string) (*ContinuousQueryV2, bool) {
	e.queryMu.RLock()
	defer e.queryMu.RUnlock()
	q, ok := e.queries[queryID]
	return q, ok
}

// ListQueries returns all queries.
func (e *ContinuousQueryEngine) ListQueries() []*ContinuousQueryV2 {
	e.queryMu.RLock()
	defer e.queryMu.RUnlock()

	queries := make([]*ContinuousQueryV2, 0, len(e.queries))
	for _, q := range e.queries {
		queries = append(queries, q)
	}

	sort.Slice(queries, func(i, j int) bool {
		return queries[i].Created.Before(queries[j].Created)
	})

	return queries
}

// GetStats returns engine statistics.
func (e *ContinuousQueryEngine) GetStats() CQEngineStats {
	e.metrics.mu.RLock()
	defer e.metrics.mu.RUnlock()

	e.queryMu.RLock()
	queryCount := len(e.queries)
	e.queryMu.RUnlock()

	e.viewMu.RLock()
	viewCount := len(e.views)
	e.viewMu.RUnlock()

	return CQEngineStats{
		QueriesTotal:      queryCount,
		QueriesCreated:    e.metrics.QueriesCreated,
		QueriesRunning:    e.metrics.QueriesRunning,
		QueriesFailed:     e.metrics.QueriesFailed,
		ViewsTotal:        viewCount,
		RecordsProcessed:  e.metrics.RecordsProcessed,
		RecordsEmitted:    e.metrics.RecordsEmitted,
		CheckpointsTotal:  e.metrics.CheckpointsTotal,
		CheckpointsFailed: e.metrics.CheckpointsFailed,
	}
}

// CreateStreamJoin creates a stream-stream join.
func (e *ContinuousQueryEngine) CreateStreamJoin(name string, config StreamJoinConfig) (*ContinuousQueryV2, error) {
	sql := fmt.Sprintf(
		"SELECT * FROM %s JOIN %s ON %s.%s = %s.%s WINDOW TUMBLING SIZE %d SECONDS",
		config.LeftStream, config.RightStream,
		config.LeftStream, config.JoinKey,
		config.RightStream, config.JoinKey,
		int(config.Window.Seconds()),
	)

	return e.CreateQuery(name, sql, CQConfig{
		Parallelism: e.config.ParallelismHint,
		OutputMode:  OutputModeUpdate,
	})
}

// CreatePartitionedQuery creates a partitioned query.
func (e *ContinuousQueryEngine) CreatePartitionedQuery(name, sql string, numPartitions int) (*PartitionedQuery, error) {
	query, err := e.CreateQuery(name, sql, CQConfig{
		Parallelism: numPartitions,
	})
	if err != nil {
		return nil, err
	}

	partitions := make([]*QueryPartition, numPartitions)
	rangeSize := uint64(1<<64-1) / uint64(numPartitions)

	for i := 0; i < numPartitions; i++ {
		partitions[i] = &QueryPartition{
			ID: i,
			KeyRange: KeyRange{
				Start: uint64(i) * rangeSize,
				End:   uint64(i+1) * rangeSize,
			},
		}
	}

	return &PartitionedQuery{
		ContinuousQueryV2: query,
		Partitions:        partitions,
	}, nil
}
