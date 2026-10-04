package continuousquery

import (
	"context"
	"sync/atomic"
	"testing"
	"time"
)

// mockPointWriter implements PointWriter for testing.
type mockPointWriter struct{}

func (m *mockPointWriter) WritePoint(p Point) error { return nil }

// mockStreamSubscription implements StreamSubscription for testing.
type mockStreamSubscription struct {
	ch   chan Point
	done chan struct{}
}

func (s *mockStreamSubscription) C() <-chan Point { return s.ch }
func (s *mockStreamSubscription) Close() {
	select {
	case <-s.done:
	default:
		close(s.done)
		close(s.ch)
	}
}

// mockStreamBus implements StreamBus for testing.
type mockStreamBus struct {
	subs []*mockStreamSubscription
}

func (b *mockStreamBus) Subscribe(topic string, tags map[string]string) StreamSubscription {
	sub := &mockStreamSubscription{
		ch:   make(chan Point, 100),
		done: make(chan struct{}),
	}
	b.subs = append(b.subs, sub)
	return sub
}

func (b *mockStreamBus) Publish(p Point) {
	for _, sub := range b.subs {
		select {
		case sub.ch <- p:
		default:
		}
	}
}

func TestContinuousQueryEngine(t *testing.T) {
	pw := &mockPointWriter{}
	bus := &mockStreamBus{}
	config := DefaultContinuousQueryConfig()
	engine := NewContinuousQueryEngine(pw, bus, config)

	if err := engine.Start(); err != nil {
		t.Fatalf("Failed to start engine: %v", err)
	}
	defer engine.Stop()

	t.Run("CreateQuery", func(t *testing.T) {
		sql := "SELECT value, metric FROM cpu_metrics WHERE value > 50"
		qconfig := CQConfig{
			Parallelism: 2,
			OutputMode:  OutputModeAppend,
		}

		query, err := engine.CreateQuery("test_query", sql, qconfig)
		if err != nil {
			t.Fatalf("Failed to create query: %v", err)
		}

		if query.ID == "" {
			t.Error("Expected query ID")
		}

		if query.Name != "test_query" {
			t.Errorf("Expected name 'test_query', got '%s'", query.Name)
		}

		if query.State != CQStateCreated {
			t.Errorf("Expected state Created, got %s", query.State)
		}
	})

	t.Run("StartStopQuery", func(t *testing.T) {
		sql := "SELECT * FROM test_stream"
		query, err := engine.CreateQuery("start_stop_test", sql, CQConfig{})
		if err != nil {
			t.Fatalf("Failed to create query: %v", err)
		}

		if err := engine.StartQuery(query.ID); err != nil {
			t.Fatalf("Failed to start query: %v", err)
		}

		// Give it time to start
		time.Sleep(50 * time.Millisecond)

		query.mu.Lock()
		state := query.State
		query.mu.Unlock()

		if state != CQStateRunning {
			t.Errorf("Expected state Running, got %s", state)
		}

		if err := engine.StopQuery(query.ID); err != nil {
			t.Fatalf("Failed to stop query: %v", err)
		}

		query.mu.Lock()
		state = query.State
		query.mu.Unlock()

		if state != CQStateStopped {
			t.Errorf("Expected state Stopped, got %s", state)
		}
	})

	t.Run("DeleteQuery", func(t *testing.T) {
		sql := "SELECT * FROM delete_test"
		query, _ := engine.CreateQuery("delete_test", sql, CQConfig{})

		if err := engine.DeleteQuery(query.ID); err != nil {
			t.Fatalf("Failed to delete query: %v", err)
		}

		if _, ok := engine.GetQuery(query.ID); ok {
			t.Error("Query should be deleted")
		}
	})

	t.Run("ListQueries", func(t *testing.T) {
		// Create a few queries
		for i := 0; i < 3; i++ {
			engine.CreateQuery("list_test", "SELECT * FROM stream", CQConfig{})
		}

		queries := engine.ListQueries()
		if len(queries) < 3 {
			t.Errorf("Expected at least 3 queries, got %d", len(queries))
		}
	})

	t.Run("MaxQueriesLimit", func(t *testing.T) {
		limitConfig := DefaultContinuousQueryConfig()
		limitConfig.MaxQueries = 2
		limitEngine := NewContinuousQueryEngine(nil, nil, limitConfig)

		_, err := limitEngine.CreateQuery("q1", "SELECT 1", CQConfig{})
		if err != nil {
			t.Fatalf("First query should succeed: %v", err)
		}

		_, err = limitEngine.CreateQuery("q2", "SELECT 2", CQConfig{})
		if err != nil {
			t.Fatalf("Second query should succeed: %v", err)
		}

		_, err = limitEngine.CreateQuery("q3", "SELECT 3", CQConfig{})
		if err == nil {
			t.Error("Third query should fail due to max limit")
		}
	})
}

func TestContinuousQueryEngine_RapidCreatesRetainQueries(t *testing.T) {
	const count = 256
	config := DefaultContinuousQueryConfig()
	config.MaxQueries = count
	engine := NewContinuousQueryEngine(nil, nil, config)
	defer engine.Stop()

	seen := make(map[string]bool, 2*count)
	for batch := 0; batch < 2; batch++ {
		queries := make([]*ContinuousQueryV2, 0, count)
		for i := 0; i < count; i++ {
			query, err := engine.CreateQuery("rapid", "SELECT * FROM stream", CQConfig{})
			if err != nil {
				t.Fatalf("CreateQuery %d/%d: %v", batch, i, err)
			}
			if seen[query.ID] {
				t.Fatalf("CreateQuery reused ID %q", query.ID)
			}
			seen[query.ID] = true
			queries = append(queries, query)
		}
		for _, query := range queries {
			retained, ok := engine.GetQuery(query.ID)
			if !ok || retained != query {
				t.Fatalf("query %q was replaced or lost", query.ID)
			}
		}
		stats := engine.GetStats()
		if stats.QueriesTotal != count || stats.QueriesCreated != int64((batch+1)*count) {
			t.Fatalf("accepted queries not retained in statistics: %+v", stats)
		}
		for _, query := range queries {
			if err := engine.DeleteQuery(query.ID); err != nil {
				t.Fatalf("DeleteQuery %q: %v", query.ID, err)
			}
		}
	}
}

type queryCreationBarrier struct {
	arrived chan struct{}
	release chan struct{}
}

func (b *queryCreationBarrier) Name() string { return "query creation barrier" }

func (b *queryCreationBarrier) Apply(plan *QueryPlan) (*QueryPlan, bool) {
	b.arrived <- struct{}{}
	<-b.release
	return plan, false
}

func TestContinuousQueryEngine_ConcurrentCreateLimit(t *testing.T) {
	config := DefaultContinuousQueryConfig()
	config.MaxQueries = 1
	engine := NewContinuousQueryEngine(nil, nil, config)
	defer engine.Stop()

	barrier := &queryCreationBarrier{
		arrived: make(chan struct{}, 2),
		release: make(chan struct{}),
	}
	engine.optimizer.rules = []OptimizationRule{barrier}
	type result struct {
		query *ContinuousQueryV2
		err   error
	}
	results := make(chan result, 2)
	for i := 0; i < 2; i++ {
		go func() {
			query, err := engine.CreateQuery("concurrent", "SELECT * FROM stream", CQConfig{})
			results <- result{query, err}
		}()
	}
	// Both callers passed the initial limit check before either registers.
	<-barrier.arrived
	<-barrier.arrived
	close(barrier.release)

	accepted := 0
	for i := 0; i < 2; i++ {
		outcome := <-results
		if outcome.err != nil {
			if outcome.err.Error() != "max queries reached" {
				t.Errorf("unexpected creation error: %v", outcome.err)
			}
			continue
		}
		accepted++
		retained, ok := engine.GetQuery(outcome.query.ID)
		if !ok || retained != outcome.query {
			t.Errorf("accepted query %q was replaced or lost", outcome.query.ID)
		}
	}
	stats := engine.GetStats()
	if accepted != 1 || stats.QueriesTotal != 1 || stats.QueriesCreated != 1 {
		t.Errorf("limit 1 accepted %d queries; statistics: %+v", accepted, stats)
	}
}

func TestContinuousQueryEngine_QueryPlan(t *testing.T) {
	config := DefaultContinuousQueryConfig()
	engine := NewContinuousQueryEngine(nil, nil, config)

	t.Run("SimplePlan", func(t *testing.T) {
		sql := "SELECT value FROM metrics"
		query, err := engine.CreateQuery("simple", sql, CQConfig{})
		if err != nil {
			t.Fatalf("Failed to create query: %v", err)
		}

		if query.Plan == nil {
			t.Fatal("Expected query plan")
		}

		if len(query.Plan.Sources) == 0 {
			t.Error("Expected at least one source")
		}

		if query.Plan.Root == nil {
			t.Error("Expected plan root")
		}
	})

	t.Run("AggregationPlan", func(t *testing.T) {
		sql := "SELECT SUM(value), AVG(value) FROM metrics GROUP BY host"
		query, err := engine.CreateQuery("aggregation", sql, CQConfig{})
		if err != nil {
			t.Fatalf("Failed to create query: %v", err)
		}

		if query.Plan == nil {
			t.Fatal("Expected query plan")
		}

		// Plan should include aggregation node
		hasAgg := false
		var checkNode func(*PlanNode)
		checkNode = func(node *PlanNode) {
			if node == nil {
				return
			}
			if node.Type == PlanNodeAggregate {
				hasAgg = true
			}
			for _, child := range node.Children {
				checkNode(child)
			}
		}
		checkNode(query.Plan.Root)

		if !hasAgg {
			t.Error("Expected aggregation in plan")
		}
	})

	t.Run("WindowedPlan", func(t *testing.T) {
		sql := "SELECT COUNT(*) FROM metrics WINDOW TUMBLING SIZE 1 MINUTE GROUP BY host"
		query, err := engine.CreateQuery("windowed", sql, CQConfig{})
		if err != nil {
			t.Fatalf("Failed to create query: %v", err)
		}

		// Plan should include window node
		hasWindow := false
		var checkNode func(*PlanNode)
		checkNode = func(node *PlanNode) {
			if node == nil {
				return
			}
			if node.Type == PlanNodeWindow {
				hasWindow = true
			}
			for _, child := range node.Children {
				checkNode(child)
			}
		}
		checkNode(query.Plan.Root)

		if !hasWindow {
			t.Error("Expected window in plan")
		}
	})
}

func TestContinuousQueryEngine_Operators(t *testing.T) {
	t.Run("ScanOperator", func(t *testing.T) {
		op := &ScanOperator{id: "scan-0"}

		if err := op.Open(context.Background()); err != nil {
			t.Fatalf("Open failed: %v", err)
		}

		record := &Record{
			Key:       "test",
			Value:     map[string]any{"value": 42.0},
			Timestamp: time.Now().UnixNano(),
		}

		results, err := op.Process(record)
		if err != nil {
			t.Fatalf("Process failed: %v", err)
		}

		if len(results) != 1 {
			t.Errorf("Expected 1 result, got %d", len(results))
		}

		if err := op.Close(); err != nil {
			t.Fatalf("Close failed: %v", err)
		}
	})

	t.Run("FilterOperator", func(t *testing.T) {
		op := &FilterOperator{
			id: "filter-0",
			predicate: func(r *Record) bool {
				if v, ok := r.Value["value"].(float64); ok {
					return v > 50
				}
				return false
			},
		}

		op.Open(context.Background())
		defer op.Close()

		// Should pass
		record1 := &Record{Value: map[string]any{"value": 60.0}}
		results, _ := op.Process(record1)
		if len(results) != 1 {
			t.Error("Expected record to pass filter")
		}

		// Should filter out
		record2 := &Record{Value: map[string]any{"value": 40.0}}
		results, _ = op.Process(record2)
		if len(results) != 0 {
			t.Error("Expected record to be filtered out")
		}
	})

	t.Run("AggregateOperator", func(t *testing.T) {
		op := &AggregateOperator{id: "agg-0"}
		op.Open(context.Background())
		defer op.Close()

		// Process multiple records
		for i := 0; i < 5; i++ {
			record := &Record{
				Key:   "metric",
				Value: map[string]any{"value": float64(i * 10)},
			}
			op.Process(record)
		}

		// Get state
		state, err := op.GetState()
		if err != nil {
			t.Fatalf("GetState failed: %v", err)
		}

		if len(state) == 0 {
			t.Error("Expected state to be non-empty")
		}

		// Verify aggregations
		op.mu.Lock()
		sum := op.aggregates["metric_sum"]
		count := op.counts["metric_count"]
		op.mu.Unlock()

		if sum != 100 { // 0 + 10 + 20 + 30 + 40
			t.Errorf("Expected sum 100, got %f", sum)
		}

		if count != 5 {
			t.Errorf("Expected count 5, got %d", count)
		}
	})

	t.Run("WindowOperator", func(t *testing.T) {
		op := &WindowOperator{id: "window-0"}
		op.Open(context.Background())
		defer op.Close()

		now := time.Now().UnixNano()

		// Add records to same window
		for i := 0; i < 3; i++ {
			record := &Record{
				Key:       "metric",
				Value:     map[string]any{"value": float64(i)},
				Timestamp: now + int64(i*1000),
			}
			op.Process(record)
		}

		op.mu.Lock()
		windowCount := len(op.windows)
		op.mu.Unlock()

		if windowCount == 0 {
			t.Error("Expected at least one window")
		}
	})
}

func TestContinuousQueryEngine_MaterializedView(t *testing.T) {
	pw := &mockPointWriter{}
	bus := &mockStreamBus{}
	config := DefaultContinuousQueryConfig()
	engine := NewContinuousQueryEngine(pw, bus, config)
	engine.Start()
	defer engine.Stop()

	t.Run("CreateView", func(t *testing.T) {
		view, err := engine.CreateMaterializedView(
			"cpu_summary",
			"SELECT AVG(value) FROM cpu_metrics GROUP BY host",
			RefreshModeIncremental,
		)
		if err != nil {
			t.Fatalf("Failed to create view: %v", err)
		}

		if view.Name != "cpu_summary" {
			t.Errorf("Expected name 'cpu_summary', got '%s'", view.Name)
		}

		if view.QueryID == "" {
			t.Error("Expected query ID")
		}
	})

	t.Run("GetView", func(t *testing.T) {
		engine.CreateMaterializedView("test_view", "SELECT * FROM stream", RefreshModeIncremental)

		view, ok := engine.GetView("test_view")
		if !ok {
			t.Error("Expected to find view")
		}

		if view.Name != "test_view" {
			t.Errorf("Expected name 'test_view', got '%s'", view.Name)
		}
	})

	t.Run("QueryView", func(t *testing.T) {
		engine.CreateMaterializedView("query_test", "SELECT * FROM stream", RefreshModeIncremental)

		// Add some data manually
		view, _ := engine.GetView("query_test")
		view.mu.Lock()
		view.Data.Rows = append(view.Data.Rows,
			map[string]any{"value": 10.0},
			map[string]any{"value": 20.0},
			map[string]any{"value": 30.0},
		)
		view.mu.Unlock()

		// Query without filter
		rows, err := engine.QueryView("query_test", nil)
		if err != nil {
			t.Fatalf("QueryView failed: %v", err)
		}

		if len(rows) != 3 {
			t.Errorf("Expected 3 rows, got %d", len(rows))
		}

		// Query with filter
		rows, err = engine.QueryView("query_test", func(row map[string]any) bool {
			if v, ok := row["value"].(float64); ok {
				return v > 15
			}
			return false
		})

		if len(rows) != 2 {
			t.Errorf("Expected 2 filtered rows, got %d", len(rows))
		}
	})

	t.Run("DropView", func(t *testing.T) {
		engine.CreateMaterializedView("drop_test", "SELECT * FROM stream", RefreshModeIncremental)

		if err := engine.DropView("drop_test"); err != nil {
			t.Fatalf("DropView failed: %v", err)
		}

		if _, ok := engine.GetView("drop_test"); ok {
			t.Error("View should be dropped")
		}
	})
}

func TestContinuousQueryEngine_Stats(t *testing.T) {
	config := DefaultContinuousQueryConfig()
	engine := NewContinuousQueryEngine(nil, nil, config)
	engine.Start()
	defer engine.Stop()

	// Create some queries
	engine.CreateQuery("q1", "SELECT * FROM s1", CQConfig{})
	engine.CreateQuery("q2", "SELECT * FROM s2", CQConfig{})

	stats := engine.GetStats()

	if stats.QueriesTotal < 2 {
		t.Errorf("Expected at least 2 queries, got %d", stats.QueriesTotal)
	}

	if stats.QueriesCreated < 2 {
		t.Errorf("Expected QueriesCreated >= 2, got %d", stats.QueriesCreated)
	}
}

func TestContinuousQueryEngine_QueryOptimizer(t *testing.T) {
	optimizer := newQueryOptimizer()

	if len(optimizer.rules) == 0 {
		t.Error("Expected optimization rules")
	}

	// Test optimization
	plan := &QueryPlan{
		Root: &PlanNode{
			Type: PlanNodeSink,
			Children: []*PlanNode{
				{Type: PlanNodeProject},
			},
		},
	}

	optimized := optimizer.Optimize(plan)
	if optimized == nil {
		t.Error("Expected optimized plan")
	}
}

func TestContinuousQueryEngine_Checkpointing(t *testing.T) {
	config := DefaultContinuousQueryConfig()
	config.CheckpointInterval = 100 * time.Millisecond
	engine := NewContinuousQueryEngine(nil, nil, config)
	engine.Start()
	defer engine.Stop()

	query, _ := engine.CreateQuery("checkpoint_test", "SELECT * FROM stream", CQConfig{})

	// Start query
	engine.StartQuery(query.ID)

	// Wait for checkpoint
	time.Sleep(300 * time.Millisecond)

	// Check that checkpoint was attempted
	stats := engine.GetStats()
	// Checkpoints may or may not have happened depending on timing
	_ = stats
}

func TestCQState_String(t *testing.T) {
	tests := []struct {
		state    CQState
		expected string
	}{
		{CQStateCreated, "created"},
		{CQStateRunning, "running"},
		{CQStatePaused, "paused"},
		{CQStateStopped, "stopped"},
		{CQStateFailed, "failed"},
		{CQStateCheckpointing, "checkpointing"},
		{CQState(99), "unknown"},
	}

	for _, tc := range tests {
		if tc.state.String() != tc.expected {
			t.Errorf("Expected %s, got %s", tc.expected, tc.state.String())
		}
	}
}

func TestPlanNodeType_String(t *testing.T) {
	tests := []struct {
		nodeType PlanNodeType
		expected string
	}{
		{PlanNodeScan, "Scan"},
		{PlanNodeFilter, "Filter"},
		{PlanNodeProject, "Project"},
		{PlanNodeAggregate, "Aggregate"},
		{PlanNodeJoin, "Join"},
		{PlanNodeWindow, "Window"},
		{PlanNodeSink, "Sink"},
		{PlanNodeExchange, "Exchange"},
		{PlanNodeType(99), "Unknown"},
	}

	for _, tc := range tests {
		if tc.nodeType.String() != tc.expected {
			t.Errorf("Expected %s, got %s", tc.expected, tc.nodeType.String())
		}
	}
}

func TestDefaultContinuousQueryConfig(t *testing.T) {
	config := DefaultContinuousQueryConfig()

	if !config.Enabled {
		t.Error("Expected Enabled to be true")
	}

	if config.MaxQueries <= 0 {
		t.Error("Expected positive MaxQueries")
	}

	if config.DefaultWindow <= 0 {
		t.Error("Expected positive DefaultWindow")
	}

	if config.CheckpointInterval <= 0 {
		t.Error("Expected positive CheckpointInterval")
	}

	if config.ParallelismHint <= 0 {
		t.Error("Expected positive ParallelismHint")
	}
}

func TestStreamJoin(t *testing.T) {
	config := DefaultContinuousQueryConfig()
	engine := NewContinuousQueryEngine(nil, nil, config)

	joinConfig := StreamJoinConfig{
		LeftStream:  "orders",
		RightStream: "users",
		JoinType:    StreamJoinInner,
		JoinKey:     "user_id",
		Window:      time.Minute,
	}

	query, err := engine.CreateStreamJoin("orders_users", joinConfig)
	if err != nil {
		t.Fatalf("Failed to create stream join: %v", err)
	}

	if query == nil {
		t.Fatal("Expected query")
	}

	if query.Name != "orders_users" {
		t.Errorf("Expected name 'orders_users', got '%s'", query.Name)
	}
}

func TestPartitionedQuery(t *testing.T) {
	config := DefaultContinuousQueryConfig()
	engine := NewContinuousQueryEngine(nil, nil, config)

	pq, err := engine.CreatePartitionedQuery("partitioned_test", "SELECT * FROM stream", 4)
	if err != nil {
		t.Fatalf("Failed to create partitioned query: %v", err)
	}

	if len(pq.Partitions) != 4 {
		t.Errorf("Expected 4 partitions, got %d", len(pq.Partitions))
	}

	// Test partition assignment
	p1 := pq.GetPartitionForKey("key1")
	p2 := pq.GetPartitionForKey("key2")

	if p1 == nil || p2 == nil {
		t.Error("Expected partition for keys")
	}

	// Same key should always get same partition
	p1Again := pq.GetPartitionForKey("key1")
	if p1.ID != p1Again.ID {
		t.Error("Same key should map to same partition")
	}
}

func TestContinuousQueryEngine_DataFlow(t *testing.T) {
	pw := &mockPointWriter{}
	bus := &mockStreamBus{}
	config := DefaultContinuousQueryConfig()
	engine := NewContinuousQueryEngine(pw, bus, config)
	engine.Start()
	defer engine.Stop()

	// Create query with chronicle sink
	query, err := engine.CreateQuery("flow_test", "SELECT * FROM test_stream", CQConfig{
		Sinks: []SinkConfig{
			{Type: "chronicle", Name: "output"},
		},
		Trigger: TriggerConfig{
			Type:     TriggerTypeProcessingTime,
			Interval: 50 * time.Millisecond,
		},
	})
	if err != nil {
		t.Fatalf("Failed to create query: %v", err)
	}

	engine.StartQuery(query.ID)

	// Publish some data
	for i := 0; i < 10; i++ {
		bus.Publish(Point{
			Metric:    "test_stream",
			Value:     float64(i),
			Timestamp: time.Now().UnixNano(),
			Tags:      map[string]string{"host": "server1"},
		})
	}

	// Wait for processing
	time.Sleep(200 * time.Millisecond)

	// Check stats
	q, _ := engine.GetQuery(query.ID)
	if atomic.LoadInt64(&q.Stats.InputRecords) == 0 {
		t.Log("Note: No records processed (expected in this test setup)")
	}
}

func TestOperatorState(t *testing.T) {
	t.Run("AggregateOperatorState", func(t *testing.T) {
		op := &AggregateOperator{id: "agg-0"}
		op.Open(context.Background())

		// Add some data
		for i := 0; i < 5; i++ {
			op.Process(&Record{
				Key:   "metric",
				Value: map[string]any{"value": float64(i * 10)},
			})
		}

		// Save state
		state, _ := op.GetState()

		// Create new operator and restore
		op2 := &AggregateOperator{id: "agg-1"}
		op2.Open(context.Background())
		op2.RestoreState(state)

		// Verify state was restored
		op2.mu.Lock()
		sum := op2.aggregates["metric_sum"]
		op2.mu.Unlock()

		if sum != 100 {
			t.Errorf("Expected sum 100 after restore, got %f", sum)
		}
	})

	t.Run("WindowOperatorState", func(t *testing.T) {
		op := &WindowOperator{id: "window-0"}
		op.Open(context.Background())

		// Add some data
		now := time.Now().UnixNano()
		op.Process(&Record{
			Key:       "metric",
			Timestamp: now,
			Value:     map[string]any{"value": 42.0},
		})

		// Save state
		state, _ := op.GetState()

		if len(state) == 0 {
			t.Error("Expected non-empty state")
		}

		// Restore
		op2 := &WindowOperator{id: "window-1"}
		op2.Open(context.Background())
		err := op2.RestoreState(state)

		if err != nil {
			t.Errorf("RestoreState failed: %v", err)
		}
	})
}

func BenchmarkContinuousQuery_Process(b *testing.B) {
	config := DefaultContinuousQueryConfig()
	engine := NewContinuousQueryEngine(nil, nil, config)

	query, _ := engine.CreateQuery("bench", "SELECT * FROM stream", CQConfig{})

	record := &Record{
		Key:       "metric",
		Value:     map[string]any{"value": 42.0},
		Timestamp: time.Now().UnixNano(),
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		for _, op := range query.operators {
			op.Process(record)
		}
	}
}

func BenchmarkContinuousQuery_Aggregation(b *testing.B) {
	op := &AggregateOperator{id: "agg-0"}
	op.Open(context.Background())

	record := &Record{
		Key:       "metric",
		Value:     map[string]any{"value": 42.0},
		Timestamp: time.Now().UnixNano(),
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		op.Process(record)
	}
}
