package chronicle

import (
	"fmt"
	"reflect"
	"testing"
	"time"
)

// ---------------------------------------------------------------------------
// Configuration
// ---------------------------------------------------------------------------

func TestDefaultDistributedFederationConfig(t *testing.T) {
	cfg := DefaultDistributedFederationConfig()

	if !cfg.Enabled {
		t.Fatal("expected Enabled=true")
	}
	if cfg.MaxFanOut != 10 {
		t.Fatalf("expected MaxFanOut=10, got %d", cfg.MaxFanOut)
	}
	if cfg.PartialResultTimeout != 5*time.Second {
		t.Fatalf("expected PartialResultTimeout=5s, got %v", cfg.PartialResultTimeout)
	}
	if !cfg.EnablePredicatePushDown {
		t.Fatal("expected EnablePredicatePushDown=true")
	}
	if !cfg.EnableLocalityRouting {
		t.Fatal("expected EnableLocalityRouting=true")
	}
	if cfg.CircuitBreakerThreshold != 5 {
		t.Fatalf("expected CircuitBreakerThreshold=5, got %d", cfg.CircuitBreakerThreshold)
	}
	if cfg.CircuitBreakerTimeout != 30*time.Second {
		t.Fatalf("expected CircuitBreakerTimeout=30s, got %v", cfg.CircuitBreakerTimeout)
	}
	if cfg.CircuitBreakerHalfOpenMax != 3 {
		t.Fatalf("expected CircuitBreakerHalfOpenMax=3, got %d", cfg.CircuitBreakerHalfOpenMax)
	}
	if cfg.MaxPartialResultSizeMB != 64 {
		t.Fatalf("expected MaxPartialResultSizeMB=64, got %d", cfg.MaxPartialResultSizeMB)
	}
	if !cfg.EnablePartialResults {
		t.Fatal("expected EnablePartialResults=true")
	}
	if cfg.RetryPolicy.InitialInterval != 100*time.Millisecond {
		t.Fatalf("expected RetryPolicy.InitialInterval=100ms, got %v", cfg.RetryPolicy.InitialInterval)
	}
	if cfg.RetryPolicy.MaxInterval != 5*time.Second {
		t.Fatalf("expected RetryPolicy.MaxInterval=5s, got %v", cfg.RetryPolicy.MaxInterval)
	}
	if cfg.RetryPolicy.Multiplier != 2.0 {
		t.Fatalf("expected RetryPolicy.Multiplier=2.0, got %f", cfg.RetryPolicy.Multiplier)
	}
	if cfg.RetryPolicy.MaxRetries != 3 {
		t.Fatalf("expected RetryPolicy.MaxRetries=3, got %d", cfg.RetryPolicy.MaxRetries)
	}
	if cfg.HealthCheckInterval != 10*time.Second {
		t.Fatalf("expected HealthCheckInterval=10s, got %v", cfg.HealthCheckInterval)
	}
}

// ---------------------------------------------------------------------------
// Predicate Push-Down
// ---------------------------------------------------------------------------

func TestPredicatePushDown_Analyze(t *testing.T) {
	pp := NewPredicatePushDown(true)

	t.Run("nil query", func(t *testing.T) {
		a := pp.AnalyzePredicates(nil)
		if len(a.Pushable) != 0 || len(a.Local) != 0 {
			t.Fatal("nil query should produce empty analysis")
		}
	})

	t.Run("disabled", func(t *testing.T) {
		disabled := NewPredicatePushDown(false)
		a := disabled.AnalyzePredicates(&Query{Metric: "cpu"})
		if len(a.Pushable) != 0 || len(a.Local) != 0 {
			t.Fatal("disabled push-down should produce empty analysis")
		}
	})

	t.Run("time range predicate", func(t *testing.T) {
		a := pp.AnalyzePredicates(&Query{Metric: "cpu", Start: 100, End: 200})
		found := false
		for _, p := range a.Pushable {
			if p.Kind == FedPredicateTimeRange && p.Safe {
				found = true
			}
		}
		if !found {
			t.Fatal("expected pushable time range predicate")
		}
	})

	t.Run("tag equality predicate", func(t *testing.T) {
		a := pp.AnalyzePredicates(&Query{
			Metric: "cpu",
			Tags:   map[string]string{"host": "server1"},
		})
		found := false
		for _, p := range a.Pushable {
			if p.Kind == FedPredicateTagEquality && p.Field == "host" && p.Value == "server1" && p.Safe {
				found = true
			}
		}
		if !found {
			t.Fatal("expected pushable tag equality predicate for host=server1")
		}
	})

	t.Run("tag filter eq is pushable", func(t *testing.T) {
		a := pp.AnalyzePredicates(&Query{
			Metric: "cpu",
			TagFilters: []TagFilter{
				{Key: "region", Op: TagOpEq, Values: []string{"us-east"}},
			},
		})
		found := false
		for _, p := range a.Pushable {
			if p.Kind == FedPredicateTagFilter && p.Field == "region" && p.Safe {
				found = true
			}
		}
		if !found {
			t.Fatal("expected pushable tag filter for equality op")
		}
	})

	t.Run("tag filter regex stays local", func(t *testing.T) {
		a := pp.AnalyzePredicates(&Query{
			Metric: "cpu",
			TagFilters: []TagFilter{
				{Key: "host", Op: TagOpRegex, Values: []string{"web-.*"}},
			},
		})
		found := false
		for _, p := range a.Local {
			if p.Kind == FedPredicateTagFilter && p.Field == "host" && !p.Safe {
				found = true
			}
		}
		if !found {
			t.Fatal("expected local tag filter for regex op")
		}
	})

	t.Run("distributive aggregation is pushable", func(t *testing.T) {
		for _, fn := range []AggFunc{AggSum, AggCount, AggMin, AggMax} {
			a := pp.AnalyzePredicates(&Query{
				Metric:      "cpu",
				Aggregation: &Aggregation{Function: fn, Window: time.Minute},
			})
			found := false
			for _, p := range a.Pushable {
				if p.Kind == FedPredicateAggregation && p.Safe {
					found = true
				}
			}
			if !found {
				t.Fatalf("expected pushable aggregation for function %d", fn)
			}
		}
	})

	t.Run("mean aggregation is pushable but not safe", func(t *testing.T) {
		a := pp.AnalyzePredicates(&Query{
			Metric:      "cpu",
			Aggregation: &Aggregation{Function: AggMean, Window: time.Minute},
		})
		found := false
		for _, p := range a.Pushable {
			if p.Kind == FedPredicateAggregation && !p.Safe {
				found = true
			}
		}
		if !found {
			t.Fatal("expected pushable but unsafe predicate for AggMean")
		}
	})

	t.Run("limit stays local", func(t *testing.T) {
		a := pp.AnalyzePredicates(&Query{Metric: "cpu", Limit: 10})
		found := false
		for _, p := range a.Local {
			if p.Kind == FedPredicateLimit && p.Value == "10" && !p.Safe {
				found = true
			}
		}
		if !found {
			t.Fatal("expected local limit predicate")
		}
	})
}

func TestPredicatePushDown_CreateRemoteQuery(t *testing.T) {
	pp := NewPredicatePushDown(true)

	t.Run("nil query returns nil", func(t *testing.T) {
		if pp.CreateRemoteQuery(nil, nil) != nil {
			t.Fatal("expected nil for nil query")
		}
	})

	t.Run("tag equality pushed", func(t *testing.T) {
		q := &Query{
			Metric: "cpu",
			Start:  100,
			End:    200,
			Tags:   map[string]string{"host": "server1"},
		}
		analysis := pp.AnalyzePredicates(q)
		remote := pp.CreateRemoteQuery(q, analysis.Pushable)
		if remote.Metric != "cpu" {
			t.Fatalf("expected metric=cpu, got %s", remote.Metric)
		}
		if remote.Start != 100 || remote.End != 200 {
			t.Fatalf("expected time range preserved, got %d-%d", remote.Start, remote.End)
		}
		if remote.Tags["host"] != "server1" {
			t.Fatal("expected tag host=server1 in remote query")
		}
	})

	t.Run("tag filter pushed", func(t *testing.T) {
		q := &Query{
			Metric: "cpu",
			TagFilters: []TagFilter{
				{Key: "region", Op: TagOpIn, Values: []string{"us-east", "us-west"}},
			},
		}
		analysis := pp.AnalyzePredicates(q)
		remote := pp.CreateRemoteQuery(q, analysis.Pushable)
		if len(remote.TagFilters) != 1 {
			t.Fatalf("expected 1 tag filter, got %d", len(remote.TagFilters))
		}
		if remote.TagFilters[0].Key != "region" {
			t.Fatalf("expected tag filter key=region, got %s", remote.TagFilters[0].Key)
		}
	})

	t.Run("same-key filters retain distinct operators and values", func(t *testing.T) {
		for _, tc := range []struct {
			name    string
			filters []TagFilter
		}{
			{
				name: "membership and exclusion",
				filters: []TagFilter{
					{Key: "host", Op: TagOpIn, Values: []string{"web-1", "db-1"}},
					{Key: "host", Op: TagOpNotIn, Values: []string{"web-1"}},
				},
			},
			{
				name: "same values with opposite operators",
				filters: []TagFilter{
					{Key: "host", Op: TagOpIn, Values: []string{"web-1"}},
					{Key: "host", Op: TagOpNotIn, Values: []string{"web-1"}},
				},
			},
			{
				name: "separate exclusions",
				filters: []TagFilter{
					{Key: "host", Op: TagOpNotIn, Values: []string{"web-1"}},
					{Key: "host", Op: TagOpNotIn, Values: []string{"web-2"}},
				},
			},
		} {
			t.Run(tc.name, func(t *testing.T) {
				q := &Query{Metric: "cpu", TagFilters: tc.filters}
				analysis := pp.AnalyzePredicates(q)
				remote := pp.CreateRemoteQuery(q, analysis.Pushable)
				if !reflect.DeepEqual(remote.TagFilters, q.TagFilters) {
					t.Fatalf("pushed filters changed: got %#v, want %#v", remote.TagFilters, q.TagFilters)
				}
			})
		}
	})

	t.Run("same-key local regex does not replace an exclusion", func(t *testing.T) {
		q := &Query{
			Metric: "cpu",
			TagFilters: []TagFilter{
				{Key: "host", Op: TagOpRegex, Values: []string{"web-1"}},
				{Key: "host", Op: TagOpNotIn, Values: []string{"web-1"}},
			},
		}
		analysis := pp.AnalyzePredicates(q)
		remote := pp.CreateRemoteQuery(q, analysis.Pushable)
		if !reflect.DeepEqual(remote.TagFilters, q.TagFilters[1:]) {
			t.Fatalf("pushed filters changed: got %#v, want %#v", remote.TagFilters, q.TagFilters[1:])
		}
	})

	t.Run("selected value does not pick another same-key filter", func(t *testing.T) {
		q := &Query{
			Metric: "cpu",
			TagFilters: []TagFilter{
				{Key: "host", Op: TagOpIn, Values: []string{"web-1", "db-1"}},
				{Key: "host", Op: TagOpNotIn, Values: []string{"web-1"}},
			},
		}
		analysis := pp.AnalyzePredicates(q)
		remote := pp.CreateRemoteQuery(q, analysis.Pushable[1:])
		if !reflect.DeepEqual(remote.TagFilters, q.TagFilters[1:]) {
			t.Fatalf("pushed filters changed: got %#v, want %#v", remote.TagFilters, q.TagFilters[1:])
		}
	})

	t.Run("aggregation pushed", func(t *testing.T) {
		q := &Query{
			Metric:      "cpu",
			Aggregation: &Aggregation{Function: AggSum, Window: time.Minute},
			GroupBy:     []string{"host"},
		}
		analysis := pp.AnalyzePredicates(q)
		remote := pp.CreateRemoteQuery(q, analysis.Pushable)
		if remote.Aggregation == nil {
			t.Fatal("expected aggregation in remote query")
		}
		if remote.Aggregation.Function != AggSum {
			t.Fatalf("expected AggSum, got %d", remote.Aggregation.Function)
		}
		if len(remote.GroupBy) != 1 || remote.GroupBy[0] != "host" {
			t.Fatal("expected GroupBy=[host] in remote query")
		}
	})
}

// ---------------------------------------------------------------------------
// Data Locality Router
// ---------------------------------------------------------------------------

func TestDataLocalityRouter_RegisterNode(t *testing.T) {
	router := NewDataLocalityRouter()

	router.RegisterNode("n1", "http://node1:8080", []string{"cpu", "mem"}, []TimeRange{
		{Start: 100, End: 200},
	})

	node, ok := router.GetNode("n1")
	if !ok {
		t.Fatal("expected node n1 to be registered")
	}
	if node.NodeID != "n1" {
		t.Fatalf("expected NodeID=n1, got %s", node.NodeID)
	}
	if node.Address != "http://node1:8080" {
		t.Fatalf("expected address, got %s", node.Address)
	}
	if len(node.Metrics) != 2 {
		t.Fatalf("expected 2 metrics, got %d", len(node.Metrics))
	}
	if !node.Healthy {
		t.Fatal("expected node to be healthy by default")
	}

	// Update health.
	router.UpdateHealth("n1", false, 0.8)
	node, _ = router.GetNode("n1")
	if node.Healthy {
		t.Fatal("expected node to be unhealthy after update")
	}
	if node.Load != 0.8 {
		t.Fatalf("expected load=0.8, got %f", node.Load)
	}

	// Remove node.
	router.RemoveNode("n1")
	_, ok = router.GetNode("n1")
	if ok {
		t.Fatal("expected node n1 to be removed")
	}

	// AllNodes
	router.RegisterNode("a", "addr-a", []string{"cpu"}, nil)
	router.RegisterNode("b", "addr-b", []string{"mem"}, nil)
	all := router.AllNodes()
	if len(all) != 2 {
		t.Fatalf("expected 2 nodes, got %d", len(all))
	}
}

func TestDataLocalityRouter_RouteQuery(t *testing.T) {
	router := NewDataLocalityRouter()

	router.RegisterNode("n1", "addr1", []string{"cpu"}, []TimeRange{{Start: 100, End: 300}})
	router.RegisterNode("n2", "addr2", []string{"cpu"}, []TimeRange{{Start: 200, End: 400}})
	router.RegisterNode("n3", "addr3", []string{"mem"}, []TimeRange{{Start: 100, End: 300}})

	t.Run("routes to nodes with matching metric", func(t *testing.T) {
		scores := router.RouteQuery(&Query{Metric: "cpu", Start: 100, End: 300})
		if len(scores) != 2 {
			t.Fatalf("expected 2 nodes for cpu, got %d", len(scores))
		}
		for _, s := range scores {
			if s.Node.NodeID != "n1" && s.Node.NodeID != "n2" {
				t.Fatalf("unexpected node %s", s.Node.NodeID)
			}
		}
	})

	t.Run("excludes unhealthy nodes", func(t *testing.T) {
		router.UpdateHealth("n1", false, 0.0)
		scores := router.RouteQuery(&Query{Metric: "cpu", Start: 100, End: 300})
		if len(scores) != 1 {
			t.Fatalf("expected 1 node after n1 unhealthy, got %d", len(scores))
		}
		if scores[0].Node.NodeID != "n2" {
			t.Fatalf("expected n2, got %s", scores[0].Node.NodeID)
		}
		router.UpdateHealth("n1", true, 0.0) // restore
	})

	t.Run("prefers lower load node", func(t *testing.T) {
		router.UpdateHealth("n1", true, 0.9) // high load
		router.UpdateHealth("n2", true, 0.1) // low load
		scores := router.RouteQuery(&Query{Metric: "cpu", Start: 200, End: 300})
		if len(scores) < 2 {
			t.Fatalf("expected >=2 scores, got %d", len(scores))
		}
		// Both have metric, overlapping range; n2 should score higher due to lower load.
		if scores[0].Node.NodeID != "n2" {
			t.Fatalf("expected n2 ranked first (lower load), got %s", scores[0].Node.NodeID)
		}
	})

	t.Run("no match returns empty", func(t *testing.T) {
		scores := router.RouteQuery(&Query{Metric: "disk"})
		if len(scores) != 0 {
			t.Fatalf("expected 0 scores for unknown metric, got %d", len(scores))
		}
	})

	t.Run("unbounded query gives moderate score", func(t *testing.T) {
		scores := router.RouteQuery(&Query{Metric: "cpu"})
		if len(scores) == 0 {
			t.Fatal("expected scores for unbounded query")
		}
		for _, s := range scores {
			if s.Score <= 0 {
				t.Fatalf("expected positive score, got %f", s.Score)
			}
		}
	})
}

// ---------------------------------------------------------------------------
// Circuit Breaker
// ---------------------------------------------------------------------------

func TestFedCircuitBreaker_ClosedState(t *testing.T) {
	cb := NewFedCircuitBreaker(3, time.Minute, 2)

	if cb.State() != FedCircuitClosed {
		t.Fatal("expected initial state to be closed")
	}

	// Successful executions keep it closed.
	for i := 0; i < 5; i++ {
		err := cb.Execute(func() error { return nil })
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
	}
	if cb.State() != FedCircuitClosed {
		t.Fatal("expected state to remain closed after successes")
	}

	// Some failures below threshold keep it closed.
	cb.RecordFailure()
	cb.RecordFailure()
	if cb.State() != FedCircuitClosed {
		t.Fatal("expected state to remain closed below threshold")
	}
}

func TestFedCircuitBreaker_OpenState(t *testing.T) {
	cb := NewFedCircuitBreaker(3, time.Minute, 2)

	// Trip the breaker.
	for i := 0; i < 3; i++ {
		cb.RecordFailure()
	}
	if cb.State() != FedCircuitOpen {
		t.Fatal("expected state to be open after reaching threshold")
	}

	// Execute should fail immediately when open.
	err := cb.Execute(func() error { return nil })
	if err == nil {
		t.Fatal("expected error when circuit is open")
	}

	// Reset returns to closed.
	cb.Reset()
	if cb.State() != FedCircuitClosed {
		t.Fatal("expected state to be closed after reset")
	}
}

func TestFedCircuitBreaker_HalfOpenState(t *testing.T) {
	cb := NewFedCircuitBreaker(3, 1*time.Millisecond, 2)

	// Trip the breaker.
	for i := 0; i < 3; i++ {
		cb.RecordFailure()
	}
	if cb.State() != FedCircuitOpen {
		t.Fatal("expected open")
	}

	// Wait for timeout to transition to half-open.
	time.Sleep(5 * time.Millisecond)
	if cb.State() != FedCircuitHalfOpen {
		t.Fatal("expected half-open after timeout")
	}

	// Successful execute in half-open transitions to closed.
	err := cb.Execute(func() error { return nil })
	if err != nil {
		t.Fatalf("unexpected error in half-open: %v", err)
	}
	if cb.State() != FedCircuitClosed {
		t.Fatal("expected closed after success in half-open")
	}

	// Trip again and test failure in half-open reopens.
	for i := 0; i < 3; i++ {
		cb.RecordFailure()
	}
	time.Sleep(5 * time.Millisecond)
	if cb.State() != FedCircuitHalfOpen {
		t.Fatal("expected half-open again")
	}
	err = cb.Execute(func() error { return fmt.Errorf("fail") })
	if err == nil {
		t.Fatal("expected error from failing function")
	}
	if cb.State() != FedCircuitOpen {
		t.Fatal("expected open after failure in half-open")
	}
}

func TestFedCircuitBreaker_HalfOpenLimit(t *testing.T) {
	cb := NewFedCircuitBreaker(2, 1*time.Millisecond, 1)

	// Trip and wait for half-open.
	cb.RecordFailure()
	cb.RecordFailure()
	time.Sleep(5 * time.Millisecond)
	if cb.State() != FedCircuitHalfOpen {
		t.Fatal("expected half-open")
	}

	// First attempt allowed.
	_ = cb.Execute(func() error { return fmt.Errorf("fail") })

	// After failure in half-open, circuit goes back to open.
	if cb.State() != FedCircuitOpen {
		t.Fatal("expected open after half-open failure")
	}
}

func TestFedCircuitBreaker_StateString(t *testing.T) {
	tests := []struct {
		state FedCircuitState
		want  string
	}{
		{FedCircuitClosed, "closed"},
		{FedCircuitOpen, "open"},
		{FedCircuitHalfOpen, "half_open"},
		{FedCircuitState(99), "unknown"},
	}
	for _, tt := range tests {
		if got := tt.state.String(); got != tt.want {
			t.Errorf("State(%d).String() = %q, want %q", tt.state, got, tt.want)
		}
	}
}

// ---------------------------------------------------------------------------
// Partial Result Assembler
// ---------------------------------------------------------------------------

func TestPartialResultAssembler(t *testing.T) {
	asm := NewPartialResultAssembler(64)

	partials := []FederatedPartialResult{
		{
			NodeID: "n1",
			Points: []Point{
				{Metric: "cpu", Timestamp: 100, Value: 1.0},
				{Metric: "cpu", Timestamp: 300, Value: 3.0},
			},
		},
		{
			NodeID: "n2",
			Points: []Point{
				{Metric: "cpu", Timestamp: 200, Value: 2.0},
				{Metric: "cpu", Timestamp: 400, Value: 4.0},
			},
		},
	}

	result := asm.Assemble(partials, nil)

	if result.NodesQueried != 2 {
		t.Fatalf("expected NodesQueried=2, got %d", result.NodesQueried)
	}
	if result.NodesResponded != 2 {
		t.Fatalf("expected NodesResponded=2, got %d", result.NodesResponded)
	}
	if result.NodesFailed != 0 {
		t.Fatalf("expected NodesFailed=0, got %d", result.NodesFailed)
	}
	if result.TotalPoints != 4 {
		t.Fatalf("expected 4 points, got %d", result.TotalPoints)
	}
	// Points should be sorted by timestamp.
	for i := 1; i < len(result.Points); i++ {
		if result.Points[i].Timestamp < result.Points[i-1].Timestamp {
			t.Fatal("points not sorted by timestamp")
		}
	}
	if result.IsPartial {
		t.Fatal("expected non-partial result when all nodes succeed")
	}
}

func TestPartialResultAssembler_WithFailures(t *testing.T) {
	asm := NewPartialResultAssembler(64)

	partials := []FederatedPartialResult{
		{
			NodeID: "n1",
			Points: []Point{
				{Metric: "cpu", Timestamp: 100, Value: 1.0},
			},
		},
		{
			NodeID: "n2",
			Error:  "connection refused",
		},
	}

	result := asm.Assemble(partials, nil)
	if result.NodesResponded != 1 {
		t.Fatalf("expected NodesResponded=1, got %d", result.NodesResponded)
	}
	if result.NodesFailed != 1 {
		t.Fatalf("expected NodesFailed=1, got %d", result.NodesFailed)
	}
	if !result.IsPartial {
		t.Fatal("expected partial result when some nodes fail")
	}
	if len(result.FailedNodes) != 1 || result.FailedNodes[0] != "n2" {
		t.Fatalf("expected FailedNodes=[n2], got %v", result.FailedNodes)
	}
}

func TestPartialResultAssembler_Dedup(t *testing.T) {
	asm := NewPartialResultAssembler(64)

	// Duplicate points from two nodes.
	dup := Point{Metric: "cpu", Timestamp: 100, Value: 1.0, Tags: map[string]string{"host": "a"}}
	partials := []FederatedPartialResult{
		{NodeID: "n1", Points: []Point{dup}},
		{NodeID: "n2", Points: []Point{dup}},
	}

	result := asm.Assemble(partials, nil)
	if result.TotalPoints != 1 {
		t.Fatalf("expected 1 point after dedup, got %d", result.TotalPoints)
	}
}

func TestPartialResultAssembler_AggregationMerge(t *testing.T) {
	asm := NewPartialResultAssembler(64)

	partials := []FederatedPartialResult{
		{
			NodeID: "n1",
			Points: []Point{
				{Metric: "cpu", Timestamp: 100, Value: 10.0, Tags: map[string]string{"host": "a"}},
			},
		},
		{
			NodeID: "n2",
			Points: []Point{
				{Metric: "cpu", Timestamp: 100, Value: 20.0, Tags: map[string]string{"host": "a"}},
			},
		},
	}

	agg := &Aggregation{Function: AggSum, Window: time.Minute}
	result := asm.Assemble(partials, agg)
	if result.TotalPoints != 1 {
		t.Fatalf("expected 1 merged point, got %d", result.TotalPoints)
	}
	if result.Points[0].Value != 30.0 {
		t.Fatalf("expected merged sum=30.0, got %f", result.Points[0].Value)
	}
}

func TestPartialResultAssembler_DefaultMaxSize(t *testing.T) {
	// Passing 0 should default to 64.
	asm := NewPartialResultAssembler(0)
	result := asm.Assemble(nil, nil)
	if result.NodesQueried != 0 {
		t.Fatalf("expected 0 nodes queried for nil partials, got %d", result.NodesQueried)
	}
}

// ---------------------------------------------------------------------------
// Query Federation Engine
// ---------------------------------------------------------------------------

func TestNewQueryFederationEngine(t *testing.T) {
	dir := t.TempDir()
	cfg := DefaultConfig(dir + "/test.db")
	db, err := Open(cfg.Path, cfg)
	if err != nil {
		t.Fatalf("failed to open db: %v", err)
	}
	defer db.Close()

	fedCfg := DefaultDistributedFederationConfig()
	engine := NewQueryFederationEngine(db, fedCfg)
	if engine == nil {
		t.Fatal("expected non-nil engine")
	}
	stats := engine.Stats()
	if stats["total_queries"] != 0 {
		t.Fatalf("expected 0 total queries, got %d", stats["total_queries"])
	}
}

func TestQueryFederationEngine_StartStop(t *testing.T) {
	dir := t.TempDir()
	cfg := DefaultConfig(dir + "/test.db")
	db, err := Open(cfg.Path, cfg)
	if err != nil {
		t.Fatalf("failed to open db: %v", err)
	}
	defer db.Close()

	fedCfg := DefaultDistributedFederationConfig()
	fedCfg.HealthCheckInterval = 50 * time.Millisecond
	engine := NewQueryFederationEngine(db, fedCfg)

	// Start should succeed.
	if err := engine.Start(); err != nil {
		t.Fatalf("start failed: %v", err)
	}

	// Starting again should be a no-op.
	if err := engine.Start(); err != nil {
		t.Fatalf("second start failed: %v", err)
	}

	// Stop should be clean.
	engine.Stop()

	// Double stop should be safe.
	engine.Stop()
}

func TestQueryFederationEngine_NodeManagement(t *testing.T) {
	dir := t.TempDir()
	cfg := DefaultConfig(dir + "/test.db")
	db, err := Open(cfg.Path, cfg)
	if err != nil {
		t.Fatalf("failed to open db: %v", err)
	}
	defer db.Close()

	fedCfg := DefaultDistributedFederationConfig()
	engine := NewQueryFederationEngine(db, fedCfg)

	// Register nodes.
	engine.RegisterNode(FederationNodeInfo{
		NodeID:  "n1",
		Address: "http://node1:8080",
		Metrics: []string{"cpu", "mem"},
		Healthy: true,
		Meta:    map[string]string{"dc": "us-east"},
	})
	engine.RegisterNode(FederationNodeInfo{
		NodeID:  "n2",
		Address: "http://node2:8080",
		Metrics: []string{"disk"},
		Healthy: true,
	})

	nodes := engine.GetNodeHealth()
	if len(nodes) != 2 {
		t.Fatalf("expected 2 nodes, got %d", len(nodes))
	}

	// Check circuit breaker state.
	state, ok := engine.GetCircuitBreakerState("n1")
	if !ok {
		t.Fatal("expected circuit breaker for n1")
	}
	if state != FedCircuitClosed {
		t.Fatalf("expected closed state, got %v", state)
	}

	// Deregister node.
	engine.DeregisterNode("n1")
	nodes = engine.GetNodeHealth()
	if len(nodes) != 1 {
		t.Fatalf("expected 1 node after deregister, got %d", len(nodes))
	}
	_, ok = engine.GetCircuitBreakerState("n1")
	if ok {
		t.Fatal("expected no circuit breaker for deregistered n1")
	}
}

func TestQueryFederationEngine_ExplainFederated(t *testing.T) {
	dir := t.TempDir()
	cfg := DefaultConfig(dir + "/test.db")
	db, err := Open(cfg.Path, cfg)
	if err != nil {
		t.Fatalf("failed to open db: %v", err)
	}
	defer db.Close()

	fedCfg := DefaultDistributedFederationConfig()
	engine := NewQueryFederationEngine(db, fedCfg)

	engine.RegisterNode(FederationNodeInfo{
		NodeID:     "n1",
		Address:    "http://node1:8080",
		Metrics:    []string{"cpu"},
		TimeRanges: []TimeRange{{Start: 100, End: 500}},
		Healthy:    true,
	})
	engine.RegisterNode(FederationNodeInfo{
		NodeID:     "n2",
		Address:    "http://node2:8080",
		Metrics:    []string{"cpu"},
		TimeRanges: []TimeRange{{Start: 300, End: 800}},
		Healthy:    true,
	})

	q := &Query{
		Metric:      "cpu",
		Start:       200,
		End:         600,
		Tags:        map[string]string{"host": "web1"},
		Aggregation: &Aggregation{Function: AggSum, Window: time.Minute},
	}

	plan := engine.ExplainFederated(q)
	if plan == nil {
		t.Fatal("expected non-nil plan")
	}
	if plan.QueryID == "" {
		t.Fatal("expected non-empty QueryID")
	}
	if plan.OriginalQuery != q {
		t.Fatal("expected original query in plan")
	}
	if plan.EstimatedFanOut != 2 {
		t.Fatalf("expected fan-out=2, got %d", plan.EstimatedFanOut)
	}
	if len(plan.Assignments) != 2 {
		t.Fatalf("expected 2 assignments, got %d", len(plan.Assignments))
	}
	if plan.FedAggregationStrategy != FedAggStrategyPartial {
		t.Fatalf("expected partial aggregation strategy, got %s", plan.FedAggregationStrategy)
	}

	// Each assignment should have a remote query with pushed predicates.
	for _, a := range plan.Assignments {
		if a.RemoteQuery == nil {
			t.Fatalf("expected remote query for node %s", a.NodeID)
		}
		if a.RemoteQuery.Metric != "cpu" {
			t.Fatalf("expected metric=cpu in remote query, got %s", a.RemoteQuery.Metric)
		}
		if a.Score <= 0 {
			t.Fatalf("expected positive score for node %s, got %f", a.NodeID, a.Score)
		}
	}

	// Predicate analysis should have pushable predicates.
	if len(plan.PredicateAnalysis.Pushable) == 0 {
		t.Fatal("expected pushable predicates in analysis")
	}
}

func TestQueryFederationEngine_ExplainNoNodes(t *testing.T) {
	dir := t.TempDir()
	cfg := DefaultConfig(dir + "/test.db")
	db, err := Open(cfg.Path, cfg)
	if err != nil {
		t.Fatalf("failed to open db: %v", err)
	}
	defer db.Close()

	fedCfg := DefaultDistributedFederationConfig()
	engine := NewQueryFederationEngine(db, fedCfg)

	plan := engine.ExplainFederated(&Query{Metric: "cpu"})
	if plan.EstimatedFanOut != 0 {
		t.Fatalf("expected fan-out=0 with no nodes, got %d", plan.EstimatedFanOut)
	}
}

func TestQueryFederationEngine_ExplainMaxFanOut(t *testing.T) {
	dir := t.TempDir()
	cfg := DefaultConfig(dir + "/test.db")
	db, err := Open(cfg.Path, cfg)
	if err != nil {
		t.Fatalf("failed to open db: %v", err)
	}
	defer db.Close()

	fedCfg := DefaultDistributedFederationConfig()
	fedCfg.MaxFanOut = 2
	engine := NewQueryFederationEngine(db, fedCfg)

	// Register more nodes than MaxFanOut.
	for i := 0; i < 5; i++ {
		engine.RegisterNode(FederationNodeInfo{
			NodeID:  fmt.Sprintf("n%d", i),
			Address: fmt.Sprintf("http://node%d:8080", i),
			Metrics: []string{"cpu"},
			Healthy: true,
		})
	}

	plan := engine.ExplainFederated(&Query{Metric: "cpu"})
	if plan.EstimatedFanOut > 2 {
		t.Fatalf("expected fan-out capped at 2, got %d", plan.EstimatedFanOut)
	}
}
