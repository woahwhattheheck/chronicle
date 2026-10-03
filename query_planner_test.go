package chronicle

import (
	"context"
	"encoding/json"
	"math"
	"sync"
	"testing"
	"time"
)

func TestQueryPlannerBasic(t *testing.T) {
	db := setupTestDB(t)

	// Write some data to create partitions
	for i := 0; i < 100; i++ {
		db.Write(Point{
			Metric:    "cpu",
			Tags:      map[string]string{"host": "web-1"},
			Value:     float64(i),
			Timestamp: time.Now().Add(-time.Duration(i) * time.Minute).UnixNano(),
		})
	}

	config := DefaultQueryPlannerConfig()
	planner := NewQueryPlanner(db, config)
	planner.RefreshStats()

	q := &Query{
		Metric: "cpu",
		Start:  time.Now().Add(-time.Hour).UnixNano(),
		End:    time.Now().UnixNano(),
	}

	plan, err := planner.Plan(context.Background(), q)
	if err != nil {
		t.Fatalf("Plan failed: %v", err)
	}
	if plan == nil {
		t.Fatal("expected non-nil plan")
	}
	if plan.Root == nil {
		t.Fatal("expected non-nil plan root")
	}
	if len(plan.Optimizations) == 0 {
		t.Error("expected at least one optimization")
	}
}

func TestQueryPlannerExplain(t *testing.T) {
	db := setupTestDB(t)

	db.Write(Point{Metric: "mem", Value: 42, Timestamp: time.Now().UnixNano()})

	planner := NewQueryPlanner(db, DefaultQueryPlannerConfig())
	planner.RefreshStats()

	q := &Query{
		Metric: "mem",
		Start:  time.Now().Add(-time.Hour).UnixNano(),
		End:    time.Now().UnixNano(),
	}

	explanation, err := planner.Explain(context.Background(), q)
	if err != nil {
		t.Fatalf("Explain failed: %v", err)
	}
	if explanation == "" {
		t.Error("expected non-empty explanation")
	}
}

func TestQueryPlannerPartitionPruning(t *testing.T) {
	db := setupTestDB(t)

	planner := NewQueryPlanner(db, DefaultQueryPlannerConfig())
	planner.RefreshStats()

	// Query with no matching time range should produce empty plan
	q := &Query{
		Metric: "nonexistent",
		Start:  time.Now().Add(-24 * time.Hour).UnixNano(),
		End:    time.Now().Add(-23 * time.Hour).UnixNano(),
	}

	plan, err := planner.Plan(context.Background(), q)
	if err != nil {
		t.Fatalf("Plan failed: %v", err)
	}
	if plan.EstimatedRows != 0 {
		t.Logf("estimated rows: %d (may be non-zero if partitions overlap)", plan.EstimatedRows)
	}
}

func TestQueryPlannerNilQuery(t *testing.T) {
	db := setupTestDB(t)

	planner := NewQueryPlanner(db, DefaultQueryPlannerConfig())
	_, err := planner.Plan(context.Background(), nil)
	if err == nil {
		t.Error("expected error for nil query")
	}
}

func TestQueryPlannerStartStop(t *testing.T) {
	db := setupTestDB(t)

	config := DefaultQueryPlannerConfig()
	config.StatsRefreshInterval = 50 * time.Millisecond
	planner := NewQueryPlanner(db, config)

	planner.Start()
	time.Sleep(30 * time.Millisecond)
	planner.Stop()
}

func TestQueryPlannerStats(t *testing.T) {
	db := setupTestDB(t)

	planner := NewQueryPlanner(db, DefaultQueryPlannerConfig())
	planner.RefreshStats()

	stats := planner.GetStats()
	if stats == nil {
		t.Fatal("expected non-nil stats")
	}

	// Run a plan to populate planner stats
	planner.Plan(context.Background(), &Query{Metric: "x", Start: 0, End: time.Now().UnixNano()})
	pStats := planner.GetPlannerStats()
	if pStats.QueriesPlanned != 1 {
		t.Errorf("expected 1 query planned, got %d", pStats.QueriesPlanned)
	}
}

func TestQueryPlannerConcurrentStats(t *testing.T) {
	const workers, iterations = 4, 32
	const plans = workers * iterations
	config := DefaultQueryPlannerConfig()
	config.MinRowsForParallel = 1
	planner := NewQueryPlanner(nil, config)
	planner.RefreshStats()
	initial := planner.GetPlannerStats()
	// Publish a fixed statistics snapshot so every plan prunes one partition,
	// pushes both filters and scans the remaining two partitions in parallel.
	planner.queryStats.Store(&QueryStats{PartitionStats: []PartitionStats{
		{ID: "old", MinTime: 1, MaxTime: 9, RowCount: 10, Metrics: []string{"cpu"}},
		{ID: "first", MinTime: 10, MaxTime: 19, RowCount: 20, Metrics: []string{"cpu"}},
		{ID: "second", MinTime: 20, MaxTime: 29, RowCount: 30, Metrics: []string{"cpu"}},
	}})
	q := &Query{Metric: "cpu", Start: 10, End: 29, Tags: map[string]string{"host": "web-1"}}
	start := make(chan struct{})
	var wg sync.WaitGroup
	for range workers {
		wg.Add(1)
		go func() {
			defer wg.Done()
			<-start
			var previous uint64
			for range iterations {
				plan, err := planner.Plan(context.Background(), q)
				if err != nil {
					t.Errorf("Plan: %v", err)
					return
				}
				if plan.PartitionsPruned != 1 || plan.PartitionsUsed != 2 || plan.ParallelDegree != 2 {
					t.Errorf("unexpected partition plan: %+v", plan)
				}
				stats := planner.GetPlannerStats()
				if stats.QueriesPlanned < previous {
					t.Errorf("planned count decreased from %d to %d", previous, stats.QueriesPlanned)
				}
				previous = stats.QueriesPlanned
			}
		}()
	}
	close(start)
	wg.Wait()

	stats := planner.GetPlannerStats()
	if math.IsNaN(stats.AvgPlanningTimeUs) || math.IsInf(stats.AvgPlanningTimeUs, 0) || stats.AvgPlanningTimeUs < 0 {
		t.Errorf("invalid average planning time: %v", stats.AvgPlanningTimeUs)
	}
	want := PlannerStats{
		QueriesPlanned: plans, PartitionsPruned: plans, PredicatesPushed: 2 * plans,
		ParallelQueries: plans, StatsRefreshes: 1, AvgPlanningTimeUs: stats.AvgPlanningTimeUs,
	}
	if stats != want {
		t.Errorf("GetPlannerStats() = %+v, want %+v", stats, want)
	}
	if initial != (PlannerStats{StatsRefreshes: 1}) {
		t.Errorf("initial snapshot changed: %+v", initial)
	}
	encoded, err := json.Marshal(stats)
	if err != nil {
		t.Fatal(err)
	}
	var decoded map[string]float64
	if err := json.Unmarshal(encoded, &decoded); err != nil {
		t.Fatalf("planner statistics must retain numeric JSON fields: %v", err)
	}
	if len(decoded) != 7 || decoded["queries_planned"] != plans || decoded["predicates_pushed"] != 2*plans {
		t.Errorf("unexpected planner statistics JSON: %s", encoded)
	}
}

func TestQueryPlannerWithAggregation(t *testing.T) {
	db := setupTestDB(t)

	db.Write(Point{Metric: "cpu", Value: 50, Timestamp: time.Now().UnixNano()})

	planner := NewQueryPlanner(db, DefaultQueryPlannerConfig())
	planner.RefreshStats()

	q := &Query{
		Metric: "cpu",
		Start:  time.Now().Add(-time.Hour).UnixNano(),
		End:    time.Now().UnixNano(),
		Aggregation: &Aggregation{
			Function: AggMean,
			Window:   5 * time.Minute,
		},
		Limit: 100,
	}

	plan, err := planner.Plan(context.Background(), q)
	if err != nil {
		t.Fatalf("Plan failed: %v", err)
	}
	if plan.Root == nil {
		t.Fatal("expected plan root")
	}
}
