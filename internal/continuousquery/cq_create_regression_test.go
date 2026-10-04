package continuousquery

import (
	"fmt"
	"sync/atomic"
	"testing"
	"time"
)

func TestContinuousQueryEngine_CreateBurst(t *testing.T) {
	const count = 512
	config := DefaultContinuousQueryConfig()
	config.MaxQueries = count
	engine := NewContinuousQueryEngine(nil, nil, config)
	defer engine.Stop()
	seen := make(map[string]bool, 2*count)
	for cycle := 0; cycle < 2; cycle++ {
		ids := make([]string, 0, count)
		for i := 0; i < count; i++ {
			query, err := engine.CreateQuery(fmt.Sprintf("q-%d-%d", cycle, i), "SELECT * FROM stream", CQConfig{})
			if err != nil {
				t.Fatalf("create %d/%d: %v", cycle, i, err)
			}
			if seen[query.ID] {
				t.Fatalf("query ID reused at %d/%d: %s", cycle, i, query.ID)
			}
			seen[query.ID] = true
			ids = append(ids, query.ID)
			if actual, ok := engine.GetQuery(query.ID); !ok || actual != query {
				t.Fatalf("accepted query %s was not retained", query.ID)
			}
		}
		if got := len(engine.ListQueries()); got != count {
			t.Fatalf("retained %d queries, want %d", got, count)
		}
		if _, err := engine.CreateQuery("overflow", "SELECT 1", CQConfig{}); err == nil {
			t.Fatal("accepted a query beyond MaxQueries")
		}
		for _, id := range ids {
			if err := engine.DeleteQuery(id); err != nil {
				t.Fatal(err)
			}
		}
	}
	if got := atomic.LoadInt64(&engine.metrics.QueriesCreated); got != 2*count {
		t.Fatalf("created metric = %d, want %d", got, 2*count)
	}
}

// Hold both requests in the existing optimizer seam after their initial limit
// checks. This makes the admission race deterministic without replacing the
// production parser, operator builder, registry, or clock.
type admissionBarrierRule struct {
	entered chan struct{}
	release <-chan struct{}
}

func (*admissionBarrierRule) Name() string { return "admission-barrier" }
func (r *admissionBarrierRule) Apply(plan *QueryPlan) (*QueryPlan, bool) {
	r.entered <- struct{}{}
	<-r.release
	return plan, false
}

func TestContinuousQueryEngine_ConcurrentAdmission(t *testing.T) {
	config := DefaultContinuousQueryConfig()
	config.MaxQueries = 1
	engine := NewContinuousQueryEngine(nil, nil, config)
	defer engine.Stop()
	entered := make(chan struct{}, 2)
	release := make(chan struct{})
	defer close(release)
	engine.optimizer.rules = []OptimizationRule{&admissionBarrierRule{entered, release}}
	type result struct {
		query *ContinuousQueryV2
		err   error
	}
	results := make(chan result, 2)
	for i := 0; i < 2; i++ {
		go func() {
			query, err := engine.CreateQuery("concurrent", "SELECT 1", CQConfig{})
			results <- result{query, err}
		}()
	}
	for i := 0; i < 2; i++ {
		select {
		case <-entered:
		case <-time.After(5 * time.Second):
			t.Fatal("both creates did not reach the optimizer")
		}
	}
	// Release each waiter without closing the channel twice on an early failure.
	release <- struct{}{}
	release <- struct{}{}
	accepted, rejected := 0, 0
	for i := 0; i < 2; i++ {
		select {
		case outcome := <-results:
			if outcome.err == nil {
				accepted++
				if actual, ok := engine.GetQuery(outcome.query.ID); !ok || actual != outcome.query {
					t.Error("accepted query was overwritten")
				}
			} else if outcome.err.Error() == "max queries reached" && outcome.query == nil {
				rejected++
			} else {
				t.Errorf("unexpected creation result: %+v", outcome)
			}
		case <-time.After(5 * time.Second):
			t.Fatal("create did not complete")
		}
	}
	if accepted != 1 || rejected != 1 {
		t.Fatalf("accepted=%d rejected=%d; want one of each", accepted, rejected)
	}
	if got := len(engine.ListQueries()); got != 1 {
		t.Fatalf("registry size=%d, want 1", got)
	}
	if got := atomic.LoadInt64(&engine.metrics.QueriesCreated); got != 1 {
		t.Fatalf("created metric=%d, want 1", got)
	}
}
