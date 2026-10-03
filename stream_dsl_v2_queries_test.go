package chronicle

import (
	"encoding/json"
	"sync"
	"testing"
	"time"
)

func TestStreamDslV2Queries_Smoke(t *testing.T) {
	// Smoke test: verify StreamDSLV2Engine types and functions from stream_dsl_v2_queries.go are accessible.
	if testing.Short() {
		t.Skip("skipping smoke test in short mode")
	}
}

func TestStreamDSLV2ConcurrentQueryStats(t *testing.T) {
	engine := NewStreamDSLV2Engine(nil, DefaultStreamDSLV2Config())
	initial := engine.Stats()
	q, err := engine.CreateContinuousQuery("concurrent_stats", "SELECT * FROM cpu.usage")
	if err != nil {
		t.Fatal(err)
	}
	if err := engine.StartQuery(q.ID); err != nil {
		t.Fatal(err)
	}
	if err := engine.RegisterPattern(CEPPattern{
		Name:   "cpu_events",
		Events: []CEPEvent{{Metric: "cpu.usage"}},
		Within: time.Minute,
	}); err != nil {
		t.Fatal(err)
	}

	const workers, iterations = 8, 32
	const matchingEvents = workers * iterations
	start := make(chan struct{})
	var wg sync.WaitGroup
	for worker := range workers {
		wg.Add(1)
		go func() {
			defer wg.Done()
			<-start
			var previous int64
			for i := range iterations {
				value := float64(worker*iterations + i)
				now := time.Now()
				if err := engine.ProcessEvent("cpu.usage", value, nil, now); err != nil {
					t.Errorf("process matching event: %v", err)
					return
				}
				if err := engine.ProcessEvent("memory.usage", value, nil, now); err != nil {
					t.Errorf("process nonmatching event: %v", err)
					return
				}
				stats := engine.Stats()
				if stats.TotalEventsProcessed < previous || stats.ActiveQueries != 1 {
					t.Errorf("unexpected live stats: %+v (previous events %d)", stats, previous)
				}
				previous = stats.TotalEventsProcessed
			}
		}()
	}
	close(start)
	wg.Wait()

	stats := engine.Stats()
	if stats.TotalEventsProcessed != 2*matchingEvents || stats.PatternsMatched != matchingEvents {
		t.Errorf("unexpected final engine counts: %+v", stats)
	}
	if stats.AvgLatency < 0 || stats.EventsPerSec <= 0 {
		t.Errorf("unexpected latency or throughput: %+v", stats)
	}
	// Query objects expose live stats, so inspect them after all producers finish.
	query, err := engine.GetQuery(q.ID)
	if err != nil {
		t.Fatal(err)
	}
	if query.Stats.EventsProcessed != matchingEvents || query.Stats.ResultsEmitted != matchingEvents {
		t.Errorf("unexpected final query counts: %+v", query.Stats)
	}
	results := engine.GetResults(q.ID)
	if len(results) != matchingEvents {
		t.Fatalf("result count = %d, want %d", len(results), matchingEvents)
	}
	seen := make(map[float64]bool, matchingEvents)
	for _, result := range results {
		if len(result.Rows) != 1 || result.Rows[0]["metric"] != "cpu.usage" {
			t.Fatalf("unexpected result rows: %+v", result.Rows)
		}
		value, ok := result.Rows[0]["value"].(float64)
		if !ok || value < 0 || value >= matchingEvents || seen[value] {
			t.Fatalf("missing or duplicate event value: %v", result.Rows[0]["value"])
		}
		seen[value] = true
	}
	if initial != (StreamDSLV2Stats{}) {
		t.Errorf("initial snapshot changed: %+v", initial)
	}
	encoded, err := json.Marshal(stats)
	if err != nil {
		t.Fatal(err)
	}
	var decoded StreamDSLV2Stats
	if err := json.Unmarshal(encoded, &decoded); err != nil {
		t.Fatalf("decode numeric stats: %v", err)
	}
	if decoded != stats {
		t.Errorf("stats JSON round trip = %+v, want %+v", decoded, stats)
	}
}
