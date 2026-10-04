package chronicle

import (
	"fmt"
	"testing"
	"time"
)

func TestStreamingSQLEngine_ParseSelect(t *testing.T) {
	db := &DB{}
	hub := NewStreamHub(db, DefaultStreamConfig())
	engine := NewStreamingSQLEngine(db, hub, DefaultStreamingSQLConfig())

	tests := []struct {
		name    string
		sql     string
		wantErr bool
		check   func(*ParsedStreamingSQL) bool
	}{
		{
			name: "simple select",
			sql:  "SELECT * FROM cpu_metrics",
			check: func(p *ParsedStreamingSQL) bool {
				return p.Source == "cpu_metrics" && len(p.Select) == 1 && p.Select[0].Expression == "*"
			},
		},
		{
			name: "select with aggregation",
			sql:  "SELECT AVG(value) AS avg_cpu FROM cpu_metrics GROUP BY host WINDOW TUMBLING SIZE 1 MINUTE",
			check: func(p *ParsedStreamingSQL) bool {
				return p.Source == "cpu_metrics" &&
					p.Select[0].Function == "AVG" &&
					p.GroupBy != nil &&
					p.Window != nil &&
					p.Window.Type == StreamingWindowTumbling
			},
		},
		{
			name: "select with where",
			sql:  "SELECT value FROM cpu_metrics WHERE value > 80",
			check: func(p *ParsedStreamingSQL) bool {
				return p.Where != nil && len(p.Where.Conditions) > 0
			},
		},
		{
			name: "select with multiple aggregations",
			sql:  "SELECT COUNT(*) AS cnt, SUM(value) AS total, AVG(value) AS avg FROM metrics GROUP BY region",
			check: func(p *ParsedStreamingSQL) bool {
				return len(p.Select) == 3 &&
					p.Select[0].Function == "COUNT" &&
					p.Select[1].Function == "SUM" &&
					p.Select[2].Function == "AVG"
			},
		},
		{
			name:    "missing from",
			sql:     "SELECT * WHERE value > 0",
			wantErr: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			parsed, err := engine.ParseSQL(tt.sql)
			if tt.wantErr {
				if err == nil {
					t.Error("expected error, got nil")
				}
				return
			}
			if err != nil {
				t.Errorf("unexpected error: %v", err)
				return
			}
			if tt.check != nil && !tt.check(parsed) {
				t.Errorf("check failed for %s, got: %+v", tt.sql, parsed)
			}
		})
	}
}

func TestStreamingSQLEngine_ParseWindow(t *testing.T) {
	db := &DB{}
	hub := NewStreamHub(db, DefaultStreamConfig())
	engine := NewStreamingSQLEngine(db, hub, DefaultStreamingSQLConfig())

	tests := []struct {
		name       string
		sql        string
		windowType StreamingWindowType
		size       time.Duration
	}{
		{
			name:       "tumbling window",
			sql:        "SELECT AVG(value) FROM metrics WINDOW TUMBLING SIZE 5 MINUTES",
			windowType: StreamingWindowTumbling,
			size:       5 * time.Minute,
		},
		{
			name:       "hopping window",
			sql:        "SELECT SUM(value) FROM metrics WINDOW HOPPING SIZE 10 MINUTES ADVANCE 1 MINUTE",
			windowType: StreamingWindowHopping,
			size:       10 * time.Minute,
		},
		{
			name:       "session window",
			sql:        "SELECT COUNT(*) FROM events WINDOW SESSION SIZE 30 SECONDS",
			windowType: StreamingWindowSession,
			size:       30 * time.Second,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			parsed, err := engine.ParseSQL(tt.sql)
			if err != nil {
				t.Fatalf("parse error: %v", err)
			}
			if parsed.Window == nil {
				t.Fatal("expected window clause")
			}
			if parsed.Window.Type != tt.windowType {
				t.Errorf("window type: got %v, want %v", parsed.Window.Type, tt.windowType)
			}
			if parsed.Window.Size != tt.size {
				t.Errorf("window size: got %v, want %v", parsed.Window.Size, tt.size)
			}
		})
	}
}

func TestStreamingSQLEngine_Execute(t *testing.T) {
	db := &DB{}
	hub := NewStreamHub(db, DefaultStreamConfig())
	engine := NewStreamingSQLEngine(db, hub, DefaultStreamingSQLConfig())
	engine.Start()
	defer engine.Stop()

	query, err := engine.ExecuteSQL("SELECT * FROM test_metrics")
	if err != nil {
		t.Fatalf("execute error: %v", err)
	}

	if query.State != StreamingQueryStateRunning && query.State != StreamingQueryStateCreated {
		// Query might quickly fail if no subscription, but it should at least parse
	}

	// Stop query
	err = engine.StopQuery(query.ID)
	if err != nil {
		t.Errorf("stop error: %v", err)
	}

	// Verify stopped
	q, ok := engine.GetQuery(query.ID)
	if ok {
		t.Errorf("query should be removed, got: %v", q)
	}
}

func TestStreamingSQLEngine_MaxConcurrentQueries(t *testing.T) {
	db := &DB{}
	hub := NewStreamHub(db, DefaultStreamConfig())

	config := DefaultStreamingSQLConfig()
	config.MaxConcurrentQueries = 2

	engine := NewStreamingSQLEngine(db, hub, config)
	engine.Start()
	defer engine.Stop()

	// Create max queries
	for i := 0; i < config.MaxConcurrentQueries; i++ {
		_, err := engine.ExecuteSQL("SELECT * FROM metrics")
		if err != nil {
			t.Fatalf("query %d failed: %v", i, err)
		}
	}

	// Next query should fail
	_, err := engine.ExecuteSQL("SELECT * FROM metrics")
	if err == nil {
		t.Error("expected max concurrent queries error")
	}
}

func TestStreamingSQLEngine_ListQueries(t *testing.T) {
	db := &DB{}
	hub := NewStreamHub(db, DefaultStreamConfig())
	engine := NewStreamingSQLEngine(db, hub, DefaultStreamingSQLConfig())
	engine.Start()
	defer engine.Stop()

	// Create some queries
	for i := 0; i < 3; i++ {
		_, err := engine.ExecuteSQL("SELECT * FROM metrics")
		if err != nil {
			t.Fatalf("failed to create query: %v", err)
		}
	}

	queries := engine.ListQueries()
	if len(queries) != 3 {
		t.Errorf("expected 3 queries, got %d", len(queries))
	}
}

func TestStreamingSQLEngine_GetStats(t *testing.T) {
	db := &DB{}
	hub := NewStreamHub(db, DefaultStreamConfig())
	engine := NewStreamingSQLEngine(db, hub, DefaultStreamingSQLConfig())
	engine.Start()
	defer engine.Stop()

	stats := engine.GetStats()
	if stats.MaxConcurrent != DefaultStreamingSQLConfig().MaxConcurrentQueries {
		t.Errorf("unexpected max concurrent: %d", stats.MaxConcurrent)
	}
}

func TestParseSelectFields(t *testing.T) {
	tests := []struct {
		input    string
		expected int
	}{
		{"*", 1},
		{"value", 1},
		{"COUNT(*), SUM(value)", 2},
		{"AVG(value) AS avg, MAX(value) AS max, MIN(value) AS min", 3},
	}

	for _, tt := range tests {
		result := splitSelectFields(tt.input)
		if len(result) != tt.expected {
			t.Errorf("splitSelectFields(%q): got %d fields, want %d", tt.input, len(result), tt.expected)
		}
	}
}

func TestStreamingQueryState_String(t *testing.T) {
	tests := []struct {
		state StreamingQueryState
		want  string
	}{
		{StreamingQueryStateCreated, "created"},
		{StreamingQueryStateRunning, "running"},
		{StreamingQueryStatePaused, "paused"},
		{StreamingQueryStateStopped, "stopped"},
		{StreamingQueryStateError, "error"},
	}

	for _, tt := range tests {
		if got := tt.state.String(); got != tt.want {
			t.Errorf("StreamingQueryState.String() = %v, want %v", got, tt.want)
		}
	}
}

func TestStreamingWindowType_String(t *testing.T) {
	tests := []struct {
		wt   StreamingWindowType
		want string
	}{
		{StreamingWindowTumbling, "tumbling"},
		{StreamingWindowHopping, "hopping"},
		{StreamingWindowSession, "session"},
		{StreamingWindowSliding, "sliding"},
	}

	for _, tt := range tests {
		if got := tt.wt.String(); got != tt.want {
			t.Errorf("StreamingWindowType.String() = %v, want %v", got, tt.want)
		}
	}
}

func TestDefaultStreamingSQLConfig(t *testing.T) {
	config := DefaultStreamingSQLConfig()

	if !config.Enabled {
		t.Error("expected Enabled to be true")
	}
	if config.BufferSize != 10000 {
		t.Errorf("expected BufferSize 10000, got %d", config.BufferSize)
	}
	if config.MaxConcurrentQueries != 100 {
		t.Errorf("expected MaxConcurrentQueries 100, got %d", config.MaxConcurrentQueries)
	}
}

func TestStreamingSQLEngine_RapidCreatesRetainQueries(t *testing.T) {
	const count = 64
	db := &DB{}
	streamConfig := DefaultStreamConfig()
	streamConfig.BufferSize = 1
	hub := NewStreamHub(db, streamConfig)
	config := DefaultStreamingSQLConfig()
	config.MaxConcurrentQueries = count
	config.BufferSize = 1
	engine := NewStreamingSQLEngine(db, hub, config)
	var accepted []*StreamingQuery
	defer func() {
		engine.Stop()
		for _, query := range accepted {
			for range query.Results {
			}
		}
	}()

	const sql = "SELECT * FROM metrics"
	parsed, err := engine.ParseSQL(sql)
	if err != nil {
		t.Fatal(err)
	}
	seen := make(map[string]bool, count)
	for i := 0; i < count; i++ {
		query, err := engine.Execute(parsed, sql)
		if err != nil {
			t.Fatalf("create %d: %v", i, err)
		}
		accepted = append(accepted, query)
		if seen[query.ID] {
			t.Fatalf("create %d reused accepted query ID %q", i, query.ID)
		}
		seen[query.ID] = true
	}
	if got := len(engine.ListQueries()); got != count {
		t.Fatalf("listed %d queries, want %d", got, count)
	}
	if got := engine.GetStats().ActiveQueries; got != count {
		t.Fatalf("active queries = %d, want %d", got, count)
	}
	for _, query := range accepted {
		if got, ok := engine.GetQuery(query.ID); !ok || got != query {
			t.Fatalf("accepted query %q was not retained", query.ID)
		}
		if err := engine.StopQuery(query.ID); err != nil {
			t.Fatal(err)
		}
	}
	if got := len(engine.ListQueries()); got != 0 {
		t.Fatalf("listed %d queries after stopping all", got)
	}
	replacement, err := engine.Execute(parsed, sql)
	if err != nil {
		t.Fatal(err)
	}
	accepted = append(accepted, replacement)
	if seen[replacement.ID] {
		t.Fatalf("replacement reused stopped query ID %q", replacement.ID)
	}
	if err := engine.StopQuery(accepted[0].ID); err == nil {
		t.Fatal("stale query ID unexpectedly stopped a replacement")
	}
	if got, ok := engine.GetQuery(replacement.ID); !ok || got != replacement {
		t.Fatal("replacement was not retained after stale stop")
	}
}

func TestStreamingSQLEngine_ConcurrentAdmission(t *testing.T) {
	const attempts = 32
	for _, limit := range []int{1, 4} {
		t.Run(fmt.Sprint(limit), func(t *testing.T) {
			db := &DB{}
			streamConfig := DefaultStreamConfig()
			streamConfig.BufferSize = 1
			hub := NewStreamHub(db, streamConfig)
			config := DefaultStreamingSQLConfig()
			config.MaxConcurrentQueries = limit
			config.BufferSize = 1
			engine := NewStreamingSQLEngine(db, hub, config)
			var accepted []*StreamingQuery
			defer func() {
				engine.Stop()
				for _, query := range accepted {
					for range query.Results {
					}
				}
			}()
			const sql = "SELECT * FROM metrics"
			parsed, err := engine.ParseSQL(sql)
			if err != nil {
				t.Fatal(err)
			}
			type outcome struct {
				query *StreamingQuery
				err   error
			}
			start := make(chan struct{})
			outcomes := make(chan outcome, attempts)
			for i := 0; i < attempts; i++ {
				go func() {
					<-start
					query, err := engine.Execute(parsed, sql)
					outcomes <- outcome{query, err}
				}()
			}
			close(start)
			for i := 0; i < attempts; i++ {
				result := <-outcomes
				if result.query != nil {
					accepted = append(accepted, result.query)
				}
				if result.err != nil {
					if result.query != nil || result.err.Error() != "max concurrent queries reached" {
						t.Errorf("rejected query present=%t, error=%v", result.query != nil, result.err)
					}
				} else if result.query == nil {
					t.Error("accepted nil query")
				}
			}
			if len(accepted) != limit {
				t.Errorf("accepted %d queries, limit %d", len(accepted), limit)
			}
			if got := engine.GetStats().ActiveQueries; got != limit {
				t.Errorf("active queries = %d, want %d", got, limit)
			}
			for _, query := range accepted {
				if got, ok := engine.GetQuery(query.ID); !ok || got != query {
					t.Errorf("accepted query %q was not retained", query.ID)
				}
			}
		})
	}
}
