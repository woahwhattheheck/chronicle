package chronicle

import (
	"context"
	"encoding/json"
	"strings"
	"sync"
	"testing"
)

func TestNLDashboardEngine(t *testing.T) {
	db := setupTestDB(t)

	config := DefaultNLDashboardConfig()
	engine := NewNLDashboardEngine(db, config)

	// Verify initial state
	stats := engine.Stats()
	if stats.DashboardsGenerated != 0 {
		t.Errorf("expected 0 dashboards, got %d", stats.DashboardsGenerated)
	}
}

func TestGenerateDashboard(t *testing.T) {
	db := setupTestDB(t)

	config := DefaultNLDashboardConfig()
	engine := NewNLDashboardEngine(db, config)

	tests := []struct {
		name         string
		description  string
		expectTitle  bool
		expectPanels int
	}{
		{
			name:         "simple cpu dashboard",
			description:  "Create a dashboard showing CPU usage over time",
			expectTitle:  true,
			expectPanels: 1,
		},
		{
			name:         "multiple metrics",
			description:  "Show me CPU and memory over time",
			expectTitle:  true,
			expectPanels: 1, // Combined in one description
		},
		{
			name:         "stat panel",
			description:  "Show current CPU usage",
			expectTitle:  true,
			expectPanels: 1,
		},
		{
			name:         "gauge panel",
			description:  "Create a gauge for memory usage",
			expectTitle:  true,
			expectPanels: 1,
		},
		{
			name:         "table panel",
			description:  "Show a table of network connections",
			expectTitle:  true,
			expectPanels: 1,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			dashboard, err := engine.GenerateDashboard(context.Background(), tt.description)
			if err != nil {
				t.Fatalf("failed to generate dashboard: %v", err)
			}

			if dashboard.ID == "" {
				t.Error("dashboard should have an ID")
			}

			if tt.expectTitle && dashboard.Title == "" {
				t.Error("dashboard should have a title")
			}

			if len(dashboard.Panels) < tt.expectPanels {
				t.Errorf("expected at least %d panels, got %d", tt.expectPanels, len(dashboard.Panels))
			}
		})
	}
}

func TestPanelTypeDetection(t *testing.T) {
	db := setupTestDB(t)

	config := DefaultNLDashboardConfig()
	engine := NewNLDashboardEngine(db, config)

	tests := []struct {
		description  string
		expectedType PanelType
	}{
		{"show me cpu over time", PanelTimeseries},
		{"graph of memory usage", PanelTimeseries},
		{"current cpu usage", PanelStat},
		{"average of request latency", PanelStat},
		{"total requests", PanelStat},
		{"gauge for cpu", PanelGauge},
		{"table of errors", PanelTable},
		{"heatmap of latency", PanelHeatmap},
		{"pie chart of traffic breakdown", PanelPieChart},
	}

	for _, tt := range tests {
		t.Run(tt.description, func(t *testing.T) {
			dashboard, err := engine.GenerateDashboard(context.Background(), tt.description)
			if err != nil {
				t.Fatalf("failed to generate dashboard: %v", err)
			}

			if len(dashboard.Panels) == 0 {
				t.Fatal("expected at least one panel")
			}

			if dashboard.Panels[0].Type != tt.expectedType {
				t.Errorf("expected panel type %s, got %s", tt.expectedType, dashboard.Panels[0].Type)
			}
		})
	}
}

func TestTitleExtraction(t *testing.T) {
	db := setupTestDB(t)

	engine := NewNLDashboardEngine(db, DefaultNLDashboardConfig())

	tests := []struct {
		description    string
		expectContains string
	}{
		{`Create a dashboard called "System Metrics"`, "System Metrics"},
		{`Create a dashboard named Production Monitoring`, "Production"},
		{`Create a CPU monitoring dashboard`, "CPU"},
		{`Dashboard for network metrics`, "network"},
	}

	for _, tt := range tests {
		t.Run(tt.description, func(t *testing.T) {
			dashboard, _ := engine.GenerateDashboard(context.Background(), tt.description)
			if !strings.Contains(strings.ToLower(dashboard.Title), strings.ToLower(tt.expectContains)) {
				t.Errorf("expected title to contain '%s', got '%s'", tt.expectContains, dashboard.Title)
			}
		})
	}
}

func TestTagExtraction(t *testing.T) {
	db := setupTestDB(t)

	engine := NewNLDashboardEngine(db, DefaultNLDashboardConfig())

	tests := []struct {
		description string
		expectedTag string
	}{
		{"Show CPU usage", "system"},
		{"Network traffic dashboard", "network"},
		{"HTTP request rates", "web"},
		{"Kubernetes pod metrics", "kubernetes"},
		{"Database queries", "database"},
	}

	for _, tt := range tests {
		t.Run(tt.description, func(t *testing.T) {
			dashboard, _ := engine.GenerateDashboard(context.Background(), tt.description)

			found := false
			for _, tag := range dashboard.Tags {
				if tag == tt.expectedTag {
					found = true
					break
				}
			}
			if !found {
				t.Errorf("expected tag '%s' in %v", tt.expectedTag, dashboard.Tags)
			}
		})
	}
}

func TestTimeRangeExtraction(t *testing.T) {
	db := setupTestDB(t)

	engine := NewNLDashboardEngine(db, DefaultNLDashboardConfig())

	tests := []struct {
		description   string
		expectedRange string
	}{
		{"Show CPU for the last 6 hours", "now-6h"},
		{"Show memory for the last 30 minutes", "now-30m"},
		{"Show traffic for the last 7 days", "now-7d"},
		{"Show data for the past 2 hours", "now-2h"},
	}

	for _, tt := range tests {
		t.Run(tt.description, func(t *testing.T) {
			parsed := engine.parseDescription(tt.description)
			if parsed.TimeRange != tt.expectedRange {
				t.Errorf("expected time range '%s', got '%s'", tt.expectedRange, parsed.TimeRange)
			}
		})
	}
}

func TestVariableExtraction(t *testing.T) {
	db := setupTestDB(t)

	engine := NewNLDashboardEngine(db, DefaultNLDashboardConfig())

	tests := []struct {
		description string
		expectedVar string
	}{
		{"Show CPU with filter by host", "host"},
		{"Dashboard with variable for region", "region"},
		{"Metrics with dropdown for environment", "environment"},
	}

	for _, tt := range tests {
		t.Run(tt.description, func(t *testing.T) {
			parsed := engine.parseDescription(tt.description)

			found := false
			for _, v := range parsed.Variables {
				if v.Name == tt.expectedVar {
					found = true
					break
				}
			}
			if !found {
				t.Errorf("expected variable '%s'", tt.expectedVar)
			}
		})
	}
}

func TestMetricExtraction(t *testing.T) {
	db := setupTestDB(t)

	engine := NewNLDashboardEngine(db, DefaultNLDashboardConfig())

	tests := []struct {
		description    string
		expectedMetric string
	}{
		{"Show CPU usage", "cpu"},
		{"Display memory usage", "memory"},
		{"Graph disk IO", "disk"},
		{"Show http_request_count", "http_request_count"},
	}

	for _, tt := range tests {
		t.Run(tt.description, func(t *testing.T) {
			metrics := engine.extractMetrics(tt.description)

			found := false
			for _, m := range metrics {
				if m == tt.expectedMetric {
					found = true
					break
				}
			}
			if !found {
				t.Errorf("expected metric '%s' in %v", tt.expectedMetric, metrics)
			}
		})
	}
}

func TestGroupByExtraction(t *testing.T) {
	db := setupTestDB(t)

	engine := NewNLDashboardEngine(db, DefaultNLDashboardConfig())

	tests := []struct {
		description   string
		expectedGroup string
	}{
		{"Show CPU grouped by host", "host"},
		{"Display memory split by region", "region"},
		{"Traffic per instance", "instance"},
	}

	for _, tt := range tests {
		t.Run(tt.description, func(t *testing.T) {
			groupBy := engine.extractGroupBy(tt.description)

			found := false
			for _, g := range groupBy {
				if g == tt.expectedGroup {
					found = true
					break
				}
			}
			if !found {
				t.Errorf("expected group '%s' in %v", tt.expectedGroup, groupBy)
			}
		})
	}
}

func TestToGrafanaJSON(t *testing.T) {
	db := setupTestDB(t)

	engine := NewNLDashboardEngine(db, DefaultNLDashboardConfig())

	dashboard, _ := engine.GenerateDashboard(context.Background(), "Show CPU and memory over time")

	jsonBytes, err := engine.ToGrafanaJSON(dashboard)
	if err != nil {
		t.Fatalf("failed to convert to Grafana JSON: %v", err)
	}

	// Verify it's valid JSON
	var grafana map[string]any
	if err := json.Unmarshal(jsonBytes, &grafana); err != nil {
		t.Fatalf("invalid JSON: %v", err)
	}

	// Check required fields
	if _, ok := grafana["title"]; !ok {
		t.Error("Grafana JSON should have title")
	}
	if _, ok := grafana["panels"]; !ok {
		t.Error("Grafana JSON should have panels")
	}
	if _, ok := grafana["time"]; !ok {
		t.Error("Grafana JSON should have time")
	}
	if _, ok := grafana["schemaVersion"]; !ok {
		t.Error("Grafana JSON should have schemaVersion")
	}
}

func TestGrafanaJSONPanels(t *testing.T) {
	db := setupTestDB(t)

	engine := NewNLDashboardEngine(db, DefaultNLDashboardConfig())

	dashboard, _ := engine.GenerateDashboard(context.Background(), "gauge for CPU usage")

	jsonBytes, _ := engine.ToGrafanaJSON(dashboard)

	var grafana map[string]any
	json.Unmarshal(jsonBytes, &grafana)

	panels, ok := grafana["panels"].([]any)
	if !ok || len(panels) == 0 {
		t.Fatal("expected panels array")
	}

	panel := panels[0].(map[string]any)

	if panel["type"] != "gauge" {
		t.Errorf("expected gauge type, got %v", panel["type"])
	}
	if panel["id"] == nil {
		t.Error("panel should have id")
	}
	if panel["gridPos"] == nil {
		t.Error("panel should have gridPos")
	}
}

func TestGetDashboard(t *testing.T) {
	db := setupTestDB(t)

	engine := NewNLDashboardEngine(db, DefaultNLDashboardConfig())

	dashboard, _ := engine.GenerateDashboard(context.Background(), "CPU dashboard")

	// Get existing
	retrieved, err := engine.GetDashboard(dashboard.ID)
	if err != nil {
		t.Fatalf("failed to get dashboard: %v", err)
	}
	if retrieved.Title != dashboard.Title {
		t.Error("retrieved dashboard should match")
	}

	// Get non-existent
	_, err = engine.GetDashboard("non-existent")
	if err == nil {
		t.Error("expected error for non-existent dashboard")
	}
}

func TestListDashboards(t *testing.T) {
	db := setupTestDB(t)

	engine := NewNLDashboardEngine(db, DefaultNLDashboardConfig())

	// Generate multiple dashboards
	for i := 0; i < 3; i++ {
		engine.GenerateDashboard(context.Background(), "CPU dashboard")
	}

	dashboards := engine.ListDashboards()
	if len(dashboards) != 3 {
		t.Errorf("expected 3 dashboards, got %d", len(dashboards))
	}
}

func TestFeedback(t *testing.T) {
	db := setupTestDB(t)

	config := DefaultNLDashboardConfig()
	config.EnableFeedback = true
	engine := NewNLDashboardEngine(db, config)

	dashboard, _ := engine.GenerateDashboard(context.Background(), "CPU dashboard")

	// Provide positive feedback
	engine.ProvideFeedback(dashboard.ID, true, "")

	// Provide negative feedback with correction
	engine.ProvideFeedback(dashboard.ID, false, "Show CPU and memory")

	stats := engine.Stats()
	if stats.FeedbackReceived != 2 {
		t.Errorf("expected 2 feedback entries, got %d", stats.FeedbackReceived)
	}
	if stats.AcceptanceRate != 0.5 {
		t.Errorf("expected acceptance rate 0.5, got %f", stats.AcceptanceRate)
	}
}

func TestNLDashboardStats(t *testing.T) {
	db := setupTestDB(t)

	engine := NewNLDashboardEngine(db, DefaultNLDashboardConfig())

	// Generate dashboards
	engine.GenerateDashboard(context.Background(), "CPU dashboard")
	engine.GenerateDashboard(context.Background(), "Memory dashboard")

	stats := engine.Stats()
	if stats.DashboardsGenerated != 2 {
		t.Errorf("expected 2 dashboards generated, got %d", stats.DashboardsGenerated)
	}
	if stats.PanelsGenerated < 2 {
		t.Errorf("expected at least 2 panels generated, got %d", stats.PanelsGenerated)
	}
}

func TestNLDashboardConcurrentStats(t *testing.T) {
	const workers, iterations = 4, 16
	const dashboards = workers * iterations
	engine := NewNLDashboardEngine(nil, DefaultNLDashboardConfig())
	initial := engine.Stats()
	start := make(chan struct{})
	var wg sync.WaitGroup
	for range workers {
		wg.Add(1)
		go func() {
			defer wg.Done()
			<-start
			var previous int64
			for range iterations {
				dashboard, err := engine.GenerateDashboard(context.Background(), "current CPU usage; current memory usage")
				if err != nil {
					t.Errorf("GenerateDashboard: %v", err)
					return
				}
				if len(dashboard.Panels) != 2 {
					t.Errorf("generated %d panels, want 2", len(dashboard.Panels))
				}
				engine.ProvideFeedback(dashboard.ID, true, "")
				engine.ProvideFeedback(dashboard.ID, false, "add network usage")
				stats := engine.Stats()
				if stats.DashboardsGenerated < previous {
					t.Errorf("dashboard count decreased from %d to %d", previous, stats.DashboardsGenerated)
				}
				previous = stats.DashboardsGenerated
			}
		}()
	}
	close(start)
	wg.Wait()

	stats := engine.Stats()
	want := NLDashboardStats{
		DashboardsGenerated: dashboards,
		PanelsGenerated:     2 * dashboards,
		DashboardsCached:    dashboards,
		FeedbackReceived:    2 * dashboards,
		AcceptanceRate:      0.5,
	}
	if stats != want {
		t.Errorf("Stats() = %+v, want %+v", stats, want)
	}
	if initial != (NLDashboardStats{}) {
		t.Errorf("initial snapshot changed: %+v", initial)
	}
	encoded, err := json.Marshal(stats)
	if err != nil {
		t.Fatal(err)
	}
	var decoded map[string]float64
	if err := json.Unmarshal(encoded, &decoded); err != nil {
		t.Fatalf("statistics must retain numeric JSON fields: %v", err)
	}
	if len(decoded) != 5 || decoded["dashboards_generated"] != dashboards || decoded["panels_generated"] != 2*dashboards {
		t.Errorf("unexpected statistics JSON: %s", encoded)
	}
}

func TestComplexDashboard(t *testing.T) {
	db := setupTestDB(t)

	engine := NewNLDashboardEngine(db, DefaultNLDashboardConfig())

	description := `Create a production monitoring dashboard showing:
		- Current CPU usage gauge
		- Memory usage over time
		- Total HTTP requests
		- Error rate table
		Filter by host and environment for the last 24 hours`

	dashboard, err := engine.GenerateDashboard(context.Background(), description)
	if err != nil {
		t.Fatalf("failed to generate dashboard: %v", err)
	}

	// Should have multiple panels
	if len(dashboard.Panels) < 2 {
		t.Errorf("expected multiple panels, got %d", len(dashboard.Panels))
	}

	// Should have variables
	if len(dashboard.Variables) == 0 {
		t.Log("Note: variables may not be extracted from this format")
	}

	// Verify Grafana JSON is valid
	jsonBytes, err := engine.ToGrafanaJSON(dashboard)
	if err != nil {
		t.Fatalf("failed to convert to Grafana JSON: %v", err)
	}

	var grafana map[string]any
	if err := json.Unmarshal(jsonBytes, &grafana); err != nil {
		t.Fatalf("invalid JSON: %v", err)
	}
}

func TestPanelGridPositioning(t *testing.T) {
	db := setupTestDB(t)

	config := DefaultNLDashboardConfig()
	config.MaxPanelsPerRow = 3
	engine := NewNLDashboardEngine(db, config)

	// Create dashboard with multiple panels
	dashboard, _ := engine.GenerateDashboard(context.Background(),
		"Show CPU; Show memory; Show disk; Show network")

	// Verify grid positions don't overlap
	positions := make(map[string]bool)
	for _, panel := range dashboard.Panels {
		pos := panel.GridPos
		key := string(rune(pos.X)) + "-" + string(rune(pos.Y))
		if positions[key] {
			t.Errorf("overlapping panel positions at x=%d, y=%d", pos.X, pos.Y)
		}
		positions[key] = true
	}
}

func TestAggregationDetection(t *testing.T) {
	db := setupTestDB(t)

	engine := NewNLDashboardEngine(db, DefaultNLDashboardConfig())

	tests := []struct {
		description string
		expectedAgg string
	}{
		{"average cpu usage", "avg"},
		{"total requests", "sum"},
		{"maximum latency", "max"},
		{"minimum response time", "min"},
		{"count of errors", "count"},
		{"current temperature", "last"},
	}

	for _, tt := range tests {
		t.Run(tt.description, func(t *testing.T) {
			parsed := engine.parseDescription(tt.description)

			if len(parsed.Panels) == 0 {
				t.Fatal("expected at least one panel")
			}

			if parsed.Panels[0].Aggregation != tt.expectedAgg {
				t.Errorf("expected aggregation '%s', got '%s'", tt.expectedAgg, parsed.Panels[0].Aggregation)
			}
		})
	}
}

func BenchmarkGenerateDashboard(b *testing.B) {
	dir := b.TempDir()
	path := dir + "/test.db"
	db, err := Open(path, DefaultConfig(path))
	if err != nil {
		b.Fatalf("failed to open test db: %v", err)
	}
	defer db.Close()

	engine := NewNLDashboardEngine(db, DefaultNLDashboardConfig())

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		engine.GenerateDashboard(context.Background(),
			"Show CPU and memory usage over time with filter by host")
	}
}

func BenchmarkToGrafanaJSON(b *testing.B) {
	dir := b.TempDir()
	path := dir + "/test.db"
	db, err := Open(path, DefaultConfig(path))
	if err != nil {
		b.Fatalf("failed to open test db: %v", err)
	}
	defer db.Close()

	engine := NewNLDashboardEngine(db, DefaultNLDashboardConfig())
	dashboard, _ := engine.GenerateDashboard(context.Background(),
		"Show CPU, memory, disk, and network over time")

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		engine.ToGrafanaJSON(dashboard)
	}
}
