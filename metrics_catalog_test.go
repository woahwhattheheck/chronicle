package chronicle

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"
)

func TestMetricsCatalogDefaultConfig(t *testing.T) {
	cfg := DefaultMetricsCatalogConfig()
	if !cfg.Enabled {
		t.Error("expected Enabled to be true")
	}
	if cfg.ScanInterval != time.Hour {
		t.Errorf("expected ScanInterval 1h, got %v", cfg.ScanInterval)
	}
	if cfg.MaxMetrics != 10000 {
		t.Errorf("expected MaxMetrics 10000, got %d", cfg.MaxMetrics)
	}
	if !cfg.TrackLineage {
		t.Error("expected TrackLineage to be true")
	}
	if !cfg.TrackQueryUsage {
		t.Error("expected TrackQueryUsage to be true")
	}
	if cfg.DeprecationGracePeriod != 7*24*time.Hour {
		t.Errorf("expected DeprecationGracePeriod 7d, got %v", cfg.DeprecationGracePeriod)
	}
}

func TestMetricsCatalogRegisterMetric(t *testing.T) {
	mc := NewMetricsCatalog(nil, DefaultMetricsCatalogConfig())

	err := mc.RegisterMetric(CatalogEntry{
		Name:        "cpu.usage",
		Description: "CPU usage percentage",
		Type:        CatalogMetricTypeGauge,
		Unit:        "percent",
		Owner:       "infra-team",
	})
	if err != nil {
		t.Fatalf("RegisterMetric failed: %v", err)
	}

	entry := mc.GetEntry("cpu.usage")
	if entry == nil {
		t.Fatal("expected entry to exist")
	}
	if entry.Description != "CPU usage percentage" {
		t.Errorf("expected description 'CPU usage percentage', got %q", entry.Description)
	}
	if entry.Type != CatalogMetricTypeGauge {
		t.Errorf("expected type gauge, got %v", entry.Type)
	}
	if entry.Owner != "infra-team" {
		t.Errorf("expected owner 'infra-team', got %q", entry.Owner)
	}
	if entry.FirstSeen.IsZero() {
		t.Error("expected FirstSeen to be set")
	}
}

func TestMetricsCatalogRegisterMetricEmptyName(t *testing.T) {
	mc := NewMetricsCatalog(nil, DefaultMetricsCatalogConfig())
	err := mc.RegisterMetric(CatalogEntry{})
	if err == nil {
		t.Fatal("expected error for empty name")
	}
}

func TestMetricsCatalogMaxMetrics(t *testing.T) {
	cfg := DefaultMetricsCatalogConfig()
	cfg.MaxMetrics = 2
	mc := NewMetricsCatalog(nil, cfg)

	_ = mc.RegisterMetric(CatalogEntry{Name: "a"})
	_ = mc.RegisterMetric(CatalogEntry{Name: "b"})
	err := mc.RegisterMetric(CatalogEntry{Name: "c"})
	if err == nil {
		t.Fatal("expected error when exceeding MaxMetrics")
	}
}

func TestMetricsCatalogScanWithDB(t *testing.T) {
	dir := t.TempDir()
	cfg := DefaultConfig(dir + "/test.db")
	db, err := Open(cfg.Path, cfg)
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	defer db.Close()

	now := time.Now().UnixNano()
	for i := 0; i < 10; i++ {
		_ = db.Write(Point{
			Metric:    "cpu.usage",
			Tags:      map[string]string{"host": "server1"},
			Value:     float64(50 + i),
			Timestamp: now + int64(i)*int64(time.Second),
		})
	}
	for i := 0; i < 10; i++ {
		_ = db.Write(Point{
			Metric:    "mem.bytes",
			Tags:      map[string]string{"host": "server1"},
			Value:     float64(1024 * (i + 1)),
			Timestamp: now + int64(i)*int64(time.Second),
		})
	}
	_ = db.Flush()

	mc := NewMetricsCatalog(db, DefaultMetricsCatalogConfig())
	if err := mc.Scan(); err != nil {
		t.Fatalf("Scan failed: %v", err)
	}

	entries := mc.Export()
	if len(entries) < 2 {
		t.Fatalf("expected at least 2 metrics, got %d", len(entries))
	}

	cpuEntry := mc.GetEntry("cpu.usage")
	if cpuEntry == nil {
		t.Fatal("expected cpu.usage entry")
	}
	if cpuEntry.PointCount == 0 {
		t.Error("expected non-zero point count")
	}

	stats := mc.Stats()
	if stats.ScanCount != 1 {
		t.Errorf("expected ScanCount 1, got %d", stats.ScanCount)
	}
}

func TestMetricsCatalogScanNilDB(t *testing.T) {
	mc := NewMetricsCatalog(nil, DefaultMetricsCatalogConfig())
	err := mc.Scan()
	if err != nil {
		t.Fatalf("Scan with nil DB should not error: %v", err)
	}
	stats := mc.Stats()
	if stats.ScanCount != 1 {
		t.Errorf("expected ScanCount 1, got %d", stats.ScanCount)
	}
}

func TestMetricsCatalogInferMetricTypeCounter(t *testing.T) {
	mc := NewMetricsCatalog(nil, DefaultMetricsCatalogConfig())

	// Monotonically increasing values = counter
	values := []float64{1, 2, 3, 4, 5, 6, 7, 8, 9, 10}
	typ := mc.inferMetricType(values)
	if typ != CatalogMetricTypeCounter {
		t.Errorf("expected counter, got %v", typ)
	}
}

func TestMetricsCatalogInferMetricTypeGauge(t *testing.T) {
	mc := NewMetricsCatalog(nil, DefaultMetricsCatalogConfig())

	// Values that go up and down = gauge
	values := []float64{50, 55, 48, 60, 42, 53, 47, 58, 51, 49}
	typ := mc.inferMetricType(values)
	if typ != CatalogMetricTypeGauge {
		t.Errorf("expected gauge, got %v", typ)
	}
}

func TestMetricsCatalogInferMetricTypeUnknown(t *testing.T) {
	mc := NewMetricsCatalog(nil, DefaultMetricsCatalogConfig())

	// Too few values
	typ := mc.inferMetricType([]float64{42})
	if typ != CatalogMetricTypeUnknown {
		t.Errorf("expected unknown for single value, got %v", typ)
	}
}

func TestMetricsCatalogInferUnit(t *testing.T) {
	mc := NewMetricsCatalog(nil, DefaultMetricsCatalogConfig())

	tests := []struct {
		name     string
		expected string
	}{
		{"http_request_bytes", "bytes"},
		{"request_duration_seconds", "seconds"},
		{"request_latency", "seconds"},
		{"cpu_usage", "percent"},
		{"disk_percent", "percent"},
		{"http_requests_total", "count"},
		{"error_count", "count"},
		{"cpu_temperature", "celsius"},
		{"random_metric", ""},
	}

	for _, tt := range tests {
		got := mc.inferUnit(tt.name)
		if got != tt.expected {
			t.Errorf("inferUnit(%q) = %q, want %q", tt.name, got, tt.expected)
		}
	}
}

func TestMetricsCatalogSearch(t *testing.T) {
	mc := NewMetricsCatalog(nil, DefaultMetricsCatalogConfig())

	_ = mc.RegisterMetric(CatalogEntry{Name: "cpu.usage", Type: CatalogMetricTypeGauge, Owner: "infra"})
	_ = mc.RegisterMetric(CatalogEntry{Name: "cpu.idle", Type: CatalogMetricTypeGauge, Owner: "infra"})
	_ = mc.RegisterMetric(CatalogEntry{Name: "mem.bytes", Type: CatalogMetricTypeCounter, Owner: "platform"})
	_ = mc.RegisterMetric(CatalogEntry{Name: "disk.io", Type: CatalogMetricTypeGauge, Owner: "platform"})

	t.Run("pattern match", func(t *testing.T) {
		result, err := mc.Search(CatalogSearchQuery{Pattern: "cpu.*"})
		if err != nil {
			t.Fatalf("Search failed: %v", err)
		}
		if result.Total != 2 {
			t.Errorf("expected 2 matches, got %d", result.Total)
		}
	})

	t.Run("filter by type", func(t *testing.T) {
		result, err := mc.Search(CatalogSearchQuery{Type: CatalogMetricTypeCounter})
		if err != nil {
			t.Fatalf("Search failed: %v", err)
		}
		if result.Total != 1 {
			t.Errorf("expected 1 counter, got %d", result.Total)
		}
		if result.Entries[0].Name != "mem.bytes" {
			t.Errorf("expected mem.bytes, got %s", result.Entries[0].Name)
		}
	})

	t.Run("filter by owner", func(t *testing.T) {
		result, err := mc.Search(CatalogSearchQuery{Owner: "infra"})
		if err != nil {
			t.Fatalf("Search failed: %v", err)
		}
		if result.Total != 2 {
			t.Errorf("expected 2 infra metrics, got %d", result.Total)
		}
	})

	t.Run("wildcard all", func(t *testing.T) {
		result, err := mc.Search(CatalogSearchQuery{Pattern: "*"})
		if err != nil {
			t.Fatalf("Search failed: %v", err)
		}
		if result.Total != 4 {
			t.Errorf("expected 4 total, got %d", result.Total)
		}
	})

	t.Run("pagination", func(t *testing.T) {
		result, err := mc.Search(CatalogSearchQuery{Limit: 2, Offset: 0})
		if err != nil {
			t.Fatalf("Search failed: %v", err)
		}
		if len(result.Entries) != 2 {
			t.Errorf("expected 2 entries, got %d", len(result.Entries))
		}
		if result.Total != 4 {
			t.Errorf("expected total 4, got %d", result.Total)
		}
	})
}

func TestMetricsCatalogSearchByStatus(t *testing.T) {
	mc := NewMetricsCatalog(nil, DefaultMetricsCatalogConfig())

	_ = mc.RegisterMetric(CatalogEntry{Name: "active_metric", Status: CatalogMetricStatusActive})
	_ = mc.RegisterMetric(CatalogEntry{Name: "deprecated_metric", Status: CatalogMetricStatusDeprecated})

	result, err := mc.Search(CatalogSearchQuery{Status: CatalogMetricStatusDeprecated})
	if err != nil {
		t.Fatalf("Search failed: %v", err)
	}
	if result.Total != 1 {
		t.Errorf("expected 1 deprecated metric, got %d", result.Total)
	}
}

func TestMetricsCatalogLineage(t *testing.T) {
	cfg := DefaultMetricsCatalogConfig()
	cfg.TrackLineage = true
	mc := NewMetricsCatalog(nil, cfg)

	_ = mc.RegisterMetric(CatalogEntry{Name: "raw.cpu"})

	lineage := mc.GetLineage("raw.cpu")
	if lineage == nil {
		t.Fatal("expected lineage to exist")
	}
	if lineage.MetricName != "raw.cpu" {
		t.Errorf("expected metric name raw.cpu, got %s", lineage.MetricName)
	}
}

func TestMetricsCatalogLineageDisabled(t *testing.T) {
	cfg := DefaultMetricsCatalogConfig()
	cfg.TrackLineage = false
	mc := NewMetricsCatalog(nil, cfg)

	_ = mc.RegisterMetric(CatalogEntry{Name: "raw.cpu"})

	lineage := mc.GetLineage("raw.cpu")
	if lineage != nil {
		t.Error("expected no lineage when tracking is disabled")
	}
}

func TestMetricsCatalogQueryUsage(t *testing.T) {
	cfg := DefaultMetricsCatalogConfig()
	cfg.TrackQueryUsage = true
	cfg.TrackLineage = true
	mc := NewMetricsCatalog(nil, cfg)

	_ = mc.RegisterMetric(CatalogEntry{Name: "cpu.usage"})

	mc.RecordQuery("cpu.usage", "SELECT * FROM cpu.usage", 10*time.Millisecond)
	mc.RecordQuery("cpu.usage", "SELECT * FROM cpu.usage", 20*time.Millisecond)
	mc.RecordQuery("cpu.usage", "SELECT avg FROM cpu.usage", 5*time.Millisecond)

	lineage := mc.GetLineage("cpu.usage")
	if lineage == nil {
		t.Fatal("expected lineage")
	}
	if len(lineage.Queries) != 2 {
		t.Fatalf("expected 2 distinct queries, got %d", len(lineage.Queries))
	}

	// Find the repeated query
	for _, q := range lineage.Queries {
		if q.Query == "SELECT * FROM cpu.usage" {
			if q.Count != 2 {
				t.Errorf("expected count 2, got %d", q.Count)
			}
		}
	}

	stats := mc.Stats()
	if stats.TotalQueries != 3 {
		t.Errorf("expected TotalQueries 3, got %d", stats.TotalQueries)
	}
}

func TestMetricsCatalogQueryUsageDisabled(t *testing.T) {
	cfg := DefaultMetricsCatalogConfig()
	cfg.TrackQueryUsage = false
	mc := NewMetricsCatalog(nil, cfg)

	mc.RecordQuery("cpu.usage", "SELECT *", time.Millisecond)
	stats := mc.Stats()
	if stats.TotalQueries != 0 {
		t.Errorf("expected 0 queries when tracking disabled, got %d", stats.TotalQueries)
	}
}

func TestMetricsCatalogDeprecation(t *testing.T) {
	mc := NewMetricsCatalog(nil, DefaultMetricsCatalogConfig())
	_ = mc.RegisterMetric(CatalogEntry{Name: "old.metric", Owner: "team-a"})

	report, err := mc.DeprecateMetric(DeprecationRequest{
		MetricName:  "old.metric",
		Reason:      "replaced by new.metric",
		Replacement: "new.metric",
		GracePeriod: 24 * time.Hour,
		NotifyOwner: true,
	})
	if err != nil {
		t.Fatalf("DeprecateMetric failed: %v", err)
	}
	if report.Metric != "old.metric" {
		t.Errorf("expected metric old.metric, got %s", report.Metric)
	}
	if report.Status != "grace_period" {
		t.Errorf("expected status grace_period, got %s", report.Status)
	}
	if report.Replacement != "new.metric" {
		t.Errorf("expected replacement new.metric, got %s", report.Replacement)
	}
	if report.GraceEndsAt.IsZero() {
		t.Error("expected GraceEndsAt to be set")
	}

	entry := mc.GetEntry("old.metric")
	if !entry.Deprecated {
		t.Error("expected entry to be deprecated")
	}
	if entry.Status != CatalogMetricStatusDeprecated {
		t.Errorf("expected deprecated status, got %v", entry.Status)
	}

	deprecated := mc.ListDeprecated()
	if len(deprecated) != 1 {
		t.Errorf("expected 1 deprecated, got %d", len(deprecated))
	}
}

func TestMetricsCatalogDeprecationNotFound(t *testing.T) {
	mc := NewMetricsCatalog(nil, DefaultMetricsCatalogConfig())
	_, err := mc.DeprecateMetric(DeprecationRequest{MetricName: "nonexistent"})
	if err == nil {
		t.Fatal("expected error for nonexistent metric")
	}
}

func TestMetricsCatalogUndeprecate(t *testing.T) {
	mc := NewMetricsCatalog(nil, DefaultMetricsCatalogConfig())
	_ = mc.RegisterMetric(CatalogEntry{Name: "test.metric"})

	_, _ = mc.DeprecateMetric(DeprecationRequest{
		MetricName: "test.metric",
		Reason:     "testing",
	})

	err := mc.UndeprecateMetric("test.metric")
	if err != nil {
		t.Fatalf("UndeprecateMetric failed: %v", err)
	}

	entry := mc.GetEntry("test.metric")
	if entry.Deprecated {
		t.Error("expected entry to not be deprecated")
	}
	if entry.Status != CatalogMetricStatusActive {
		t.Errorf("expected active status, got %v", entry.Status)
	}

	deprecated := mc.ListDeprecated()
	if len(deprecated) != 0 {
		t.Errorf("expected 0 deprecated, got %d", len(deprecated))
	}
}

func TestMetricsCatalogUndeprecateNotFound(t *testing.T) {
	mc := NewMetricsCatalog(nil, DefaultMetricsCatalogConfig())
	err := mc.UndeprecateMetric("nonexistent")
	if err == nil {
		t.Fatal("expected error for nonexistent metric")
	}
}

func TestMetricsCatalogSetOwner(t *testing.T) {
	mc := NewMetricsCatalog(nil, DefaultMetricsCatalogConfig())
	_ = mc.RegisterMetric(CatalogEntry{Name: "cpu.usage"})

	err := mc.SetOwner("cpu.usage", "sre-team")
	if err != nil {
		t.Fatalf("SetOwner failed: %v", err)
	}

	entry := mc.GetEntry("cpu.usage")
	if entry.Owner != "sre-team" {
		t.Errorf("expected owner sre-team, got %q", entry.Owner)
	}
}

func TestMetricsCatalogSetOwnerNotFound(t *testing.T) {
	mc := NewMetricsCatalog(nil, DefaultMetricsCatalogConfig())
	err := mc.SetOwner("nonexistent", "team")
	if err == nil {
		t.Fatal("expected error for nonexistent metric")
	}
}

func TestMetricsCatalogUpdateMetric(t *testing.T) {
	mc := NewMetricsCatalog(nil, DefaultMetricsCatalogConfig())
	_ = mc.RegisterMetric(CatalogEntry{Name: "cpu.usage"})

	err := mc.UpdateMetric("cpu.usage", map[string]string{
		"description": "updated desc",
		"owner":       "new-owner",
		"unit":        "percent",
		"team":        "backend",
	})
	if err != nil {
		t.Fatalf("UpdateMetric failed: %v", err)
	}

	entry := mc.GetEntry("cpu.usage")
	if entry.Description != "updated desc" {
		t.Errorf("expected updated desc, got %q", entry.Description)
	}
	if entry.Owner != "new-owner" {
		t.Errorf("expected new-owner, got %q", entry.Owner)
	}
	if entry.Unit != "percent" {
		t.Errorf("expected percent, got %q", entry.Unit)
	}
	if entry.Labels["team"] != "backend" {
		t.Errorf("expected label team=backend, got %q", entry.Labels["team"])
	}
}

func TestMetricsCatalogUpdateMetricNotFound(t *testing.T) {
	mc := NewMetricsCatalog(nil, DefaultMetricsCatalogConfig())
	err := mc.UpdateMetric("nonexistent", map[string]string{"description": "x"})
	if err == nil {
		t.Fatal("expected error for nonexistent metric")
	}
}

func TestMetricsCatalogStats(t *testing.T) {
	mc := NewMetricsCatalog(nil, DefaultMetricsCatalogConfig())

	_ = mc.RegisterMetric(CatalogEntry{Name: "m1", Owner: "team-a", Status: CatalogMetricStatusActive})
	_ = mc.RegisterMetric(CatalogEntry{Name: "m2", Owner: "team-b", Status: CatalogMetricStatusActive})
	_ = mc.RegisterMetric(CatalogEntry{Name: "m3", Owner: "team-a", Status: CatalogMetricStatusDeprecated, Deprecated: true})

	stats := mc.Stats()
	if stats.TotalMetrics != 3 {
		t.Errorf("expected 3 total, got %d", stats.TotalMetrics)
	}
	if stats.ActiveMetrics != 2 {
		t.Errorf("expected 2 active, got %d", stats.ActiveMetrics)
	}
	if stats.DeprecatedMetrics != 1 {
		t.Errorf("expected 1 deprecated, got %d", stats.DeprecatedMetrics)
	}
	if stats.UniqueOwners != 2 {
		t.Errorf("expected 2 unique owners, got %d", stats.UniqueOwners)
	}
}

func TestMetricsCatalogExport(t *testing.T) {
	mc := NewMetricsCatalog(nil, DefaultMetricsCatalogConfig())

	_ = mc.RegisterMetric(CatalogEntry{Name: "b.metric"})
	_ = mc.RegisterMetric(CatalogEntry{Name: "a.metric"})

	entries := mc.Export()
	if len(entries) != 2 {
		t.Fatalf("expected 2 entries, got %d", len(entries))
	}
	// Should be sorted by name
	if entries[0].Name != "a.metric" {
		t.Errorf("expected first entry a.metric, got %s", entries[0].Name)
	}
}

func TestMetricsCatalogStartStop(t *testing.T) {
	cfg := DefaultMetricsCatalogConfig()
	cfg.ScanInterval = 50 * time.Millisecond
	mc := NewMetricsCatalog(nil, cfg)

	mc.Start()
	// Starting again should be a no-op
	mc.Start()
	time.Sleep(100 * time.Millisecond)
	mc.Stop()
	// Stopping again should be a no-op
	mc.Stop()
}

func TestMetricsCatalogHTTPStats(t *testing.T) {
	mc := NewMetricsCatalog(nil, DefaultMetricsCatalogConfig())
	_ = mc.RegisterMetric(CatalogEntry{Name: "test.metric"})

	mux := http.NewServeMux()
	mc.RegisterHTTPHandlers(mux)

	req := httptest.NewRequest(http.MethodGet, "/api/v1/catalog/stats", nil)
	w := httptest.NewRecorder()
	mux.ServeHTTP(w, req)

	if w.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d", w.Code)
	}

	var stats MetricsCatalogStats
	if err := json.NewDecoder(w.Body).Decode(&stats); err != nil {
		t.Fatalf("decode failed: %v", err)
	}
	if stats.TotalMetrics != 1 {
		t.Errorf("expected 1 metric, got %d", stats.TotalMetrics)
	}
}

func TestMetricsCatalogHTTPExport(t *testing.T) {
	mc := NewMetricsCatalog(nil, DefaultMetricsCatalogConfig())
	_ = mc.RegisterMetric(CatalogEntry{Name: "export.test"})

	mux := http.NewServeMux()
	mc.RegisterHTTPHandlers(mux)

	req := httptest.NewRequest(http.MethodGet, "/api/v1/catalog/export", nil)
	w := httptest.NewRecorder()
	mux.ServeHTTP(w, req)

	if w.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d", w.Code)
	}

	var entries []*CatalogEntry
	if err := json.NewDecoder(w.Body).Decode(&entries); err != nil {
		t.Fatalf("decode failed: %v", err)
	}
	if len(entries) != 1 {
		t.Errorf("expected 1 entry, got %d", len(entries))
	}
}

func TestMetricsCatalogHTTPMetrics(t *testing.T) {
	mc := NewMetricsCatalog(nil, DefaultMetricsCatalogConfig())
	_ = mc.RegisterMetric(CatalogEntry{Name: "cpu.usage"})
	_ = mc.RegisterMetric(CatalogEntry{Name: "mem.usage"})

	mux := http.NewServeMux()
	mc.RegisterHTTPHandlers(mux)

	req := httptest.NewRequest(http.MethodGet, "/api/v1/catalog/metrics?pattern=cpu.*", nil)
	w := httptest.NewRecorder()
	mux.ServeHTTP(w, req)

	if w.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d", w.Code)
	}

	var result CatalogSearchResult
	if err := json.NewDecoder(w.Body).Decode(&result); err != nil {
		t.Fatalf("decode failed: %v", err)
	}
	if result.Total != 1 {
		t.Errorf("expected 1 match, got %d", result.Total)
	}
}

func TestMetricsCatalogHTTPDeprecate(t *testing.T) {
	mc := NewMetricsCatalog(nil, DefaultMetricsCatalogConfig())
	_ = mc.RegisterMetric(CatalogEntry{Name: "old.metric"})

	mux := http.NewServeMux()
	mc.RegisterHTTPHandlers(mux)

	body := `{"metric_name":"old.metric","reason":"obsolete","replacement":"new.metric"}`
	req := httptest.NewRequest(http.MethodPost, "/api/v1/catalog/deprecate", strings.NewReader(body))
	w := httptest.NewRecorder()
	mux.ServeHTTP(w, req)

	if w.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d: %s", w.Code, w.Body.String())
	}

	var report DeprecationReport
	if err := json.NewDecoder(w.Body).Decode(&report); err != nil {
		t.Fatalf("decode failed: %v", err)
	}
	if report.Metric != "old.metric" {
		t.Errorf("expected old.metric, got %s", report.Metric)
	}
}

func TestMetricsCatalogHTTPScan(t *testing.T) {
	mc := NewMetricsCatalog(nil, DefaultMetricsCatalogConfig())

	mux := http.NewServeMux()
	mc.RegisterHTTPHandlers(mux)

	req := httptest.NewRequest(http.MethodPost, "/api/v1/catalog/scan", nil)
	w := httptest.NewRecorder()
	mux.ServeHTTP(w, req)

	if w.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d", w.Code)
	}
}

func TestMetricsCatalogHTTPListDeprecated(t *testing.T) {
	mc := NewMetricsCatalog(nil, DefaultMetricsCatalogConfig())
	_ = mc.RegisterMetric(CatalogEntry{Name: "old"})
	_, _ = mc.DeprecateMetric(DeprecationRequest{MetricName: "old", Reason: "test"})

	mux := http.NewServeMux()
	mc.RegisterHTTPHandlers(mux)

	req := httptest.NewRequest(http.MethodGet, "/api/v1/catalog/deprecated", nil)
	w := httptest.NewRecorder()
	mux.ServeHTTP(w, req)

	if w.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d", w.Code)
	}

	var reports []*DeprecationReport
	if err := json.NewDecoder(w.Body).Decode(&reports); err != nil {
		t.Fatalf("decode failed: %v", err)
	}
	if len(reports) != 1 {
		t.Errorf("expected 1 deprecated, got %d", len(reports))
	}
}

func TestMetricsCatalogHTTPGetMetric(t *testing.T) {
	mc := NewMetricsCatalog(nil, DefaultMetricsCatalogConfig())
	_ = mc.RegisterMetric(CatalogEntry{Name: "test.metric", Description: "test"})

	mux := http.NewServeMux()
	mc.RegisterHTTPHandlers(mux)

	req := httptest.NewRequest(http.MethodGet, "/api/v1/catalog/metrics/test.metric", nil)
	w := httptest.NewRecorder()
	mux.ServeHTTP(w, req)

	if w.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d: %s", w.Code, w.Body.String())
	}
}

func TestMetricsCatalogHTTPGetMetricNotFound(t *testing.T) {
	mc := NewMetricsCatalog(nil, DefaultMetricsCatalogConfig())

	mux := http.NewServeMux()
	mc.RegisterHTTPHandlers(mux)

	req := httptest.NewRequest(http.MethodGet, "/api/v1/catalog/metrics/nonexistent", nil)
	w := httptest.NewRecorder()
	mux.ServeHTTP(w, req)

	if w.Code != http.StatusNotFound {
		t.Fatalf("expected 404, got %d", w.Code)
	}
}

func TestMetricsCatalogHTTPGetLineage(t *testing.T) {
	cfg := DefaultMetricsCatalogConfig()
	cfg.TrackLineage = true
	mc := NewMetricsCatalog(nil, cfg)
	_ = mc.RegisterMetric(CatalogEntry{Name: "test.metric"})

	mux := http.NewServeMux()
	mc.RegisterHTTPHandlers(mux)

	req := httptest.NewRequest(http.MethodGet, "/api/v1/catalog/metrics/test.metric/lineage", nil)
	w := httptest.NewRecorder()
	mux.ServeHTTP(w, req)

	if w.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d: %s", w.Code, w.Body.String())
	}
}

func TestMetricsCatalogGlobMatch(t *testing.T) {
	tests := []struct {
		pattern string
		input   string
		match   bool
	}{
		{"*", "anything", true},
		{"cpu.*", "cpu.usage", true},
		{"cpu.*", "mem.usage", false},
		{"*.usage", "cpu.usage", true},
		{"*.usage", "cpu.idle", false},
		{"*cpu*", "system.cpu.usage", true},
		{"exact", "exact", true},
		{"exact", "other", false},
	}

	for _, tt := range tests {
		got := matchGlob(tt.pattern, tt.input)
		if got != tt.match {
			t.Errorf("matchGlob(%q, %q) = %v, want %v", tt.pattern, tt.input, got, tt.match)
		}
	}
}

func TestCatalogMetricTypeString(t *testing.T) {
	tests := []struct {
		t    CatalogMetricType
		want string
	}{
		{CatalogMetricTypeUnknown, "unknown"},
		{CatalogMetricTypeGauge, "gauge"},
		{CatalogMetricTypeCounter, "counter"},
		{CatalogMetricTypeHistogram, "histogram"},
		{CatalogMetricTypeSummary, "summary"},
	}
	for _, tt := range tests {
		if got := tt.t.String(); got != tt.want {
			t.Errorf("%d.String() = %q, want %q", tt.t, got, tt.want)
		}
	}
}

func TestCatalogMetricStatusString(t *testing.T) {
	tests := []struct {
		s    CatalogMetricStatus
		want string
	}{
		{CatalogMetricStatusActive, "active"},
		{CatalogMetricStatusInactive, "inactive"},
		{CatalogMetricStatusDeprecated, "deprecated"},
		{CatalogMetricStatusArchived, "archived"},
	}
	for _, tt := range tests {
		if got := tt.s.String(); got != tt.want {
			t.Errorf("%d.String() = %q, want %q", tt.s, got, tt.want)
		}
	}
}

func TestMetricsCatalogGetEntryNil(t *testing.T) {
	mc := NewMetricsCatalog(nil, DefaultMetricsCatalogConfig())
	entry := mc.GetEntry("nonexistent")
	if entry != nil {
		t.Error("expected nil for nonexistent entry")
	}
}

func TestMetricsCatalogDeprecateEmptyName(t *testing.T) {
	mc := NewMetricsCatalog(nil, DefaultMetricsCatalogConfig())
	_, err := mc.DeprecateMetric(DeprecationRequest{})
	if err == nil {
		t.Fatal("expected error for empty name")
	}
}
