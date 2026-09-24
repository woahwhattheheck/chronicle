package chronicle

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"
)

func TestTraceCorrelator_NewTraceCorrelator(t *testing.T) {
	config := DefaultTracingConfig()
	tc := NewTraceCorrelator(nil, config)

	if tc == nil {
		t.Fatal("Expected non-nil TraceCorrelator")
	}

	if tc.config.TraceIDTagKey != "trace_id" {
		t.Errorf("Expected trace_id tag key, got %s", tc.config.TraceIDTagKey)
	}
}

func TestTraceCorrelator_RecordTraceReference(t *testing.T) {
	config := DefaultTracingConfig()
	tc := NewTraceCorrelator(nil, config)

	ref := &TraceReference{
		TraceID:     "abc123",
		SpanID:      "span456",
		ServiceName: "test-service",
		Metric:      "http_requests_total",
		Value:       100.0,
		Timestamp:   time.Now().UnixNano(),
	}

	tc.RecordTraceReference(ref)

	// Verify it was recorded
	tc.mu.RLock()
	stored := tc.traceIndex["abc123"]
	tc.mu.RUnlock()

	if stored == nil {
		t.Fatal("Expected trace reference to be stored")
	}

	if stored.ServiceName != "test-service" {
		t.Errorf("Expected service name test-service, got %s", stored.ServiceName)
	}
}

func TestTraceCorrelator_GetTracesForMetric(t *testing.T) {
	config := DefaultTracingConfig()
	tc := NewTraceCorrelator(nil, config)

	now := time.Now().UnixNano()

	// Record multiple traces for same metric
	for i := 0; i < 5; i++ {
		ref := &TraceReference{
			TraceID:   "trace" + string(rune('a'+i)),
			Metric:    "http_latency",
			Timestamp: now - int64(i)*int64(time.Minute),
		}
		tc.RecordTraceReference(ref)
	}

	// Query traces
	start := now - int64(10*time.Minute)
	end := now + int64(time.Minute)
	refs := tc.GetTracesForMetric("http_latency", start, end)

	if len(refs) != 5 {
		t.Errorf("Expected 5 traces, got %d", len(refs))
	}

	// Should be sorted by timestamp descending
	for i := 1; i < len(refs); i++ {
		if refs[i].Timestamp > refs[i-1].Timestamp {
			t.Error("Traces should be sorted by timestamp descending")
		}
	}
}

func TestTraceCorrelator_RecordFromExemplar(t *testing.T) {
	config := DefaultTracingConfig()
	tc := NewTraceCorrelator(nil, config)

	exemplar := &Exemplar{
		Labels: map[string]string{
			"trace_id": "exemplar-trace",
			"span_id":  "exemplar-span",
			"service":  "exemplar-service",
		},
		Value:     42.0,
		Timestamp: time.Now().UnixNano(),
	}

	tc.RecordFromExemplar("metric", nil, 42.0, time.Now().UnixNano(), exemplar)

	tc.mu.RLock()
	ref := tc.traceIndex["exemplar-trace"]
	tc.mu.RUnlock()

	if ref == nil {
		t.Fatal("Expected trace reference from exemplar")
	}

	if ref.SpanID != "exemplar-span" {
		t.Errorf("Expected span ID exemplar-span, got %s", ref.SpanID)
	}
}

func TestTraceCorrelator_GetTraceURL(t *testing.T) {
	config := DefaultTracingConfig()
	config.JaegerEndpoint = "http://jaeger:16686"
	tc := NewTraceCorrelator(nil, config)

	url := tc.getTraceURL("abc123")
	expected := "http://jaeger:16686/trace/abc123"

	if url != expected {
		t.Errorf("Expected %s, got %s", expected, url)
	}

	// Test Zipkin
	config.JaegerEndpoint = ""
	config.ZipkinEndpoint = "http://zipkin:9411"
	tc = NewTraceCorrelator(nil, config)

	url = tc.getTraceURL("abc123")
	expected = "http://zipkin:9411/traces/abc123"

	if url != expected {
		t.Errorf("Expected %s, got %s", expected, url)
	}
}

func TestTraceCorrelator_GetTraceStats(t *testing.T) {
	config := DefaultTracingConfig()
	tc := NewTraceCorrelator(nil, config)

	// Record some traces
	tc.RecordTraceReference(&TraceReference{
		TraceID:     "t1",
		ServiceName: "service-a",
		Metric:      "metric1",
		Timestamp:   time.Now().UnixNano(),
	})
	tc.RecordTraceReference(&TraceReference{
		TraceID:     "t2",
		ServiceName: "service-a",
		Metric:      "metric1",
		Timestamp:   time.Now().UnixNano(),
	})
	tc.RecordTraceReference(&TraceReference{
		TraceID:     "t3",
		ServiceName: "service-b",
		Metric:      "metric2",
		Timestamp:   time.Now().UnixNano(),
	})

	stats := tc.GetTraceStats()

	if stats.TotalTraces != 3 {
		t.Errorf("Expected 3 total traces, got %d", stats.TotalTraces)
	}

	if stats.TracedMetrics != 2 {
		t.Errorf("Expected 2 traced metrics, got %d", stats.TracedMetrics)
	}

	if stats.TracesByService["service-a"] != 2 {
		t.Errorf("Expected 2 traces for service-a, got %d", stats.TracesByService["service-a"])
	}
}

func TestTraceCorrelator_FetchTrace_Jaeger(t *testing.T) {
	// Mock Jaeger server
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		response := map[string]any{
			"data": []map[string]any{
				{
					"traceID": "test-trace",
					"spans": []map[string]any{
						{
							"traceID":       "test-trace",
							"spanID":        "span1",
							"operationName": "test-op",
							"startTime":     time.Now().UnixMicro(),
							"duration":      1000,
							"processID":     "p1",
							"tags":          []map[string]any{},
						},
					},
					"processes": map[string]any{
						"p1": map[string]any{
							"serviceName": "test-service",
						},
					},
				},
			},
		}
		json.NewEncoder(w).Encode(response)
	}))
	defer server.Close()

	config := DefaultTracingConfig()
	config.JaegerEndpoint = server.URL
	tc := NewTraceCorrelator(nil, config)

	trace, err := tc.FetchTrace("test-trace")
	if err != nil {
		t.Fatalf("FetchTrace failed: %v", err)
	}

	if trace == nil {
		t.Fatal("Expected trace")
	}

	if trace.TraceID != "test-trace" {
		t.Errorf("Expected trace ID test-trace, got %s", trace.TraceID)
	}

	if len(trace.Spans) != 1 {
		t.Errorf("Expected 1 span, got %d", len(trace.Spans))
	}
}

func TestTraceCorrelator_FetchTrace_Zipkin(t *testing.T) {
	// Mock Zipkin server
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		response := []map[string]any{
			{
				"traceId":   "test-trace",
				"id":        "span1",
				"name":      "test-op",
				"timestamp": time.Now().UnixMicro(),
				"duration":  1000,
				"localEndpoint": map[string]any{
					"serviceName": "test-service",
				},
			},
		}
		json.NewEncoder(w).Encode(response)
	}))
	defer server.Close()

	config := DefaultTracingConfig()
	config.ZipkinEndpoint = server.URL
	tc := NewTraceCorrelator(nil, config)

	trace, err := tc.FetchTrace("test-trace")
	if err != nil {
		t.Fatalf("FetchTrace failed: %v", err)
	}

	if trace == nil {
		t.Fatal("Expected trace")
	}

	if trace.SpanCount != 1 {
		t.Errorf("Expected 1 span, got %d", trace.SpanCount)
	}
}

func TestTraceCorrelator_GetTracesForAnomaly(t *testing.T) {
	config := DefaultTracingConfig()
	tc := NewTraceCorrelator(nil, config)

	now := time.Now().UnixNano()

	// Record a trace near the anomaly time
	tc.RecordTraceReference(&TraceReference{
		TraceID:     "anomaly-trace",
		ServiceName: "api-service",
		Metric:      "http_errors",
		Timestamp:   now,
	})

	anomaly := &ClassifiedAnomaly{
		AnomalyResult: AnomalyResult{
			IsAnomaly: true,
			Score:     0.8,
		},
		Metric:    "http_errors",
		Timestamp: now + int64(time.Second),
		Type:      AnomalyTypeSpike,
	}

	correlations, err := tc.GetTracesForAnomaly(anomaly)
	if err != nil {
		t.Fatalf("GetTracesForAnomaly failed: %v", err)
	}

	if len(correlations) == 0 {
		t.Error("Expected at least one correlation")
		return
	}

	if correlations[0].TraceID != "anomaly-trace" {
		t.Errorf("Expected trace ID anomaly-trace, got %s", correlations[0].TraceID)
	}

	if correlations[0].CorrelationScore <= 0 {
		t.Error("Expected positive correlation score")
	}

	if correlations[0].SuggestedAction == "" {
		t.Error("Expected suggested action")
	}
}

func TestTraceCache(t *testing.T) {
	cache := newTraceCache(3)

	// Add traces
	cache.put("t1", &Trace{TraceID: "t1"})
	cache.put("t2", &Trace{TraceID: "t2"})
	cache.put("t3", &Trace{TraceID: "t3"})

	// Verify all present
	if cache.get("t1") == nil || cache.get("t2") == nil || cache.get("t3") == nil {
		t.Error("Expected all traces in cache")
	}

	// Add one more - should evict t1
	cache.put("t4", &Trace{TraceID: "t4"})

	if cache.get("t1") != nil {
		t.Error("Expected t1 to be evicted")
	}

	if cache.get("t4") == nil {
		t.Error("Expected t4 in cache")
	}
}

func TestParseJaegerTrace(t *testing.T) {
	data := []byte(`{
		"data": [{
			"traceID": "abc123",
			"spans": [{
				"traceID": "abc123",
				"spanID": "span1",
				"operationName": "GET /api",
				"startTime": 1609459200000000,
				"duration": 1000,
				"processID": "p1",
				"tags": [{"key": "http.status_code", "value": 200}]
			}],
			"processes": {
				"p1": {"serviceName": "api-gateway"}
			}
		}]
	}`)

	trace, err := parseJaegerTrace(data)
	if err != nil {
		t.Fatalf("parseJaegerTrace failed: %v", err)
	}

	if trace.TraceID != "abc123" {
		t.Errorf("Expected trace ID abc123, got %s", trace.TraceID)
	}

	if len(trace.Spans) != 1 {
		t.Errorf("Expected 1 span, got %d", len(trace.Spans))
	}

	if trace.Spans[0].OperationName != "GET /api" {
		t.Errorf("Expected operation name 'GET /api', got %s", trace.Spans[0].OperationName)
	}
}

func TestParseZipkinTrace(t *testing.T) {
	data := []byte(`[{
		"traceId": "xyz789",
		"id": "span1",
		"name": "POST /users",
		"timestamp": 1609459200000000,
		"duration": 2000,
		"localEndpoint": {"serviceName": "user-service"},
		"tags": {"http.method": "POST"}
	}]`)

	trace, err := parseZipkinTrace(data)
	if err != nil {
		t.Fatalf("parseZipkinTrace failed: %v", err)
	}

	if trace.TraceID != "xyz789" {
		t.Errorf("Expected trace ID xyz789, got %s", trace.TraceID)
	}

	if trace.Spans[0].ServiceName != "user-service" {
		t.Errorf("Expected service name user-service, got %s", trace.Spans[0].ServiceName)
	}
}

func TestDefaultTracingConfig(t *testing.T) {
	config := DefaultTracingConfig()

	if config.TraceIDTagKey != "trace_id" {
		t.Errorf("Expected trace_id tag key, got %s", config.TraceIDTagKey)
	}

	if config.MaxTracesPerQuery != 100 {
		t.Errorf("Expected 100 max traces, got %d", config.MaxTracesPerQuery)
	}

	if config.CacheSize != 10000 {
		t.Errorf("Expected 10000 cache size, got %d", config.CacheSize)
	}
}

func TestTraceCorrelator_Cleanup(t *testing.T) {
	config := DefaultTracingConfig()
	config.TraceRetention = time.Millisecond // Very short for testing
	tc := NewTraceCorrelator(nil, config)

	// Record an old trace
	oldTime := time.Now().Add(-time.Hour).UnixNano()
	tc.RecordTraceReference(&TraceReference{
		TraceID:   "old-trace",
		Metric:    "test",
		Timestamp: oldTime,
	})

	// Run cleanup
	tc.cleanup()

	// Verify old trace was removed
	tc.mu.RLock()
	_, exists := tc.traceIndex["old-trace"]
	tc.mu.RUnlock()

	if exists {
		t.Error("Expected old trace to be cleaned up")
	}
}

func TestSuggestAction(t *testing.T) {
	tc := NewTraceCorrelator(nil, DefaultTracingConfig())

	tests := []struct {
		anomalyType AnomalyType
		statusCode  string
		expected    string
	}{
		{AnomalyTypeSpike, "500", "Investigate server errors in trace"},
		{AnomalyTypeSpike, "200", "Check trace for high latency operations"},
		{AnomalyTypeDip, "", "Verify service health from trace spans"},
		{AnomalyTypeOutlier, "", "Review trace for unusual operations"},
		{AnomalyTypeDrift, "", "Inspect trace for root cause"},
	}

	for _, tt := range tests {
		anomaly := &ClassifiedAnomaly{Type: tt.anomalyType}
		ref := &TraceReference{StatusCode: tt.statusCode}

		action := tc.suggestAction(anomaly, ref)
		if action != tt.expected {
			t.Errorf("suggestAction(%v, %s) = %s, want %s", tt.anomalyType, tt.statusCode, action, tt.expected)
		}
	}
}
