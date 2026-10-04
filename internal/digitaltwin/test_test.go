package digitaltwin

import (
	"context"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	chronicle "github.com/chronicle-db/chronicle"
)

func TestDigitalTwinEngine(t *testing.T) {
	db := setupTestDB(t)

	config := DefaultDigitalTwinConfig()
	config.Enabled = false // Disable background loops for testing

	engine := NewDigitalTwinEngine(db, config)
	defer engine.Close()

	// Verify initial state
	stats := engine.Stats()
	if stats.Connections != 0 {
		t.Errorf("expected 0 connections, got %d", stats.Connections)
	}
}

func TestAddConnection(t *testing.T) {
	db := setupTestDB(t)

	config := DefaultDigitalTwinConfig()
	config.Enabled = false

	engine := NewDigitalTwinEngine(db, config)
	defer engine.Close()

	// Create mock server
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
	}))
	defer server.Close()

	conn := &TwinConnection{
		Name:     "test-connection",
		Platform: PlatformCustom,
		Endpoint: server.URL,
		Enabled:  true,
	}

	err := engine.AddConnection(conn)
	if err != nil {
		t.Fatalf("failed to add connection: %v", err)
	}

	if conn.ID == "" {
		t.Error("connection should have an ID")
	}

	if conn.Status != TwinStatusConnected {
		t.Errorf("expected status 'connected', got '%s'", conn.Status)
	}

	// List connections
	connections := engine.ListConnections()
	if len(connections) != 1 {
		t.Errorf("expected 1 connection, got %d", len(connections))
	}

	// Get connection
	retrieved, err := engine.GetConnection(conn.ID)
	if err != nil {
		t.Fatalf("failed to get connection: %v", err)
	}
	if retrieved.Name != "test-connection" {
		t.Errorf("expected name 'test-connection', got '%s'", retrieved.Name)
	}
}

func TestRemoveConnection(t *testing.T) {
	db := setupTestDB(t)

	config := DefaultDigitalTwinConfig()
	config.Enabled = false

	engine := NewDigitalTwinEngine(db, config)
	defer engine.Close()

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
	}))
	defer server.Close()

	conn := &TwinConnection{
		Platform: PlatformCustom,
		Endpoint: server.URL,
		Enabled:  true,
	}

	engine.AddConnection(conn)

	// Remove connection
	err := engine.RemoveConnection(conn.ID)
	if err != nil {
		t.Fatalf("failed to remove connection: %v", err)
	}

	// Verify removed
	_, err = engine.GetConnection(conn.ID)
	if err == nil {
		t.Error("expected error for removed connection")
	}
}

func TestAddMapping(t *testing.T) {
	db := setupTestDB(t)

	config := DefaultDigitalTwinConfig()
	config.Enabled = false

	engine := NewDigitalTwinEngine(db, config)
	defer engine.Close()

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
	}))
	defer server.Close()

	conn := &TwinConnection{
		Platform: PlatformCustom,
		Endpoint: server.URL,
		Enabled:  true,
	}
	engine.AddConnection(conn)

	mapping := &TwinMapping{
		ConnectionID:    conn.ID,
		ChronicleMetric: "temperature",
		ChronicleTags:   map[string]string{"sensor": "sensor1"},
		TwinID:          "device-001",
		TwinProperty:    "temperature",
		TwinComponent:   "environment",
		Direction:       SyncToTwin,
		Enabled:         true,
	}

	err := engine.AddMapping(mapping)
	if err != nil {
		t.Fatalf("failed to add mapping: %v", err)
	}

	if mapping.ID == "" {
		t.Error("mapping should have an ID")
	}

	// List mappings
	mappings := engine.ListMappings(conn.ID)
	if len(mappings) != 1 {
		t.Errorf("expected 1 mapping, got %d", len(mappings))
	}
}

func TestMappingWithInvalidConnection(t *testing.T) {
	db := setupTestDB(t)

	config := DefaultDigitalTwinConfig()
	config.Enabled = false

	engine := NewDigitalTwinEngine(db, config)
	defer engine.Close()

	mapping := &TwinMapping{
		ConnectionID:    "non-existent",
		ChronicleMetric: "temperature",
		TwinID:          "device-001",
		TwinProperty:    "temperature",
	}

	err := engine.AddMapping(mapping)
	if err == nil {
		t.Error("expected error for mapping with invalid connection")
	}
}

func TestRemoveMapping(t *testing.T) {
	db := setupTestDB(t)

	config := DefaultDigitalTwinConfig()
	config.Enabled = false

	engine := NewDigitalTwinEngine(db, config)
	defer engine.Close()

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
	}))
	defer server.Close()

	conn := &TwinConnection{
		Platform: PlatformCustom,
		Endpoint: server.URL,
		Enabled:  true,
	}
	engine.AddConnection(conn)

	mapping := &TwinMapping{
		ConnectionID:    conn.ID,
		ChronicleMetric: "temperature",
		TwinID:          "device-001",
		TwinProperty:    "temperature",
		Direction:       SyncToTwin,
		Enabled:         true,
	}
	engine.AddMapping(mapping)

	// Remove mapping
	err := engine.RemoveMapping(mapping.ID)
	if err != nil {
		t.Fatalf("failed to remove mapping: %v", err)
	}

	// Verify removed
	mappings := engine.ListMappings(conn.ID)
	if len(mappings) != 0 {
		t.Errorf("expected 0 mappings, got %d", len(mappings))
	}
}

func TestPushMetric(t *testing.T) {
	db := setupTestDB(t)

	config := DefaultDigitalTwinConfig()
	config.Enabled = false

	engine := NewDigitalTwinEngine(db, config)
	defer engine.Close()

	receivedUpdates := make(chan bool, 1)
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		receivedUpdates <- true
		w.WriteHeader(http.StatusOK)
	}))
	defer server.Close()

	conn := &TwinConnection{
		Platform: PlatformCustom,
		Endpoint: server.URL,
		Enabled:  true,
	}
	engine.AddConnection(conn)

	mapping := &TwinMapping{
		ConnectionID:    conn.ID,
		ChronicleMetric: "temperature",
		ChronicleTags:   map[string]string{"sensor": "sensor1"},
		TwinID:          "device-001",
		TwinProperty:    "temperature",
		Direction:       SyncToTwin,
		Enabled:         true,
	}
	engine.AddMapping(mapping)

	// Push metric
	err := engine.PushMetric("temperature", map[string]string{"sensor": "sensor1"}, 25.5, time.Now())
	if err != nil {
		t.Fatalf("failed to push metric: %v", err)
	}

	// Check stats
	stats := engine.Stats()
	if stats.PendingUpdates != 1 {
		t.Errorf("expected 1 pending update, got %d", stats.PendingUpdates)
	}
}

func TestPushMetricWithTransform(t *testing.T) {
	db := setupTestDB(t)

	config := DefaultDigitalTwinConfig()
	config.Enabled = false

	engine := NewDigitalTwinEngine(db, config)
	defer engine.Close()

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
	}))
	defer server.Close()

	conn := &TwinConnection{
		Platform: PlatformCustom,
		Endpoint: server.URL,
		Enabled:  true,
	}
	engine.AddConnection(conn)

	// Add mapping with transform
	mapping := &TwinMapping{
		ConnectionID:    conn.ID,
		ChronicleMetric: "temperature",
		TwinID:          "device-001",
		TwinProperty:    "temperature_f",
		Direction:       SyncToTwin,
		Enabled:         true,
		Transform: &PropertyTransform{
			Type: "scale_offset",
			Parameters: map[string]any{
				"factor": 1.8,
				"offset": 32.0,
			},
		},
	}
	engine.AddMapping(mapping)

	// Push metric (Celsius)
	err := engine.PushMetric("temperature", nil, 0.0, time.Now()) // 0C = 32F
	if err != nil {
		t.Fatalf("failed to push metric: %v", err)
	}
}

func TestTransformFunctions(t *testing.T) {
	tests := []struct {
		name      string
		transform *PropertyTransform
		input     float64
		expected  float64
	}{
		{
			name:      "scale",
			transform: &PropertyTransform{Type: "scale", Parameters: map[string]any{"factor": 2.0}},
			input:     10.0,
			expected:  20.0,
		},
		{
			name:      "offset",
			transform: &PropertyTransform{Type: "offset", Parameters: map[string]any{"offset": 5.0}},
			input:     10.0,
			expected:  15.0,
		},
		{
			name:      "scale_offset",
			transform: &PropertyTransform{Type: "scale_offset", Parameters: map[string]any{"factor": 2.0, "offset": 5.0}},
			input:     10.0,
			expected:  25.0, // (10 * 2) + 5
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := applyTransform(tt.input, tt.transform)
			if result != tt.expected {
				t.Errorf("expected %f, got %f", tt.expected, result)
			}
		})
	}
}

func TestReverseTransform(t *testing.T) {
	tests := []struct {
		name      string
		transform *PropertyTransform
		input     float64
		expected  float64
	}{
		{
			name:      "scale",
			transform: &PropertyTransform{Type: "scale", Parameters: map[string]any{"factor": 2.0}},
			input:     20.0,
			expected:  10.0,
		},
		{
			name:      "offset",
			transform: &PropertyTransform{Type: "offset", Parameters: map[string]any{"offset": 5.0}},
			input:     15.0,
			expected:  10.0,
		},
		{
			name:      "scale_offset",
			transform: &PropertyTransform{Type: "scale_offset", Parameters: map[string]any{"factor": 2.0, "offset": 5.0}},
			input:     25.0,
			expected:  10.0, // (25 - 5) / 2
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := applyReverseTransform(tt.input, tt.transform)
			if result != tt.expected {
				t.Errorf("expected %f, got %f", tt.expected, result)
			}
		})
	}
}

func TestTagsMatch(t *testing.T) {
	tests := []struct {
		name     string
		actual   map[string]string
		required map[string]string
		expected bool
	}{
		{
			name:     "empty required",
			actual:   map[string]string{"host": "server1"},
			required: nil,
			expected: true,
		},
		{
			name:     "exact match",
			actual:   map[string]string{"host": "server1", "region": "us-east"},
			required: map[string]string{"host": "server1"},
			expected: true,
		},
		{
			name:     "no match",
			actual:   map[string]string{"host": "server1"},
			required: map[string]string{"host": "server2"},
			expected: false,
		},
		{
			name:     "missing key",
			actual:   map[string]string{"host": "server1"},
			required: map[string]string{"region": "us-east"},
			expected: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := tagsMatch(tt.actual, tt.required)
			if result != tt.expected {
				t.Errorf("expected %v, got %v", tt.expected, result)
			}
		})
	}
}

func TestTwinCallbacks(t *testing.T) {
	db := setupTestDB(t)

	config := DefaultDigitalTwinConfig()
	config.Enabled = false

	engine := NewDigitalTwinEngine(db, config)
	defer engine.Close()

	var syncCalled bool
	var errorCalled bool

	engine.OnSyncComplete(func(conn *TwinConnection, sent, received int) {
		syncCalled = true
	})

	engine.OnSyncError(func(conn *TwinConnection, err error) {
		errorCalled = true
	})

	_ = syncCalled
	_ = errorCalled

	// Callbacks should be set
	if engine.onSyncComplete == nil {
		t.Error("onSyncComplete callback not set")
	}
	if engine.onSyncError == nil {
		t.Error("onSyncError callback not set")
	}
}

func TestAzureAdapter(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.Method {
		case "PATCH":
			w.WriteHeader(http.StatusNoContent)
		case "GET":
			w.Header().Set("Content-Type", "application/json")
			w.Write([]byte(`{"$dtId": "device-001", "temperature": 25.5}`))
		default:
			w.WriteHeader(http.StatusMethodNotAllowed)
		}
	}))
	defer server.Close()

	conn := &TwinConnection{
		Platform: PlatformAzureDigitalTwins,
		Endpoint: server.URL,
	}

	adapter := NewAzureDigitalTwinsAdapter(conn)

	ctx := context.Background()

	// Test connect
	err := adapter.Connect(ctx, conn)
	if err != nil {
		t.Fatalf("connect failed: %v", err)
	}

	// Test push update
	update := &TwinUpdate{
		TwinID:    "device-001",
		Property:  "temperature",
		Value:     25.5,
		Timestamp: time.Now(),
	}

	err = adapter.PushUpdate(ctx, update)
	if err != nil {
		t.Fatalf("push update failed: %v", err)
	}

	// Test get state
	state, err := adapter.GetTwinState(ctx, "device-001")
	if err != nil {
		t.Fatalf("get state failed: %v", err)
	}

	if state["$dtId"] != "device-001" {
		t.Errorf("expected $dtId 'device-001', got '%v'", state["$dtId"])
	}
}

func TestEclipseDittoAdapter(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.Method {
		case "PUT":
			w.WriteHeader(http.StatusNoContent)
		case "GET":
			if r.URL.Path == "/status/health" {
				w.WriteHeader(http.StatusOK)
			} else if r.URL.Path == "/api/2/things" {
				w.Header().Set("Content-Type", "application/json")
				w.Write([]byte(`[{"thingId": "ns:thing-001"}, {"thingId": "ns:thing-002"}]`))
			} else {
				w.Header().Set("Content-Type", "application/json")
				w.Write([]byte(`{"thingId": "ns:thing-001", "attributes": {"temperature": 25.5}}`))
			}
		default:
			w.WriteHeader(http.StatusMethodNotAllowed)
		}
	}))
	defer server.Close()

	conn := &TwinConnection{
		Platform: PlatformEclipseDitto,
		Endpoint: server.URL,
	}

	adapter := NewEclipseDittoAdapter(conn)

	ctx := context.Background()

	// Test connect
	err := adapter.Connect(ctx, conn)
	if err != nil {
		t.Fatalf("connect failed: %v", err)
	}

	// Test health check
	err = adapter.HealthCheck(ctx)
	if err != nil {
		t.Fatalf("health check failed: %v", err)
	}

	// Test list twins
	twins, err := adapter.ListTwins(ctx)
	if err != nil {
		t.Fatalf("list twins failed: %v", err)
	}

	if len(twins) != 2 {
		t.Errorf("expected 2 twins, got %d", len(twins))
	}
}

func TestCustomAdapter(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/health":
			w.WriteHeader(http.StatusOK)
		case "/updates":
			if r.Method == "POST" {
				w.WriteHeader(http.StatusOK)
			} else {
				w.Header().Set("Content-Type", "application/json")
				w.Write([]byte(`{"updates": []}`))
			}
		case "/twins":
			w.Header().Set("Content-Type", "application/json")
			w.Write([]byte(`{"twins": ["twin-001", "twin-002"]}`))
		default:
			w.WriteHeader(http.StatusOK)
		}
	}))
	defer server.Close()

	conn := &TwinConnection{
		Platform: PlatformCustom,
		Endpoint: server.URL,
		Credentials: &TwinCredentials{
			Type:   "api_key",
			APIKey: "test-key",
		},
	}

	adapter := NewCustomTwinAdapter(conn)

	ctx := context.Background()

	// Test connect
	err := adapter.Connect(ctx, conn)
	if err != nil {
		t.Fatalf("connect failed: %v", err)
	}

	// Test health check
	err = adapter.HealthCheck(ctx)
	if err != nil {
		t.Fatalf("health check failed: %v", err)
	}

	// Test list twins
	twins, err := adapter.ListTwins(ctx)
	if err != nil {
		t.Fatalf("list twins failed: %v", err)
	}

	if len(twins) != 2 {
		t.Errorf("expected 2 twins, got %d", len(twins))
	}

	// Test push batch
	updates := []*TwinUpdate{
		{TwinID: "twin-001", Property: "value", Value: 1.0, Timestamp: time.Now()},
		{TwinID: "twin-002", Property: "value", Value: 2.0, Timestamp: time.Now()},
	}

	err = adapter.PushBatch(ctx, updates)
	if err != nil {
		t.Fatalf("push batch failed: %v", err)
	}

	// Test pull updates
	retrieved, err := adapter.PullUpdates(ctx, time.Now().Add(-time.Hour))
	if err != nil {
		t.Fatalf("pull updates failed: %v", err)
	}

	// Should be empty from mock
	if len(retrieved) != 0 {
		t.Errorf("expected 0 updates from mock, got %d", len(retrieved))
	}
}

func TestPlatformCreation(t *testing.T) {
	db := setupTestDB(t)

	config := DefaultDigitalTwinConfig()
	config.Enabled = false

	engine := NewDigitalTwinEngine(db, config)
	defer engine.Close()

	platforms := []TwinPlatform{
		PlatformAzureDigitalTwins,
		PlatformAWSIoTTwinMaker,
		PlatformEclipseDitto,
		PlatformCustom,
	}

	for _, platform := range platforms {
		t.Run(string(platform), func(t *testing.T) {
			conn := &TwinConnection{
				Platform: platform,
				Endpoint: "http://localhost:8080",
			}
			adapter, err := engine.createAdapter(conn)
			if err != nil {
				t.Fatalf("failed to create adapter for %s: %v", platform, err)
			}
			if adapter == nil {
				t.Errorf("adapter should not be nil for %s", platform)
			}
		})
	}

	// Test unsupported platform
	conn := &TwinConnection{
		Platform: "unsupported",
		Endpoint: "http://localhost:8080",
	}
	_, err := engine.createAdapter(conn)
	if err == nil {
		t.Error("expected error for unsupported platform")
	}
}

func BenchmarkPushMetric(b *testing.B) {
	dir := b.TempDir()
	path := dir + "/test.db"
	db, err := chronicle.Open(path, chronicle.DefaultConfig(path))
	if err != nil {
		b.Fatalf("failed to open test db: %v", err)
	}
	defer db.Close()

	config := DefaultDigitalTwinConfig()
	config.Enabled = false
	config.BufferSize = 100000

	engine := NewDigitalTwinEngine(db, config)
	defer engine.Close()

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
	}))
	defer server.Close()

	conn := &TwinConnection{
		Platform: PlatformCustom,
		Endpoint: server.URL,
		Enabled:  true,
	}
	engine.AddConnection(conn)

	mapping := &TwinMapping{
		ConnectionID:    conn.ID,
		ChronicleMetric: "temperature",
		TwinID:          "device-001",
		TwinProperty:    "temperature",
		Direction:       SyncToTwin,
		Enabled:         true,
	}
	engine.AddMapping(mapping)

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		engine.PushMetric("temperature", nil, float64(i), time.Now())
	}
}

func setupTestDB(t *testing.T) *chronicle.DB {
	t.Helper()
	dir := t.TempDir()
	path := dir + "/test.db"
	db, err := chronicle.Open(path, chronicle.DefaultConfig(path))
	if err != nil {
		t.Fatalf("failed to open test db: %v", err)
	}
	t.Cleanup(func() {
		if err := db.Close(); err != nil {
			t.Errorf("failed to close test db: %v", err)
		}
	})
	return db
}

func tagsMatch(seriesTags, queryTags map[string]string) bool {
	for k, v := range queryTags {
		if seriesTags[k] != v {
			return false
		}
	}
	return true
}
