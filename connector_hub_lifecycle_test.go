package chronicle

import (
	"encoding/json"
	"fmt"
	"sync"
	"testing"
	"time"
)

// mockConnectorDriver implements ConnectorDriver for testing.
type mockConnectorDriver struct {
	mu       sync.Mutex
	name     string
	connType ConnectorType
	initErr  error
	writeErr error
	healthy  bool
	written  []Point
	closed   bool
}

func (m *mockConnectorDriver) Name() string        { return m.name }
func (m *mockConnectorDriver) Type() ConnectorType { return m.connType }
func (m *mockConnectorDriver) Initialize(config map[string]string) error {
	return m.initErr
}
func (m *mockConnectorDriver) Write(points []Point) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.writeErr != nil {
		return m.writeErr
	}
	m.written = append(m.written, points...)
	return nil
}
func (m *mockConnectorDriver) Read(limit int) ([]Point, error) {
	return nil, nil
}
func (m *mockConnectorDriver) HealthCheck() error {
	if !m.healthy {
		return fmt.Errorf("unhealthy")
	}
	return nil
}
func (m *mockConnectorDriver) Close() error {
	m.closed = true
	return nil
}

func newTestConnectorHub(t *testing.T) *ConnectorHub {
	t.Helper()
	db := setupTestDB(t)
	cfg := DefaultConnectorHubConfig()
	cfg.MaxConnectors = 10
	return NewConnectorHub(db, cfg)
}

func TestConnectorHubRegisterDriver(t *testing.T) {
	hub := newTestConnectorHub(t)

	hub.RegisterDriver("mock", func() ConnectorDriver {
		return &mockConnectorDriver{name: "mock", connType: ConnectorTypeSink, healthy: true}
	})

	drivers := hub.ListDrivers()
	found := false
	for _, d := range drivers {
		if d == "mock" {
			found = true
		}
	}
	if !found {
		t.Error("Expected 'mock' driver to be registered")
	}
}

func TestConnectorHubCreateConnector(t *testing.T) {
	hub := newTestConnectorHub(t)

	err := hub.CreateConnector(ConnectorConfig{
		Name:   "test-sink",
		Type:   ConnectorTypeSink,
		Driver: "mock",
	})
	if err != nil {
		t.Fatalf("CreateConnector failed: %v", err)
	}

	connectors := hub.ListConnectors()
	if len(connectors) != 1 {
		t.Errorf("Expected 1 connector, got %d", len(connectors))
	}
}

func TestConnectorHubCreateDuplicate(t *testing.T) {
	hub := newTestConnectorHub(t)

	_ = hub.CreateConnector(ConnectorConfig{Name: "dup", Type: ConnectorTypeSink, Driver: "mock"})
	err := hub.CreateConnector(ConnectorConfig{Name: "dup", Type: ConnectorTypeSink, Driver: "mock"})
	if err == nil {
		t.Error("Expected error creating duplicate connector")
	}
}

func TestConnectorHubCreateExceedsMax(t *testing.T) {
	hub := newTestConnectorHub(t)
	hub.config.MaxConnectors = 2

	_ = hub.CreateConnector(ConnectorConfig{Name: "c1", Type: ConnectorTypeSink, Driver: "x"})
	_ = hub.CreateConnector(ConnectorConfig{Name: "c2", Type: ConnectorTypeSink, Driver: "x"})
	err := hub.CreateConnector(ConnectorConfig{Name: "c3", Type: ConnectorTypeSink, Driver: "x"})
	if err == nil {
		t.Error("Expected error when exceeding max connectors")
	}
}

func TestConnectorHubStartConnector(t *testing.T) {
	hub := newTestConnectorHub(t)
	hub.RegisterDriver("mock", func() ConnectorDriver {
		return &mockConnectorDriver{name: "mock", connType: ConnectorTypeSink, healthy: true}
	})

	_ = hub.CreateConnector(ConnectorConfig{
		Name: "start-test", Type: ConnectorTypeSink, Driver: "mock",
	})

	err := hub.StartConnector("start-test")
	if err != nil {
		t.Fatalf("StartConnector failed: %v", err)
	}
	t.Cleanup(func() { hub.StopConnector("start-test") })

	// Verify it's running.
	conn, _ := hub.GetConnector("start-test")
	if conn == nil {
		t.Fatal("GetConnector returned nil")
	}
	if conn.Status != ConnectorStatusRunning {
		t.Errorf("Expected running status, got %s", conn.Status)
	}
}

func TestConnectorHubStartNonexistent(t *testing.T) {
	hub := newTestConnectorHub(t)

	err := hub.StartConnector("nonexistent")
	if err == nil {
		t.Error("Expected error starting nonexistent connector")
	}
}

func TestConnectorHubStartMissingDriver(t *testing.T) {
	hub := newTestConnectorHub(t)

	_ = hub.CreateConnector(ConnectorConfig{
		Name: "no-driver", Type: ConnectorTypeSink, Driver: "missing",
	})

	err := hub.StartConnector("no-driver")
	if err == nil {
		t.Error("Expected error when driver is not registered")
	}
}

func TestConnectorHubStopConnector(t *testing.T) {
	hub := newTestConnectorHub(t)
	hub.RegisterDriver("mock", func() ConnectorDriver {
		return &mockConnectorDriver{name: "mock", connType: ConnectorTypeSink, healthy: true}
	})

	_ = hub.CreateConnector(ConnectorConfig{
		Name: "stop-test", Type: ConnectorTypeSink, Driver: "mock",
	})
	_ = hub.StartConnector("stop-test")

	err := hub.StopConnector("stop-test")
	if err != nil {
		t.Fatalf("StopConnector failed: %v", err)
	}

	conn, _ := hub.GetConnector("stop-test")
	if conn != nil && conn.Status == ConnectorStatusRunning {
		t.Error("Expected connector to be stopped")
	}
}

func TestConnectorHubStopNonexistent(t *testing.T) {
	hub := newTestConnectorHub(t)

	err := hub.StopConnector("nonexistent")
	if err == nil {
		t.Error("Expected error stopping nonexistent connector")
	}
}

func TestConnectorHubDeleteConnector(t *testing.T) {
	hub := newTestConnectorHub(t)

	_ = hub.CreateConnector(ConnectorConfig{Name: "del-test", Type: ConnectorTypeSink, Driver: "mock"})

	err := hub.DeleteConnector("del-test")
	if err != nil {
		t.Fatalf("DeleteConnector failed: %v", err)
	}

	connectors := hub.ListConnectors()
	if len(connectors) != 0 {
		t.Errorf("Expected 0 connectors after delete, got %d", len(connectors))
	}
}

func TestConnectorHubDeleteNonexistent(t *testing.T) {
	hub := newTestConnectorHub(t)

	err := hub.DeleteConnector("nonexistent")
	if err == nil {
		t.Error("Expected error deleting nonexistent connector")
	}
}

func TestConnectorHubGetConnector(t *testing.T) {
	hub := newTestConnectorHub(t)
	_ = hub.CreateConnector(ConnectorConfig{Name: "get-test", Type: ConnectorTypeSink, Driver: "mock"})

	conn, _ := hub.GetConnector("get-test")
	if conn == nil {
		t.Error("Expected non-nil connector")
	}

	conn2, _ := hub.GetConnector("nonexistent")
	if conn2 != nil {
		t.Error("Expected nil for nonexistent connector")
	}
}

func TestConnectorHubSinkFlushStats(t *testing.T) {
	for _, failWrite := range []bool{false, true} {
		name := "success"
		if failWrite {
			name = "failure_then_retry"
		}
		t.Run(name, func(t *testing.T) {
			hub := newTestConnectorHub(t)
			const metric, connector = "sink_test", "stats_sink"
			writeTestPoints(t, hub.db, metric, 3, time.Now().Add(-time.Minute))
			if err := hub.CreateConnector(ConnectorConfig{
				Name: connector, Type: ConnectorTypeSink, Driver: "mock", BatchSize: 10,
				Filters: ConnectorFilters{IncludeMetrics: []string{metric}},
			}); err != nil {
				t.Fatal(err)
			}
			initial, ok := hub.GetConnector(connector)
			if !ok {
				t.Fatal("connector not found")
			}
			initialList := hub.ListConnectors()
			hub.mu.RLock()
			inst := hub.connectors[connector]
			hub.mu.RUnlock()
			driver := &mockConnectorDriver{}
			if failWrite {
				driver.writeErr = fmt.Errorf("sink rejected batch")
				hub.sinkFlush(connector, driver, inst)
				failed, _ := hub.GetConnector(connector)
				if failed.Stats != (ConnectorInstanceStats{RecordsFailed: 3}) || failed.LastError != "sink rejected batch" {
					t.Fatalf("unexpected failed flush: %+v", failed)
				}
				letters := hub.ListDeadLetters(10)
				if len(letters) != 3 {
					t.Fatalf("dead letter count = %d, want 3", len(letters))
				}
				for _, letter := range letters {
					if letter.ConnectorName != connector || letter.Point.Metric != metric || letter.Error != "sink rejected batch" {
						t.Errorf("unexpected dead letter: %+v", letter)
					}
				}
				driver.mu.Lock()
				driver.writeErr = nil
				driver.mu.Unlock()
			}

			before := time.Now().UnixNano()
			hub.sinkFlush(connector, driver, inst)
			after := time.Now().UnixNano()
			snapshot, _ := hub.GetConnector(connector)
			want := ConnectorInstanceStats{
				RecordsProcessed: 3, BatchesSent: 1, LastActivityAt: snapshot.Stats.LastActivityAt,
			}
			if failWrite {
				want.RecordsFailed = 3
			}
			if snapshot.Stats != want || snapshot.Stats.LastActivityAt < before || snapshot.Stats.LastActivityAt > after {
				t.Fatalf("unexpected successful flush stats: %+v", snapshot.Stats)
			}
			listed := hub.ListConnectors()
			if len(listed) != 1 || listed[0].Stats != snapshot.Stats {
				t.Fatalf("list and get statistics differ: %+v", listed)
			}
			if initial.Stats != (ConnectorInstanceStats{}) || len(initialList) != 1 || initialList[0].Stats != (ConnectorInstanceStats{}) {
				t.Error("an earlier connector snapshot changed after flushing")
			}
			// The checkpoint excludes the already-sent points on the next flush.
			hub.sinkFlush(connector, driver, inst)
			unchanged, _ := hub.GetConnector(connector)
			if unchanged.Stats != snapshot.Stats {
				t.Errorf("empty flush changed statistics: %+v", unchanged.Stats)
			}
			driver.mu.Lock()
			written := append([]Point(nil), driver.written...)
			driver.mu.Unlock()
			if len(written) != 3 {
				t.Fatalf("written point count = %d, want 3", len(written))
			}
			seen := make(map[float64]bool)
			for _, point := range written {
				if point.Metric != metric || point.Value < 0 || point.Value > 2 || seen[point.Value] {
					t.Errorf("unexpected or duplicate written point: %+v", point)
				}
				seen[point.Value] = true
			}
			encoded, err := json.Marshal(snapshot.Stats)
			if err != nil {
				t.Fatal(err)
			}
			var decoded ConnectorInstanceStats
			if err := json.Unmarshal(encoded, &decoded); err != nil || decoded != snapshot.Stats {
				t.Fatalf("statistics JSON changed numeric values: %s (%v)", encoded, err)
			}
			snapshot.Stats.RecordsProcessed = 99
			snapshot.Status = ConnectorStatusFailed
			fresh, _ := hub.GetConnector(connector)
			if fresh.Stats != want || fresh.Status != ConnectorStatusStopped {
				t.Error("editing a returned snapshot changed the connector")
			}
		})
	}
}

func TestConnectorHubDeadLetters(t *testing.T) {
	hub := newTestConnectorHub(t)
	hub.config.DeadLetterEnabled = true

	// Initially empty.
	letters := hub.ListDeadLetters(100)
	if len(letters) != 0 {
		t.Errorf("Expected 0 dead letters, got %d", len(letters))
	}
}

func TestConnectorHubDefaultBatchSize(t *testing.T) {
	hub := newTestConnectorHub(t)

	_ = hub.CreateConnector(ConnectorConfig{
		Name: "defaults", Type: ConnectorTypeSink, Driver: "mock",
	})

	conn, _ := hub.GetConnector("defaults")
	if conn == nil {
		t.Fatal("Expected non-nil connector")
	}
	if conn.Config.BatchSize != hub.config.DefaultBatchSize {
		t.Errorf("Expected default batch size %d, got %d", hub.config.DefaultBatchSize, conn.Config.BatchSize)
	}
}
