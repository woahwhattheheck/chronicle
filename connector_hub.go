package chronicle

import (
	"encoding/json"
	"fmt"
	"os"
	"sync"
	"time"
)

// ConnectorType represents the type of a connector.
type ConnectorType string

const (
	ConnectorTypeSource ConnectorType = "source"
	ConnectorTypeSink   ConnectorType = "sink"
)

// ConnectorStatus represents the lifecycle state of a connector.
type ConnectorStatus string

const (
	ConnectorStatusStopped ConnectorStatus = "stopped"
	ConnectorStatusRunning ConnectorStatus = "running"
	ConnectorStatusFailed  ConnectorStatus = "failed"
	ConnectorStatusPaused  ConnectorStatus = "paused"
)

// ConnectorHubConfig configures the connector hub.
type ConnectorHubConfig struct {
	Enabled             bool          `json:"enabled"`
	MaxConnectors       int           `json:"max_connectors"`
	HealthCheckInterval time.Duration `json:"health_check_interval"`
	DeadLetterEnabled   bool          `json:"dead_letter_enabled"`
	DeadLetterMax       int           `json:"dead_letter_max"`
	DefaultBatchSize    int           `json:"default_batch_size"`
	DefaultFlushMs      int           `json:"default_flush_ms"`
}

// DefaultConnectorHubConfig returns sensible defaults.
func DefaultConnectorHubConfig() ConnectorHubConfig {
	return ConnectorHubConfig{
		Enabled:             false,
		MaxConnectors:       50,
		HealthCheckInterval: 30 * time.Second,
		DeadLetterEnabled:   true,
		DeadLetterMax:       10000,
		DefaultBatchSize:    500,
		DefaultFlushMs:      5000,
	}
}

// ConnectorConfig is the configuration for a single connector instance.
type ConnectorConfig struct {
	Name            string            `json:"name"`
	Type            ConnectorType     `json:"type"`
	Driver          string            `json:"driver"`
	BatchSize       int               `json:"batch_size"`
	FlushIntervalMs int               `json:"flush_interval_ms"`
	Properties      map[string]string `json:"properties"`
	Filters         ConnectorFilters  `json:"filters,omitempty"`
}

// ConnectorFilters defines which data to include/exclude.
type ConnectorFilters struct {
	IncludeMetrics []string          `json:"include_metrics,omitempty"`
	ExcludeMetrics []string          `json:"exclude_metrics,omitempty"`
	TagFilters     map[string]string `json:"tag_filters,omitempty"`
}

// ConnectorInstance represents a running connector instance.
type ConnectorInstance struct {
	Config    ConnectorConfig        `json:"config"`
	Status    ConnectorStatus        `json:"status"`
	StartedAt time.Time              `json:"started_at,omitempty"`
	Stats     ConnectorInstanceStats `json:"stats"`
	LastError string                 `json:"last_error,omitempty"`
	done      chan struct{}
}

// ConnectorInstanceStats tracks connector throughput.
type ConnectorInstanceStats struct {
	RecordsProcessed uint64 `json:"records_processed"`
	RecordsFailed    uint64 `json:"records_failed"`
	BytesProcessed   int64  `json:"bytes_processed"`
	BatchesSent      uint64 `json:"batches_sent"`
	LastActivityAt   int64  `json:"last_activity_at"`
}

// DeadLetterEntry represents a failed record in the dead letter queue.
type DeadLetterEntry struct {
	ConnectorName string    `json:"connector_name"`
	Point         Point     `json:"point"`
	Error         string    `json:"error"`
	Timestamp     time.Time `json:"timestamp"`
	RetryCount    int       `json:"retry_count"`
}

// ConnectorDriver defines the interface for connector implementations.
type ConnectorDriver interface {
	Name() string
	Type() ConnectorType
	Initialize(config map[string]string) error
	Write(points []Point) error
	Read(limit int) ([]Point, error)
	HealthCheck() error
	Close() error
}

// ConnectorHub manages pluggable source and sink connectors.
type ConnectorHub struct {
	config     ConnectorHubConfig
	db         *DB
	connectors map[string]*ConnectorInstance
	drivers    map[string]func() ConnectorDriver
	deadLetter []DeadLetterEntry

	mu   sync.RWMutex
	done chan struct{}
}

// NewConnectorHub creates a new connector hub.
func NewConnectorHub(db *DB, config ConnectorHubConfig) *ConnectorHub {
	hub := &ConnectorHub{
		config:     config,
		db:         db,
		connectors: make(map[string]*ConnectorInstance),
		drivers:    make(map[string]func() ConnectorDriver),
		deadLetter: make([]DeadLetterEntry, 0),
		done:       make(chan struct{}),
	}
	// Register built-in drivers
	hub.registerBuiltinDrivers()
	return hub
}

// RegisterDriver registers a connector driver factory.
func (h *ConnectorHub) RegisterDriver(name string, factory func() ConnectorDriver) {
	h.mu.Lock()
	defer h.mu.Unlock()
	h.drivers[name] = factory
}

// CreateConnector creates and registers a new connector instance.
func (h *ConnectorHub) CreateConnector(config ConnectorConfig) error {
	h.mu.Lock()
	defer h.mu.Unlock()

	if _, exists := h.connectors[config.Name]; exists {
		return fmt.Errorf("connector %q already exists", config.Name)
	}
	if len(h.connectors) >= h.config.MaxConnectors {
		return fmt.Errorf("max connectors reached (%d)", h.config.MaxConnectors)
	}
	if config.BatchSize <= 0 {
		config.BatchSize = h.config.DefaultBatchSize
	}
	if config.FlushIntervalMs <= 0 {
		config.FlushIntervalMs = h.config.DefaultFlushMs
	}

	h.connectors[config.Name] = &ConnectorInstance{
		Config: config,
		Status: ConnectorStatusStopped,
		done:   make(chan struct{}),
	}
	return nil
}

// StartConnector starts a connector by name.
func (h *ConnectorHub) StartConnector(name string) error {
	h.mu.Lock()
	inst, exists := h.connectors[name]
	if !exists {
		h.mu.Unlock()
		return fmt.Errorf("connector %q not found", name)
	}
	if inst.Status == ConnectorStatusRunning {
		h.mu.Unlock()
		return fmt.Errorf("connector %q already running", name)
	}

	factory, hasDriver := h.drivers[inst.Config.Driver]
	if !hasDriver {
		h.mu.Unlock()
		return fmt.Errorf("driver %q not registered", inst.Config.Driver)
	}

	driver := factory()
	if err := driver.Initialize(inst.Config.Properties); err != nil {
		h.mu.Unlock()
		return fmt.Errorf("driver init failed: %w", err)
	}

	inst.Status = ConnectorStatusRunning
	inst.StartedAt = time.Now()
	inst.done = make(chan struct{})
	h.mu.Unlock()

	if inst.Config.Type == ConnectorTypeSink {
		go h.runSinkLoop(name, driver, inst)
	}
	return nil
}

// StopConnector stops a running connector.
func (h *ConnectorHub) StopConnector(name string) error {
	h.mu.Lock()
	defer h.mu.Unlock()

	inst, exists := h.connectors[name]
	if !exists {
		return fmt.Errorf("connector %q not found", name)
	}
	if inst.Status != ConnectorStatusRunning {
		return nil
	}
	select {
	case <-inst.done:
	default:
		close(inst.done)
	}
	inst.Status = ConnectorStatusStopped
	return nil
}

// DeleteConnector removes a connector.
func (h *ConnectorHub) DeleteConnector(name string) error {
	h.mu.Lock()
	defer h.mu.Unlock()

	inst, exists := h.connectors[name]
	if !exists {
		return fmt.Errorf("connector %q not found", name)
	}
	if inst.Status == ConnectorStatusRunning {
		select {
		case <-inst.done:
		default:
			close(inst.done)
		}
	}
	delete(h.connectors, name)
	return nil
}

// ListConnectors returns all registered connectors.
func (h *ConnectorHub) ListConnectors() []ConnectorInstance {
	h.mu.RLock()
	defer h.mu.RUnlock()

	result := make([]ConnectorInstance, 0, len(h.connectors))
	for _, inst := range h.connectors {
		result = append(result, *inst)
	}
	return result
}

// GetConnector returns a snapshot of a specific connector by name.
func (h *ConnectorHub) GetConnector(name string) (*ConnectorInstance, bool) {
	h.mu.RLock()
	defer h.mu.RUnlock()
	inst, ok := h.connectors[name]
	if !ok {
		return nil, false
	}
	snapshot := *inst
	return &snapshot, true
}

// ListDeadLetters returns entries from the dead letter queue.
func (h *ConnectorHub) ListDeadLetters(limit int) []DeadLetterEntry {
	h.mu.RLock()
	defer h.mu.RUnlock()

	if limit <= 0 || limit > len(h.deadLetter) {
		limit = len(h.deadLetter)
	}
	result := make([]DeadLetterEntry, limit)
	copy(result, h.deadLetter[:limit])
	return result
}

// DrainDeadLetters removes and returns all dead letter entries.
func (h *ConnectorHub) DrainDeadLetters() []DeadLetterEntry {
	h.mu.Lock()
	defer h.mu.Unlock()

	result := h.deadLetter
	h.deadLetter = make([]DeadLetterEntry, 0)
	return result
}

// ListDrivers returns all registered driver names.
func (h *ConnectorHub) ListDrivers() []string {
	h.mu.RLock()
	defer h.mu.RUnlock()

	names := make([]string, 0, len(h.drivers))
	for name := range h.drivers {
		names = append(names, name)
	}
	return names
}

// Start begins the connector hub background services.
func (h *ConnectorHub) Start() {
	if !h.config.Enabled {
		return
	}
	go h.healthCheckLoop()
}

// Stop halts all connectors and the hub.
func (h *ConnectorHub) Stop() {
	select {
	case <-h.done:
	default:
		close(h.done)
	}

	h.mu.Lock()
	for _, inst := range h.connectors {
		if inst.Status == ConnectorStatusRunning {
			select {
			case <-inst.done:
			default:
				close(inst.done)
			}
			inst.Status = ConnectorStatusStopped
		}
	}
	h.mu.Unlock()
}

func (h *ConnectorHub) runSinkLoop(name string, driver ConnectorDriver, inst *ConnectorInstance) {
	flushInterval := time.Duration(inst.Config.FlushIntervalMs) * time.Millisecond
	ticker := time.NewTicker(flushInterval)
	defer ticker.Stop()
	defer driver.Close()

	for {
		select {
		case <-inst.done:
			return
		case <-ticker.C:
			h.sinkFlush(name, driver, inst)
		}
	}
}

func (h *ConnectorHub) sinkFlush(name string, driver ConnectorDriver, inst *ConnectorInstance) {
	if h.db == nil {
		return
	}

	// Read recent points from the database for this sink
	h.mu.RLock()
	lastActivity := inst.Stats.LastActivityAt
	h.mu.RUnlock()
	q := &Query{
		Start: lastActivity,
		End:   time.Now().UnixNano(),
		Limit: inst.Config.BatchSize,
	}
	if len(inst.Config.Filters.IncludeMetrics) > 0 {
		q.Metric = inst.Config.Filters.IncludeMetrics[0]
	}

	result, err := h.db.Execute(q)
	if err != nil || result == nil || len(result.Points) == 0 {
		return
	}

	points := h.filterPoints(result.Points, inst.Config.Filters)
	if len(points) == 0 {
		return
	}

	if writeErr := driver.Write(points); writeErr != nil {
		h.mu.Lock()
		inst.Stats.RecordsFailed += uint64(len(points))
		inst.LastError = writeErr.Error()
		if h.config.DeadLetterEnabled {
			for _, p := range points {
				if len(h.deadLetter) < h.config.DeadLetterMax {
					h.deadLetter = append(h.deadLetter, DeadLetterEntry{
						ConnectorName: name,
						Point:         p,
						Error:         writeErr.Error(),
						Timestamp:     time.Now(),
					})
				}
			}
		}
		h.mu.Unlock()
		return
	}

	h.mu.Lock()
	inst.Stats.RecordsProcessed += uint64(len(points))
	inst.Stats.BatchesSent++
	inst.Stats.LastActivityAt = time.Now().UnixNano()
	h.mu.Unlock()
}

func (h *ConnectorHub) filterPoints(points []Point, filters ConnectorFilters) []Point {
	if len(filters.IncludeMetrics) == 0 && len(filters.ExcludeMetrics) == 0 && len(filters.TagFilters) == 0 {
		return points
	}

	include := make(map[string]bool, len(filters.IncludeMetrics))
	for _, m := range filters.IncludeMetrics {
		include[m] = true
	}
	exclude := make(map[string]bool, len(filters.ExcludeMetrics))
	for _, m := range filters.ExcludeMetrics {
		exclude[m] = true
	}

	result := make([]Point, 0, len(points))
	for _, p := range points {
		if len(include) > 0 && !include[p.Metric] {
			continue
		}
		if exclude[p.Metric] {
			continue
		}
		if len(filters.TagFilters) > 0 {
			match := true
			for k, v := range filters.TagFilters {
				if p.Tags[k] != v {
					match = false
					break
				}
			}
			if !match {
				continue
			}
		}
		result = append(result, p)
	}
	return result
}

func (h *ConnectorHub) healthCheckLoop() {
	ticker := time.NewTicker(h.config.HealthCheckInterval)
	defer ticker.Stop()

	for {
		select {
		case <-h.done:
			return
		case <-ticker.C:
			// Health check is a no-op for driver-less connectors (built-in health tracking via stats)
		}
	}
}

func (h *ConnectorHub) registerBuiltinDrivers() {
	h.drivers["stdout"] = func() ConnectorDriver { return &stdoutConnectorDriver{} }
	h.drivers["noop"] = func() ConnectorDriver { return &noopConnectorDriver{} }
	h.drivers["file"] = func() ConnectorDriver { return &fileConnectorDriver{} }
}

// stdoutConnectorDriver writes points to stdout as JSON (useful for debugging).
type stdoutConnectorDriver struct{}

func (d *stdoutConnectorDriver) Name() string                         { return "stdout" }
func (d *stdoutConnectorDriver) Type() ConnectorType                  { return ConnectorTypeSink }
func (d *stdoutConnectorDriver) Initialize(_ map[string]string) error { return nil }
func (d *stdoutConnectorDriver) Write(points []Point) error {
	for _, p := range points {
		data, _ := json.Marshal(p)
		fmt.Println(string(data))
	}
	return nil
}
func (d *stdoutConnectorDriver) Read(_ int) ([]Point, error) {
	return nil, fmt.Errorf("stdout is sink-only")
}
func (d *stdoutConnectorDriver) HealthCheck() error { return nil }
func (d *stdoutConnectorDriver) Close() error       { return nil }

// noopConnectorDriver discards all data (useful for testing).
type noopConnectorDriver struct{}

func (d *noopConnectorDriver) Name() string                         { return "noop" }
func (d *noopConnectorDriver) Type() ConnectorType                  { return ConnectorTypeSink }
func (d *noopConnectorDriver) Initialize(_ map[string]string) error { return nil }
func (d *noopConnectorDriver) Write(_ []Point) error                { return nil }
func (d *noopConnectorDriver) Read(_ int) ([]Point, error)          { return nil, nil }
func (d *noopConnectorDriver) HealthCheck() error                   { return nil }
func (d *noopConnectorDriver) Close() error                         { return nil }

// fileConnectorDriver writes points as JSON lines to a file (for persistent export).
type fileConnectorDriver struct {
	mu   sync.Mutex
	path string
	file *os.File
}

func (d *fileConnectorDriver) Name() string        { return "file" }
func (d *fileConnectorDriver) Type() ConnectorType { return ConnectorTypeSink }
func (d *fileConnectorDriver) HealthCheck() error  { return nil }

func (d *fileConnectorDriver) Initialize(config map[string]string) error {
	d.path = config["path"]
	if d.path == "" {
		return fmt.Errorf("file driver requires 'path' property")
	}
	f, err := os.OpenFile(d.path, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0600)
	if err != nil {
		return fmt.Errorf("open file: %w", err)
	}
	d.file = f
	return nil
}

func (d *fileConnectorDriver) Write(points []Point) error {
	d.mu.Lock()
	defer d.mu.Unlock()
	if d.file == nil {
		return fmt.Errorf("file driver not initialized")
	}
	for _, p := range points {
		data, err := json.Marshal(p)
		if err != nil {
			continue
		}
		d.file.Write(append(data, '\n'))
	}
	return nil
}

func (d *fileConnectorDriver) Read(_ int) ([]Point, error) {
	return nil, fmt.Errorf("file driver is sink-only")
}

func (d *fileConnectorDriver) Close() error {
	d.mu.Lock()
	defer d.mu.Unlock()
	if d.file != nil {
		err := d.file.Close()
		d.file = nil
		return err
	}
	return nil
}

// Ensure drivers implement the interface.
var (
	_ ConnectorDriver = (*stdoutConnectorDriver)(nil)
	_ ConnectorDriver = (*noopConnectorDriver)(nil)
	_ ConnectorDriver = (*fileConnectorDriver)(nil)
)
