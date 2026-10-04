package chronicle

import (
	"context"
	"errors"
	"fmt"
	"regexp"
	"strings"
	"sync"
	"time"
)

// StreamingSQLConfig configures the streaming SQL engine.
type StreamingSQLConfig struct {
	// Enabled enables streaming SQL
	Enabled bool `json:"enabled"`

	// BufferSize for internal channels
	BufferSize int `json:"buffer_size"`

	// WindowTimeout for tumbling windows
	WindowTimeout time.Duration `json:"window_timeout"`

	// MaxConcurrentQueries limits concurrent streaming queries
	MaxConcurrentQueries int `json:"max_concurrent_queries"`

	// StateStoreSize for aggregation state
	StateStoreSize int `json:"state_store_size"`

	// EmitInterval how often to emit results for continuous queries
	EmitInterval time.Duration `json:"emit_interval"`
}

// DefaultStreamingSQLConfig returns default configuration.
func DefaultStreamingSQLConfig() StreamingSQLConfig {
	return StreamingSQLConfig{
		Enabled:              true,
		BufferSize:           10000,
		WindowTimeout:        time.Minute,
		MaxConcurrentQueries: 100,
		StateStoreSize:       100000,
		EmitInterval:         time.Second,
	}
}

// StreamingSQLEngine provides KSQL-style streaming SQL capabilities.
type StreamingSQLEngine struct {
	db     *DB
	config StreamingSQLConfig
	hub    *StreamHub
	mu     sync.RWMutex

	// Active streaming queries and their ID sequence, guarded by queryMu.
	queries       map[string]*StreamingQuery
	queryMu       sync.RWMutex
	querySequence uint64

	// State store for aggregations
	stateStore *StateStore

	// Background context
	ctx    context.Context
	cancel context.CancelFunc
	wg     sync.WaitGroup
}

// StreamingQuery represents an active streaming SQL query.
type StreamingQuery struct {
	ID       string                `json:"id"`
	SQL      string                `json:"sql"`
	Parsed   *ParsedStreamingSQL   `json:"parsed"`
	State    StreamingQueryState   `json:"state"`
	Created  time.Time             `json:"created"`
	LastEmit time.Time             `json:"last_emit"`
	Results  chan *StreamingResult `json:"-"`
	cancel   context.CancelFunc
	mu       sync.Mutex
}

// StreamingQueryState represents query execution state.
type StreamingQueryState int

const (
	StreamingQueryStateCreated StreamingQueryState = iota
	StreamingQueryStateRunning
	StreamingQueryStatePaused
	StreamingQueryStateStopped
	StreamingQueryStateError
)

func (s StreamingQueryState) String() string {
	switch s {
	case StreamingQueryStateCreated:
		return "created"
	case StreamingQueryStateRunning:
		return "running"
	case StreamingQueryStatePaused:
		return "paused"
	case StreamingQueryStateStopped:
		return "stopped"
	case StreamingQueryStateError:
		return "error"
	default:
		return "unknown"
	}
}

// ParsedStreamingSQL represents a parsed streaming SQL statement.
type ParsedStreamingSQL struct {
	Type    StreamingSQLType `json:"type"`
	Source  string           `json:"source"`
	Sink    string           `json:"sink,omitempty"`
	Select  []SelectField    `json:"select"`
	Joins   []StreamJoin     `json:"joins,omitempty"`
	Where   *WhereClause     `json:"where,omitempty"`
	GroupBy *GroupByClause   `json:"group_by,omitempty"`
	Window  *WindowClause    `json:"window,omitempty"`
	Having  *HavingClause    `json:"having,omitempty"`
	Emit    *EmitClause      `json:"emit,omitempty"`
}

// StreamingSQLType identifies the SQL statement type.
type StreamingSQLType int

const (
	StreamingSQLTypeSelect StreamingSQLType = iota
	StreamingSQLTypeCreateStream
	StreamingSQLTypeCreateTable
	StreamingSQLTypeInsertInto
)

// SelectField represents a field in SELECT.
type SelectField struct {
	Expression string `json:"expression"`
	Alias      string `json:"alias,omitempty"`
	Function   string `json:"function,omitempty"`
	Field      string `json:"field,omitempty"`
}

// StreamJoin represents a stream join.
type StreamJoin struct {
	Type      JoinType `json:"type"`
	Stream    string   `json:"stream"`
	Condition string   `json:"condition"`
	Window    string   `json:"window,omitempty"`
}

// JoinType identifies join types.
type JoinType int

const (
	JoinTypeInner JoinType = iota
	JoinTypeLeft
	JoinTypeOuter
)

// WhereClause represents WHERE conditions.
type WhereClause struct {
	Conditions []Condition `json:"conditions"`
}

// Condition represents a filter condition.
type Condition struct {
	Field    string     `json:"field"`
	Operator string     `json:"operator"`
	Value    any        `json:"value"`
	And      *Condition `json:"and,omitempty"`
	Or       *Condition `json:"or,omitempty"`
}

// GroupByClause represents GROUP BY.
type GroupByClause struct {
	Fields []string `json:"fields"`
}

// WindowClause represents window specifications.
type WindowClause struct {
	Type    StreamingWindowType `json:"type"`
	Size    time.Duration       `json:"size"`
	Advance time.Duration       `json:"advance,omitempty"`
	Grace   time.Duration       `json:"grace,omitempty"`
}

// StreamingWindowType identifies window types for streaming SQL.
type StreamingWindowType int

const (
	StreamingWindowTumbling StreamingWindowType = iota
	StreamingWindowHopping
	StreamingWindowSession
	StreamingWindowSliding
)

func (w StreamingWindowType) String() string {
	switch w {
	case StreamingWindowTumbling:
		return "tumbling"
	case StreamingWindowHopping:
		return "hopping"
	case StreamingWindowSession:
		return "session"
	case StreamingWindowSliding:
		return "sliding"
	default:
		return "unknown"
	}
}

// HavingClause represents HAVING conditions.
type HavingClause struct {
	Conditions []Condition `json:"conditions"`
}

// EmitClause represents EMIT specifications.
type EmitClause struct {
	Type     EmitType      `json:"type"`
	Interval time.Duration `json:"interval,omitempty"`
}

// EmitType identifies emit types.
type EmitType int

const (
	EmitTypeChanges EmitType = iota
	EmitTypeFinal
	EmitTypeInterval
)

// StreamingResult represents a streaming query result.
type StreamingResult struct {
	QueryID     string         `json:"query_id"`
	Timestamp   int64          `json:"timestamp"`
	WindowStart int64          `json:"window_start,omitempty"`
	WindowEnd   int64          `json:"window_end,omitempty"`
	GroupKey    string         `json:"group_key,omitempty"`
	Values      map[string]any `json:"values"`
	Type        ResultType     `json:"type"`
}

// ResultType identifies result types.
type ResultType int

const (
	ResultTypeUpdate ResultType = iota
	ResultTypeFinal
	ResultTypeHeartbeat
)

// StateStore manages aggregation state.
type StateStore struct {
	mu      sync.RWMutex
	windows map[string]*WindowState
	maxSize int
}

// WindowState holds state for a window.
type WindowState struct {
	Key     string
	Start   int64
	End     int64
	Values  map[string]float64
	Count   int64
	Sum     float64
	Min     float64
	Max     float64
	Created time.Time
	Updated time.Time
}

// NewStreamingSQLEngine creates a new streaming SQL engine.
func NewStreamingSQLEngine(db *DB, hub *StreamHub, config StreamingSQLConfig) *StreamingSQLEngine {
	ctx, cancel := context.WithCancel(context.Background())

	engine := &StreamingSQLEngine{
		db:      db,
		config:  config,
		hub:     hub,
		queries: make(map[string]*StreamingQuery),
		stateStore: &StateStore{
			windows: make(map[string]*WindowState),
			maxSize: config.StateStoreSize,
		},
		ctx:    ctx,
		cancel: cancel,
	}

	return engine
}

// Start starts the streaming SQL engine.
func (e *StreamingSQLEngine) Start() {
	e.wg.Add(1)
	go e.maintenanceLoop()
}

// Stop stops the streaming SQL engine.
func (e *StreamingSQLEngine) Stop() {
	e.cancel()
	e.wg.Wait()

	// Stop all queries
	e.queryMu.Lock()
	for _, q := range e.queries {
		q.cancel()
	}
	e.queryMu.Unlock()
}

func (e *StreamingSQLEngine) maintenanceLoop() {
	defer e.wg.Done()

	ticker := time.NewTicker(time.Minute)
	defer ticker.Stop()

	for {
		select {
		case <-e.ctx.Done():
			return
		case <-ticker.C:
			e.cleanupExpiredWindows()
		}
	}
}

func (e *StreamingSQLEngine) cleanupExpiredWindows() {
	e.stateStore.mu.Lock()
	defer e.stateStore.mu.Unlock()

	cutoff := time.Now().Add(-e.config.WindowTimeout).UnixNano()

	for key, window := range e.stateStore.windows {
		if window.End < cutoff {
			delete(e.stateStore.windows, key)
		}
	}
}

// ExecuteSQL parses and executes a streaming SQL statement.
func (e *StreamingSQLEngine) ExecuteSQL(sql string) (*StreamingQuery, error) {
	parsed, err := e.ParseSQL(sql)
	if err != nil {
		return nil, fmt.Errorf("parse error: %w", err)
	}

	return e.Execute(parsed, sql)
}

// Execute executes a parsed streaming SQL query.
func (e *StreamingSQLEngine) Execute(parsed *ParsedStreamingSQL, originalSQL string) (*StreamingQuery, error) {
	e.queryMu.Lock()
	if len(e.queries) >= e.config.MaxConcurrentQueries {
		e.queryMu.Unlock()
		return nil, errors.New("max concurrent queries reached")
	}

	ctx, cancel := context.WithCancel(e.ctx)
	created := time.Now()
	e.querySequence++

	query := &StreamingQuery{
		ID:      fmt.Sprintf("ssql-%d-%d", created.UnixNano(), e.querySequence),
		SQL:     originalSQL,
		Parsed:  parsed,
		State:   StreamingQueryStateCreated,
		Created: created,
		Results: make(chan *StreamingResult, e.config.BufferSize),
		cancel:  cancel,
	}

	e.queries[query.ID] = query
	e.queryMu.Unlock()

	// Start query execution
	go e.runQuery(ctx, query)

	return query, nil
}

func (e *StreamingSQLEngine) runQuery(ctx context.Context, query *StreamingQuery) {
	query.mu.Lock()
	query.State = StreamingQueryStateRunning
	query.mu.Unlock()

	defer func() {
		query.mu.Lock()
		if query.State == StreamingQueryStateRunning {
			query.State = StreamingQueryStateStopped
		}
		close(query.Results)
		query.mu.Unlock()
	}()

	// Subscribe to source stream
	sub := e.hub.Subscribe(query.Parsed.Source, nil)
	if sub == nil {
		query.mu.Lock()
		query.State = StreamingQueryStateError
		query.mu.Unlock()
		return
	}
	defer sub.Close()

	// Window state for this query
	windowStates := make(map[string]*WindowState)

	// Emit ticker if needed
	var emitTicker *time.Ticker
	emitInterval := e.config.EmitInterval
	if query.Parsed.Emit != nil && query.Parsed.Emit.Interval > 0 {
		emitInterval = query.Parsed.Emit.Interval
	}
	emitTicker = time.NewTicker(emitInterval)
	defer emitTicker.Stop()

	for {
		select {
		case <-ctx.Done():
			return

		case point, ok := <-sub.C():
			if !ok {
				return
			}

			// Apply WHERE filter
			if query.Parsed.Where != nil && !e.matchesWhere(point, query.Parsed.Where) {
				continue
			}

			// Process aggregation
			if query.Parsed.GroupBy != nil || query.Parsed.Window != nil {
				e.processAggregation(query, point, windowStates)
			} else {
				// Direct projection
				result := e.projectPoint(query, point)
				select {
				case query.Results <- result:
				default:
					// Buffer full, drop
				}
			}

		case <-emitTicker.C:
			// Emit window results
			if query.Parsed.Window != nil {
				e.emitWindowResults(query, windowStates)
			}
		}
	}
}

func (e *StreamingSQLEngine) matchesWhere(point Point, where *WhereClause) bool {
	for _, cond := range where.Conditions {
		if !e.matchesCondition(point, cond) {
			return false
		}
	}
	return true
}

func (e *StreamingSQLEngine) matchesCondition(point Point, cond Condition) bool {
	var value any

	switch cond.Field {
	case "value":
		value = point.Value
	case "metric":
		value = point.Metric
	default:
		// Check tags
		if v, ok := point.Tags[cond.Field]; ok {
			value = v
		} else {
			return false
		}
	}

	// Compare
	switch cond.Operator {
	case "=", "==":
		return fmt.Sprintf("%v", value) == fmt.Sprintf("%v", cond.Value)
	case "!=", "<>":
		return fmt.Sprintf("%v", value) != fmt.Sprintf("%v", cond.Value)
	case ">":
		if v, ok := value.(float64); ok {
			if cv, ok := cond.Value.(float64); ok {
				return v > cv
			}
		}
	case ">=":
		if v, ok := value.(float64); ok {
			if cv, ok := cond.Value.(float64); ok {
				return v >= cv
			}
		}
	case "<":
		if v, ok := value.(float64); ok {
			if cv, ok := cond.Value.(float64); ok {
				return v < cv
			}
		}
	case "<=":
		if v, ok := value.(float64); ok {
			if cv, ok := cond.Value.(float64); ok {
				return v <= cv
			}
		}
	case "LIKE":
		if s, ok := value.(string); ok {
			pattern := strings.ReplaceAll(fmt.Sprintf("%v", cond.Value), "%", ".*")
			matched, _ := regexp.MatchString(pattern, s)
			return matched
		}
	}

	return false
}

func (e *StreamingSQLEngine) processAggregation(query *StreamingQuery, point Point, windowStates map[string]*WindowState) {
	// Calculate window boundaries
	windowSize := e.config.WindowTimeout
	if query.Parsed.Window != nil {
		windowSize = query.Parsed.Window.Size
	}

	windowStart := (point.Timestamp / int64(windowSize)) * int64(windowSize)
	windowEnd := windowStart + int64(windowSize)

	// Build group key
	groupKey := ""
	if query.Parsed.GroupBy != nil {
		var parts []string
		for _, field := range query.Parsed.GroupBy.Fields {
			if v, ok := point.Tags[field]; ok {
				parts = append(parts, v)
			}
		}
		groupKey = strings.Join(parts, "|")
	}

	// State key combines window and group
	stateKey := fmt.Sprintf("%d:%s", windowStart, groupKey)

	// Get or create window state
	state, ok := windowStates[stateKey]
	if !ok {
		state = &WindowState{
			Key:     groupKey,
			Start:   windowStart,
			End:     windowEnd,
			Values:  make(map[string]float64),
			Min:     point.Value,
			Max:     point.Value,
			Created: time.Now(),
		}
		windowStates[stateKey] = state
	}

	// Update aggregations
	state.Count++
	state.Sum += point.Value
	if point.Value < state.Min {
		state.Min = point.Value
	}
	if point.Value > state.Max {
		state.Max = point.Value
	}
	state.Updated = time.Now()

	// Store field values for complex aggregations
	for _, sel := range query.Parsed.Select {
		if sel.Field != "" {
			state.Values[sel.Field] = point.Value
		}
	}
}
