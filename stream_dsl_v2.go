package chronicle

import (
	"crypto/rand"
	"encoding/hex"
	"fmt"
	"strings"
	"sync"
	"sync/atomic"
	"time"
)

// DSLV2WindowType defines the type of window for the v2 stream DSL.
type DSLV2WindowType int

const (
	DSLV2WindowTumbling DSLV2WindowType = iota
	DSLV2WindowSliding
	DSLV2WindowSession
	DSLV2WindowCount
	DSLV2WindowGlobal
)

func (t DSLV2WindowType) String() string {
	switch t {
	case DSLV2WindowTumbling:
		return "TUMBLING"
	case DSLV2WindowSliding:
		return "SLIDING"
	case DSLV2WindowSession:
		return "SESSION"
	case DSLV2WindowCount:
		return "COUNT"
	case DSLV2WindowGlobal:
		return "GLOBAL"
	default:
		return "UNKNOWN"
	}
}

// StreamWindowV2 specifies a window for stream processing.
type StreamWindowV2 struct {
	Type     DSLV2WindowType `json:"type"`
	Size     time.Duration   `json:"size"`
	Slide    time.Duration   `json:"slide"`
	Gap      time.Duration   `json:"gap"`
	MaxCount int             `json:"max_count"`
}

// DSLV2JoinType defines the type of join operation.
type DSLV2JoinType int

const (
	DSLV2JoinInner DSLV2JoinType = iota
	DSLV2JoinLeft
	DSLV2JoinOuter
	DSLV2JoinCross
)

func (t DSLV2JoinType) String() string {
	switch t {
	case DSLV2JoinInner:
		return "INNER"
	case DSLV2JoinLeft:
		return "LEFT"
	case DSLV2JoinOuter:
		return "OUTER"
	case DSLV2JoinCross:
		return "CROSS"
	default:
		return "UNKNOWN"
	}
}

// DSLV2Join describes a join between two streams.
type DSLV2Join struct {
	Type        DSLV2JoinType `json:"type"`
	LeftStream  string        `json:"left_stream"`
	RightStream string        `json:"right_stream"`
	OnCondition string        `json:"on_condition"`
	Within      time.Duration `json:"within"`
}

// CEPEvent is a single event specification in a CEP pattern.
type CEPEvent struct {
	Metric    string `json:"metric"`
	Condition string `json:"condition"`
	Optional  bool   `json:"optional"`
}

// CEPPattern defines a Complex Event Processing pattern.
type CEPPattern struct {
	Name   string                         `json:"name"`
	Events []CEPEvent                     `json:"events"`
	Within time.Duration                  `json:"within"`
	Action func(matched []map[string]any) `json:"-"`
}

// StreamDSLV2StatementType defines the type of DSL statement.
type StreamDSLV2StatementType int

const (
	StmtSelect StreamDSLV2StatementType = iota
	StmtWindow
	StmtJoin
	StmtPattern
	StmtEmit
)

func (t StreamDSLV2StatementType) String() string {
	switch t {
	case StmtSelect:
		return "SELECT"
	case StmtWindow:
		return "WINDOW"
	case StmtJoin:
		return "JOIN"
	case StmtPattern:
		return "PATTERN"
	case StmtEmit:
		return "EMIT"
	default:
		return "UNKNOWN"
	}
}

// StreamDSLV2Statement is a parsed DSL statement.
type StreamDSLV2Statement struct {
	Type     StreamDSLV2StatementType `json:"type"`
	Source   string                   `json:"source"`
	Window   *StreamWindowV2          `json:"window,omitempty"`
	Joins    []DSLV2Join              `json:"joins,omitempty"`
	Patterns []CEPPattern             `json:"-"`
	GroupBy  []string                 `json:"group_by,omitempty"`
	Having   string                   `json:"having,omitempty"`
	EmitTo   string                   `json:"emit_to,omitempty"`
	Raw      string                   `json:"raw"`
}

// StreamDSLV2Result holds results from a continuous query evaluation.
type StreamDSLV2Result struct {
	Statement   *StreamDSLV2Statement `json:"statement"`
	Rows        []map[string]any      `json:"rows"`
	WindowStart time.Time             `json:"window_start"`
	WindowEnd   time.Time             `json:"window_end"`
	Watermark   time.Time             `json:"watermark"`
}

// DSLV2ContinuousQueryState represents the lifecycle state of a continuous query.
type DSLV2ContinuousQueryState int

const (
	QueryV2Created DSLV2ContinuousQueryState = iota
	QueryV2Running
	QueryV2Paused
	QueryV2Stopped
	QueryV2Error
)

func (s DSLV2ContinuousQueryState) String() string {
	switch s {
	case QueryV2Created:
		return "created"
	case QueryV2Running:
		return "running"
	case QueryV2Paused:
		return "paused"
	case QueryV2Stopped:
		return "stopped"
	case QueryV2Error:
		return "error"
	default:
		return "unknown"
	}
}

// DSLV2ContinuousQueryStats holds per-query statistics.
type DSLV2ContinuousQueryStats struct {
	EventsProcessed int64         `json:"events_processed"`
	ResultsEmitted  int64         `json:"results_emitted"`
	Errors          int64         `json:"errors"`
	AvgLatency      time.Duration `json:"avg_latency_ns"`
}

// DSLV2ContinuousQuery represents a registered continuous query.
type DSLV2ContinuousQuery struct {
	ID       string                    `json:"id"`
	Name     string                    `json:"name"`
	DSL      string                    `json:"dsl"`
	Compiled *StreamDSLV2Statement     `json:"compiled"`
	State    DSLV2ContinuousQueryState `json:"state"`
	Created  time.Time                 `json:"created"`
	Stats    DSLV2ContinuousQueryStats `json:"stats"`
}

// StreamDSLV2Config holds configuration for the v2 stream DSL engine.
type StreamDSLV2Config struct {
	MaxConcurrentQueries int           `json:"max_concurrent_queries"`
	DefaultWindowSize    time.Duration `json:"default_window_size"`
	MaxWindowSize        time.Duration `json:"max_window_size"`
	EnableCEP            bool          `json:"enable_cep"`
	EnableJoins          bool          `json:"enable_joins"`
	StateBackend         string        `json:"state_backend"` // "memory" or "disk"
	CheckpointInterval   time.Duration `json:"checkpoint_interval"`
	MaxStateSize         int64         `json:"max_state_size"`
}

// DefaultStreamDSLV2Config returns a StreamDSLV2Config with sensible defaults.
func DefaultStreamDSLV2Config() StreamDSLV2Config {
	return StreamDSLV2Config{
		MaxConcurrentQueries: 100,
		DefaultWindowSize:    1 * time.Minute,
		MaxWindowSize:        24 * time.Hour,
		EnableCEP:            true,
		EnableJoins:          true,
		StateBackend:         "memory",
		CheckpointInterval:   30 * time.Second,
		MaxStateSize:         256 * 1024 * 1024, // 256 MB
	}
}

// StreamDSLV2Stats holds engine-wide statistics.
type StreamDSLV2Stats struct {
	ActiveQueries        int           `json:"active_queries"`
	TotalEventsProcessed int64         `json:"total_events_processed"`
	EventsPerSec         float64       `json:"events_per_sec"`
	PatternsMatched      int64         `json:"patterns_matched"`
	AvgLatency           time.Duration `json:"avg_latency_ns"`
	StateSizeBytes       int64         `json:"state_size_bytes"`
}

// streamDSLV2StateStore is an in-memory state store for windowed aggregation.
type streamDSLV2StateStore struct {
	mu      sync.Mutex
	windows map[string][]streamDSLV2WindowEntry
	size    int64
}

type streamDSLV2WindowEntry struct {
	Value     float64
	Tags      map[string]string
	Timestamp time.Time
}

func newStreamDSLV2StateStore() *streamDSLV2StateStore {
	return &streamDSLV2StateStore{
		windows: make(map[string][]streamDSLV2WindowEntry),
	}
}

func (s *streamDSLV2StateStore) add(key string, entry streamDSLV2WindowEntry) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.windows[key] = append(s.windows[key], entry)
	s.size += 64 // approximate entry size
}

func (s *streamDSLV2StateStore) get(key string) []streamDSLV2WindowEntry {
	s.mu.Lock()
	defer s.mu.Unlock()
	entries := s.windows[key]
	cp := make([]streamDSLV2WindowEntry, len(entries))
	copy(cp, entries)
	return cp
}

func (s *streamDSLV2StateStore) clear(key string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if entries, ok := s.windows[key]; ok {
		s.size -= int64(len(entries)) * 64
		if s.size < 0 {
			s.size = 0
		}
	}
	delete(s.windows, key)
}

func (s *streamDSLV2StateStore) sizeBytes() int64 {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.size
}

// StreamDSLV2Engine is the main engine for the v2 stream DSL.
//
// 🧪 EXPERIMENTAL: This API may change or be removed without notice.
// See api_stability.go for stability classifications.
type StreamDSLV2Engine struct {
	db       *DB
	config   StreamDSLV2Config
	queries  map[string]*DSLV2ContinuousQuery
	patterns map[string]*CEPPattern
	results  map[string][]StreamDSLV2Result
	state    *streamDSLV2StateStore
	mu       sync.RWMutex

	totalEvents     atomic.Int64
	patternsMatched atomic.Int64
	totalLatencyNs  atomic.Int64
	latencyCount    atomic.Int64
	startTime       time.Time
}

// NewStreamDSLV2Engine creates a new v2 stream DSL engine.
func NewStreamDSLV2Engine(db *DB, config StreamDSLV2Config) *StreamDSLV2Engine {
	return &StreamDSLV2Engine{
		db:        db,
		config:    config,
		queries:   make(map[string]*DSLV2ContinuousQuery),
		patterns:  make(map[string]*CEPPattern),
		results:   make(map[string][]StreamDSLV2Result),
		state:     newStreamDSLV2StateStore(),
		startTime: time.Now(),
	}
}

func streamDSLV2GenerateID() (string, error) {
	b := make([]byte, 8)
	if _, err := rand.Read(b); err != nil {
		return "", fmt.Errorf("crypto/rand failed: %w", err)
	}
	return hex.EncodeToString(b), nil
}

// Parse parses a DSL string into a StreamDSLV2Statement.
//
// Supported DSL forms:
//
//	SELECT ... FROM <source> [WINDOW TUMBLING|SLIDING|SESSION|COUNT|GLOBAL <size>] [GROUP BY ...] [HAVING ...] [EMIT TO ...]
//	PATTERN <name> (<metric>[?] [, ...]) WITHIN <dur>
//	JOIN INNER|LEFT|OUTER|CROSS <left> <right> ON <cond> WITHIN <dur>
func (e *StreamDSLV2Engine) Parse(dsl string) (*StreamDSLV2Statement, error) {
	dsl = strings.TrimSpace(dsl)
	if dsl == "" {
		return nil, fmt.Errorf("stream dsl v2: empty statement")
	}

	tokens := strings.Fields(dsl)
	keyword := strings.ToUpper(tokens[0])

	switch keyword {
	case "SELECT":
		return e.parseSelect(tokens, dsl)
	case "PATTERN":
		return e.parsePattern(tokens, dsl)
	case "JOIN":
		return e.parseJoin(tokens, dsl)
	default:
		return nil, fmt.Errorf("stream dsl v2: unsupported statement type %q", keyword)
	}
}

func (e *StreamDSLV2Engine) parseSelect(tokens []string, raw string) (*StreamDSLV2Statement, error) {
	stmt := &StreamDSLV2Statement{
		Type: StmtSelect,
		Raw:  raw,
	}

	// Find FROM
	fromIdx := -1
	for i, t := range tokens {
		if strings.ToUpper(t) == "FROM" {
			fromIdx = i
			break
		}
	}
	if fromIdx < 0 || fromIdx+1 >= len(tokens) {
		return nil, fmt.Errorf("stream dsl v2: SELECT requires FROM clause")
	}
	stmt.Source = tokens[fromIdx+1]

	// Parse optional WINDOW clause
	for i := fromIdx + 2; i < len(tokens); i++ {
		upper := strings.ToUpper(tokens[i])
		switch upper {
		case "WINDOW":
			if i+2 >= len(tokens) {
				return nil, fmt.Errorf("stream dsl v2: incomplete WINDOW clause")
			}
			w, consumed, err := e.parseWindowClause(tokens[i+1:])
			if err != nil {
				return nil, err
			}
			stmt.Window = w
			stmt.Type = StmtWindow
			i += consumed
		case "GROUP":
			if i+1 < len(tokens) && strings.ToUpper(tokens[i+1]) == "BY" {
				i += 2
				for i < len(tokens) {
					upper2 := strings.ToUpper(tokens[i])
					if upper2 == "HAVING" || upper2 == "EMIT" || upper2 == "WINDOW" {
						break
					}
					stmt.GroupBy = append(stmt.GroupBy, strings.TrimRight(tokens[i], ","))
					i++
				}
				i-- // will be incremented by loop
			}
		case "HAVING":
			if i+1 < len(tokens) {
				i++
				var havParts []string
				for i < len(tokens) {
					upper2 := strings.ToUpper(tokens[i])
					if upper2 == "EMIT" || upper2 == "WINDOW" || upper2 == "GROUP" {
						break
					}
					havParts = append(havParts, tokens[i])
					i++
				}
				stmt.Having = strings.Join(havParts, " ")
				i-- // will be incremented by loop
			}
		case "EMIT":
			if i+1 < len(tokens) && strings.ToUpper(tokens[i+1]) == "TO" {
				if i+2 < len(tokens) {
					stmt.EmitTo = tokens[i+2]
					stmt.Type = StmtEmit
					i += 2
				}
			}
		}
	}

	return stmt, nil
}

func (e *StreamDSLV2Engine) parseWindowClause(tokens []string) (*StreamWindowV2, int, error) {
	if len(tokens) < 2 {
		return nil, 0, fmt.Errorf("stream dsl v2: incomplete WINDOW clause")
	}

	w := &StreamWindowV2{}
	consumed := 0

	switch strings.ToUpper(tokens[0]) {
	case "TUMBLING":
		w.Type = DSLV2WindowTumbling
	case "SLIDING":
		w.Type = DSLV2WindowSliding
	case "SESSION":
		w.Type = DSLV2WindowSession
	case "COUNT":
		w.Type = DSLV2WindowCount
	case "GLOBAL":
		w.Type = DSLV2WindowGlobal
		consumed = 1
		return w, consumed, nil
	default:
		return nil, 0, fmt.Errorf("stream dsl v2: unknown window type %q", tokens[0])
	}
	consumed++

	d, err := time.ParseDuration(tokens[1])
	if err != nil {
		return nil, 0, fmt.Errorf("stream dsl v2: invalid window size %q: %w", tokens[1], err)
	}
	w.Size = d
	consumed++

	if w.Size > e.config.MaxWindowSize {
		return nil, 0, fmt.Errorf("stream dsl v2: window size %v exceeds max %v", w.Size, e.config.MaxWindowSize)
	}

	// Sliding windows need a SLIDE clause
	if w.Type == DSLV2WindowSliding && consumed < len(tokens) && strings.ToUpper(tokens[consumed]) == "SLIDE" {
		consumed++
		if consumed >= len(tokens) {
			return nil, 0, fmt.Errorf("stream dsl v2: SLIDE requires duration")
		}
		sd, err := time.ParseDuration(tokens[consumed])
		if err != nil {
			return nil, 0, fmt.Errorf("stream dsl v2: invalid SLIDE duration %q: %w", tokens[consumed], err)
		}
		w.Slide = sd
		consumed++
	}

	// Session windows need a GAP clause
	if w.Type == DSLV2WindowSession && consumed < len(tokens) && strings.ToUpper(tokens[consumed]) == "GAP" {
		consumed++
		if consumed >= len(tokens) {
			return nil, 0, fmt.Errorf("stream dsl v2: GAP requires duration")
		}
		gd, err := time.ParseDuration(tokens[consumed])
		if err != nil {
			return nil, 0, fmt.Errorf("stream dsl v2: invalid GAP duration %q: %w", tokens[consumed], err)
		}
		w.Gap = gd
		consumed++
	}

	return w, consumed, nil
}

func (e *StreamDSLV2Engine) parsePattern(tokens []string, raw string) (*StreamDSLV2Statement, error) {
	// PATTERN <name> (<events...>) WITHIN <dur>
	if len(tokens) < 4 {
		return nil, fmt.Errorf("stream dsl v2: incomplete PATTERN statement")
	}

	stmt := &StreamDSLV2Statement{
		Type: StmtPattern,
		Raw:  raw,
	}

	patternName := tokens[1]
	pattern := CEPPattern{Name: patternName}

	// Find WITHIN
	withinIdx := -1
	for i, t := range tokens {
		if strings.ToUpper(t) == "WITHIN" {
			withinIdx = i
			break
		}
	}

	// Parse events between name and WITHIN
	eventStart := 2
	eventEnd := len(tokens)
	if withinIdx > 0 {
		eventEnd = withinIdx
	}

	for i := eventStart; i < eventEnd; i++ {
		metric := strings.Trim(tokens[i], "(),")
		if metric == "" {
			continue
		}
		optional := false
		if strings.HasSuffix(metric, "?") {
			optional = true
			metric = strings.TrimSuffix(metric, "?")
		}
		pattern.Events = append(pattern.Events, CEPEvent{
			Metric:   metric,
			Optional: optional,
		})
	}

	if withinIdx > 0 && withinIdx+1 < len(tokens) {
		d, err := time.ParseDuration(tokens[withinIdx+1])
		if err != nil {
			return nil, fmt.Errorf("stream dsl v2: invalid WITHIN duration %q: %w", tokens[withinIdx+1], err)
		}
		pattern.Within = d
	}

	stmt.Patterns = []CEPPattern{pattern}
	stmt.Source = patternName
	return stmt, nil
}

func (e *StreamDSLV2Engine) parseJoin(tokens []string, raw string) (*StreamDSLV2Statement, error) {
	// JOIN INNER|LEFT|OUTER|CROSS <left> <right> ON <cond> WITHIN <dur>
	if len(tokens) < 5 {
		return nil, fmt.Errorf("stream dsl v2: incomplete JOIN statement")
	}

	stmt := &StreamDSLV2Statement{
		Type: StmtJoin,
		Raw:  raw,
	}

	if !e.config.EnableJoins {
		return nil, fmt.Errorf("stream dsl v2: joins are disabled")
	}

	join := DSLV2Join{}
	switch strings.ToUpper(tokens[1]) {
	case "INNER":
		join.Type = DSLV2JoinInner
	case "LEFT":
		join.Type = DSLV2JoinLeft
	case "OUTER":
		join.Type = DSLV2JoinOuter
	case "CROSS":
		join.Type = DSLV2JoinCross
	default:
		return nil, fmt.Errorf("stream dsl v2: unknown join type %q", tokens[1])
	}

	join.LeftStream = tokens[2]
	join.RightStream = tokens[3]
	stmt.Source = join.LeftStream

	// Parse ON and WITHIN
	for i := 4; i < len(tokens); i++ {
		upper := strings.ToUpper(tokens[i])
		if upper == "ON" && i+1 < len(tokens) {
			var condParts []string
			i++
			for i < len(tokens) && strings.ToUpper(tokens[i]) != "WITHIN" {
				condParts = append(condParts, tokens[i])
				i++
			}
			join.OnCondition = strings.Join(condParts, " ")
			// After inner loop, i may point at WITHIN; re-check
			if i < len(tokens) && strings.ToUpper(tokens[i]) == "WITHIN" {
				if i+1 < len(tokens) {
					d, err := time.ParseDuration(tokens[i+1])
					if err != nil {
						return nil, fmt.Errorf("stream dsl v2: invalid WITHIN duration: %w", err)
					}
					join.Within = d
					i++
				}
			}
			continue
		}
		if upper == "WITHIN" && i+1 < len(tokens) {
			d, err := time.ParseDuration(tokens[i+1])
			if err != nil {
				return nil, fmt.Errorf("stream dsl v2: invalid WITHIN duration: %w", err)
			}
			join.Within = d
			i++
		}
	}

	stmt.Joins = []DSLV2Join{join}
	return stmt, nil
}

// Validate validates a DSL string without executing it.
func (e *StreamDSLV2Engine) Validate(dsl string) error {
	_, err := e.Parse(dsl)
	return err
}
