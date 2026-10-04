package continuousquery

import (
	"context"
	"sync"
	"time"
)

// ContinuousQueryConfig configures the continuous query engine v2.
type ContinuousQueryConfig struct {
	// Enabled enables continuous queries.
	Enabled bool

	// MaxQueries limits concurrent continuous queries.
	MaxQueries int

	// DefaultWindow is the default window size.
	DefaultWindow time.Duration

	// CheckpointInterval is how often to checkpoint state.
	CheckpointInterval time.Duration

	// StateBackend configures where to store query state.
	StateBackend StateBackendType

	// EnableWatermarks enables event-time watermarks.
	EnableWatermarks bool

	// WatermarkDelay is the allowed late arrival time.
	WatermarkDelay time.Duration

	// EnableExactlyOnce enables exactly-once processing semantics.
	EnableExactlyOnce bool

	// ParallelismHint is the suggested parallelism for queries.
	ParallelismHint int

	// EnableQueryOptimizer enables query optimization.
	EnableQueryOptimizer bool

	// MetricsEnabled enables internal metrics collection.
	MetricsEnabled bool

	// RetryOnFailure enables automatic retry on transient failures.
	RetryOnFailure bool

	// MaxRetries is the maximum number of retries.
	MaxRetries int
}

// StateBackendType identifies the state backend.
type StateBackendType int

const (
	// StateBackendMemory uses in-memory state.
	StateBackendMemory StateBackendType = iota
	// StateBackendRocksDB uses RocksDB for state.
	StateBackendRocksDB
	// StateBackendChronicle uses Chronicle DB for state.
	StateBackendChronicle
)

// DefaultContinuousQueryConfig returns default configuration.
func DefaultContinuousQueryConfig() ContinuousQueryConfig {
	return ContinuousQueryConfig{
		Enabled:              true,
		MaxQueries:           100,
		DefaultWindow:        time.Minute,
		CheckpointInterval:   time.Minute,
		StateBackend:         StateBackendMemory,
		EnableWatermarks:     true,
		WatermarkDelay:       10 * time.Second,
		EnableExactlyOnce:    false,
		ParallelismHint:      4,
		EnableQueryOptimizer: true,
		MetricsEnabled:       true,
		RetryOnFailure:       true,
		MaxRetries:           3,
	}
}

// Point represents a time-series data point.
type Point struct {
	Metric    string
	Timestamp int64
	Value     float64
	Tags      map[string]string
}

// ContinuousQueryEngine provides Flink-style continuous query processing.
type ContinuousQueryEngine struct {
	pw     PointWriter
	bus    StreamBus
	config ContinuousQueryConfig

	// Query registry
	queries     map[string]*ContinuousQueryV2
	queryMu     sync.RWMutex
	lastQueryID int64 // guarded by queryMu; never reset when a query is deleted

	// Materialized views
	views  map[string]*MaterializedView
	viewMu sync.RWMutex

	// State management
	stateManager *QueryStateManager

	// Watermark tracking
	watermarkTracker *WatermarkTracker

	// Query optimizer
	optimizer *CQQueryOptimizer

	// Metrics
	metrics *CQMetrics

	// Lifecycle
	ctx    context.Context
	cancel context.CancelFunc
	wg     sync.WaitGroup
}

// ContinuousQueryV2 represents an enhanced continuous query definition.
type ContinuousQueryV2 struct {
	ID             string     `json:"id"`
	Name           string     `json:"name"`
	SQL            string     `json:"sql"`
	Plan           *QueryPlan `json:"plan"`
	State          CQState    `json:"state"`
	Config         CQConfig   `json:"config"`
	Created        time.Time  `json:"created"`
	LastCheckpoint time.Time  `json:"last_checkpoint"`
	Stats          CQStats    `json:"stats"`

	// Runtime
	ctx       context.Context
	cancel    context.CancelFunc
	operators []CQOperator
	mu        sync.Mutex
}

// CQState represents continuous query state.
type CQState int

const (
	CQStateCreated CQState = iota
	CQStateRunning
	CQStatePaused
	CQStateStopped
	CQStateFailed
	CQStateCheckpointing
)

// CQConfig contains per-query configuration.
type CQConfig struct {
	// Parallelism for this query.
	Parallelism int `json:"parallelism"`

	// OutputMode specifies how results are emitted.
	OutputMode OutputMode `json:"output_mode"`

	// Watermark configuration.
	WatermarkColumn string        `json:"watermark_column"`
	WatermarkDelay  time.Duration `json:"watermark_delay"`

	// Trigger configuration.
	Trigger TriggerConfig `json:"trigger"`

	// Sink configuration.
	Sinks []SinkConfig `json:"sinks"`
}

// OutputMode specifies result output behavior.
type OutputMode int

const (
	// OutputModeAppend only outputs new rows.
	OutputModeAppend OutputMode = iota
	// OutputModeComplete outputs all rows on each trigger.
	OutputModeComplete
	// OutputModeUpdate outputs only changed rows.
	OutputModeUpdate
)

// TriggerConfig configures when to emit results.
type TriggerConfig struct {
	Type     TriggerType   `json:"type"`
	Interval time.Duration `json:"interval,omitempty"`
}

// TriggerType identifies trigger types.
type TriggerType int

const (
	// TriggerTypeProcessingTime triggers based on processing time.
	TriggerTypeProcessingTime TriggerType = iota
	// TriggerTypeEventTime triggers based on watermarks.
	TriggerTypeEventTime
	// TriggerTypeOnce triggers once when data is available.
	TriggerTypeOnce
	// TriggerTypeContinuous triggers continuously.
	TriggerTypeContinuous
)

// SinkConfig configures query output sink.
type SinkConfig struct {
	Type   string         `json:"type"`
	Name   string         `json:"name"`
	Config map[string]any `json:"config"`
}

// CQStats contains query statistics.
type CQStats struct {
	InputRecords    int64         `json:"input_records"`
	OutputRecords   int64         `json:"output_records"`
	ProcessingTime  time.Duration `json:"processing_time"`
	Watermark       int64         `json:"watermark"`
	LastTriggerTime time.Time     `json:"last_trigger_time"`
	CheckpointCount int64         `json:"checkpoint_count"`
	FailureCount    int64         `json:"failure_count"`
	RecoveryCount   int64         `json:"recovery_count"`
}

// QueryPlan represents an optimized query execution plan.
type QueryPlan struct {
	Root       *PlanNode `json:"root"`
	Sources    []string  `json:"sources"`
	Sinks      []string  `json:"sinks"`
	Partitions int       `json:"partitions"`
	Cost       float64   `json:"cost"`
}

// PlanNode represents a node in the query plan.
type PlanNode struct {
	ID         string         `json:"id"`
	Type       PlanNodeType   `json:"type"`
	Properties map[string]any `json:"properties"`
	Children   []*PlanNode    `json:"children,omitempty"`
}

// PlanNodeType identifies plan node types.
type PlanNodeType int

const (
	PlanNodeScan PlanNodeType = iota
	PlanNodeFilter
	PlanNodeProject
	PlanNodeAggregate
	PlanNodeJoin
	PlanNodeWindow
	PlanNodeSink
	PlanNodeExchange
)

// CQOperator represents a query operator.
type CQOperator interface {
	Open(ctx context.Context) error
	Process(record *Record) ([]*Record, error)
	Close() error
	GetState() ([]byte, error)
	RestoreState([]byte) error
}

// Record represents a data record flowing through operators.
type Record struct {
	Key       string            `json:"key"`
	Value     map[string]any    `json:"value"`
	Timestamp int64             `json:"timestamp"`
	Watermark int64             `json:"watermark"`
	Metadata  map[string]string `json:"metadata"`
}

// MaterializedView represents a materialized continuous query result.
type MaterializedView struct {
	Name        string        `json:"name"`
	Query       string        `json:"query"`
	Schema      []CQColumnDef `json:"schema"`
	RefreshMode RefreshMode   `json:"refresh_mode"`
	Data        *ViewData     `json:"data"`
	QueryID     string        `json:"query_id"`
	Created     time.Time     `json:"created"`
	Updated     time.Time     `json:"updated"`
	mu          sync.RWMutex
}

// CQColumnDef defines a column in the view schema.
type CQColumnDef struct {
	Name string `json:"name"`
	Type string `json:"type"`
}

// RefreshMode specifies how the view is refreshed.
type RefreshMode int

const (
	// RefreshModeIncremental updates incrementally.
	RefreshModeIncremental RefreshMode = iota
	// RefreshModeFull rebuilds the entire view.
	RefreshModeFull
)

// ViewData holds materialized view data.
type ViewData struct {
	Rows      []map[string]any `json:"rows"`
	RowCount  int              `json:"row_count"`
	UpdatedAt time.Time        `json:"updated_at"`
}

// QueryStateManager manages state for continuous queries.
type QueryStateManager struct {
	backend StateBackendType
	states  map[string][]byte
	mu      sync.RWMutex
}

// WatermarkTracker tracks watermarks for event-time processing.
type WatermarkTracker struct {
	watermarks map[string]int64
	mu         sync.RWMutex
}

// CQQueryOptimizer optimizes continuous query plans.
type CQQueryOptimizer struct {
	rules []OptimizationRule
}

// OptimizationRule represents a query optimization rule.
type OptimizationRule interface {
	Name() string
	Apply(*QueryPlan) (*QueryPlan, bool)
}

// CQMetrics tracks continuous query metrics.
type CQMetrics struct {
	QueriesCreated    int64
	QueriesRunning    int64
	QueriesFailed     int64
	RecordsProcessed  int64
	RecordsEmitted    int64
	CheckpointsTotal  int64
	CheckpointsFailed int64
	ProcessingLatency time.Duration
	mu                sync.RWMutex
}

// NewContinuousQueryEngine creates a new continuous query engine.
func NewContinuousQueryEngine(pw PointWriter, bus StreamBus, config ContinuousQueryConfig) *ContinuousQueryEngine {
	ctx, cancel := context.WithCancel(context.Background())

	engine := &ContinuousQueryEngine{
		pw:      pw,
		bus:     bus,
		config:  config,
		queries: make(map[string]*ContinuousQueryV2),
		views:   make(map[string]*MaterializedView),
		stateManager: &QueryStateManager{
			backend: config.StateBackend,
			states:  make(map[string][]byte),
		},
		watermarkTracker: &WatermarkTracker{
			watermarks: make(map[string]int64),
		},
		optimizer: newQueryOptimizer(),
		metrics:   &CQMetrics{},
		ctx:       ctx,
		cancel:    cancel,
	}

	return engine
}

// newQueryOptimizer creates a query optimizer with default rules.
func newQueryOptimizer() *CQQueryOptimizer {
	return &CQQueryOptimizer{
		rules: []OptimizationRule{
			&PredicatePushdownRule{},
			&ProjectionPushdownRule{},
			&JoinReorderRule{},
			&AggregationOptimizationRule{},
		},
	}
}

// QueryCheckpoint represents a query checkpoint.
type QueryCheckpoint struct {
	QueryID     string   `json:"query_id"`
	Timestamp   int64    `json:"timestamp"`
	Watermark   int64    `json:"watermark"`
	States      [][]byte `json:"states"`
	InputOffset int64    `json:"input_offset"`
}

// PredicatePushdownRule pushes filters closer to sources.
type PredicatePushdownRule struct{}

// ProjectionPushdownRule pushes projections closer to sources.
type ProjectionPushdownRule struct{}

// JoinReorderRule reorders joins for efficiency.
type JoinReorderRule struct{}

// AggregationOptimizationRule optimizes aggregations.
type AggregationOptimizationRule struct{}

// ScanOperator reads from a source.
type ScanOperator struct {
	id    string
	state []byte
}

// FilterOperator filters records.
type FilterOperator struct {
	id        string
	predicate func(*Record) bool
	state     []byte
}

// ProjectOperator projects fields.
type ProjectOperator struct {
	id     string
	fields []string
	state  []byte
}

// AggregateOperator performs aggregations.
type AggregateOperator struct {
	id         string
	aggregates map[string]float64
	counts     map[string]int64
	mu         sync.Mutex
	state      []byte
}

// WindowOperator manages windowed state.
type WindowOperator struct {
	id      string
	windows map[string]*WindowBuffer
	mu      sync.Mutex
	state   []byte
}

// WindowBuffer holds window data.
type WindowBuffer struct {
	Start   int64
	End     int64
	Records []*Record
}

// SinkOperator outputs records.
type SinkOperator struct {
	id    string
	state []byte
}

// PassthroughOperator passes records unchanged.
type PassthroughOperator struct {
	id    string
	state []byte
}

// CQEngineStats contains engine statistics.
type CQEngineStats struct {
	QueriesTotal      int   `json:"queries_total"`
	QueriesCreated    int64 `json:"queries_created"`
	QueriesRunning    int64 `json:"queries_running"`
	QueriesFailed     int64 `json:"queries_failed"`
	ViewsTotal        int   `json:"views_total"`
	RecordsProcessed  int64 `json:"records_processed"`
	RecordsEmitted    int64 `json:"records_emitted"`
	CheckpointsTotal  int64 `json:"checkpoints_total"`
	CheckpointsFailed int64 `json:"checkpoints_failed"`
}

// StreamJoinConfig configures a stream-stream join.
type StreamJoinConfig struct {
	LeftStream  string
	RightStream string
	JoinType    StreamJoinType
	JoinKey     string
	Window      time.Duration
}

// StreamJoinType identifies join types.
type StreamJoinType int

const (
	StreamJoinInner StreamJoinType = iota
	StreamJoinLeft
	StreamJoinOuter
)

// PartitionedQuery represents a query partitioned across multiple workers.
type PartitionedQuery struct {
	*ContinuousQueryV2
	Partitions []*QueryPartition
}

// QueryPartition represents a query partition.
type QueryPartition struct {
	ID        int
	KeyRange  KeyRange
	Operators []CQOperator
	State     []byte
}

// KeyRange represents a key range for partitioning.
type KeyRange struct {
	Start uint64
	End   uint64
}
