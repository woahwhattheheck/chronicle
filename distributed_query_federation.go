package chronicle

import (
	"fmt"
	"sort"
	"strings"
	"sync"
	"time"
)

// ---------------------------------------------------------------------------
// Configuration
// ---------------------------------------------------------------------------

// RetryBackoffConfig configures exponential backoff for retries.
type RetryBackoffConfig struct {
	InitialInterval time.Duration `json:"initial_interval"`
	MaxInterval     time.Duration `json:"max_interval"`
	Multiplier      float64       `json:"multiplier"`
	MaxRetries      int           `json:"max_retries"`
}

// DistributedFederationConfig controls the distributed query federation engine.
type DistributedFederationConfig struct {
	Enabled              bool          `json:"enabled"`
	MaxFanOut            int           `json:"max_fan_out"`
	PartialResultTimeout time.Duration `json:"partial_result_timeout"`

	EnablePredicatePushDown bool `json:"enable_predicate_push_down"`
	EnableLocalityRouting   bool `json:"enable_locality_routing"`

	CircuitBreakerThreshold   int           `json:"circuit_breaker_threshold"`
	CircuitBreakerTimeout     time.Duration `json:"circuit_breaker_timeout"`
	CircuitBreakerHalfOpenMax int           `json:"circuit_breaker_half_open_max"`

	MaxPartialResultSizeMB int  `json:"max_partial_result_size_mb"`
	EnablePartialResults   bool `json:"enable_partial_results"`

	RetryPolicy         RetryBackoffConfig `json:"retry_policy"`
	HealthCheckInterval time.Duration      `json:"health_check_interval"`
}

// DefaultDistributedFederationConfig returns a production-ready default configuration.
func DefaultDistributedFederationConfig() DistributedFederationConfig {
	return DistributedFederationConfig{
		Enabled:                   true,
		MaxFanOut:                 10,
		PartialResultTimeout:      5 * time.Second,
		EnablePredicatePushDown:   true,
		EnableLocalityRouting:     true,
		CircuitBreakerThreshold:   5,
		CircuitBreakerTimeout:     30 * time.Second,
		CircuitBreakerHalfOpenMax: 3,
		MaxPartialResultSizeMB:    64,
		EnablePartialResults:      true,
		RetryPolicy: RetryBackoffConfig{
			InitialInterval: 100 * time.Millisecond,
			MaxInterval:     5 * time.Second,
			Multiplier:      2.0,
			MaxRetries:      3,
		},
		HealthCheckInterval: 10 * time.Second,
	}
}

// ---------------------------------------------------------------------------
// Predicate Push-Down
// ---------------------------------------------------------------------------

// FedPredicateKind classifies how a predicate can be pushed.
type FedPredicateKind int

const (
	FedPredicateTimeRange FedPredicateKind = iota
	FedPredicateTagEquality
	FedPredicateTagFilter
	FedPredicateAggregation
	FedPredicateLimit
)

// PushablePredicate describes a single predicate that can be pushed to a remote node.
type PushablePredicate struct {
	Kind  FedPredicateKind `json:"kind"`
	Field string           `json:"field"`
	Value string           `json:"value,omitempty"`
	Safe  bool             `json:"safe"` // always safe to push without changing semantics
}

// PredicateAnalysis is the result of analysing a query for push-down.
type PredicateAnalysis struct {
	Pushable []PushablePredicate `json:"pushable"`
	Local    []PushablePredicate `json:"local"`
}

// PredicatePushDown analyses queries and creates optimised remote queries.
type PredicatePushDown struct {
	enabled bool
}

// NewPredicatePushDown returns a new push-down analyser.
func NewPredicatePushDown(enabled bool) *PredicatePushDown {
	return &PredicatePushDown{enabled: enabled}
}

// AnalyzePredicates splits a query's predicates into pushable and local sets.
func (pp *PredicatePushDown) AnalyzePredicates(q *Query) PredicateAnalysis {
	var analysis PredicateAnalysis
	if !pp.enabled || q == nil {
		return analysis
	}

	// Time range — always safe to push.
	if q.Start != 0 || q.End != 0 {
		analysis.Pushable = append(analysis.Pushable, PushablePredicate{
			Kind:  FedPredicateTimeRange,
			Field: "time",
			Value: fmt.Sprintf("%d-%d", q.Start, q.End),
			Safe:  true,
		})
	}

	// Tag equality predicates — always safe.
	for k, v := range q.Tags {
		analysis.Pushable = append(analysis.Pushable, PushablePredicate{
			Kind:  FedPredicateTagEquality,
			Field: k,
			Value: v,
			Safe:  true,
		})
	}

	// TagFilters: equality/in are safe; regex stays local.
	for _, tf := range q.TagFilters {
		p := PushablePredicate{
			Kind:  FedPredicateTagFilter,
			Field: tf.Key,
		}
		switch tf.Op {
		case TagOpEq, TagOpNotEq, TagOpIn, TagOpNotIn:
			p.Safe = true
			p.Value = strings.Join(tf.Values, ",")
			analysis.Pushable = append(analysis.Pushable, p)
		default:
			p.Safe = false
			analysis.Local = append(analysis.Local, p)
		}
	}

	// Aggregation — partial aggregation is pushable for distributive funcs.
	if q.Aggregation != nil {
		p := PushablePredicate{
			Kind:  FedPredicateAggregation,
			Field: fmt.Sprintf("agg_%d", q.Aggregation.Function),
		}
		switch q.Aggregation.Function {
		case AggSum, AggCount, AggMin, AggMax:
			p.Safe = true
			analysis.Pushable = append(analysis.Pushable, p)
		case AggMean:
			// Mean needs sum+count locally, push partial.
			p.Safe = false
			analysis.Pushable = append(analysis.Pushable, p)
		default:
			p.Safe = false
			analysis.Local = append(analysis.Local, p)
		}
	}

	// Limit stays local (applied after merge).
	if q.Limit > 0 {
		analysis.Local = append(analysis.Local, PushablePredicate{
			Kind:  FedPredicateLimit,
			Field: "limit",
			Value: fmt.Sprintf("%d", q.Limit),
			Safe:  false,
		})
	}

	return analysis
}

// CreateRemoteQuery builds a query suitable for a remote node by applying pushable predicates.
func (pp *PredicatePushDown) CreateRemoteQuery(q *Query, pushable []PushablePredicate) *Query {
	if q == nil {
		return nil
	}
	remote := &Query{
		Metric: q.Metric,
		Start:  q.Start,
		End:    q.End,
	}
	type filterKey struct{ key, value string }
	availableFilters := make(map[filterKey][]TagFilter)
	for _, tf := range q.TagFilters {
		switch tf.Op {
		case TagOpEq, TagOpNotEq, TagOpIn, TagOpNotIn:
			key := filterKey{tf.Key, strings.Join(tf.Values, ",")}
			availableFilters[key] = append(availableFilters[key], tf)
		}
	}
	for _, p := range pushable {
		switch p.Kind {
		case FedPredicateTagEquality:
			if remote.Tags == nil {
				remote.Tags = make(map[string]string)
			}
			remote.Tags[p.Field] = p.Value
		case FedPredicateTagFilter:
			if !p.Safe {
				continue
			}
			// Analysis emits one predicate per eligible filter in source order.
			// Consume each matching occurrence once so same-key exclusions are
			// not replaced by the first inclusion or a local-only regex.
			key := filterKey{p.Field, p.Value}
			if filters := availableFilters[key]; len(filters) > 0 {
				remote.TagFilters = append(remote.TagFilters, filters[0])
				availableFilters[key] = filters[1:]
			}
		case FedPredicateAggregation:
			if q.Aggregation != nil {
				agg := *q.Aggregation
				remote.Aggregation = &agg
				remote.GroupBy = q.GroupBy
			}
		}
	}
	return remote
}

// ---------------------------------------------------------------------------
// Data Locality Router
// ---------------------------------------------------------------------------

// FederationNodeInfo describes a node's capabilities.
type FederationNodeInfo struct {
	NodeID     string            `json:"node_id"`
	Address    string            `json:"address"`
	Metrics    []string          `json:"metrics"`
	TimeRanges []TimeRange       `json:"time_ranges"`
	Load       float64           `json:"load"` // 0.0–1.0
	Healthy    bool              `json:"healthy"`
	LastSeen   time.Time         `json:"last_seen"`
	Meta       map[string]string `json:"meta,omitempty"`
}

// NodeScore pairs a node with a routing score.
type NodeScore struct {
	Node  FederationNodeInfo `json:"node"`
	Score float64            `json:"score"`
}

// DataLocalityRouter routes queries to optimal nodes based on data locality.
type DataLocalityRouter struct {
	mu    sync.RWMutex
	nodes map[string]*FederationNodeInfo
}

// NewDataLocalityRouter creates a new router.
func NewDataLocalityRouter() *DataLocalityRouter {
	return &DataLocalityRouter{nodes: make(map[string]*FederationNodeInfo)}
}

// RegisterNode adds or updates a node's capabilities.
func (r *DataLocalityRouter) RegisterNode(nodeID, address string, metrics []string, timeRanges []TimeRange) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.nodes[nodeID] = &FederationNodeInfo{
		NodeID:     nodeID,
		Address:    address,
		Metrics:    metrics,
		TimeRanges: timeRanges,
		Healthy:    true,
		LastSeen:   time.Now(),
		Meta:       make(map[string]string),
	}
}

// RemoveNode deregisters a node.
func (r *DataLocalityRouter) RemoveNode(nodeID string) {
	r.mu.Lock()
	defer r.mu.Unlock()
	delete(r.nodes, nodeID)
}

// UpdateHealth sets a node's health status and load.
func (r *DataLocalityRouter) UpdateHealth(nodeID string, healthy bool, load float64) {
	r.mu.Lock()
	defer r.mu.Unlock()
	if n, ok := r.nodes[nodeID]; ok {
		n.Healthy = healthy
		n.Load = load
		n.LastSeen = time.Now()
	}
}

// GetNode returns info for a single node.
func (r *DataLocalityRouter) GetNode(nodeID string) (FederationNodeInfo, bool) {
	r.mu.RLock()
	defer r.mu.RUnlock()
	n, ok := r.nodes[nodeID]
	if !ok {
		return FederationNodeInfo{}, false
	}
	return *n, true
}

// AllNodes returns all registered nodes.
func (r *DataLocalityRouter) AllNodes() []FederationNodeInfo {
	r.mu.RLock()
	defer r.mu.RUnlock()
	out := make([]FederationNodeInfo, 0, len(r.nodes))
	for _, n := range r.nodes {
		out = append(out, *n)
	}
	return out
}

// RouteQuery returns a ranked list of nodes best suited for the query.
func (r *DataLocalityRouter) RouteQuery(q *Query) []NodeScore {
	r.mu.RLock()
	defer r.mu.RUnlock()

	var scores []NodeScore
	for _, node := range r.nodes {
		if !node.Healthy {
			continue
		}
		score := r.scoreNode(node, q)
		if score > 0 {
			scores = append(scores, NodeScore{Node: *node, Score: score})
		}
	}
	sort.Slice(scores, func(i, j int) bool { return scores[i].Score > scores[j].Score })
	return scores
}

func (r *DataLocalityRouter) scoreNode(node *FederationNodeInfo, q *Query) float64 {
	var score float64

	// Metric presence: highest weight.
	hasMetric := false
	for _, m := range node.Metrics {
		if m == q.Metric {
			hasMetric = true
			break
		}
	}
	if !hasMetric && len(node.Metrics) > 0 {
		return 0 // node doesn't have data
	}
	if hasMetric {
		score += 50
	}

	// Time range overlap.
	if q.Start != 0 || q.End != 0 {
		for _, tr := range node.TimeRanges {
			overlap := r.timeOverlap(q.Start, q.End, tr.Start, tr.End)
			score += overlap * 30 // up to 30 points
		}
	} else {
		score += 15 // unbounded query: moderate score
	}

	// Load penalty: prefer less-loaded nodes.
	score += (1.0 - node.Load) * 20

	return score
}

func (r *DataLocalityRouter) timeOverlap(qStart, qEnd, nStart, nEnd int64) float64 {
	if qEnd != 0 && qEnd < nStart {
		return 0
	}
	if qStart != 0 && nEnd != 0 && qStart > nEnd {
		return 0
	}
	// Compute overlap fraction relative to query range.
	qS, qE := qStart, qEnd
	if qS == 0 {
		qS = nStart
	}
	if qE == 0 {
		qE = nEnd
	}
	if qE == 0 || qS == 0 {
		return 0.5 // can't determine range
	}
	overlapStart := qS
	if nStart > overlapStart {
		overlapStart = nStart
	}
	overlapEnd := qE
	if nEnd != 0 && nEnd < overlapEnd {
		overlapEnd = nEnd
	}
	if overlapEnd <= overlapStart {
		return 0
	}
	qRange := float64(qE - qS)
	if qRange == 0 {
		return 1.0
	}
	return float64(overlapEnd-overlapStart) / qRange
}

// ---------------------------------------------------------------------------
// Circuit Breaker
// ---------------------------------------------------------------------------

// FedCircuitState represents the state of a circuit breaker.
type FedCircuitState int

const (
	FedCircuitClosed   FedCircuitState = iota // normal operation
	FedCircuitOpen                            // failing, reject fast
	FedCircuitHalfOpen                        // testing recovery
)

// String returns a human-readable circuit state.
func (s FedCircuitState) String() string {
	switch s {
	case FedCircuitClosed:
		return "closed"
	case FedCircuitOpen:
		return "open"
	case FedCircuitHalfOpen:
		return "half_open"
	default:
		return "unknown"
	}
}

// CircuitBreaker implements a per-node circuit breaker with three states.
type FedCircuitBreaker struct {
	mu               sync.Mutex
	state            FedCircuitState
	failures         int
	successes        int
	threshold        int
	timeout          time.Duration
	halfOpenMax      int
	lastFailure      time.Time
	halfOpenAttempts int
}

// NewFedCircuitBreaker creates a circuit breaker with the given thresholds.
func NewFedCircuitBreaker(threshold int, timeout time.Duration, halfOpenMax int) *FedCircuitBreaker {
	if threshold <= 0 {
		threshold = 5
	}
	if timeout <= 0 {
		timeout = 30 * time.Second
	}
	if halfOpenMax <= 0 {
		halfOpenMax = 3
	}
	return &FedCircuitBreaker{
		state:       FedCircuitClosed,
		threshold:   threshold,
		timeout:     timeout,
		halfOpenMax: halfOpenMax,
	}
}

// State returns the current circuit breaker state.
func (cb *FedCircuitBreaker) State() FedCircuitState {
	cb.mu.Lock()
	defer cb.mu.Unlock()
	cb.checkStateTransition()
	return cb.state
}

// Execute wraps fn with circuit breaker logic. Returns an error immediately if
// the circuit is open.
func (cb *FedCircuitBreaker) Execute(fn func() error) error {
	cb.mu.Lock()
	cb.checkStateTransition()

	switch cb.state {
	case FedCircuitOpen:
		cb.mu.Unlock()
		return fmt.Errorf("circuit breaker open")
	case FedCircuitHalfOpen:
		if cb.halfOpenAttempts >= cb.halfOpenMax {
			cb.mu.Unlock()
			return fmt.Errorf("circuit breaker half-open limit reached")
		}
		cb.halfOpenAttempts++
	}
	cb.mu.Unlock()

	err := fn()

	if err != nil {
		cb.RecordFailure()
	} else {
		cb.RecordSuccess()
	}
	return err
}

// RecordSuccess records a successful call.
func (cb *FedCircuitBreaker) RecordSuccess() {
	cb.mu.Lock()
	defer cb.mu.Unlock()
	cb.successes++
	if cb.state == FedCircuitHalfOpen {
		// Recovered.
		cb.state = FedCircuitClosed
		cb.failures = 0
		cb.successes = 0
		cb.halfOpenAttempts = 0
	}
}

// RecordFailure records a failed call.
func (cb *FedCircuitBreaker) RecordFailure() {
	cb.mu.Lock()
	defer cb.mu.Unlock()
	cb.failures++
	cb.lastFailure = time.Now()

	switch cb.state {
	case FedCircuitClosed:
		if cb.failures >= cb.threshold {
			cb.state = FedCircuitOpen
		}
	case FedCircuitHalfOpen:
		// Any failure in half-open reopens.
		cb.state = FedCircuitOpen
		cb.halfOpenAttempts = 0
	}
}

// Reset forces the circuit breaker back to closed.
func (cb *FedCircuitBreaker) Reset() {
	cb.mu.Lock()
	defer cb.mu.Unlock()
	cb.state = FedCircuitClosed
	cb.failures = 0
	cb.successes = 0
	cb.halfOpenAttempts = 0
}

// checkStateTransition must be called with mu held.
func (cb *FedCircuitBreaker) checkStateTransition() {
	if cb.state == FedCircuitOpen && time.Since(cb.lastFailure) >= cb.timeout {
		cb.state = FedCircuitHalfOpen
		cb.halfOpenAttempts = 0
	}
}
