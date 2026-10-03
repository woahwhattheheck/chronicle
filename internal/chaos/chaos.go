package chaos

import (
	"context"
	"fmt"
	"math/rand"
	"strings"
	"sync"
	"sync/atomic"
	"time"
)

// ChaosEventType represents the type of a chaos engineering event.
type ChaosEventType int

const (
	ChaosEventFaultInjected ChaosEventType = iota
	ChaosEventFaultRemoved
	ChaosEventInvariantCheck
	ChaosEventStepExecuted
)

// FaultType represents the category of fault to inject.
type FaultType int

const (
	FaultNetworkPartition FaultType = iota
	FaultNetworkDelay
	FaultDiskSlow
	FaultDiskFail
	FaultMemoryPressure
	FaultClockSkew
	FaultProcessPause
	FaultCorruptData
)

// ChaosConfig controls the behavior of the chaos engineering framework.
type ChaosConfig struct {
	Enabled             bool  `json:"enabled"`
	Seed                int64 `json:"seed"`
	MaxConcurrentFaults int   `json:"max_concurrent_faults"`
	SafetyChecks        bool  `json:"safety_checks"`
	LogEvents           bool  `json:"log_events"`
}

// DefaultChaosConfig returns a ChaosConfig with sensible defaults.
func DefaultChaosConfig() ChaosConfig {
	return ChaosConfig{
		Enabled:             true,
		Seed:                time.Now().UnixNano(),
		MaxConcurrentFaults: 5,
		SafetyChecks:        true,
		LogEvents:           true,
	}
}

// FaultConfig describes a fault to be injected.
type FaultConfig struct {
	Type        FaultType      `json:"type"`
	Duration    time.Duration  `json:"duration"`
	Probability float64        `json:"probability"`
	Target      string         `json:"target"`
	Parameters  map[string]any `json:"parameters"`
}

// ActiveFault represents a currently active fault in the system.
type ActiveFault struct {
	// Keep the atomically accessed counter first for 64-bit alignment on ARMv7.
	TriggerCount int64        `json:"trigger_count"`
	ID           string       `json:"id"`
	Config       *FaultConfig `json:"config"`
	ActivatedAt  time.Time    `json:"activated_at"`
	DeactivateAt time.Time    `json:"deactivate_at"`
}

// FaultInterceptor is a callback invoked when a fault is triggered.
type FaultInterceptor func(fault *ActiveFault) error

// ChaosEvent records something that happened during chaos testing.
type ChaosEvent struct {
	Type        ChaosEventType `json:"type"`
	FaultID     string         `json:"fault_id"`
	Timestamp   time.Time      `json:"timestamp"`
	Description string         `json:"description"`
	Details     map[string]any `json:"details"`
}

// ---------------------------------------------------------------------------
// FaultInjector
// ---------------------------------------------------------------------------

// FaultInjector is the core chaos injection engine.
type FaultInjector struct {
	config       ChaosConfig
	activeFaults map[string]*ActiveFault
	interceptors map[FaultType][]FaultInterceptor
	mu           sync.RWMutex
	rng          *rand.Rand
	events       []ChaosEvent
}

// NewFaultInjector creates a new FaultInjector with the given configuration.
func NewFaultInjector(config ChaosConfig) *FaultInjector {
	if config.MaxConcurrentFaults <= 0 {
		config.MaxConcurrentFaults = 5
	}
	return &FaultInjector{
		config:       config,
		activeFaults: make(map[string]*ActiveFault),
		interceptors: make(map[FaultType][]FaultInterceptor),
		rng:          rand.New(rand.NewSource(config.Seed)),
		events:       make([]ChaosEvent, 0),
	}
}

// InjectFault activates a fault and returns the ActiveFault handle.
func (fi *FaultInjector) InjectFault(fault *FaultConfig) (*ActiveFault, error) {
	fi.mu.Lock()
	defer fi.mu.Unlock()

	if !fi.config.Enabled {
		return nil, fmt.Errorf("chaos: injector is disabled")
	}
	if fi.config.SafetyChecks && len(fi.activeFaults) >= fi.config.MaxConcurrentFaults {
		return nil, fmt.Errorf("chaos: max concurrent faults (%d) reached", fi.config.MaxConcurrentFaults)
	}

	now := time.Now()
	af := &ActiveFault{
		ID:           fmt.Sprintf("fault-%d-%d", fault.Type, now.UnixNano()),
		Config:       fault,
		ActivatedAt:  now,
		DeactivateAt: now.Add(fault.Duration),
	}
	fi.activeFaults[af.ID] = af

	if fi.config.LogEvents {
		fi.events = append(fi.events, ChaosEvent{
			Type:        ChaosEventFaultInjected,
			FaultID:     af.ID,
			Timestamp:   now,
			Description: fmt.Sprintf("Injected fault type %d on %s", fault.Type, fault.Target),
			Details:     fault.Parameters,
		})
	}

	// Run interceptors outside the lock would be ideal but we keep it simple.
	for _, fn := range fi.interceptors[fault.Type] {
		if err := fn(af); err != nil {
			return af, fmt.Errorf("chaos: interceptor error: %w", err)
		}
	}

	return af, nil
}

// RemoveFault deactivates a fault by its ID.
func (fi *FaultInjector) RemoveFault(faultID string) error {
	fi.mu.Lock()
	defer fi.mu.Unlock()

	if _, ok := fi.activeFaults[faultID]; !ok {
		return fmt.Errorf("chaos: fault %s not found", faultID)
	}
	delete(fi.activeFaults, faultID)

	if fi.config.LogEvents {
		fi.events = append(fi.events, ChaosEvent{
			Type:        ChaosEventFaultRemoved,
			FaultID:     faultID,
			Timestamp:   time.Now(),
			Description: fmt.Sprintf("Removed fault %s", faultID),
		})
	}
	return nil
}

// RemoveAll clears every active fault.
func (fi *FaultInjector) RemoveAll() {
	fi.mu.Lock()
	defer fi.mu.Unlock()

	for id := range fi.activeFaults {
		if fi.config.LogEvents {
			fi.events = append(fi.events, ChaosEvent{
				Type:        ChaosEventFaultRemoved,
				FaultID:     id,
				Timestamp:   time.Now(),
				Description: fmt.Sprintf("Removed fault %s", id),
			})
		}
	}
	fi.activeFaults = make(map[string]*ActiveFault)
}

// ActiveFaults returns a snapshot of all currently active faults.
func (fi *FaultInjector) ActiveFaults() []*ActiveFault {
	fi.mu.RLock()
	defer fi.mu.RUnlock()

	faults := make([]*ActiveFault, 0, len(fi.activeFaults))
	for _, af := range fi.activeFaults {
		faults = append(faults, af)
	}
	return faults
}

// ShouldFail performs a probabilistic check across active faults of the given type.
func (fi *FaultInjector) ShouldFail(faultType FaultType) bool {
	fi.mu.Lock()
	defer fi.mu.Unlock()

	for _, af := range fi.activeFaults {
		if af.Config.Type == faultType {
			if fi.rng.Float64() < af.Config.Probability {
				atomic.AddInt64(&af.TriggerCount, 1)
				return true
			}
		}
	}
	return false
}

// RegisterInterceptor registers a callback for a specific fault type.
func (fi *FaultInjector) RegisterInterceptor(faultType FaultType, interceptor FaultInterceptor) {
	fi.mu.Lock()
	defer fi.mu.Unlock()
	fi.interceptors[faultType] = append(fi.interceptors[faultType], interceptor)
}

// Events returns a copy of all recorded chaos events.
func (fi *FaultInjector) Events() []ChaosEvent {
	fi.mu.RLock()
	defer fi.mu.RUnlock()

	out := make([]ChaosEvent, len(fi.events))
	copy(out, fi.events)
	return out
}

// ---------------------------------------------------------------------------
// NetworkFaultSimulator
// ---------------------------------------------------------------------------

// NetworkFaultSimulator simulates network partitions and delays.
type NetworkFaultSimulator struct {
	injector   *FaultInjector
	partitions map[string]map[string]bool
	delays     map[string]time.Duration
	mu         sync.RWMutex
}

// NewNetworkFaultSimulator creates a NetworkFaultSimulator tied to an injector.
func NewNetworkFaultSimulator(injector *FaultInjector) *NetworkFaultSimulator {
	return &NetworkFaultSimulator{
		injector:   injector,
		partitions: make(map[string]map[string]bool),
		delays:     make(map[string]time.Duration),
	}
}

func partitionKey(a, b string) (string, string) {
	if a > b {
		return b, a
	}
	return a, b
}

// PartitionNodes creates a bidirectional network partition between two nodes.
func (ns *NetworkFaultSimulator) PartitionNodes(nodeA, nodeB string) error {
	ns.mu.Lock()
	defer ns.mu.Unlock()

	a, b := partitionKey(nodeA, nodeB)
	if ns.partitions[a] == nil {
		ns.partitions[a] = make(map[string]bool)
	}
	ns.partitions[a][b] = true

	_, err := ns.injector.InjectFault(&FaultConfig{
		Type:        FaultNetworkPartition,
		Probability: 1.0,
		Target:      fmt.Sprintf("%s<->%s", a, b),
		Parameters:  map[string]any{"nodeA": a, "nodeB": b},
	})
	return err
}

// HealPartition removes the partition between two nodes.
func (ns *NetworkFaultSimulator) HealPartition(nodeA, nodeB string) {
	ns.mu.Lock()
	defer ns.mu.Unlock()

	a, b := partitionKey(nodeA, nodeB)
	if ns.partitions[a] != nil {
		delete(ns.partitions[a], b)
	}
}

// AddDelay introduces artificial latency for a target.
func (ns *NetworkFaultSimulator) AddDelay(target string, delay time.Duration) {
	ns.mu.Lock()
	defer ns.mu.Unlock()
	ns.delays[target] = delay
}

// RemoveDelay removes artificial latency for a target.
func (ns *NetworkFaultSimulator) RemoveDelay(target string) {
	ns.mu.Lock()
	defer ns.mu.Unlock()
	delete(ns.delays, target)
}

// IsPartitioned checks whether two nodes are partitioned.
func (ns *NetworkFaultSimulator) IsPartitioned(nodeA, nodeB string) bool {
	ns.mu.RLock()
	defer ns.mu.RUnlock()

	a, b := partitionKey(nodeA, nodeB)
	if ns.partitions[a] != nil {
		return ns.partitions[a][b]
	}
	return false
}

// GetDelay returns the artificial delay for a target (zero if none).
func (ns *NetworkFaultSimulator) GetDelay(target string) time.Duration {
	ns.mu.RLock()
	defer ns.mu.RUnlock()
	return ns.delays[target]
}

// ---------------------------------------------------------------------------
// DiskFaultSimulator
// ---------------------------------------------------------------------------

// DiskFaultSimulator simulates disk slowness and failures.
type DiskFaultSimulator struct {
	injector  *FaultInjector
	slowPaths map[string]time.Duration
	failPaths map[string]bool
	mu        sync.RWMutex
}

// NewDiskFaultSimulator creates a DiskFaultSimulator tied to an injector.
func NewDiskFaultSimulator(injector *FaultInjector) *DiskFaultSimulator {
	return &DiskFaultSimulator{
		injector:  injector,
		slowPaths: make(map[string]time.Duration),
		failPaths: make(map[string]bool),
	}
}

// SlowPath marks a path as artificially slow.
func (ds *DiskFaultSimulator) SlowPath(path string, latency time.Duration) {
	ds.mu.Lock()
	defer ds.mu.Unlock()
	ds.slowPaths[path] = latency
}

// FailPath marks a path as failing all I/O.
func (ds *DiskFaultSimulator) FailPath(path string) {
	ds.mu.Lock()
	defer ds.mu.Unlock()
	ds.failPaths[path] = true
}

// HealPath removes both slow and fail faults from a path.
func (ds *DiskFaultSimulator) HealPath(path string) {
	ds.mu.Lock()
	defer ds.mu.Unlock()
	delete(ds.slowPaths, path)
	delete(ds.failPaths, path)
}

// ShouldSlow reports whether I/O to the given path should be delayed.
func (ds *DiskFaultSimulator) ShouldSlow(path string) (bool, time.Duration) {
	ds.mu.RLock()
	defer ds.mu.RUnlock()

	if lat, ok := ds.slowPaths[path]; ok {
		return true, lat
	}
	return false, 0
}

// ShouldFail reports whether I/O to the given path should error.
func (ds *DiskFaultSimulator) ShouldFail(path string) bool {
	ds.mu.RLock()
	defer ds.mu.RUnlock()
	return ds.failPaths[path]
}

// ---------------------------------------------------------------------------
// ClockSimulator
// ---------------------------------------------------------------------------

// ClockSimulator simulates clock skew and drift.
type ClockSimulator struct {
	offset    time.Duration
	driftRate float64
	baseTime  time.Time
	mu        sync.RWMutex
}

// NewClockSimulator creates a ClockSimulator with no skew.
func NewClockSimulator() *ClockSimulator {
	return &ClockSimulator{
		driftRate: 1.0,
		baseTime:  time.Now(),
	}
}

// SetOffset sets a fixed offset added to the real time.
func (cs *ClockSimulator) SetOffset(offset time.Duration) {
	cs.mu.Lock()
	defer cs.mu.Unlock()
	cs.offset = offset
}

// SetDrift sets the clock drift rate (multiplier on elapsed real time).
func (cs *ClockSimulator) SetDrift(rate float64) {
	cs.mu.Lock()
	defer cs.mu.Unlock()
	cs.driftRate = rate
}

// Now returns the simulated current time accounting for offset and drift.
func (cs *ClockSimulator) Now() time.Time {
	cs.mu.RLock()
	defer cs.mu.RUnlock()

	elapsed := time.Since(cs.baseTime)
	drifted := time.Duration(float64(elapsed) * cs.driftRate)
	return cs.baseTime.Add(drifted).Add(cs.offset)
}

// Reset removes all clock skew and drift.
func (cs *ClockSimulator) Reset() {
	cs.mu.Lock()
	defer cs.mu.Unlock()
	cs.offset = 0
	cs.driftRate = 1.0
	cs.baseTime = time.Now()
}

// ---------------------------------------------------------------------------
// ChaosScenario
// ---------------------------------------------------------------------------

// ChaosStep is a single action within a chaos scenario.
type ChaosStep struct {
	Name   string
	Action func(ctx context.Context, injector *FaultInjector) error
	Delay  time.Duration
}

// ChaosInvariant is a condition that must hold throughout a scenario.
type ChaosInvariant struct {
	Name  string
	Check func() error
}

// ChaosStepResult records the outcome of executing a single step.
type ChaosStepResult struct {
	StepName string        `json:"step_name"`
	Success  bool          `json:"success"`
	Error    string        `json:"error"`
	Duration time.Duration `json:"duration"`
}

// ChaosInvariantResult records the outcome of checking an invariant.
type ChaosInvariantResult struct {
	InvariantName string `json:"invariant_name"`
	Passed        bool   `json:"passed"`
	Error         string `json:"error"`
}

// ChaosScenarioResult captures the full outcome of running a scenario.
type ChaosScenarioResult struct {
	Scenario         string                 `json:"scenario"`
	Passed           bool                   `json:"passed"`
	StepResults      []ChaosStepResult      `json:"step_results"`
	InvariantResults []ChaosInvariantResult `json:"invariant_results"`
	Duration         time.Duration          `json:"duration"`
	Events           []ChaosEvent           `json:"events"`
}

// ChaosScenario is a declarative description of a chaos test.
type ChaosScenario struct {
	Name        string
	Description string
	Steps       []ChaosStep
	Invariants  []ChaosInvariant
	mu          sync.RWMutex
}

// NewChaosScenario creates a named scenario.
func NewChaosScenario(name string) *ChaosScenario {
	return &ChaosScenario{
		Name:       name,
		Steps:      make([]ChaosStep, 0),
		Invariants: make([]ChaosInvariant, 0),
	}
}

// AddStep appends a step to the scenario (fluent API).
func (cs *ChaosScenario) AddStep(step ChaosStep) *ChaosScenario {
	cs.mu.Lock()
	defer cs.mu.Unlock()
	cs.Steps = append(cs.Steps, step)
	return cs
}

// AddInvariant appends an invariant to the scenario (fluent API).
func (cs *ChaosScenario) AddInvariant(inv ChaosInvariant) *ChaosScenario {
	cs.mu.Lock()
	defer cs.mu.Unlock()
	cs.Invariants = append(cs.Invariants, inv)
	return cs
}

// Execute runs the scenario against the provided injector.
func (cs *ChaosScenario) Execute(ctx context.Context, injector *FaultInjector) (*ChaosScenarioResult, error) {
	cs.mu.RLock()
	steps := make([]ChaosStep, len(cs.Steps))
	copy(steps, cs.Steps)
	invariants := make([]ChaosInvariant, len(cs.Invariants))
	copy(invariants, cs.Invariants)
	cs.mu.RUnlock()

	start := time.Now()
	result := &ChaosScenarioResult{
		Scenario:         cs.Name,
		Passed:           true,
		StepResults:      make([]ChaosStepResult, 0, len(steps)),
		InvariantResults: make([]ChaosInvariantResult, 0, len(invariants)),
	}

	// Execute each step sequentially.
	for _, step := range steps {
		if err := ctx.Err(); err != nil {
			return result, fmt.Errorf("chaos: scenario cancelled: %w", err)
		}

		if step.Delay > 0 {
			select {
			case <-time.After(step.Delay):
			case <-ctx.Done():
				return result, ctx.Err()
			}
		}

		stepStart := time.Now()
		err := step.Action(ctx, injector)
		sr := ChaosStepResult{
			StepName: step.Name,
			Success:  err == nil,
			Duration: time.Since(stepStart),
		}
		if err != nil {
			sr.Error = err.Error()
			result.Passed = false
		}
		result.StepResults = append(result.StepResults, sr)

		// Record step event.
		injector.mu.Lock()
		if injector.config.LogEvents {
			injector.events = append(injector.events, ChaosEvent{
				Type:        ChaosEventStepExecuted,
				Timestamp:   time.Now(),
				Description: fmt.Sprintf("Step %q completed (success=%v)", step.Name, sr.Success),
			})
		}
		injector.mu.Unlock()
	}

	// Check invariants.
	for _, inv := range invariants {
		err := inv.Check()
		ir := ChaosInvariantResult{
			InvariantName: inv.Name,
			Passed:        err == nil,
		}
		if err != nil {
			ir.Error = err.Error()
			result.Passed = false
		}
		result.InvariantResults = append(result.InvariantResults, ir)

		injector.mu.Lock()
		if injector.config.LogEvents {
			injector.events = append(injector.events, ChaosEvent{
				Type:        ChaosEventInvariantCheck,
				Timestamp:   time.Now(),
				Description: fmt.Sprintf("Invariant %q checked (passed=%v)", inv.Name, ir.Passed),
			})
		}
		injector.mu.Unlock()
	}

	result.Duration = time.Since(start)
	result.Events = injector.Events()
	return result, nil
}

// ---------------------------------------------------------------------------
// Built-in Scenarios
// ---------------------------------------------------------------------------

// NetworkPartitionScenario creates a scenario that partitions two nodes for
// the given duration and then heals.
func NetworkPartitionScenario(nodeA, nodeB string, duration time.Duration) *ChaosScenario {
	return NewChaosScenario(fmt.Sprintf("network-partition-%s-%s", nodeA, nodeB)).
		AddStep(ChaosStep{
			Name: "inject-partition",
			Action: func(_ context.Context, injector *FaultInjector) error {
				_, err := injector.InjectFault(&FaultConfig{
					Type:        FaultNetworkPartition,
					Duration:    duration,
					Probability: 1.0,
					Target:      fmt.Sprintf("%s<->%s", nodeA, nodeB),
					Parameters:  map[string]any{"nodeA": nodeA, "nodeB": nodeB},
				})
				return err
			},
		}).
		AddStep(ChaosStep{
			Name:  "wait-duration",
			Delay: duration,
			Action: func(_ context.Context, _ *FaultInjector) error {
				return nil
			},
		}).
		AddStep(ChaosStep{
			Name: "heal-partition",
			Action: func(_ context.Context, injector *FaultInjector) error {
				injector.RemoveAll()
				return nil
			},
		})
}

// DiskFailureScenario creates a scenario that fails a disk path for a
// specified duration.
func DiskFailureScenario(path string, duration time.Duration) *ChaosScenario {
	return NewChaosScenario(fmt.Sprintf("disk-failure-%s", path)).
		AddStep(ChaosStep{
			Name: "inject-disk-failure",
			Action: func(_ context.Context, injector *FaultInjector) error {
				_, err := injector.InjectFault(&FaultConfig{
					Type:        FaultDiskFail,
					Duration:    duration,
					Probability: 1.0,
					Target:      path,
					Parameters:  map[string]any{"path": path},
				})
				return err
			},
		}).
		AddStep(ChaosStep{
			Name:  "wait-duration",
			Delay: duration,
			Action: func(_ context.Context, _ *FaultInjector) error {
				return nil
			},
		}).
		AddStep(ChaosStep{
			Name: "heal-disk",
			Action: func(_ context.Context, injector *FaultInjector) error {
				injector.RemoveAll()
				return nil
			},
		})
}

// SplitBrainScenario creates a scenario that partitions a set of nodes into
// two halves.
func SplitBrainScenario(nodes []string) *ChaosScenario {
	scenario := NewChaosScenario("split-brain")
	if len(nodes) < 2 {
		return scenario
	}

	mid := len(nodes) / 2
	groupA := nodes[:mid]
	groupB := nodes[mid:]

	scenario.AddStep(ChaosStep{
		Name: "inject-split-brain",
		Action: func(_ context.Context, injector *FaultInjector) error {
			for _, a := range groupA {
				for _, b := range groupB {
					if _, err := injector.InjectFault(&FaultConfig{
						Type:        FaultNetworkPartition,
						Duration:    time.Minute,
						Probability: 1.0,
						Target:      fmt.Sprintf("%s<->%s", a, b),
						Parameters:  map[string]any{"nodeA": a, "nodeB": b},
					}); err != nil {
						return err
					}
				}
			}
			return nil
		},
	})

	scenario.AddStep(ChaosStep{
		Name:  "heal-split-brain",
		Delay: time.Minute,
		Action: func(_ context.Context, injector *FaultInjector) error {
			injector.RemoveAll()
			return nil
		},
	})

	return scenario
}

// RollingRestartScenario simulates restarting nodes one at a time with an
// interval between each restart.
func RollingRestartScenario(nodes []string, interval time.Duration) *ChaosScenario {
	scenario := NewChaosScenario("rolling-restart")

	for _, node := range nodes {
		n := node // capture
		scenario.AddStep(ChaosStep{
			Name: fmt.Sprintf("pause-%s", n),
			Action: func(_ context.Context, injector *FaultInjector) error {
				_, err := injector.InjectFault(&FaultConfig{
					Type:        FaultProcessPause,
					Duration:    interval,
					Probability: 1.0,
					Target:      n,
					Parameters:  map[string]any{"node": n},
				})
				return err
			},
		})
		scenario.AddStep(ChaosStep{
			Name:  fmt.Sprintf("wait-restart-%s", n),
			Delay: interval,
			Action: func(_ context.Context, injector *FaultInjector) error {
				injector.RemoveAll()
				return nil
			},
		})
	}

	return scenario
}

// ---------------------------------------------------------------------------
// ChaosReport
// ---------------------------------------------------------------------------

// ChaosReport aggregates results from multiple chaos scenarios.
type ChaosReport struct {
	Results        []*ChaosScenarioResult `json:"results"`
	TotalScenarios int                    `json:"total_scenarios"`
	Passed         int                    `json:"passed"`
	Failed         int                    `json:"failed"`
	Duration       time.Duration          `json:"duration"`
	Summary        string                 `json:"summary"`
}

// GenerateChaosReport builds a ChaosReport from a set of scenario results.
func GenerateChaosReport(results []*ChaosScenarioResult) *ChaosReport {
	report := &ChaosReport{
		Results:        results,
		TotalScenarios: len(results),
	}

	var totalDuration time.Duration
	var failedNames []string

	for _, r := range results {
		totalDuration += r.Duration
		if r.Passed {
			report.Passed++
		} else {
			report.Failed++
			failedNames = append(failedNames, r.Scenario)
		}
	}

	report.Duration = totalDuration

	if report.Failed == 0 {
		report.Summary = fmt.Sprintf("All %d chaos scenarios passed", report.TotalScenarios)
	} else {
		report.Summary = fmt.Sprintf("%d/%d chaos scenarios failed: %s",
			report.Failed, report.TotalScenarios, strings.Join(failedNames, ", "))
	}

	return report
}
