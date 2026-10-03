package chronicle

import (
	"context"
	"testing"
	"time"
)

func TestFaultInjector_InjectRemove(t *testing.T) {
	cfg := DefaultChaosConfig()
	fi := NewFaultInjector(cfg)

	fault, err := fi.InjectFault(&FaultConfig{
		Type:        FaultNetworkPartition,
		Duration:    time.Minute,
		Probability: 1.0,
		Target:      "node-1",
	})
	if err != nil {
		t.Fatalf("InjectFault: %v", err)
	}
	if fault.ID == "" {
		t.Errorf("expected non-empty fault ID")
	}

	active := fi.ActiveFaults()
	if len(active) != 1 {
		t.Fatalf("expected 1 active fault, got %d", len(active))
	}

	if err := fi.RemoveFault(fault.ID); err != nil {
		t.Fatalf("RemoveFault: %v", err)
	}

	active = fi.ActiveFaults()
	if len(active) != 0 {
		t.Errorf("expected 0 active faults after remove, got %d", len(active))
	}

	// Removing again should error.
	if err := fi.RemoveFault(fault.ID); err == nil {
		t.Errorf("expected error removing already-removed fault")
	}
}

func TestFaultInjector_ShouldFail(t *testing.T) {
	cfg := DefaultChaosConfig()
	cfg.Seed = 42
	fi := NewFaultInjector(cfg)

	// No faults active, ShouldFail should return false.
	if fi.ShouldFail(FaultDiskFail) {
		t.Errorf("expected ShouldFail=false with no active faults")
	}

	// Inject a fault with probability 1.0.
	fault, err := fi.InjectFault(&FaultConfig{
		Type:        FaultDiskFail,
		Duration:    time.Minute,
		Probability: 1.0,
		Target:      "/data",
	})
	if err != nil {
		t.Fatalf("InjectFault: %v", err)
	}

	if !fi.ShouldFail(FaultDiskFail) {
		t.Errorf("expected ShouldFail=true with probability=1.0")
	}
	if fault.TriggerCount != 1 {
		t.Errorf("expected one recorded trigger, got %d", fault.TriggerCount)
	}

	// Different fault type should not fail.
	if fi.ShouldFail(FaultMemoryPressure) {
		t.Errorf("expected ShouldFail=false for unrelated fault type")
	}
	if fault.TriggerCount != 1 {
		t.Errorf("unrelated fault changed trigger count to %d", fault.TriggerCount)
	}
}

func TestNetworkFaultSimulator_Partition(t *testing.T) {
	fi := NewFaultInjector(DefaultChaosConfig())
	ns := NewNetworkFaultSimulator(fi)

	if err := ns.PartitionNodes("node-A", "node-B"); err != nil {
		t.Fatalf("PartitionNodes: %v", err)
	}

	if !ns.IsPartitioned("node-A", "node-B") {
		t.Errorf("expected nodes to be partitioned")
	}
	if !ns.IsPartitioned("node-B", "node-A") {
		t.Errorf("expected partition to be bidirectional")
	}
	if ns.IsPartitioned("node-A", "node-C") {
		t.Errorf("expected no partition between A and C")
	}

	ns.HealPartition("node-A", "node-B")
	if ns.IsPartitioned("node-A", "node-B") {
		t.Errorf("expected partition to be healed")
	}

	t.Run("Delay", func(t *testing.T) {
		ns.AddDelay("node-A", 100*time.Millisecond)
		d := ns.GetDelay("node-A")
		if d != 100*time.Millisecond {
			t.Errorf("expected delay 100ms, got %v", d)
		}
		ns.RemoveDelay("node-A")
		if ns.GetDelay("node-A") != 0 {
			t.Errorf("expected 0 delay after remove")
		}
	})
}

func TestDiskFaultSimulator_SlowFail(t *testing.T) {
	fi := NewFaultInjector(DefaultChaosConfig())
	ds := NewDiskFaultSimulator(fi)

	ds.SlowPath("/data/wal", 50*time.Millisecond)
	slow, lat := ds.ShouldSlow("/data/wal")
	if !slow {
		t.Errorf("expected ShouldSlow=true")
	}
	if lat != 50*time.Millisecond {
		t.Errorf("expected latency 50ms, got %v", lat)
	}

	ds.FailPath("/data/wal")
	if !ds.ShouldFail("/data/wal") {
		t.Errorf("expected ShouldFail=true")
	}

	ds.HealPath("/data/wal")
	slow, _ = ds.ShouldSlow("/data/wal")
	if slow {
		t.Errorf("expected ShouldSlow=false after heal")
	}
	if ds.ShouldFail("/data/wal") {
		t.Errorf("expected ShouldFail=false after heal")
	}
}

func TestClockSimulator_OffsetDrift(t *testing.T) {
	cs := NewClockSimulator()

	realNow := time.Now()
	simNow := cs.Now()
	diff := simNow.Sub(realNow)
	if diff < -time.Second || diff > time.Second {
		t.Errorf("expected simulated time close to real time, diff=%v", diff)
	}

	cs.SetOffset(10 * time.Second)
	simNow = cs.Now()
	diff = simNow.Sub(time.Now())
	if diff < 9*time.Second || diff > 11*time.Second {
		t.Errorf("expected ~10s offset, got %v", diff)
	}

	cs.SetDrift(2.0)
	// Drift rate of 2.0 means simulated time advances twice as fast.
	// Hard to test precisely, but we can verify it's different.
	time.Sleep(10 * time.Millisecond)
	simNow2 := cs.Now()
	if simNow2.Before(simNow) {
		t.Errorf("expected time to advance with drift")
	}

	cs.Reset()
	simNow3 := cs.Now()
	diff = simNow3.Sub(time.Now())
	if diff < -time.Second || diff > time.Second {
		t.Errorf("expected near-zero offset after reset, got %v", diff)
	}
}

func TestChaosScenario_Execute(t *testing.T) {
	fi := NewFaultInjector(DefaultChaosConfig())

	scenario := NewChaosScenario("test-scenario").
		AddStep(ChaosStep{
			Name: "inject",
			Action: func(_ context.Context, injector *FaultInjector) error {
				_, err := injector.InjectFault(&FaultConfig{
					Type:        FaultDiskFail,
					Duration:    time.Second,
					Probability: 1.0,
					Target:      "/data",
				})
				return err
			},
		}).
		AddStep(ChaosStep{
			Name: "cleanup",
			Action: func(_ context.Context, injector *FaultInjector) error {
				injector.RemoveAll()
				return nil
			},
		}).
		AddInvariant(ChaosInvariant{
			Name:  "no-active-faults",
			Check: func() error { return nil },
		})

	result, err := scenario.Execute(context.Background(), fi)
	if err != nil {
		t.Fatalf("Execute: %v", err)
	}
	if !result.Passed {
		t.Errorf("expected scenario to pass")
	}
	if len(result.StepResults) != 2 {
		t.Errorf("expected 2 step results, got %d", len(result.StepResults))
	}
	if len(result.InvariantResults) != 1 {
		t.Errorf("expected 1 invariant result, got %d", len(result.InvariantResults))
	}
}

func TestBuiltinScenarios(t *testing.T) {
	t.Run("NetworkPartition", func(t *testing.T) {
		s := NetworkPartitionScenario("node-1", "node-2", 1*time.Millisecond)
		if s.Name == "" {
			t.Errorf("expected non-empty scenario name")
		}
		if len(s.Steps) != 3 {
			t.Errorf("expected 3 steps, got %d", len(s.Steps))
		}
	})

	t.Run("DiskFailure", func(t *testing.T) {
		s := DiskFailureScenario("/data", 1*time.Millisecond)
		if len(s.Steps) != 3 {
			t.Errorf("expected 3 steps, got %d", len(s.Steps))
		}
	})

	t.Run("SplitBrain", func(t *testing.T) {
		s := SplitBrainScenario([]string{"n1", "n2", "n3", "n4"})
		if len(s.Steps) != 2 {
			t.Errorf("expected 2 steps, got %d", len(s.Steps))
		}
	})

	t.Run("RollingRestart", func(t *testing.T) {
		s := RollingRestartScenario([]string{"n1", "n2", "n3"}, time.Millisecond)
		if len(s.Steps) != 6 {
			t.Errorf("expected 6 steps (2 per node), got %d", len(s.Steps))
		}
	})
}

func TestChaosReport_Generate(t *testing.T) {
	results := []*ChaosScenarioResult{
		{Scenario: "test-1", Passed: true, Duration: time.Second},
		{Scenario: "test-2", Passed: false, Duration: 2 * time.Second},
		{Scenario: "test-3", Passed: true, Duration: time.Second},
	}

	report := GenerateChaosReport(results)
	if report.TotalScenarios != 3 {
		t.Errorf("expected 3 total scenarios, got %d", report.TotalScenarios)
	}
	if report.Passed != 2 {
		t.Errorf("expected 2 passed, got %d", report.Passed)
	}
	if report.Failed != 1 {
		t.Errorf("expected 1 failed, got %d", report.Failed)
	}
	if report.Summary == "" {
		t.Errorf("expected non-empty summary")
	}
}
