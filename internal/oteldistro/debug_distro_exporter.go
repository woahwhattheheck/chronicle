package oteldistro

import (
	"context"
	"fmt"
	"sync/atomic"
)

// debugExporterState holds per-instance state for DebugDistroExporter.
type debugExporterState struct {
	exportCount atomic.Int64
}

func (e *DebugDistroExporter) Start(ctx context.Context, host Host) error { return nil }

func (e *DebugDistroExporter) Shutdown(ctx context.Context) error {
	return nil
}

func (e *DebugDistroExporter) ExportMetrics(ctx context.Context, metrics *Metrics) error {
	if metrics == nil {
		return nil
	}

	count := e.state.exportCount.Add(1)
	verbosity := e.config.Verbosity

	totalMetrics := 0
	for _, rm := range metrics.ResourceMetrics {
		for _, sm := range rm.ScopeMetrics {
			totalMetrics += len(sm.Metrics)
		}
	}

	switch verbosity {
	case "basic":
		fmt.Printf("[DebugExporter] Export #%d: %d metrics\n", count, totalMetrics)
	case "detailed":
		fmt.Printf("[DebugExporter] Export #%d: %d metrics\n", count, totalMetrics)
		for _, rm := range metrics.ResourceMetrics {
			fmt.Printf("  Resource: %v\n", rm.Resource.Attributes)
			for _, sm := range rm.ScopeMetrics {
				fmt.Printf("  Scope: %s/%s\n", sm.Scope.Name, sm.Scope.Version)
				for _, m := range sm.Metrics {
					fmt.Printf("    Metric: %s (%s) [%s] = %v\n", m.Name, m.Description, m.Unit, m.Data)
				}
			}
		}
	default: // "normal"
		fmt.Printf("[DebugExporter] Export #%d: %d metrics", count, totalMetrics)
		for _, rm := range metrics.ResourceMetrics {
			for _, sm := range rm.ScopeMetrics {
				for _, m := range sm.Metrics {
					fmt.Printf(" %s", m.Name)
				}
			}
		}
		fmt.Println()
	}

	return nil
}
