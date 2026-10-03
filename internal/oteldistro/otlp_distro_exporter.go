package oteldistro

import (
	"context"
	"encoding/json"
	"fmt"
	"sync"
	"sync/atomic"
)

// otlpExporterState holds per-instance state for OTLPDistroExporter.
type otlpExporterState struct {
	mu       sync.Mutex
	buffer   [][]byte
	exported atomic.Int64
	failed   atomic.Int64
}

func (e *OTLPDistroExporter) Start(ctx context.Context, host Host) error { return nil }

func (e *OTLPDistroExporter) Shutdown(ctx context.Context) error {
	e.state.mu.Lock()
	defer e.state.mu.Unlock()
	if len(e.state.buffer) > 0 {
		e.flushLocked(&e.state)
	}
	return nil
}

func (e *OTLPDistroExporter) ExportMetrics(ctx context.Context, metrics *Metrics) error {
	if metrics == nil {
		return nil
	}

	data, err := json.Marshal(metrics)
	if err != nil {
		e.state.failed.Add(1)
		return fmt.Errorf("failed to serialize metrics: %w", err)
	}

	e.state.mu.Lock()
	e.state.buffer = append(e.state.buffer, data)
	e.state.mu.Unlock()

	e.state.exported.Add(1)
	return nil
}

// Flush sends buffered metrics to the remote OTLP endpoint.
func (e *OTLPDistroExporter) Flush() error {
	e.state.mu.Lock()
	defer e.state.mu.Unlock()
	return e.flushLocked(&e.state)
}

func (e *OTLPDistroExporter) flushLocked(s *otlpExporterState) error {
	if len(s.buffer) == 0 {
		return nil
	}
	// Stub: in production this would POST to e.config.Endpoint using e.client
	s.buffer = nil
	return nil
}
