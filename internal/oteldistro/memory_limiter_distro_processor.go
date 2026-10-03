package oteldistro

import (
	"context"
	"fmt"
	"runtime"
	"sync"
	"time"
)

// memLimiterState holds per-instance state for MemoryLimiterDistroProcessor.
type memLimiterState struct {
	mu        sync.Mutex
	memStats  runtime.MemStats
	lastCheck time.Time
	dropped   int64
}

func (p *MemoryLimiterDistroProcessor) Start(ctx context.Context, host Host) error {
	p.state.mu.Lock()
	defer p.state.mu.Unlock()
	runtime.ReadMemStats(&p.state.memStats)
	p.state.lastCheck = time.Now()
	return nil
}

func (p *MemoryLimiterDistroProcessor) Shutdown(ctx context.Context) error {
	return nil
}

func (p *MemoryLimiterDistroProcessor) ProcessMetrics(ctx context.Context, metrics *Metrics) (*Metrics, error) {
	p.state.mu.Lock()
	defer p.state.mu.Unlock()

	checkInterval := p.config.CheckInterval
	if checkInterval <= 0 {
		checkInterval = time.Second
	}

	if time.Since(p.state.lastCheck) >= checkInterval {
		runtime.ReadMemStats(&p.state.memStats)
		p.state.lastCheck = time.Now()
	}

	if p.config.LimitMiB > 0 {
		usedMiB := p.state.memStats.Alloc / (1024 * 1024)
		if usedMiB >= uint64(p.config.LimitMiB) {
			p.state.dropped++
			return nil, fmt.Errorf("memory limit exceeded: %d MiB used, limit %d MiB", usedMiB, p.config.LimitMiB)
		}
	}

	return metrics, nil
}
