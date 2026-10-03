package chronicle

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"sync"
	"sync/atomic"
	"time"
)

// BackpressureStrategy defines how the pipeline handles slow consumers.
type BackpressureStrategy int

const (
	BackpressureDrop BackpressureStrategy = iota
	BackpressureBlock
	BackpressureSample
)

// ETLErrorHandler is a callback invoked when a stage encounters an error.
type ETLErrorHandler func(error, *Point)

// ETLTransformFunc transforms a point, returning a modified copy or an error.
type ETLTransformFunc func(*Point) (*Point, error)

// ETLFilterFunc returns true if a point should pass through the pipeline.
type ETLFilterFunc func(*Point) bool

// ETLEnrichFunc enriches a point with additional data.
type ETLEnrichFunc func(*Point) (*Point, error)

// ETLRouteFunc returns a routing key for a point.
type ETLRouteFunc func(*Point) string

// ---------------------------------------------------------------------------
// Interfaces
// ---------------------------------------------------------------------------

// ETLSource produces points for a pipeline.
type ETLSource interface {
	Open(ctx context.Context) error
	Read(ctx context.Context) (*Point, error)
	Close() error
}

// ETLSink consumes points produced by a pipeline.
type ETLSink interface {
	Open(ctx context.Context) error
	Write(ctx context.Context, point *Point) error
	Flush(ctx context.Context) error
	Close() error
}

// ETLStage is a single processing step in a pipeline.
type ETLStage interface {
	Process(ctx context.Context, point *Point) ([]*Point, error)
	Name() string
}

// ---------------------------------------------------------------------------
// Config
// ---------------------------------------------------------------------------

// ETLPipelineConfig holds configuration for an ETL pipeline.
type ETLPipelineConfig struct {
	Name                 string
	MaxBufferSize        int
	CheckpointInterval   time.Duration
	Workers              int
	BackpressureStrategy BackpressureStrategy
	ErrorHandler         ETLErrorHandler
}

// DefaultETLPipelineConfig returns sensible defaults for a pipeline.
func DefaultETLPipelineConfig() ETLPipelineConfig {
	return ETLPipelineConfig{
		Name:                 "default",
		MaxBufferSize:        10000,
		CheckpointInterval:   30 * time.Second,
		Workers:              1,
		BackpressureStrategy: BackpressureBlock,
	}
}

// ---------------------------------------------------------------------------
// Pipeline Stats
// ---------------------------------------------------------------------------

// ETLPipelineStats tracks runtime metrics for a pipeline.
type ETLPipelineStats struct {
	PointsRead         uint64
	PointsWritten      uint64
	PointsFiltered     uint64
	PointsErrored      uint64
	BytesProcessed     int64
	Uptime             time.Duration
	StageLatencies     map[string]time.Duration
	BackpressureEvents uint64
	LastCheckpoint     time.Time
	startTime          time.Time
	mu                 sync.RWMutex
}

func newETLPipelineStats() *ETLPipelineStats {
	return &ETLPipelineStats{
		StageLatencies: make(map[string]time.Duration),
		startTime:      time.Now(),
	}
}

func (s *ETLPipelineStats) snapshot() *ETLPipelineStats {
	s.mu.RLock()
	defer s.mu.RUnlock()
	latencies := make(map[string]time.Duration, len(s.StageLatencies))
	for k, v := range s.StageLatencies {
		latencies[k] = v
	}
	return &ETLPipelineStats{
		PointsRead:         atomic.LoadUint64(&s.PointsRead),
		PointsWritten:      atomic.LoadUint64(&s.PointsWritten),
		PointsFiltered:     atomic.LoadUint64(&s.PointsFiltered),
		PointsErrored:      atomic.LoadUint64(&s.PointsErrored),
		BytesProcessed:     atomic.LoadInt64(&s.BytesProcessed),
		Uptime:             time.Since(s.startTime),
		StageLatencies:     latencies,
		BackpressureEvents: s.BackpressureEvents,
		LastCheckpoint:     s.LastCheckpoint,
	}
}

// ---------------------------------------------------------------------------
// Checkpoint
// ---------------------------------------------------------------------------

// ETLCheckpoint persists pipeline progress so it can resume after a restart.
type ETLCheckpoint struct {
	PipelineName    string    `json:"pipeline_name"`
	LastProcessed   int64     `json:"last_processed"`
	PointsProcessed uint64    `json:"points_processed"`
	SavedAt         time.Time `json:"saved_at"`
}

func (c *ETLCheckpoint) save(path string) error {
	data, err := json.MarshalIndent(c, "", "  ")
	if err != nil {
		return fmt.Errorf("etl checkpoint marshal: %w", err)
	}
	return os.WriteFile(path, data, 0644)
}

func loadETLCheckpoint(path string) (*ETLCheckpoint, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return nil, err
	}
	var cp ETLCheckpoint
	if err := json.Unmarshal(data, &cp); err != nil {
		return nil, fmt.Errorf("etl checkpoint unmarshal: %w", err)
	}
	return &cp, nil
}

// ---------------------------------------------------------------------------
// ETL Pipeline
// ---------------------------------------------------------------------------

// ETLPipeline orchestrates an Extract-Transform-Load flow over time-series points.
type ETLPipeline struct {
	config     ETLPipelineConfig
	source     ETLSource
	stages     []ETLStage
	sink       ETLSink
	running    bool
	mu         sync.RWMutex
	ctx        context.Context
	cancel     context.CancelFunc
	wg         sync.WaitGroup
	stats      *ETLPipelineStats
	checkpoint *ETLCheckpoint
}

// NewETLPipeline creates a new pipeline with the given configuration.
func NewETLPipeline(config ETLPipelineConfig) *ETLPipeline {
	if config.MaxBufferSize <= 0 {
		config.MaxBufferSize = 10000
	}
	if config.Workers <= 0 {
		config.Workers = 1
	}
	if config.CheckpointInterval <= 0 {
		config.CheckpointInterval = 30 * time.Second
	}
	return &ETLPipeline{
		config: config,
		stats:  newETLPipelineStats(),
		checkpoint: &ETLCheckpoint{
			PipelineName: config.Name,
		},
	}
}

// From sets the pipeline source (fluent API).
func (p *ETLPipeline) From(source ETLSource) *ETLPipeline {
	p.source = source
	return p
}

// Transform adds a transform stage to the pipeline.
func (p *ETLPipeline) Transform(fn ETLTransformFunc) *ETLPipeline {
	p.stages = append(p.stages, &etlTransformStage{fn: fn})
	return p
}

// Filter adds a filter stage to the pipeline.
func (p *ETLPipeline) Filter(fn ETLFilterFunc) *ETLPipeline {
	p.stages = append(p.stages, &etlFilterStage{fn: fn})
	return p
}

// Enrich adds an enrichment stage to the pipeline.
func (p *ETLPipeline) Enrich(fn ETLEnrichFunc) *ETLPipeline {
	p.stages = append(p.stages, &etlEnrichStage{fn: fn})
	return p
}

// Route adds a routing stage to the pipeline.
func (p *ETLPipeline) Route(fn ETLRouteFunc) *ETLPipeline {
	p.stages = append(p.stages, &etlRouteStage{fn: fn})
	return p
}

// Aggregate adds a windowed aggregation stage to the pipeline.
func (p *ETLPipeline) Aggregate(config ETLAggregateConfig) *ETLPipeline {
	p.stages = append(p.stages, newETLAggregateStage(config))
	return p
}

// To sets the pipeline sink (fluent API).
func (p *ETLPipeline) To(sink ETLSink) *ETLPipeline {
	p.sink = sink
	return p
}

// Start begins pipeline processing.
func (p *ETLPipeline) Start() error {
	p.mu.Lock()
	defer p.mu.Unlock()

	if p.running {
		return fmt.Errorf("etl pipeline %q already running", p.config.Name)
	}
	if p.source == nil {
		return fmt.Errorf("etl pipeline %q has no source", p.config.Name)
	}
	if p.sink == nil {
		return fmt.Errorf("etl pipeline %q has no sink", p.config.Name)
	}

	p.ctx, p.cancel = context.WithCancel(context.Background())
	p.stats = newETLPipelineStats()

	if err := p.source.Open(p.ctx); err != nil {
		return fmt.Errorf("etl source open: %w", err)
	}
	if err := p.sink.Open(p.ctx); err != nil {
		closeQuietly(p.source)
		return fmt.Errorf("etl sink open: %w", err)
	}

	p.running = true

	for i := 0; i < p.config.Workers; i++ {
		p.wg.Add(1)
		go p.worker()
	}

	p.wg.Add(1)
	go p.checkpointLoop()

	return nil
}

// Stop gracefully shuts down the pipeline.
func (p *ETLPipeline) Stop() error {
	p.mu.Lock()
	if !p.running {
		p.mu.Unlock()
		return fmt.Errorf("etl pipeline %q not running", p.config.Name)
	}
	p.cancel()
	p.mu.Unlock()

	p.wg.Wait()

	p.mu.Lock()
	defer p.mu.Unlock()
	p.running = false

	if err := p.sink.Flush(context.Background()); err != nil {
		return fmt.Errorf("etl sink flush: %w", err)
	}
	if err := p.sink.Close(); err != nil {
		return fmt.Errorf("etl sink close: %w", err)
	}
	if err := p.source.Close(); err != nil {
		return fmt.Errorf("etl source close: %w", err)
	}
	return nil
}

// Stats returns a snapshot of the pipeline statistics.
func (p *ETLPipeline) Stats() *ETLPipelineStats {
	return p.stats.snapshot()
}

func (p *ETLPipeline) worker() {
	defer p.wg.Done()
	for {
		select {
		case <-p.ctx.Done():
			return
		default:
		}

		pt, err := p.source.Read(p.ctx)
		if err != nil {
			if p.ctx.Err() != nil {
				return
			}
			atomic.AddUint64(&p.stats.PointsErrored, 1)
			if p.config.ErrorHandler != nil {
				p.config.ErrorHandler(err, nil)
			}
			continue
		}
		if pt == nil {
			continue
		}

		atomic.AddUint64(&p.stats.PointsRead, 1)

		points := []*Point{pt}
		filtered := false
		for _, stage := range p.stages {
			start := time.Now()
			var next []*Point
			for _, sp := range points {
				out, sErr := stage.Process(p.ctx, sp)
				if sErr != nil {
					atomic.AddUint64(&p.stats.PointsErrored, 1)
					if p.config.ErrorHandler != nil {
						p.config.ErrorHandler(sErr, sp)
					}
					continue
				}
				next = append(next, out...)
			}
			elapsed := time.Since(start)
			p.stats.mu.Lock()
			p.stats.StageLatencies[stage.Name()] = elapsed
			p.stats.mu.Unlock()

			points = next
			if len(points) == 0 {
				filtered = true
				break
			}
		}

		if filtered {
			atomic.AddUint64(&p.stats.PointsFiltered, 1)
			continue
		}

		for _, out := range points {
			if wErr := p.sink.Write(p.ctx, out); wErr != nil {
				atomic.AddUint64(&p.stats.PointsErrored, 1)
				if p.config.ErrorHandler != nil {
					p.config.ErrorHandler(wErr, out)
				}
				continue
			}
			atomic.AddUint64(&p.stats.PointsWritten, 1)
			p.checkpoint.LastProcessed = out.Timestamp
			atomic.AddUint64(&p.checkpoint.PointsProcessed, 1)
		}
	}
}

func (p *ETLPipeline) checkpointLoop() {
	defer p.wg.Done()
	ticker := time.NewTicker(p.config.CheckpointInterval)
	defer ticker.Stop()
	for {
		select {
		case <-p.ctx.Done():
			return
		case <-ticker.C:
			p.checkpoint.SavedAt = time.Now()
			path := fmt.Sprintf("checkpoint_%s.json", p.config.Name)
			_ = p.checkpoint.save(path) //nolint:errcheck // best-effort ETL processing
			p.stats.mu.Lock()
			p.stats.LastCheckpoint = p.checkpoint.SavedAt
			p.stats.mu.Unlock()
		}
	}
}

// ---------------------------------------------------------------------------
// Built-in Stages
// ---------------------------------------------------------------------------

type etlTransformStage struct{ fn ETLTransformFunc }

func (s *etlTransformStage) Name() string { return "transform" }
func (s *etlTransformStage) Process(_ context.Context, pt *Point) ([]*Point, error) {
	out, err := s.fn(pt)
	if err != nil {
		return nil, err
	}
	if out == nil {
		return nil, nil
	}
	return []*Point{out}, nil
}

type etlFilterStage struct{ fn ETLFilterFunc }

func (s *etlFilterStage) Name() string { return "filter" }
func (s *etlFilterStage) Process(_ context.Context, pt *Point) ([]*Point, error) {
	if s.fn(pt) {
		return []*Point{pt}, nil
	}
	return nil, nil
}

type etlEnrichStage struct{ fn ETLEnrichFunc }

func (s *etlEnrichStage) Name() string { return "enrich" }
func (s *etlEnrichStage) Process(_ context.Context, pt *Point) ([]*Point, error) {
	out, err := s.fn(pt)
	if err != nil {
		return nil, err
	}
	if out == nil {
		return nil, nil
	}
	return []*Point{out}, nil
}

type etlRouteStage struct{ fn ETLRouteFunc }
