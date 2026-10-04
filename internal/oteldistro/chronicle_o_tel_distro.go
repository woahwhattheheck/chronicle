package oteldistro

import (
	"context"
	"fmt"
	"sort"
	"sync/atomic"
	"time"
)

// Start starts the distribution.
func (d *ChronicleOTelDistro) Start() error {
	d.mu.Lock()
	defer d.mu.Unlock()

	if d.running {
		return nil
	}

	if err := d.initializeReceivers(); err != nil {
		return fmt.Errorf("failed to initialize receivers: %w", err)
	}

	if err := d.initializeProcessors(); err != nil {
		return fmt.Errorf("failed to initialize processors: %w", err)
	}

	if err := d.initializeExporters(); err != nil {
		return fmt.Errorf("failed to initialize exporters: %w", err)
	}

	if err := d.initializeExtensions(); err != nil {
		return fmt.Errorf("failed to initialize extensions: %w", err)
	}

	if err := d.initializePipelines(); err != nil {
		return fmt.Errorf("failed to initialize pipelines: %w", err)
	}

	for name, ext := range d.extensions {
		if err := ext.Start(d.ctx, d); err != nil {
			return fmt.Errorf("failed to start extension %s: %w", name, err)
		}
	}

	for name, pipeline := range d.pipelines {
		if err := d.startPipeline(pipeline); err != nil {
			return fmt.Errorf("failed to start pipeline %s: %w", name, err)
		}
		atomic.AddInt64(&d.metrics.PipelinesStarted, 1)
	}

	d.running = true
	return nil
}

// Stop stops the distribution.
func (d *ChronicleOTelDistro) Stop() error {
	d.mu.Lock()
	defer d.mu.Unlock()

	if !d.running {
		return nil
	}

	d.cancel()

	for _, pipeline := range d.pipelines {
		d.stopPipeline(pipeline)
	}

	for _, ext := range d.extensions {
		ext.Shutdown(context.Background())
	}

	for _, exp := range d.exporters {
		exp.Shutdown(context.Background())
	}

	for _, proc := range d.processors {
		proc.Shutdown(context.Background())
	}

	for _, recv := range d.receivers {
		recv.Shutdown(context.Background())
	}

	d.wg.Wait()
	d.running = false
	return nil
}

// GetExtension implements Host interface.
func (d *ChronicleOTelDistro) GetExtension(id string) Extension {
	d.mu.RLock()
	defer d.mu.RUnlock()
	return d.extensions[id]
}

// ReportFatalError implements Host interface.
func (d *ChronicleOTelDistro) ReportFatalError(err error) {
	d.metrics.mu.Lock()
	d.metrics.Errors++
	d.metrics.LastError = err.Error()
	d.metrics.mu.Unlock()
}

func (d *ChronicleOTelDistro) initializeReceivers() error {

	if d.config.Receivers.OTLP != nil {
		recv := NewOTLPDistroReceiver(d.config.Receivers.OTLP, d)
		d.receivers["otlp"] = recv
		atomic.AddInt64(&d.metrics.ReceiversStarted, 1)
	}

	if d.config.Receivers.Prometheus != nil {
		recv := NewPrometheusDistroReceiver(d.config.Receivers.Prometheus, d)
		d.receivers["prometheus"] = recv
		atomic.AddInt64(&d.metrics.ReceiversStarted, 1)
	}

	if d.config.Receivers.HostMetrics != nil {
		recv := NewHostMetricsDistroReceiver(d.config.Receivers.HostMetrics, d)
		d.receivers["hostmetrics"] = recv
		atomic.AddInt64(&d.metrics.ReceiversStarted, 1)
	}

	return nil
}

func (d *ChronicleOTelDistro) initializeProcessors() error {

	if d.config.Processors.Batch != nil {
		proc := NewBatchDistroProcessor(d.config.Processors.Batch)
		d.processors["batch"] = proc
		atomic.AddInt64(&d.metrics.ProcessorsStarted, 1)
	}

	if d.config.Processors.Memory != nil {
		proc := NewMemoryLimiterDistroProcessor(d.config.Processors.Memory)
		d.processors["memory_limiter"] = proc
		atomic.AddInt64(&d.metrics.ProcessorsStarted, 1)
	}

	if d.config.Processors.Attributes != nil {
		proc := NewAttributesDistroProcessor(d.config.Processors.Attributes)
		d.processors["attributes"] = proc
		atomic.AddInt64(&d.metrics.ProcessorsStarted, 1)
	}

	if d.config.Processors.Filter != nil {
		proc := NewFilterDistroProcessor(d.config.Processors.Filter)
		d.processors["filter"] = proc
		atomic.AddInt64(&d.metrics.ProcessorsStarted, 1)
	}

	return nil
}

func (d *ChronicleOTelDistro) initializeExporters() error {

	if d.config.Exporters.Chronicle != nil {
		exp := NewChronicleDistroExporter(d.pw, d.config.Exporters.Chronicle)
		d.exporters["chronicle"] = exp
		atomic.AddInt64(&d.metrics.ExportersStarted, 1)
	}

	if d.config.Exporters.OTLP != nil {
		exp := NewOTLPDistroExporter(d.config.Exporters.OTLP)
		d.exporters["otlp"] = exp
		atomic.AddInt64(&d.metrics.ExportersStarted, 1)
	}

	if d.config.Exporters.Debug != nil {
		exp := NewDebugDistroExporter(d.config.Exporters.Debug)
		d.exporters["debug"] = exp
		atomic.AddInt64(&d.metrics.ExportersStarted, 1)
	}

	return nil
}

func (d *ChronicleOTelDistro) initializeExtensions() error {

	if d.config.Extensions.Health != nil {
		ext := NewHealthCheckExtension(d.config.Extensions.Health, d)
		d.extensions["health_check"] = ext
	}

	if d.config.Extensions.ZPages != nil {
		ext := NewZPagesExtension(d.config.Extensions.ZPages)
		d.extensions["zpages"] = ext
	}

	return nil
}

func (d *ChronicleOTelDistro) initializePipelines() error {

	if d.config.Pipelines.Metrics != nil {
		pipeline := &Pipeline{
			Name:     "metrics",
			dataChan: make(chan *Metrics, 1000),
		}

		for _, name := range d.config.Pipelines.Metrics.Receivers {
			if recv, ok := d.receivers[name]; ok {
				pipeline.Receivers = append(pipeline.Receivers, recv)
			}
		}

		for _, name := range d.config.Pipelines.Metrics.Processors {
			if proc, ok := d.processors[name]; ok {
				pipeline.Processors = append(pipeline.Processors, proc)
			}
		}

		for _, name := range d.config.Pipelines.Metrics.Exporters {
			if exp, ok := d.exporters[name]; ok {
				pipeline.Exporters = append(pipeline.Exporters, exp)
			}
		}

		d.pipelines["metrics"] = pipeline
	}

	return nil
}

func (d *ChronicleOTelDistro) startPipeline(pipeline *Pipeline) error {
	pipeline.mu.Lock()
	defer pipeline.mu.Unlock()

	if pipeline.running {
		return nil
	}

	for _, recv := range pipeline.Receivers {
		if err := recv.Start(d.ctx, d); err != nil {
			return err
		}
	}

	for _, proc := range pipeline.Processors {
		if err := proc.Start(d.ctx, d); err != nil {
			return err
		}
	}

	for _, exp := range pipeline.Exporters {
		if err := exp.Start(d.ctx, d); err != nil {
			return err
		}
	}

	d.wg.Add(1)
	go d.pipelineWorker(pipeline)

	pipeline.running = true
	return nil
}

func (d *ChronicleOTelDistro) stopPipeline(pipeline *Pipeline) {
	pipeline.mu.Lock()
	defer pipeline.mu.Unlock()

	if !pipeline.running {
		return
	}

	close(pipeline.dataChan)
	pipeline.running = false
}

func (d *ChronicleOTelDistro) pipelineWorker(pipeline *Pipeline) {
	defer d.wg.Done()

	for metrics := range pipeline.dataChan {
		// Process through all processors
		var err error
		for _, proc := range pipeline.Processors {
			metrics, err = proc.ProcessMetrics(d.ctx, metrics)
			if err != nil {
				atomic.AddInt64(&d.metrics.Errors, 1)
				continue
			}
		}

		atomic.AddInt64(&d.metrics.MetricsProcessed, 1)

		for _, exp := range pipeline.Exporters {
			if err := exp.ExportMetrics(d.ctx, metrics); err != nil {
				atomic.AddInt64(&d.metrics.Errors, 1)
				atomic.AddInt64(&d.metrics.MetricsDropped, 1)
			} else {
				atomic.AddInt64(&d.metrics.MetricsExported, 1)
			}
		}
	}
}

// PushMetrics pushes metrics to the pipeline.
func (d *ChronicleOTelDistro) PushMetrics(metrics *Metrics) {
	d.mu.RLock()
	pipeline, ok := d.pipelines["metrics"]
	d.mu.RUnlock()

	if !ok {
		return
	}

	// stopPipeline closes dataChan under this same lock. Keep the running
	// check and nonblocking send in one admission interval with shutdown.
	pipeline.mu.Lock()
	defer pipeline.mu.Unlock()
	if !pipeline.running {
		return
	}

	atomic.AddInt64(&d.metrics.MetricsReceived, 1)

	select {
	case pipeline.dataChan <- metrics:
	default:
		atomic.AddInt64(&d.metrics.MetricsDropped, 1)
	}
}

// GetMetrics returns distribution metrics.
func (d *ChronicleOTelDistro) GetMetrics() DistroMetrics {
	d.metrics.mu.RLock()
	defer d.metrics.mu.RUnlock()

	return DistroMetrics{
		ReceiversStarted:  d.metrics.ReceiversStarted,
		ProcessorsStarted: d.metrics.ProcessorsStarted,
		ExportersStarted:  d.metrics.ExportersStarted,
		PipelinesStarted:  d.metrics.PipelinesStarted,
		MetricsReceived:   d.metrics.MetricsReceived,
		MetricsProcessed:  d.metrics.MetricsProcessed,
		MetricsExported:   d.metrics.MetricsExported,
		MetricsDropped:    d.metrics.MetricsDropped,
		Errors:            d.metrics.Errors,
		LastError:         d.metrics.LastError,
		Uptime:            time.Since(d.metrics.StartTime),
		StartTime:         d.metrics.StartTime,
	}
}

// ListConfiguredComponents returns configured components summary.
func (d *ChronicleOTelDistro) ListConfiguredComponents() map[string][]string {
	d.mu.RLock()
	defer d.mu.RUnlock()

	result := make(map[string][]string)

	var receivers []string
	for name := range d.receivers {
		receivers = append(receivers, name)
	}
	sort.Strings(receivers)
	result["receivers"] = receivers

	var processors []string
	for name := range d.processors {
		processors = append(processors, name)
	}
	sort.Strings(processors)
	result["processors"] = processors

	var exporters []string
	for name := range d.exporters {
		exporters = append(exporters, name)
	}
	sort.Strings(exporters)
	result["exporters"] = exporters

	var extensions []string
	for name := range d.extensions {
		extensions = append(extensions, name)
	}
	sort.Strings(extensions)
	result["extensions"] = extensions

	return result
}
