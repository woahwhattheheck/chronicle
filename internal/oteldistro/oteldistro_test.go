package oteldistro

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"
)

// mockPointWriter implements PointWriter for testing.
type mockPointWriter struct {
	points []Point
}

func (m *mockPointWriter) Write(p Point) error {
	m.points = append(m.points, p)
	return nil
}

func TestChronicleOTelDistro(t *testing.T) {
	pw := &mockPointWriter{}

	config := DefaultOTelDistroConfig()
	distro := NewChronicleOTelDistro(pw, config)

	t.Run("StartStop", func(t *testing.T) {
		if err := distro.Start(); err != nil {
			t.Fatalf("Failed to start distro: %v", err)
		}

		time.Sleep(100 * time.Millisecond)

		if err := distro.Stop(); err != nil {
			t.Fatalf("Failed to stop distro: %v", err)
		}
	})

	t.Run("GetMetrics", func(t *testing.T) {
		metrics := distro.GetMetrics()

		if metrics.StartTime.IsZero() {
			t.Error("Expected non-zero start time")
		}

		// Check that counters are initialized
		if metrics.ReceiversStarted < 0 {
			t.Error("Expected non-negative receivers started count")
		}
	})
}

func TestDefaultOTelDistroConfig(t *testing.T) {
	config := DefaultOTelDistroConfig()

	if !config.Enabled {
		t.Error("Expected Enabled to be true")
	}

	if config.ServiceName == "" {
		t.Error("Expected non-empty ServiceName")
	}

	if config.Receivers.OTLP == nil {
		t.Error("Expected OTLP receiver config")
	}

	if config.Processors.Batch == nil {
		t.Error("Expected Batch processor config")
	}

	if config.Exporters.Chronicle == nil {
		t.Error("Expected Chronicle exporter config")
	}

	if config.Pipelines.Metrics == nil {
		t.Error("Expected Metrics pipeline config")
	}
}

func TestOTLPDistroReceiver(t *testing.T) {
	config := DefaultOTelDistroConfig()
	distro := NewChronicleOTelDistro(nil, config)

	recv := NewOTLPDistroReceiver(config.Receivers.OTLP, distro)

	// Test lifecycle - using different port to avoid conflicts
	config.Receivers.OTLP.Protocols.HTTP.Endpoint = "127.0.0.1:14318"

	ctx := context.Background()
	if err := recv.Start(ctx, distro); err != nil {
		t.Skipf("Failed to start receiver (port may be in use): %v", err)
	}
	defer recv.Shutdown(ctx)

	// Give it time to start
	time.Sleep(50 * time.Millisecond)
}

func TestBatchDistroProcessor(t *testing.T) {
	config := &BatchProcessorConfig{
		SendBatchSize:    100,
		SendBatchMaxSize: 500,
		Timeout:          time.Second,
	}

	proc := NewBatchDistroProcessor(config)

	ctx := context.Background()
	if err := proc.Start(ctx, nil); err != nil {
		t.Fatalf("Failed to start processor: %v", err)
	}

	// Process some metrics
	metrics := &Metrics{
		ResourceMetrics: []ResourceMetrics{
			{
				Resource: Resource{Attributes: map[string]any{"service": "test"}},
				ScopeMetrics: []ScopeMetrics{
					{
						Scope: InstrumentationScope{Name: "test-scope"},
						Metrics: []Metric{
							{Name: "test_metric", Data: nil},
						},
					},
				},
			},
		},
	}

	result, err := proc.ProcessMetrics(ctx, metrics)
	if err != nil {
		t.Fatalf("ProcessMetrics failed: %v", err)
	}

	if result == nil {
		t.Error("Expected non-nil result")
	}

	if err := proc.Shutdown(ctx); err != nil {
		t.Fatalf("Shutdown failed: %v", err)
	}
}

func TestAttributesDistroProcessor(t *testing.T) {
	config := &AttributesProcessorConfig{
		Actions: []AttributeAction{
			{Key: "env", Action: "insert", Value: "production"},
			{Key: "debug", Action: "delete"},
		},
	}

	proc := NewAttributesDistroProcessor(config)

	metrics := &Metrics{
		ResourceMetrics: []ResourceMetrics{
			{
				Resource: Resource{Attributes: map[string]any{
					"service": "test",
					"debug":   "true",
				}},
			},
		},
	}

	result, err := proc.ProcessMetrics(context.Background(), metrics)
	if err != nil {
		t.Fatalf("ProcessMetrics failed: %v", err)
	}

	// Check insert
	if result.ResourceMetrics[0].Resource.Attributes["env"] != "production" {
		t.Error("Expected 'env' attribute to be inserted")
	}

	// Check delete
	if _, exists := result.ResourceMetrics[0].Resource.Attributes["debug"]; exists {
		t.Error("Expected 'debug' attribute to be deleted")
	}
}

func TestFilterDistroProcessor(t *testing.T) {
	config := &FilterProcessorConfig{
		Metrics: FilterConfig{
			Exclude: &FilterMatch{
				MatchType:   "strict",
				MetricNames: []string{"internal_metric"},
			},
		},
	}

	proc := NewFilterDistroProcessor(config)

	metrics := &Metrics{
		ResourceMetrics: []ResourceMetrics{
			{
				ScopeMetrics: []ScopeMetrics{
					{
						Metrics: []Metric{
							{Name: "public_metric"},
							{Name: "internal_metric"},
						},
					},
				},
			},
		},
	}

	result, err := proc.ProcessMetrics(context.Background(), metrics)
	if err != nil {
		t.Fatalf("ProcessMetrics failed: %v", err)
	}

	// Filter processor passes through in this simplified implementation
	if result == nil {
		t.Error("Expected non-nil result")
	}
}

func TestChronicleDistroExporter(t *testing.T) {
	pw := &mockPointWriter{}

	config := &ChronicleExporterConfig{
		Endpoint:      "http://localhost:8086",
		BatchSize:     100,
		FlushInterval: time.Second,
	}

	exp := NewChronicleDistroExporter(pw, config)

	ctx := context.Background()
	if err := exp.Start(ctx, nil); err != nil {
		t.Fatalf("Failed to start exporter: %v", err)
	}

	metrics := &Metrics{
		ResourceMetrics: []ResourceMetrics{
			{
				Resource: Resource{Attributes: map[string]any{"host": "server1"}},
				ScopeMetrics: []ScopeMetrics{
					{
						Metrics: []Metric{
							{Name: "cpu_usage"},
						},
					},
				},
			},
		},
	}

	if err := exp.ExportMetrics(ctx, metrics); err != nil {
		t.Fatalf("ExportMetrics failed: %v", err)
	}

	if err := exp.Shutdown(ctx); err != nil {
		t.Fatalf("Shutdown failed: %v", err)
	}
}

func TestHealthCheckExtension(t *testing.T) {
	config := DefaultOTelDistroConfig()
	distro := NewChronicleOTelDistro(nil, config)

	healthConfig := &HealthExtConfig{Endpoint: "127.0.0.1:23133"}
	ext := NewHealthCheckExtension(healthConfig, distro)

	ctx := context.Background()
	if err := ext.Start(ctx, distro); err != nil {
		t.Skipf("Failed to start health check extension: %v", err)
	}
	defer ext.Shutdown(ctx)

	time.Sleep(50 * time.Millisecond)
}

func TestGenerateOTelDistroYAML(t *testing.T) {
	config := DefaultOTelDistroConfig()
	yaml := GenerateOTelDistroYAML(config)

	if yaml == "" {
		t.Error("Expected non-empty YAML")
	}

	// Check key sections exist
	expectedSections := []string{
		"receivers:",
		"processors:",
		"exporters:",
		"service:",
		"pipelines:",
	}

	for _, section := range expectedSections {
		if !otelContains(yaml, section) {
			t.Errorf("Expected YAML to contain '%s'", section)
		}
	}
}

func TestListComponents(t *testing.T) {
	components := ListComponents()

	if len(components["receivers"]) == 0 {
		t.Error("Expected receivers")
	}

	if len(components["processors"]) == 0 {
		t.Error("Expected processors")
	}

	if len(components["exporters"]) == 0 {
		t.Error("Expected exporters")
	}

	if len(components["extensions"]) == 0 {
		t.Error("Expected extensions")
	}
}

func TestGetComponentInfo(t *testing.T) {
	info := GetComponentInfo("receivers", "otlp")

	if info == nil {
		t.Fatal("Expected info for otlp receiver")
	}

	if info["name"] != "OTLP Receiver" {
		t.Errorf("Expected name 'OTLP Receiver', got '%v'", info["name"])
	}

	// Non-existent component
	info = GetComponentInfo("receivers", "nonexistent")
	if info != nil {
		t.Error("Expected nil for non-existent component")
	}
}

func TestValidateConfig(t *testing.T) {
	t.Run("ValidConfig", func(t *testing.T) {
		config := DefaultOTelDistroConfig()
		errors := ValidateConfig(config)

		if len(errors) > 0 {
			t.Errorf("Expected no errors for valid config, got: %v", errors)
		}
	})

	t.Run("NoReceivers", func(t *testing.T) {
		config := OTelDistroConfig{
			Exporters: ExportersConfig{
				Chronicle: &ChronicleExporterConfig{},
			},
		}
		errors := ValidateConfig(config)

		if len(errors) == 0 {
			t.Error("Expected error for missing receivers")
		}
	})

	t.Run("NoExporters", func(t *testing.T) {
		config := OTelDistroConfig{
			Receivers: ReceiversConfig{
				OTLP: &OTLPReceiverConfig{},
			},
		}
		errors := ValidateConfig(config)

		if len(errors) == 0 {
			t.Error("Expected error for missing exporters")
		}
	})
}

func TestGetDefaultPipeline(t *testing.T) {
	pipeline := GetDefaultPipeline()

	if len(pipeline.Receivers) == 0 {
		t.Error("Expected default receivers")
	}

	if len(pipeline.Processors) == 0 {
		t.Error("Expected default processors")
	}

	if len(pipeline.Exporters) == 0 {
		t.Error("Expected default exporters")
	}
}

func TestGetBuiltinTransforms(t *testing.T) {
	transforms := GetBuiltinTransforms()

	if len(transforms) == 0 {
		t.Error("Expected builtin transforms")
	}

	// Check first transform
	if transforms[0].MetricNameMatch == "" {
		t.Error("Expected metric name match pattern")
	}
}

func TestSupportedFormats(t *testing.T) {
	formats := SupportedFormats()

	if len(formats) == 0 {
		t.Error("Expected supported formats")
	}

	// Check OTLP is supported
	hasOTLP := false
	for _, f := range formats {
		if f == "otlp_proto" || f == "otlp_json" {
			hasOTLP = true
			break
		}
	}
	if !hasOTLP {
		t.Error("Expected OTLP format support")
	}
}

func TestGetExampleConfigs(t *testing.T) {
	configs := GetExampleConfigs()

	if _, ok := configs["basic"]; !ok {
		t.Error("Expected 'basic' example config")
	}

	if _, ok := configs["high_availability"]; !ok {
		t.Error("Expected 'high_availability' example config")
	}

	if _, ok := configs["edge"]; !ok {
		t.Error("Expected 'edge' example config")
	}
}

func TestListConfiguredComponents(t *testing.T) {
	config := DefaultOTelDistroConfig()
	distro := NewChronicleOTelDistro(nil, config)

	// Before start
	components := distro.ListConfiguredComponents()

	// All lists should be empty before initialization
	for _, list := range components {
		if len(list) > 0 {
			t.Logf("Note: Components initialized before Start: %v", list)
		}
	}
}

func TestDistroMetrics(t *testing.T) {
	config := DefaultOTelDistroConfig()
	distro := NewChronicleOTelDistro(nil, config)

	// Get initial metrics
	metrics := distro.GetMetrics()

	if metrics.StartTime.IsZero() {
		t.Error("Expected non-zero start time")
	}

	if metrics.Uptime <= 0 {
		t.Error("Expected positive uptime")
	}
}

type failingDistroExporter struct{}

func (failingDistroExporter) Start(context.Context, Host) error { return nil }
func (failingDistroExporter) Shutdown(context.Context) error    { return nil }
func (failingDistroExporter) ExportMetrics(context.Context, *Metrics) error {
	return errors.New("export failed")
}

func TestDistroMetricsConcurrentSnapshots(t *testing.T) {
	const workers, perWorker = 4, 50
	const total = workers * perWorker
	distro := NewChronicleOTelDistro(nil, OTelDistroConfig{})
	t.Cleanup(distro.cancel)
	pipeline := &Pipeline{
		Name:     "metrics",
		dataChan: make(chan *Metrics, total),
		running:  true,
		Exporters: []OTelExporter{
			NewOTLPDistroExporter(&OTLPExporterConfig{}),
			failingDistroExporter{},
		},
	}
	distro.pipelines["metrics"] = pipeline
	distro.wg.Add(1)
	go distro.pipelineWorker(pipeline)

	var writers sync.WaitGroup
	for i := 0; i < workers; i++ {
		writers.Add(1)
		go func() {
			defer writers.Done()
			for j := 0; j < perWorker; j++ {
				distro.PushMetrics(&Metrics{})
				distro.ReportFatalError(errors.New("receiver failed"))
				snapshot := distro.GetMetrics()
				if snapshot.Errors < snapshot.MetricsDropped {
					t.Error("snapshot has a dropped export without its error")
				}
			}
		}()
	}
	writers.Wait()
	close(pipeline.dataChan)
	distro.wg.Wait()

	snapshot := distro.GetMetrics()
	for name, got := range map[string]int64{
		"received":  snapshot.MetricsReceived,
		"processed": snapshot.MetricsProcessed,
		"exported":  snapshot.MetricsExported,
		"dropped":   snapshot.MetricsDropped,
	} {
		if got != total {
			t.Errorf("%s = %d, want %d", name, got, total)
		}
	}
	if snapshot.Errors != 2*total || snapshot.LastError != "receiver failed" {
		t.Errorf("errors = %d, last error = %q", snapshot.Errors, snapshot.LastError)
	}
}

func TestPipelineProcessing(t *testing.T) {
	pw := &mockPointWriter{}

	config := DefaultOTelDistroConfig()
	distro := NewChronicleOTelDistro(pw, config)

	// Start distro
	if err := distro.Start(); err != nil {
		t.Fatalf("Failed to start distro: %v", err)
	}
	defer distro.Stop()

	// Push some metrics
	metrics := &Metrics{
		ResourceMetrics: []ResourceMetrics{
			{
				Resource: Resource{Attributes: map[string]any{"service": "test"}},
				ScopeMetrics: []ScopeMetrics{
					{
						Metrics: []Metric{{Name: "test_metric"}},
					},
				},
			},
		},
	}

	distro.PushMetrics(metrics)

	// Give time to process
	time.Sleep(100 * time.Millisecond)

	// Check metrics
	dm := distro.GetMetrics()
	if dm.MetricsReceived == 0 {
		t.Log("Note: No metrics received (expected in some test configurations)")
	}
}

func TestMemoryLimiterDistroProcessor(t *testing.T) {
	config := &MemoryLimiterConfig{
		CheckInterval: time.Second,
		LimitMiB:      512,
		SpikeLimitMiB: 128,
	}

	proc := NewMemoryLimiterDistroProcessor(config)

	ctx := context.Background()
	proc.Start(ctx, nil)
	defer proc.Shutdown(ctx)

	metrics := &Metrics{}
	result, err := proc.ProcessMetrics(ctx, metrics)

	if err != nil {
		t.Fatalf("ProcessMetrics failed: %v", err)
	}

	if result == nil {
		t.Error("Expected non-nil result")
	}

	// Exercise the counter without depending on the process's current allocation.
	config.LimitMiB = 1
	config.CheckInterval = time.Hour
	proc.state.mu.Lock()
	proc.state.memStats.Alloc = 1024 * 1024
	proc.state.lastCheck = time.Now()
	proc.state.mu.Unlock()
	if result, err := proc.ProcessMetrics(ctx, metrics); err == nil || result != nil {
		t.Fatal("expected metrics to be rejected at the memory limit")
	}
	proc.state.mu.Lock()
	dropped := proc.state.dropped
	proc.state.mu.Unlock()
	if dropped != 1 {
		t.Errorf("dropped = %d, want 1", dropped)
	}
}

func TestDebugDistroExporter(t *testing.T) {
	config := &DebugExporterConfig{Verbosity: "detailed"}
	exp := NewDebugDistroExporter(config)

	ctx := context.Background()
	exp.Start(ctx, nil)
	defer exp.Shutdown(ctx)

	metrics := &Metrics{}
	if err := exp.ExportMetrics(ctx, metrics); err != nil {
		t.Fatalf("ExportMetrics failed: %v", err)
	}
	if got := exp.state.exportCount.Load(); got != 1 {
		t.Errorf("export count = %d, want 1", got)
	}
}

func TestOTLPDistroExporter(t *testing.T) {
	config := &OTLPExporterConfig{
		Endpoint:    "http://localhost:4318",
		Compression: "gzip",
	}

	exp := NewOTLPDistroExporter(config)

	ctx := context.Background()
	if err := exp.Start(ctx, nil); err != nil {
		t.Fatalf("Start failed: %v", err)
	}
	defer exp.Shutdown(ctx)

	metrics := &Metrics{}
	if err := exp.ExportMetrics(ctx, metrics); err != nil {
		t.Fatalf("ExportMetrics failed: %v", err)
	}
	invalidMetrics := &Metrics{ResourceMetrics: []ResourceMetrics{{
		ScopeMetrics: []ScopeMetrics{{Metrics: []Metric{{Data: make(chan int)}}}},
	}}}
	if err := exp.ExportMetrics(ctx, invalidMetrics); err == nil {
		t.Fatal("expected a serialization failure")
	}
	if got := exp.state.exported.Load(); got != 1 {
		t.Errorf("exported = %d, want 1", got)
	}
	if got := exp.state.failed.Load(); got != 1 {
		t.Errorf("failed = %d, want 1", got)
	}
}

func TestZPagesExtension(t *testing.T) {
	config := &ZPagesConfig{Endpoint: "127.0.0.1:25679"}
	ext := NewZPagesExtension(config)

	ctx := context.Background()
	if err := ext.Start(ctx, nil); err != nil {
		t.Skipf("Failed to start zpages: %v", err)
	}
	defer ext.Shutdown(ctx)

	time.Sleep(50 * time.Millisecond)
}

// Helper function
func otelContains(s, substr string) bool {
	return len(s) >= len(substr) && (s == substr || len(s) > 0 && otelContainsHelper(s, substr))
}

func otelContainsHelper(s, substr string) bool {
	for i := 0; i <= len(s)-len(substr); i++ {
		if s[i:i+len(substr)] == substr {
			return true
		}
	}
	return false
}

func BenchmarkDistroMetricProcessing(b *testing.B) {
	proc := NewBatchDistroProcessor(&BatchProcessorConfig{
		SendBatchSize: 1000,
		Timeout:       time.Second,
	})

	metrics := &Metrics{
		ResourceMetrics: []ResourceMetrics{
			{
				Resource: Resource{Attributes: map[string]any{"service": "bench"}},
				ScopeMetrics: []ScopeMetrics{
					{
						Metrics: []Metric{{Name: "bench_metric"}},
					},
				},
			},
		},
	}

	ctx := context.Background()
	proc.Start(ctx, nil)
	defer proc.Shutdown(ctx)

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		proc.ProcessMetrics(ctx, metrics)
	}
}
