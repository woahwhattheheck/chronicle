// Package main demonstrates Chronicle as a Kubernetes sidecar that scrapes
// Prometheus metrics from a co-located app and adds Downward API pod metadata.
//
// Run both components locally with go run ., or use -mode=app and -mode=sidecar
// in separate containers sharing the same pod network.
package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"log"
	"math"
	"net"
	"net/http"
	"net/url"
	"os"
	"os/signal"
	"path/filepath"
	"strings"
	"sync/atomic"
	"syscall"
	"time"

	"github.com/chronicle-db/chronicle"
)

type options struct {
	mode           string
	dataDir        string
	metricsPort    int
	metricsPath    string
	healthPort     int
	httpPort       int
	scrapeInterval time.Duration
	scrapeTimeout  time.Duration
	retention      time.Duration
}

func parseOptions(args []string) (options, error) {
	var cfg options
	flags := flag.NewFlagSet("chronicle-sidecar-example", flag.ContinueOnError)
	flags.StringVar(&cfg.mode, "mode", "demo", "demo (both components), app, or sidecar")
	flags.StringVar(&cfg.dataDir, "data-dir", envOr("CHRONICLE_DATA_DIR", "./sidecar-data"), "Chronicle data directory")
	flags.IntVar(&cfg.metricsPort, "metrics-port", 9090, "application metrics port")
	flags.StringVar(&cfg.metricsPath, "metrics-path", "/metrics", "application metrics path")
	flags.IntVar(&cfg.healthPort, "health-port", 8080, "sidecar health and statistics port")
	flags.IntVar(&cfg.httpPort, "http-port", 8086, "Chronicle query API port")
	flags.DurationVar(&cfg.scrapeInterval, "scrape-interval", 2*time.Second, "metrics collection interval")
	flags.DurationVar(&cfg.scrapeTimeout, "scrape-timeout", 10*time.Second, "timeout for a metrics request")
	flags.DurationVar(&cfg.retention, "retention", 24*time.Hour, "local metrics retention")
	if err := flags.Parse(args); err != nil {
		return cfg, err
	}
	if flags.NArg() != 0 {
		return cfg, fmt.Errorf("unexpected arguments: %s", strings.Join(flags.Args(), " "))
	}
	if cfg.mode != "demo" && cfg.mode != "app" && cfg.mode != "sidecar" {
		return cfg, fmt.Errorf("mode must be demo, app, or sidecar; got %q", cfg.mode)
	}
	for name, port := range map[string]int{
		"metrics-port": cfg.metricsPort,
		"health-port":  cfg.healthPort,
		"http-port":    cfg.httpPort,
	} {
		if port < 1 || port > 65535 {
			return cfg, fmt.Errorf("%s must be between 1 and 65535", name)
		}
	}
	if !strings.HasPrefix(cfg.metricsPath, "/") || strings.ContainsAny(cfg.metricsPath, "?#") {
		return cfg, errors.New("metrics-path must be an absolute URL path without a query or fragment")
	}
	if _, err := url.ParseRequestURI(cfg.metricsPath); err != nil {
		return cfg, fmt.Errorf("invalid metrics-path: %w", err)
	}
	if cfg.scrapeInterval <= 0 || cfg.scrapeTimeout <= 0 || cfg.retention <= 0 {
		return cfg, errors.New("scrape-interval, scrape-timeout, and retention must be positive")
	}
	if cfg.mode != "app" && (cfg.metricsPort == cfg.healthPort ||
		cfg.metricsPort == cfg.httpPort || cfg.healthPort == cfg.httpPort) {
		return cfg, errors.New("metrics-port, health-port, and http-port must be different")
	}
	if cfg.dataDir == "" {
		return cfg, errors.New("data-dir must not be empty")
	}
	return cfg, nil
}

func main() {
	cfg, err := parseOptions(os.Args[1:])
	if errors.Is(err, flag.ErrHelp) {
		return
	}
	if err != nil {
		log.Printf("configuration: %v", err)
		os.Exit(2)
	}
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	if err := run(ctx, cfg); err != nil {
		log.Printf("sidecar example: %v", err)
		os.Exit(1)
	}
}

func run(ctx context.Context, cfg options) (result error) {
	var appErrors <-chan error
	if cfg.mode == "demo" || cfg.mode == "app" {
		server, failures, err := startDemoAppMetrics(cfg.metricsPort, cfg.metricsPath)
		if err != nil {
			return fmt.Errorf("start application metrics: %w", err)
		}
		appErrors = failures
		defer func() {
			shutdown, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			defer cancel()
			result = errors.Join(result, server.Shutdown(shutdown))
		}()
		log.Printf("application metrics: http://127.0.0.1:%d%s", cfg.metricsPort, cfg.metricsPath)
	}

	var sidecar *chronicle.K8sSidecar
	var progress <-chan time.Time
	if cfg.mode != "app" {
		if err := os.MkdirAll(cfg.dataDir, 0o755); err != nil {
			return fmt.Errorf("create data directory: %w", err)
		}
		dbPath := filepath.Join(cfg.dataDir, "sidecar.db")
		db, err := chronicle.Open(dbPath, chronicle.Config{
			Path:              dbPath,
			MaxMemory:         64 * 1024 * 1024,
			PartitionDuration: time.Hour,
			RetentionDuration: cfg.retention,
			BufferSize:        5000,
			SyncInterval:      2 * time.Second,
			HTTPEnabled:       true,
			HTTPPort:          cfg.httpPort,
		})
		if err != nil {
			return fmt.Errorf("open db: %w", err)
		}
		defer func() { result = errors.Join(result, db.Close()) }()

		if cfg.mode == "demo" {
			ensureDemoPodInfo()
		}
		sidecarConfig := chronicle.DefaultK8sSidecarConfig()
		sidecarConfig.MetricsPort = cfg.metricsPort
		sidecarConfig.MetricsPath = cfg.metricsPath
		sidecarConfig.ScrapeInterval = cfg.scrapeInterval
		sidecarConfig.ScrapeTimeout = cfg.scrapeTimeout
		sidecarConfig.RetentionDuration = cfg.retention
		sidecarConfig.HealthPort = cfg.healthPort
		sidecarConfig.DiscoveryEnabled = false
		sidecar, err = chronicle.NewK8sSidecar(db, sidecarConfig)
		if err != nil {
			return fmt.Errorf("configure sidecar: %w", err)
		}
		if err := sidecar.Start(); err != nil {
			return fmt.Errorf("start sidecar: %w", err)
		}
		defer func() { result = errors.Join(result, sidecar.Stop()) }()

		log.Printf("sidecar health/stats: http://127.0.0.1:%d", cfg.healthPort)
		log.Printf("Chronicle HTTP API: http://127.0.0.1:%d", cfg.httpPort)
		ticker := time.NewTicker(5 * time.Second)
		defer ticker.Stop()
		progress = ticker.C
	}

	log.Printf("Chronicle example running in %s mode", cfg.mode)
	for {
		select {
		case <-ctx.Done():
			log.Println("shutting down")
			return nil
		case err := <-appErrors:
			return fmt.Errorf("application metrics server: %w", err)
		case <-progress:
			stats := sidecar.GetStats()
			log.Printf("scrapes=%d points=%d errors=%d targets=%d",
				stats.ScrapeCount, stats.PointsCollected, stats.ScrapeErrors, len(sidecar.GetTargets()))
		}
	}
}

func startDemoAppMetrics(port int, path string) (*http.Server, <-chan error, error) {
	metricsURL, err := url.ParseRequestURI(path)
	if err != nil {
		return nil, nil, fmt.Errorf("invalid metrics-path: %w", err)
	}
	start := time.Now()
	var requests atomic.Int64
	// The configured endpoint is a literal URL path, not a ServeMux pattern.
	handler := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != metricsURL.Path {
			http.NotFound(w, r)
			return
		}
		w.Header().Set("Content-Type", "text/plain; version=0.0.4")
		uptime := time.Since(start).Seconds()
		fmt.Fprintln(w, "# HELP demo_up Always 1 while the demo app is healthy")
		fmt.Fprintln(w, "# TYPE demo_up gauge")
		fmt.Fprintln(w, "demo_up 1")
		fmt.Fprintln(w, "# HELP demo_uptime_seconds Process uptime")
		fmt.Fprintln(w, "# TYPE demo_uptime_seconds counter")
		fmt.Fprintf(w, "demo_uptime_seconds %f\n", uptime)
		fmt.Fprintln(w, "# HELP demo_requests_total Metrics requests served by the example application")
		fmt.Fprintln(w, "# TYPE demo_requests_total counter")
		fmt.Fprintf(w, "demo_requests_total{method=\"GET\",status=\"200\"} %d\n", requests.Add(1))
		fmt.Fprintln(w, "# HELP demo_cpu_ratio Simulated CPU ratio")
		fmt.Fprintln(w, "# TYPE demo_cpu_ratio gauge")
		fmt.Fprintf(w, "demo_cpu_ratio %.4f\n", 0.2+0.1*math.Sin(uptime/10))
	})
	listener, err := net.Listen("tcp", fmt.Sprintf(":%d", port))
	if err != nil {
		return nil, nil, err
	}
	server := &http.Server{
		Addr:              listener.Addr().String(),
		Handler:           handler,
		ReadHeaderTimeout: 5 * time.Second,
	}
	failures := make(chan error, 1)
	go func() {
		if err := server.Serve(listener); err != nil && !errors.Is(err, http.ErrServerClosed) {
			failures <- err
		}
	}()
	return server, failures, nil
}

func ensureDemoPodInfo() {
	if os.Getenv("POD_NAME") == "" {
		_ = os.Setenv("POD_NAME", "demo-app-0")
	}
	if os.Getenv("POD_NAMESPACE") == "" {
		_ = os.Setenv("POD_NAMESPACE", "default")
	}
	if os.Getenv("NODE_NAME") == "" {
		_ = os.Setenv("NODE_NAME", "local-demo")
	}
}

func envOr(key, fallback string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return fallback
}
