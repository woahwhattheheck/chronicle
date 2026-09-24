// Package main demonstrates Chronicle as a Kubernetes sidecar that scrapes
// Prometheus metrics from a co-located app on localhost and tags them with
// Downward API pod metadata.
//
// Local demo (no cluster required):
//
//	cd examples/kubernetes-sidecar && go run .
//
// Then:
//
//	curl -s http://localhost:8080/health
//	curl -s http://localhost:8080/stats
//	curl -s http://localhost:9090/metrics | head
package main

import (
	"fmt"
	"log"
	"math"
	"math/rand"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/chronicle-db/chronicle"
)

func main() {
	dataDir := envOr("CHRONICLE_DATA_DIR", "./sidecar-data")
	_ = os.MkdirAll(dataDir, 0o755)
	dbPath := dataDir + "/sidecar.db"

	db, err := chronicle.Open(dbPath, chronicle.Config{
		Path:              dbPath,
		MaxMemory:         64 * 1024 * 1024,
		PartitionDuration: time.Hour,
		RetentionDuration: 24 * time.Hour,
		BufferSize:        5000,
		SyncInterval:      2 * time.Second,
		HTTPEnabled:       true,
		HTTPPort:          8086,
	})
	if err != nil {
		log.Fatalf("open db: %v", err)
	}
	defer db.Close()

	// Simulate the application container exposing Prometheus metrics.
	appPort := 9090
	go serveDemoAppMetrics(appPort)

	// Simulate Downward API files when not running inside a pod.
	ensureDemoPodInfo(dataDir)

	cfg := chronicle.DefaultK8sSidecarConfig()
	cfg.MetricsPort = appPort
	cfg.MetricsPath = "/metrics"
	cfg.ScrapeInterval = 2 * time.Second
	cfg.HealthPort = 8080
	cfg.DiscoveryEnabled = false
	cfg.AddPodLabels = true
	cfg.AddNamespace = true
	cfg.AddPodName = true
	cfg.AddNodeName = true

	sidecar, err := chronicle.NewK8sSidecar(db, cfg)
	if err != nil {
		log.Fatalf("sidecar: %v", err)
	}
	if err := sidecar.Start(); err != nil {
		log.Fatalf("start sidecar: %v", err)
	}
	defer sidecar.Stop()

	log.Println("Chronicle Kubernetes sidecar demo running")
	log.Printf("  app metrics:     http://127.0.0.1:%d/metrics", appPort)
	log.Printf("  sidecar health:  http://127.0.0.1:%d/health", cfg.HealthPort)
	log.Printf("  sidecar stats:   http://127.0.0.1:%d/stats", cfg.HealthPort)
	log.Printf("  chronicle HTTP:  http://127.0.0.1:8086/health")
	log.Println("Press Ctrl+C to stop")

	// Periodically show scrape progress.
	go func() {
		t := time.NewTicker(5 * time.Second)
		defer t.Stop()
		for range t.C {
			stats := sidecar.GetStats()
			log.Printf("scrapes=%d points=%d errors=%d targets=%d",
				stats.ScrapeCount, stats.PointsCollected, stats.ScrapeErrors, len(sidecar.GetTargets()))
		}
	}()

	sig := make(chan os.Signal, 1)
	signal.Notify(sig, syscall.SIGINT, syscall.SIGTERM)
	<-sig
	log.Println("shutting down")
}

func serveDemoAppMetrics(port int) {
	mux := http.NewServeMux()
	start := time.Now()
	mux.HandleFunc("/metrics", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "text/plain; version=0.0.4")
		uptime := time.Since(start).Seconds()
		fmt.Fprintf(w, "# HELP demo_up Always 1 while the demo app is healthy\n")
		fmt.Fprintf(w, "# TYPE demo_up gauge\n")
		fmt.Fprintf(w, "demo_up 1\n")
		fmt.Fprintf(w, "# HELP demo_uptime_seconds Process uptime\n")
		fmt.Fprintf(w, "# TYPE demo_uptime_seconds counter\n")
		fmt.Fprintf(w, "demo_uptime_seconds %f\n", uptime)
		fmt.Fprintf(w, "# HELP demo_requests_total Simulated request counter\n")
		fmt.Fprintf(w, "# TYPE demo_requests_total counter\n")
		fmt.Fprintf(w, "demo_requests_total{method=\"GET\",status=\"200\"} %d\n", int(uptime*10)+rand.Intn(5))
		fmt.Fprintf(w, "# HELP demo_cpu_ratio Simulated CPU ratio\n")
		fmt.Fprintf(w, "# TYPE demo_cpu_ratio gauge\n")
		fmt.Fprintf(w, "demo_cpu_ratio %.4f\n", 0.2+0.1*math.Sin(uptime/10))
	})
	addr := fmt.Sprintf(":%d", port)
	log.Printf("demo app listening on %s", addr)
	if err := http.ListenAndServe(addr, mux); err != nil {
		log.Fatalf("demo app: %v", err)
	}
}

func ensureDemoPodInfo(dataDir string) {
	// K8sSidecar reads standard Downward API env vars when present.
	if os.Getenv("POD_NAME") == "" {
		_ = os.Setenv("POD_NAME", "demo-app-0")
	}
	if os.Getenv("POD_NAMESPACE") == "" {
		_ = os.Setenv("POD_NAMESPACE", "default")
	}
	if os.Getenv("NODE_NAME") == "" {
		_ = os.Setenv("NODE_NAME", "kind-control-plane")
	}
	_ = dataDir
}

func envOr(key, fallback string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return fallback
}
