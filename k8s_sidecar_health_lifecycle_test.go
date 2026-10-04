package chronicle

import (
	"context"
	"fmt"
	"net"
	"net/http"
	"testing"
	"time"
)

func TestK8sSidecarHealthBindFailureAndRetry(t *testing.T) {
	occupied, err := net.Listen("tcp", ":0")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = occupied.Close() })
	port := occupied.Addr().(*net.TCPAddr).Port

	config := DefaultK8sSidecarConfig()
	config.HealthPort = port
	config.ScrapeInterval = time.Hour
	config.DiscoveryEnabled = false
	// No scrape can run during this lifecycle check; the real database is not needed.
	sidecar, err := NewK8sSidecar(nil, config)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = sidecar.Stop() })

	if err := sidecar.Start(); err == nil {
		t.Fatal("Start accepted an occupied health port")
	}
	if sidecar.running.Load() {
		t.Fatal("failed Start left the sidecar running")
	}
	if err := sidecar.ctx.Err(); err != nil {
		t.Fatalf("failed Start canceled the context needed for retry: %v", err)
	}
	if err := occupied.Close(); err != nil {
		t.Fatal(err)
	}
	if err := sidecar.Start(); err != nil {
		t.Fatalf("retry after releasing the health port: %v", err)
	}
	if !sidecar.running.Load() {
		t.Fatal("successful retry did not mark the sidecar running")
	}

	transport := &http.Transport{Proxy: nil}
	defer transport.CloseIdleConnections()
	client := &http.Client{Transport: transport, Timeout: 2 * time.Second}
	for _, path := range []string{"/health", "/api/v1/sidecar/health", "/stats", "/targets", "/api/v1/sidecar/metrics"} {
		t.Run(path, func(t *testing.T) {
			response, err := client.Get(fmt.Sprintf("http://127.0.0.1:%d%s", port, path))
			if err != nil {
				t.Fatal(err)
			}
			defer response.Body.Close()
			if response.StatusCode != http.StatusOK {
				t.Fatalf("status = %d, want 200", response.StatusCode)
			}
		})
	}
	if err := sidecar.Start(); err == nil {
		t.Fatal("duplicate Start succeeded")
	}
	if !sidecar.running.Load() {
		t.Fatal("duplicate Start cleared the running flag")
	}
	if err := sidecar.Stop(); err != nil {
		t.Fatal(err)
	}
	if sidecar.running.Load() {
		t.Fatal("Stop left the sidecar running")
	}
	if err := sidecar.ctx.Err(); err != context.Canceled {
		t.Fatalf("context after Stop = %v, want context.Canceled", err)
	}
	if err := sidecar.Stop(); err != nil {
		t.Fatalf("second Stop: %v", err)
	}
	rebound, err := net.Listen("tcp", fmt.Sprintf(":%d", port))
	if err != nil {
		t.Fatalf("health listener leaked after Stop: %v", err)
	}
	_ = rebound.Close()
}
