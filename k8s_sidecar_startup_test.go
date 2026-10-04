package chronicle

import (
	"encoding/json"
	"errors"
	"fmt"
	"net"
	"net/http"
	"testing"
	"time"
)

func TestK8sSidecar_HealthBindFailureAndRetry(t *testing.T) {
	occupied, err := net.Listen("tcp", ":0")
	if err != nil {
		t.Fatal(err)
	}
	defer occupied.Close()

	config := DefaultK8sSidecarConfig()
	config.HealthPort = occupied.Addr().(*net.TCPAddr).Port
	config.ScrapeInterval = time.Hour
	config.DiscoveryEnabled = false
	sidecar, err := NewK8sSidecar(nil, config)
	if err != nil {
		t.Fatal(err)
	}
	defer sidecar.cancel()
	defer sidecar.Stop()

	err = sidecar.Start()
	if err == nil {
		t.Fatal("Start succeeded with an occupied health port")
	}
	var bindError *net.OpError
	if !errors.As(err, &bindError) || bindError.Op != "listen" {
		t.Fatalf("Start did not preserve the listen error: %v", err)
	}
	if sidecar.running.Load() {
		t.Fatal("failed Start left sidecar running")
	}
	if sidecar.ctx.Err() != nil {
		t.Fatal("failed Start canceled the context needed for retry")
	}

	// A failed bind must not leave a scrape/discovery worker behind.
	workersDone := make(chan struct{})
	go func() {
		sidecar.wg.Wait()
		close(workersDone)
	}()
	select {
	case <-workersDone:
	case <-time.After(time.Second):
		t.Fatal("failed Start left background workers running")
	}

	if err := occupied.Close(); err != nil {
		t.Fatal(err)
	}
	if err := sidecar.Start(); err != nil {
		t.Fatalf("retry after releasing the health port: %v", err)
	}
	assertSidecarHealthResponse(t, config.HealthPort, "/health")
	assertSidecarHealthResponse(t, config.HealthPort, "/api/v1/sidecar/health")
	if err := sidecar.Stop(); err != nil {
		t.Fatal(err)
	}
	if sidecar.running.Load() {
		t.Fatal("Stop left sidecar running")
	}

	// Stop must release the real listener, not only flip the running flag.
	rebound, err := net.Listen("tcp", fmt.Sprintf(":%d", config.HealthPort))
	if err != nil {
		t.Fatalf("health listener still held after Stop: %v", err)
	}
	rebound.Close()
}

func TestK8sSidecar_HealthRejectsDuplicateStart(t *testing.T) {
	config := DefaultK8sSidecarConfig()
	config.HealthPort = 0
	config.ScrapeInterval = time.Hour
	config.DiscoveryEnabled = false
	sidecar, err := NewK8sSidecar(nil, config)
	if err != nil {
		t.Fatal(err)
	}
	defer sidecar.cancel()
	defer sidecar.Stop()
	if err := sidecar.Start(); err != nil {
		t.Fatal(err)
	}
	originalServer := sidecar.server
	if err := sidecar.Start(); err == nil {
		t.Fatal("duplicate Start succeeded")
	}
	if !sidecar.running.Load() || sidecar.server != originalServer {
		t.Fatal("duplicate Start replaced or stopped the original server")
	}
	if err := sidecar.Stop(); err != nil {
		t.Fatal(err)
	}
	if err := sidecar.Stop(); err != nil {
		t.Fatalf("second Stop: %v", err)
	}
}

func assertSidecarHealthResponse(t *testing.T, port int, path string) {
	t.Helper()
	client := &http.Client{Timeout: time.Second}
	response, err := client.Get(fmt.Sprintf("http://127.0.0.1:%d%s", port, path))
	if err != nil {
		t.Fatalf("health listener did not serve: %v", err)
	}
	defer response.Body.Close()
	if response.StatusCode != http.StatusOK {
		t.Fatalf("health status: %d", response.StatusCode)
	}
	var body map[string]string
	if err := json.NewDecoder(response.Body).Decode(&body); err != nil {
		t.Fatal(err)
	}
	if body["status"] != "healthy" {
		t.Fatalf("health response: %v", body)
	}
}
