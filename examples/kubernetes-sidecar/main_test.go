package main

import (
	"context"
	"io"
	"net"
	"net/http"
	"strconv"
	"strings"
	"testing"
	"time"
)

func TestParseOptionsModes(t *testing.T) {
	t.Setenv("CHRONICLE_DATA_DIR", "/tmp/chronicle-example")
	for _, mode := range []string{"demo", "app", "sidecar"} {
		cfg, err := parseOptions([]string{"-mode=" + mode, "-metrics-path=/custom", "-scrape-interval=15s"})
		if err != nil {
			t.Fatal(err)
		}
		if cfg.mode != mode || cfg.metricsPath != "/custom" || cfg.scrapeInterval != 15*time.Second || cfg.dataDir != "/tmp/chronicle-example" {
			t.Fatalf("unexpected configuration: %+v", cfg)
		}
	}
	cfg, err := parseOptions(nil)
	if err != nil || cfg.mode != "demo" {
		t.Fatalf("default mode = %q, err = %v; want demo", cfg.mode, err)
	}
}

func TestParseOptionsRejectsUnusableConfiguration(t *testing.T) {
	for _, arg := range []string{
		"-mode=unknown", "-metrics-port=0", "-health-port=65536",
		"-http-port=9090", "-metrics-path=metrics", "-metrics-path=/metrics?x=1",
		"-scrape-interval=0", "-scrape-timeout=-1s", "-retention=0", "extra-argument",
	} {
		t.Run(arg, func(t *testing.T) {
			if _, err := parseOptions([]string{arg}); err == nil {
				t.Fatalf("accepted unusable configuration %q", arg)
			}
		})
	}
}

func TestAppServesMetricsAndReleasesPort(t *testing.T) {
	server, _, err := startDemoAppMetrics(0, "/custom-metrics")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = server.Close() })
	_, port, err := net.SplitHostPort(server.Addr)
	if err != nil {
		t.Fatal(err)
	}
	client := &http.Client{Timeout: 2 * time.Second}
	for i := 1; i <= 2; i++ {
		resp, err := client.Get("http://127.0.0.1:" + port + "/custom-metrics")
		if err != nil {
			t.Fatal(err)
		}
		body, err := io.ReadAll(resp.Body)
		_ = resp.Body.Close()
		if err != nil {
			t.Fatal(err)
		}
		counter := "demo_requests_total{method=\"GET\",status=\"200\"} " + strconv.Itoa(i) + "\n"
		if resp.StatusCode != http.StatusOK || !strings.Contains(string(body), "demo_up 1\n") || !strings.Contains(string(body), counter) {
			t.Fatalf("unexpected metrics response %d: %s", resp.StatusCode, body)
		}
	}
	portNumber, err := strconv.Atoi(port)
	if err != nil {
		t.Fatal(err)
	}
	if duplicate, _, err := startDemoAppMetrics(portNumber, "/metrics"); err == nil {
		_ = duplicate.Close()
		t.Fatal("starting a second app on the same port succeeded")
	}
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	if err := server.Shutdown(ctx); err != nil {
		t.Fatal(err)
	}
	replacement, _, err := startDemoAppMetrics(portNumber, "/metrics")
	if err != nil {
		t.Fatalf("port was not released: %v", err)
	}
	_ = replacement.Close()
}

func TestDemoPodInfoPreservesDownwardAPI(t *testing.T) {
	t.Setenv("POD_NAME", "existing-pod")
	t.Setenv("POD_NAMESPACE", "existing-namespace")
	t.Setenv("NODE_NAME", "existing-node")
	ensureDemoPodInfo()
	for key, expected := range map[string]string{
		"POD_NAME": "existing-pod", "POD_NAMESPACE": "existing-namespace", "NODE_NAME": "existing-node",
	} {
		if got := envOr(key, ""); got != expected {
			t.Fatalf("%s = %q, want %q", key, got, expected)
		}
	}
}
