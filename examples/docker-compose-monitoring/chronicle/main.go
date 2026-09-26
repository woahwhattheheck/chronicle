// Package main runs Chronicle behind a container-facing HTTP listener.
// Chronicle's embedded HTTP server listens on loopback, so this example
// forwards container traffic to that server and exports scrapeable metrics.
package main

import (
	"context"
	"fmt"
	"log"
	"net/http"
	"net/http/httputil"
	"net/url"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/chronicle-db/chronicle"
)

func main() {
	if len(os.Args) == 2 && os.Args[1] == "health" {
		client := &http.Client{Timeout: 2 * time.Second}
		resp, err := client.Get("http://127.0.0.1:8086/health")
		if err != nil {
			log.Fatal(err)
		}
		defer resp.Body.Close()
		if resp.StatusCode != http.StatusOK {
			log.Fatalf("health status: %s", resp.Status)
		}
		return
	}

	const dbPath = "/data/chronicle.db"
	cfg := chronicle.DefaultConfig(dbPath)
	cfg.HTTP.HTTPEnabled = true
	cfg.HTTP.HTTPPort = 8087
	db, err := chronicle.Open(dbPath, cfg)
	if err != nil {
		log.Fatal(err)
	}
	defer db.Close()

	target := &url.URL{Scheme: "http", Host: "127.0.0.1:8087"}
	proxy := httputil.NewSingleHostReverseProxy(target)
	mux := http.NewServeMux()
	mux.HandleFunc("GET /metrics", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "text/plain; version=0.0.4")
		fmt.Fprintln(w, "# HELP chronicle_up Chronicle database process is running.")
		fmt.Fprintln(w, "# TYPE chronicle_up gauge")
		fmt.Fprintln(w, "chronicle_up 1")
		fmt.Fprintln(w, "# HELP chronicle_metric_count Number of metric names in Chronicle.")
		fmt.Fprintln(w, "# TYPE chronicle_metric_count gauge")
		fmt.Fprintf(w, "chronicle_metric_count %d\n", len(db.Metrics()))
	})
	mux.Handle("/", proxy)

	server := &http.Server{
		Addr:              ":8086",
		Handler:           mux,
		ReadHeaderTimeout: 5 * time.Second,
	}
	errCh := make(chan error, 1)
	go func() { errCh <- server.ListenAndServe() }()
	log.Println("Chronicle monitoring example listening on :8086")

	stop := make(chan os.Signal, 1)
	signal.Notify(stop, syscall.SIGINT, syscall.SIGTERM)
	select {
	case sig := <-stop:
		log.Printf("shutting down after %s", sig)
	case err := <-errCh:
		log.Fatalf("HTTP listener: %v", err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := server.Shutdown(ctx); err != nil {
		log.Printf("HTTP shutdown: %v", err)
	}
}
