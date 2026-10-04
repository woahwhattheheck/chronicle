package chronicle

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"net"
	"net/http"
	"net/http/pprof"
	"time"
)

func startHTTPServer(db *DB, port int) (*httpServer, error) {
	if port <= 0 || port > 65535 {
		port = 8086
	}

	// Rate limiter from config or default
	rateLimit := db.config.RateLimitPerSecond
	if rateLimit <= 0 {
		rateLimit = 1000
	}
	var rl *rateLimiter
	if rateLimit > 0 {
		rl = newRateLimiter(rateLimit, time.Second)
	}

	// Authentication from config
	auth := newAuthenticator(db.config.Auth)

	// HTTP request metrics
	httpMetrics := NewHTTPMetrics()

	// Helper to wrap handlers with middleware
	wrap := func(h http.HandlerFunc) http.HandlerFunc {
		h = securityHeadersMiddleware(h)
		h = csrfProtectionMiddleware(h)
		h = bodySizeLimitMiddleware(h)
		h = requestIDMiddleware(h)
		h = authMiddleware(auth, h)
		if len(db.config.HTTP.CORSAllowedOrigins) > 0 {
			h = corsMiddlewareGlobal(db.config.HTTP.CORSAllowedOrigins)(h)
		}
		h = httpMetricsMiddleware(httpMetrics)(h)
		if rl != nil {
			h = rateLimitMiddleware(rl, h)
		}
		return h
	}

	mux := http.NewServeMux()

	// Setup route groups
	setupWriteRoutes(mux, db, wrap)
	setupQueryRoutes(mux, db, wrap)
	setupPrometheusRoutes(mux, db, wrap)
	setupAdminRoutes(mux, db, wrap, auth)
	setupAlertingRoutes(mux, db, wrap, auth)
	setupFeatureRoutes(mux, db, wrap)
	setupNextGenRoutes(mux, db, wrap)

	// Setup ClickHouse-compatible routes if enabled
	if db.config.ClickHouse != nil && db.config.ClickHouse.Enabled {
		setupClickHouseRoutes(mux, db, *db.config.ClickHouse, wrap)
	}

	// Register standard Go pprof handlers for runtime profiling.
	// These are behind the admin auth middleware when auth is enabled.
	adminWrap := func(h http.HandlerFunc) http.HandlerFunc {
		return wrap(adminOnlyMiddleware(auth, h))
	}
	mux.HandleFunc("/debug/pprof/", adminWrap(pprof.Index))
	mux.HandleFunc("/debug/pprof/cmdline", adminWrap(pprof.Cmdline))
	mux.HandleFunc("/debug/pprof/profile", adminWrap(pprof.Profile))
	mux.HandleFunc("/debug/pprof/symbol", adminWrap(pprof.Symbol))
	mux.HandleFunc("/debug/pprof/trace", adminWrap(pprof.Trace))

	// HTTP request metrics endpoint
	mux.HandleFunc("/api/v1/http/metrics", wrap(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(httpMetrics.Snapshot())
	}))

	bindAddress := db.config.HTTP.HTTPBindAddress
	if bindAddress == "" {
		bindAddress = "127.0.0.1"
	}
	addr := net.JoinHostPort(bindAddress, fmt.Sprint(port))
	listener, err := net.Listen("tcp", addr)
	if err != nil {
		return nil, err
	}

	// Wrap the mux with a global body size limit to protect all handlers,
	// including feature handlers registered via RegisterHTTPHandlers.
	handler := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Body != nil && r.ContentLength != 0 {
			r.Body = http.MaxBytesReader(w, r.Body, maxBodySize)
		}
		mux.ServeHTTP(w, r)
	})

	srv := &http.Server{
		Handler:      handler,
		ReadTimeout:  15 * time.Second,
		WriteTimeout: 15 * time.Second,
	}

	s := &httpServer{srv: srv, rl: rl}

	s.wg.Add(1)
	go func(srv *http.Server) {
		defer s.wg.Done()
		if err := srv.Serve(listener); err != nil && err != http.ErrServerClosed {
			slog.Error("http server error", "err", err)
		}
	}(srv)

	return s, nil
}

func (s *httpServer) Close() error {
	if s == nil || s.srv == nil {
		return nil
	}
	if s.rl != nil {
		s.rl.Stop()
	}
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	err := s.srv.Shutdown(ctx)
	s.wg.Wait()
	return err
}
