package chronicle

import (
	"encoding/json"
	"fmt"
	"net/http"
	"strings"
	"time"
)

// Continuous query management, CEP pattern processing, event evaluation, and HTTP handlers for stream DSL v2.

// CreateContinuousQuery creates and registers a continuous query.
func (e *StreamDSLV2Engine) CreateContinuousQuery(name, dsl string) (*DSLV2ContinuousQuery, error) {
	if name == "" {
		return nil, fmt.Errorf("stream dsl v2: query name is required")
	}

	stmt, err := e.Parse(dsl)
	if err != nil {
		return nil, fmt.Errorf("stream dsl v2: parse error: %w", err)
	}

	e.mu.Lock()
	defer e.mu.Unlock()

	if len(e.queries) >= e.config.MaxConcurrentQueries {
		return nil, fmt.Errorf("stream dsl v2: max concurrent queries (%d) reached", e.config.MaxConcurrentQueries)
	}

	genID, err := streamDSLV2GenerateID()
	if err != nil {
		return nil, err
	}
	id := "sdv2-" + genID
	q := &DSLV2ContinuousQuery{
		ID:       id,
		Name:     name,
		DSL:      dsl,
		Compiled: stmt,
		State:    QueryV2Created,
		Created:  time.Now(),
	}
	e.queries[id] = q
	e.results[id] = nil

	return q, nil
}

// StartQuery transitions a query to the running state.
func (e *StreamDSLV2Engine) StartQuery(id string) error {
	e.mu.Lock()
	defer e.mu.Unlock()

	q, ok := e.queries[id]
	if !ok {
		return fmt.Errorf("stream dsl v2: query %q not found", id)
	}
	if q.State == QueryV2Running {
		return fmt.Errorf("stream dsl v2: query %q is already running", id)
	}
	q.State = QueryV2Running
	return nil
}

// PauseQuery transitions a running query to the paused state.
func (e *StreamDSLV2Engine) PauseQuery(id string) error {
	e.mu.Lock()
	defer e.mu.Unlock()

	q, ok := e.queries[id]
	if !ok {
		return fmt.Errorf("stream dsl v2: query %q not found", id)
	}
	if q.State != QueryV2Running {
		return fmt.Errorf("stream dsl v2: query %q is not running", id)
	}
	q.State = QueryV2Paused
	return nil
}

// StopQuery transitions a query to the stopped state.
func (e *StreamDSLV2Engine) StopQuery(id string) error {
	e.mu.Lock()
	defer e.mu.Unlock()

	q, ok := e.queries[id]
	if !ok {
		return fmt.Errorf("stream dsl v2: query %q not found", id)
	}
	if q.State != QueryV2Running && q.State != QueryV2Paused {
		return fmt.Errorf("stream dsl v2: query %q is not running or paused", id)
	}
	q.State = QueryV2Stopped
	return nil
}

// GetQuery returns a continuous query by ID.
func (e *StreamDSLV2Engine) GetQuery(id string) (*DSLV2ContinuousQuery, error) {
	e.mu.RLock()
	defer e.mu.RUnlock()

	q, ok := e.queries[id]
	if !ok {
		return nil, fmt.Errorf("stream dsl v2: query %q not found", id)
	}
	return q, nil
}

// ListQueries returns all registered continuous queries.
func (e *StreamDSLV2Engine) ListQueries() []*DSLV2ContinuousQuery {
	e.mu.RLock()
	defer e.mu.RUnlock()

	result := make([]*DSLV2ContinuousQuery, 0, len(e.queries))
	for _, q := range e.queries {
		result = append(result, q)
	}
	return result
}

// RegisterPattern registers a CEP pattern for event matching.
func (e *StreamDSLV2Engine) RegisterPattern(pattern CEPPattern) error {
	if !e.config.EnableCEP {
		return fmt.Errorf("stream dsl v2: CEP is disabled")
	}
	if pattern.Name == "" {
		return fmt.Errorf("stream dsl v2: pattern name is required")
	}
	if len(pattern.Events) == 0 {
		return fmt.Errorf("stream dsl v2: pattern must have at least one event")
	}

	e.mu.Lock()
	defer e.mu.Unlock()
	e.patterns[pattern.Name] = &pattern
	return nil
}

// ListPatterns returns all registered CEP patterns.
func (e *StreamDSLV2Engine) ListPatterns() []CEPPattern {
	e.mu.RLock()
	defer e.mu.RUnlock()

	result := make([]CEPPattern, 0, len(e.patterns))
	for _, p := range e.patterns {
		result = append(result, *p)
	}
	return result
}

// ProcessEvent feeds an event into all active queries and CEP patterns.
func (e *StreamDSLV2Engine) ProcessEvent(metric string, value float64, tags map[string]string, ts time.Time) error {
	start := time.Now()
	e.totalEvents.Add(1)

	e.mu.RLock()
	queries := make([]*DSLV2ContinuousQuery, 0)
	for _, q := range e.queries {
		if q.State == QueryV2Running {
			queries = append(queries, q)
		}
	}
	patterns := make([]*CEPPattern, 0)
	for _, p := range e.patterns {
		patterns = append(patterns, p)
	}
	e.mu.RUnlock()

	// Process through active queries
	for _, q := range queries {
		if q.Compiled == nil {
			continue
		}
		// Check source match
		if q.Compiled.Source != "" && q.Compiled.Source != metric && q.Compiled.Source != "*" {
			continue
		}

		entry := streamDSLV2WindowEntry{
			Value:     value,
			Tags:      tags,
			Timestamp: ts,
		}
		e.state.add(q.ID, entry)

		e.mu.Lock()
		q.Stats.EventsProcessed++
		e.mu.Unlock()

		// For windowed queries, check if window should emit
		if q.Compiled.Window != nil {
			e.evaluateWindow(q, entry)
		} else {
			// Non-windowed: emit immediately
			row := map[string]any{
				"metric":    metric,
				"value":     value,
				"timestamp": ts,
				"tags":      tags,
			}
			result := StreamDSLV2Result{
				Statement:   q.Compiled,
				Rows:        []map[string]any{row},
				WindowStart: ts,
				WindowEnd:   ts,
				Watermark:   ts,
			}
			e.mu.Lock()
			e.results[q.ID] = append(e.results[q.ID], result)
			q.Stats.ResultsEmitted++
			e.mu.Unlock()
		}
	}

	// Process CEP patterns
	for _, p := range patterns {
		for _, ev := range p.Events {
			if ev.Metric == metric {
				e.patternsMatched.Add(1)
				break
			}
		}
	}

	elapsed := time.Since(start)
	e.totalLatencyNs.Add(int64(elapsed))
	e.latencyCount.Add(1)

	return nil
}

func (e *StreamDSLV2Engine) evaluateWindow(q *DSLV2ContinuousQuery, entry streamDSLV2WindowEntry) {
	w := q.Compiled.Window
	entries := e.state.get(q.ID)
	if len(entries) == 0 {
		return
	}

	var shouldEmit bool
	switch w.Type {
	case DSLV2WindowTumbling:
		first := entries[0].Timestamp
		if entry.Timestamp.Sub(first) >= w.Size {
			shouldEmit = true
		}
	case DSLV2WindowSliding:
		first := entries[0].Timestamp
		if entry.Timestamp.Sub(first) >= w.Size {
			shouldEmit = true
		}
	case DSLV2WindowSession:
		if len(entries) >= 2 {
			prev := entries[len(entries)-2]
			gap := w.Gap
			if gap == 0 {
				gap = w.Size
			}
			if entry.Timestamp.Sub(prev.Timestamp) > gap {
				shouldEmit = true
			}
		}
	case DSLV2WindowCount:
		maxCount := w.MaxCount
		if maxCount == 0 {
			maxCount = int(w.Size.Seconds())
		}
		if maxCount > 0 && len(entries) >= maxCount {
			shouldEmit = true
		}
	case DSLV2WindowGlobal:
		// Global windows never auto-emit
	}

	if shouldEmit {
		var rows []map[string]any
		var sum float64
		for _, ent := range entries {
			sum += ent.Value
			rows = append(rows, map[string]any{
				"value":     ent.Value,
				"tags":      ent.Tags,
				"timestamp": ent.Timestamp,
			})
		}
		result := StreamDSLV2Result{
			Statement:   q.Compiled,
			Rows:        rows,
			WindowStart: entries[0].Timestamp,
			WindowEnd:   entries[len(entries)-1].Timestamp,
			Watermark:   entry.Timestamp,
		}
		e.mu.Lock()
		e.results[q.ID] = append(e.results[q.ID], result)
		q.Stats.ResultsEmitted++
		e.mu.Unlock()

		e.state.clear(q.ID)
	}
}

// GetResults returns the latest results for a query.
func (e *StreamDSLV2Engine) GetResults(queryID string) []StreamDSLV2Result {
	e.mu.RLock()
	defer e.mu.RUnlock()

	results := e.results[queryID]
	cp := make([]StreamDSLV2Result, len(results))
	copy(cp, results)
	return cp
}

// Stats returns engine-wide statistics.
func (e *StreamDSLV2Engine) Stats() StreamDSLV2Stats {
	total := e.totalEvents.Load()
	matched := e.patternsMatched.Load()
	totalLat := e.totalLatencyNs.Load()
	latCount := e.latencyCount.Load()

	var avgLat time.Duration
	if latCount > 0 {
		avgLat = time.Duration(totalLat / latCount)
	}

	elapsed := time.Since(e.startTime).Seconds()
	var eps float64
	if elapsed > 0 {
		eps = float64(total) / elapsed
	}

	e.mu.RLock()
	active := 0
	for _, q := range e.queries {
		if q.State == QueryV2Running {
			active++
		}
	}
	e.mu.RUnlock()

	return StreamDSLV2Stats{
		ActiveQueries:        active,
		TotalEventsProcessed: total,
		EventsPerSec:         eps,
		PatternsMatched:      matched,
		AvgLatency:           avgLat,
		StateSizeBytes:       e.state.sizeBytes(),
	}
}

// RegisterHTTPHandlers registers v2 stream DSL HTTP endpoints.
func (e *StreamDSLV2Engine) RegisterHTTPHandlers(mux *http.ServeMux) {
	mux.HandleFunc("/api/v2/stream-dsl/queries", e.handleQueries)
	mux.HandleFunc("/api/v2/stream-dsl/queries/", e.handleQueryAction)
	mux.HandleFunc("/api/v2/stream-dsl/patterns", e.handlePatterns)
	mux.HandleFunc("/api/v2/stream-dsl/events", e.handleEvents)
	mux.HandleFunc("/api/v2/stream-dsl/stats", e.handleStats)
	mux.HandleFunc("/api/v2/stream-dsl/validate", e.handleValidate)
}

func (e *StreamDSLV2Engine) handleQueries(w http.ResponseWriter, r *http.Request) {
	switch r.Method {
	case http.MethodGet:
		queries := e.ListQueries()
		writeJSON(w, queries)

	case http.MethodPost:
		var req struct {
			Name string `json:"name"`
			DSL  string `json:"dsl"`
		}
		r.Body = http.MaxBytesReader(w, r.Body, maxBodySize)
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			writeError(w, err.Error(), http.StatusBadRequest)
			return
		}
		q, err := e.CreateContinuousQuery(req.Name, req.DSL)
		if err != nil {
			writeError(w, err.Error(), http.StatusBadRequest)
			return
		}
		writeJSONStatus(w, http.StatusCreated, q)

	default:
		w.WriteHeader(http.StatusMethodNotAllowed)
	}
}

func (e *StreamDSLV2Engine) handleQueryAction(w http.ResponseWriter, r *http.Request) {
	path := strings.TrimPrefix(r.URL.Path, "/api/v2/stream-dsl/queries/")
	if path == "" {
		w.WriteHeader(http.StatusNotFound)
		return
	}

	parts := strings.SplitN(path, "/", 2)
	id := parts[0]
	action := ""
	if len(parts) == 2 {
		action = parts[1]
	}

	switch {
	case action == "" && r.Method == http.MethodGet:
		q, err := e.GetQuery(id)
		if err != nil {
			writeError(w, err.Error(), http.StatusNotFound)
			return
		}
		writeJSON(w, q)

	case action == "start" && r.Method == http.MethodPost:
		if err := e.StartQuery(id); err != nil {
			writeError(w, err.Error(), http.StatusBadRequest)
			return
		}
		writeJSON(w, map[string]string{"status": "started", "id": id})

	case action == "pause" && r.Method == http.MethodPost:
		if err := e.PauseQuery(id); err != nil {
			writeError(w, err.Error(), http.StatusBadRequest)
			return
		}
		writeJSON(w, map[string]string{"status": "paused", "id": id})

	case action == "stop" && r.Method == http.MethodPost:
		if err := e.StopQuery(id); err != nil {
			writeError(w, err.Error(), http.StatusBadRequest)
			return
		}
		writeJSON(w, map[string]string{"status": "stopped", "id": id})

	case action == "results" && r.Method == http.MethodGet:
		results := e.GetResults(id)
		writeJSON(w, results)

	default:
		w.WriteHeader(http.StatusNotFound)
	}
}

func (e *StreamDSLV2Engine) handlePatterns(w http.ResponseWriter, r *http.Request) {
	switch r.Method {
	case http.MethodGet:
		patterns := e.ListPatterns()
		writeJSON(w, patterns)

	case http.MethodPost:
		var req struct {
			Name   string     `json:"name"`
			Events []CEPEvent `json:"events"`
			Within string     `json:"within"`
		}
		r.Body = http.MaxBytesReader(w, r.Body, maxBodySize)
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			writeError(w, err.Error(), http.StatusBadRequest)
			return
		}
		var within time.Duration
		if req.Within != "" {
			d, err := time.ParseDuration(req.Within)
			if err != nil {
				writeError(w, fmt.Sprintf("invalid within: %s", err), http.StatusBadRequest)
				return
			}
			within = d
		}
		pattern := CEPPattern{
			Name:   req.Name,
			Events: req.Events,
			Within: within,
		}
		if err := e.RegisterPattern(pattern); err != nil {
			writeError(w, err.Error(), http.StatusBadRequest)
			return
		}
		writeJSONStatus(w, http.StatusCreated, map[string]string{"status": "registered", "name": req.Name})

	default:
		w.WriteHeader(http.StatusMethodNotAllowed)
	}
}

func (e *StreamDSLV2Engine) handleEvents(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		w.WriteHeader(http.StatusMethodNotAllowed)
		return
	}

	var req struct {
		Metric string            `json:"metric"`
		Value  float64           `json:"value"`
		Tags   map[string]string `json:"tags"`
		Time   string            `json:"time"`
	}
	r.Body = http.MaxBytesReader(w, r.Body, maxBodySize)
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		writeError(w, err.Error(), http.StatusBadRequest)
		return
	}

	ts := time.Now()
	if req.Time != "" {
		parsed, err := time.Parse(time.RFC3339, req.Time)
		if err == nil {
			ts = parsed
		}
	}

	if err := e.ProcessEvent(req.Metric, req.Value, req.Tags, ts); err != nil {
		internalError(w, err, "internal error")
		return
	}
	writeJSON(w, map[string]string{"status": "ok"})
}

func (e *StreamDSLV2Engine) handleStats(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		w.WriteHeader(http.StatusMethodNotAllowed)
		return
	}
	writeJSON(w, e.Stats())
}

func (e *StreamDSLV2Engine) handleValidate(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		w.WriteHeader(http.StatusMethodNotAllowed)
		return
	}

	var req struct {
		DSL string `json:"dsl"`
	}
	r.Body = http.MaxBytesReader(w, r.Body, maxBodySize)
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		writeError(w, err.Error(), http.StatusBadRequest)
		return
	}

	if err := e.Validate(req.DSL); err != nil {
		writeJSON(w, map[string]any{"valid": false, "error": err.Error()})
		return
	}
	writeJSON(w, map[string]any{"valid": true})
}
