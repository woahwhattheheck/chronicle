package chronicle

import (
	"context"
	"encoding/json"
	"fmt"
	"sort"
	"strings"
	"sync"
	"sync/atomic"
	"time"
)

// Span query types and signal correlation for multi-modal storage.

// SpanQuery defines span search criteria
type SpanQuery struct {
	StartTime   time.Time         `json:"start_time"`
	EndTime     time.Time         `json:"end_time"`
	Service     string            `json:"service"`
	Name        string            `json:"name"`
	Status      StatusCode        `json:"status"`
	Kind        SpanKind          `json:"kind"`
	MinDuration time.Duration     `json:"min_duration"`
	MaxDuration time.Duration     `json:"max_duration"`
	Attributes  map[string]string `json:"attributes"`
	Limit       int               `json:"limit"`
}

// Correlate finds correlations between signals
func (mms *MultiModalStorage) Correlate(ctx context.Context, traceID string) (*CorrelatedSignals, error) {
	if !mms.config.CorrelationEnabled {
		return nil, fmt.Errorf("correlation not enabled")
	}

	result := &CorrelatedSignals{
		TraceID:      traceID,
		Correlations: make([]Correlation, 0),
	}

	// Get trace spans
	trace, _ := mms.GetTrace(traceID)
	if trace != nil {
		result.Spans = trace.Spans
		result.TimeRange = MMTimeRange{
			Start: trace.StartTime.Add(-mms.config.CorrelationWindow),
			End:   trace.EndTime.Add(mms.config.CorrelationWindow),
		}
	} else {
		// Use default time range
		result.TimeRange = MMTimeRange{
			Start: time.Now().Add(-mms.config.CorrelationWindow),
			End:   time.Now(),
		}
	}

	// Get correlated logs
	logs, _ := mms.QueryLogs(&MMLogQuery{
		TraceID:   traceID,
		StartTime: result.TimeRange.Start,
		EndTime:   result.TimeRange.End,
	})
	result.Logs = logs

	// Get correlated metrics
	if mms.db != nil && trace != nil {
		for _, svc := range trace.Services {
			queryResult, _ := mms.db.Execute(&Query{
				Metric: svc,
				Start:  result.TimeRange.Start.UnixNano(),
				End:    result.TimeRange.End.UnixNano(),
			})
			if queryResult != nil {
				result.Metrics = append(result.Metrics, queryResult.Points...)
			}
		}
	}

	// Build correlations
	for _, span := range result.Spans {
		for _, log := range result.Logs {
			if log.SpanID == span.SpanID {
				result.Correlations = append(result.Correlations, Correlation{
					Type: "span_log",
					Signal1: SignalRef{
						Type:      SignalTrace,
						ID:        span.SpanID,
						Timestamp: span.StartTime,
					},
					Signal2: SignalRef{
						Type:      SignalLog,
						ID:        log.ID,
						Timestamp: log.Timestamp,
					},
					Confidence: 1.0,
				})
			}
		}
	}

	return result, nil
}

// CorrelateByTime correlates signals within a time window
func (mms *MultiModalStorage) CorrelateByTime(start, end time.Time, services []string) (*CorrelatedSignals, error) {
	result := &CorrelatedSignals{
		TimeRange: MMTimeRange{Start: start, End: end},
	}

	// Get logs in time range
	logs, _ := mms.QueryLogs(&MMLogQuery{
		StartTime: start,
		EndTime:   end,
	})
	result.Logs = logs

	// Get spans in time range
	spans, _ := mms.QuerySpans(&SpanQuery{
		StartTime: start,
		EndTime:   end,
	})
	result.Spans = spans

	// Get metrics
	if mms.db != nil {
		for _, svc := range services {
			queryResult, _ := mms.db.Execute(&Query{
				Metric: svc,
				Start:  start.UnixNano(),
				End:    end.UnixNano(),
			})
			if queryResult != nil {
				result.Metrics = append(result.Metrics, queryResult.Points...)
			}
		}
	}

	return result, nil
}

// UnifiedQuery performs a unified query across all signal types
func (mms *MultiModalStorage) UnifiedQuery(query *UnifiedQuery) (*UnifiedResult, error) {
	atomic.AddInt64(&mms.queries, 1)

	result := &UnifiedResult{
		Query: query,
	}

	// Query metrics
	if query.IncludeMetrics && mms.db != nil {
		for _, series := range query.Series {
			queryResult, _ := mms.db.Execute(&Query{
				Metric: series,
				Start:  query.StartTime.UnixNano(),
				End:    query.EndTime.UnixNano(),
				Limit:  query.Limit,
			})
			if queryResult != nil {
				result.Metrics = append(result.Metrics, queryResult.Points...)
			}
		}
	}

	// Query logs
	if query.IncludeLogs {
		logs, _ := mms.QueryLogs(&MMLogQuery{
			StartTime: query.StartTime,
			EndTime:   query.EndTime,
			Service:   query.Service,
			Search:    query.Search,
			Limit:     query.Limit,
		})
		result.Logs = logs
	}

	// Query traces
	if query.IncludeTraces {
		spans, _ := mms.QuerySpans(&SpanQuery{
			StartTime: query.StartTime,
			EndTime:   query.EndTime,
			Service:   query.Service,
			Limit:     query.Limit,
		})
		result.Spans = spans
	}

	return result, nil
}

// UnifiedQuery defines a unified query across signal types
type UnifiedQuery struct {
	StartTime      time.Time `json:"start_time"`
	EndTime        time.Time `json:"end_time"`
	Service        string    `json:"service"`
	Series         []string  `json:"series"`
	Search         string    `json:"search"`
	IncludeMetrics bool      `json:"include_metrics"`
	IncludeLogs    bool      `json:"include_logs"`
	IncludeTraces  bool      `json:"include_traces"`
	Limit          int       `json:"limit"`
}

// UnifiedResult contains unified query results
type UnifiedResult struct {
	Query   *UnifiedQuery `json:"query"`
	Metrics []Point       `json:"metrics,omitempty"`
	Logs    []MMLogEntry  `json:"logs,omitempty"`
	Spans   []*Span       `json:"spans,omitempty"`
}

// Stats returns storage statistics
func (mms *MultiModalStorage) Stats() MultiModalStats {
	mms.logsMu.RLock()
	logCount := len(mms.logs)
	mms.logsMu.RUnlock()

	mms.tracesMu.RLock()
	spanCount := len(mms.spans)
	traceCount := len(mms.traces)
	mms.tracesMu.RUnlock()

	return MultiModalStats{
		TotalLogs:      atomic.LoadInt64(&mms.totalLogs),
		TotalSpans:     atomic.LoadInt64(&mms.totalSpans),
		TotalTraces:    atomic.LoadInt64(&mms.totalTraces),
		TotalQueries:   atomic.LoadInt64(&mms.queries),
		LogsInMemory:   logCount,
		SpansInMemory:  spanCount,
		TracesInMemory: traceCount,
	}
}

// MultiModalStats contains storage statistics
type MultiModalStats struct {
	TotalLogs      int64 `json:"total_logs"`
	TotalSpans     int64 `json:"total_spans"`
	TotalTraces    int64 `json:"total_traces"`
	TotalQueries   int64 `json:"total_queries"`
	LogsInMemory   int   `json:"logs_in_memory"`
	SpansInMemory  int   `json:"spans_in_memory"`
	TracesInMemory int   `json:"traces_in_memory"`
}

// Close shuts down the storage
func (mms *MultiModalStorage) Close() error {
	mms.cancel()
	mms.wg.Wait()
	return nil
}

func (mms *MultiModalStorage) retentionWorker() {
	defer mms.wg.Done()

	ticker := time.NewTicker(time.Hour)
	defer ticker.Stop()

	for {
		select {
		case <-mms.ctx.Done():
			return
		case <-ticker.C:
			mms.applyRetention()
		}
	}
}

func (mms *MultiModalStorage) applyRetention() {
	now := time.Now()

	// Clean old logs
	if mms.config.LogRetention > 0 {
		cutoff := now.Add(-mms.config.LogRetention)
		mms.logsMu.Lock()
		newLogs := make([]MMLogEntry, 0)
		for _, log := range mms.logs {
			if log.Timestamp.After(cutoff) {
				newLogs = append(newLogs, log)
			}
		}
		mms.logs = newLogs
		mms.logsMu.Unlock()
	}

	// Clean old spans
	if mms.config.TraceRetention > 0 {
		cutoff := now.Add(-mms.config.TraceRetention)
		mms.tracesMu.Lock()
		for spanID, span := range mms.spans {
			if span.EndTime.Before(cutoff) {
				delete(mms.spans, spanID)
			}
		}

		// Clean trace index
		for traceID, spans := range mms.traces {
			newSpans := make([]*Span, 0)
			for _, span := range spans {
				if span.EndTime.After(cutoff) {
					newSpans = append(newSpans, span)
				}
			}
			if len(newSpans) == 0 {
				delete(mms.traces, traceID)
			} else {
				mms.traces[traceID] = newSpans
			}
		}
		mms.tracesMu.Unlock()
	}
}

func (mms *MultiModalStorage) pruneOldestSpans() {
	// Find oldest spans and remove them
	type spanAge struct {
		id   string
		time time.Time
	}

	ages := make([]spanAge, 0, len(mms.spans))
	for id, span := range mms.spans {
		ages = append(ages, spanAge{id, span.StartTime})
	}

	sort.Slice(ages, func(i, j int) bool {
		return ages[i].time.Before(ages[j].time)
	})

	// Remove oldest 10%
	removeCount := len(ages) / 10
	if removeCount < 1 {
		removeCount = 1
	}

	for i := 0; i < removeCount && i < len(ages); i++ {
		delete(mms.spans, ages[i].id)
	}
}

// Inverted index for full-text search
type invertedIndex struct {
	index     map[string]map[string]bool // term -> docIDs
	ngramSize int
	mu        sync.RWMutex
}

func newInvertedIndex(ngramSize int) *invertedIndex {
	return &invertedIndex{
		index:     make(map[string]map[string]bool),
		ngramSize: ngramSize,
	}
}

func (idx *invertedIndex) add(docID, text string) {
	text = strings.ToLower(text)
	terms := idx.tokenize(text)

	for _, term := range terms {
		if idx.index[term] == nil {
			idx.index[term] = make(map[string]bool)
		}
		idx.index[term][docID] = true
	}
}

func (idx *invertedIndex) search(query string) map[string]bool {
	query = strings.ToLower(query)
	terms := idx.tokenize(query)

	if len(terms) == 0 {
		return nil
	}

	// Intersect results for all terms
	var result map[string]bool
	for _, term := range terms {
		docIDs := idx.index[term]
		if result == nil {
			result = make(map[string]bool)
			for id := range docIDs {
				result[id] = true
			}
		} else {
			for id := range result {
				if !docIDs[id] {
					delete(result, id)
				}
			}
		}
	}

	return result
}

func (idx *invertedIndex) tokenize(text string) []string {
	// Simple word tokenization + n-grams
	terms := make([]string, 0)

	// Words
	words := strings.Fields(text)
	for _, word := range words {
		word = strings.Trim(word, ".,!?;:\"'()[]{}")
		if len(word) > 0 {
			terms = append(terms, word)
		}
	}

	// N-grams
	if idx.ngramSize > 0 && len(text) >= idx.ngramSize {
		for i := 0; i <= len(text)-idx.ngramSize; i++ {
			terms = append(terms, text[i:i+idx.ngramSize])
		}
	}

	return terms
}

// Helper functions

var multiModalIDSequence atomic.Uint64

// Log and span identities must not repeat when the clock has coarse resolution
// or moves backward. Sharing the sequence also separates the two ID namespaces.
func generateMultiModalID() string {
	for {
		previous := multiModalIDSequence.Load()
		next := uint64(time.Now().UnixNano())
		if next <= previous {
			next = previous + 1
		}
		if multiModalIDSequence.CompareAndSwap(previous, next) {
			return fmt.Sprintf("%016x", next)
		}
	}
}

func generateLogID() string {
	return generateMultiModalID()
}

func generateSpanID() string {
	return generateMultiModalID()
}

func isLevelAtOrAbove(level, threshold LogLevel) bool {
	levels := map[LogLevel]int{
		LogLevelDebug: 0,
		LogLevelInfo:  1,
		LogLevelWarn:  2,
		LogLevelError: 3,
		LogLevelFatal: 4,
	}
	return levels[level] >= levels[threshold]
}

// JSON helpers
func (e *MMLogEntry) MarshalJSON() ([]byte, error) {
	type Alias MMLogEntry
	return json.Marshal(&struct {
		*Alias
	}{Alias: (*Alias)(e)})
}

func (s *Span) MarshalJSON() ([]byte, error) {
	type Alias Span
	return json.Marshal(&struct {
		*Alias
	}{Alias: (*Alias)(s)})
}
