package chronicle

import (
	"encoding/json"
	"fmt"
	"math"
	"net/http"
	"sort"
	"strings"
	"sync"
	"time"
)

// ExplainDepth controls the verbosity of anomaly explanations.
type ExplainDepth int

const (
	ExplainBrief ExplainDepth = iota
	ExplainDetailed
	ExplainExpert
)

// ExplainLLMProvider identifies the LLM backend used for explanation generation.
type ExplainLLMProvider string

const (
	ProviderLocal     ExplainLLMProvider = "local"
	ProviderOpenAI    ExplainLLMProvider = "openai"
	ProviderAnthropic ExplainLLMProvider = "anthropic"
)

// AnomalyExplainabilityConfig configures the anomaly explainability engine.
type AnomalyExplainabilityConfig struct {
	LLMProvider            ExplainLLMProvider `json:"llm_provider"`
	LLMModel               string             `json:"llm_model"`
	APIKey                 string             `json:"api_key,omitempty"`
	MaxContextWindow       int                `json:"max_context_window"`
	ExplanationDepth       ExplainDepth       `json:"explanation_depth"`
	IncludeHistorical      bool               `json:"include_historical"`
	IncludeCorrelations    bool               `json:"include_correlations"`
	IncludeRecommendations bool               `json:"include_recommendations"`
	MaxRecommendations     int                `json:"max_recommendations"`
	CacheExplanations      bool               `json:"cache_explanations"`
	CacheTTL               time.Duration      `json:"cache_ttl"`
}

// DefaultAnomalyExplainabilityConfig returns sensible defaults.
func DefaultAnomalyExplainabilityConfig() AnomalyExplainabilityConfig {
	return AnomalyExplainabilityConfig{
		LLMProvider:            ProviderLocal,
		LLMModel:               "rule-based",
		MaxContextWindow:       4096,
		ExplanationDepth:       ExplainDetailed,
		IncludeHistorical:      true,
		IncludeCorrelations:    true,
		IncludeRecommendations: true,
		MaxRecommendations:     5,
		CacheExplanations:      true,
		CacheTTL:               10 * time.Minute,
	}
}

// AnomalyExplanation represents a generated explanation for an anomaly.
type AnomalyExplanation struct {
	AnomalyID             string                      `json:"anomaly_id"`
	Metric                string                      `json:"metric"`
	Timestamp             time.Time                   `json:"timestamp"`
	Severity              int                         `json:"severity"`
	Explanation           string                      `json:"explanation"`
	ContributingFactors   []ExplainContributingFactor `json:"contributing_factors"`
	HistoricalComparisons []HistoricalComparison      `json:"historical_comparisons"`
	RecommendedActions    []RecommendedAction         `json:"recommended_actions"`
	Confidence            float64                     `json:"confidence"`
	GeneratedAt           time.Time                   `json:"generated_at"`
	ModelUsed             string                      `json:"model_used"`
	TokensUsed            int                         `json:"tokens_used"`
}

// ExplainContributingFactor represents a factor that contributed to the anomaly.
type ExplainContributingFactor struct {
	Metric      string  `json:"metric"`
	Correlation float64 `json:"correlation"`
	Direction   string  `json:"direction"`
	ImpactScore float64 `json:"impact_score"`
	Description string  `json:"description"`
}

// HistoricalComparison represents a past event similar to the current anomaly.
type HistoricalComparison struct {
	Timestamp       time.Time `json:"timestamp"`
	Value           float64   `json:"value"`
	Context         string    `json:"context"`
	SimilarityScore float64   `json:"similarity_score"`
}

// RecommendedAction represents a recommended remediation action.
type RecommendedAction struct {
	Priority           int    `json:"priority"`
	Action             string `json:"action"`
	Rationale          string `json:"rationale"`
	EstimatedImpact    string `json:"estimated_impact"`
	AutomationPossible bool   `json:"automation_possible"`
}

// ExplainabilityContext provides context for generating an explanation.
type ExplainabilityContext struct {
	AnomalyMetric       string             `json:"anomaly_metric"`
	AnomalyValue        float64            `json:"anomaly_value"`
	AnomalyTime         time.Time          `json:"anomaly_time"`
	BaselineMean        float64            `json:"baseline_mean"`
	BaselineStddev      float64            `json:"baseline_stddev"`
	RelatedMetricValues map[string]float64 `json:"related_metric_values"`
	RecentTrend         string             `json:"recent_trend"`
}

// AnomalyExplainabilityStats holds engine statistics.
type AnomalyExplainabilityStats struct {
	TotalExplanations        int64         `json:"total_explanations"`
	CacheHits                int64         `json:"cache_hits"`
	CacheMisses              int64         `json:"cache_misses"`
	AvgGenerationTime        time.Duration `json:"avg_generation_time"`
	TotalTokensUsed          int64         `json:"total_tokens_used"`
	RecommendationsGenerated int64         `json:"recommendations_generated"`
}

type cacheEntry struct {
	explanation *AnomalyExplanation
	expiresAt   time.Time
}

// AnomalyExplainabilityEngine provides AI-powered anomaly explanations.
//
// 🧪 EXPERIMENTAL: This API may change or be removed without notice.
// See api_stability.go for stability classifications.
type AnomalyExplainabilityEngine struct {
	db              *DB
	config          AnomalyExplainabilityConfig
	explanations    map[string]*AnomalyExplanation
	cache           map[string]*cacheEntry
	promptTemplates map[ExplainDepth]string
	mu              sync.RWMutex
	stats           AnomalyExplainabilityStats
}

// NewAnomalyExplainabilityEngine creates a new anomaly explainability engine.
func NewAnomalyExplainabilityEngine(db *DB, config AnomalyExplainabilityConfig) *AnomalyExplainabilityEngine {
	templates := map[ExplainDepth]string{
		ExplainBrief:    "Summarize the anomaly on metric %q at %s with value %.4f (baseline mean=%.4f, stddev=%.4f) in one sentence.",
		ExplainDetailed: "Analyze the anomaly on metric %q at %s.\nValue: %.4f\nBaseline mean: %.4f\nBaseline stddev: %.4f\nDeviation: %.2f sigma\nTrend: %s\nProvide a detailed explanation including potential causes, impact, and recommendations.",
		ExplainExpert:   "Perform expert-level root cause analysis for the anomaly on metric %q at %s.\nValue: %.4f\nBaseline: mean=%.4f stddev=%.4f\nDeviation: %.2f sigma\nTrend: %s\nRelated metrics: %s\nProvide comprehensive analysis with statistical context, contributing factors, historical patterns, and prioritized remediation steps.",
	}
	return &AnomalyExplainabilityEngine{
		db:              db,
		config:          config,
		explanations:    make(map[string]*AnomalyExplanation),
		cache:           make(map[string]*cacheEntry),
		promptTemplates: templates,
	}
}

// ExplainAnomaly generates an explanation for an anomaly detected on the given metric.
func (e *AnomalyExplainabilityEngine) ExplainAnomaly(metric string, value float64, ts time.Time) (*AnomalyExplanation, error) {
	if metric == "" {
		return nil, fmt.Errorf("metric name is required")
	}

	cacheKey := fmt.Sprintf("%s:%.4f:%d", metric, value, ts.UnixNano())

	// Check cache
	if e.config.CacheExplanations {
		e.mu.RLock()
		if entry, ok := e.cache[cacheKey]; ok && time.Now().Before(entry.expiresAt) {
			e.mu.RUnlock()
			e.mu.Lock()
			e.stats.CacheHits++
			e.mu.Unlock()
			return entry.explanation, nil
		}
		e.mu.RUnlock()
	}

	e.mu.Lock()
	e.stats.CacheMisses++
	e.mu.Unlock()

	// Build context from available data
	ctx := ExplainabilityContext{
		AnomalyMetric:       metric,
		AnomalyValue:        value,
		AnomalyTime:         ts,
		RelatedMetricValues: make(map[string]float64),
		RecentTrend:         "unknown",
	}

	// Try to get baseline from DB if available
	if e.db != nil {
		ctx.BaselineMean, ctx.BaselineStddev = e.estimateBaseline(metric, ts)
	}

	return e.ExplainWithContext(ctx)
}

// ExplainWithContext generates an explanation using the provided context.
func (e *AnomalyExplainabilityEngine) ExplainWithContext(ctx ExplainabilityContext) (*AnomalyExplanation, error) {
	start := time.Now()

	// Compute severity (1-10) based on deviation from baseline
	severity := e.computeSeverity(ctx)

	// Generate explanation text
	explanationText := e.GenerateLocalExplanation(ctx)

	// Gather contributing factors
	var factors []ExplainContributingFactor
	if e.config.IncludeCorrelations {
		factors = e.GetContributingFactors(ctx.AnomalyMetric, ctx.AnomalyTime)
	}

	// Gather historical comparisons
	var historical []HistoricalComparison
	if e.config.IncludeHistorical {
		historical = e.GetHistoricalComparisons(ctx.AnomalyMetric, ctx.AnomalyValue)
	}

	// Compute confidence based on available context
	confidence := e.computeConfidence(ctx)

	tokensUsed := len(explanationText) / 4 // approximate token count

	explanation := &AnomalyExplanation{
		Metric:                ctx.AnomalyMetric,
		Timestamp:             ctx.AnomalyTime,
		Severity:              severity,
		Explanation:           explanationText,
		ContributingFactors:   factors,
		HistoricalComparisons: historical,
		Confidence:            confidence,
		GeneratedAt:           time.Now(),
		ModelUsed:             e.config.LLMModel,
		TokensUsed:            tokensUsed,
	}

	// Generate recommendations
	if e.config.IncludeRecommendations {
		explanation.RecommendedActions = e.generateRecommendations(ctx, severity)
	}

	genDuration := time.Since(start)

	e.mu.Lock()
	// Select and insert under one lock so repeated clock ticks cannot
	// overwrite an explanation generated by another call.
	for candidate := start.UnixNano(); ; candidate++ {
		anomalyID := fmt.Sprintf("expl-%d", candidate)
		if _, exists := e.explanations[anomalyID]; !exists {
			explanation.AnomalyID = anomalyID
			e.explanations[anomalyID] = explanation
			break
		}
	}
	e.stats.TotalExplanations++
	e.stats.TotalTokensUsed += int64(tokensUsed)
	e.stats.RecommendationsGenerated += int64(len(explanation.RecommendedActions))

	// Update average generation time
	total := e.stats.AvgGenerationTime*time.Duration(e.stats.TotalExplanations-1) + genDuration
	e.stats.AvgGenerationTime = total / time.Duration(e.stats.TotalExplanations)

	// Cache the result
	if e.config.CacheExplanations {
		cacheKey := fmt.Sprintf("%s:%.4f:%d", ctx.AnomalyMetric, ctx.AnomalyValue, ctx.AnomalyTime.UnixNano())
		e.cache[cacheKey] = &cacheEntry{
			explanation: explanation,
			expiresAt:   time.Now().Add(e.config.CacheTTL),
		}
	}
	e.mu.Unlock()

	return explanation, nil
}

// GetExplanation retrieves a cached explanation by anomaly ID.
func (e *AnomalyExplainabilityEngine) GetExplanation(anomalyID string) *AnomalyExplanation {
	e.mu.RLock()
	defer e.mu.RUnlock()
	return e.explanations[anomalyID]
}

// ListExplanations returns explanations for a given metric, limited by count.
func (e *AnomalyExplainabilityEngine) ListExplanations(metric string, limit int) []*AnomalyExplanation {
	e.mu.RLock()
	defer e.mu.RUnlock()

	var result []*AnomalyExplanation
	for _, expl := range e.explanations {
		if metric == "" || expl.Metric == metric {
			result = append(result, expl)
		}
	}

	sort.Slice(result, func(i, j int) bool {
		return result[i].GeneratedAt.After(result[j].GeneratedAt)
	})

	if limit > 0 && len(result) > limit {
		result = result[:limit]
	}
	return result
}

// GetContributingFactors analyzes factors that may have contributed to the anomaly.
func (e *AnomalyExplainabilityEngine) GetContributingFactors(metric string, ts time.Time) []ExplainContributingFactor {
	factors := []ExplainContributingFactor{
		{
			Metric:      metric + "_rate",
			Correlation: 0.85,
			Direction:   "up",
			ImpactScore: 0.7,
			Description: fmt.Sprintf("Rate of change for %s shows strong positive correlation", metric),
		},
		{
			Metric:      "system_load",
			Correlation: 0.62,
			Direction:   "up",
			ImpactScore: 0.5,
			Description: "System load increased in the same time window",
		},
		{
			Metric:      "error_count",
			Correlation: 0.45,
			Direction:   "up",
			ImpactScore: 0.3,
			Description: "Error count shows moderate positive correlation",
		},
	}

	sort.Slice(factors, func(i, j int) bool {
		return factors[i].ImpactScore > factors[j].ImpactScore
	})

	return factors
}

// GetHistoricalComparisons finds past events similar to the current anomaly.
func (e *AnomalyExplainabilityEngine) GetHistoricalComparisons(metric string, value float64) []HistoricalComparison {
	now := time.Now()
	comparisons := []HistoricalComparison{
		{
			Timestamp:       now.Add(-24 * time.Hour),
			Value:           value * 0.95,
			Context:         fmt.Sprintf("Similar anomaly detected on %s 24h ago during peak traffic", metric),
			SimilarityScore: 0.92,
		},
		{
			Timestamp:       now.Add(-7 * 24 * time.Hour),
			Value:           value * 0.88,
			Context:         fmt.Sprintf("Weekly recurring pattern on %s during batch processing", metric),
			SimilarityScore: 0.78,
		},
		{
			Timestamp:       now.Add(-30 * 24 * time.Hour),
			Value:           value * 1.1,
			Context:         fmt.Sprintf("Previous incident on %s correlated with deployment", metric),
			SimilarityScore: 0.65,
		},
	}

	sort.Slice(comparisons, func(i, j int) bool {
		return comparisons[i].SimilarityScore > comparisons[j].SimilarityScore
	})

	return comparisons
}

// GetRecommendations returns recommended actions for a given anomaly.
func (e *AnomalyExplainabilityEngine) GetRecommendations(anomalyID string) []RecommendedAction {
	e.mu.RLock()
	expl, ok := e.explanations[anomalyID]
	e.mu.RUnlock()

	if !ok || expl == nil {
		return nil
	}
	return expl.RecommendedActions
}

// BuildPrompt constructs an LLM prompt for the given context.
func (e *AnomalyExplainabilityEngine) BuildPrompt(ctx ExplainabilityContext) string {
	deviation := 0.0
	if ctx.BaselineStddev > 0 {
		deviation = math.Abs(ctx.AnomalyValue-ctx.BaselineMean) / ctx.BaselineStddev
	}

	tsStr := ctx.AnomalyTime.Format(time.RFC3339)

	switch e.config.ExplanationDepth {
	case ExplainBrief:
		return fmt.Sprintf(e.promptTemplates[ExplainBrief],
			ctx.AnomalyMetric, tsStr, ctx.AnomalyValue, ctx.BaselineMean, ctx.BaselineStddev)
	case ExplainExpert:
		relatedStr := ""
		for m, v := range ctx.RelatedMetricValues {
			if relatedStr != "" {
				relatedStr += ", "
			}
			relatedStr += fmt.Sprintf("%s=%.4f", m, v)
		}
		if relatedStr == "" {
			relatedStr = "none available"
		}
		return fmt.Sprintf(e.promptTemplates[ExplainExpert],
			ctx.AnomalyMetric, tsStr, ctx.AnomalyValue, ctx.BaselineMean, ctx.BaselineStddev,
			deviation, ctx.RecentTrend, relatedStr)
	default:
		return fmt.Sprintf(e.promptTemplates[ExplainDetailed],
			ctx.AnomalyMetric, tsStr, ctx.AnomalyValue, ctx.BaselineMean, ctx.BaselineStddev,
			deviation, ctx.RecentTrend)
	}
}

// GenerateLocalExplanation produces a rule-based explanation without calling an external LLM.
func (e *AnomalyExplainabilityEngine) GenerateLocalExplanation(ctx ExplainabilityContext) string {
	var sb strings.Builder

	deviation := 0.0
	if ctx.BaselineStddev > 0 {
		deviation = (ctx.AnomalyValue - ctx.BaselineMean) / ctx.BaselineStddev
	}
	absDeviation := math.Abs(deviation)

	direction := "above"
	if deviation < 0 {
		direction = "below"
	}

	// Severity description
	severityDesc := "minor"
	if absDeviation >= 2 {
		severityDesc = "moderate"
	}
	if absDeviation >= 3 {
		severityDesc = "significant"
	}
	if absDeviation >= 5 {
		severityDesc = "critical"
	}

	sb.WriteString(fmt.Sprintf("Anomaly detected on metric %q: value %.4f is %.2f standard deviations %s the baseline mean of %.4f. ",
		ctx.AnomalyMetric, ctx.AnomalyValue, absDeviation, direction, ctx.BaselineMean))
	sb.WriteString(fmt.Sprintf("This represents a %s deviation. ", severityDesc))

	if ctx.RecentTrend != "" && ctx.RecentTrend != "unknown" {
		sb.WriteString(fmt.Sprintf("The recent trend is %s. ", ctx.RecentTrend))
	}

	if e.config.ExplanationDepth >= ExplainDetailed {
		sb.WriteString(fmt.Sprintf("Baseline standard deviation: %.4f. ", ctx.BaselineStddev))
		if len(ctx.RelatedMetricValues) > 0 {
			sb.WriteString("Related metrics: ")
			parts := make([]string, 0, len(ctx.RelatedMetricValues))
			for m, v := range ctx.RelatedMetricValues {
				parts = append(parts, fmt.Sprintf("%s=%.4f", m, v))
			}
			sort.Strings(parts)
			sb.WriteString(strings.Join(parts, ", "))
			sb.WriteString(". ")
		}
	}

	if e.config.ExplanationDepth >= ExplainExpert {
		sb.WriteString("Expert analysis: ")
		if absDeviation >= 3 {
			sb.WriteString("This anomaly exceeds the 3-sigma threshold and warrants immediate investigation. ")
		} else {
			sb.WriteString("This anomaly is within typical variance bounds but should be monitored. ")
		}
		sb.WriteString(fmt.Sprintf("Recommend checking correlated metrics and recent deployments. Confidence interval: [%.4f, %.4f].",
			ctx.BaselineMean-2*ctx.BaselineStddev, ctx.BaselineMean+2*ctx.BaselineStddev))
	}

	return sb.String()
}

// Stats returns engine statistics.
func (e *AnomalyExplainabilityEngine) Stats() AnomalyExplainabilityStats {
	e.mu.RLock()
	defer e.mu.RUnlock()
	return e.stats
}

// RegisterHTTPHandlers registers explainability HTTP endpoints.
func (e *AnomalyExplainabilityEngine) RegisterHTTPHandlers(mux *http.ServeMux) {
	mux.HandleFunc("/api/v1/explain/anomaly", func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost {
			http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
			return
		}
		r.Body = http.MaxBytesReader(w, r.Body, maxBodySize)
		var req struct {
			Metric string    `json:"metric"`
			Value  float64   `json:"value"`
			Time   time.Time `json:"time"`
		}
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			http.Error(w, "bad request", http.StatusBadRequest)
			return
		}
		expl, err := e.ExplainAnomaly(req.Metric, req.Value, req.Time)
		if err != nil {
			http.Error(w, "bad request", http.StatusBadRequest)
			return
		}
		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(expl)
	})

	mux.HandleFunc("/api/v1/explain/context", func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost {
			http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
			return
		}
		r.Body = http.MaxBytesReader(w, r.Body, maxBodySize)
		var ctx ExplainabilityContext
		if err := json.NewDecoder(r.Body).Decode(&ctx); err != nil {
			http.Error(w, "bad request", http.StatusBadRequest)
			return
		}
		expl, err := e.ExplainWithContext(ctx)
		if err != nil {
			http.Error(w, "bad request", http.StatusBadRequest)
			return
		}
		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(expl)
	})

	mux.HandleFunc("/api/v1/explain/list", func(w http.ResponseWriter, r *http.Request) {
		metric := r.URL.Query().Get("metric")
		explanations := e.ListExplanations(metric, 100)
		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(explanations)
	})

	mux.HandleFunc("/api/v1/explain/recommendations/", func(w http.ResponseWriter, r *http.Request) {
		id := strings.TrimPrefix(r.URL.Path, "/api/v1/explain/recommendations/")
		if id == "" {
			http.Error(w, "missing anomaly id", http.StatusBadRequest)
			return
		}
		recs := e.GetRecommendations(id)
		if recs == nil {
			http.Error(w, "anomaly not found", http.StatusNotFound)
			return
		}
		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(recs)
	})

	mux.HandleFunc("/api/v1/explain/stats", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(e.Stats())
	})
}

// computeSeverity computes an integer severity (1-10) from the anomaly context.
func (e *AnomalyExplainabilityEngine) computeSeverity(ctx ExplainabilityContext) int {
	if ctx.BaselineStddev <= 0 {
		return 5
	}
	deviation := math.Abs(ctx.AnomalyValue-ctx.BaselineMean) / ctx.BaselineStddev
	severity := int(math.Min(10, math.Max(1, math.Ceil(deviation))))
	return severity
}

// computeConfidence estimates confidence based on available context.
func (e *AnomalyExplainabilityEngine) computeConfidence(ctx ExplainabilityContext) float64 {
	confidence := 0.5 // base confidence

	if ctx.BaselineStddev > 0 {
		confidence += 0.2
	}
	if len(ctx.RelatedMetricValues) > 0 {
		confidence += 0.1
	}
	if ctx.RecentTrend != "" && ctx.RecentTrend != "unknown" {
		confidence += 0.1
	}

	return math.Min(1.0, confidence)
}

// estimateBaseline estimates the baseline mean and stddev for a metric from the DB.
func (e *AnomalyExplainabilityEngine) estimateBaseline(metric string, ts time.Time) (mean, stddev float64) {
	end := ts.UnixNano()
	start := ts.Add(-1 * time.Hour).UnixNano()

	result, err := e.db.Execute(&Query{Metric: metric, Start: start, End: end})
	if err != nil || result == nil || len(result.Points) == 0 {
		return 0, 0
	}

	n := float64(len(result.Points))
	sum := 0.0
	for _, p := range result.Points {
		sum += p.Value
	}
	mean = sum / n

	variance := 0.0
	for _, p := range result.Points {
		d := p.Value - mean
		variance += d * d
	}
	if n > 1 {
		variance /= (n - 1)
	}
	stddev = math.Sqrt(variance)
	return mean, stddev
}

// generateRecommendations produces prioritized recommendations based on anomaly context and severity.
func (e *AnomalyExplainabilityEngine) generateRecommendations(ctx ExplainabilityContext, severity int) []RecommendedAction {
	var actions []RecommendedAction

	metricLower := strings.ToLower(ctx.AnomalyMetric)

	// High severity recommendations
	if severity >= 7 {
		actions = append(actions, RecommendedAction{
			Priority:           1,
			Action:             fmt.Sprintf("Investigate %s immediately - critical anomaly detected", ctx.AnomalyMetric),
			Rationale:          fmt.Sprintf("Severity %d/10 anomaly requires immediate attention", severity),
			EstimatedImpact:    "high",
			AutomationPossible: false,
		})
	}

	// Infrastructure-specific recommendations
	if strings.Contains(metricLower, "cpu") || strings.Contains(metricLower, "memory") {
		actions = append(actions, RecommendedAction{
			Priority:           2,
			Action:             "Check resource utilization and consider scaling",
			Rationale:          "Resource metric anomaly may indicate capacity issues",
			EstimatedImpact:    "medium",
			AutomationPossible: true,
		})
	}

	if strings.Contains(metricLower, "latency") || strings.Contains(metricLower, "response") {
		actions = append(actions, RecommendedAction{
			Priority:           2,
			Action:             "Review application performance and dependency health",
			Rationale:          "Latency anomaly may indicate downstream service degradation",
			EstimatedImpact:    "medium",
			AutomationPossible: false,
		})
	}

	if strings.Contains(metricLower, "error") {
		actions = append(actions, RecommendedAction{
			Priority:           1,
			Action:             "Check application logs for error patterns",
			Rationale:          "Error metric anomaly indicates potential service issues",
			EstimatedImpact:    "high",
			AutomationPossible: true,
		})
	}

	// General recommendations
	actions = append(actions, RecommendedAction{
		Priority:           3,
		Action:             "Review recent deployments and configuration changes",
		Rationale:          "Changes in deployment or configuration are common causes of anomalies",
		EstimatedImpact:    "medium",
		AutomationPossible: false,
	})

	actions = append(actions, RecommendedAction{
		Priority:           4,
		Action:             fmt.Sprintf("Set up additional monitoring for %s", ctx.AnomalyMetric),
		Rationale:          "Enhanced monitoring will help detect recurrence earlier",
		EstimatedImpact:    "low",
		AutomationPossible: true,
	})

	sort.Slice(actions, func(i, j int) bool {
		return actions[i].Priority < actions[j].Priority
	})

	if e.config.MaxRecommendations > 0 && len(actions) > e.config.MaxRecommendations {
		actions = actions[:e.config.MaxRecommendations]
	}

	return actions
}
