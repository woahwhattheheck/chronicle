package chronicle

import (
	"encoding/json"
	"fmt"
	"regexp"
	"strings"
	"time"
)

// Panel description parsing and metric extraction for NL dashboards.

func (e *NLDashboardEngine) parsePanelDescription(description string) *PanelDescription {
	// Match against patterns
	var bestMatch *NLPattern
	var bestSubmatch []string
	bestPriority := -1

	for _, pattern := range e.patterns {
		if matches := pattern.Pattern.FindStringSubmatch(description); len(matches) > 0 {
			if pattern.Priority > bestPriority {
				bestMatch = pattern
				bestSubmatch = matches
				bestPriority = pattern.Priority
			}
		}
	}

	if bestMatch == nil {
		// Try to extract metrics anyway
		metrics := e.extractMetrics(description)
		if len(metrics) == 0 {
			return nil
		}
		return &PanelDescription{
			Title:   metrics[0],
			Type:    PanelTimeseries,
			Metrics: metrics,
		}
	}

	// Extract metric from match
	metricPart := ""
	if len(bestSubmatch) > 1 {
		metricPart = bestSubmatch[1]
	} else {
		metricPart = description
	}

	metrics := e.extractMetrics(metricPart)
	tags := e.extractMetricTags(metricPart)
	groupBy := e.extractGroupBy(description)

	title := metricPart
	if len(metrics) > 0 {
		title = strings.Join(metrics, " & ")
	}

	return &PanelDescription{
		Title:       strings.Title(title),
		Type:        bestMatch.PanelType,
		Metrics:     metrics,
		Tags:        tags,
		Aggregation: bestMatch.Aggregation,
		GroupBy:     groupBy,
	}
}

func (e *NLDashboardEngine) extractMetrics(description string) []string {
	metrics := make([]string, 0)

	// Common metric patterns
	metricPatterns := []*regexp.Regexp{
		regexp.MustCompile(`(?i)(cpu|memory|disk|network|http|request|error|latency|throughput|connections?|bytes|packets|load|temperature|humidity|pressure)`),
		regexp.MustCompile(`(?i)(\w+_\w+(?:_\w+)*)`),   // snake_case metrics
		regexp.MustCompile(`(?i)(\w+\.\w+(?:\.\w+)*)`), // dot.separated.metrics
	}

	for _, pattern := range metricPatterns {
		matches := pattern.FindAllString(description, -1)
		for _, match := range matches {
			normalized := strings.ToLower(match)
			if !nlContains(metrics, normalized) {
				metrics = append(metrics, normalized)
			}
		}
	}

	return metrics
}

func (e *NLDashboardEngine) extractMetricTags(description string) map[string]string {
	tags := make(map[string]string)

	// Look for tag specifications
	tagPatterns := []*regexp.Regexp{
		regexp.MustCompile(`(?i)(?:where|for|with)\s+(\w+)\s*[=:]\s*"?(\w+)"?`),
		regexp.MustCompile(`(?i)(\w+)\s*[=:]\s*"([^"]+)"`),
	}

	for _, pattern := range tagPatterns {
		matches := pattern.FindAllStringSubmatch(description, -1)
		for _, match := range matches {
			if len(match) > 2 {
				tags[match[1]] = match[2]
			}
		}
	}

	return tags
}

func (e *NLDashboardEngine) extractGroupBy(description string) []string {
	groupBy := make([]string, 0)

	patterns := []*regexp.Regexp{
		regexp.MustCompile(`(?i)(?:group|grouped|split|broken\s+down)\s+by\s+(\w+(?:\s*,\s*\w+)*)`),
		regexp.MustCompile(`(?i)per\s+(\w+)`),
		regexp.MustCompile(`(?i)by\s+(\w+)`),
	}

	for _, pattern := range patterns {
		if matches := pattern.FindStringSubmatch(description); len(matches) > 1 {
			parts := strings.Split(matches[1], ",")
			for _, p := range parts {
				groupBy = append(groupBy, strings.TrimSpace(p))
			}
		}
	}

	return groupBy
}

func (e *NLDashboardEngine) createPanel(id int, desc *PanelDescription, col, row int) *PanelSpec {
	// Calculate grid position
	panelWidth := 24 / e.config.MaxPanelsPerRow
	panelHeight := 8

	panel := &PanelSpec{
		ID:    id,
		Title: desc.Title,
		Type:  desc.Type,
		GridPos: &GridPos{
			H: panelHeight,
			W: panelWidth,
			X: col * panelWidth,
			Y: row * panelHeight,
		},
		Targets: make([]*TargetSpec, 0),
		Options: make(map[string]any),
	}

	// Add targets for each metric
	refID := 'A'
	for _, metric := range desc.Metrics {
		target := &TargetSpec{
			RefID:       string(refID),
			Metric:      metric,
			Tags:        desc.Tags,
			Aggregation: desc.Aggregation,
			GroupBy:     desc.GroupBy,
		}
		panel.Targets = append(panel.Targets, target)
		refID++
	}

	// Set type-specific options
	switch desc.Type {
	case PanelStat:
		panel.Options["colorMode"] = "value"
		panel.Options["graphMode"] = "area"
		panel.Options["justifyMode"] = "auto"
		panel.Options["textMode"] = "auto"

	case PanelGauge:
		panel.Options["showThresholdLabels"] = false
		panel.Options["showThresholdMarkers"] = true
		panel.FieldConfig = &FieldConfig{
			Defaults: &FieldDefaults{
				Min: ptrFloat64(0),
				Max: ptrFloat64(100),
			},
		}
		panel.Thresholds = []*Threshold{
			{Value: 0, Color: "green"},
			{Value: 70, Color: "yellow"},
			{Value: 90, Color: "red"},
		}

	case PanelTimeseries:
		panel.Options["legend"] = map[string]any{
			"displayMode": "list",
			"placement":   "bottom",
		}
		panel.Options["tooltip"] = map[string]any{
			"mode": "single",
		}

	case PanelTable:
		panel.Options["showHeader"] = true

	case PanelPieChart:
		panel.Options["pieType"] = "pie"
		panel.Options["displayLabels"] = []string{"percent"}
	}

	return panel
}

// ToGrafanaJSON converts the dashboard spec to Grafana JSON format.
func (e *NLDashboardEngine) ToGrafanaJSON(spec *DashboardSpec) ([]byte, error) {
	// Build Grafana dashboard structure
	grafana := map[string]any{
		"id":            nil,
		"uid":           spec.ID,
		"title":         spec.Title,
		"description":   spec.Description,
		"tags":          spec.Tags,
		"timezone":      "browser",
		"schemaVersion": 38,
		"version":       1,
		"refresh":       spec.Refresh,
		"time": map[string]string{
			"from": spec.TimeRange.From,
			"to":   spec.TimeRange.To,
		},
		"panels":      e.convertPanels(spec.Panels),
		"templating":  e.convertVariables(spec.Variables),
		"annotations": e.convertAnnotations(spec.Annotations),
		"links":       e.convertLinks(spec.Links),
	}

	return json.MarshalIndent(grafana, "", "  ")
}

func (e *NLDashboardEngine) convertPanels(panels []*PanelSpec) []map[string]any {
	result := make([]map[string]any, 0, len(panels))

	for _, panel := range panels {
		p := map[string]any{
			"id":      panel.ID,
			"title":   panel.Title,
			"type":    string(panel.Type),
			"gridPos": panel.GridPos,
			"options": panel.Options,
			"targets": e.convertTargets(panel.Targets),
		}

		if panel.FieldConfig != nil {
			p["fieldConfig"] = panel.FieldConfig
		}

		if len(panel.Thresholds) > 0 {
			p["fieldConfig"] = map[string]any{
				"defaults": map[string]any{
					"thresholds": map[string]any{
						"mode":  "absolute",
						"steps": panel.Thresholds,
					},
				},
			}
		}

		result = append(result, p)
	}

	return result
}

func (e *NLDashboardEngine) convertTargets(targets []*TargetSpec) []map[string]any {
	result := make([]map[string]any, 0, len(targets))

	for _, target := range targets {
		t := map[string]any{
			"refId": target.RefID,
		}

		// Build Chronicle query
		query := target.Metric
		if target.Aggregation != "" {
			query = fmt.Sprintf("%s(%s)", target.Aggregation, target.Metric)
		}

		if len(target.Tags) > 0 {
			filters := make([]string, 0)
			for k, v := range target.Tags {
				filters = append(filters, fmt.Sprintf(`%s="%s"`, sanitizeIdentifier(k), escapeSQL(v)))
			}
			query = fmt.Sprintf("%s{%s}", query, strings.Join(filters, ", "))
		}

		if len(target.GroupBy) > 0 {
			sanitized := make([]string, len(target.GroupBy))
			for i, g := range target.GroupBy {
				sanitized[i] = sanitizeIdentifier(g)
			}
			query = fmt.Sprintf("%s by (%s)", query, strings.Join(sanitized, ", "))
		}

		t["expr"] = query
		t["legendFormat"] = target.Alias

		if target.RawQuery != "" {
			t["rawQuery"] = true
			t["query"] = target.RawQuery
		}

		result = append(result, t)
	}

	return result
}

func (e *NLDashboardEngine) convertVariables(variables []*VariableSpec) map[string]any {
	list := make([]map[string]any, 0)

	for _, v := range variables {
		list = append(list, map[string]any{
			"name":    v.Name,
			"type":    v.Type,
			"label":   v.Label,
			"query":   v.Query,
			"multi":   v.Multi,
			"current": map[string]any{"text": v.Current, "value": v.Current},
		})
	}

	return map[string]any{"list": list}
}

func (e *NLDashboardEngine) convertAnnotations(annotations []*AnnotationSpec) map[string]any {
	list := make([]map[string]any, 0)

	for _, a := range annotations {
		list = append(list, map[string]any{
			"name":      a.Name,
			"enable":    a.Enable,
			"query":     a.Query,
			"iconColor": a.IconColor,
		})
	}

	return map[string]any{"list": list}
}

func (e *NLDashboardEngine) convertLinks(links []*LinkSpec) []map[string]any {
	result := make([]map[string]any, 0)

	for _, l := range links {
		result = append(result, map[string]any{
			"title": l.Title,
			"url":   l.URL,
			"type":  l.Type,
		})
	}

	return result
}

// GetDashboard returns a dashboard by ID.
func (e *NLDashboardEngine) GetDashboard(id string) (*DashboardSpec, error) {
	e.dashboardsMu.RLock()
	defer e.dashboardsMu.RUnlock()

	dashboard, ok := e.dashboards[id]
	if !ok {
		return nil, fmt.Errorf("dashboard not found: %s", id)
	}
	return dashboard, nil
}

// ListDashboards returns all dashboards.
func (e *NLDashboardEngine) ListDashboards() []*DashboardSpec {
	e.dashboardsMu.RLock()
	defer e.dashboardsMu.RUnlock()

	result := make([]*DashboardSpec, 0, len(e.dashboards))
	for _, d := range e.dashboards {
		result = append(result, d)
	}
	return result
}

// ProvideFeedback records feedback for a dashboard.
func (e *NLDashboardEngine) ProvideFeedback(dashboardID string, accepted bool, correction string) {
	if !e.config.EnableFeedback {
		return
	}

	e.dashboardsMu.RLock()
	dashboard, ok := e.dashboards[dashboardID]
	e.dashboardsMu.RUnlock()

	if !ok {
		return
	}

	e.feedbackMu.Lock()
	defer e.feedbackMu.Unlock()

	e.feedback = append(e.feedback, DashboardFeedback{
		OriginalNL:  dashboard.SourceNL,
		CorrectedNL: correction,
		Accepted:    accepted,
		Timestamp:   time.Now(),
	})
}

// Stats returns engine statistics.
func (e *NLDashboardEngine) Stats() NLDashboardStats {
	e.dashboardsMu.RLock()
	dashboardCount := len(e.dashboards)
	e.dashboardsMu.RUnlock()

	e.feedbackMu.Lock()
	feedbackCount := len(e.feedback)
	acceptedCount := 0
	for _, f := range e.feedback {
		if f.Accepted {
			acceptedCount++
		}
	}
	e.feedbackMu.Unlock()

	return NLDashboardStats{
		DashboardsGenerated: e.dashboardsGenerated.Load(),
		PanelsGenerated:     e.panelsGenerated.Load(),
		DashboardsCached:    dashboardCount,
		FeedbackReceived:    feedbackCount,
		AcceptanceRate:      float64(acceptedCount) / float64(max(feedbackCount, 1)),
	}
}

// NLDashboardStats contains engine statistics.
type NLDashboardStats struct {
	DashboardsGenerated int64   `json:"dashboards_generated"`
	PanelsGenerated     int64   `json:"panels_generated"`
	DashboardsCached    int     `json:"dashboards_cached"`
	FeedbackReceived    int     `json:"feedback_received"`
	AcceptanceRate      float64 `json:"acceptance_rate"`
}

// Helper functions

func ptrFloat64(f float64) *float64 {
	return &f
}

func nlContains(slice []string, item string) bool {
	for _, s := range slice {
		if s == item {
			return true
		}
	}
	return false
}

func uniqueStrings(slice []string) []string {
	seen := make(map[string]bool)
	result := make([]string, 0)
	for _, s := range slice {
		if !seen[s] {
			seen[s] = true
			result = append(result, s)
		}
	}
	return result
}
