package chronicle

// alert_builder_http.go contains HTTP route handlers for the AlertBuilder.
// See alert_builder.go for types and CRUD operations.

import (
	"encoding/json"
	"fmt"
	"net/http"
	"regexp"
	"strings"
	"sync/atomic"
	"time"
)

var alertIDSequence atomic.Int64

func generateAlertID() string {
	// Rules and triggers share this allocator. Clock resolution or rollback
	// must not cause a new record to overwrite one created earlier.
	for {
		previous := alertIDSequence.Load()
		next := time.Now().UnixNano()
		if next <= previous {
			next = previous + 1
		}
		if alertIDSequence.CompareAndSwap(previous, next) {
			return fmt.Sprintf("alert-%d", next)
		}
	}
}

// ExportRules exports rules as JSON.
func (ab *AlertBuilder) ExportRules() ([]byte, error) {
	ab.mu.RLock()
	defer ab.mu.RUnlock()

	rules := make([]*BuilderRule, 0, len(ab.rules))
	for _, r := range ab.rules {
		rules = append(rules, r)
	}

	return json.Marshal(rules)
}

// ImportRules imports rules from JSON.
func (ab *AlertBuilder) ImportRules(data []byte) error {
	var rules []*BuilderRule
	if err := json.Unmarshal(data, &rules); err != nil {
		return fmt.Errorf("failed to unmarshal rules: %w", err)
	}

	ab.mu.Lock()
	defer ab.mu.Unlock()

	for _, rule := range rules {
		if err := ab.validateRule(rule); err != nil {
			return fmt.Errorf("invalid rule %q: %w", rule.Name, err)
		}
		ab.rules[rule.ID] = rule
	}

	return nil
}

// ValidateExpression validates a rule expression string.
func (ab *AlertBuilder) ValidateExpression(expr string) error {
	// Simple validation for common patterns
	if expr == "" {
		return fmt.Errorf("expression cannot be empty")
	}

	// Check for valid metric pattern
	metricPattern := regexp.MustCompile(`^[a-zA-Z_][a-zA-Z0-9_.]*\s*(>|>=|<|<=|==|!=)\s*[\d.]+$`)
	if !metricPattern.MatchString(strings.TrimSpace(expr)) {
		return fmt.Errorf("invalid expression format: expected 'metric operator value'")
	}

	return nil
}

// HTTP Handlers

// setupAlertBuilderRoutes sets up HTTP routes for the alert builder.
func setupAlertBuilderRoutes(mux *http.ServeMux, ab *AlertBuilder, wrap func(http.HandlerFunc) http.HandlerFunc) {
	mux.HandleFunc("/api/v1/alerts/rules", wrap(ab.handleRules))
	mux.HandleFunc("/api/v1/alerts/rules/", wrap(ab.handleRule))
	mux.HandleFunc("/api/v1/alerts/preview", wrap(ab.handlePreview))
	mux.HandleFunc("/api/v1/alerts/triggers", wrap(ab.handleTriggers))
	mux.HandleFunc("/api/v1/alerts/triggers/", wrap(ab.handleTrigger))
	mux.HandleFunc("/api/v1/alerts/metrics", wrap(ab.handleMetrics))
	mux.HandleFunc("/api/v1/alerts/templates", wrap(ab.handleTemplates))
	mux.HandleFunc("/api/v1/alerts/validate", wrap(ab.handleValidate))
}

func (ab *AlertBuilder) handleRules(w http.ResponseWriter, r *http.Request) {
	switch r.Method {
	case http.MethodGet:
		rules := ab.ListRules()
		writeJSONStatus(w, http.StatusOK, rules)

	case http.MethodPost:
		var rule BuilderRule
		r.Body = http.MaxBytesReader(w, r.Body, maxBodySize)
		if err := json.NewDecoder(r.Body).Decode(&rule); err != nil {
			writeJSONStatus(w, http.StatusBadRequest, map[string]string{"error": err.Error()})
			return
		}

		if err := ab.CreateRule(&rule); err != nil {
			writeJSONStatus(w, http.StatusBadRequest, map[string]string{"error": err.Error()})
			return
		}

		writeJSONStatus(w, http.StatusCreated, rule)

	default:
		w.WriteHeader(http.StatusMethodNotAllowed)
	}
}

func (ab *AlertBuilder) handleRule(w http.ResponseWriter, r *http.Request) {
	id := strings.TrimPrefix(r.URL.Path, "/api/v1/alerts/rules/")
	if id == "" {
		writeJSONStatus(w, http.StatusBadRequest, map[string]string{"error": "rule ID required"})
		return
	}

	switch r.Method {
	case http.MethodGet:
		rule, err := ab.GetRule(id)
		if err != nil {
			writeJSONStatus(w, http.StatusNotFound, map[string]string{"error": err.Error()})
			return
		}
		writeJSONStatus(w, http.StatusOK, rule)

	case http.MethodPut:
		var rule BuilderRule
		r.Body = http.MaxBytesReader(w, r.Body, maxBodySize)
		if err := json.NewDecoder(r.Body).Decode(&rule); err != nil {
			writeJSONStatus(w, http.StatusBadRequest, map[string]string{"error": err.Error()})
			return
		}

		if err := ab.UpdateRule(id, &rule); err != nil {
			writeJSONStatus(w, http.StatusBadRequest, map[string]string{"error": err.Error()})
			return
		}

		writeJSONStatus(w, http.StatusOK, rule)

	case http.MethodDelete:
		if err := ab.DeleteRule(id); err != nil {
			writeJSONStatus(w, http.StatusNotFound, map[string]string{"error": err.Error()})
			return
		}
		w.WriteHeader(http.StatusNoContent)

	default:
		w.WriteHeader(http.StatusMethodNotAllowed)
	}
}

func (ab *AlertBuilder) handlePreview(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		w.WriteHeader(http.StatusMethodNotAllowed)
		return
	}

	var rule BuilderRule
	r.Body = http.MaxBytesReader(w, r.Body, maxBodySize)
	if err := json.NewDecoder(r.Body).Decode(&rule); err != nil {
		writeJSONStatus(w, http.StatusBadRequest, map[string]string{"error": err.Error()})
		return
	}

	result, err := ab.PreviewRule(&rule)
	if err != nil {
		writeJSONStatus(w, http.StatusBadRequest, map[string]string{"error": err.Error()})
		return
	}

	writeJSONStatus(w, http.StatusOK, result)
}

func (ab *AlertBuilder) handleTriggers(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		w.WriteHeader(http.StatusMethodNotAllowed)
		return
	}

	state := TriggerState(r.URL.Query().Get("state"))
	triggers := ab.ListTriggers(state)
	writeJSONStatus(w, http.StatusOK, triggers)
}

func (ab *AlertBuilder) handleTrigger(w http.ResponseWriter, r *http.Request) {
	id := strings.TrimPrefix(r.URL.Path, "/api/v1/alerts/triggers/")
	if id == "" {
		writeJSONStatus(w, http.StatusBadRequest, map[string]string{"error": "trigger ID required"})
		return
	}

	switch r.Method {
	case http.MethodGet:
		trigger, err := ab.GetTrigger(id)
		if err != nil {
			writeJSONStatus(w, http.StatusNotFound, map[string]string{"error": err.Error()})
			return
		}
		writeJSONStatus(w, http.StatusOK, trigger)

	case http.MethodPost:
		// Acknowledge trigger
		action := r.URL.Query().Get("action")
		if action == "acknowledge" {
			if err := ab.AcknowledgeTrigger(id); err != nil {
				writeJSONStatus(w, http.StatusBadRequest, map[string]string{"error": err.Error()})
				return
			}
			w.WriteHeader(http.StatusOK)
		} else {
			writeJSONStatus(w, http.StatusBadRequest, map[string]string{"error": "unknown action"})
		}

	default:
		w.WriteHeader(http.StatusMethodNotAllowed)
	}
}

func (ab *AlertBuilder) handleMetrics(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		w.WriteHeader(http.StatusMethodNotAllowed)
		return
	}

	metrics := ab.GetAvailableMetrics()
	writeJSONStatus(w, http.StatusOK, metrics)
}

func (ab *AlertBuilder) handleTemplates(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		w.WriteHeader(http.StatusMethodNotAllowed)
		return
	}

	templates := ab.GetConditionTemplates()
	writeJSONStatus(w, http.StatusOK, templates)
}

func (ab *AlertBuilder) handleValidate(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		w.WriteHeader(http.StatusMethodNotAllowed)
		return
	}

	var req struct {
		Expression string       `json:"expression"`
		Rule       *BuilderRule `json:"rule,omitempty"`
	}
	r.Body = http.MaxBytesReader(w, r.Body, maxBodySize)
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		writeJSONStatus(w, http.StatusBadRequest, map[string]string{"error": err.Error()})
		return
	}

	if req.Expression != "" {
		if err := ab.ValidateExpression(req.Expression); err != nil {
			writeJSONStatus(w, http.StatusOK, map[string]any{
				"valid": false,
				"error": err.Error(),
			})
			return
		}
	}

	if req.Rule != nil {
		if err := ab.validateRule(req.Rule); err != nil {
			writeJSONStatus(w, http.StatusOK, map[string]any{
				"valid": false,
				"error": err.Error(),
			})
			return
		}
	}

	writeJSONStatus(w, http.StatusOK, map[string]any{
		"valid": true,
	})
}
