package chronicle

import (
	"context"
	"fmt"
	"regexp"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"
)

// EXPERIMENTAL: This API is unstable and may change without notice.
// NLDashboardConfig configures the natural language dashboard generator.
type NLDashboardConfig struct {
	// Enabled turns on NL dashboard generation
	Enabled bool

	// DefaultTimeRange for dashboards
	DefaultTimeRange string

	// DefaultRefreshInterval for panels
	DefaultRefreshInterval string

	// MaxPanelsPerRow in dashboards
	MaxPanelsPerRow int

	// EnableAutoComplete for metric suggestions
	EnableAutoComplete bool

	// EnableFeedback for learning from corrections
	EnableFeedback bool
}

// DefaultNLDashboardConfig returns default configuration.
func DefaultNLDashboardConfig() NLDashboardConfig {
	return NLDashboardConfig{
		Enabled:                true,
		DefaultTimeRange:       "now-1h",
		DefaultRefreshInterval: "30s",
		MaxPanelsPerRow:        3,
		EnableAutoComplete:     true,
		EnableFeedback:         true,
	}
}

// DashboardSpec represents the intermediate dashboard specification.
type DashboardSpec struct {
	ID          string            `json:"id"`
	Title       string            `json:"title"`
	Description string            `json:"description,omitempty"`
	Tags        []string          `json:"tags,omitempty"`
	TimeRange   *TimeRangeSpec    `json:"time_range,omitempty"`
	Refresh     string            `json:"refresh,omitempty"`
	Panels      []*PanelSpec      `json:"panels"`
	Variables   []*VariableSpec   `json:"variables,omitempty"`
	Annotations []*AnnotationSpec `json:"annotations,omitempty"`
	Links       []*LinkSpec       `json:"links,omitempty"`
	CreatedAt   time.Time         `json:"created_at"`
	SourceNL    string            `json:"source_nl,omitempty"`
}

// TimeRangeSpec represents a time range.
type TimeRangeSpec struct {
	From string `json:"from"`
	To   string `json:"to"`
}

// PanelSpec represents a dashboard panel.
type PanelSpec struct {
	ID          int            `json:"id"`
	Title       string         `json:"title"`
	Type        PanelType      `json:"type"`
	GridPos     *GridPos       `json:"gridPos"`
	Targets     []*TargetSpec  `json:"targets"`
	Options     map[string]any `json:"options,omitempty"`
	FieldConfig *FieldConfig   `json:"fieldConfig,omitempty"`
	Thresholds  []*Threshold   `json:"thresholds,omitempty"`
}

// PanelType defines the type of panel.
type PanelType string

const (
	PanelGraph      PanelType = "graph"
	PanelTimeseries PanelType = "timeseries"
	PanelStat       PanelType = "stat"
	PanelGauge      PanelType = "gauge"
	PanelTable      PanelType = "table"
	PanelHeatmap    PanelType = "heatmap"
	PanelPieChart   PanelType = "piechart"
	PanelText       PanelType = "text"
	PanelAlert      PanelType = "alertlist"
	PanelLogs       PanelType = "logs"
)

// GridPos represents panel position.
type GridPos struct {
	H int `json:"h"`
	W int `json:"w"`
	X int `json:"x"`
	Y int `json:"y"`
}

// TargetSpec represents a query target.
type TargetSpec struct {
	RefID       string            `json:"refId"`
	Metric      string            `json:"metric"`
	Tags        map[string]string `json:"tags,omitempty"`
	Aggregation string            `json:"aggregation,omitempty"`
	GroupBy     []string          `json:"groupBy,omitempty"`
	Alias       string            `json:"alias,omitempty"`
	RawQuery    string            `json:"rawQuery,omitempty"`
}

// FieldConfig for panel visualization.
type FieldConfig struct {
	Defaults  *FieldDefaults `json:"defaults,omitempty"`
	Overrides []any          `json:"overrides,omitempty"`
}

// FieldDefaults for panel fields.
type FieldDefaults struct {
	Unit     string       `json:"unit,omitempty"`
	Decimals int          `json:"decimals,omitempty"`
	Min      *float64     `json:"min,omitempty"`
	Max      *float64     `json:"max,omitempty"`
	Color    *ColorConfig `json:"color,omitempty"`
}

// ColorConfig for visualization colors.
type ColorConfig struct {
	Mode       string `json:"mode"`
	FixedColor string `json:"fixedColor,omitempty"`
}

// Threshold for panel thresholds.
type Threshold struct {
	Value float64 `json:"value"`
	Color string  `json:"color"`
}

// VariableSpec represents a dashboard variable.
type VariableSpec struct {
	Name    string   `json:"name"`
	Type    string   `json:"type"`
	Label   string   `json:"label,omitempty"`
	Query   string   `json:"query,omitempty"`
	Options []string `json:"options,omitempty"`
	Multi   bool     `json:"multi,omitempty"`
	Current string   `json:"current,omitempty"`
}

// AnnotationSpec represents a dashboard annotation.
type AnnotationSpec struct {
	Name      string `json:"name"`
	Enable    bool   `json:"enable"`
	Query     string `json:"query,omitempty"`
	IconColor string `json:"iconColor,omitempty"`
}

// LinkSpec represents a dashboard link.
type LinkSpec struct {
	Title string `json:"title"`
	URL   string `json:"url"`
	Type  string `json:"type"`
}

// NLDashboardEngine generates dashboards from natural language.
type NLDashboardEngine struct {
	db     *DB
	config NLDashboardConfig

	// Pattern matchers
	patterns []*NLPattern

	// Dashboard cache
	dashboards   map[string]*DashboardSpec
	dashboardsMu sync.RWMutex

	// Feedback for learning
	feedback   []DashboardFeedback
	feedbackMu sync.Mutex

	// Stats
	dashboardsGenerated atomic.Int64
	panelsGenerated     atomic.Int64
}

// NLPattern represents a natural language pattern.
type NLPattern struct {
	Pattern     *regexp.Regexp
	PanelType   PanelType
	Aggregation string
	Description string
	Priority    int
}

// DashboardFeedback for learning from corrections.
type DashboardFeedback struct {
	OriginalNL  string    `json:"original_nl"`
	CorrectedNL string    `json:"corrected_nl,omitempty"`
	Accepted    bool      `json:"accepted"`
	Timestamp   time.Time `json:"timestamp"`
}

// NewNLDashboardEngine creates a new NL dashboard engine.
func NewNLDashboardEngine(db *DB, config NLDashboardConfig) *NLDashboardEngine {
	engine := &NLDashboardEngine{
		db:         db,
		config:     config,
		patterns:   initPatterns(),
		dashboards: make(map[string]*DashboardSpec),
		feedback:   make([]DashboardFeedback, 0),
	}

	return engine
}

func initPatterns() []*NLPattern {
	return []*NLPattern{
		// Time series patterns
		{regexp.MustCompile(`(?i)show\s+(?:me\s+)?(.+?)\s+over\s+time`), PanelTimeseries, "", "Time series visualization", 10},
		{regexp.MustCompile(`(?i)graph\s+(?:of\s+)?(.+)`), PanelTimeseries, "", "Graph visualization", 9},
		{regexp.MustCompile(`(?i)trend\s+(?:of\s+)?(.+)`), PanelTimeseries, "", "Trend visualization", 9},
		{regexp.MustCompile(`(?i)chart\s+(?:of\s+)?(.+)`), PanelTimeseries, "", "Chart visualization", 8},

		// Stat patterns
		{regexp.MustCompile(`(?i)(?:current|latest|last)\s+(.+)`), PanelStat, "last", "Current value stat", 10},
		{regexp.MustCompile(`(?i)average\s+(?:of\s+)?(.+)`), PanelStat, "avg", "Average stat", 10},
		{regexp.MustCompile(`(?i)total\s+(?:of\s+)?(.+)`), PanelStat, "sum", "Total stat", 10},
		{regexp.MustCompile(`(?i)maximum\s+(?:of\s+)?(.+)`), PanelStat, "max", "Maximum stat", 10},
		{regexp.MustCompile(`(?i)minimum\s+(?:of\s+)?(.+)`), PanelStat, "min", "Minimum stat", 10},
		{regexp.MustCompile(`(?i)count\s+(?:of\s+)?(.+)`), PanelStat, "count", "Count stat", 10},

		// Gauge patterns
		{regexp.MustCompile(`(?i)gauge\s+(?:for\s+)?(.+)`), PanelGauge, "last", "Gauge visualization", 9},
		{regexp.MustCompile(`(?i)(.+)\s+gauge`), PanelGauge, "last", "Gauge visualization", 8},

		// Table patterns
		{regexp.MustCompile(`(?i)table\s+(?:of\s+)?(.+)`), PanelTable, "", "Table visualization", 9},
		{regexp.MustCompile(`(?i)list\s+(?:of\s+)?(.+)`), PanelTable, "", "Table visualization", 8},

		// Heatmap patterns
		{regexp.MustCompile(`(?i)heatmap\s+(?:of\s+)?(.+)`), PanelHeatmap, "", "Heatmap visualization", 9},
		{regexp.MustCompile(`(?i)distribution\s+(?:of\s+)?(.+)`), PanelHeatmap, "", "Distribution heatmap", 8},

		// Pie chart patterns
		{regexp.MustCompile(`(?i)(?:pie|breakdown)\s+(?:chart\s+)?(?:of\s+)?(.+)`), PanelPieChart, "", "Pie chart", 9},
	}
}

// GenerateDashboard generates a dashboard from natural language.
func (e *NLDashboardEngine) GenerateDashboard(ctx context.Context, description string) (*DashboardSpec, error) {
	// Parse the description
	parsed := e.parseDescription(description)

	// Create dashboard spec
	genID1, _ := generateID()
	dashboard := &DashboardSpec{
		ID:          genID1,
		Title:       parsed.Title,
		Description: description,
		Tags:        parsed.Tags,
		TimeRange: &TimeRangeSpec{
			From: e.config.DefaultTimeRange,
			To:   "now",
		},
		Refresh:   e.config.DefaultRefreshInterval,
		Panels:    make([]*PanelSpec, 0),
		Variables: parsed.Variables,
		CreatedAt: time.Now(),
		SourceNL:  description,
	}

	// Generate panels
	panelID := 1
	row := 0
	col := 0

	for _, panelDesc := range parsed.Panels {
		panel := e.createPanel(panelID, panelDesc, col, row)
		dashboard.Panels = append(dashboard.Panels, panel)

		panelID++
		col++
		if col >= e.config.MaxPanelsPerRow {
			col = 0
			row++
		}

		e.panelsGenerated.Add(1)
	}

	// Cache dashboard
	e.dashboardsMu.Lock()
	e.dashboards[dashboard.ID] = dashboard
	e.dashboardsMu.Unlock()

	e.dashboardsGenerated.Add(1)

	return dashboard, nil
}

// ParsedDescription contains parsed NL description.
type ParsedDescription struct {
	Title     string
	Tags      []string
	Panels    []*PanelDescription
	Variables []*VariableSpec
	TimeRange string
}

// PanelDescription describes a panel to create.
type PanelDescription struct {
	Title       string
	Type        PanelType
	Metrics     []string
	Tags        map[string]string
	Aggregation string
	GroupBy     []string
}

func (e *NLDashboardEngine) parseDescription(description string) *ParsedDescription {
	parsed := &ParsedDescription{
		Panels:    make([]*PanelDescription, 0),
		Variables: make([]*VariableSpec, 0),
	}

	// Extract title
	parsed.Title = e.extractTitle(description)

	// Extract tags
	parsed.Tags = e.extractTags(description)

	// Extract time range
	parsed.TimeRange = e.extractTimeRange(description)

	// Extract variables
	parsed.Variables = e.extractVariables(description)

	// Split into panel descriptions
	panelDescriptions := e.splitIntoPanels(description)

	for _, desc := range panelDescriptions {
		panelDesc := e.parsePanelDescription(desc)
		if panelDesc != nil {
			parsed.Panels = append(parsed.Panels, panelDesc)
		}
	}

	// If no panels extracted, create a default one
	if len(parsed.Panels) == 0 {
		metrics := e.extractMetrics(description)
		for _, metric := range metrics {
			parsed.Panels = append(parsed.Panels, &PanelDescription{
				Title:   metric,
				Type:    PanelTimeseries,
				Metrics: []string{metric},
			})
		}
	}

	return parsed
}

func (e *NLDashboardEngine) extractTitle(description string) string {
	// Look for explicit title
	titlePatterns := []*regexp.Regexp{
		regexp.MustCompile(`(?i)(?:create|make|build)\s+(?:a\s+)?dashboard\s+(?:called|named|titled)\s+"([^"]+)"`),
		regexp.MustCompile(`(?i)(?:create|make|build)\s+(?:a\s+)?dashboard\s+(?:called|named|titled)\s+(\S+)`),
		regexp.MustCompile(`(?i)(?:create|make|build)\s+(?:a\s+)?(.+?)\s+dashboard`),
		regexp.MustCompile(`(?i)dashboard\s+(?:for|showing|displaying)\s+(.+)`),
	}

	for _, pattern := range titlePatterns {
		if matches := pattern.FindStringSubmatch(description); len(matches) > 1 {
			return strings.TrimSpace(matches[1])
		}
	}

	// Generate title from metrics
	metrics := e.extractMetrics(description)
	if len(metrics) > 0 {
		return fmt.Sprintf("%s Dashboard", strings.Join(metrics[:min(len(metrics), 3)], " & "))
	}

	return "Generated Dashboard"
}

func (e *NLDashboardEngine) extractTags(description string) []string {
	tags := make([]string, 0)

	// Look for explicit tags
	tagPattern := regexp.MustCompile(`(?i)tags?:\s*\[?([^\]]+)\]?`)
	if matches := tagPattern.FindStringSubmatch(description); len(matches) > 1 {
		parts := strings.Split(matches[1], ",")
		for _, p := range parts {
			tag := strings.TrimSpace(p)
			if tag != "" {
				tags = append(tags, tag)
			}
		}
	}

	// Infer tags from content
	keywords := map[string]string{
		"cpu":        "system",
		"memory":     "system",
		"disk":       "system",
		"network":    "network",
		"http":       "web",
		"request":    "web",
		"database":   "database",
		"postgres":   "database",
		"mysql":      "database",
		"kubernetes": "kubernetes",
		"k8s":        "kubernetes",
		"docker":     "containers",
	}

	lowerDesc := strings.ToLower(description)
	for keyword, tag := range keywords {
		if strings.Contains(lowerDesc, keyword) {
			tags = append(tags, tag)
		}
	}

	return uniqueStrings(tags)
}

func (e *NLDashboardEngine) extractTimeRange(description string) string {
	timePatterns := map[*regexp.Regexp]string{
		regexp.MustCompile(`(?i)last\s+(\d+)\s*h(?:ours?)?`):   "now-%dh",
		regexp.MustCompile(`(?i)last\s+(\d+)\s*m(?:inutes?)?`): "now-%dm",
		regexp.MustCompile(`(?i)last\s+(\d+)\s*d(?:ays?)?`):    "now-%dd",
		regexp.MustCompile(`(?i)last\s+(\d+)\s*w(?:eeks?)?`):   "now-%dw",
		regexp.MustCompile(`(?i)past\s+(\d+)\s*h(?:ours?)?`):   "now-%dh",
	}

	for pattern, format := range timePatterns {
		if matches := pattern.FindStringSubmatch(description); len(matches) > 1 {
			if num, err := strconv.Atoi(matches[1]); err == nil {
				return fmt.Sprintf(format, num)
			}
		}
	}

	return e.config.DefaultTimeRange
}

func (e *NLDashboardEngine) extractVariables(description string) []*VariableSpec {
	variables := make([]*VariableSpec, 0)

	// Look for variable patterns
	varPatterns := []*regexp.Regexp{
		regexp.MustCompile(`(?i)filter\s+by\s+(\w+)`),
		regexp.MustCompile(`(?i)variable\s+(?:for\s+)?(\w+)`),
		regexp.MustCompile(`(?i)dropdown\s+(?:for\s+)?(\w+)`),
	}

	for _, pattern := range varPatterns {
		matches := pattern.FindAllStringSubmatch(description, -1)
		for _, match := range matches {
			if len(match) > 1 {
				variables = append(variables, &VariableSpec{
					Name:  match[1],
					Type:  "query",
					Label: strings.Title(match[1]),
					Query: fmt.Sprintf("label_values(%s)", match[1]),
					Multi: true,
				})
			}
		}
	}

	return variables
}

func (e *NLDashboardEngine) splitIntoPanels(description string) []string {
	// First, split by bullet-point lines (e.g. "- item" or "* item")
	bulletPattern := regexp.MustCompile(`(?m)^\s*[-*]\s+`)
	if bulletPattern.MatchString(description) {
		lines := strings.Split(description, "\n")
		var bullets []string
		for _, line := range lines {
			trimmed := strings.TrimSpace(line)
			if len(trimmed) > 2 && (trimmed[0] == '-' || trimmed[0] == '*') {
				bullets = append(bullets, strings.TrimSpace(trimmed[1:]))
			}
		}
		if len(bullets) > 1 {
			return bullets
		}
	}

	// Split by common separators
	separators := []string{
		" and also ",
		" as well as ",
		" together with ",
		" alongside ",
		", and ",
		"; ",
	}

	parts := []string{description}
	for _, sep := range separators {
		newParts := make([]string, 0)
		for _, part := range parts {
			newParts = append(newParts, strings.Split(part, sep)...)
		}
		parts = newParts
	}

	return parts
}
