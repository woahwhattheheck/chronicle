package chronicle

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"os"
	"regexp"
	"strings"
	"sync"
	"sync/atomic"
	"time"
)

// K8sSidecarConfig configures the Kubernetes sidecar mode.
type K8sSidecarConfig struct {
	// Enabled enables sidecar mode.
	Enabled bool `json:"enabled"`

	// ScrapeInterval is how often to scrape metrics.
	ScrapeInterval time.Duration `json:"scrape_interval"`

	// ScrapeTimeout is the timeout for scrape operations.
	ScrapeTimeout time.Duration `json:"scrape_timeout"`

	// MetricsPort is the port to scrape on the localhost.
	MetricsPort int `json:"metrics_port"`

	// MetricsPath is the path to scrape metrics from.
	MetricsPath string `json:"metrics_path"`

	// DiscoveryEnabled enables pod/service discovery.
	DiscoveryEnabled bool `json:"discovery_enabled"`

	// NamespaceFilter limits discovery to specific namespaces.
	NamespaceFilter []string `json:"namespace_filter,omitempty"`

	// LabelSelector filters pods by labels.
	LabelSelector map[string]string `json:"label_selector,omitempty"`

	// AnnotationPrefix for pod annotations.
	AnnotationPrefix string `json:"annotation_prefix"`

	// AddPodLabels adds pod labels as metric tags.
	AddPodLabels bool `json:"add_pod_labels"`

	// AddNamespace adds namespace as a tag.
	AddNamespace bool `json:"add_namespace"`

	// AddPodName adds pod name as a tag.
	AddPodName bool `json:"add_pod_name"`

	// AddNodeName adds node name as a tag.
	AddNodeName bool `json:"add_node_name"`

	// RetentionDuration for local storage.
	RetentionDuration time.Duration `json:"retention_duration"`

	// MaxStorageBytes limits local storage.
	MaxStorageBytes int64 `json:"max_storage_bytes"`

	// RemoteWriteEnabled enables remote write to cloud.
	RemoteWriteEnabled bool `json:"remote_write_enabled"`

	// RemoteWriteURL is the remote write endpoint.
	RemoteWriteURL string `json:"remote_write_url,omitempty"`

	// HealthPort is the port for the health endpoint.
	HealthPort int `json:"health_port"`
}

// DefaultK8sSidecarConfig returns default sidecar configuration.
func DefaultK8sSidecarConfig() K8sSidecarConfig {
	return K8sSidecarConfig{
		Enabled:           true,
		ScrapeInterval:    15 * time.Second,
		ScrapeTimeout:     10 * time.Second,
		MetricsPort:       9090,
		MetricsPath:       "/metrics",
		DiscoveryEnabled:  true,
		AnnotationPrefix:  "chronicle.io/",
		AddPodLabels:      true,
		AddNamespace:      true,
		AddPodName:        true,
		AddNodeName:       true,
		RetentionDuration: 24 * time.Hour,
		MaxStorageBytes:   100 * 1024 * 1024, // 100MB
		HealthPort:        8080,
	}
}

// K8sSidecar implements the Kubernetes sidecar pattern for Chronicle.
type K8sSidecar struct {
	db     *DB
	config K8sSidecarConfig
	client *http.Client
	server *http.Server

	// Scrape targets
	targets   []ScrapeTarget
	targetsMu sync.RWMutex

	// Pod metadata
	podInfo *PodInfo

	// Statistics
	stats   K8sSidecarStats
	statsMu sync.RWMutex

	// Lifecycle
	running atomic.Bool
	ctx     context.Context
	cancel  context.CancelFunc
	wg      sync.WaitGroup
}

// ScrapeTarget represents a metrics endpoint to scrape.
type ScrapeTarget struct {
	Address     string            `json:"address"`
	Port        int               `json:"port"`
	Path        string            `json:"path"`
	Scheme      string            `json:"scheme"`
	Labels      map[string]string `json:"labels,omitempty"`
	Interval    time.Duration     `json:"interval,omitempty"`
	Timeout     time.Duration     `json:"timeout,omitempty"`
	HonorLabels bool              `json:"honor_labels"`
}

// PodInfo contains information about the current pod.
type PodInfo struct {
	Name        string            `json:"name"`
	Namespace   string            `json:"namespace"`
	NodeName    string            `json:"node_name"`
	PodIP       string            `json:"pod_ip"`
	HostIP      string            `json:"host_ip"`
	Labels      map[string]string `json:"labels,omitempty"`
	Annotations map[string]string `json:"annotations,omitempty"`
}

// K8sSidecarStats contains sidecar statistics.
type K8sSidecarStats struct {
	ScrapeCount        int64         `json:"scrape_count"`
	ScrapeErrors       int64         `json:"scrape_errors"`
	PointsCollected    int64         `json:"points_collected"`
	PointsWritten      int64         `json:"points_written"`
	LastScrapeTime     time.Time     `json:"last_scrape_time"`
	LastScrapeError    string        `json:"last_scrape_error,omitempty"`
	LastScrapeDuration time.Duration `json:"last_scrape_duration"`
	TargetCount        int           `json:"target_count"`
	StorageBytes       int64         `json:"storage_bytes"`
}

// NewK8sSidecar creates a new Kubernetes sidecar.
func NewK8sSidecar(db *DB, config K8sSidecarConfig) (*K8sSidecar, error) {
	if !config.Enabled {
		return nil, errors.New("sidecar mode not enabled")
	}

	ctx, cancel := context.WithCancel(context.Background())

	s := &K8sSidecar{
		db:     db,
		config: config,
		client: &http.Client{
			Timeout: config.ScrapeTimeout,
		},
		ctx:    ctx,
		cancel: cancel,
	}

	// Load pod info from environment/downward API
	s.podInfo = s.loadPodInfo()

	// Add default localhost target
	s.targets = append(s.targets, ScrapeTarget{
		Address: "localhost",
		Port:    config.MetricsPort,
		Path:    config.MetricsPath,
		Scheme:  "http",
	})

	return s, nil
}

// Start starts the sidecar.
func (s *K8sSidecar) Start() error {
	if s.running.Swap(true) {
		return errors.New("already running")
	}

	// Start health server
	if err := s.startHealthServer(); err != nil {
		s.running.Store(false)
		return err
	}

	// Start scrape loop
	s.wg.Add(1)
	go s.scrapeLoop()

	// Start discovery if enabled
	if s.config.DiscoveryEnabled {
		s.wg.Add(1)
		go s.discoveryLoop()
	}

	return nil
}

// Stop stops the sidecar.
func (s *K8sSidecar) Stop() error {
	if !s.running.Swap(false) {
		return nil
	}

	s.cancel()
	s.wg.Wait()

	if s.server != nil {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		return s.server.Shutdown(ctx)
	}

	return nil
}

func (s *K8sSidecar) startHealthServer() error {
	mux := http.NewServeMux()
	mux.HandleFunc("/health", s.handleHealth)
	mux.HandleFunc("/api/v1/sidecar/health", s.handleHealth)
	mux.HandleFunc("/ready", s.handleReady)
	mux.HandleFunc("/api/v1/sidecar/metrics", s.handleMetrics)
	mux.HandleFunc("/targets", s.handleTargets)
	mux.HandleFunc("/stats", s.handleStats)

	s.server = &http.Server{
		Addr:    fmt.Sprintf(":%d", s.config.HealthPort),
		Handler: mux,
	}

	listener, err := net.Listen("tcp", s.server.Addr)
	if err != nil {
		return fmt.Errorf("listen for sidecar health: %w", err)
	}

	go s.server.Serve(listener)
	return nil
}

func (s *K8sSidecar) handleHealth(w http.ResponseWriter, r *http.Request) {
	w.WriteHeader(http.StatusOK)
	json.NewEncoder(w).Encode(map[string]string{"status": "healthy"})
}

func (s *K8sSidecar) handleReady(w http.ResponseWriter, r *http.Request) {
	// Check if we have targets and have scraped recently
	s.statsMu.RLock()
	lastScrape := s.stats.LastScrapeTime
	s.statsMu.RUnlock()

	if time.Since(lastScrape) > 2*s.config.ScrapeInterval {
		w.WriteHeader(http.StatusServiceUnavailable)
		json.NewEncoder(w).Encode(map[string]string{"status": "not ready"})
		return
	}

	w.WriteHeader(http.StatusOK)
	json.NewEncoder(w).Encode(map[string]string{"status": "ready"})
}

func (s *K8sSidecar) handleMetrics(w http.ResponseWriter, r *http.Request) {
	// Prometheus format metrics output
	s.statsMu.RLock()
	stats := s.stats
	s.statsMu.RUnlock()

	fmt.Fprintf(w, "# HELP chronicle_sidecar_scrape_count Total number of scrapes\n")
	fmt.Fprintf(w, "# TYPE chronicle_sidecar_scrape_count counter\n")
	fmt.Fprintf(w, "chronicle_sidecar_scrape_count %d\n", stats.ScrapeCount)

	fmt.Fprintf(w, "# HELP chronicle_sidecar_scrape_errors Total number of scrape errors\n")
	fmt.Fprintf(w, "# TYPE chronicle_sidecar_scrape_errors counter\n")
	fmt.Fprintf(w, "chronicle_sidecar_scrape_errors %d\n", stats.ScrapeErrors)

	fmt.Fprintf(w, "# HELP chronicle_sidecar_points_collected Total points collected\n")
	fmt.Fprintf(w, "# TYPE chronicle_sidecar_points_collected counter\n")
	fmt.Fprintf(w, "chronicle_sidecar_points_collected %d\n", stats.PointsCollected)

	fmt.Fprintf(w, "# HELP chronicle_sidecar_points_written Total points written\n")
	fmt.Fprintf(w, "# TYPE chronicle_sidecar_points_written counter\n")
	fmt.Fprintf(w, "chronicle_sidecar_points_written %d\n", stats.PointsWritten)

	fmt.Fprintf(w, "# HELP chronicle_sidecar_target_count Number of scrape targets\n")
	fmt.Fprintf(w, "# TYPE chronicle_sidecar_target_count gauge\n")
	fmt.Fprintf(w, "chronicle_sidecar_target_count %d\n", stats.TargetCount)
}

func (s *K8sSidecar) handleTargets(w http.ResponseWriter, r *http.Request) {
	s.targetsMu.RLock()
	defer s.targetsMu.RUnlock()

	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(s.targets)
}

func (s *K8sSidecar) handleStats(w http.ResponseWriter, r *http.Request) {
	s.statsMu.RLock()
	stats := s.stats
	s.statsMu.RUnlock()

	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(stats)
}

func (s *K8sSidecar) scrapeLoop() {
	defer s.wg.Done()

	ticker := time.NewTicker(s.config.ScrapeInterval)
	defer ticker.Stop()

	for {
		select {
		case <-s.ctx.Done():
			return
		case <-ticker.C:
			s.scrapeAll()
		}
	}
}

func (s *K8sSidecar) scrapeAll() {
	s.targetsMu.RLock()
	targets := make([]ScrapeTarget, len(s.targets))
	copy(targets, s.targets)
	s.targetsMu.RUnlock()

	s.statsMu.Lock()
	s.stats.TargetCount = len(targets)
	s.statsMu.Unlock()

	var wg sync.WaitGroup
	for _, target := range targets {
		wg.Add(1)
		go func(t ScrapeTarget) {
			defer wg.Done()
			s.scrapeTarget(t)
		}(target)
	}
	wg.Wait()
}

func (s *K8sSidecar) scrapeTarget(target ScrapeTarget) {
	start := time.Now()

	url := fmt.Sprintf("%s://%s:%d%s", target.Scheme, target.Address, target.Port, target.Path)

	ctx, cancel := context.WithTimeout(s.ctx, s.config.ScrapeTimeout)
	defer cancel()

	req, err := http.NewRequestWithContext(ctx, "GET", url, nil)
	if err != nil {
		s.recordScrapeError(err)
		return
	}

	resp, err := s.client.Do(req)
	if err != nil {
		s.recordScrapeError(err)
		return
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		s.recordScrapeError(fmt.Errorf("HTTP %d", resp.StatusCode))
		return
	}

	body, err := io.ReadAll(resp.Body)
	if err != nil {
		s.recordScrapeError(err)
		return
	}

	// Parse Prometheus format metrics
	points := s.parsePrometheusMetrics(string(body), target.Labels)

	if len(points) > 0 {
		// Add pod metadata tags
		s.addMetadataTags(points)

		// Write to Chronicle
		if err := s.db.WriteBatch(points); err != nil {
			s.recordScrapeError(err)
			return
		}
	}

	// Update stats
	s.statsMu.Lock()
	s.stats.ScrapeCount++
	s.stats.PointsCollected += int64(len(points))
	s.stats.PointsWritten += int64(len(points))
	s.stats.LastScrapeTime = time.Now()
	s.stats.LastScrapeDuration = time.Since(start)
	s.stats.LastScrapeError = ""
	s.statsMu.Unlock()
}

func (s *K8sSidecar) parsePrometheusMetrics(data string, extraLabels map[string]string) []Point {
	var points []Point
	now := time.Now().UnixNano()

	lines := strings.Split(data, "\n")
	for _, line := range lines {
		line = strings.TrimSpace(line)

		// Skip comments and empty lines
		if line == "" || strings.HasPrefix(line, "#") {
			continue
		}

		// Parse metric line: metric_name{label="value"} value [timestamp]
		point, err := s.parseMetricLine(line, now)
		if err != nil {
			continue
		}

		// Add extra labels
		point.ensureTags()
		for k, v := range extraLabels {
			point.Tags[k] = v
		}

		points = append(points, point)
	}

	return points
}

func (s *K8sSidecar) parseMetricLine(line string, defaultTs int64) (Point, error) {
	point := Point{
		Tags:      make(map[string]string),
		Timestamp: defaultTs,
	}

	// Find metric name and labels
	braceIdx := strings.Index(line, "{")
	spaceIdx := strings.Index(line, " ")

	if braceIdx != -1 && braceIdx < spaceIdx {
		// Has labels
		point.Metric = line[:braceIdx]

		closeBraceIdx := strings.Index(line, "}")
		if closeBraceIdx == -1 {
			return point, errors.New("malformed labels")
		}

		labelStr := line[braceIdx+1 : closeBraceIdx]
		point.Tags = s.parseLabels(labelStr)

		// Parse value
		rest := strings.TrimSpace(line[closeBraceIdx+1:])
		parts := strings.Fields(rest)
		if len(parts) >= 1 {
			_, err := fmt.Sscanf(parts[0], "%f", &point.Value)
			if err != nil {
				return point, err
			}
		}
	} else if spaceIdx != -1 {
		// No labels
		point.Metric = line[:spaceIdx]
		rest := line[spaceIdx+1:]
		parts := strings.Fields(rest)
		if len(parts) >= 1 {
			_, err := fmt.Sscanf(parts[0], "%f", &point.Value)
			if err != nil {
				return point, err
			}
		}
	} else {
		return point, errors.New("invalid metric line")
	}

	return point, nil
}

var sidecarLabelRegex = regexp.MustCompile(`(\w+)="([^"]*)"`)

func (s *K8sSidecar) parseLabels(labelStr string) map[string]string {
	labels := make(map[string]string)

	// Simple label parser
	// Format: key="value",key2="value2"
	matches := sidecarLabelRegex.FindAllStringSubmatch(labelStr, -1)

	for _, match := range matches {
		if len(match) == 3 {
			labels[match[1]] = match[2]
		}
	}

	return labels
}

func (s *K8sSidecar) addMetadataTags(points []Point) {
	if s.podInfo == nil {
		return
	}

	for i := range points {
		points[i].ensureTags()
		if s.config.AddNamespace && s.podInfo.Namespace != "" {
			points[i].Tags["namespace"] = s.podInfo.Namespace
		}
		if s.config.AddPodName && s.podInfo.Name != "" {
			points[i].Tags["pod"] = s.podInfo.Name
		}
		if s.config.AddNodeName && s.podInfo.NodeName != "" {
			points[i].Tags["node"] = s.podInfo.NodeName
		}
		if s.config.AddPodLabels {
			for k, v := range s.podInfo.Labels {
				// Prefix pod labels to avoid conflicts
				points[i].Tags["pod_label_"+k] = v
			}
		}
	}
}

func (s *K8sSidecar) recordScrapeError(err error) {
	s.statsMu.Lock()
	s.stats.ScrapeErrors++
	s.stats.LastScrapeError = err.Error()
	s.statsMu.Unlock()
}

func (s *K8sSidecar) loadPodInfo() *PodInfo {
	info := &PodInfo{
		Labels:      make(map[string]string),
		Annotations: make(map[string]string),
	}

	// Load from environment variables (set via Downward API)
	info.Name = os.Getenv("POD_NAME")
	info.Namespace = os.Getenv("POD_NAMESPACE")
	info.NodeName = os.Getenv("NODE_NAME")
	info.PodIP = os.Getenv("POD_IP")
	info.HostIP = os.Getenv("HOST_IP")

	// Load labels from file (Downward API volume mount)
	if labels, err := os.ReadFile("/etc/podinfo/labels"); err == nil {
		info.Labels = s.parseDownwardAPILabels(string(labels))
	}

	// Load annotations from file
	if annotations, err := os.ReadFile("/etc/podinfo/annotations"); err == nil {
		info.Annotations = s.parseDownwardAPILabels(string(annotations))
	}

	return info
}

func (s *K8sSidecar) parseDownwardAPILabels(data string) map[string]string {
	labels := make(map[string]string)
	lines := strings.Split(data, "\n")
	for _, line := range lines {
		if idx := strings.Index(line, "="); idx != -1 {
			key := strings.TrimSpace(line[:idx])
			value := strings.Trim(strings.TrimSpace(line[idx+1:]), "\"")
			labels[key] = value
		}
	}
	return labels
}

func (s *K8sSidecar) discoveryLoop() {
	defer s.wg.Done()

	ticker := time.NewTicker(30 * time.Second)
	defer ticker.Stop()

	for {
		select {
		case <-s.ctx.Done():
			return
		case <-ticker.C:
			s.discoverTargets()
		}
	}
}

func (s *K8sSidecar) discoverTargets() {
	// In a real implementation, this would use the Kubernetes API
	// to discover pods with the appropriate annotations
	// For now, this is a placeholder that would be expanded

	// Look for pods with annotation: chronicle.io/scrape: "true"
	// and chronicle.io/port: "9090"
	// and chronicle.io/path: "/metrics"
}

// AddTarget manually adds a scrape target.
func (s *K8sSidecar) AddTarget(target ScrapeTarget) {
	s.targetsMu.Lock()
	defer s.targetsMu.Unlock()

	// Check for duplicate
	for _, t := range s.targets {
		if t.Address == target.Address && t.Port == target.Port {
			return
		}
	}

	s.targets = append(s.targets, target)
}

// RemoveTarget removes a scrape target.
func (s *K8sSidecar) RemoveTarget(address string, port int) {
	s.targetsMu.Lock()
	defer s.targetsMu.Unlock()

	var filtered []ScrapeTarget
	for _, t := range s.targets {
		if t.Address != address || t.Port != port {
			filtered = append(filtered, t)
		}
	}
	s.targets = filtered
}

// GetTargets returns all scrape targets.
func (s *K8sSidecar) GetTargets() []ScrapeTarget {
	s.targetsMu.RLock()
	defer s.targetsMu.RUnlock()

	targets := make([]ScrapeTarget, len(s.targets))
	copy(targets, s.targets)
	return targets
}

// GetStats returns sidecar statistics.
func (s *K8sSidecar) GetStats() K8sSidecarStats {
	s.statsMu.RLock()
	defer s.statsMu.RUnlock()
	return s.stats
}

// GetPodInfo returns the current pod information.
func (s *K8sSidecar) GetPodInfo() *PodInfo {
	return s.podInfo
}

// --- Helm Chart Generation ---

// GenerateHelmValues generates Helm chart values for the sidecar.
func GenerateHelmValues(config K8sSidecarConfig) map[string]any {
	return map[string]any{
		"sidecar": map[string]any{
			"enabled":        config.Enabled,
			"scrapeInterval": config.ScrapeInterval.String(),
			"scrapeTimeout":  config.ScrapeTimeout.String(),
			"metricsPort":    config.MetricsPort,
			"metricsPath":    config.MetricsPath,
			"healthPort":     config.HealthPort,
			"resources": map[string]any{
				"limits": map[string]string{
					"cpu":    "100m",
					"memory": "128Mi",
				},
				"requests": map[string]string{
					"cpu":    "50m",
					"memory": "64Mi",
				},
			},
		},
		"storage": map[string]any{
			"retentionDuration": config.RetentionDuration.String(),
			"maxStorageBytes":   config.MaxStorageBytes,
		},
		"remoteWrite": map[string]any{
			"enabled": config.RemoteWriteEnabled,
			"url":     config.RemoteWriteURL,
		},
	}
}

// GenerateSidecarManifest generates a Kubernetes sidecar container spec.
func GenerateSidecarManifest(config K8sSidecarConfig, imageName string) map[string]any {
	return map[string]any{
		"name":  "chronicle-sidecar",
		"image": imageName,
		"ports": []map[string]any{
			{"name": "health", "containerPort": config.HealthPort},
		},
		"env": []map[string]any{
			{
				"name": "POD_NAME",
				"valueFrom": map[string]any{
					"fieldRef": map[string]string{
						"fieldPath": "metadata.name",
					},
				},
			},
			{
				"name": "POD_NAMESPACE",
				"valueFrom": map[string]any{
					"fieldRef": map[string]string{
						"fieldPath": "metadata.namespace",
					},
				},
			},
			{
				"name": "NODE_NAME",
				"valueFrom": map[string]any{
					"fieldRef": map[string]string{
						"fieldPath": "spec.nodeName",
					},
				},
			},
			{
				"name": "POD_IP",
				"valueFrom": map[string]any{
					"fieldRef": map[string]string{
						"fieldPath": "status.podIP",
					},
				},
			},
		},
		"volumeMounts": []map[string]any{
			{
				"name":      "chronicle-data",
				"mountPath": "/data",
			},
			{
				"name":      "podinfo",
				"mountPath": "/etc/podinfo",
			},
		},
		"livenessProbe": map[string]any{
			"httpGet": map[string]any{
				"path": "/health",
				"port": config.HealthPort,
			},
			"initialDelaySeconds": 10,
			"periodSeconds":       10,
		},
		"readinessProbe": map[string]any{
			"httpGet": map[string]any{
				"path": "/ready",
				"port": config.HealthPort,
			},
			"initialDelaySeconds": 5,
			"periodSeconds":       5,
		},
		"resources": map[string]any{
			"limits": map[string]string{
				"cpu":    "100m",
				"memory": "128Mi",
			},
			"requests": map[string]string{
				"cpu":    "50m",
				"memory": "64Mi",
			},
		},
	}
}

// GeneratePodInfoVolume generates the downward API volume for pod info.
func GeneratePodInfoVolume() map[string]any {
	return map[string]any{
		"name": "podinfo",
		"downwardAPI": map[string]any{
			"items": []map[string]any{
				{
					"path": "labels",
					"fieldRef": map[string]string{
						"fieldPath": "metadata.labels",
					},
				},
				{
					"path": "annotations",
					"fieldRef": map[string]string{
						"fieldPath": "metadata.annotations",
					},
				},
			},
		},
	}
}

// SidecarTargetDiscoveryAnnotations returns the annotations needed for auto-discovery.
func SidecarTargetDiscoveryAnnotations(port int, path string) map[string]string {
	return map[string]string{
		"chronicle.io/scrape": "true",
		"chronicle.io/port":   fmt.Sprintf("%d", port),
		"chronicle.io/path":   path,
	}
}
