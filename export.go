package chronicle

import (
	"compress/gzip"
	"encoding/binary"
	"encoding/csv"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"runtime"
	"sort"
	"strconv"
	"strings"
	"time"
)

// validateExportPath validates that the output path is safe to write to.
// It prevents writes to sensitive system directories and ensures the path is absolute.
func validateExportPath(outputPath string) (string, error) {
	if outputPath == "" {
		return "", errors.New("output path required")
	}

	// Clean and resolve the path
	cleanPath := filepath.Clean(outputPath)
	absPath, err := filepath.Abs(cleanPath)
	if err != nil {
		return "", fmt.Errorf("invalid output path: %w", err)
	}

	// Compare directory segments within the volume using platform path rules.
	// Keep absPath unchanged so the returned destination retains its spelling.
	comparisonPath := absPath
	if runtime.GOOS == "windows" {
		comparisonPath = strings.ToLower(filepath.ToSlash(absPath))
		// Go 1.24 treats only \\?\UNC as the extended UNC volume. Use
		// ordinary UNC form so the comparison volume includes host and share.
		if strings.HasPrefix(comparisonPath, "//?/unc/") {
			comparisonPath = "//" + comparisonPath[len("//?/unc/"):]
		}
		comparisonPath = comparisonPath[len(filepath.VolumeName(comparisonPath)):]
	}

	// Prevent writes to common sensitive directories
	sensitivePatterns := []string{
		"/etc", "/bin", "/sbin", "/usr/bin", "/usr/sbin",
		"/boot", "/dev", "/proc", "/sys", "/root",
	}
	for _, pattern := range sensitivePatterns {
		if strings.HasPrefix(comparisonPath, pattern+"/") || comparisonPath == pattern {
			return "", fmt.Errorf("cannot write to sensitive directory: %s", pattern)
		}
	}

	return absPath, nil
}

// ExportFormat defines the output format for data export.
type ExportFormat int

const (
	// ExportFormatCSV exports data as CSV.
	ExportFormatCSV ExportFormat = iota
	// ExportFormatJSON exports data as JSON lines.
	ExportFormatJSON
	// ExportFormatParquet exports data as Apache Parquet.
	ExportFormatParquet
)

// ExportConfig configures data export operations.
type ExportConfig struct {
	// Format is the output format (CSV, JSON, Parquet).
	Format ExportFormat

	// OutputPath is the file or directory for output.
	OutputPath string

	// Metrics is the list of metrics to export (empty = all).
	Metrics []string

	// Tags filters data by tag values.
	Tags map[string]string

	// Start is the start timestamp (nanoseconds, 0 = beginning).
	Start int64

	// End is the end timestamp (nanoseconds, 0 = now).
	End int64

	// Compression enables gzip compression.
	Compression bool

	// BatchSize is the number of points per batch (for streaming).
	BatchSize int

	// IncludeHeaders includes column headers (CSV).
	IncludeHeaders bool

	// TimestampFormat for human-readable timestamps (empty = Unix nano).
	TimestampFormat string
}

// DefaultExportConfig returns default export configuration.
func DefaultExportConfig() ExportConfig {
	return ExportConfig{
		Format:          ExportFormatCSV,
		BatchSize:       10000,
		IncludeHeaders:  true,
		TimestampFormat: time.RFC3339Nano,
	}
}

// Exporter handles data export operations.
type Exporter struct {
	db     *DB
	config ExportConfig
}

// NewExporter creates a new exporter.
func NewExporter(db *DB, config ExportConfig) *Exporter {
	if config.BatchSize <= 0 {
		config.BatchSize = 10000
	}
	return &Exporter{
		db:     db,
		config: config,
	}
}

// ExportResult contains export operation results.
type ExportResult struct {
	PointsExported int64
	BytesWritten   int64
	Duration       time.Duration
	Files          []string
}

// Export exports data to the configured format and destination.
func (e *Exporter) Export() (*ExportResult, error) {
	start := time.Now()

	// Validate output path
	validatedPath, err := validateExportPath(e.config.OutputPath)
	if err != nil {
		return nil, err
	}
	e.config.OutputPath = validatedPath

	// Determine metrics to export
	metrics := e.config.Metrics
	if len(metrics) == 0 {
		metrics = e.db.Metrics()
	}

	if len(metrics) == 0 {
		return &ExportResult{Duration: time.Since(start)}, nil
	}

	// Set time range defaults
	endTime := e.config.End
	if endTime == 0 {
		endTime = time.Now().UnixNano()
	}

	switch e.config.Format {
	case ExportFormatCSV:
		return e.exportCSV(metrics, e.config.Start, endTime)
	case ExportFormatJSON:
		return e.exportJSON(metrics, e.config.Start, endTime)
	case ExportFormatParquet:
		return e.exportParquet(metrics, e.config.Start, endTime)
	default:
		return nil, fmt.Errorf("unsupported export format: %d", e.config.Format)
	}
}

// exportCSV exports data as CSV.
func (e *Exporter) exportCSV(metrics []string, start, end int64) (*ExportResult, error) {
	startTime := time.Now()
	result := &ExportResult{}

	// Create output file
	outputPath := e.config.OutputPath
	if !strings.HasSuffix(outputPath, ".csv") && !strings.HasSuffix(outputPath, ".csv.gz") {
		outputPath = filepath.Join(outputPath, "export.csv")
	}
	if e.config.Compression && !strings.HasSuffix(outputPath, ".gz") {
		outputPath += ".gz"
	}
	outputPath, err := validateExportPath(outputPath)
	if err != nil {
		return nil, err
	}

	// Ensure directory exists
	if err := os.MkdirAll(filepath.Dir(outputPath), 0755); err != nil {
		return nil, fmt.Errorf("failed to create output directory: %w", err)
	}

	file, err := os.Create(outputPath)
	if err != nil {
		return nil, fmt.Errorf("failed to create output file: %w", err)
	}
	defer file.Close()

	var writer io.Writer = file
	if e.config.Compression {
		gzWriter := gzip.NewWriter(file)
		defer gzWriter.Close()
		writer = gzWriter
	}

	csvWriter := csv.NewWriter(writer)
	defer csvWriter.Flush()

	// Write header
	if e.config.IncludeHeaders {
		header := []string{"timestamp", "metric", "value", "tags"}
		if err := csvWriter.Write(header); err != nil {
			return nil, fmt.Errorf("failed to write header: %w", err)
		}
	}

	// Export each metric
	for _, metric := range metrics {
		points, err := e.queryPoints(metric, start, end)
		if err != nil {
			return nil, fmt.Errorf("failed to query metric %s: %w", metric, err)
		}

		for _, p := range points {
			ts := formatTimestamp(p.Timestamp, e.config.TimestampFormat)
			tags := formatExportTags(p.Tags)
			record := []string{ts, p.Metric, strconv.FormatFloat(p.Value, 'g', -1, 64), tags}
			if err := csvWriter.Write(record); err != nil {
				return nil, fmt.Errorf("failed to write record: %w", err)
			}
			result.PointsExported++
		}
	}

	csvWriter.Flush()
	if err := csvWriter.Error(); err != nil {
		return nil, fmt.Errorf("csv write error: %w", err)
	}

	// Get file size
	if info, err := file.Stat(); err == nil {
		result.BytesWritten = info.Size()
	}

	result.Duration = time.Since(startTime)
	result.Files = []string{outputPath}

	return result, nil
}

// exportJSON exports data as JSON lines.
func (e *Exporter) exportJSON(metrics []string, start, end int64) (*ExportResult, error) {
	startTime := time.Now()
	result := &ExportResult{}

	// Create output file
	outputPath := e.config.OutputPath
	if !strings.HasSuffix(outputPath, ".json") && !strings.HasSuffix(outputPath, ".jsonl") && !strings.HasSuffix(outputPath, ".json.gz") {
		outputPath = filepath.Join(outputPath, "export.jsonl")
	}
	if e.config.Compression && !strings.HasSuffix(outputPath, ".gz") {
		outputPath += ".gz"
	}
	outputPath, err := validateExportPath(outputPath)
	if err != nil {
		return nil, err
	}

	if err := os.MkdirAll(filepath.Dir(outputPath), 0755); err != nil {
		return nil, fmt.Errorf("failed to create output directory: %w", err)
	}

	file, err := os.Create(outputPath)
	if err != nil {
		return nil, fmt.Errorf("failed to create output file: %w", err)
	}
	defer file.Close()

	var writer io.Writer = file
	if e.config.Compression {
		gzWriter := gzip.NewWriter(file)
		defer gzWriter.Close()
		writer = gzWriter
	}

	// Export each metric
	for _, metric := range metrics {
		points, err := e.queryPoints(metric, start, end)
		if err != nil {
			return nil, fmt.Errorf("failed to query metric %s: %w", metric, err)
		}

		for _, p := range points {
			line := fmt.Sprintf(`{"timestamp":%d,"metric":"%s","value":%g,"tags":{`, p.Timestamp, p.Metric, p.Value)

			tagParts := make([]string, 0, len(p.Tags))
			for k, v := range p.Tags {
				tagParts = append(tagParts, fmt.Sprintf(`"%s":"%s"`, k, v))
			}
			sort.Strings(tagParts)
			line += strings.Join(tagParts, ",") + "}}\n"

			if _, err := writer.Write([]byte(line)); err != nil {
				return nil, fmt.Errorf("failed to write record: %w", err)
			}
			result.PointsExported++
		}
	}

	if info, err := file.Stat(); err == nil {
		result.BytesWritten = info.Size()
	}

	result.Duration = time.Since(startTime)
	result.Files = []string{outputPath}

	return result, nil
}

// exportParquet exports data as Apache Parquet.
// Note: This is a simplified implementation using a custom binary format
// that follows Parquet-like columnar structure. For production use,
// consider using github.com/xitongsys/parquet-go.
func (e *Exporter) exportParquet(metrics []string, start, end int64) (*ExportResult, error) {
	startTime := time.Now()
	result := &ExportResult{}

	outputPath := e.config.OutputPath
	if !strings.HasSuffix(outputPath, ".parquet") {
		outputPath = filepath.Join(outputPath, "export.parquet")
	}
	outputPath, err := validateExportPath(outputPath)
	if err != nil {
		return nil, err
	}

	if err := os.MkdirAll(filepath.Dir(outputPath), 0755); err != nil {
		return nil, fmt.Errorf("failed to create output directory: %w", err)
	}

	file, err := os.Create(outputPath)
	if err != nil {
		return nil, fmt.Errorf("failed to create output file: %w", err)
	}
	defer file.Close()

	// Write Parquet-like header (magic bytes)
	if _, err := file.Write([]byte("PAR1")); err != nil {
		return nil, err
	}

	// Collect all points
	var allPoints []Point
	for _, metric := range metrics {
		points, err := e.queryPoints(metric, start, end)
		if err != nil {
			return nil, fmt.Errorf("failed to query metric %s: %w", metric, err)
		}
		allPoints = append(allPoints, points...)
	}

	// Sort by timestamp for better compression
	sort.Slice(allPoints, func(i, j int) bool {
		return allPoints[i].Timestamp < allPoints[j].Timestamp
	})

	// Write row count
	if err := binary.Write(file, binary.LittleEndian, int64(len(allPoints))); err != nil {
		return nil, err
	}

	// Write columnar data: timestamps
	for _, p := range allPoints {
		if err := binary.Write(file, binary.LittleEndian, p.Timestamp); err != nil {
			return nil, err
		}
	}

	// Write columnar data: values
	for _, p := range allPoints {
		if err := binary.Write(file, binary.LittleEndian, p.Value); err != nil {
			return nil, err
		}
	}

	// Write columnar data: metrics (length-prefixed strings)
	for _, p := range allPoints {
		metricBytes := []byte(p.Metric)
		if err := binary.Write(file, binary.LittleEndian, int32(len(metricBytes))); err != nil {
			return nil, err
		}
		if _, err := file.Write(metricBytes); err != nil {
			return nil, err
		}
	}

	// Write columnar data: tags (JSON-encoded)
	for _, p := range allPoints {
		tagsStr := formatExportTags(p.Tags)
		tagsBytes := []byte(tagsStr)
		if err := binary.Write(file, binary.LittleEndian, int32(len(tagsBytes))); err != nil {
			return nil, err
		}
		if _, err := file.Write(tagsBytes); err != nil {
			return nil, err
		}
	}

	// Write footer magic
	if _, err := file.Write([]byte("PAR1")); err != nil {
		return nil, err
	}

	result.PointsExported = int64(len(allPoints))
	if info, err := file.Stat(); err == nil {
		result.BytesWritten = info.Size()
	}

	result.Duration = time.Since(startTime)
	result.Files = []string{outputPath}

	return result, nil
}

// queryPoints queries points for a metric within a time range.
func (e *Exporter) queryPoints(metric string, start, end int64) ([]Point, error) {
	result, err := e.db.Execute(&Query{
		Metric: metric,
		Tags:   e.config.Tags,
		Start:  start,
		End:    end,
	})
	if err != nil {
		return nil, err
	}
	return result.Points, nil
}

// formatTimestamp formats a timestamp according to the configured format.
func formatTimestamp(ts int64, format string) string {
	if format == "" {
		return strconv.FormatInt(ts, 10)
	}
	return time.Unix(0, ts).UTC().Format(format)
}

// formatExportTags formats tags as a comma-separated key=value string.
func formatExportTags(tags map[string]string) string {
	if len(tags) == 0 {
		return ""
	}
	parts := make([]string, 0, len(tags))
	for k, v := range tags {
		parts = append(parts, k+"="+v)
	}
	sort.Strings(parts)
	return strings.Join(parts, ",")
}

// ExportToCSV is a convenience function to export to CSV.
func (db *DB) ExportToCSV(path string, metrics []string, start, end int64) (*ExportResult, error) {
	config := DefaultExportConfig()
	config.OutputPath = path
	config.Metrics = metrics
	config.Start = start
	config.End = end
	return NewExporter(db, config).Export()
}

// ExportToJSON is a convenience function to export to JSON.
func (db *DB) ExportToJSON(path string, metrics []string, start, end int64) (*ExportResult, error) {
	config := DefaultExportConfig()
	config.Format = ExportFormatJSON
	config.OutputPath = path
	config.Metrics = metrics
	config.Start = start
	config.End = end
	return NewExporter(db, config).Export()
}

// ExportToParquet is a convenience function to export to Parquet.
func (db *DB) ExportToParquet(path string, metrics []string, start, end int64) (*ExportResult, error) {
	config := DefaultExportConfig()
	config.Format = ExportFormatParquet
	config.OutputPath = path
	config.Metrics = metrics
	config.Start = start
	config.End = end
	return NewExporter(db, config).Export()
}
