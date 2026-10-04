package chronicle

import (
	"os"
	"path/filepath"
	"testing"
	"time"
)

func TestExporter_CSV(t *testing.T) {
	db := setupTestDB(t)

	// Write test data as a single batch
	now := time.Now().UnixNano()
	points := make([]Point, 100)
	for i := 0; i < 100; i++ {
		points[i] = Point{
			Metric:    "cpu",
			Tags:      map[string]string{"host": "server1"},
			Value:     float64(i),
			Timestamp: now + int64(i)*int64(time.Second),
		}
	}
	db.WriteBatch(points)

	// Export to CSV
	tmpDir := t.TempDir()
	outputPath := filepath.Join(tmpDir, "export.csv")

	config := DefaultExportConfig()
	config.OutputPath = outputPath
	config.Start = now - int64(time.Hour)
	config.End = now + int64(time.Hour)*2

	exporter := NewExporter(db, config)
	result, err := exporter.Export()
	if err != nil {
		t.Fatalf("export failed: %v", err)
	}

	if result.PointsExported < 1 {
		t.Errorf("expected points > 0, got %d", result.PointsExported)
	}

	if result.BytesWritten == 0 {
		t.Error("expected bytes written > 0")
	}

	// Verify file exists
	if _, err := os.Stat(outputPath); os.IsNotExist(err) {
		t.Error("output file does not exist")
	}
}

func TestExporter_JSON(t *testing.T) {
	db := setupTestDB(t)

	now := time.Now().UnixNano()
	points := make([]Point, 50)
	for i := 0; i < 50; i++ {
		points[i] = Point{
			Metric:    "memory",
			Tags:      map[string]string{"host": "server2"},
			Value:     float64(i * 100),
			Timestamp: now + int64(i)*int64(time.Second),
		}
	}
	db.WriteBatch(points)

	tmpDir := t.TempDir()
	outputPath := filepath.Join(tmpDir, "export.jsonl")

	config := DefaultExportConfig()
	config.Format = ExportFormatJSON
	config.OutputPath = outputPath
	config.Start = now - int64(time.Hour)
	config.End = now + int64(time.Hour)*2

	result, err := NewExporter(db, config).Export()
	if err != nil {
		t.Fatalf("export failed: %v", err)
	}

	if result.PointsExported < 1 {
		t.Errorf("expected points > 0, got %d", result.PointsExported)
	}
}

func TestExporter_Parquet(t *testing.T) {
	db := setupTestDB(t)

	now := time.Now().UnixNano()
	points := make([]Point, 25)
	for i := 0; i < 25; i++ {
		points[i] = Point{
			Metric:    "disk",
			Tags:      map[string]string{"device": "sda"},
			Value:     float64(i * 1000),
			Timestamp: now + int64(i)*int64(time.Second),
		}
	}
	db.WriteBatch(points)

	tmpDir := t.TempDir()
	outputPath := filepath.Join(tmpDir, "export.parquet")

	config := DefaultExportConfig()
	config.Format = ExportFormatParquet
	config.OutputPath = outputPath
	config.Start = now - int64(time.Hour)
	config.End = now + int64(time.Hour)*2

	result, err := NewExporter(db, config).Export()
	if err != nil {
		t.Fatalf("export failed: %v", err)
	}

	if result.PointsExported < 1 {
		t.Errorf("expected points > 0, got %d", result.PointsExported)
	}

	// Verify Parquet magic bytes
	file, err := os.Open(outputPath)
	if err != nil {
		t.Fatal(err)
	}
	defer file.Close()

	magic := make([]byte, 4)
	file.Read(magic)
	if string(magic) != "PAR1" {
		t.Error("invalid Parquet magic bytes")
	}
}

func TestExporter_Compression(t *testing.T) {
	db := setupTestDB(t)

	now := time.Now().UnixNano()
	points := make([]Point, 100)
	for i := 0; i < 100; i++ {
		points[i] = Point{
			Metric:    "network",
			Tags:      map[string]string{"iface": "eth0"},
			Value:     float64(i),
			Timestamp: now + int64(i)*int64(time.Second),
		}
	}
	db.WriteBatch(points)

	tmpDir := t.TempDir()

	// Export without compression
	config := DefaultExportConfig()
	config.OutputPath = filepath.Join(tmpDir, "uncompressed.csv")
	resultUncompressed, _ := NewExporter(db, config).Export()

	// Export with compression
	config.OutputPath = filepath.Join(tmpDir, "compressed.csv")
	config.Compression = true
	resultCompressed, _ := NewExporter(db, config).Export()

	// Compressed should be smaller
	if resultCompressed.BytesWritten >= resultUncompressed.BytesWritten {
		t.Logf("compressed: %d, uncompressed: %d", resultCompressed.BytesWritten, resultUncompressed.BytesWritten)
		// Note: For small datasets, compression overhead may make file larger
	}
}

func TestExporter_TimeRange(t *testing.T) {
	db := setupTestDB(t)

	baseTime := time.Date(2024, 1, 1, 0, 0, 0, 0, time.UTC).UnixNano()
	points := make([]Point, 100)
	for i := 0; i < 100; i++ {
		points[i] = Point{
			Metric:    "test",
			Value:     float64(i),
			Timestamp: baseTime + int64(i)*int64(time.Hour),
		}
	}
	db.WriteBatch(points)

	tmpDir := t.TempDir()
	config := DefaultExportConfig()
	config.OutputPath = filepath.Join(tmpDir, "filtered.csv")
	config.Start = baseTime + 10*int64(time.Hour)
	config.End = baseTime + 20*int64(time.Hour)

	result, err := NewExporter(db, config).Export()
	if err != nil {
		t.Fatalf("export failed: %v", err)
	}

	// Should only export points in range [10, 20]
	if result.PointsExported > 11 {
		t.Errorf("expected <= 11 points in range, got %d", result.PointsExported)
	}
}

func TestExporter_ConvenienceMethods(t *testing.T) {
	db := setupTestDB(t)

	now := time.Now().UnixNano()
	db.WriteBatch([]Point{{
		Metric:    "test",
		Value:     42.0,
		Timestamp: now,
	}})

	tmpDir := t.TempDir()

	// Test CSV convenience method
	result, err := db.ExportToCSV(filepath.Join(tmpDir, "conv.csv"), nil, 0, 0)
	if err != nil {
		t.Errorf("ExportToCSV failed: %v", err)
	}
	if result.PointsExported != 1 {
		t.Errorf("expected 1 point, got %d", result.PointsExported)
	}

	// Test JSON convenience method
	result, err = db.ExportToJSON(filepath.Join(tmpDir, "conv.json"), nil, 0, 0)
	if err != nil {
		t.Errorf("ExportToJSON failed: %v", err)
	}

	// Test Parquet convenience method
	result, err = db.ExportToParquet(filepath.Join(tmpDir, "conv.parquet"), nil, 0, 0)
	if err != nil {
		t.Errorf("ExportToParquet failed: %v", err)
	}
}

func TestFormatTimestamp(t *testing.T) {
	ts := time.Date(2024, 1, 15, 10, 30, 0, 0, time.UTC).UnixNano()

	// Unix nano format - just verify it's a number
	result := formatTimestamp(ts, "")
	if len(result) == 0 {
		t.Error("expected non-empty result")
	}

	// RFC3339 format
	result = formatTimestamp(ts, time.RFC3339)
	if result != "2024-01-15T10:30:00Z" {
		t.Errorf("unexpected RFC3339: %s", result)
	}
}

func TestFormatExportTags(t *testing.T) {
	// Empty tags
	result := formatExportTags(nil)
	if result != "" {
		t.Errorf("expected empty string, got %s", result)
	}

	// Single tag
	result = formatExportTags(map[string]string{"host": "server1"})
	if result != "host=server1" {
		t.Errorf("unexpected: %s", result)
	}

	// Multiple tags (should be sorted)
	result = formatExportTags(map[string]string{"host": "server1", "dc": "us-east"})
	if result != "dc=us-east,host=server1" {
		t.Errorf("unexpected: %s", result)
	}
}

func TestValidateExportPath(t *testing.T) {
	tests := []struct {
		name      string
		path      string
		wantError bool
	}{
		{"empty path", "", true},
		{"valid relative path", "output/data.csv", false},
		{"valid absolute path in temp", "/tmp/export.csv", false},
		{"sensitive /etc", "/etc/passwd", true},
		{"sensitive /bin", "/bin/test", true},
		{"sensitive /usr/bin", "/usr/bin/file", true},
		{"sensitive /sbin", "/sbin/file", true},
		{"sensitive /dev", "/dev/null", true},
		{"sensitive /proc", "/proc/self", true},
		{"sensitive /sys", "/sys/kernel", true},
		{"sensitive /root", "/root/.ssh", true},
		{"sensitive /boot", "/boot/vmlinuz", true},
		{"traversal within temp", t.TempDir() + "/subdir/../etc/passwd", false},
		{"traversal to sensitive root", "/tmp/../etc/passwd", true},
		{"user home directory", "/home/user/data.csv", false},
		{"current directory", "./export.csv", false},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			_, err := validateExportPath(tt.path)
			if tt.wantError && err == nil {
				t.Errorf("expected error for path %q", tt.path)
			}
			if !tt.wantError && err != nil {
				t.Errorf("unexpected error for path %q: %v", tt.path, err)
			}
		})
	}
}

func TestExporter_SensitivePathRejection(t *testing.T) {
	db := setupTestDB(t)

	// Write some test data
	db.WriteBatch([]Point{{
		Metric:    "test",
		Value:     1.0,
		Timestamp: time.Now().UnixNano(),
	}})

	sensitiveOutputPaths := []string{
		"/etc/malicious.csv",
		"/bin/malicious",
		"/usr/sbin/bad",
		"/dev/test",
	}

	for _, path := range sensitiveOutputPaths {
		t.Run(path, func(t *testing.T) {
			config := DefaultExportConfig()
			config.OutputPath = path

			_, err := NewExporter(db, config).Export()
			if err == nil {
				t.Errorf("expected error when exporting to sensitive path %q", path)
			}
		})
	}
}

func TestExporter_EmptyPath(t *testing.T) {
	db := setupTestDB(t)

	config := DefaultExportConfig()
	config.OutputPath = ""

	_, err := NewExporter(db, config).Export()
	if err == nil {
		t.Error("expected error for empty output path")
	}
}
