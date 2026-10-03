package anomaly

import (
	"math"
	"testing"
)

func TestNormalize(t *testing.T) {
	data := []float64{1, 2, 3, 4, 5}
	normalized, mean, std := normalize(data)

	if len(normalized) != len(data) {
		t.Fatalf("expected %d elements, got %d", len(data), len(normalized))
	}
	if math.Abs(mean-3.0) > 1e-9 {
		t.Errorf("expected mean 3.0, got %f", mean)
	}
	if std <= 0 {
		t.Errorf("expected positive std, got %f", std)
	}
	// Normalized values should have mean ~0
	normalizedMean := 0.0
	for _, v := range normalized {
		normalizedMean += v
	}
	normalizedMean /= float64(len(normalized))
	if math.Abs(normalizedMean) > 1e-9 {
		t.Errorf("expected normalized mean ~0, got %f", normalizedMean)
	}
}

func TestNormalizeConstant(t *testing.T) {
	data := []float64{5, 5, 5, 5}
	_, _, std := normalize(data)

	// With zero variance, std should be set to 1
	if std != 1 {
		t.Errorf("expected std=1 for constant data, got %f", std)
	}
}

func TestRandomSample(t *testing.T) {
	data := []float64{1, 2, 3, 4, 5, 6, 7, 8, 9, 10}

	// Size larger than data returns original
	result := randomSample(data, 20)
	if len(result) != len(data) {
		t.Errorf("expected %d elements for oversized sample, got %d", len(data), len(result))
	}

	// Size smaller returns that size
	result = randomSample(data, 3)
	if len(result) != 3 {
		t.Errorf("expected 3 elements, got %d", len(result))
	}
	for _, value := range result {
		if value < 1 || value > 10 || math.Trunc(value) != value {
			t.Errorf("sample value %v is not in the input", value)
		}
	}
}

func TestRandomHelpersBounds(t *testing.T) {
	for _, max := range []int{1, 3, 10, 1000000, int(^uint(0) >> 1)} {
		for i := 0; i < 1000; i++ {
			if value := randInt(max); value < 0 || value >= max {
				t.Fatalf("randInt(%d) = %d, want a value in [0, %d)", max, value, max)
			}
		}
	}
	for i := 0; i < 1000; i++ {
		if value := randFloat(); value < 0 || value >= 1 {
			t.Fatalf("randFloat() = %v, want a value in [0, 1)", value)
		}
	}
}

func TestMinMax(t *testing.T) {
	tests := []struct {
		data    []float64
		wantMin float64
		wantMax float64
	}{
		{[]float64{}, 0, 0},
		{[]float64{5}, 5, 5},
		{[]float64{1, 5, 3, 9, 2}, 1, 9},
		{[]float64{-3, -1, -7}, -7, -1},
	}

	for _, tt := range tests {
		min, max := minMax(tt.data)
		if min != tt.wantMin || max != tt.wantMax {
			t.Errorf("minMax(%v) = (%f, %f), want (%f, %f)", tt.data, min, max, tt.wantMin, tt.wantMax)
		}
	}
}

func TestSigmoid(t *testing.T) {
	if v := sigmoid(0); math.Abs(v-0.5) > 1e-9 {
		t.Errorf("sigmoid(0) = %f, want 0.5", v)
	}
	if v := sigmoid(-600); v != 0 {
		t.Errorf("sigmoid(-600) = %f, want 0", v)
	}
	if v := sigmoid(600); v != 1 {
		t.Errorf("sigmoid(600) = %f, want 1", v)
	}
}

func TestRelu(t *testing.T) {
	if v := relu(5); v != 5 {
		t.Errorf("relu(5) = %f, want 5", v)
	}
	if v := relu(-3); v != 0 {
		t.Errorf("relu(-3) = %f, want 0", v)
	}
}

func TestTanh(t *testing.T) {
	if v := tanh(0); math.Abs(v) > 1e-9 {
		t.Errorf("tanh(0) = %f, want 0", v)
	}
}

func TestReconstructionError(t *testing.T) {
	// Identical arrays should have zero error
	a := []float64{1, 2, 3}
	if err := reconstructionError(a, a); err != 0 {
		t.Errorf("identical arrays should have 0 error, got %f", err)
	}

	// Empty arrays
	if err := reconstructionError(nil, nil); err != 0 {
		t.Errorf("empty arrays should have 0 error, got %f", err)
	}

	// Mismatched lengths
	b := []float64{1, 2}
	err := reconstructionError(a, b)
	if err < 0 {
		t.Errorf("reconstruction error should be non-negative, got %f", err)
	}
}

func TestStdDevSingle(t *testing.T) {
	if v := stdDevSingle(nil); v != 0 {
		t.Errorf("stdDevSingle(nil) = %f, want 0", v)
	}
	if v := stdDevSingle([]float64{5, 5, 5}); v != 0 {
		t.Errorf("stdDevSingle(constant) = %f, want 0", v)
	}
}

func TestPercentile(t *testing.T) {
	if v := percentile(nil, 50); v != 0 {
		t.Errorf("percentile(nil, 50) = %f, want 0", v)
	}
	data := []float64{1, 2, 3, 4, 5}
	if v := percentile(data, 0); v != 1 {
		t.Errorf("percentile(data, 0) = %f, want 1", v)
	}
	if v := percentile(data, 100); v != 5 {
		t.Errorf("percentile(data, 100) = %f, want 5", v)
	}
}

func TestRandomMatrix(t *testing.T) {
	m := randomMatrix(3, 4, 1.0)
	if len(m) != 3 {
		t.Fatalf("expected 3 rows, got %d", len(m))
	}
	for i, row := range m {
		if len(row) != 4 {
			t.Errorf("row %d: expected 4 cols, got %d", i, len(row))
		}
		for j, value := range row {
			if value < -1 || value >= 1 {
				t.Errorf("matrix[%d][%d] = %v, want a value in [-1, 1)", i, j, value)
			}
		}
	}
}

func TestAnomalyDetectionDB(t *testing.T) {
	// Test NewAnomalyDetectionDB creates properly
	config := AnomalyConfig{
		Sensitivity: 0.8,
	}
	adb := NewAnomalyDetectionDB(nil, nil, config)
	if adb == nil {
		t.Fatal("NewAnomalyDetectionDB returned nil")
	}
	if adb.Detector() == nil {
		t.Fatal("Detector() returned nil")
	}
}

func TestExportImportModel(t *testing.T) {
	config := AnomalyConfig{
		Sensitivity: 0.8,
	}
	detector := NewAnomalyDetector(nil, config)

	// Export untrained model should fail
	_, err := detector.ExportModel()
	if err == nil {
		t.Fatal("expected error exporting untrained model")
	}

	// Train the detector
	data := TimeSeriesData{
		Values:     make([]float64, 100),
		Timestamps: make([]int64, 100),
	}
	for i := range data.Values {
		data.Values[i] = float64(i) * 0.5
		data.Timestamps[i] = int64(i * 1000000000)
	}
	_ = detector.Train(data)

	// Export trained model
	exported, err := detector.ExportModel()
	if err != nil {
		t.Fatalf("ExportModel failed: %v", err)
	}
	if len(exported) == 0 {
		t.Fatal("exported data is empty")
	}

	// Import into new detector
	detector2 := NewAnomalyDetector(nil, config)
	if err := detector2.ImportModel(exported); err != nil {
		t.Fatalf("ImportModel failed: %v", err)
	}

	// Invalid JSON should fail
	if err := detector2.ImportModel([]byte("invalid")); err == nil {
		t.Fatal("expected error importing invalid JSON")
	}
}
