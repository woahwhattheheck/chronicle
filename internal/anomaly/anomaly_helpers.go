package anomaly

import (
	"encoding/json"
	"errors"
	"math"
	"math/rand/v2"
)

// ========== Helper Functions ==========

func normalize(data []float64) (normalized []float64, mean, std float64) {
	mean = 0
	for _, v := range data {
		mean += v
	}
	mean /= float64(len(data))

	variance := 0.0
	for _, v := range data {
		variance += (v - mean) * (v - mean)
	}
	std = math.Sqrt(variance / float64(len(data)))

	if std == 0 {
		std = 1
	}

	normalized = make([]float64, len(data))
	for i, v := range data {
		normalized[i] = (v - mean) / std
	}

	return normalized, mean, std
}

func randomSample(data []float64, size int) []float64 {
	if len(data) <= size {
		return data
	}

	sample := make([]float64, size)
	for i := 0; i < size; i++ {
		sample[i] = data[randInt(len(data))]
	}
	return sample
}

func minMax(data []float64) (float64, float64) {
	if len(data) == 0 {
		return 0, 0
	}
	min, max := data[0], data[0]
	for _, v := range data {
		if v < min {
			min = v
		}
		if v > max {
			max = v
		}
	}
	return min, max
}

func randFloat() float64 {
	// Simple random float between 0 and 1
	return float64(randInt(1000000)) / 1000000.0
}

func randInt(max int) int {
	return rand.IntN(max)
}

func randomMatrix(rows, cols int, scale float64) [][]float64 {
	matrix := make([][]float64, rows)
	for i := range matrix {
		matrix[i] = make([]float64, cols)
		for j := range matrix[i] {
			matrix[i][j] = (randFloat() - 0.5) * 2 * scale
		}
	}
	return matrix
}

func sigmoid(x float64) float64 {
	if x < -500 {
		return 0
	}
	if x > 500 {
		return 1
	}
	return 1.0 / (1.0 + math.Exp(-x))
}

func tanh(x float64) float64 {
	return math.Tanh(x)
}

func relu(x float64) float64 {
	if x > 0 {
		return x
	}
	return 0
}

func reconstructionError(original, reconstructed []float64) float64 {
	if len(original) != len(reconstructed) {
		minLen := len(original)
		if len(reconstructed) < minLen {
			minLen = len(reconstructed)
		}
		original = original[:minLen]
		reconstructed = reconstructed[:minLen]
	}

	if len(original) == 0 {
		return 0
	}

	sumSq := 0.0
	for i := range original {
		diff := original[i] - reconstructed[i]
		sumSq += diff * diff
	}

	return math.Sqrt(sumSq / float64(len(original)))
}

func stdDevSingle(data []float64) float64 {
	if len(data) == 0 {
		return 0
	}
	m := mean(data)
	variance := 0.0
	for _, v := range data {
		variance += (v - m) * (v - m)
	}
	return math.Sqrt(variance / float64(len(data)))
}

func percentile(sorted []float64, p float64) float64 {
	if len(sorted) == 0 {
		return 0
	}
	k := (p / 100) * float64(len(sorted)-1)
	f := math.Floor(k)
	c := math.Ceil(k)
	if f == c {
		return sorted[int(k)]
	}
	return sorted[int(f)]*(c-k) + sorted[int(c)]*(k-f)
}

// AnomalyDetectionDB wraps a DB with anomaly detection capabilities.
type AnomalyDetectionDB struct {
	ds       DataSource
	pw       PointWriter
	detector *AnomalyDetector
}

// NewAnomalyDetectionDB creates a new anomaly detection enabled database.
func NewAnomalyDetectionDB(ds DataSource, pw PointWriter, config AnomalyConfig) *AnomalyDetectionDB {
	detector := NewAnomalyDetector(ds, config)

	return &AnomalyDetectionDB{
		ds:       ds,
		pw:       pw,
		detector: detector,
	}
}

// Start starts anomaly detection.
func (adb *AnomalyDetectionDB) Start() {
	adb.detector.Start()
}

// Stop stops anomaly detection.
func (adb *AnomalyDetectionDB) Stop() {
	adb.detector.Stop()
}

// Detector returns the anomaly detector.
func (adb *AnomalyDetectionDB) Detector() *AnomalyDetector {
	return adb.detector
}

// WriteAndDetect writes a point and performs anomaly detection.
func (adb *AnomalyDetectionDB) WriteAndDetect(p PointData, context []float64) (*AnomalyResult, error) {
	// Write the point
	if err := adb.pw.WritePoint(p); err != nil {
		return nil, err
	}

	// Detect anomaly
	return adb.detector.Detect(p.Value, context)
}

// ExportModel exports the trained model as JSON.
func (ad *AnomalyDetector) ExportModel() ([]byte, error) {
	ad.mu.RLock()
	defer ad.mu.RUnlock()

	if !ad.trained {
		return nil, errors.New("model not trained")
	}

	export := map[string]any{
		"model":      ad.config.Model.String(),
		"trained":    ad.trained,
		"last_train": ad.lastTrain,
		"config":     ad.config,
		"statistical": map[string]float64{
			"mean": ad.statistical.mean,
			"std":  ad.statistical.std,
			"q1":   ad.statistical.q1,
			"q3":   ad.statistical.q3,
		},
	}

	return json.Marshal(export)
}

// ImportModel imports a previously exported model.
func (ad *AnomalyDetector) ImportModel(data []byte) error {
	var export map[string]any
	if err := json.Unmarshal(data, &export); err != nil {
		return err
	}

	ad.mu.Lock()
	defer ad.mu.Unlock()

	if stats, ok := export["statistical"].(map[string]any); ok {
		if mean, ok := stats["mean"].(float64); ok {
			ad.statistical.mean = mean
		}
		if std, ok := stats["std"].(float64); ok {
			ad.statistical.std = std
		}
		if q1, ok := stats["q1"].(float64); ok {
			ad.statistical.q1 = q1
		}
		if q3, ok := stats["q3"].(float64); ok {
			ad.statistical.q3 = q3
		}
	}

	ad.trained = true
	return nil
}
