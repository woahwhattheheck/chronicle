//go:build experimental

package chronicle

import (
	"testing"
	"time"
)

func TestFederatedLearningConfig(t *testing.T) {
	cfg := DefaultFederatedLearningConfig()

	if !cfg.Enabled {
		t.Error("expected Enabled to be true")
	}
	if cfg.Role != RoleParticipant {
		t.Errorf("expected Role Participant, got %s", cfg.Role)
	}
	if cfg.MinParticipants != 2 {
		t.Errorf("expected MinParticipants 2, got %d", cfg.MinParticipants)
	}
	if cfg.AggregationStrategy != StrategyFedAvg {
		t.Errorf("expected AggregationStrategy FedAvg, got %s", cfg.AggregationStrategy)
	}
}

func TestFederatedLearningEngine_RegisterModel(t *testing.T) {
	db := setupTestDB(t)
	defer func() { _ = db.Close() }()

	engine := NewFederatedLearningEngine(db, DefaultFederatedLearningConfig())
	defer func() { _ = engine.Close() }()

	model := &FederatedModel{
		ID:      "model1",
		Name:    "Anomaly Detector",
		Type:    "anomaly_detection",
		Weights: []float64{0.1, 0.2, 0.3},
	}

	err := engine.RegisterModel(model)
	if err != nil {
		t.Fatalf("failed to register model: %v", err)
	}

	retrieved, err := engine.GetModel("model1")
	if err != nil {
		t.Fatalf("failed to get model: %v", err)
	}
	if retrieved.Name != "Anomaly Detector" {
		t.Errorf("expected name 'Anomaly Detector', got '%s'", retrieved.Name)
	}
}

func TestFederatedLearningEngine_AggregateUpdates_FedAvg(t *testing.T) {
	db := setupTestDB(t)
	defer func() { _ = db.Close() }()

	config := DefaultFederatedLearningConfig()
	config.Role = RoleCoordinator
	config.AggregationStrategy = StrategyFedAvg

	engine := NewFederatedLearningEngine(db, config)
	defer func() { _ = engine.Close() }()

	globalModel := &FederatedModel{
		ID:      "model1",
		Name:    "Test Model",
		Weights: []float64{1.0, 2.0, 3.0},
	}

	updates := []*ParticipantUpdate{
		{
			ParticipantID: "p1",
			ModelWeights:  []float64{1.1, 2.1, 3.1},
			SampleCount:   100,
		},
		{
			ParticipantID: "p2",
			ModelWeights:  []float64{0.9, 1.9, 2.9},
			SampleCount:   100,
		},
	}

	aggregated, err := engine.AggregateUpdates(updates, globalModel)
	if err != nil {
		t.Fatalf("failed to aggregate: %v", err)
	}

	// Average should be (1.1+0.9)/2 = 1.0, (2.1+1.9)/2 = 2.0, (3.1+2.9)/2 = 3.0
	expected := []float64{1.0, 2.0, 3.0}
	for i, w := range aggregated.Weights {
		if w != expected[i] {
			t.Errorf("weight[%d]: expected %f, got %f", i, expected[i], w)
		}
	}
}

func TestFederatedLearningEngine_AggregateUpdates_Weighted(t *testing.T) {
	db := setupTestDB(t)
	defer func() { _ = db.Close() }()

	config := DefaultFederatedLearningConfig()
	config.Role = RoleCoordinator
	config.AggregationStrategy = StrategyWeighted

	engine := NewFederatedLearningEngine(db, config)
	defer func() { _ = engine.Close() }()

	globalModel := &FederatedModel{
		ID:      "model1",
		Weights: []float64{0, 0, 0},
	}

	updates := []*ParticipantUpdate{
		{
			ParticipantID: "p1",
			ModelWeights:  []float64{1.0, 2.0, 3.0},
			SampleCount:   300, // 75% weight
		},
		{
			ParticipantID: "p2",
			ModelWeights:  []float64{3.0, 4.0, 5.0},
			SampleCount:   100, // 25% weight
		},
	}

	aggregated, err := engine.AggregateUpdates(updates, globalModel)
	if err != nil {
		t.Fatalf("failed to aggregate: %v", err)
	}

	// Weighted average: 0.75*1 + 0.25*3 = 1.5, etc.
	expectedFirst := 1.0*0.75 + 3.0*0.25 // 1.5
	if aggregated.Weights[0] != expectedFirst {
		t.Errorf("weight[0]: expected %f, got %f", expectedFirst, aggregated.Weights[0])
	}
}

func TestFederatedLearningEngine_AggregateUpdates_Median(t *testing.T) {
	db := setupTestDB(t)
	defer func() { _ = db.Close() }()

	config := DefaultFederatedLearningConfig()
	config.Role = RoleCoordinator
	config.AggregationStrategy = StrategyMedian

	engine := NewFederatedLearningEngine(db, config)
	defer func() { _ = engine.Close() }()

	globalModel := &FederatedModel{
		ID:      "model1",
		Weights: []float64{0, 0, 0},
	}

	updates := []*ParticipantUpdate{
		{ParticipantID: "p1", ModelWeights: []float64{1.0, 2.0, 3.0}, SampleCount: 100},
		{ParticipantID: "p2", ModelWeights: []float64{5.0, 6.0, 7.0}, SampleCount: 100},
		{ParticipantID: "p3", ModelWeights: []float64{2.0, 3.0, 4.0}, SampleCount: 100},
	}

	aggregated, err := engine.AggregateUpdates(updates, globalModel)
	if err != nil {
		t.Fatalf("failed to aggregate: %v", err)
	}

	// Median: [1,2,5] -> 2, [2,3,6] -> 3, [3,4,7] -> 4
	expected := []float64{2.0, 3.0, 4.0}
	for i, w := range aggregated.Weights {
		if w != expected[i] {
			t.Errorf("weight[%d]: expected %f, got %f", i, expected[i], w)
		}
	}
}

func TestFederatedLearningEngine_AggregateUpdates_TrimmedMean(t *testing.T) {
	db := setupTestDB(t)
	defer func() { _ = db.Close() }()

	config := DefaultFederatedLearningConfig()
	config.Role = RoleCoordinator
	config.AggregationStrategy = StrategyTrimmedMean

	engine := NewFederatedLearningEngine(db, config)
	defer func() { _ = engine.Close() }()

	globalModel := &FederatedModel{
		ID:      "model1",
		Weights: []float64{0},
	}

	// Include outliers that should be trimmed
	updates := []*ParticipantUpdate{
		{ParticipantID: "p1", ModelWeights: []float64{100.0}, SampleCount: 100}, // outlier
		{ParticipantID: "p2", ModelWeights: []float64{1.0}, SampleCount: 100},
		{ParticipantID: "p3", ModelWeights: []float64{2.0}, SampleCount: 100},
		{ParticipantID: "p4", ModelWeights: []float64{3.0}, SampleCount: 100},
		{ParticipantID: "p5", ModelWeights: []float64{-100.0}, SampleCount: 100}, // outlier
	}

	aggregated, err := engine.AggregateUpdates(updates, globalModel)
	if err != nil {
		t.Fatalf("failed to aggregate: %v", err)
	}

	// After trimming outliers: [1,2,3] -> mean = 2.0
	if aggregated.Weights[0] != 2.0 {
		t.Errorf("expected 2.0 after trimming, got %f", aggregated.Weights[0])
	}
}

func TestFederatedLearningEngine_RegisterParticipant(t *testing.T) {
	db := setupTestDB(t)
	defer func() { _ = db.Close() }()

	config := DefaultFederatedLearningConfig()
	config.Role = RoleCoordinator

	engine := NewFederatedLearningEngine(db, config)
	defer func() { _ = engine.Close() }()

	participant := &FederatedParticipant{
		ID:   "participant1",
		Name: "Edge Node 1",
		URL:  "http://edge1:8080",
	}

	err := engine.RegisterParticipant(participant)
	if err != nil {
		t.Fatalf("failed to register participant: %v", err)
	}

	participants := engine.ListParticipants()
	if len(participants) != 1 {
		t.Errorf("expected 1 participant, got %d", len(participants))
	}

	if participants[0].Status != FLStatusActive {
		t.Errorf("expected status Active, got %s", participants[0].Status)
	}
}

func TestFederatedLearningEngine_Stats(t *testing.T) {
	db := setupTestDB(t)
	defer func() { _ = db.Close() }()

	config := DefaultFederatedLearningConfig()
	config.Role = RoleCoordinator

	engine := NewFederatedLearningEngine(db, config)
	defer func() { _ = engine.Close() }()

	_ = engine.RegisterModel(&FederatedModel{ID: "m1", Name: "Model 1", Weights: []float64{1, 2, 3}})

	stats := engine.Stats()
	if stats.ModelsRegistered != 1 {
		t.Errorf("expected ModelsRegistered 1, got %d", stats.ModelsRegistered)
	}
	if stats.Role != "coordinator" {
		t.Errorf("expected Role coordinator, got %s", stats.Role)
	}
}

func TestFederatedModel(t *testing.T) {
	model := &FederatedModel{
		ID:      "model-123",
		Name:    "Anomaly Detection Model",
		Type:    "anomaly_detection",
		Version: 1,
		Weights: []float64{0.1, 0.2, 0.3, 0.4},
		Hyperparams: map[string]any{
			"learning_rate": 0.01,
			"batch_size":    32,
		},
		Metrics: &ModelMetrics{
			Accuracy: 0.95,
			Loss:     0.05,
			F1Score:  0.92,
		},
		CreatedAt: time.Now(),
	}

	if model.ID != "model-123" {
		t.Errorf("expected ID 'model-123', got '%s'", model.ID)
	}
	if len(model.Weights) != 4 {
		t.Errorf("expected 4 weights, got %d", len(model.Weights))
	}
	if model.Metrics.Accuracy != 0.95 {
		t.Errorf("expected accuracy 0.95, got %f", model.Metrics.Accuracy)
	}
}

func TestTrainingRound(t *testing.T) {
	round := &TrainingRound{
		ID:          "round-1",
		ModelID:     "model-1",
		RoundNumber: 5,
		Status:      RoundTraining,
		StartTime:   time.Now(),
		Participants: map[string]*ParticipantUpdate{
			"p1": {ParticipantID: "p1", SampleCount: 100},
			"p2": {ParticipantID: "p2", SampleCount: 200},
		},
		Config: &RoundConfig{
			LocalEpochs:  2,
			BatchSize:    64,
			LearningRate: 0.001,
		},
	}

	if round.RoundNumber != 5 {
		t.Errorf("expected round number 5, got %d", round.RoundNumber)
	}
	if len(round.Participants) != 2 {
		t.Errorf("expected 2 participants, got %d", len(round.Participants))
	}
}

func TestParticipantUpdate(t *testing.T) {
	update := &ParticipantUpdate{
		ParticipantID: "node-1",
		ModelWeights:  []float64{0.5, 0.6, 0.7},
		Gradients:     []float64{0.01, 0.02, 0.03},
		SampleCount:   1000,
		Metrics: &ModelMetrics{
			Loss:        0.1,
			SampleCount: 1000,
		},
		ReceivedAt: time.Now(),
		Verified:   true,
	}

	if update.SampleCount != 1000 {
		t.Errorf("expected sample count 1000, got %d", update.SampleCount)
	}
	if !update.Verified {
		t.Error("expected verified to be true")
	}
}

func TestCloneModel(t *testing.T) {
	original := &FederatedModel{
		ID:      "m1",
		Name:    "Original",
		Version: 1,
		Weights: []float64{1.0, 2.0, 3.0},
		Hyperparams: map[string]any{
			"key": "value",
		},
	}

	cloned := cloneModel(original)

	if cloned.ID != original.ID {
		t.Errorf("expected ID %s, got %s", original.ID, cloned.ID)
	}

	// Modify cloned weights - original should be unchanged
	cloned.Weights[0] = 999.0
	if original.Weights[0] == 999.0 {
		t.Error("original was modified when clone was changed")
	}
}

func TestCompressGradients(t *testing.T) {
	gradients := []float64{0.001, 0.5, 0.002, 1.0, 0.003}
	threshold := 0.01

	compressed := compressGradients(gradients, threshold)

	// Only values above threshold should remain
	if compressed[0] != 0 {
		t.Errorf("expected 0 (below threshold), got %f", compressed[0])
	}
	if compressed[1] != 0.5 {
		t.Errorf("expected 0.5, got %f", compressed[1])
	}
	if compressed[3] != 1.0 {
		t.Errorf("expected 1.0, got %f", compressed[3])
	}
}

func TestRoundStatus(t *testing.T) {
	statuses := []RoundStatus{
		RoundPending,
		RoundTraining,
		RoundAggregating,
		RoundCompleted,
		RoundFailed,
	}

	expected := []string{
		"pending", "training", "aggregating", "completed", "failed",
	}

	for i, status := range statuses {
		if string(status) != expected[i] {
			t.Errorf("expected '%s', got '%s'", expected[i], status)
		}
	}
}

func TestParticipantStatus(t *testing.T) {
	statuses := []ParticipantStatus{
		FLStatusActive,
		FLStatusInactive,
		FLStatusTraining,
		FLStatusError,
	}

	if len(statuses) != 4 {
		t.Errorf("expected 4 participant statuses, got %d", len(statuses))
	}
}

func TestAggregationStrategy(t *testing.T) {
	strategies := []AggregationStrategy{
		StrategyFedAvg,
		StrategyFedProx,
		StrategyFedAdam,
		StrategyWeighted,
		StrategyMedian,
		StrategyTrimmedMean,
	}

	if len(strategies) != 6 {
		t.Errorf("expected 6 aggregation strategies, got %d", len(strategies))
	}
}

func TestFederatedParticipant(t *testing.T) {
	participant := &FederatedParticipant{
		ID:            "node-1",
		Name:          "Edge Node",
		URL:           "http://edge:8080",
		Status:        FLStatusActive,
		LastHeartbeat: time.Now(),
		Capabilities:  []string{"anomaly_detection", "forecasting"},
		DataStats: &DataStatistics{
			SampleCount:  10000,
			FeatureCount: 12,
			MetricTypes:  []string{"cpu", "memory"},
		},
		JoinedAt: time.Now(),
	}

	if participant.ID != "node-1" {
		t.Errorf("expected ID 'node-1', got '%s'", participant.ID)
	}
	if len(participant.Capabilities) != 2 {
		t.Errorf("expected 2 capabilities, got %d", len(participant.Capabilities))
	}
	if participant.DataStats.SampleCount != 10000 {
		t.Errorf("expected 10000 samples, got %d", participant.DataStats.SampleCount)
	}
}

func TestModelMetrics(t *testing.T) {
	metrics := &ModelMetrics{
		Accuracy:    0.95,
		Loss:        0.05,
		Precision:   0.92,
		Recall:      0.90,
		F1Score:     0.91,
		SampleCount: 5000,
	}

	if metrics.Accuracy != 0.95 {
		t.Errorf("expected accuracy 0.95, got %f", metrics.Accuracy)
	}
	if metrics.F1Score != 0.91 {
		t.Errorf("expected F1 0.91, got %f", metrics.F1Score)
	}
}

func TestRoundConfig(t *testing.T) {
	config := &RoundConfig{
		LocalEpochs:  5,
		BatchSize:    128,
		LearningRate: 0.001,
		MinSamples:   500,
	}

	if config.LocalEpochs != 5 {
		t.Errorf("expected 5 epochs, got %d", config.LocalEpochs)
	}
	if config.BatchSize != 128 {
		t.Errorf("expected batch size 128, got %d", config.BatchSize)
	}
}

func TestDataStatistics(t *testing.T) {
	stats := &DataStatistics{
		SampleCount:  50000,
		FeatureCount: 24,
		MetricTypes:  []string{"cpu", "memory", "disk", "network"},
		Distribution: map[string]float64{
			"cpu":     0.3,
			"memory":  0.3,
			"disk":    0.2,
			"network": 0.2,
		},
	}

	if stats.SampleCount != 50000 {
		t.Errorf("expected 50000 samples, got %d", stats.SampleCount)
	}
	if len(stats.MetricTypes) != 4 {
		t.Errorf("expected 4 metric types, got %d", len(stats.MetricTypes))
	}
}

func TestTrainingSample(t *testing.T) {
	sample := TrainingSample{
		Features:  []float64{0.1, 0.2, 0.3, 0.4, 0.5},
		Label:     1.0,
		Timestamp: time.Now().UnixNano(),
		Weight:    1.0,
	}

	if len(sample.Features) != 5 {
		t.Errorf("expected 5 features, got %d", len(sample.Features))
	}
	if sample.Label != 1.0 {
		t.Errorf("expected label 1.0, got %f", sample.Label)
	}
}

func TestFederatedLearningEngine_VerifyUpdate(t *testing.T) {
	db := setupTestDB(t)
	defer func() { _ = db.Close() }()

	engine := NewFederatedLearningEngine(db, DefaultFederatedLearningConfig())
	defer func() { _ = engine.Close() }()

	// Valid update
	validUpdate := &ParticipantUpdate{
		ParticipantID: "p1",
		ModelWeights:  []float64{1.0, 2.0, 3.0},
		SampleCount:   100,
	}
	if !engine.verifyUpdate(validUpdate) {
		t.Error("expected valid update to pass verification")
	}

	// Invalid - empty weights
	invalidUpdate1 := &ParticipantUpdate{
		ParticipantID: "p1",
		ModelWeights:  []float64{},
		SampleCount:   100,
	}
	if engine.verifyUpdate(invalidUpdate1) {
		t.Error("expected empty weights to fail verification")
	}

	// Invalid - zero sample count
	invalidUpdate2 := &ParticipantUpdate{
		ParticipantID: "p1",
		ModelWeights:  []float64{1.0},
		SampleCount:   0,
	}
	if engine.verifyUpdate(invalidUpdate2) {
		t.Error("expected zero samples to fail verification")
	}
}

func TestFederatedLearningExtractFeatures(t *testing.T) {
	points := make([]Point, 20)
	for i := range points {
		points[i] = Point{Value: float64(i)}
	}

	// Should return nil for index < windowSize (10)
	features := extractFeatures(points, 5)
	if features != nil {
		t.Error("expected nil for index < windowSize")
	}

	// Should return features for valid index
	features = extractFeatures(points, 15)
	if features == nil {
		t.Error("expected features for valid index")
	}
	if len(features) != 12 { // 10 window + 2 stats (mean, stddev)
		t.Errorf("expected 12 features, got %d", len(features))
	}
}

func TestGaussianNoise(t *testing.T) {
	// Just verify it doesn't crash and returns different values
	values := make(map[float64]bool)
	for i := 0; i < 10; i++ {
		noise := gaussianNoise()
		values[noise] = true
	}
	// Should have some variation
	if len(values) < 5 {
		t.Error("expected some variation in noise values")
	}
}
