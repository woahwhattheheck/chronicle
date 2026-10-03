//go:build experimental

package chronicle

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"math"
	"math/rand"
	randv2 "math/rand/v2"
	"sync/atomic"
	"time"
)

// Round aggregation, participant communication, health monitoring, and utility functions for federated learning.

func (e *FederatedLearningEngine) aggregateRound(round *TrainingRound) {
	e.roundsMu.Lock()
	round.Status = RoundAggregating
	e.roundsMu.Unlock()

	model, err := e.GetModel(round.ModelID)
	if err != nil {
		e.roundsMu.Lock()
		round.Status = RoundFailed
		e.roundsMu.Unlock()
		return
	}

	updates := make([]*ParticipantUpdate, 0, len(round.Participants))
	for _, u := range round.Participants {
		if u.Verified {
			updates = append(updates, u)
		}
	}

	aggregatedModel, err := e.AggregateUpdates(updates, model)
	if err != nil {
		e.roundsMu.Lock()
		round.Status = RoundFailed
		e.roundsMu.Unlock()
		return
	}

	// Update the global model
	e.modelsMu.Lock()
	e.models[model.ID] = aggregatedModel
	e.modelsMu.Unlock()

	e.roundsMu.Lock()
	round.AggregatedModel = aggregatedModel
	round.Status = RoundCompleted
	round.EndTime = time.Now()
	e.roundsMu.Unlock()

	atomic.AddInt64(&e.roundsCompleted, 1)
	atomic.AddInt64(&e.modelsAggregated, 1)

	// Broadcast updated model to participants
	e.broadcastModel(aggregatedModel)
}

func (e *FederatedLearningEngine) notifyParticipants(round *TrainingRound, model *FederatedModel) {
	e.participantsMu.RLock()
	defer e.participantsMu.RUnlock()

	e.roundsMu.Lock()
	round.Status = RoundTraining
	e.roundsMu.Unlock()

	msg := map[string]any{
		"type":     "start_training",
		"round_id": round.ID,
		"model":    model,
		"config":   round.Config,
	}
	payload, _ := json.Marshal(msg)

	for _, p := range e.participants {
		if p.Status == FLStatusActive {
			go e.sendToParticipant(p, payload)
		}
	}
}

func (e *FederatedLearningEngine) broadcastModel(model *FederatedModel) {
	e.participantsMu.RLock()
	defer e.participantsMu.RUnlock()

	msg := map[string]any{
		"type":  "model_update",
		"model": model,
	}
	payload, _ := json.Marshal(msg)

	for _, p := range e.participants {
		if p.Status == FLStatusActive {
			go e.sendToParticipant(p, payload)
		}
	}
}

func (e *FederatedLearningEngine) sendToParticipant(p *FederatedParticipant, payload []byte) {
	// In production, this would send HTTP request to participant
	_ = payload //nolint:errcheck // payload reserved for serialization
}

func (e *FederatedLearningEngine) verifyUpdate(update *ParticipantUpdate) bool {
	// Verify update integrity and validity
	if len(update.ModelWeights) == 0 {
		return false
	}
	if update.SampleCount <= 0 {
		return false
	}

	// Check for NaN or Inf values
	for _, w := range update.ModelWeights {
		if math.IsNaN(w) || math.IsInf(w, 0) {
			return false
		}
	}

	return true
}

func (e *FederatedLearningEngine) getLocalTrainingData(model *FederatedModel) ([]TrainingSample, error) {
	// Query local time series data for training
	ctx, cancel := context.WithTimeout(e.ctx, 30*time.Second)
	defer cancel()

	// Get recent data points
	query := &Query{
		Start: time.Now().Add(-24 * time.Hour).UnixNano(),
		End:   time.Now().UnixNano(),
		Limit: 10000,
	}

	result, err := e.db.ExecuteContext(ctx, query)
	if err != nil {
		return nil, err
	}

	samples := make([]TrainingSample, 0, len(result.Points))
	for i, point := range result.Points {
		// Simple feature extraction - windowed values
		features := extractFeatures(result.Points, i)
		if len(features) > 0 {
			samples = append(samples, TrainingSample{
				Features:  features,
				Label:     point.Value,
				Timestamp: point.Timestamp,
				Weight:    1.0,
			})
		}
	}

	return samples, nil
}

func extractFeatures(points []Point, index int) []float64 {
	windowSize := 10
	if index < windowSize {
		return nil
	}

	features := make([]float64, windowSize)
	for i := 0; i < windowSize; i++ {
		features[i] = points[index-windowSize+i].Value
	}

	// Add statistical features
	mean := 0.0
	for _, f := range features {
		mean += f
	}
	mean /= float64(len(features))

	variance := 0.0
	for _, f := range features {
		variance += (f - mean) * (f - mean)
	}
	variance /= float64(len(features))

	features = append(features, mean, math.Sqrt(variance))
	return features
}

func (e *FederatedLearningEngine) getParticipantID() (string, error) {
	b := make([]byte, 16)
	if _, err := rand.Read(b); err != nil {
		return "", fmt.Errorf("crypto/rand failed: %w", err)
	}
	hash := sha256.Sum256(b)
	return hex.EncodeToString(hash[:8]), nil
}

func (e *FederatedLearningEngine) coordinatorLoop() {
	defer e.wg.Done()

	heartbeatTicker := time.NewTicker(e.config.HeartbeatInterval)
	defer heartbeatTicker.Stop()

	timeoutTicker := time.NewTicker(30 * time.Second)
	defer timeoutTicker.Stop()

	for {
		select {
		case <-e.ctx.Done():
			return
		case <-heartbeatTicker.C:
			e.checkParticipantHealth()
		case <-timeoutTicker.C:
			e.checkRoundTimeouts()
		}
	}
}

func (e *FederatedLearningEngine) participantLoop() {
	defer e.wg.Done()

	heartbeatTicker := time.NewTicker(e.config.HeartbeatInterval)
	defer heartbeatTicker.Stop()

	for {
		select {
		case <-e.ctx.Done():
			return
		case <-heartbeatTicker.C:
			e.sendHeartbeat()
		}
	}
}

func (e *FederatedLearningEngine) checkParticipantHealth() {
	e.participantsMu.Lock()
	defer e.participantsMu.Unlock()

	staleThreshold := 3 * e.config.HeartbeatInterval

	for _, p := range e.participants {
		if time.Since(p.LastHeartbeat) > staleThreshold {
			p.Status = FLStatusInactive
		}
	}
}

func (e *FederatedLearningEngine) checkRoundTimeouts() {
	e.roundsMu.Lock()
	defer e.roundsMu.Unlock()

	for _, round := range e.rounds {
		if round.Status == RoundTraining && time.Since(round.StartTime) > e.config.RoundTimeout {
			round.Status = RoundFailed
			round.EndTime = time.Now()
		}
	}
}

func (e *FederatedLearningEngine) sendHeartbeat() {
	if e.config.CoordinatorURL == "" {
		return
	}

	// In production, send heartbeat to coordinator
}

// RegisterParticipant registers a participant (coordinator only).
func (e *FederatedLearningEngine) RegisterParticipant(participant *FederatedParticipant) error {
	if e.config.Role != RoleCoordinator {
		return errors.New("only coordinator can register participants")
	}

	e.participantsMu.Lock()
	defer e.participantsMu.Unlock()

	participant.Status = FLStatusActive
	participant.LastHeartbeat = time.Now()
	participant.JoinedAt = time.Now()

	e.participants[participant.ID] = participant
	return nil
}

// GetRound returns a training round by ID.
func (e *FederatedLearningEngine) GetRound(roundID string) (*TrainingRound, error) {
	e.roundsMu.RLock()
	defer e.roundsMu.RUnlock()

	round, ok := e.rounds[roundID]
	if !ok {
		return nil, fmt.Errorf("round not found: %s", roundID)
	}
	return round, nil
}

// ListRounds returns all training rounds.
func (e *FederatedLearningEngine) ListRounds() []*TrainingRound {
	e.roundsMu.RLock()
	defer e.roundsMu.RUnlock()

	rounds := make([]*TrainingRound, 0, len(e.rounds))
	for _, r := range e.rounds {
		rounds = append(rounds, r)
	}
	return rounds
}

// ListParticipants returns all participants.
func (e *FederatedLearningEngine) ListParticipants() []*FederatedParticipant {
	e.participantsMu.RLock()
	defer e.participantsMu.RUnlock()

	participants := make([]*FederatedParticipant, 0, len(e.participants))
	for _, p := range e.participants {
		participants = append(participants, p)
	}
	return participants
}

// Stats returns engine statistics.
func (e *FederatedLearningEngine) Stats() FederatedLearningStats {
	e.participantsMu.RLock()
	activeCount := 0
	for _, p := range e.participants {
		if p.Status == FLStatusActive {
			activeCount++
		}
	}
	e.participantsMu.RUnlock()

	return FederatedLearningStats{
		Role:               string(e.config.Role),
		ModelsRegistered:   int64(len(e.models)),
		RoundsCompleted:    atomic.LoadInt64(&e.roundsCompleted),
		ModelsAggregated:   atomic.LoadInt64(&e.modelsAggregated),
		UpdatesReceived:    atomic.LoadInt64(&e.updatesReceived),
		UpdatesSent:        atomic.LoadInt64(&e.updatesSent),
		ActiveParticipants: int64(activeCount),
	}
}

// FederatedLearningStats contains engine statistics.
type FederatedLearningStats struct {
	Role               string `json:"role"`
	ModelsRegistered   int64  `json:"models_registered"`
	RoundsCompleted    int64  `json:"rounds_completed"`
	ModelsAggregated   int64  `json:"models_aggregated"`
	UpdatesReceived    int64  `json:"updates_received"`
	UpdatesSent        int64  `json:"updates_sent"`
	ActiveParticipants int64  `json:"active_participants"`
}

// Close shuts down the federated learning engine.
func (e *FederatedLearningEngine) Close() error {
	e.cancel()
	e.wg.Wait()
	return nil
}

// LocalTrainer methods

func (t *LocalTrainer) trainEpoch(batchSize int, learningRate float64) {
	t.mu.Lock()
	defer t.mu.Unlock()

	if len(t.trainingSamples) == 0 {
		return
	}

	// Shuffle samples
	shuffled := make([]TrainingSample, len(t.trainingSamples))
	copy(shuffled, t.trainingSamples)
	for i := range shuffled {
		j := i + randv2.IntN(len(shuffled)-i)
		shuffled[i], shuffled[j] = shuffled[j], shuffled[i]
	}

	// Process in batches
	for i := 0; i < len(shuffled); i += batchSize {
		end := i + batchSize
		if end > len(shuffled) {
			end = len(shuffled)
		}
		batch := shuffled[i:end]
		t.processBatch(batch, learningRate)
	}
}

func (t *LocalTrainer) processBatch(batch []TrainingSample, learningRate float64) {
	if len(batch) == 0 || len(t.model.Weights) == 0 {
		return
	}

	// Initialize gradients if needed
	if len(t.model.Gradients) != len(t.model.Weights) {
		t.model.Gradients = make([]float64, len(t.model.Weights))
	}

	// Compute gradients via simple gradient descent
	for _, sample := range batch {
		// Forward pass - simple linear model
		prediction := 0.0
		for j, w := range t.model.Weights {
			if j < len(sample.Features) {
				prediction += w * sample.Features[j]
			}
		}

		// Loss gradient (MSE)
		error := prediction - sample.Label

		// Backprop
		for j := range t.model.Gradients {
			if j < len(sample.Features) {
				t.model.Gradients[j] += error * sample.Features[j] * sample.Weight
			}
		}
	}

	// Average gradients and update weights
	batchLen := float64(len(batch))
	for j := range t.model.Weights {
		t.model.Gradients[j] /= batchLen
		t.model.Weights[j] -= learningRate * t.model.Gradients[j]
	}
}

func (t *LocalTrainer) applyDifferentialPrivacy(epsilon, delta, clipNorm float64) {
	t.mu.Lock()
	defer t.mu.Unlock()

	// Gradient clipping
	gradNorm := 0.0
	for _, g := range t.model.Gradients {
		gradNorm += g * g
	}
	gradNorm = math.Sqrt(gradNorm)

	if gradNorm > clipNorm {
		scale := clipNorm / gradNorm
		for i := range t.model.Gradients {
			t.model.Gradients[i] *= scale
		}
	}

	// Add Gaussian noise for (ε, δ)-differential privacy
	// Noise scale = clipNorm * sqrt(2 * ln(1.25/delta)) / epsilon
	noiseScale := clipNorm * math.Sqrt(2*math.Log(1.25/delta)) / epsilon

	for i := range t.model.Gradients {
		noise := gaussianNoise() * noiseScale
		t.model.Gradients[i] += noise
		t.model.Weights[i] -= t.model.Gradients[i] // Apply noisy gradient
	}
}

func (t *LocalTrainer) computeMetrics() *ModelMetrics {
	t.mu.RLock()
	defer t.mu.RUnlock()

	if len(t.trainingSamples) == 0 {
		return &ModelMetrics{}
	}

	// Compute loss on training data
	totalLoss := 0.0
	for _, sample := range t.trainingSamples {
		prediction := 0.0
		for j, w := range t.model.Weights {
			if j < len(sample.Features) {
				prediction += w * sample.Features[j]
			}
		}
		error := prediction - sample.Label
		totalLoss += error * error
	}

	mse := totalLoss / float64(len(t.trainingSamples))

	return &ModelMetrics{
		Loss:        mse,
		SampleCount: int64(len(t.trainingSamples)),
	}
}

// Helper functions

func cloneModel(model *FederatedModel) *FederatedModel {
	weights := make([]float64, len(model.Weights))
	copy(weights, model.Weights)

	gradients := make([]float64, len(model.Gradients))
	copy(gradients, model.Gradients)

	hyperparams := make(map[string]any)
	for k, v := range model.Hyperparams {
		hyperparams[k] = v
	}

	return &FederatedModel{
		ID:          model.ID,
		Name:        model.Name,
		Type:        model.Type,
		Version:     model.Version,
		Weights:     weights,
		Gradients:   gradients,
		Hyperparams: hyperparams,
		Metrics:     model.Metrics,
		CreatedAt:   model.CreatedAt,
		UpdatedAt:   model.UpdatedAt,
	}
}

func compressGradients(gradients []float64, threshold float64) []float64 {
	// Top-K sparsification
	compressed := make([]float64, len(gradients))

	// Keep only gradients above threshold magnitude
	for i, g := range gradients {
		if math.Abs(g) > threshold {
			compressed[i] = g
		}
	}

	return compressed
}

func generateRoundID() (string, error) {
	b := make([]byte, 16)
	if _, err := rand.Read(b); err != nil {
		return "", fmt.Errorf("crypto/rand failed: %w", err)
	}
	return hex.EncodeToString(b), nil
}

func gaussianNoise() float64 {
	// Box-Muller transform for generating Gaussian noise
	u1, _ := randFloat64()
	u2, _ := randFloat64()
	return math.Sqrt(-2*math.Log(u1)) * math.Cos(2*math.Pi*u2)
}

func randFloat64() (float64, error) {
	b := make([]byte, 8)
	if _, err := rand.Read(b); err != nil {
		return 0, fmt.Errorf("crypto/rand failed: %w", err)
	}
	return float64(b[0]^b[1]^b[2]^b[3]^b[4]^b[5]^b[6]^b[7]) / 256.0, nil
}
