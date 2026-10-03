//go:build experimental

package chronicle

import (
	"fmt"
	"math"
	"testing"
)

func TestFederatedLearningComm_Smoke(t *testing.T) {
	// Smoke test: verify FederatedLearningStats types and functions from federated_learning_comm.go are accessible.
	if testing.Short() {
		t.Skip("skipping smoke test in short mode")
	}
}

func TestLocalTrainerTrainEpochSampleMembership(t *testing.T) {
	for _, count := range []int{0, 1, 2, 7, 32} {
		t.Run(fmt.Sprintf("samples_%d", count), func(t *testing.T) {
			samples := make([]TrainingSample, count)
			for i := range samples {
				features := make([]float64, count)
				features[i] = 1
				samples[i] = TrainingSample{
					Features: features, Label: float64(i + 1), Timestamp: int64(i), Weight: 1,
				}
			}
			for range 16 {
				trainer := &LocalTrainer{
					model:           &FederatedModel{Weights: make([]float64, count)},
					trainingSamples: samples,
				}
				// A full batch of orthogonal samples has an order-independent update:
				// each coordinate receives exactly its sample's label once.
				trainer.trainEpoch(max(1, count), float64(max(1, count)))
				for i, weight := range trainer.model.Weights {
					if want := float64(i + 1); math.Abs(weight-want) > 1e-12 {
						t.Fatalf("weight[%d] = %v, want %v after one epoch", i, weight, want)
					}
				}
				for i, sample := range trainer.trainingSamples {
					if sample.Timestamp != int64(i) || sample.Label != float64(i+1) || sample.Features[i] != 1 {
						t.Fatalf("source sample %d changed during shuffling: %+v", i, sample)
					}
				}
			}
		})
	}
}
