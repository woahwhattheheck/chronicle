package chronicle

import (
	"sync"
	"testing"
	"time"
)

func TestCloudRelayEnqueue(t *testing.T) {
	db := setupTestDB(t)

	config := DefaultCloudRelayConfig()
	config.MaxQueueSize = 100
	relay := NewCloudRelay(db, config)

	p := Point{
		Metric:    "temperature",
		Tags:      map[string]string{"sensor": "1"},
		Value:     22.5,
		Timestamp: time.Now().UnixNano(),
	}

	if err := relay.Enqueue(p); err != nil {
		t.Fatalf("Enqueue failed: %v", err)
	}

	stats := relay.Stats()
	if stats.PointsQueued != 1 {
		t.Errorf("expected 1 queued point, got %d", stats.PointsQueued)
	}
	if stats.QueueDepth != 1 {
		t.Errorf("expected queue depth 1, got %d", stats.QueueDepth)
	}
}

func TestCloudRelayEnqueueBatch(t *testing.T) {
	db := setupTestDB(t)

	config := DefaultCloudRelayConfig()
	relay := NewCloudRelay(db, config)

	points := make([]Point, 10)
	for i := range points {
		points[i] = Point{
			Metric:    "cpu",
			Value:     float64(i),
			Timestamp: time.Now().UnixNano(),
		}
	}

	if err := relay.EnqueueBatch(points); err != nil {
		t.Fatalf("EnqueueBatch failed: %v", err)
	}

	stats := relay.Stats()
	if stats.PointsQueued != 10 {
		t.Errorf("expected 10 queued points, got %d", stats.PointsQueued)
	}
}

func TestCloudRelayQueueFull(t *testing.T) {
	db := setupTestDB(t)

	config := DefaultCloudRelayConfig()
	config.MaxQueueSize = 2
	relay := NewCloudRelay(db, config)

	p := Point{Metric: "m", Value: 1, Timestamp: time.Now().UnixNano()}

	relay.Enqueue(p)
	relay.Enqueue(p)
	err := relay.Enqueue(p)
	if err == nil {
		t.Error("expected queue full error")
	}
}

func TestCloudRelayConcurrentStats(t *testing.T) {
	config := DefaultCloudRelayConfig()
	config.MaxQueueSize = 400
	relay := NewCloudRelay(setupTestDB(t), config)

	var writers sync.WaitGroup
	writers.Add(4)
	for worker := 0; worker < 4; worker++ {
		go func() {
			defer writers.Done()
			for i := 0; i < 100; i++ {
				if err := relay.Enqueue(Point{Metric: "concurrent", Value: float64(i)}); err != nil {
					t.Errorf("Enqueue failed: %v", err)
				}
			}
		}()
	}
	done := make(chan struct{})
	go func() {
		writers.Wait()
		close(done)
	}()
	for {
		select {
		case <-done:
			stats := relay.Stats()
			if stats.PointsQueued != 400 || stats.QueueDepth != 400 {
				t.Fatalf("lost queued points: %+v", stats)
			}
			return
		default:
			_ = relay.Stats()
		}
	}
}

func TestCloudRelayStatus(t *testing.T) {
	db := setupTestDB(t)

	config := DefaultCloudRelayConfig()
	relay := NewCloudRelay(db, config)

	if relay.Status() != RelayNodeOffline {
		t.Errorf("expected offline, got %s", relay.Status())
	}
}

func TestCloudRelayStartStop(t *testing.T) {
	db := setupTestDB(t)

	config := DefaultCloudRelayConfig()
	config.Enabled = true
	config.FlushInterval = 50 * time.Millisecond
	config.HeartbeatInterval = 50 * time.Millisecond
	relay := NewCloudRelay(db, config)

	relay.Start()
	if relay.Status() != RelayNodeOnline {
		t.Errorf("expected online after start, got %s", relay.Status())
	}
	time.Sleep(30 * time.Millisecond)
	relay.Stop()
	if relay.Status() != RelayNodeOffline {
		t.Errorf("expected offline after stop, got %s", relay.Status())
	}
}

func TestCloudRelayCollectBatch(t *testing.T) {
	db := setupTestDB(t)

	config := DefaultCloudRelayConfig()
	config.BatchSize = 5
	relay := NewCloudRelay(db, config)

	for i := 0; i < 3; i++ {
		relay.Enqueue(Point{Metric: "m", Value: float64(i), Timestamp: time.Now().UnixNano()})
	}

	batch := relay.collectBatch()
	if len(batch) != 3 {
		t.Errorf("expected 3 points in batch, got %d", len(batch))
	}
}

func TestCloudRelayDefaultConfig(t *testing.T) {
	config := DefaultCloudRelayConfig()
	if config.BatchSize != 1000 {
		t.Errorf("expected batch size 1000, got %d", config.BatchSize)
	}
	if config.MaxRetries != 5 {
		t.Errorf("expected max retries 5, got %d", config.MaxRetries)
	}
	if !config.CompressionEnabled {
		t.Error("expected compression enabled by default")
	}
}
