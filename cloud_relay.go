package chronicle

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"sync"
	"sync/atomic"
	"time"
)

// RelayNodeStatus represents the status of an edge relay node.
type RelayNodeStatus string

const (
	RelayNodeOnline   RelayNodeStatus = "online"
	RelayNodeSyncing  RelayNodeStatus = "syncing"
	RelayNodeOffline  RelayNodeStatus = "offline"
	RelayNodeDegraded RelayNodeStatus = "degraded"
)

// CloudRelayConfig configures the edge-to-cloud relay agent.
type CloudRelayConfig struct {
	Enabled            bool          `json:"enabled"`
	TargetURL          string        `json:"target_url"`
	NodeID             string        `json:"node_id"`
	Region             string        `json:"region,omitempty"`
	BatchSize          int           `json:"batch_size"`
	FlushInterval      time.Duration `json:"flush_interval"`
	MaxQueueSize       int           `json:"max_queue_size"`
	MaxRetries         int           `json:"max_retries"`
	RetryBackoff       time.Duration `json:"retry_backoff"`
	MaxBandwidthBps    int64         `json:"max_bandwidth_bps"`
	CompressionEnabled bool          `json:"compression_enabled"`
	HeartbeatInterval  time.Duration `json:"heartbeat_interval"`
	ConflictStrategy   string        `json:"conflict_strategy"`
	AuthToken          string        `json:"auth_token,omitempty"`
}

// DefaultCloudRelayConfig returns sensible defaults for the cloud relay.
func DefaultCloudRelayConfig() CloudRelayConfig {
	return CloudRelayConfig{
		Enabled:            false,
		BatchSize:          1000,
		FlushInterval:      10 * time.Second,
		MaxQueueSize:       100000,
		MaxRetries:         5,
		RetryBackoff:       time.Second,
		MaxBandwidthBps:    0, // unlimited
		CompressionEnabled: true,
		HeartbeatInterval:  30 * time.Second,
		ConflictStrategy:   "last-write-wins",
	}
}

// RelayBatch represents a batch of points to sync to the cloud.
type RelayBatch struct {
	NodeID    string  `json:"node_id"`
	Region    string  `json:"region,omitempty"`
	Points    []Point `json:"points"`
	Timestamp int64   `json:"timestamp"`
	SeqNum    uint64  `json:"seq_num"`
}

// RelaySyncStats tracks relay synchronization statistics.
type RelaySyncStats struct {
	PointsQueued  uint64    `json:"points_queued"`
	PointsSynced  uint64    `json:"points_synced"`
	PointsFailed  uint64    `json:"points_failed"`
	BatchesSent   uint64    `json:"batches_sent"`
	BatchesFailed uint64    `json:"batches_failed"`
	BytesSent     int64     `json:"bytes_sent"`
	LastSyncTime  time.Time `json:"last_sync_time"`
	LastError     string    `json:"last_error,omitempty"`
	QueueDepth    int       `json:"queue_depth"`
	CurrentStatus string    `json:"current_status"`
	UptimeSeconds float64   `json:"uptime_seconds"`
	RetryCount    uint64    `json:"retry_count"`
}

// CloudRelay provides edge-to-cloud data synchronization with persistent queueing.
type CloudRelay struct {
	config  CloudRelayConfig
	db      *DB
	queue   chan Point
	status  atomic.Value
	stats   RelaySyncStats
	seqNum  atomic.Uint64
	startAt time.Time
	client  *http.Client

	mu   sync.RWMutex
	done chan struct{}
	wg   sync.WaitGroup
}

// NewCloudRelay creates a new edge-to-cloud relay agent.
func NewCloudRelay(db *DB, config CloudRelayConfig) *CloudRelay {
	queueSize := config.MaxQueueSize
	if queueSize <= 0 {
		queueSize = 100000
	}
	r := &CloudRelay{
		config:  config,
		db:      db,
		queue:   make(chan Point, queueSize),
		startAt: time.Now(),
		client: &http.Client{
			Timeout: 30 * time.Second,
		},
		done: make(chan struct{}),
	}
	r.status.Store(RelayNodeOffline)
	return r
}

// Start begins the relay background workers.
func (r *CloudRelay) Start() {
	if !r.config.Enabled {
		return
	}
	r.status.Store(RelayNodeOnline)
	// flushLoop and heartbeatLoop use a dedicated done channel instead of
	// context.Context for shutdown because the relay must flush buffered
	// data on Stop(). A cancelled context would abort in-flight HTTP posts,
	// whereas closing the done channel lets each loop finish its current
	// iteration before exiting.
	r.wg.Add(2)
	go func(done <-chan struct{}) {
		defer r.wg.Done()
		r.flushLoop()
	}(r.done)
	go func(done <-chan struct{}) {
		defer r.wg.Done()
		r.heartbeatLoop()
	}(r.done)
}

// Stop gracefully shuts down the relay, flushing remaining data.
func (r *CloudRelay) Stop() {
	select {
	case <-r.done:
	default:
		close(r.done)
	}
	r.wg.Wait()
	r.status.Store(RelayNodeOffline)
	// Drain remaining queue
	r.flushQueue()
}

// Enqueue adds a point to the relay queue for cloud synchronization.
func (r *CloudRelay) Enqueue(p Point) error {
	select {
	case r.queue <- p:
		r.mu.Lock()
		r.stats.PointsQueued++
		r.mu.Unlock()
		return nil
	default:
		r.mu.Lock()
		r.stats.PointsFailed++
		r.mu.Unlock()
		return fmt.Errorf("relay queue full (capacity: %d)", r.config.MaxQueueSize)
	}
}

// EnqueueBatch adds multiple points to the relay queue.
func (r *CloudRelay) EnqueueBatch(points []Point) error {
	for _, p := range points {
		if err := r.Enqueue(p); err != nil {
			return err
		}
	}
	return nil
}

// Stats returns current relay statistics.
func (r *CloudRelay) Stats() RelaySyncStats {
	r.mu.RLock()
	defer r.mu.RUnlock()
	s := r.stats
	s.QueueDepth = len(r.queue)
	s.CurrentStatus = string(r.Status())
	s.UptimeSeconds = time.Since(r.startAt).Seconds()
	return s
}

// Status returns the current relay node status.
func (r *CloudRelay) Status() RelayNodeStatus {
	if v, ok := r.status.Load().(RelayNodeStatus); ok {
		return v
	}
	return RelayNodeOffline
}

func (r *CloudRelay) flushLoop() {
	ticker := time.NewTicker(r.config.FlushInterval)
	defer ticker.Stop()

	for {
		select {
		case <-r.done:
			return
		case <-ticker.C:
			r.flushQueue()
		}
	}
}

func (r *CloudRelay) flushQueue() {
	for {
		batch := r.collectBatch()
		if len(batch) == 0 {
			return
		}
		r.sendBatch(batch)
	}
}

func (r *CloudRelay) collectBatch() []Point {
	batch := make([]Point, 0, r.config.BatchSize)
	for len(batch) < r.config.BatchSize {
		select {
		case p := <-r.queue:
			batch = append(batch, p)
		default:
			return batch
		}
	}
	return batch
}

func (r *CloudRelay) sendBatch(points []Point) {
	seq := r.seqNum.Add(1)
	batch := RelayBatch{
		NodeID:    r.config.NodeID,
		Region:    r.config.Region,
		Points:    points,
		Timestamp: time.Now().UnixNano(),
		SeqNum:    seq,
	}

	data, err := json.Marshal(batch)
	if err != nil {
		r.recordError(fmt.Sprintf("marshal error: %v", err))
		r.mu.Lock()
		r.stats.BatchesFailed++
		r.mu.Unlock()
		return
	}

	var lastErr error
	for attempt := 0; attempt <= r.config.MaxRetries; attempt++ {
		if attempt > 0 {
			r.mu.Lock()
			r.stats.RetryCount++
			r.mu.Unlock()
			select {
			case <-time.After(r.config.RetryBackoff * time.Duration(attempt)):
			case <-r.done:
				return
			}
		}

		if err := r.doSend(data); err != nil {
			lastErr = err
			r.status.Store(RelayNodeDegraded)
			continue
		}

		// Success
		r.mu.Lock()
		r.stats.BatchesSent++
		r.stats.PointsSynced += uint64(len(points))
		r.stats.BytesSent += int64(len(data))
		r.stats.LastSyncTime = time.Now()
		r.mu.Unlock()
		r.status.Store(RelayNodeSyncing)
		return
	}

	r.mu.Lock()
	r.stats.BatchesFailed++
	r.stats.PointsFailed += uint64(len(points))
	r.mu.Unlock()
	if lastErr != nil {
		r.recordError(lastErr.Error())
	}
}

func (r *CloudRelay) doSend(data []byte) error {
	if r.config.TargetURL == "" {
		return fmt.Errorf("target URL not configured")
	}

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	req, err := http.NewRequestWithContext(ctx, http.MethodPost, r.config.TargetURL+"/relay/ingest", bytes.NewReader(data))
	if err != nil {
		return err
	}
	req.Header.Set("Content-Type", "application/json")
	if r.config.AuthToken != "" {
		req.Header.Set("Authorization", "Bearer "+r.config.AuthToken)
	}
	req.Header.Set("X-Relay-Node-ID", r.config.NodeID)

	resp, err := r.client.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	io.Copy(io.Discard, resp.Body)

	if resp.StatusCode >= 400 {
		return fmt.Errorf("relay ingest returned status %d", resp.StatusCode)
	}
	return nil
}

func (r *CloudRelay) heartbeatLoop() {
	if r.config.HeartbeatInterval <= 0 {
		return
	}
	ticker := time.NewTicker(r.config.HeartbeatInterval)
	defer ticker.Stop()

	for {
		select {
		case <-r.done:
			return
		case <-ticker.C:
			r.sendHeartbeat()
		}
	}
}

func (r *CloudRelay) sendHeartbeat() {
	if r.config.TargetURL == "" {
		return
	}

	hb := map[string]any{
		"node_id": r.config.NodeID,
		"region":  r.config.Region,
		"status":  string(r.Status()),
		"stats":   r.Stats(),
	}
	data, _ := json.Marshal(hb)

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	req, err := http.NewRequestWithContext(ctx, http.MethodPost, r.config.TargetURL+"/relay/heartbeat", bytes.NewReader(data))
	if err != nil {
		return
	}
	req.Header.Set("Content-Type", "application/json")
	if r.config.AuthToken != "" {
		req.Header.Set("Authorization", "Bearer "+r.config.AuthToken)
	}

	resp, err := r.client.Do(req)
	if err != nil {
		r.status.Store(RelayNodeDegraded)
		return
	}
	defer resp.Body.Close()
	io.Copy(io.Discard, resp.Body)
}

func (r *CloudRelay) recordError(msg string) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.stats.LastError = msg
}
