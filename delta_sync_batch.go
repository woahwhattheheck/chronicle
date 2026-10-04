package chronicle

import (
	"bytes"
	"compress/gzip"
	"context"
	"crypto/sha256"
	"encoding/binary"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"os"
	"path/filepath"
	"sort"
	"sync/atomic"
	"time"
)

// Batch splitting, delta encoding, conflict resolution, queue management, and checkpoint logic for delta sync.

var deltaSyncBatchIDSequence atomic.Int64

func nextDeltaSyncBatchID(deviceID string) string {
	for {
		previous := deltaSyncBatchIDSequence.Load()
		next := time.Now().UnixNano()
		if next <= previous {
			next = previous + 1
		}
		if deltaSyncBatchIDSequence.CompareAndSwap(previous, next) {
			return fmt.Sprintf("%s-%d", deviceID, next)
		}
	}
}

func (m *DeltaSyncManager) splitIntoBatches(points []Point) []DeltaBatch {
	var batches []DeltaBatch

	for i := 0; i < len(points); i += m.config.BatchSize {
		end := i + m.config.BatchSize
		if end > len(points) {
			end = len(points)
		}

		batchPoints := points[i:end]

		// Increment vector clock
		m.vectorClock.Increment(m.config.DeviceID)

		batch := DeltaBatch{
			ID:          nextDeltaSyncBatchID(m.config.DeviceID),
			DeviceID:    m.config.DeviceID,
			Timestamp:   time.Now().UnixNano(),
			Points:      batchPoints,
			VectorClock: m.vectorClock.clocks,
		}

		// Apply delta encoding if enabled
		if m.config.DeltaCompressionEnabled {
			batch.Points = m.deltaEncode(batchPoints)
			batch.DeltaEncoded = true
		}

		// Calculate checksum
		if m.config.EnableChecksum {
			checksum, err := m.calculateBatchChecksum(batch)
			if err != nil {
				return nil
			}
			batch.Checksum = checksum
		}

		batches = append(batches, batch)
	}

	return batches
}

func (m *DeltaSyncManager) deltaEncode(points []Point) []Point {
	if len(points) == 0 {
		return points
	}

	// Sort by timestamp
	sort.Slice(points, func(i, j int) bool {
		return points[i].Timestamp < points[j].Timestamp
	})

	// Delta encode - store differences from previous point
	encoded := make([]Point, len(points))
	encoded[0] = points[0] // First point is stored as-is

	for i := 1; i < len(points); i++ {
		encoded[i] = Point{
			Metric:    points[i].Metric,
			Value:     points[i].Value - points[i-1].Value,         // Delta value
			Timestamp: points[i].Timestamp - points[i-1].Timestamp, // Delta timestamp
			Tags:      points[i].Tags,
		}
	}

	return encoded
}

func (m *DeltaSyncManager) deltaDecode(points []Point) []Point {
	if len(points) == 0 {
		return points
	}

	decoded := make([]Point, len(points))
	decoded[0] = points[0]

	for i := 1; i < len(points); i++ {
		decoded[i] = Point{
			Metric:    points[i].Metric,
			Value:     decoded[i-1].Value + points[i].Value,
			Timestamp: decoded[i-1].Timestamp + points[i].Timestamp,
			Tags:      points[i].Tags,
		}
	}

	return decoded
}

func (m *DeltaSyncManager) calculateBatchChecksum(batch DeltaBatch) (string, error) {
	data, err := json.Marshal(batch.Points)
	if err != nil {
		return "", fmt.Errorf("failed to marshal batch for checksum: %w", err)
	}
	hash := sha256.Sum256(data)
	return hex.EncodeToString(hash[:]), nil
}

func (m *DeltaSyncManager) sendBatch(batch DeltaBatch) error {
	// Prepare payload
	payload, err := json.Marshal(batch)
	if err != nil {
		return err
	}

	originalSize := len(payload)

	// Compress if enabled
	if m.config.CompressPayload {
		var buf bytes.Buffer
		gw := gzip.NewWriter(&buf)
		gw.Write(payload)
		gw.Close()
		payload = buf.Bytes()

		// Update compression stats
		m.statsMu.Lock()
		m.stats.BytesSaved += int64(originalSize - len(payload))
		if originalSize > 0 {
			m.stats.CompressionRatio = float64(len(payload)) / float64(originalSize)
		}
		m.statsMu.Unlock()
	}

	// Send with retry
	var lastErr error
	backoff := m.config.RetryBackoff

	for attempt := 0; attempt < m.config.MaxRetries; attempt++ {
		ctx, cancel := context.WithTimeout(m.ctx, 30*time.Second)
		req, err := http.NewRequestWithContext(ctx, http.MethodPost,
			m.config.CloudEndpoint+"/api/v1/delta/push", bytes.NewReader(payload))

		if err != nil {
			cancel()
			lastErr = err
			continue
		}

		req.Header.Set("Content-Type", "application/json")
		if m.config.CompressPayload {
			req.Header.Set("Content-Encoding", "gzip")
		}
		m.addAuthHeaders(req)
		req.Header.Set("X-Device-ID", m.config.DeviceID)
		req.Header.Set("X-Batch-ID", batch.ID)

		resp, err := m.client.Do(req)
		if err != nil {
			cancel()
			lastErr = err
			select {
			case <-time.After(backoff):
			case <-m.ctx.Done():
				return m.ctx.Err()
			}
			backoff = backoff * 2
			if backoff > m.config.MaxRetryBackoff {
				backoff = m.config.MaxRetryBackoff
			}
			continue
		}
		resp.Body.Close()
		cancel()

		if resp.StatusCode == http.StatusOK || resp.StatusCode == http.StatusCreated {
			// Success - update checkpoint
			m.updateCheckpoint(batch)

			m.statsMu.Lock()
			m.stats.PointsSynced += int64(len(batch.Points))
			m.stats.BytesSynced += int64(len(payload))
			m.statsMu.Unlock()

			return nil
		}

		if resp.StatusCode == http.StatusConflict {
			// Conflict - need resolution
			return m.handleConflict(batch, resp)
		}

		lastErr = fmt.Errorf("server returned %d", resp.StatusCode)
		select {
		case <-time.After(backoff):
		case <-m.ctx.Done():
			return m.ctx.Err()
		}
		backoff = backoff * 2
		if backoff > m.config.MaxRetryBackoff {
			backoff = m.config.MaxRetryBackoff
		}
	}

	return fmt.Errorf("failed after %d attempts: %w", m.config.MaxRetries, lastErr)
}

func (m *DeltaSyncManager) handleConflict(batch DeltaBatch, resp *http.Response) error {
	m.statsMu.Lock()
	m.stats.ConflictsResolved++
	m.statsMu.Unlock()

	switch m.config.ConflictStrategy {
	case DeltaConflictCloudWins:
		// Discard local changes, pull cloud version
		return m.pullFromCloud()

	case DeltaConflictEdgeWins:
		// Force push local changes
		return m.forcePush(batch)

	case DeltaConflictMerge:
		// Merge both versions
		return m.mergeChanges(batch)

	case DeltaConflictVectorClock:
		// Use vector clock comparison
		return m.resolveWithVectorClock(batch)

	default: // LastWriteWins
		// Compare timestamps and keep latest
		return m.lastWriteWinsResolve(batch)
	}
}

func (m *DeltaSyncManager) forcePush(batch DeltaBatch) error {
	// Add force flag
	ctx, cancel := context.WithTimeout(m.ctx, 30*time.Second)
	defer cancel()

	payload, _ := json.Marshal(batch)

	req, err := http.NewRequestWithContext(ctx, http.MethodPost,
		m.config.CloudEndpoint+"/api/v1/delta/push?force=true", bytes.NewReader(payload))
	if err != nil {
		return err
	}

	req.Header.Set("Content-Type", "application/json")
	m.addAuthHeaders(req)

	resp, err := m.client.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()

	if resp.StatusCode >= 400 {
		return fmt.Errorf("force push failed: %d", resp.StatusCode)
	}

	return nil
}

func (m *DeltaSyncManager) mergeChanges(batch DeltaBatch) error {
	// Pull cloud version
	if err := m.pullFromCloud(); err != nil {
		return err
	}

	// Re-push local changes (they'll be merged at cloud)
	return m.sendBatch(batch)
}

func (m *DeltaSyncManager) resolveWithVectorClock(batch DeltaBatch) error {
	// The cloud should have sent its vector clock in the response
	// Compare and decide which version is newer
	cloudClock := NewVectorClock()
	cloudClock.clocks = batch.VectorClock

	cmp := m.vectorClock.Compare(cloudClock)

	if cmp > 0 {
		// Local is newer, force push
		return m.forcePush(batch)
	} else if cmp < 0 {
		// Cloud is newer, pull
		return m.pullFromCloud()
	}

	// Concurrent - merge
	return m.mergeChanges(batch)
}

func (m *DeltaSyncManager) lastWriteWinsResolve(batch DeltaBatch) error {
	// Simply retry with latest timestamp
	batch.Timestamp = time.Now().UnixNano()
	return m.sendBatch(batch)
}

func (m *DeltaSyncManager) applyBatch(batch DeltaBatch) error {
	points := batch.Points

	// Decode if delta encoded
	if batch.DeltaEncoded {
		points = m.deltaDecode(points)
	}

	// Verify checksum
	if m.config.EnableChecksum && batch.Checksum != "" {
		expected, err := m.calculateBatchChecksum(DeltaBatch{Points: batch.Points})
		if err != nil {
			return fmt.Errorf("checksum calculation failed: %w", err)
		}
		if expected != batch.Checksum {
			return errors.New("checksum mismatch")
		}
	}

	// Write to database
	if err := m.db.WriteBatch(points); err != nil {
		return err
	}

	// Merge vector clock
	cloudClock := &VectorClock{clocks: batch.VectorClock}
	m.vectorClock.Merge(cloudClock)

	return nil
}

func (m *DeltaSyncManager) queueBatch(batch DeltaBatch) {
	if m.queue == nil {
		return
	}

	m.queue.mu.Lock()
	defer m.queue.mu.Unlock()

	batchSize := int64(len(batch.Points) * 100) // Rough estimate

	// Check capacity
	if m.queue.sizeBytes+batchSize > m.queue.maxBytes {
		// Remove oldest batches
		for m.queue.sizeBytes+batchSize > m.queue.maxBytes && len(m.queue.items) > 0 {
			removed := m.queue.items[0]
			m.queue.items = m.queue.items[1:]
			m.queue.sizeBytes -= int64(len(removed.Points) * 100)
		}
	}

	m.queue.items = append(m.queue.items, batch)
	m.queue.sizeBytes += batchSize
}

func (m *DeltaSyncManager) syncOfflineQueue() {
	if m.queue == nil {
		return
	}

	m.queue.mu.Lock()
	items := m.queue.items
	m.queue.items = nil
	m.queue.sizeBytes = 0
	m.queue.mu.Unlock()

	for i, batch := range items {
		if err := m.sendBatch(batch); err != nil {
			// Keep the failed batch and every batch not yet attempted.
			for _, pending := range items[i:] {
				m.queueBatch(pending)
			}
			return
		}
	}
}

func (m *DeltaSyncManager) updateCheckpoint(batch DeltaBatch) {
	m.checkpoint.mu.Lock()
	defer m.checkpoint.mu.Unlock()

	// Update last synced timestamp
	maxTs := m.checkpoint.LastSyncedTimestamp
	for _, pt := range batch.Points {
		if pt.Timestamp > maxTs {
			maxTs = pt.Timestamp
		}
	}
	m.checkpoint.LastSyncedTimestamp = maxTs

	// Update vector clock
	m.checkpoint.VectorClock = m.vectorClock.clocks

	// Save checkpoint
	go m.saveCheckpoint()
}

func (m *DeltaSyncManager) loadCheckpoint() {
	if m.config.OfflineQueuePath == "" {
		return
	}

	path := filepath.Join(filepath.Dir(m.config.OfflineQueuePath), "delta_checkpoint.json")
	data, err := os.ReadFile(path)
	if err != nil {
		return
	}

	var cp DeltaCheckpoint
	if err := json.Unmarshal(data, &cp); err != nil {
		return
	}

	m.checkpoint = &cp
	if cp.VectorClock != nil {
		m.vectorClock.clocks = cp.VectorClock
	}
}

func (m *DeltaSyncManager) saveCheckpoint() {
	if m.config.OfflineQueuePath == "" {
		return
	}

	m.checkpoint.mu.RLock()
	data, err := json.Marshal(m.checkpoint)
	m.checkpoint.mu.RUnlock()

	if err != nil {
		return
	}

	path := filepath.Join(filepath.Dir(m.config.OfflineQueuePath), "delta_checkpoint.json")
	os.WriteFile(path, data, 0644)
}

func (m *DeltaSyncManager) loadQueue() {
	if m.queue == nil {
		return
	}

	data, err := os.ReadFile(m.queue.path)
	if err != nil {
		return
	}

	var items []DeltaBatch
	if err := json.Unmarshal(data, &items); err != nil {
		return
	}

	m.queue.items = items
	for _, item := range items {
		m.queue.sizeBytes += int64(len(item.Points) * 100)
	}
}

func (m *DeltaSyncManager) saveQueue() {
	if m.queue == nil {
		return
	}

	m.queue.mu.Lock()
	data, err := json.Marshal(m.queue.items)
	m.queue.mu.Unlock()

	if err != nil {
		return
	}

	os.WriteFile(m.queue.path, data, 0644)
}

func (m *DeltaSyncManager) addAuthHeaders(req *http.Request) {
	if m.config.Auth == nil {
		return
	}

	switch m.config.Auth.Type {
	case "api_key":
		req.Header.Set("X-API-Key", m.config.Auth.APIKey)
	case "bearer":
		req.Header.Set("Authorization", "Bearer "+m.config.Auth.BearerToken)
	case "basic":
		req.SetBasicAuth(m.config.Auth.Username, m.config.Auth.Password)
	}
}

func (m *DeltaSyncManager) updateAverageLatency(elapsed time.Duration) {
	m.statsMu.Lock()
	defer m.statsMu.Unlock()

	attempts := m.stats.SyncAttempts
	if attempts == 0 {
		attempts = 1
	}

	// Exponential moving average
	alpha := 0.2
	m.stats.AverageSyncLatencyMs = m.stats.AverageSyncLatencyMs*(1-alpha) +
		float64(elapsed.Milliseconds())*alpha
}

// SyncNow triggers an immediate sync.
func (m *DeltaSyncManager) SyncNow() error {
	m.performSync()
	return nil
}

// GetStats returns sync statistics.
func (m *DeltaSyncManager) GetStats() DeltaSyncStats {
	m.statsMu.RLock()
	defer m.statsMu.RUnlock()

	stats := m.stats
	if m.queue != nil {
		m.queue.mu.Lock()
		stats.QueuedPoints = int64(len(m.queue.items))
		stats.QueuedBytes = m.queue.sizeBytes
		m.queue.mu.Unlock()
	}
	return stats
}

// GetVectorClock returns the current vector clock state.
func (m *DeltaSyncManager) GetVectorClock() map[string]uint64 {
	return m.vectorClock.clocks
}

// DeltaPullResponse is the response from cloud pull.
type DeltaPullResponse struct {
	Batches     []DeltaBatch      `json:"batches"`
	VectorClock map[string]uint64 `json:"vector_clock"`
	HasMore     bool              `json:"has_more"`
}

// --- Delta Encoding Utilities ---

// DeltaEncoder provides efficient delta encoding for time-series.
type DeltaEncoder struct {
	lastTimestamp int64
	lastValue     float64
	initialized   bool
}

func NewDeltaEncoder() *DeltaEncoder {
	return &DeltaEncoder{}
}

func (e *DeltaEncoder) Encode(timestamp int64, value float64) (int64, float64) {
	if !e.initialized {
		e.initialized = true
		e.lastTimestamp = timestamp
		e.lastValue = value
		return timestamp, value
	}

	deltaTs := timestamp - e.lastTimestamp
	deltaVal := value - e.lastValue

	e.lastTimestamp = timestamp
	e.lastValue = value

	return deltaTs, deltaVal
}

func (e *DeltaEncoder) Reset() {
	e.initialized = false
	e.lastTimestamp = 0
	e.lastValue = 0
}

// DeltaDecoder decodes delta-encoded values.
type DeltaDecoder struct {
	lastTimestamp int64
	lastValue     float64
	initialized   bool
}

func NewDeltaDecoder() *DeltaDecoder {
	return &DeltaDecoder{}
}

func (d *DeltaDecoder) Decode(deltaTs int64, deltaVal float64) (int64, float64) {
	if !d.initialized {
		d.initialized = true
		d.lastTimestamp = deltaTs
		d.lastValue = deltaVal
		return deltaTs, deltaVal
	}

	timestamp := d.lastTimestamp + deltaTs
	value := d.lastValue + deltaVal

	d.lastTimestamp = timestamp
	d.lastValue = value

	return timestamp, value
}

// VarintEncoder provides varint encoding for compressing integers.
type VarintEncoder struct {
	buf []byte
}

func NewVarintEncoder() *VarintEncoder {
	return &VarintEncoder{buf: make([]byte, binary.MaxVarintLen64)}
}

func (e *VarintEncoder) EncodeInt64(v int64) []byte {
	n := binary.PutVarint(e.buf, v)
	result := make([]byte, n)
	copy(result, e.buf[:n])
	return result
}

func (e *VarintEncoder) DecodeInt64(data []byte) (int64, int) {
	return binary.Varint(data)
}
