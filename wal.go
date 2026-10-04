package chronicle

// wal.go implements the Write-Ahead Log for crash recovery.
//
// The WAL ensures durability by writing all points to a sequential log
// before they are applied to partitions. On crash recovery, the WAL is
// replayed to restore any points that were written but not yet flushed.
//
// Key behaviors:
//   - Background syncLoop batches fsync calls (configurable interval)
//   - WAL rotation occurs when file size exceeds WALMaxSize
//   - Old WAL segments are retained per WALRetain config
//   - Recovery replays all segments in order

import (
	"bufio"
	"encoding/binary"
	"errors"
	"fmt"
	"hash/crc32"
	"io"
	"log/slog"
	"os"
	"path/filepath"
	"sort"
	"sync"
	"time"
)

// walMaxEntrySize is the maximum size for a single WAL entry (64MB).
// Entries larger than this are rejected to prevent OOM from corrupted length fields.
const walMaxEntrySize = 64 * 1024 * 1024

// WAL is a write-ahead log for crash recovery.
type WAL struct {
	path         string
	file         *os.File
	mu           sync.Mutex
	writer       *bufio.Writer
	syncInterval time.Duration
	closeCh      chan struct{}
	wg           sync.WaitGroup
	maxSize      int64
	retain       int

	// syncErrors tracks consecutive sync errors for monitoring.
	// After walMaxConsecutiveSyncErrors failures, writes are rejected
	// with ErrWALSync to prevent silent data loss.
	syncErrors  int
	syncFailed  bool         // true when consecutive errors exceed threshold
	onSyncError func(error)  // optional callback for sync errors
}

// walMaxConsecutiveSyncErrors is the number of consecutive sync failures
// before the WAL starts rejecting writes. This prevents silent data loss
// when the underlying storage is permanently broken.
const walMaxConsecutiveSyncErrors = 5

// WALOption configures a WAL instance.
type WALOption func(*WAL)

// WithSyncErrorCallback sets a callback for sync errors.
func WithSyncErrorCallback(fn func(error)) WALOption {
	return func(w *WAL) {
		w.onSyncError = fn
	}
}

// NewWAL creates or opens a WAL file.
func NewWAL(path string, syncInterval time.Duration, maxSize int64, retain int, opts ...WALOption) (*WAL, error) {
	file, err := os.OpenFile(path, os.O_RDWR|os.O_CREATE|os.O_APPEND, 0o644)
	if err != nil {
		return nil, err
	}
	wal := &WAL{
		path:         path,
		file:         file,
		writer:       bufio.NewWriter(file),
		syncInterval: syncInterval,
		closeCh:      make(chan struct{}),
		maxSize:      maxSize,
		retain:       retain,
	}

	for _, opt := range opts {
		opt(wal)
	}

	wal.wg.Add(1)
	// syncLoop periodically flushes buffered WAL entries to disk.
	go wal.syncLoop()

	return wal, nil
}

// Close closes the WAL.
func (w *WAL) Close() error {
	close(w.closeCh)
	w.wg.Wait()
	w.mu.Lock()
	defer w.mu.Unlock()
	if err := w.writer.Flush(); err != nil {
		slog.Error("WAL close: flush failed", "err", err)
		closeQuietly(w.file)
		return err
	}
	if err := w.file.Sync(); err != nil {
		slog.Error("WAL close: sync failed", "err", err)
		closeQuietly(w.file)
		return err
	}
	return w.file.Close()
}

// Write appends points to the WAL.
func (w *WAL) Write(points []Point) error {
	if len(points) == 0 {
		return nil
	}

	w.mu.Lock()
	defer w.mu.Unlock()

	if w.syncFailed {
		return ErrWALSync
	}

	if err := w.rotateIfNeeded(); err != nil {
		return err
	}

	payload, err := encodePoints(points)
	if err != nil {
		return err
	}

	// WAL entry format: [4-byte length][4-byte CRC32][payload]
	if err := binary.Write(w.writer, binary.LittleEndian, uint32(len(payload))); err != nil {
		return err
	}
	checksum := crc32.ChecksumIEEE(payload)
	if err := binary.Write(w.writer, binary.LittleEndian, checksum); err != nil {
		return err
	}
	if _, err := w.writer.Write(payload); err != nil {
		return err
	}

	// Don't flush here on every write — the background syncLoop handles
	// periodic flush + fsync to batch WAL I/O for better throughput.
	// Data is still in the bufio.Writer and will be persisted on the next
	// sync tick or on Close().
	return nil
}

// ReadAll reads all points from the WAL.
func (w *WAL) ReadAll() ([]Point, error) {
	w.mu.Lock()
	defer w.mu.Unlock()

	if _, err := w.file.Seek(0, io.SeekStart); err != nil {
		return nil, err
	}

	reader := bufio.NewReader(w.file)
	var out []Point

	for {
		var length uint32
		if err := binary.Read(reader, binary.LittleEndian, &length); err != nil {
			if errors.Is(err, io.EOF) {
				break
			}
			return nil, err
		}
		if length == 0 {
			continue
		}
		if length > walMaxEntrySize {
			slog.Warn("WAL: skipping oversized entry", "length", length, "max", walMaxEntrySize)
			break // corrupted length field — stop reading
		}

		var storedChecksum uint32
		if err := binary.Read(reader, binary.LittleEndian, &storedChecksum); err != nil {
			if errors.Is(err, io.EOF) {
				break // truncated entry at EOF
			}
			return nil, err
		}

		payload := make([]byte, length)
		if _, err := io.ReadFull(reader, payload); err != nil {
			return nil, err
		}

		// Validate checksum
		if crc32.ChecksumIEEE(payload) != storedChecksum {
			slog.Warn("WAL: checksum mismatch, skipping corrupted entry")
			continue // skip corrupted entry, try to read next
		}

		points, err := decodePoints(payload)
		if err != nil {
			return nil, err
		}
		out = append(out, points...)
	}

	if _, err := w.file.Seek(0, io.SeekEnd); err != nil {
		return nil, err
	}

	return out, nil
}

// Reset truncates the WAL after recovery.
func (w *WAL) Reset() error {
	w.mu.Lock()
	defer w.mu.Unlock()

	if err := truncateWALFile(w.file); err != nil {
		return err
	}
	if _, err := w.file.Seek(0, io.SeekStart); err != nil {
		return err
	}
	w.writer = bufio.NewWriter(w.file)
	return nil
}

func (w *WAL) rotateIfNeeded() error {
	if w.maxSize <= 0 {
		return nil
	}
	info, err := w.file.Stat()
	if err != nil {
		return err
	}
	if info.Size() < w.maxSize {
		return nil
	}

	if err := w.writer.Flush(); err != nil {
		return err
	}
	if err := w.file.Sync(); err != nil {
		return err
	}
	if err := w.file.Close(); err != nil {
		return err
	}

	rotated := w.path + "." + time.Now().Format("20060102T150405")
	if err := os.Rename(w.path, rotated); err != nil {
		// Reopen the original path since file is closed
		if f, openErr := os.OpenFile(w.path, os.O_RDWR|os.O_CREATE|os.O_APPEND, 0o644); openErr == nil {
			w.file = f
			w.writer = bufio.NewWriter(f)
		}
		return err
	}

	file, err := os.OpenFile(w.path, os.O_RDWR|os.O_CREATE|os.O_APPEND, 0o644)
	if err != nil {
		// Rename succeeded but new file creation failed. Try to rename back
		// to restore a usable state.
		if renameErr := os.Rename(rotated, w.path); renameErr == nil {
			if f, openErr := os.OpenFile(w.path, os.O_RDWR|os.O_APPEND, 0o644); openErr == nil {
				w.file = f
				w.writer = bufio.NewWriter(f)
				return fmt.Errorf("WAL rotation: new file creation failed (restored original): %w", err)
			}
		}
		return fmt.Errorf("WAL rotation: new file creation failed (state unrecoverable): %w", err)
	}
	w.file = file
	w.writer = bufio.NewWriter(file)

	if w.retain > 0 {
		if err := w.cleanupRotations(); err != nil {
			slog.Warn("WAL: failed to cleanup old rotations", "err", err)
		}
	}
	return nil
}

func (w *WAL) cleanupRotations() error {
	files, err := filepath.Glob(w.path + ".*")
	if err != nil {
		return err
	}
	if len(files) <= w.retain {
		return nil
	}
	sort.Strings(files)
	excess := len(files) - w.retain
	for i := 0; i < excess; i++ {
		if err := os.Remove(files[i]); err != nil {
			slog.Warn("WAL: failed to remove old segment", "file", files[i], "err", err)
		}
	}
	return nil
}

func (w *WAL) syncLoop() {
	defer w.wg.Done()
	if w.syncInterval <= 0 {
		return
	}

	ticker := time.NewTicker(w.syncInterval)
	defer ticker.Stop()

	for {
		select {
		case <-w.closeCh:
			return
		case <-ticker.C:
			w.mu.Lock()
			var syncErr *WALSyncError
			flushErr := w.writer.Flush()
			fileErr := w.file.Sync()

			if flushErr != nil || fileErr != nil {
				syncErr = &WALSyncError{FlushErr: flushErr, SyncErr: fileErr}
				w.syncErrors++

				if w.syncErrors >= walMaxConsecutiveSyncErrors && !w.syncFailed {
					w.syncFailed = true
					slog.Error("WAL sync circuit breaker OPEN: rejecting new writes",
						"consecutive_errors", w.syncErrors)
				}

				// Log the error
				slog.Error("WAL sync error", "count", w.syncErrors, "err", syncErr)

				// Call error callback if set
				if w.onSyncError != nil {
					w.onSyncError(syncErr)
				}
			} else {
				// Reset error counter and circuit breaker on success
				if w.syncFailed {
					slog.Info("WAL sync recovered, accepting writes again")
				}
				w.syncErrors = 0
				w.syncFailed = false
			}
			w.mu.Unlock()
		}
	}
}

// Position returns the current WAL position (file size).
func (w *WAL) Position() int64 {
	w.mu.Lock()
	defer w.mu.Unlock()

	// Flush to get accurate position
	if err := w.writer.Flush(); err != nil {
		slog.Warn("WAL: flush failed in Position", "err", err)
	}

	info, err := w.file.Stat()
	if err != nil {
		return 0
	}
	return info.Size()
}

// ReadRange reads WAL entries between two positions.
func (w *WAL) ReadRange(start, end int64) ([]byte, error) {
	w.mu.Lock()
	defer w.mu.Unlock()

	if start < 0 || end < start {
		return nil, errors.New("invalid range")
	}

	// Flush pending writes
	if err := w.writer.Flush(); err != nil {
		slog.Warn("WAL: flush failed in ReadRange", "err", err)
	}

	// Read the range
	size := end - start
	if size == 0 {
		return nil, nil
	}

	if _, err := w.file.Seek(start, io.SeekStart); err != nil {
		return nil, err
	}

	data := make([]byte, size)
	n, err := io.ReadFull(w.file, data)
	if err != nil && !errors.Is(err, io.EOF) && !errors.Is(err, io.ErrUnexpectedEOF) {
		return nil, err
	}

	// Seek back to end for writing
	if _, err := w.file.Seek(0, io.SeekEnd); err != nil {
		return nil, err
	}

	return data[:n], nil
}

// ApplyEntries replays WAL entries from encoded data.
func (w *WAL) ApplyEntries(data []byte) error {
	if len(data) == 0 {
		return nil
	}

	w.mu.Lock()
	defer w.mu.Unlock()

	// Append the data to the WAL
	if _, err := w.writer.Write(data); err != nil {
		return err
	}

	return w.writer.Flush()
}
