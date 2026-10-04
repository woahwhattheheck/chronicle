package raft

import (
	"encoding/gob"
	"hash/crc32"
	"io"
	"os"
	"path/filepath"
)

// NewRaftLog creates a new Raft log.
func NewRaftLog(path string) (*RaftLog, error) {
	rl := &RaftLog{
		entries:    make([]*RaftLogEntry, 0),
		startIndex: 1,
	}

	if path != "" {
		dir := filepath.Dir(path)
		if err := os.MkdirAll(dir, 0755); err != nil {
			return nil, err
		}

		file, err := os.OpenFile(path, os.O_RDWR|os.O_CREATE|os.O_APPEND, 0644)
		if err != nil {
			return nil, err
		}
		rl.file = file
		rl.encoder = gob.NewEncoder(file)

		if err := rl.load(); err != nil {
			_ = file.Close()
			return nil, err
		}
	}

	return rl, nil
}

func (rl *RaftLog) load() error {
	if rl.file == nil {
		return nil
	}

	if _, err := rl.file.Seek(0, 0); err != nil {
		return err
	}

	decoder := gob.NewDecoder(rl.file)
	for {
		var entry RaftLogEntry
		if err := decoder.Decode(&entry); err != nil {
			if err == io.EOF {
				break
			}
			return err
		}
		if entry.Validate() {
			rl.entries = append(rl.entries, &entry)
		}
	}

	if len(rl.entries) > 0 {
		rl.startIndex = rl.entries[0].Index
	}

	return nil
}

// Append adds entries to the log.
func (rl *RaftLog) Append(entries ...*RaftLogEntry) error {
	rl.mu.Lock()
	defer rl.mu.Unlock()

	for _, entry := range entries {
		entry.CRC = crc32.ChecksumIEEE(entry.Data)
		rl.entries = append(rl.entries, entry)

		if rl.encoder != nil {
			if err := rl.encoder.Encode(entry); err != nil {
				return err
			}
		}
	}

	if rl.file != nil {
		return rl.file.Sync()
	}
	return nil
}

// Get returns the entry at the given index.
func (rl *RaftLog) Get(index uint64) *RaftLogEntry {
	rl.mu.RLock()
	defer rl.mu.RUnlock()

	if index < rl.startIndex || index > rl.LastIndex() {
		return nil
	}

	arrayIndex := index - rl.startIndex
	if int(arrayIndex) >= len(rl.entries) {
		return nil
	}
	return rl.entries[arrayIndex]
}

// GetRange returns entries from startIdx to endIdx (inclusive).
func (rl *RaftLog) GetRange(startIdx, endIdx uint64) []*RaftLogEntry {
	rl.mu.RLock()
	defer rl.mu.RUnlock()

	if startIdx < rl.startIndex {
		startIdx = rl.startIndex
	}
	if endIdx > rl.LastIndex() {
		endIdx = rl.LastIndex()
	}
	if startIdx > endIdx {
		return nil
	}

	start := startIdx - rl.startIndex
	end := endIdx - rl.startIndex + 1
	if int(end) > len(rl.entries) {
		end = uint64(len(rl.entries))
	}

	result := make([]*RaftLogEntry, end-start)
	copy(result, rl.entries[start:end])
	return result
}

// LastIndex returns the last log index.
func (rl *RaftLog) LastIndex() uint64 {
	rl.mu.RLock()
	defer rl.mu.RUnlock()
	if len(rl.entries) == 0 {
		return rl.startIndex - 1
	}
	return rl.entries[len(rl.entries)-1].Index
}

// LastTerm returns the term of the last entry.
func (rl *RaftLog) LastTerm() uint64 {
	rl.mu.RLock()
	defer rl.mu.RUnlock()

	if len(rl.entries) == 0 {
		return 0
	}
	return rl.entries[len(rl.entries)-1].Term
}

// TermAt returns the term at the given index.
func (rl *RaftLog) TermAt(index uint64) uint64 {
	entry := rl.Get(index)
	if entry == nil {
		return 0
	}
	return entry.Term
}

// TruncateAfter removes all entries after the given index.
func (rl *RaftLog) TruncateAfter(index uint64) {
	rl.mu.Lock()
	defer rl.mu.Unlock()

	if index < rl.startIndex {
		rl.entries = rl.entries[:0]
		return
	}

	keepCount := index - rl.startIndex + 1
	if uint64(len(rl.entries)) > keepCount {
		rl.entries = rl.entries[:keepCount]
	}
}

// CompactBefore removes entries before the given index.
func (rl *RaftLog) CompactBefore(index uint64) {
	rl.mu.Lock()
	defer rl.mu.Unlock()

	if index <= rl.startIndex {
		return
	}

	removeCount := index - rl.startIndex
	if removeCount >= uint64(len(rl.entries)) {
		rl.entries = rl.entries[:0]
		rl.startIndex = index
		return
	}

	rl.entries = rl.entries[removeCount:]
	rl.startIndex = index
}

// Len returns the number of entries.
func (rl *RaftLog) Len() int {
	rl.mu.RLock()
	defer rl.mu.RUnlock()
	return len(rl.entries)
}

// Close closes the log file.
func (rl *RaftLog) Close() error {
	if rl.file != nil {
		return rl.file.Close()
	}
	return nil
}
