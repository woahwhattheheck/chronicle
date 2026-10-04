//go:build windows

package raft

import (
	"errors"
	"os"
	"path/filepath"
	"runtime/debug"
	"strings"
	"sync"
	"testing"
	"time"
)

// Failed constructors return no object to close. Disabling GC in this serial
// test prevents an unrelated finalizer from hiding a leaked Windows handle.
func TestRaftResourceLifecycle(t *testing.T) {
	previousGC := debug.SetGCPercent(-1)
	defer debug.SetGCPercent(previousGC)
	newConfig := func(t *testing.T) RaftConfig {
		config := DefaultRaftConfig()
		config.DataDir = t.TempDir()
		config.BindAddr = ""
		config.ElectionTimeoutMin = time.Hour
		config.ElectionTimeoutMax = 2 * time.Hour
		config.SnapshotInterval = 0
		config.BatchingEnabled = false
		config.CheckQuorumEnabled = false
		return config
	}

	t.Run("stop_before_start", func(t *testing.T) {
		config := newConfig(t)
		node, err := NewRaftNode(newTestStore(), config)
		if err != nil {
			t.Fatal(err)
		}
		defer node.log.Close()
		var wg sync.WaitGroup
		errorsCh := make(chan error, 8)
		for i := 0; i < 8; i++ {
			wg.Add(1)
			go func() {
				defer wg.Done()
				errorsCh <- node.Stop()
			}()
		}
		wg.Wait()
		close(errorsCh)
		for err := range errorsCh {
			if err != nil {
				t.Errorf("Stop must be idempotent: %v", err)
			}
		}
		if _, err := node.log.file.Stat(); !errors.Is(err, os.ErrClosed) {
			t.Errorf("Stop left the pre-start log open: %v", err)
		}
		if node.ctx.Err() == nil {
			t.Error("Stop did not cancel the pre-start node")
		}
		if err := node.Start(); err == nil {
			_ = node.Stop()
			t.Error("Start accepted a stopped node")
		}
	})

	t.Run("started_node", func(t *testing.T) {
		config := newConfig(t)
		node, err := NewRaftNode(newTestStore(), config)
		if err != nil {
			t.Fatal(err)
		}
		defer node.log.Close()
		if err := node.Start(); err != nil {
			t.Fatal(err)
		}
		if err := node.Stop(); err != nil {
			t.Fatal(err)
		}
		if err := node.Stop(); err != nil {
			t.Fatalf("repeated Stop: %v", err)
		}
		if _, err := node.log.file.Stat(); !errors.Is(err, os.ErrClosed) {
			t.Errorf("started node log remains open: %v", err)
		}
		if _, err := os.Stat(filepath.Join(config.DataDir, "raft.state")); err != nil {
			t.Errorf("normal Stop did not save state: %v", err)
		}
		if err := os.Remove(filepath.Join(config.DataDir, "raft.log")); err != nil {
			t.Errorf("normal Stop did not release Windows handle: %v", err)
		}
	})

	t.Run("invalid_state", func(t *testing.T) {
		config := newConfig(t)
		statePath := filepath.Join(config.DataDir, "raft.state")
		if err := os.WriteFile(statePath, []byte("{"), 0600); err != nil {
			t.Fatal(err)
		}
		if node, err := NewRaftNode(newTestStore(), config); err == nil || node != nil || !strings.Contains(err.Error(), "failed to load state") {
			t.Fatalf("expected original state-decode error: node=%v err=%v", node, err)
		}
		if err := os.Remove(filepath.Join(config.DataDir, "raft.log")); err != nil {
			t.Fatalf("failed constructor retained the log handle: %v", err)
		}
		if err := os.WriteFile(statePath, []byte(`{"current_term":7,"voted_for":"peer-7"}`), 0600); err != nil {
			t.Fatal(err)
		}
		node, err := NewRaftNode(newTestStore(), config)
		if err != nil {
			t.Fatalf("corrected state could not reopen: %v", err)
		}
		defer node.Stop()
		if node.currentTerm != 7 || node.votedFor != "peer-7" {
			t.Error("corrected persistent state changed")
		}
	})

	t.Run("invalid_log", func(t *testing.T) {
		path := filepath.Join(t.TempDir(), "raft.log")
		if err := os.WriteFile(path, []byte("not a gob record"), 0600); err != nil {
			t.Fatal(err)
		}
		if log, err := NewRaftLog(path); err == nil || log != nil {
			t.Fatalf("expected original log-decode error: log=%v err=%v", log, err)
		}
		if err := os.Remove(path); err != nil {
			t.Fatalf("failed log constructor retained its handle: %v", err)
		}
		log, err := NewRaftLog(path)
		if err != nil {
			t.Fatal(err)
		}
		if err := log.Append(&RaftLogEntry{Index: 1, Term: 1, Data: []byte("retained")}); err != nil {
			_ = log.Close()
			t.Fatal(err)
		}
		if err := log.Close(); err != nil {
			t.Fatal(err)
		}
		reopened, err := NewRaftLog(path)
		if err != nil {
			t.Fatal(err)
		}
		defer reopened.Close()
		if entry := reopened.Get(1); entry == nil || string(entry.Data) != "retained" {
			t.Error("normal append/reopen behavior changed")
		}
	})
}
