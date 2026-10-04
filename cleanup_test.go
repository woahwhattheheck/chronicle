package raft

import (
	"errors"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

func stopRaftNodeOnCleanup(t *testing.T, node *RaftNode) {
	t.Helper()
	t.Cleanup(func() {
		if err := node.Stop(); err != nil {
			t.Errorf("Stop: %v", err)
		}
	})
}

func TestRaftResourceLifecycle(t *testing.T) {
	for _, started := range []bool{false, true} {
		name := "unstarted"
		if started {
			name = "started"
		}
		t.Run(name, func(t *testing.T) {
			config := DefaultRaftConfig()
			config.DataDir = t.TempDir()
			config.BindAddr = ""
			config.ElectionTimeoutMin = time.Hour
			config.ElectionTimeoutMax = 2 * time.Hour
			config.SnapshotInterval = 0
			config.BatchingEnabled = false
			config.CheckQuorumEnabled = false
			node, err := NewRaftNode(newTestStore(), config)
			if err != nil {
				t.Fatal(err)
			}
			file := node.log.file
			// Release the baseline's leaked handle after observing it, not before.
			t.Cleanup(func() { _ = file.Close() })
			stopRaftNodeOnCleanup(t, node)
			if started {
				if err := node.Start(); err != nil {
					t.Fatal(err)
				}
			}
			for i := 0; i < 2; i++ {
				if err := node.Stop(); err != nil {
					t.Fatalf("Stop %d: %v", i+1, err)
				}
			}
			if _, err := file.Stat(); !errors.Is(err, os.ErrClosed) {
				t.Errorf("log remains open after Stop: %v", err)
			}
			if node.ctx.Err() == nil {
				t.Error("Stop did not cancel the node context")
			}
		})
	}
}

func TestRaftConstructorFailureReleasesLog(t *testing.T) {
	config := DefaultRaftConfig()
	config.DataDir = t.TempDir()
	config.BindAddr = ""
	if err := os.WriteFile(filepath.Join(config.DataDir, "raft.state"), []byte("invalid-json"), 0600); err != nil {
		t.Fatal(err)
	}
	node, err := NewRaftNode(newTestStore(), config)
	if node != nil || err == nil || !strings.Contains(err.Error(), "failed to load state") {
		t.Fatalf("expected the state-load failure, got node=%v err=%v", node, err)
	}
	// Native Windows refuses removal while the constructor's file is open.
	if err := os.Remove(filepath.Join(config.DataDir, "raft.log")); err != nil {
		t.Fatalf("failed constructor retained its log handle: %v", err)
	}
}

func TestRaftLogConstructorFailureReleasesFile(t *testing.T) {
	path := filepath.Join(t.TempDir(), "corrupt.log")
	if err := os.WriteFile(path, []byte("not valid gob"), 0600); err != nil {
		t.Fatal(err)
	}
	log, err := NewRaftLog(path)
	if log != nil || err == nil {
		t.Fatalf("expected corrupt-log error, got log=%v err=%v", log, err)
	}
	if err := os.Remove(path); err != nil {
		t.Fatalf("failed log constructor retained its file handle: %v", err)
	}
}
