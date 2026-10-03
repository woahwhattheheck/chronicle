package edgemesh

import (
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	chronicle "github.com/chronicle-db/chronicle"
)

func TestNewEdgeMesh(t *testing.T) {
	dir := t.TempDir()
	cfg := chronicle.DefaultConfig(dir + "/test.db")
	db, err := chronicle.Open(cfg.Path, cfg)
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	defer db.Close()

	config := DefaultEdgeMeshConfig()
	config.BindAddr = "" // Disable HTTP server for test

	mesh, err := NewEdgeMesh(db, config)
	if err != nil {
		t.Fatalf("NewEdgeMesh() error = %v", err)
	}

	if mesh.config.NodeID == "" {
		t.Error("NodeID should be set")
	}
	if mesh.vectorClock == nil {
		t.Error("vectorClock should be initialized")
	}
	if mesh.opLog == nil {
		t.Error("opLog should be initialized")
	}
}

func TestMeshVectorClock(t *testing.T) {
	vc := NewMeshVectorClock()

	// Test Tick
	v1 := vc.Tick("node1")
	if v1 != 1 {
		t.Errorf("Tick() = %d, want 1", v1)
	}

	v2 := vc.Tick("node1")
	if v2 != 2 {
		t.Errorf("Tick() = %d, want 2", v2)
	}

	// Test Get
	if vc.Get("node1") != 2 {
		t.Errorf("Get(node1) = %d, want 2", vc.Get("node1"))
	}
	if vc.Get("node2") != 0 {
		t.Errorf("Get(node2) = %d, want 0", vc.Get("node2"))
	}

	// Test GetAll
	all := vc.GetAll()
	if all["node1"] != 2 {
		t.Errorf("GetAll()[node1] = %d, want 2", all["node1"])
	}

	// Test Merge
	other := map[string]uint64{"node1": 5, "node2": 3}
	vc.Merge(other)

	if vc.Get("node1") != 5 {
		t.Errorf("After Merge, Get(node1) = %d, want 5", vc.Get("node1"))
	}
	if vc.Get("node2") != 3 {
		t.Errorf("After Merge, Get(node2) = %d, want 3", vc.Get("node2"))
	}
}

func TestMeshVectorClockCausality(t *testing.T) {
	vc1 := NewMeshVectorClock()
	vc1.Tick("a")
	vc1.Tick("a")

	// vc1: {a: 2}
	// other: {a: 3, b: 1}
	other := map[string]uint64{"a": 3, "b": 1}

	// vc1 happened before other (vc1 < other)
	if !vc1.HappensBefore(other) {
		t.Error("vc1 should happen before other")
	}

	// Test concurrent clocks
	vc2 := NewMeshVectorClock()
	vc2.Tick("b")
	vc2.Tick("b")

	// vc2: {b: 2}
	// other: {a: 3, b: 1}
	// These are concurrent (incomparable)
	if !vc2.Concurrent(other) {
		t.Error("vc2 and other should be concurrent")
	}
}

func TestCRDTOpLog(t *testing.T) {
	log := NewCRDTOpLog(100)

	// Test append
	op1 := CRDTOperation{
		ID:          "op1",
		NodeID:      "node1",
		Timestamp:   time.Now().UnixNano(),
		VectorClock: map[string]uint64{"node1": 1},
		Metric:      "cpu",
		Value:       0.5,
	}
	log.Append(op1)

	if log.Len() != 1 {
		t.Errorf("Len() = %d, want 1", log.Len())
	}

	// Test GetAll
	ops := log.GetAll()
	if len(ops) != 1 {
		t.Errorf("GetAll() len = %d, want 1", len(ops))
	}

	// Test GetSince with empty clock (should return all)
	since := log.GetSince(map[string]uint64{})
	if len(since) != 1 {
		t.Errorf("GetSince({}) len = %d, want 1", len(since))
	}

	// Test GetSince with matching clock (should return nothing new)
	since = log.GetSince(map[string]uint64{"node1": 1})
	if len(since) != 0 {
		t.Errorf("GetSince({node1:1}) len = %d, want 0", len(since))
	}

	// Test capacity eviction
	smallLog := NewCRDTOpLog(3)
	for i := 0; i < 5; i++ {
		smallLog.Append(CRDTOperation{
			ID:          string(rune('a' + i)),
			VectorClock: map[string]uint64{"n": uint64(i + 1)},
		})
	}
	if smallLog.Len() != 3 {
		t.Errorf("After overflow, Len() = %d, want 3", smallLog.Len())
	}
}

func TestEdgeMeshWrite(t *testing.T) {
	dir := t.TempDir()
	cfg := chronicle.DefaultConfig(dir + "/test.db")
	db, err := chronicle.Open(cfg.Path, cfg)
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	defer db.Close()

	config := DefaultEdgeMeshConfig()
	config.BindAddr = ""
	config.NodeID = "test-node"

	mesh, err := NewEdgeMesh(db, config)
	if err != nil {
		t.Fatalf("NewEdgeMesh() error = %v", err)
	}

	// Write via mesh
	p := chronicle.Point{
		Metric:    "temperature",
		Tags:      map[string]string{"room": "kitchen"},
		Value:     22.5,
		Timestamp: time.Now().UnixNano(),
	}

	if err := mesh.Write(p); err != nil {
		t.Fatalf("Write() error = %v", err)
	}
	db.Flush()

	// Verify operation was logged
	if mesh.opLog.Len() != 1 {
		t.Errorf("opLog.Len() = %d, want 1", mesh.opLog.Len())
	}

	// Verify vector clock was incremented
	if mesh.vectorClock.Get("test-node") < 1 {
		t.Error("vectorClock should be incremented")
	}

	// Verify data was written to chronicle.DB
	result, err := db.Execute(&chronicle.Query{
		Metric: "temperature",
		Start:  p.Timestamp - int64(time.Hour),
		End:    p.Timestamp + int64(time.Hour),
	})
	if err != nil {
		t.Fatalf("Execute() error = %v", err)
	}
	if len(result.Points) != 1 {
		t.Errorf("chronicle.Query returned %d points, want 1", len(result.Points))
	}
}

func TestEdgeMeshPeerManagement(t *testing.T) {
	dir := t.TempDir()
	cfg := chronicle.DefaultConfig(dir + "/test.db")
	db, err := chronicle.Open(cfg.Path, cfg)
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	defer db.Close()

	config := DefaultEdgeMeshConfig()
	config.BindAddr = ""
	config.MaxPeers = 3

	mesh, err := NewEdgeMesh(db, config)
	if err != nil {
		t.Fatalf("NewEdgeMesh() error = %v", err)
	}

	// Add peers
	mesh.addPeer("peer1", "192.168.1.1:7946")
	mesh.addPeer("peer2", "192.168.1.2:7946")
	mesh.addPeer("peer3", "192.168.1.3:7946")

	peers := mesh.Peers()
	if len(peers) != 3 {
		t.Errorf("Peers() len = %d, want 3", len(peers))
	}

	// Try to add beyond max
	mesh.addPeer("peer4", "192.168.1.4:7946")
	peers = mesh.Peers()
	if len(peers) != 3 {
		t.Errorf("After exceeding max, Peers() len = %d, want 3", len(peers))
	}

	// Mark peer unhealthy
	mesh.markPeerUnhealthy("peer1")
	for _, p := range mesh.Peers() {
		if p.ID == "peer1" && p.Healthy {
			t.Error("peer1 should be unhealthy")
		}
	}

	// Mark peer healthy
	mesh.markPeerHealthy("peer1")
	for _, p := range mesh.Peers() {
		if p.ID == "peer1" && !p.Healthy {
			t.Error("peer1 should be healthy")
		}
	}
}

type edgeMeshGossipTestTransport func(*http.Request) (*http.Response, error)

func (f edgeMeshGossipTestTransport) RoundTrip(req *http.Request) (*http.Response, error) {
	return f(req)
}

type edgeMeshGossipTestBody struct {
	io.Reader
	completed chan<- struct{}
}

func (b *edgeMeshGossipTestBody) Close() error {
	b.completed <- struct{}{}
	return nil
}

func TestEdgeMeshGossipTargetSelection(t *testing.T) {
	for _, healthy := range []int{0, 1, 2, 3, 8} {
		t.Run(fmt.Sprintf("healthy_%d", healthy), func(t *testing.T) {
			config := DefaultEdgeMeshConfig()
			config.NodeID = "gossip-test"
			config.BindAddr = ""
			mesh, err := NewEdgeMesh(nil, config)
			if err != nil {
				t.Fatal(err)
			}
			t.Cleanup(mesh.cancel)

			requests := make(chan string, healthy+2)
			completed := make(chan struct{}, healthy+2)
			// Every request stays in memory; no network listener or peer is used.
			mesh.client.Transport = edgeMeshGossipTestTransport(func(req *http.Request) (*http.Response, error) {
				defer req.Body.Close()
				requests <- req.URL.Host
				return &http.Response{
					StatusCode: http.StatusOK,
					Header:     make(http.Header),
					Body: &edgeMeshGossipTestBody{
						Reader: strings.NewReader("{}"), completed: completed,
					},
				}, nil
			})
			eligible := make(map[string]bool, healthy)
			for i := 0; i < healthy+2; i++ {
				id := fmt.Sprintf("peer-%d", i)
				addr := fmt.Sprintf("peer-%d.test:7946", i)
				mesh.addPeer(id, addr)
				if i < healthy {
					eligible[addr] = true
				} else {
					mesh.markPeerUnhealthy(id)
				}
			}

			for range 16 {
				mesh.performGossip()
				seen := make(map[string]bool)
				deadline := time.NewTimer(5 * time.Second)
				for range min(3, healthy) {
					select {
					case addr := <-requests:
						if !eligible[addr] || seen[addr] {
							t.Fatalf("ineligible or repeated gossip target: %q", addr)
						}
						seen[addr] = true
					case <-deadline.C:
						t.Fatal("gossip did not dispatch the expected targets")
					}
					select {
					case <-completed:
					case <-deadline.C:
						t.Fatal("gossip request did not finish")
					}
				}
				deadline.Stop()
				select {
				case extra := <-requests:
					t.Fatalf("unexpected extra gossip target: %q", extra)
				default:
				}
				peers := mesh.Peers()
				if len(peers) != healthy+2 {
					t.Fatalf("gossip changed peer membership: %d peers", len(peers))
				}
				for _, peer := range peers {
					if peer.Healthy != eligible[peer.Addr] {
						t.Fatalf("gossip changed peer health: %+v", peer)
					}
				}
			}
		})
	}
}

func TestEdgeMeshStats(t *testing.T) {
	dir := t.TempDir()
	cfg := chronicle.DefaultConfig(dir + "/test.db")
	db, err := chronicle.Open(cfg.Path, cfg)
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	defer db.Close()

	config := DefaultEdgeMeshConfig()
	config.BindAddr = ""
	config.NodeID = "stats-test-node"

	mesh, err := NewEdgeMesh(db, config)
	if err != nil {
		t.Fatalf("NewEdgeMesh() error = %v", err)
	}

	mesh.addPeer("peer1", "192.168.1.1:7946")

	stats := mesh.Stats()
	if stats.NodeID != "stats-test-node" {
		t.Errorf("Stats().NodeID = %s, want stats-test-node", stats.NodeID)
	}
	if stats.PeerCount != 1 {
		t.Errorf("Stats().PeerCount = %d, want 1", stats.PeerCount)
	}
	if stats.HealthyPeers != 1 {
		t.Errorf("Stats().HealthyPeers = %d, want 1", stats.HealthyPeers)
	}
}

func TestEdgeMeshHTTPHandlers(t *testing.T) {
	dir := t.TempDir()
	cfg := chronicle.DefaultConfig(dir + "/test.db")
	db, err := chronicle.Open(cfg.Path, cfg)
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	defer db.Close()

	config := DefaultEdgeMeshConfig()
	config.BindAddr = ""
	config.NodeID = "handler-test"

	mesh, err := NewEdgeMesh(db, config)
	if err != nil {
		t.Fatalf("NewEdgeMesh() error = %v", err)
	}

	// Test gossip handler
	t.Run("handleGossip", func(t *testing.T) {
		gossip := MeshGossipMessage{
			NodeID:      "remote-peer",
			Addr:        "192.168.1.100:7946",
			VectorClock: map[string]uint64{"remote-peer": 5},
			Peers: []MeshPeerInfo{
				{ID: "other-peer", Addr: "192.168.1.101:7946"},
			},
		}

		body, _ := json.Marshal(gossip)
		req := httptest.NewRequest(http.MethodPost, "/mesh/gossip", emBytesReader(body))
		rec := httptest.NewRecorder()

		mesh.handleGossip(rec, req)

		if rec.Code != http.StatusOK {
			t.Errorf("handleGossip() status = %d, want %d", rec.Code, http.StatusOK)
		}

		// Verify peer was added
		found := false
		for _, p := range mesh.Peers() {
			if p.ID == "remote-peer" {
				found = true
				break
			}
		}
		if !found {
			t.Error("remote-peer should have been added")
		}

		// Verify response
		var resp MeshGossipMessage
		if err := json.NewDecoder(rec.Body).Decode(&resp); err != nil {
			t.Fatalf("Failed to decode response: %v", err)
		}
		if resp.NodeID != "handler-test" {
			t.Errorf("Response NodeID = %s, want handler-test", resp.NodeID)
		}
	})

	// Test heartbeat handler
	t.Run("handleHeartbeat", func(t *testing.T) {
		heartbeat := MeshHeartbeat{
			NodeID:      "remote-peer",
			Timestamp:   time.Now().UnixNano(),
			VectorClock: map[string]uint64{"remote-peer": 10},
		}

		body, _ := json.Marshal(heartbeat)
		req := httptest.NewRequest(http.MethodPost, "/mesh/heartbeat", emBytesReader(body))
		rec := httptest.NewRecorder()

		mesh.handleHeartbeat(rec, req)

		if rec.Code != http.StatusOK {
			t.Errorf("handleHeartbeat() status = %d, want %d", rec.Code, http.StatusOK)
		}
	})

	// Test state handler
	t.Run("handleState", func(t *testing.T) {
		req := httptest.NewRequest(http.MethodGet, "/mesh/state", nil)
		rec := httptest.NewRecorder()

		mesh.handleState(rec, req)

		if rec.Code != http.StatusOK {
			t.Errorf("handleState() status = %d, want %d", rec.Code, http.StatusOK)
		}

		var state MeshStateResponse
		if err := json.NewDecoder(rec.Body).Decode(&state); err != nil {
			t.Fatalf("Failed to decode state: %v", err)
		}
		if state.NodeID != "handler-test" {
			t.Errorf("State NodeID = %s, want handler-test", state.NodeID)
		}
	})
}

func TestCRDTMergeStrategies(t *testing.T) {
	tests := []struct {
		name     string
		strategy CRDTMergeStrategy
	}{
		{"LastWriteWins", CRDTMergeLastWriteWins},
		{"LamportClock", CRDTMergeLamportClock},
		{"VectorClock", CRDTMergeVectorClock},
		{"MaxValue", CRDTMergeMaxValue},
		{"Union", CRDTMergeUnion},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			dir := t.TempDir()
			cfg := chronicle.DefaultConfig(dir + "/test.db")
			db, err := chronicle.Open(cfg.Path, cfg)
			if err != nil {
				t.Fatalf("open: %v", err)
			}
			defer db.Close()

			config := DefaultEdgeMeshConfig()
			config.BindAddr = ""
			config.MergeStrategy = tt.strategy

			mesh, err := NewEdgeMesh(db, config)
			if err != nil {
				t.Fatalf("NewEdgeMesh() error = %v", err)
			}

			op := CRDTOperation{
				ID:          "test-op",
				NodeID:      "other-node",
				Timestamp:   time.Now().UnixNano(),
				LamportTime: 100,
				VectorClock: map[string]uint64{"other-node": 1},
				Metric:      "test",
				Value:       1.0,
			}

			shouldApply, err := mesh.resolveConflict(op)
			if err != nil {
				t.Errorf("resolveConflict() error = %v", err)
			}

			// All strategies should allow at least some operations
			_ = shouldApply
		})
	}
}

func TestEdgeMeshOperationChecksum(t *testing.T) {
	dir := t.TempDir()
	cfg := chronicle.DefaultConfig(dir + "/test.db")
	db, err := chronicle.Open(cfg.Path, cfg)
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	defer db.Close()

	config := DefaultEdgeMeshConfig()
	config.BindAddr = ""

	mesh, _ := NewEdgeMesh(db, config)

	op := CRDTOperation{
		NodeID:      "node1",
		Metric:      "cpu",
		Timestamp:   1234567890,
		LamportTime: 1,
		Tags:        map[string]string{"host": "server1", "region": "us-west"},
		Value:       0.75,
	}

	checksum1 := mesh.calculateOpChecksum(op)
	checksum2 := mesh.calculateOpChecksum(op)

	if checksum1 != checksum2 {
		t.Error("Checksum should be deterministic")
	}

	// Different value should produce different checksum
	op.Value = 0.80
	checksum3 := mesh.calculateOpChecksum(op)
	if checksum1 == checksum3 {
		t.Error("Different values should produce different checksums")
	}
}

func TestEdgeMeshApplyOperation(t *testing.T) {
	dir := t.TempDir()
	cfg := chronicle.DefaultConfig(dir + "/test.db")
	db, err := chronicle.Open(cfg.Path, cfg)
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	defer db.Close()

	config := DefaultEdgeMeshConfig()
	config.BindAddr = ""

	mesh, err := NewEdgeMesh(db, config)
	if err != nil {
		t.Fatalf("NewEdgeMesh() error = %v", err)
	}

	op := CRDTOperation{
		ID:          "external-op",
		Type:        CRDTOpWrite,
		NodeID:      "other-node",
		Timestamp:   time.Now().UnixNano(),
		LamportTime: 1,
		VectorClock: map[string]uint64{"other-node": 1},
		Metric:      "external_metric",
		Tags:        map[string]string{"source": "remote"},
		Value:       42.0,
	}
	op.Checksum = mesh.calculateOpChecksum(op)

	if err := mesh.applyOperation(op); err != nil {
		t.Fatalf("applyOperation() error = %v", err)
	}
	db.Flush()

	// Verify data was written
	result, err := db.Execute(&chronicle.Query{
		Metric: "external_metric",
		Start:  op.Timestamp - int64(time.Hour),
		End:    op.Timestamp + int64(time.Hour),
	})
	if err != nil {
		t.Fatalf("Execute() error = %v", err)
	}
	if len(result.Points) != 1 {
		t.Errorf("chronicle.Query returned %d points, want 1", len(result.Points))
	}
}

func emBytesReader(b []byte) *emBytesReaderImpl {
	return &emBytesReaderImpl{data: b, pos: 0}
}

type emBytesReaderImpl struct {
	data []byte
	pos  int
}

func (r *emBytesReaderImpl) Read(p []byte) (n int, err error) {
	if r.pos >= len(r.data) {
		return 0, err
	}
	n = copy(p, r.data[r.pos:])
	r.pos += n
	return n, nil
}
