package edgemesh

import (
	"bytes"
	"compress/gzip"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"math/rand/v2"
	"net"
	"net/http"

	"github.com/chronicle-db/chronicle/internal/httputil"
	"strings"
	"sync/atomic"
	"time"

	chronicle "github.com/chronicle-db/chronicle"
)

// Start begins mesh network operations.
func (m *EdgeMesh) Start() error {
	if m.running.Swap(true) {
		return errors.New("mesh already running")
	}

	if m.config.BindAddr != "" {
		mux := http.NewServeMux()
		mux.HandleFunc("/mesh/sync", m.handleSync)
		mux.HandleFunc("/mesh/gossip", m.handleGossip)
		mux.HandleFunc("/mesh/heartbeat", m.handleHeartbeat)
		mux.HandleFunc("/mesh/operations", m.handleOperations)
		mux.HandleFunc("/mesh/state", m.handleState)

		m.server = &http.Server{
			Addr:    m.config.BindAddr,
			Handler: mux,
		}

		m.wg.Add(1)
		go func(ctx context.Context) {
			defer m.wg.Done()
			if err := m.server.ListenAndServe(); err != http.ErrServerClosed {
				slog.Error("edge mesh server error", "err", err)
			}
		}(m.ctx)
	}

	if m.config.EnableMDNS {
		m.startMDNS()
	}

	for _, seed := range m.config.Seeds {
		m.wg.Add(1)
		go func(addr string) {
			defer m.wg.Done()
			m.connectPeer(addr)
		}(seed)
	}

	m.wg.Add(3)
	go m.syncLoop()
	go m.gossipLoop()
	go m.heartbeatLoop()

	return nil
}

// Stop stops mesh network operations.
func (m *EdgeMesh) Stop() error {
	if !m.running.Swap(false) {
		return nil
	}

	m.cancel()

	if m.mdnsServer != nil {
		m.mdnsServer.Stop()
	}

	if m.server != nil {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		m.server.Shutdown(ctx)
	}

	m.wg.Wait()
	return nil
}

// Write performs a CRDT-aware write operation.
func (m *EdgeMesh) Write(p chronicle.Point) error {

	lamport := atomic.AddUint64(&m.lamportTime, 1)
	vc := m.vectorClock.Tick(m.config.NodeID)

	op := CRDTOperation{
		ID:          fmt.Sprintf("%s-%d-%d", m.config.NodeID, p.Timestamp, lamport),
		Type:        CRDTOpWrite,
		NodeID:      m.config.NodeID,
		Timestamp:   p.Timestamp,
		LamportTime: lamport,
		VectorClock: m.vectorClock.GetAll(),
		Metric:      p.Metric,
		Tags:        p.Tags,
		Value:       p.Value,
	}
	op.Checksum = m.calculateOpChecksum(op)

	m.opLog.Append(op)

	if err := m.db.Write(p); err != nil {
		return err
	}

	go m.broadcastOperation(op)

	_ = vc

	return nil
}

// WriteBatch performs batch CRDT-aware writes.
func (m *EdgeMesh) WriteBatch(points []chronicle.Point) error {
	for _, p := range points {
		if err := m.Write(p); err != nil {
			return err
		}
	}
	return nil
}

// Peers returns all known peers.
func (m *EdgeMesh) Peers() []*MeshPeer {
	m.peersMu.RLock()
	defer m.peersMu.RUnlock()

	peers := make([]*MeshPeer, 0, len(m.peers))
	for _, p := range m.peers {
		peers = append(peers, &MeshPeer{
			ID:            p.ID,
			Addr:          p.Addr,
			LastSeen:      p.LastSeen,
			LastSynced:    p.LastSynced,
			Healthy:       p.Healthy,
			VectorClock:   p.VectorClock,
			Metadata:      p.Metadata,
			BytesSent:     p.BytesSent,
			BytesReceived: p.BytesReceived,
		})
	}
	return peers
}

// Stats returns mesh statistics.
func (m *EdgeMesh) Stats() EdgeMeshStats {
	m.statsMu.RLock()
	defer m.statsMu.RUnlock()

	m.peersMu.RLock()
	peerCount := len(m.peers)
	healthyPeers := 0
	for _, p := range m.peers {
		if p.Healthy {
			healthyPeers++
		}
	}
	m.peersMu.RUnlock()

	stats := m.stats
	stats.NodeID = m.config.NodeID
	stats.PeerCount = peerCount
	stats.HealthyPeers = healthyPeers
	stats.OperationCount = uint64(m.opLog.Len())
	return stats
}

// SyncNow triggers immediate sync with all peers.
func (m *EdgeMesh) SyncNow() error {
	m.peersMu.RLock()
	peers := make([]*MeshPeer, 0, len(m.peers))
	for _, p := range m.peers {
		if p.Healthy {
			peers = append(peers, p)
		}
	}
	m.peersMu.RUnlock()

	var lastErr error
	for _, peer := range peers {
		if err := m.syncWithPeer(peer); err != nil {
			lastErr = err
		}
	}
	return lastErr
}

func (m *EdgeMesh) syncLoop() {
	defer m.wg.Done()

	ticker := time.NewTicker(m.config.SyncInterval)
	defer ticker.Stop()

	for {
		select {
		case <-m.ctx.Done():
			return
		case <-ticker.C:
			m.performSync()
		}
	}
}

func (m *EdgeMesh) gossipLoop() {
	defer m.wg.Done()

	ticker := time.NewTicker(m.config.GossipInterval)
	defer ticker.Stop()

	for {
		select {
		case <-m.ctx.Done():
			return
		case <-ticker.C:
			m.performGossip()
		}
	}
}

func (m *EdgeMesh) heartbeatLoop() {
	defer m.wg.Done()

	ticker := time.NewTicker(m.config.HeartbeatInterval)
	defer ticker.Stop()

	for {
		select {
		case <-m.ctx.Done():
			return
		case <-ticker.C:
			m.sendHeartbeats()
			m.checkPeerHealth()
		}
	}
}

func (m *EdgeMesh) performSync() {
	m.peersMu.RLock()
	peers := make([]*MeshPeer, 0, len(m.peers))
	for _, p := range m.peers {
		if p.Healthy {
			peers = append(peers, p)
		}
	}
	m.peersMu.RUnlock()

	for _, peer := range peers {
		go func(p *MeshPeer) {
			if err := m.syncWithPeer(p); err != nil {
				m.recordSyncError(p.ID, err)
			}
		}(peer)
	}
}

func (m *EdgeMesh) syncWithPeer(peer *MeshPeer) error {

	m.syncStateMu.RLock()
	peerState, exists := m.syncState[peer.ID]
	m.syncStateMu.RUnlock()

	var lastVC map[string]uint64
	if exists {
		lastVC = peerState.LastVectorClock
	} else {
		lastVC = make(map[string]uint64)
	}

	ops := m.opLog.GetSince(lastVC)
	if len(ops) == 0 {
		return nil
	}

	req := MeshSyncRequest{
		NodeID:      m.config.NodeID,
		VectorClock: m.vectorClock.GetAll(),
		Operations:  ops,
	}

	resp, err := m.sendSyncRequest(peer, req)
	if err != nil {
		return err
	}

	for _, op := range resp.Operations {
		if err := m.applyOperation(op); err != nil {
			continue
		}
	}

	m.vectorClock.Merge(resp.VectorClock)

	m.syncStateMu.Lock()
	if m.syncState[peer.ID] == nil {
		m.syncState[peer.ID] = &PeerSyncState{PeerID: peer.ID}
	}
	m.syncState[peer.ID].LastSyncTime = time.Now()
	m.syncState[peer.ID].LastVectorClock = resp.VectorClock
	m.syncState[peer.ID].SentOpCount += uint64(len(ops))
	m.syncState[peer.ID].ReceivedOpCount += uint64(len(resp.Operations))
	m.syncStateMu.Unlock()

	m.peersMu.Lock()
	if p, ok := m.peers[peer.ID]; ok {
		p.LastSynced = time.Now()
		p.VectorClock = resp.VectorClock
	}
	m.peersMu.Unlock()

	m.statsMu.Lock()
	m.stats.SyncCount++
	m.stats.LastSyncTime = time.Now()
	m.statsMu.Unlock()

	return nil
}

func (m *EdgeMesh) sendSyncRequest(peer *MeshPeer, req MeshSyncRequest) (*MeshSyncResponse, error) {
	data, err := json.Marshal(req)
	if err != nil {
		return nil, err
	}

	if m.config.CompressSync {
		var buf bytes.Buffer
		gw := gzip.NewWriter(&buf)
		gw.Write(data)
		gw.Close()
		data = buf.Bytes()
	}

	httpReq, err := http.NewRequestWithContext(m.ctx, http.MethodPost,
		fmt.Sprintf("http://%s/mesh/sync", peer.Addr), bytes.NewReader(data))
	if err != nil {
		return nil, err
	}

	httpReq.Header.Set("Content-Type", "application/json")
	if m.config.CompressSync {
		httpReq.Header.Set("Content-Encoding", "gzip")
	}
	httpReq.Header.Set("X-Node-ID", m.config.NodeID)

	resp, err := m.client.Do(httpReq)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		body, _ := io.ReadAll(resp.Body)
		return nil, fmt.Errorf("sync failed: %d - %s", resp.StatusCode, string(body))
	}

	m.peersMu.Lock()
	if p, ok := m.peers[peer.ID]; ok {
		p.BytesSent += int64(len(data))
	}
	m.peersMu.Unlock()

	var syncResp MeshSyncResponse
	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, err
	}

	if resp.Header.Get("Content-Encoding") == "gzip" {
		gr, err := gzip.NewReader(bytes.NewReader(body))
		if err != nil {
			return nil, err
		}
		body, err = io.ReadAll(gr)
		gr.Close()
		if err != nil {
			return nil, err
		}
	}

	if err := json.Unmarshal(body, &syncResp); err != nil {
		return nil, err
	}

	m.peersMu.Lock()
	if p, ok := m.peers[peer.ID]; ok {
		p.BytesReceived += int64(len(body))
	}
	m.peersMu.Unlock()

	m.statsMu.Lock()
	m.stats.BytesSent += int64(len(data))
	m.stats.BytesReceived += int64(len(body))
	m.statsMu.Unlock()

	return &syncResp, nil
}

func (m *EdgeMesh) applyOperation(op CRDTOperation) error {

	if op.Checksum != "" {
		expected := m.calculateOpChecksum(op)
		if op.Checksum != expected {
			return errors.New("operation checksum mismatch")
		}
	}

	shouldApply, err := m.resolveConflict(op)
	if err != nil {
		return err
	}

	if !shouldApply {
		return nil
	}

	switch op.Type {
	case CRDTOpWrite:
		p := chronicle.Point{
			Metric:    op.Metric,
			Tags:      op.Tags,
			Value:     op.Value,
			Timestamp: op.Timestamp,
		}
		if err := m.db.Write(p); err != nil {
			return err
		}

	case CRDTOpDelete:

	}

	m.opLog.Append(op)

	m.vectorClock.Merge(op.VectorClock)

	return nil
}

func (m *EdgeMesh) resolveConflict(op CRDTOperation) (bool, error) {
	switch m.config.MergeStrategy {
	case CRDTMergeLastWriteWins:

		return true, nil

	case CRDTMergeLamportClock:

		if op.LamportTime > atomic.LoadUint64(&m.lamportTime) {
			atomic.StoreUint64(&m.lamportTime, op.LamportTime)
			return true, nil
		}

		if op.LamportTime == atomic.LoadUint64(&m.lamportTime) {
			return op.NodeID > m.config.NodeID, nil
		}
		return false, nil

	case CRDTMergeVectorClock:

		if m.vectorClock.HappensBefore(op.VectorClock) {

			return true, nil
		}
		if m.vectorClock.Concurrent(op.VectorClock) {

			m.statsMu.Lock()
			m.stats.ConflictsResolved++
			m.statsMu.Unlock()

			return op.NodeID > m.config.NodeID, nil
		}

		return false, nil

	case CRDTMergeMaxValue:

		return true, nil

	case CRDTMergeUnion:

		return true, nil

	default:
		return true, nil
	}
}

func (m *EdgeMesh) broadcastOperation(op CRDTOperation) {
	m.peersMu.RLock()
	peers := make([]*MeshPeer, 0, len(m.peers))
	for _, p := range m.peers {
		if p.Healthy {
			peers = append(peers, p)
		}
	}
	m.peersMu.RUnlock()

	for _, peer := range peers {
		go func(p *MeshPeer) {

			req := MeshOperationRequest{
				NodeID:    m.config.NodeID,
				Operation: op,
			}

			data, _ := json.Marshal(req)
			httpReq, err := http.NewRequestWithContext(m.ctx, http.MethodPost,
				fmt.Sprintf("http://%s/mesh/operations", p.Addr), bytes.NewReader(data))
			if err != nil {
				return
			}

			httpReq.Header.Set("Content-Type", "application/json")
			httpReq.Header.Set("X-Node-ID", m.config.NodeID)

			resp, err := m.client.Do(httpReq)
			if err != nil {
				return
			}
			resp.Body.Close()
		}(peer)
	}
}

func (m *EdgeMesh) calculateOpChecksum(op CRDTOperation) string {

	data := fmt.Sprintf("%s:%s:%d:%d:%s:%.15f",
		op.NodeID, op.Metric, op.Timestamp, op.LamportTime,
		sortedTagsString(op.Tags), op.Value)
	hash := sha256.Sum256([]byte(data))
	return hex.EncodeToString(hash[:8])
}

func (m *EdgeMesh) performGossip() {
	m.peersMu.RLock()
	peers := make([]*MeshPeer, 0, len(m.peers))
	for _, p := range m.peers {
		if p.Healthy {
			peers = append(peers, p)
		}
	}
	m.peersMu.RUnlock()

	if len(peers) == 0 {
		return
	}

	numTargets := 3
	if len(peers) < numTargets {
		numTargets = len(peers)
	}

	for i := len(peers) - 1; i > 0; i-- {
		j := rand.IntN(i + 1)
		peers[i], peers[j] = peers[j], peers[i]
	}

	gossip := MeshGossipMessage{
		NodeID:      m.config.NodeID,
		Addr:        m.config.AdvertiseAddr,
		VectorClock: m.vectorClock.GetAll(),
		Peers:       m.getPeerList(),
	}

	data, _ := json.Marshal(gossip)

	for i := 0; i < numTargets; i++ {
		go func(peer *MeshPeer) {
			httpReq, err := http.NewRequestWithContext(m.ctx, http.MethodPost,
				fmt.Sprintf("http://%s/mesh/gossip", peer.Addr), bytes.NewReader(data))
			if err != nil {
				return
			}

			httpReq.Header.Set("Content-Type", "application/json")
			httpReq.Header.Set("X-Node-ID", m.config.NodeID)

			resp, err := m.client.Do(httpReq)
			if err != nil {
				return
			}
			defer resp.Body.Close()

			var respGossip MeshGossipMessage
			if err := json.NewDecoder(resp.Body).Decode(&respGossip); err != nil {
				return
			}

			for _, p := range respGossip.Peers {
				m.addPeer(p.ID, p.Addr)
			}

			m.vectorClock.Merge(respGossip.VectorClock)
		}(peers[i])
	}
}

func (m *EdgeMesh) getPeerList() []MeshPeerInfo {
	m.peersMu.RLock()
	defer m.peersMu.RUnlock()

	peers := make([]MeshPeerInfo, 0, len(m.peers))
	for _, p := range m.peers {
		peers = append(peers, MeshPeerInfo{
			ID:   p.ID,
			Addr: p.Addr,
		})
	}
	return peers
}

func (m *EdgeMesh) sendHeartbeats() {
	m.peersMu.RLock()
	peers := make([]*MeshPeer, 0, len(m.peers))
	for _, p := range m.peers {
		peers = append(peers, p)
	}
	m.peersMu.RUnlock()

	heartbeat := MeshHeartbeat{
		NodeID:      m.config.NodeID,
		Timestamp:   time.Now().UnixNano(),
		VectorClock: m.vectorClock.GetAll(),
	}

	data, _ := json.Marshal(heartbeat)

	for _, peer := range peers {
		go func(p *MeshPeer) {
			httpReq, err := http.NewRequestWithContext(m.ctx, http.MethodPost,
				fmt.Sprintf("http://%s/mesh/heartbeat", p.Addr), bytes.NewReader(data))
			if err != nil {
				return
			}

			httpReq.Header.Set("Content-Type", "application/json")
			httpReq.Header.Set("X-Node-ID", m.config.NodeID)

			resp, err := m.client.Do(httpReq)
			if err != nil {
				m.markPeerUnhealthy(p.ID)
				return
			}
			resp.Body.Close()

			if resp.StatusCode == http.StatusOK {
				m.markPeerHealthy(p.ID)
			}
		}(peer)
	}
}

func (m *EdgeMesh) checkPeerHealth() {
	now := time.Now()

	m.peersMu.Lock()
	defer m.peersMu.Unlock()

	for _, peer := range m.peers {
		if now.Sub(peer.LastSeen) > m.config.PeerTimeout {
			peer.Healthy = false
		}
	}
}

func (m *EdgeMesh) addPeer(id, addr string) {
	if id == m.config.NodeID {
		return
	}

	m.peersMu.Lock()
	defer m.peersMu.Unlock()

	if len(m.peers) >= m.config.MaxPeers {
		return
	}

	if _, exists := m.peers[id]; !exists {
		m.peers[id] = &MeshPeer{
			ID:          id,
			Addr:        addr,
			LastSeen:    time.Now(),
			Healthy:     true,
			VectorClock: make(map[string]uint64),
			Metadata:    make(map[string]string),
		}
	}
}

func (m *EdgeMesh) markPeerHealthy(id string) {
	m.peersMu.Lock()
	defer m.peersMu.Unlock()
	if peer, ok := m.peers[id]; ok {
		peer.Healthy = true
		peer.LastSeen = time.Now()
	}
}

func (m *EdgeMesh) markPeerUnhealthy(id string) {
	m.peersMu.Lock()
	defer m.peersMu.Unlock()
	if peer, ok := m.peers[id]; ok {
		peer.Healthy = false
	}
}

func (m *EdgeMesh) connectPeer(addr string) {

	gossip := MeshGossipMessage{
		NodeID:      m.config.NodeID,
		Addr:        m.config.AdvertiseAddr,
		VectorClock: m.vectorClock.GetAll(),
		Peers:       m.getPeerList(),
	}

	data, _ := json.Marshal(gossip)

	httpReq, err := http.NewRequestWithContext(m.ctx, http.MethodPost,
		fmt.Sprintf("http://%s/mesh/gossip", addr), bytes.NewReader(data))
	if err != nil {
		return
	}

	httpReq.Header.Set("Content-Type", "application/json")
	httpReq.Header.Set("X-Node-ID", m.config.NodeID)

	resp, err := m.client.Do(httpReq)
	if err != nil {
		return
	}
	defer resp.Body.Close()

	var respGossip MeshGossipMessage
	if err := json.NewDecoder(resp.Body).Decode(&respGossip); err != nil {
		return
	}

	m.addPeer(respGossip.NodeID, addr)

	for _, p := range respGossip.Peers {
		m.addPeer(p.ID, p.Addr)
	}
}

func (m *EdgeMesh) recordSyncError(peerID string, err error) {
	m.syncStateMu.Lock()
	defer m.syncStateMu.Unlock()
	if state, ok := m.syncState[peerID]; ok {
		state.LastError = err.Error()
	}
}

func (m *EdgeMesh) handleSync(w http.ResponseWriter, r *http.Request) {
	var req MeshSyncRequest

	body, err := io.ReadAll(r.Body)
	if err != nil {
		http.Error(w, "bad request", http.StatusBadRequest)
		return
	}

	if r.Header.Get("Content-Encoding") == "gzip" {
		gr, err := gzip.NewReader(bytes.NewReader(body))
		if err != nil {
			http.Error(w, "bad request", http.StatusBadRequest)
			return
		}
		body, _ = io.ReadAll(gr) //nolint:errcheck // best-effort decompression
		gr.Close()
	}

	if err := json.Unmarshal(body, &req); err != nil {
		http.Error(w, "bad request", http.StatusBadRequest)
		return
	}

	for _, op := range req.Operations {
		_ = m.applyOperation(op) //nolint:errcheck // best-effort operation replay
	}

	opsToSend := m.opLog.GetSince(req.VectorClock)

	if len(opsToSend) > m.config.SyncBatchSize {
		opsToSend = opsToSend[:m.config.SyncBatchSize]
	}

	resp := MeshSyncResponse{
		NodeID:      m.config.NodeID,
		VectorClock: m.vectorClock.GetAll(),
		Operations:  opsToSend,
	}

	respData, _ := json.Marshal(resp)

	if m.config.CompressSync && r.Header.Get("Accept-Encoding") == "gzip" {
		w.Header().Set("Content-Encoding", "gzip")
		var buf bytes.Buffer
		gw := gzip.NewWriter(&buf)
		gw.Write(respData)
		gw.Close()
		respData = buf.Bytes()
	}

	w.Header().Set("Content-Type", "application/json")
	w.Write(respData)
}

func (m *EdgeMesh) handleGossip(w http.ResponseWriter, r *http.Request) {
	var gossip MeshGossipMessage
	if err := json.NewDecoder(r.Body).Decode(&gossip); err != nil {
		http.Error(w, "bad request", http.StatusBadRequest)
		return
	}

	m.addPeer(gossip.NodeID, gossip.Addr)
	m.markPeerHealthy(gossip.NodeID)

	for _, p := range gossip.Peers {
		m.addPeer(p.ID, p.Addr)
	}

	m.vectorClock.Merge(gossip.VectorClock)

	resp := MeshGossipMessage{
		NodeID:      m.config.NodeID,
		Addr:        m.config.AdvertiseAddr,
		VectorClock: m.vectorClock.GetAll(),
		Peers:       m.getPeerList(),
	}

	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(resp)
}

func (m *EdgeMesh) handleHeartbeat(w http.ResponseWriter, r *http.Request) {
	var heartbeat MeshHeartbeat
	if err := json.NewDecoder(r.Body).Decode(&heartbeat); err != nil {
		http.Error(w, "bad request", http.StatusBadRequest)
		return
	}

	m.markPeerHealthy(heartbeat.NodeID)

	m.vectorClock.Merge(heartbeat.VectorClock)

	w.WriteHeader(http.StatusOK)
}

func (m *EdgeMesh) handleOperations(w http.ResponseWriter, r *http.Request) {
	var req MeshOperationRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		http.Error(w, "bad request", http.StatusBadRequest)
		return
	}

	if err := m.applyOperation(req.Operation); err != nil {
		httputil.InternalError(w, err, "internal error")
		return
	}

	w.WriteHeader(http.StatusOK)
}

func (m *EdgeMesh) handleState(w http.ResponseWriter, r *http.Request) {
	state := MeshStateResponse{
		NodeID:      m.config.NodeID,
		VectorClock: m.vectorClock.GetAll(),
		PeerCount:   len(m.peers),
		OpLogSize:   m.opLog.Len(),
		Stats:       m.Stats(),
	}

	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(state)
}

func (m *EdgeMesh) startMDNS() {
	if m.config.MDNSService == "" {
		return
	}

	ctx, cancel := context.WithCancel(m.ctx)
	m.mdnsServer = &mdnsServer{
		service: m.config.MDNSService,
		nodeID:  m.config.NodeID,
		addr:    m.config.AdvertiseAddr,
		cancel:  cancel,
	}
	m.mdnsServer.running.Store(true)

	go m.mdnsAdvertise(ctx)

	go m.mdnsBrowse(ctx)
}

func (m *EdgeMesh) mdnsAdvertise(ctx context.Context) {

	addr, err := net.ResolveUDPAddr("udp4", "224.0.0.251:5353")
	if err != nil {
		return
	}

	conn, err := net.ListenMulticastUDP("udp4", nil, addr)
	if err != nil {
		return
	}
	defer conn.Close()

	ticker := time.NewTicker(30 * time.Second)
	defer ticker.Stop()

	announcement := fmt.Sprintf("%s._chronicle._tcp.local\tIN\tTXT\t\"id=%s\" \"addr=%s\"",
		m.config.NodeID, m.config.NodeID, m.config.AdvertiseAddr)

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			conn.WriteToUDP([]byte(announcement), addr)
		}
	}
}

func (m *EdgeMesh) mdnsBrowse(ctx context.Context) {
	addr, err := net.ResolveUDPAddr("udp4", "224.0.0.251:5353")
	if err != nil {
		return
	}

	conn, err := net.ListenMulticastUDP("udp4", nil, addr)
	if err != nil {
		return
	}
	defer conn.Close()

	buf := make([]byte, 1500)
	for {
		select {
		case <-ctx.Done():
			return
		default:
			conn.SetReadDeadline(time.Now().Add(time.Second))
			n, _, err := conn.ReadFromUDP(buf)
			if err != nil {
				continue
			}

			response := string(buf[:n])
			if strings.Contains(response, "_chronicle._tcp") {

			}
		}
	}
}
