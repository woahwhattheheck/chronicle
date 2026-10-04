package raft

import (
	"context"
	"encoding/gob"
	"encoding/json"
	"errors"
	"fmt"
	"hash/crc32"
	"log/slog"
	"net/http"
	"os"
	"path/filepath"
	"sync"
	"sync/atomic"
	"time"
)

// RaftConfig configures the Raft consensus algorithm.
type RaftConfig struct {
	// NodeID is the unique identifier for this node.
	NodeID string

	// DataDir is the directory for persistent storage.
	DataDir string

	// BindAddr is the address to bind for Raft communication.
	BindAddr string

	// AdvertiseAddr is the address to advertise to other nodes.
	AdvertiseAddr string

	// Peers are the initial cluster members (excluding self).
	Peers []RaftPeer

	// ElectionTimeoutMin is the minimum election timeout.
	ElectionTimeoutMin time.Duration

	// ElectionTimeoutMax is the maximum election timeout.
	ElectionTimeoutMax time.Duration

	// HeartbeatInterval is how often the leader sends heartbeats.
	HeartbeatInterval time.Duration

	// SnapshotInterval is how often to check for snapshot conditions.
	SnapshotInterval time.Duration

	// SnapshotThreshold is the log size that triggers snapshotting.
	SnapshotThreshold uint64

	// MaxAppendEntries is the maximum entries per AppendEntries RPC.
	MaxAppendEntries int

	// MaxInflightRequests is the maximum concurrent replication requests.
	MaxInflightRequests int

	// LeaseTimeout for linearizable reads (leader lease).
	LeaseTimeout time.Duration

	// PreVoteEnabled enables the pre-vote protocol to prevent disruptions.
	PreVoteEnabled bool

	// CheckQuorumEnabled checks quorum connectivity periodically.
	CheckQuorumEnabled bool

	// BatchingEnabled enables request batching for throughput.
	BatchingEnabled bool

	// BatchMaxSize is the maximum batch size.
	BatchMaxSize int

	// BatchMaxWait is the maximum time to wait for batching.
	BatchMaxWait time.Duration
}

// RaftPeer represents a peer in the Raft cluster.
type RaftPeer struct {
	ID   string `json:"id"`
	Addr string `json:"addr"`
}

// DefaultRaftConfig returns default Raft configuration.
func DefaultRaftConfig() RaftConfig {
	return RaftConfig{
		NodeID:              fmt.Sprintf("raft-%d", time.Now().UnixNano()%10000),
		ElectionTimeoutMin:  150 * time.Millisecond,
		ElectionTimeoutMax:  300 * time.Millisecond,
		HeartbeatInterval:   50 * time.Millisecond,
		SnapshotInterval:    30 * time.Second,
		SnapshotThreshold:   10000,
		MaxAppendEntries:    100,
		MaxInflightRequests: 10,
		LeaseTimeout:        100 * time.Millisecond,
		PreVoteEnabled:      true,
		CheckQuorumEnabled:  true,
		BatchingEnabled:     true,
		BatchMaxSize:        100,
		BatchMaxWait:        5 * time.Millisecond,
	}
}

// RaftRole represents the role of a Raft node.
type RaftRole int

const (
	RaftRoleFollower RaftRole = iota
	RaftRoleCandidate
	RaftRoleLeader
	RaftRolePreCandidate // For pre-vote protocol
)

func (r RaftRole) String() string {
	switch r {
	case RaftRoleFollower:
		return "follower"
	case RaftRoleCandidate:
		return "candidate"
	case RaftRoleLeader:
		return "leader"
	case RaftRolePreCandidate:
		return "pre-candidate"
	default:
		return "unknown"
	}
}

// RaftLogEntryType identifies the type of log entry.
type RaftLogEntryType int

const (
	RaftLogCommand RaftLogEntryType = iota
	RaftLogConfiguration
	RaftLogNoop
	RaftLogBarrier // For linearizable reads
)

// RaftLogEntry represents a log entry in the Raft log.
type RaftLogEntry struct {
	Index uint64           `json:"index"`
	Term  uint64           `json:"term"`
	Type  RaftLogEntryType `json:"type"`
	Data  []byte           `json:"data"`
	CRC   uint32           `json:"crc"`
}

// Validate checks if the log entry is valid.
func (e *RaftLogEntry) Validate() bool {
	return crc32.ChecksumIEEE(e.Data) == e.CRC
}

// RaftCommand represents a command to be applied to the state machine.
type RaftCommand struct {
	Op    string          `json:"op"`
	Key   string          `json:"key"`
	Value json.RawMessage `json:"value"`
}

// RaftNode implements the Raft consensus algorithm.
type RaftNode struct {
	config RaftConfig
	store  StorageEngine

	// Persistent state (must be saved to stable storage)
	currentTerm uint64
	votedFor    string
	log         *RaftLog

	// Volatile state on all servers
	commitIndex uint64
	lastApplied uint64

	// Volatile state on leaders (reinitialized after election)
	nextIndex  map[string]uint64
	matchIndex map[string]uint64

	// Role management
	role     RaftRole
	leaderID string

	// Cluster membership
	peers   map[string]*RaftPeerState
	peersMu sync.RWMutex

	// Leader lease for linearizable reads
	leaseExpiry   time.Time
	leaseExtended atomic.Bool

	// Pending proposals
	proposals     map[uint64]*RaftProposal
	proposalsMu   sync.Mutex
	proposalIndex uint64

	// Request batching
	batchCh     chan *RaftProposal
	batchDoneCh chan struct{}

	// Communication
	transport *RaftTransport
	server    *http.Server

	// Timers
	electionTimer   *time.Timer
	heartbeatTicker *time.Ticker

	// Snapshots
	lastSnapshotIndex uint64
	lastSnapshotTerm  uint64
	snapshotting      atomic.Bool

	// Lifecycle
	ctx       context.Context
	cancel    context.CancelFunc
	wg        sync.WaitGroup
	running   atomic.Bool
	stopOnce  sync.Once
	stopErr   error
	stateMu   sync.RWMutex
	applyCh   chan *RaftLogEntry
	listeners []RaftEventListener
}

// RaftPeerState tracks replication state for a peer.
type RaftPeerState struct {
	ID         string
	Addr       string
	NextIndex  uint64
	MatchIndex uint64
	LastSeen   time.Time
	Healthy    bool
	Inflight   int32
}

// RaftProposal represents a pending proposal.
type RaftProposal struct {
	Index    uint64
	Term     uint64
	Command  []byte
	Response chan error
	Done     chan struct{}
}

// RaftEventListener receives Raft events.
type RaftEventListener interface {
	OnLeaderChange(leaderID string, term uint64)
	OnRoleChange(role RaftRole)
	OnCommit(index uint64)
	OnSnapshot(index uint64, term uint64)
}

// RaftLog manages the Raft log with persistence.
type RaftLog struct {
	entries    []*RaftLogEntry
	startIndex uint64
	mu         sync.RWMutex
	file       *os.File
	encoder    *gob.Encoder
}

// RaftTransport handles RPC communication.
type RaftTransport struct {
	localAddr string
	client    *http.Client
	timeout   time.Duration
}

// NewRaftNode creates a new Raft node.
func NewRaftNode(store StorageEngine, config RaftConfig) (*RaftNode, error) {
	if config.NodeID == "" {
		config.NodeID = fmt.Sprintf("raft-%d", time.Now().UnixNano()%10000)
	}
	if config.ElectionTimeoutMin <= 0 {
		config.ElectionTimeoutMin = 150 * time.Millisecond
	}
	if config.ElectionTimeoutMax <= 0 {
		config.ElectionTimeoutMax = 300 * time.Millisecond
	}
	if config.HeartbeatInterval <= 0 {
		config.HeartbeatInterval = 50 * time.Millisecond
	}

	ctx, cancel := context.WithCancel(context.Background())

	logPath := ""
	if config.DataDir != "" {
		logPath = filepath.Join(config.DataDir, "raft.log")
	}

	log, err := NewRaftLog(logPath)
	if err != nil {
		cancel()
		return nil, fmt.Errorf("failed to create raft log: %w", err)
	}

	rn := &RaftNode{
		config:      config,
		store:       store,
		currentTerm: 0,
		votedFor:    "",
		log:         log,
		commitIndex: 0,
		lastApplied: 0,
		role:        RaftRoleFollower,
		peers:       make(map[string]*RaftPeerState),
		nextIndex:   make(map[string]uint64),
		matchIndex:  make(map[string]uint64),
		proposals:   make(map[uint64]*RaftProposal),
		transport:   NewRaftTransport(config.AdvertiseAddr, config.ElectionTimeoutMin),
		ctx:         ctx,
		cancel:      cancel,
		applyCh:     make(chan *RaftLogEntry, 1000),
		batchCh:     make(chan *RaftProposal, config.BatchMaxSize),
		batchDoneCh: make(chan struct{}),
	}

	for _, peer := range config.Peers {
		rn.peers[peer.ID] = &RaftPeerState{
			ID:        peer.ID,
			Addr:      peer.Addr,
			NextIndex: 1,
			Healthy:   true,
		}
	}

	if err := rn.loadState(); err != nil {
		_ = log.Close()
		cancel()
		return nil, fmt.Errorf("failed to load state: %w", err)
	}

	return rn, nil
}

// Start begins Raft operations.
func (rn *RaftNode) Start() error {
	if rn.ctx.Err() != nil {
		return errors.New("raft node has been stopped")
	}
	if rn.running.Load() {
		return errors.New("raft node already running")
	}
	rn.running.Store(true)

	if rn.config.BindAddr != "" {
		mux := http.NewServeMux()
		mux.HandleFunc("/raft/vote", rn.handleRequestVote)
		mux.HandleFunc("/raft/prevote", rn.handlePreVote)
		mux.HandleFunc("/raft/append", rn.handleAppendEntries)
		mux.HandleFunc("/raft/snapshot", rn.handleInstallSnapshot)
		mux.HandleFunc("/raft/forward", rn.handleForwardProposal)

		rn.server = &http.Server{
			Addr:    rn.config.BindAddr,
			Handler: mux,
		}

		rn.wg.Add(1)
		go func(ctx context.Context) {
			defer rn.wg.Done()
			if err := rn.server.ListenAndServe(); err != http.ErrServerClosed {
				slog.Error("raft server error", "err", err)
			}
		}(rn.ctx)
	}

	rn.wg.Add(1)
	go rn.applyLoop()

	rn.resetElectionTimer()

	if rn.config.BatchingEnabled {
		rn.wg.Add(1)
		go rn.batchLoop()
	}

	if rn.config.CheckQuorumEnabled {
		rn.wg.Add(1)
		go rn.quorumCheckLoop()
	}

	if rn.config.SnapshotInterval > 0 {
		rn.wg.Add(1)
		go rn.snapshotLoop()
	}

	return nil
}

// Stop releases node resources, including a log opened before Start.
// A stopped node cannot be restarted; create a new node to reopen persisted state.
func (rn *RaftNode) Stop() error {
	rn.stopOnce.Do(func() {
		rn.stopErr = rn.stop()
	})
	return rn.stopErr
}

func (rn *RaftNode) stop() error {
	if !rn.running.Load() {
		rn.cancel()
		return rn.log.Close()
	}
	rn.running.Store(false)
	rn.cancel()

	if rn.electionTimer != nil {
		rn.electionTimer.Stop()
	}
	if rn.heartbeatTicker != nil {
		rn.heartbeatTicker.Stop()
	}

	close(rn.batchDoneCh)

	if rn.server != nil {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		if err := rn.server.Shutdown(ctx); err != nil {
			slog.Debug("shutdown error", "err", err)
		}
	}

	rn.wg.Wait()

	if err := rn.log.Close(); err != nil {
		return err
	}

	return rn.saveState()
}

// Propose submits a command to the Raft cluster.
func (rn *RaftNode) Propose(ctx context.Context, command []byte) error {
	if !rn.running.Load() {
		return errors.New("raft node not running")
	}

	rn.stateMu.RLock()
	role := rn.role
	leaderID := rn.leaderID
	rn.stateMu.RUnlock()

	if role != RaftRoleLeader {
		if leaderID == "" {
			return errors.New("no leader available")
		}
		return rn.forwardToLeader(ctx, command)
	}

	proposal := &RaftProposal{
		Command:  command,
		Response: make(chan error, 1),
		Done:     make(chan struct{}),
	}

	if rn.config.BatchingEnabled {
		select {
		case rn.batchCh <- proposal:
		case <-ctx.Done():
			return ctx.Err()
		case <-rn.ctx.Done():
			return errors.New("raft node stopped")
		}
	} else {
		if err := rn.appendProposal(proposal); err != nil {
			return err
		}
	}

	select {
	case err := <-proposal.Response:
		return err
	case <-ctx.Done():
		return ctx.Err()
	case <-rn.ctx.Done():
		return errors.New("raft node stopped")
	}
}

func (rn *RaftNode) appendProposal(proposal *RaftProposal) error {
	rn.stateMu.Lock()
	defer rn.stateMu.Unlock()

	if rn.role != RaftRoleLeader {
		return errors.New("not the leader")
	}

	entry := &RaftLogEntry{
		Index: rn.log.LastIndex() + 1,
		Term:  rn.currentTerm,
		Type:  RaftLogCommand,
		Data:  proposal.Command,
	}

	if err := rn.log.Append(entry); err != nil {
		return err
	}

	proposal.Index = entry.Index
	proposal.Term = entry.Term

	rn.proposalsMu.Lock()
	rn.proposals[entry.Index] = proposal
	rn.proposalsMu.Unlock()

	go rn.replicateToFollowers()

	return nil
}

func (rn *RaftNode) batchLoop() {
	defer rn.wg.Done()

	batch := make([]*RaftProposal, 0, rn.config.BatchMaxSize)
	timer := time.NewTimer(rn.config.BatchMaxWait)

	flush := func() {
		if len(batch) == 0 {
			return
		}

		rn.stateMu.Lock()
		if rn.role != RaftRoleLeader {
			rn.stateMu.Unlock()
			for _, p := range batch {
				p.Response <- errors.New("not the leader")
			}
			batch = batch[:0]
			return
		}

		baseIndex := rn.log.LastIndex() + 1
		entries := make([]*RaftLogEntry, len(batch))

		for i, p := range batch {
			entry := &RaftLogEntry{
				Index: baseIndex + uint64(i),
				Term:  rn.currentTerm,
				Type:  RaftLogCommand,
				Data:  p.Command,
			}
			entries[i] = entry
			p.Index = entry.Index
			p.Term = entry.Term
		}

		if err := rn.log.Append(entries...); err != nil {
			rn.stateMu.Unlock()
			for _, p := range batch {
				p.Response <- err
			}
			batch = batch[:0]
			return
		}

		rn.proposalsMu.Lock()
		for _, p := range batch {
			rn.proposals[p.Index] = p
		}
		rn.proposalsMu.Unlock()

		rn.stateMu.Unlock()

		go rn.replicateToFollowers()

		batch = batch[:0]
	}

	for {
		select {
		case <-rn.batchDoneCh:
			flush()
			return
		case <-rn.ctx.Done():
			flush()
			return
		case p := <-rn.batchCh:
			batch = append(batch, p)
			if len(batch) >= rn.config.BatchMaxSize {
				flush()
				timer.Reset(rn.config.BatchMaxWait)
			}
		case <-timer.C:
			flush()
			timer.Reset(rn.config.BatchMaxWait)
		}
	}
}

// LinearizableRead performs a linearizable read.
func (rn *RaftNode) LinearizableRead(ctx context.Context) error {
	rn.stateMu.RLock()
	role := rn.role
	leaseExpiry := rn.leaseExpiry
	rn.stateMu.RUnlock()

	if role != RaftRoleLeader {
		return errors.New("not the leader")
	}

	if time.Now().Before(leaseExpiry) && rn.leaseExtended.Load() {
		return nil
	}

	return rn.confirmLeadership(ctx)
}

func (rn *RaftNode) confirmLeadership(ctx context.Context) error {
	rn.peersMu.RLock()
	peers := make([]*RaftPeerState, 0, len(rn.peers))
	for _, p := range rn.peers {
		if p.Healthy {
			peers = append(peers, p)
		}
	}
	rn.peersMu.RUnlock()

	quorum := (len(peers)+1)/2 + 1
	responses := make(chan bool, len(peers))

	for _, peer := range peers {
		go func(p *RaftPeerState) {
			success := rn.sendHeartbeat(p)
			responses <- success
		}(peer)
	}

	successCount := 1
	timeout := time.After(rn.config.HeartbeatInterval * 2)

	for i := 0; i < len(peers); i++ {
		select {
		case success := <-responses:
			if success {
				successCount++
			}
		case <-timeout:

		case <-ctx.Done():
			return ctx.Err()
		}

		if successCount >= quorum {
			rn.stateMu.Lock()
			rn.leaseExpiry = time.Now().Add(rn.config.LeaseTimeout)
			rn.leaseExtended.Store(true)
			rn.stateMu.Unlock()
			return nil
		}
	}

	return errors.New("failed to confirm leadership")
}

// Role returns the current role.
func (rn *RaftNode) Role() RaftRole {
	rn.stateMu.RLock()
	defer rn.stateMu.RUnlock()
	return rn.role
}

// Leader returns the current leader ID.
func (rn *RaftNode) Leader() string {
	rn.stateMu.RLock()
	defer rn.stateMu.RUnlock()
	return rn.leaderID
}

// Term returns the current term.
func (rn *RaftNode) Term() uint64 {
	rn.stateMu.RLock()
	defer rn.stateMu.RUnlock()
	return rn.currentTerm
}

// IsLeader returns true if this node is the leader.
func (rn *RaftNode) IsLeader() bool {
	return rn.Role() == RaftRoleLeader
}

// Stats returns Raft statistics.
func (rn *RaftNode) Stats() RaftStats {
	rn.stateMu.RLock()
	role := rn.role
	term := rn.currentTerm
	leaderID := rn.leaderID
	commitIndex := rn.commitIndex
	lastApplied := rn.lastApplied
	rn.stateMu.RUnlock()

	rn.peersMu.RLock()
	peerCount := len(rn.peers)
	healthyPeers := 0
	for _, p := range rn.peers {
		if p.Healthy {
			healthyPeers++
		}
	}
	rn.peersMu.RUnlock()

	return RaftStats{
		NodeID:            rn.config.NodeID,
		Role:              role.String(),
		Term:              term,
		LeaderID:          leaderID,
		CommitIndex:       commitIndex,
		LastApplied:       lastApplied,
		LastLogIndex:      rn.log.LastIndex(),
		LastLogTerm:       rn.log.LastTerm(),
		LogLength:         rn.log.Len(),
		PeerCount:         peerCount,
		HealthyPeers:      healthyPeers,
		SnapshotIndex:     rn.lastSnapshotIndex,
		SnapshotTerm:      rn.lastSnapshotTerm,
		LeaseValid:        time.Now().Before(rn.leaseExpiry),
		PendingProposals:  len(rn.proposals),
		BatchingEnabled:   rn.config.BatchingEnabled,
		PreVoteEnabled:    rn.config.PreVoteEnabled,
		CheckQuorumActive: rn.config.CheckQuorumEnabled,
	}
}

// RaftStats contains Raft statistics.
type RaftStats struct {
	NodeID            string `json:"node_id"`
	Role              string `json:"role"`
	Term              uint64 `json:"term"`
	LeaderID          string `json:"leader_id"`
	CommitIndex       uint64 `json:"commit_index"`
	LastApplied       uint64 `json:"last_applied"`
	LastLogIndex      uint64 `json:"last_log_index"`
	LastLogTerm       uint64 `json:"last_log_term"`
	LogLength         int    `json:"log_length"`
	PeerCount         int    `json:"peer_count"`
	HealthyPeers      int    `json:"healthy_peers"`
	SnapshotIndex     uint64 `json:"snapshot_index"`
	SnapshotTerm      uint64 `json:"snapshot_term"`
	LeaseValid        bool   `json:"lease_valid"`
	PendingProposals  int    `json:"pending_proposals"`
	BatchingEnabled   bool   `json:"batching_enabled"`
	PreVoteEnabled    bool   `json:"pre_vote_enabled"`
	CheckQuorumActive bool   `json:"check_quorum_active"`
}

// RaftSnapshot represents a snapshot of the state machine.
type RaftSnapshot struct {
	LastIndex uint64 `json:"last_index"`
	LastTerm  uint64 `json:"last_term"`
	Data      []byte `json:"data"`
}

type persistentState struct {
	CurrentTerm uint64 `json:"current_term"`
	VotedFor    string `json:"voted_for"`
}

// PreVoteRPCRequest is a pre-vote request.
type PreVoteRPCRequest struct {
	Term         uint64 `json:"term"`
	CandidateID  string `json:"candidate_id"`
	LastLogIndex uint64 `json:"last_log_index"`
	LastLogTerm  uint64 `json:"last_log_term"`
}

// PreVoteRPCResponse is a pre-vote response.
type PreVoteRPCResponse struct {
	Term        uint64 `json:"term"`
	VoteGranted bool   `json:"vote_granted"`
}

// RequestVoteRPCRequest is a request vote request.
type RequestVoteRPCRequest struct {
	Term         uint64 `json:"term"`
	CandidateID  string `json:"candidate_id"`
	LastLogIndex uint64 `json:"last_log_index"`
	LastLogTerm  uint64 `json:"last_log_term"`
}

// RequestVoteRPCResponse is a request vote response.
type RequestVoteRPCResponse struct {
	Term        uint64 `json:"term"`
	VoteGranted bool   `json:"vote_granted"`
}

// AppendEntriesRPCRequest is an append entries request.
type AppendEntriesRPCRequest struct {
	Term         uint64          `json:"term"`
	LeaderID     string          `json:"leader_id"`
	PrevLogIndex uint64          `json:"prev_log_index"`
	PrevLogTerm  uint64          `json:"prev_log_term"`
	Entries      []*RaftLogEntry `json:"entries"`
	LeaderCommit uint64          `json:"leader_commit"`
}

// AppendEntriesRPCResponse is an append entries response.
type AppendEntriesRPCResponse struct {
	Term          uint64 `json:"term"`
	Success       bool   `json:"success"`
	ConflictIndex uint64 `json:"conflict_index,omitempty"`
	ConflictTerm  uint64 `json:"conflict_term,omitempty"`
}

// InstallSnapshotRPCRequest is an install snapshot request.
type InstallSnapshotRPCRequest struct {
	Term              uint64 `json:"term"`
	LeaderID          string `json:"leader_id"`
	LastIncludedIndex uint64 `json:"last_included_index"`
	LastIncludedTerm  uint64 `json:"last_included_term"`
	Data              []byte `json:"data"`
}

// InstallSnapshotRPCResponse is an install snapshot response.
type InstallSnapshotRPCResponse struct {
	Term uint64 `json:"term"`
}

// ForwardProposalRequest is a forward proposal request.
type ForwardProposalRequest struct {
	Command []byte `json:"command"`
}

// MembershipChangeType represents a membership change type.
type MembershipChangeType int

const (
	MembershipAddNode MembershipChangeType = iota
	MembershipRemoveNode
)

// MembershipChange represents a cluster membership change.
type MembershipChange struct {
	Type MembershipChangeType `json:"type"`
	Node RaftPeer             `json:"node"`
}

// RaftCluster wraps RaftNode for easier cluster management.
type RaftCluster struct {
	node  *RaftNode
	store StorageEngine
}
