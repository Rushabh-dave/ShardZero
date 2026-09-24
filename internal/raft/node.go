package raft

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"math/rand/v2"
	"net/url"
	"sort"
	"sync"
	"time"

	"github.com/Rushabh-dave/shardzero/internal/events"
	"github.com/Rushabh-dave/shardzero/internal/statemachine"
)

type outcome struct {
	result statemachine.Result
	err    error
}
type waiter struct {
	readKey *string
	done    chan outcome
}

type Node struct {
	mu        sync.Mutex
	cfg       Config
	peers     map[string]Peer
	store     *diskStore
	machine   *statemachine.Machine
	transport Transport
	role      string
	leader    string
	applied   uint64
	next      map[string]uint64
	match     map[string]uint64
	wake      map[string]chan struct{}
	pending   map[uint64]waiter
	deadline  time.Time
	failed    error
	closed    bool
	ctx       context.Context
	cancel    context.CancelFunc
	wg        sync.WaitGroup
	rt        Runtime
	outbox    []Message
	busy      map[string]uint64
	votes     map[string]bool
	sequence  uint64
	paused    bool
	events    *events.Buffer
	bootID    string
}

func Open(cfg Config, transport Transport) (*Node, error) {
	return OpenWithRuntime(cfg, transport, Runtime{})
}
func OpenWithRuntime(cfg Config, transport Transport, rt Runtime) (*Node, error) {
	if rt.Now == nil {
		rt.Now = time.Now
	}
	if rt.Int64N == nil {
		rt.Int64N = rand.Int64N
	}
	if cfg.ID == "" || cfg.ClusterID == "" || transport == nil && !rt.Manual {
		return nil, errors.New("node ID, cluster ID, and transport required")
	}
	if cfg.Heartbeat == 0 {
		cfg.Heartbeat = 100 * time.Millisecond
	}
	if cfg.ElectionMin == 0 {
		cfg.ElectionMin = 600 * time.Millisecond
	}
	if cfg.ElectionMax == 0 {
		cfg.ElectionMax = 1100 * time.Millisecond
	}
	if cfg.RPCTimeout == 0 {
		cfg.RPCTimeout = 300 * time.Millisecond
	}
	if cfg.RequestTimeout == 0 {
		cfg.RequestTimeout = 3 * time.Second
	}
	if cfg.Heartbeat <= 0 || cfg.ElectionMin <= 2*cfg.Heartbeat || cfg.ElectionMax <= cfg.ElectionMin || cfg.RPCTimeout <= 0 || cfg.RequestTimeout <= 0 {
		return nil, errors.New("invalid timeouts")
	}
	peers := make(map[string]Peer)
	addresses := make(map[string]bool)
	ids := make([]string, 0, len(cfg.Peers))
	for _, p := range cfg.Peers {
		if p.ID == "" {
			return nil, errors.New("empty peer ID")
		}
		if _, exists := peers[p.ID]; exists {
			return nil, errors.New("duplicate peer ID")
		}
		for _, address := range []string{p.PeerURL, p.ClientURL} {
			u, err := url.Parse(address)
			if err != nil || (u.Scheme != "http" && u.Scheme != "https") || u.Host == "" || u.User != nil || u.RawQuery != "" || u.Fragment != "" || u.Path != "" {
				return nil, fmt.Errorf("invalid peer URL %q (use an origin without a trailing slash)", address)
			}
			if addresses[address] {
				return nil, fmt.Errorf("each peer and client endpoint must be unique: %s", address)
			}
			addresses[address] = true
		}
		peers[p.ID] = p
		ids = append(ids, p.ID)
	}
	if _, ok := peers[cfg.ID]; !ok {
		return nil, errors.New("membership must include this node")
	}
	if cfg.FixedLeader != "" {
		if _, ok := peers[cfg.FixedLeader]; !ok {
			return nil, errors.New("fixed leader is not a member")
		}
	}
	sort.Strings(ids)
	identity, _ := json.Marshal([]any{"raft-v1", cfg.ClusterID, cfg.ID, ids, cfg.FixedLeader})
	hash := sha256.Sum256(identity)
	var store *diskStore
	var err error
	if rt.OpenJournal != nil {
		store, err = openStoreUsing(hex.EncodeToString(hash[:]), rt.OpenJournal)
	} else {
		store, err = openStore(cfg.DataDir, hex.EncodeToString(hash[:]))
	}
	if err != nil {
		return nil, err
	}
	cfg.Peers = append([]Peer(nil), cfg.Peers...)
	ctx, cancel := context.WithCancel(context.Background())
	n := &Node{cfg: cfg, peers: peers, store: store, machine: statemachine.New(), transport: transport, role: "follower", next: make(map[string]uint64), match: make(map[string]uint64), wake: make(map[string]chan struct{}), pending: make(map[uint64]waiter), ctx: ctx, cancel: cancel}
	n.rt = rt
	n.busy = make(map[string]uint64)
	n.events = events.New(512)
	n.bootID = fmt.Sprintf("%s-%d", cfg.ID, rt.Now().UnixNano())
	if store.state.base != 0 {
		// The checkpoint represents application through base. The remaining
		// committed suffix is replayed below.
		var checkpointMachine statemachine.Snapshot
		// The durable checkpoint is retained in the sentinel only indirectly;
		// openStore has already validated it, so rebuild from its record state.
		// snapshot is restored by the store accessor before Open returns.
		checkpointMachine = store.snapshot
		if err := n.machine.Restore(checkpointMachine); err != nil {
			cancel()
			_ = store.wal.Close()
			return nil, err
		}
		n.applied = store.state.base
	}
	n.applyLocked()
	n.resetTimerLocked()
	if cfg.FixedLeader == cfg.ID {
		if !n.persistLocked(store.state.term+1, cfg.ID, store.state.commit, 0, nil) {
			cancel()
			_ = store.wal.Close()
			return nil, n.failed
		}
		n.becomeLeaderLocked()
	}
	if n.failed != nil {
		cancel()
		_ = store.wal.Close()
		return nil, n.failed
	}
	if rt.Manual {
		return n, nil
	}
	for _, p := range cfg.Peers {
		if p.ID != cfg.ID {
			ch := make(chan struct{}, 1)
			n.wake[p.ID] = ch
			n.wg.Add(1)
			go n.replicator(p, ch)
		}
	}
	n.wg.Add(1)
	go n.run()
	return n, nil
}

func (n *Node) lastLocked() uint64             { return n.store.last() }
func (n *Node) entryLocked(index uint64) Entry { return n.store.entry(index) }
func (n *Node) resetTimerLocked() {
	n.deadline = n.rt.Now().Add(n.cfg.ElectionMin + time.Duration(n.rt.Int64N(int64(n.cfg.ElectionMax-n.cfg.ElectionMin))))
}
func (n *Node) leaderErrorLocked() error {
	return &NotLeaderError{n.leader, n.peers[n.leader].ClientURL}
}
func (n *Node) availableLocked() bool { return !n.closed && !n.paused && n.failed == nil }
func (n *Node) kickLocked() {
	if n.rt.Manual {
		for _, p := range n.cfg.Peers {
			if p.ID != n.cfg.ID {
				n.queueAppendLocked(p)
			}
		}
		return
	}
	for _, ch := range n.wake {
		select {
		case ch <- struct{}{}:
		default:
		}
	}
}

func (n *Node) failPendingLocked(err error) {
	for index, w := range n.pending {
		w.done <- outcome{err: err}
		delete(n.pending, index)
	}
}

func (n *Node) persistLocked(term uint64, vote string, commit, from uint64, entries []Entry) bool {
	if err := n.store.save(term, vote, commit, from, entries); err != nil {
		if !n.rt.Manual {
			slog.Error("raft storage failed", "nodeId", n.cfg.ID, "error", err)
		}
		n.failed = err
		n.emitLocked("DISK_ERROR", 0, "", "", "")
		n.role = "follower"
		n.leader = ""
		n.failPendingLocked(ErrUnavailable)
		return false
	}
	return true
}

func (n *Node) stepDownLocked(term uint64, leader string) bool {
	changed := n.role != "follower" || term > n.store.state.term
	if term > n.store.state.term && !n.persistLocked(term, "", n.store.state.commit, 0, nil) {
		return false
	}
	n.role = "follower"
	n.leader = leader
	if changed {
		if !n.rt.Manual {
			slog.Info("raft stepped down", "nodeId", n.cfg.ID, "term", n.store.state.term, "leaderId", leader)
		}
		n.emitLocked("STEPPED_DOWN", 0, leader, "", "")
	}
	n.failPendingLocked(n.leaderErrorLocked())
	return true
}

func (n *Node) becomeLeaderLocked() {
	n.role = "leader"
	n.leader = n.cfg.ID
	if !n.rt.Manual {
		slog.Info("raft leader elected", "nodeId", n.cfg.ID, "term", n.store.state.term)
	}
	n.emitLocked("LEADER_ELECTED", 0, "", "", "")
	for id := range n.peers {
		n.next[id] = n.lastLocked() + 1
		n.match[id] = 0
	}
	// Committing an entry in this term establishes the inherited committed prefix.
	entry := Entry{Index: n.lastLocked() + 1, Term: n.store.state.term}
	if !n.persistLocked(n.store.state.term, n.store.state.vote, n.store.state.commit, entry.Index, []Entry{entry}) {
		return
	}
	n.match[n.cfg.ID] = entry.Index
	n.advanceCommitLocked()
	n.kickLocked()
}

func (n *Node) advanceCommitLocked() {
	if n.role != "leader" || !n.availableLocked() {
		return
	}
	for index := n.lastLocked(); index > n.store.state.commit; index-- {
		if n.entryLocked(index).Term != n.store.state.term {
			continue
		}
		votes := 1
		for id := range n.peers {
			if id != n.cfg.ID && n.match[id] >= index {
				votes++
			}
		}
		if votes > len(n.peers)/2 {
			if n.persistLocked(n.store.state.term, n.store.state.vote, index, 0, nil) {
				n.emitLocked("COMMITTED", index, "", "", "")
				n.applyLocked()
				n.kickLocked()
			}
			return
		}
	}
}

func (n *Node) applyLocked() {
	for n.applied < n.store.state.commit {
		n.applied++
		entry := n.entryLocked(n.applied)
		n.emitLocked("APPLIED", n.applied, "", "", "")
		var out outcome
		if entry.Command != nil {
			out.result, out.err = n.machine.Apply(*entry.Command)
		}
		if w, ok := n.pending[n.applied]; ok {
			if w.readKey != nil {
				out.result.Value, out.result.Found = n.machine.Get(*w.readKey)
			}
			w.done <- out
			delete(n.pending, n.applied)
		}
	}
	n.maybeSnapshotLocked()
}

func (n *Node) maybeSnapshotLocked() {
	if n.cfg.SnapshotEvery == 0 || n.store.state.commit-n.store.state.base < n.cfg.SnapshotEvery || n.role != "leader" {
		return
	}
	// All fixed members must have the prefix. This avoids requiring snapshot
	// transfer for a member that was offline while the prefix was removed.
	for id := range n.peers {
		if id != n.cfg.ID && n.match[id] < n.store.state.commit {
			return
		}
	}
	if err := n.store.checkpoint(n.machine.Snapshot(), n.store.state.commit); err != nil {
		n.failed = err
		n.role = "follower"
		n.leader = ""
		n.failPendingLocked(ErrUnavailable)
		n.emitLocked("DISK_ERROR", 0, "", "", "")
		return
	}
	n.emitLocked("SNAPSHOT_CREATED", n.store.state.base, "", "", "")
}

func (n *Node) Execute(ctx context.Context, command statemachine.Command) (statemachine.Result, error) {
	if err := statemachine.Validate(command); err != nil {
		return statemachine.Result{}, err
	}
	return n.propose(ctx, &command, nil)
}

func (n *Node) Get(ctx context.Context, key string) (string, bool, error) {
	if key == "" || len(key) > 1024 {
		return "", false, statemachine.ErrInvalid
	}
	r, err := n.propose(ctx, nil, &key)
	return r.Value, r.Found, err
}

// Every GET is an ordered no-op barrier. Even cached duplicate writes go through
// quorum commit, so a disconnected old leader cannot serve a stale result.
func (n *Node) propose(parent context.Context, command *statemachine.Command, readKey *string) (statemachine.Result, error) {
	ctx, cancel := context.WithTimeout(parent, n.cfg.RequestTimeout)
	defer cancel()
	n.mu.Lock()
	if err := ctx.Err(); err != nil {
		n.mu.Unlock()
		return statemachine.Result{}, err
	}
	ticket, err := n.submitLocked(command, readKey)
	n.mu.Unlock()
	if err != nil {
		return statemachine.Result{}, err
	}
	select {
	case out := <-ticket.done:
		return out.result, out.err
	case <-ctx.Done():
		ticket.Cancel()
		return statemachine.Result{}, errors.Join(ErrNoQuorum, ctx.Err())
	}
}

func (n *Node) Status() Status {
	n.mu.Lock()
	defer n.mu.Unlock()
	return n.statusLocked()
}
func (n *Node) statusLocked() Status {
	keys, operations := n.machine.Stats()
	mode := "raft"
	if n.cfg.FixedLeader != "" {
		mode = "fixed-leader"
	}
	return Status{NodeID: n.cfg.ID, Mode: mode, Role: n.role, Term: n.store.state.term, LeaderID: n.leader, LeaderURL: n.peers[n.leader].ClientURL, LastLogIndex: n.lastLocked(), CommitIndex: n.store.state.commit, LastApplied: n.applied, SnapshotIndex: n.store.state.base, Keys: keys, AppliedOperations: operations, Healthy: n.availableLocked(), Paused: n.paused, Peers: append([]Peer(nil), n.cfg.Peers...)}
}

func (n *Node) Close() error {
	n.mu.Lock()
	if n.closed {
		n.mu.Unlock()
		return nil
	}
	n.closed = true
	n.cancel()
	n.failPendingLocked(ErrUnavailable)
	n.mu.Unlock()
	n.wg.Wait()
	return n.store.wal.Close()
}

func (n *Node) run() {
	defer n.wg.Done()
	tick := time.NewTicker(n.cfg.Heartbeat)
	defer tick.Stop()
	for {
		select {
		case <-n.ctx.Done():
			return
		case <-tick.C:
			n.Tick()
		}
	}
}

func (n *Node) startElectionLocked() {
	term := n.store.state.term + 1
	if !n.persistLocked(term, n.cfg.ID, n.store.state.commit, 0, nil) {
		return
	}
	n.role = "candidate"
	n.leader = ""
	n.resetTimerLocked()
	n.votes = map[string]bool{n.cfg.ID: true}
	n.emitLocked("ELECTION_STARTED", 0, "", "", "")
	if len(n.votes) > len(n.peers)/2 {
		n.becomeLeaderLocked()
		return
	}
	req := VoteRequest{ClusterID: n.cfg.ClusterID, Term: term, CandidateID: n.cfg.ID, LastLogIndex: n.lastLocked(), LastLogTerm: n.entryLocked(n.lastLocked()).Term}
	for _, peer := range n.cfg.Peers {
		if peer.ID != n.cfg.ID {
			n.sequence++
			request := req
			request.MessageID = fmt.Sprintf("%s/%d", n.bootID, n.sequence)
			n.emitLocked("VOTE_SENT", 0, peer.ID, request.MessageID, "")
			if n.rt.Manual {
				n.outbox = append(n.outbox, Message{Sequence: n.sequence, From: n.cfg.ID, To: peer.ID, Vote: &request})
				continue
			}
			n.wg.Add(1)
			go func(peer Peer, req VoteRequest) {
				defer n.wg.Done()
				ctx, cancel := context.WithTimeout(n.ctx, n.cfg.RPCTimeout)
				defer cancel()
				resp, err := n.transport.RequestVote(ctx, peer, req)
				n.mu.Lock()
				defer n.mu.Unlock()
				n.voteResponseLocked(peer, req, resp, err)
			}(peer, request)
		}
	}
}
