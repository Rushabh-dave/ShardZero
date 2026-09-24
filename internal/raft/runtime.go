package raft

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"time"

	"github.com/Rushabh-dave/shardzero/internal/events"
	"github.com/Rushabh-dave/shardzero/internal/statemachine"
)

func EntryHash(e Entry) string {
	b, _ := json.Marshal(e)
	sum := sha256.Sum256(b)
	return hex.EncodeToString(sum[:])
}

// Runtime replaces the nondeterministic boundaries for a single-threaded runner.
// Manual nodes start no goroutines; the runner drives Tick, messages and tickets.
// Live nodes use the same protocol transition functions with real timers/HTTP.
type Runtime struct {
	Manual      bool
	Now         func() time.Time
	Int64N      func(int64) int64
	OpenJournal func(replay func([]byte) error) (Journal, error)
}
type Journal interface {
	Append([]byte) error
	Close() error
}
type Message struct {
	Sequence uint64         `json:"sequence"`
	From     string         `json:"from"`
	To       string         `json:"to"`
	Vote     *VoteRequest   `json:"vote,omitempty"`
	Append   *AppendRequest `json:"append,omitempty"`
}
type Ticket struct {
	Index uint64
	node  *Node
	done  chan outcome
}

func (t *Ticket) Poll() (statemachine.Result, error, bool) {
	select {
	case out := <-t.done:
		return out.result, out.err, true
	default:
		return statemachine.Result{}, nil, false
	}
}
func (t *Ticket) Cancel() {
	t.node.mu.Lock()
	defer t.node.mu.Unlock()
	if w, ok := t.node.pending[t.Index]; ok && w.done == t.done {
		delete(t.node.pending, t.Index)
	}
}

// Submit appends a proposal without blocking. Ticket completion is produced only
// by the ordinary commit/apply path; the simulator never applies commands itself.
func (n *Node) Submit(command *statemachine.Command, readKey *string) (*Ticket, error) {
	n.mu.Lock()
	defer n.mu.Unlock()
	return n.submitLocked(command, readKey)
}
func (n *Node) submitLocked(command *statemachine.Command, readKey *string) (*Ticket, error) {
	if !n.availableLocked() {
		return nil, ErrUnavailable
	}
	if n.role != "leader" {
		return nil, n.leaderErrorLocked()
	}
	if command != nil {
		if err := statemachine.Validate(*command); err != nil {
			return nil, err
		}
		c := *command
		command = &c
	}
	if readKey != nil {
		k := *readKey
		readKey = &k
	}
	if n.lastLocked()-n.store.state.commit >= 256 {
		return nil, ErrNoQuorum
	}
	entry := Entry{Index: n.lastLocked() + 1, Term: n.store.state.term, Command: command}
	if !n.persistLocked(n.store.state.term, n.store.state.vote, n.store.state.commit, entry.Index, []Entry{entry}) {
		return nil, ErrUnavailable
	}
	w := waiter{readKey: readKey, done: make(chan outcome, 1)}
	n.pending[entry.Index] = w
	n.match[n.cfg.ID] = entry.Index
	n.emitLocked("PROPOSED", entry.Index, "", "", "")
	n.advanceCommitLocked()
	n.kickLocked()
	return &Ticket{entry.Index, n, w.done}, nil
}
func (n *Node) Tick() {
	n.mu.Lock()
	defer n.mu.Unlock()
	if !n.availableLocked() {
		return
	}
	if n.role == "leader" {
		n.kickLocked()
	} else if n.cfg.FixedLeader == "" && !n.rt.Now().Before(n.deadline) {
		n.startElectionLocked()
	}
}
func (n *Node) DrainMessages() []Message {
	n.mu.Lock()
	defer n.mu.Unlock()
	out := n.outbox
	n.outbox = nil
	return out
}
func (n *Node) DeliverVote(m Message, response VoteResponse, err error) {
	n.mu.Lock()
	defer n.mu.Unlock()
	if m.Vote != nil {
		n.voteResponseLocked(n.peers[m.To], *m.Vote, response, err)
	}
}
func (n *Node) DeliverAppend(m Message, response AppendResponse, err error) {
	n.mu.Lock()
	defer n.mu.Unlock()
	if m.Append == nil || n.busy[m.To] != m.Sequence {
		return
	}
	delete(n.busy, m.To)
	if n.acceptAppendLocked(n.peers[m.To], *m.Append, response, err) {
		n.queueAppendLocked(n.peers[m.To])
	}
}
func (n *Node) queueAppendLocked(peer Peer) {
	if n.busy[peer.ID] != 0 || !n.availableLocked() || n.role != "leader" {
		return
	}
	req := n.prepareAppendLocked(peer)
	n.busy[peer.ID] = n.sequence
	n.outbox = append(n.outbox, Message{Sequence: n.sequence, From: n.cfg.ID, To: peer.ID, Append: &req})
}
func (n *Node) SetPaused(paused bool) {
	n.mu.Lock()
	defer n.mu.Unlock()
	n.paused = paused
	if paused {
		n.failPendingLocked(ErrUnavailable)
	} else {
		n.resetTimerLocked()
		n.kickLocked()
	}
	kind := "RESUMED"
	if paused {
		kind = "PAUSED"
	}
	n.emitLocked(kind, 0, "", "", "")
}
func (n *Node) Events(after uint64) []events.Event { return n.events.After(after) }
func (n *Node) emitLocked(kind string, index uint64, peer, message, detail string) {
	n.events.Publish(events.Event{Time: n.rt.Now(), NodeID: n.cfg.ID, Type: kind, Term: n.store.state.term, Index: index, Peer: peer, MessageID: message, Detail: detail})
}
func (n *Node) voteResponseLocked(peer Peer, req VoteRequest, resp VoteResponse, err error) {
	if !n.availableLocked() || err != nil {
		return
	}
	n.emitLocked("VOTE_RESPONSE", 0, peer.ID, req.MessageID, "")
	if resp.Term > n.store.state.term {
		n.stepDownLocked(resp.Term, "")
		return
	}
	if n.role != "candidate" || n.store.state.term != req.Term || resp.Term != req.Term {
		return
	}
	if resp.Granted {
		n.votes[peer.ID] = true
		if len(n.votes) > len(n.peers)/2 {
			n.becomeLeaderLocked()
		}
	}
}

// Audit is for local verification. Only Inspect's redacted metadata is sent over HTTP.
type Audit struct {
	Status    Status
	Vote      string
	Log       []Entry
	StateHash string
}

func (n *Node) Audit() Audit {
	n.mu.Lock()
	defer n.mu.Unlock()
	return Audit{n.statusLocked(), n.store.state.vote, cloneEntries(n.store.state.log), n.machine.Digest()}
}

type LogMeta struct {
	Index     uint64 `json:"index"`
	Term      uint64 `json:"term"`
	Operation string `json:"operation"`
	Key       string `json:"key,omitempty"`
	Hash      string `json:"hash"`
	Committed bool   `json:"committed"`
	Applied   bool   `json:"applied"`
}
type Inspection struct {
	Status
	BootID    string         `json:"bootId"`
	StateHash string         `json:"stateHash"`
	Log       []LogMeta      `json:"log"`
	Events    []events.Event `json:"events"`
}

func (n *Node) Inspect(after uint64) Inspection {
	n.mu.Lock()
	defer n.mu.Unlock()
	out := Inspection{Status: n.statusLocked(), BootID: n.bootID, StateHash: n.machine.Digest(), Log: make([]LogMeta, 0), Events: n.events.After(after)}
	start := max(uint64(1), n.lastLocked()+1-min(n.lastLocked(), 100))
	for i := start; i <= n.lastLocked(); i++ {
		e := n.entryLocked(i)
		m := LogMeta{Index: i, Term: e.Term, Operation: "BARRIER", Hash: EntryHash(e), Committed: i <= n.store.state.commit, Applied: i <= n.applied}
		if e.Command != nil {
			m.Operation = e.Command.Operation
			m.Key = e.Command.Key
		}
		out.Log = append(out.Log, m)
	}
	return out
}

var ErrInjectedDisk = errors.New("injected disk failure")
