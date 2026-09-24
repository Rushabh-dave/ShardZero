// Package simulation drives the production Raft transition functions from a
// single virtual-time priority queue. It uses no network, sleeps or goroutines.
package simulation

import (
	"container/heap"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"hash"
	"math/rand/v2"
	"sort"
	"time"

	"github.com/Rushabh-dave/shardzero/internal/raft"
	"github.com/Rushabh-dave/shardzero/internal/statemachine"
)

type Fault struct {
	AtMS  int64  `json:"atMs"`
	Kind  string `json:"kind"`
	Node  int    `json:"node"`
	Other int    `json:"other,omitempty"`
}
type Scenario struct {
	Version          int     `json:"version"`
	Seed             uint64  `json:"seed"`
	Nodes            int     `json:"nodes"`
	Operations       int     `json:"operations"`
	MaxDelayMS       int     `json:"maxDelayMs"`
	DropPercent      int     `json:"dropPercent"`
	DuplicatePercent int     `json:"duplicatePercent"`
	Faults           []Fault `json:"faults"`
}
type Trace struct {
	AtMS   int64  `json:"atMs"`
	Type   string `json:"type"`
	Node   string `json:"node,omitempty"`
	Detail string `json:"detail,omitempty"`
}
type Result struct {
	Scenario     Scenario `json:"scenario"`
	Passed       bool     `json:"passed"`
	Error        string   `json:"error,omitempty"`
	Steps        int      `json:"steps"`
	VirtualMS    int64    `json:"virtualMs"`
	Acknowledged int      `json:"acknowledged"`
	Pending      int      `json:"pending"`
	Converged    bool     `json:"converged"`
	TraceHash    string   `json:"traceHash"`
	Checks       []string `json:"checks"`
	Trace        []Trace  `json:"trace"`
}

func Default(seed uint64, nodes, operations int) Scenario {
	span := int64(max(operations*60, 2400))
	return Scenario{Version: 1, Seed: seed, Nodes: nodes, Operations: operations, MaxDelayMS: 80, DropPercent: 8, DuplicatePercent: 5, Faults: []Fault{
		{AtMS: 1000, Kind: "crash", Node: 0}, {AtMS: 1400, Kind: "restart", Node: 0},
		{AtMS: 1500 + span/5, Kind: "partition", Node: 1}, {AtMS: 2100 + span/5, Kind: "heal"},
		{AtMS: 2300 + span/4, Kind: "pause", Node: 2}, {AtMS: 2600 + span/4, Kind: "resume", Node: 2},
		{AtMS: 2800 + span/3, Kind: "disk-after", Node: 0}, {AtMS: 3400 + span/3, Kind: "restart", Node: 0},
	}}
}
func (s Scenario) Validate() error {
	if s.Version != 1 || s.Nodes < 3 || s.Nodes > 5 || s.Operations < 1 || s.Operations > 10000 || s.MaxDelayMS < 0 || s.MaxDelayMS > 2000 || s.DropPercent < 0 || s.DropPercent > 100 || s.DuplicatePercent < 0 || s.DuplicatePercent > 100 || len(s.Faults) > 100 {
		return errors.New("invalid scenario bounds")
	}
	for _, f := range s.Faults {
		if f.AtMS < 0 || f.AtMS > 3600000 || f.Node < 0 || f.Node >= s.Nodes {
			return errors.New("invalid fault position")
		}
		switch f.Kind {
		case "crash", "restart", "pause", "resume", "partition", "heal", "disk-before", "disk-after":
		default:
			return fmt.Errorf("unknown fault %q", f.Kind)
		}
	}
	return nil
}

type item struct {
	at   int64
	seq  uint64
	kind string
	run  func()
}
type queue []item

func (q queue) Len() int { return len(q) }
func (q queue) Less(i, j int) bool {
	if q[i].at == q[j].at {
		return q[i].seq < q[j].seq
	}
	return q[i].at < q[j].at
}
func (q queue) Swap(i, j int) { q[i], q[j] = q[j], q[i] }
func (q *queue) Push(x any)   { *q = append(*q, x.(item)) }
func (q *queue) Pop() any     { old := *q; x := old[len(old)-1]; *q = old[:len(old)-1]; return x }

type memoryDisk struct {
	records [][]byte
	failure string
}

func (d *memoryDisk) Open(replay func([]byte) error) (raft.Journal, error) {
	for _, b := range d.records {
		if err := replay(append([]byte(nil), b...)); err != nil {
			return nil, err
		}
	}
	return d, nil
}
func (d *memoryDisk) Append(b []byte) error {
	if d.failure == "before" {
		return raft.ErrInjectedDisk
	}
	d.records = append(d.records, append([]byte(nil), b...))
	if d.failure == "after" {
		return raft.ErrInjectedDisk
	}
	return nil
}
func (d *memoryDisk) Close() error { return nil }

type operation struct {
	command statemachine.Command
	ticket  *raft.Ticket
	source  *raft.Node
	done    bool
	retryAt int64
}
type runner struct {
	s               Scenario
	now             int64
	seq             uint64
	q               queue
	random          *rand.Rand
	nodes           []*raft.Node
	disks           []*memoryDisk
	peers           []raft.Peer
	isolated        map[int]bool
	drop            int
	delay           int
	traceHash       hash.Hash
	result          Result
	err             error
	recoverAt       int64
	ops             []*operation
	leaders         map[uint64]string
	votes           map[string]string
	committed       map[uint64]string
	committedTerms  map[uint64]uint64
	logMatching     map[string]string
	previousCommit  []uint64
	previousApplied []uint64
	eventCursor     []uint64
}

func Run(ctx context.Context, s Scenario) Result {
	r := &runner{s: s, random: rand.New(rand.NewPCG(s.Seed, s.Seed^0x853c49e6748fea9b)), isolated: make(map[int]bool), drop: s.DropPercent, delay: s.MaxDelayMS, traceHash: sha256.New(), leaders: make(map[uint64]string), votes: make(map[string]string), committed: make(map[uint64]string)}
	r.logMatching = make(map[string]string)
	r.committedTerms = make(map[uint64]uint64)
	r.result = Result{Scenario: s, Checks: []string{"one leader per term", "one vote per term", "log matching", "committed prefix preserved", "commit/apply monotonicity", "acknowledgement after commit", "state convergence after healing"}, Trace: make([]Trace, 0)}
	if err := s.Validate(); err != nil {
		r.result.Error = err.Error()
		return r.result
	}
	for i := 0; i < s.Nodes; i++ {
		r.peers = append(r.peers, raft.Peer{ID: fmt.Sprintf("node-%d", i+1), PeerURL: fmt.Sprintf("http://peer:%d", 8000+i), ClientURL: fmt.Sprintf("http://client:%d", 7000+i)})
		r.nodes = append(r.nodes, nil)
		r.disks = append(r.disks, &memoryDisk{})
		r.previousCommit = append(r.previousCommit, 0)
		r.previousApplied = append(r.previousApplied, 0)
		r.eventCursor = append(r.eventCursor, 0)
	}
	defer func() {
		for _, n := range r.nodes {
			if n != nil {
				_ = n.Close()
			}
		}
	}()
	for i := range r.nodes {
		r.start(i)
	}
	r.recoverAt = int64(s.Operations*60 + 1800)
	for _, fault := range s.Faults {
		f := fault
		r.recoverAt = max(r.recoverAt, f.AtMS+1000)
		r.schedule(f.AtMS, "fault", func() { r.fault(f) })
	}
	for i := 0; i < s.Operations; i++ {
		op := &operation{command: statemachine.Command{ClientID: fmt.Sprintf("client-%d", i), RequestID: 1, Operation: "SET", Key: fmt.Sprintf("key-%d", i%7), Value: fmt.Sprintf("value-%d", i)}}
		op.retryAt = 800 + int64(i*60)
		r.ops = append(r.ops, op)
		r.schedule(800+int64(i*60), "client", func() { op.retryAt = r.now; r.try(op) })
	}
	r.schedule(r.recoverAt, "recover-all", func() {
		r.isolated = make(map[int]bool)
		r.drop = 0
		r.delay = 5
		for i, n := range r.nodes {
			r.disks[i].failure = ""
			// Remove a pause before checking disk health. A paused node can
			// also have a poisoned journal, which still requires replay.
			wasPaused := n != nil && n.Status().Paused
			if wasPaused {
				n.SetPaused(false)
			}
			if n == nil || !n.Status().Healthy {
				r.start(i)
			} else if !wasPaused {
				n.SetPaused(false)
			}
		}
		r.record("HEAL_ALL", "", "")
	})
	var tick func()
	tick = func() {
		for _, n := range r.nodes {
			if n != nil {
				n.Tick()
			}
		}
		for _, op := range r.ops {
			if !op.done && op.ticket == nil && op.retryAt <= r.now && r.now >= 800 {
				r.try(op)
			}
		}
		r.schedule(r.now+50, "tick", tick)
	}
	r.schedule(50, "tick", tick)
	limit := r.recoverAt + max(int64(12000), int64(s.Operations*10))
	for len(r.q) > 0 && r.err == nil && r.result.Steps < 2000000 {
		if err := ctx.Err(); err != nil {
			r.err = err
			break
		}
		e := heap.Pop(&r.q).(item)
		if e.at > limit {
			break
		}
		r.now = e.at
		r.result.Steps++
		r.record("SCHEDULE", "", fmt.Sprintf("%d:%s", e.seq, e.kind))
		e.run()
		r.collect()
		r.drain()
		r.check()
		if r.now > r.recoverAt+1500 && r.result.Acknowledged == s.Operations && r.converged() {
			r.result.Converged = true
			break
		}
	}
	r.result.Pending = s.Operations - r.result.Acknowledged
	r.result.VirtualMS = r.now
	r.result.TraceHash = hex.EncodeToString(r.traceHash.Sum(nil))
	r.result.Passed = r.err == nil && r.result.Converged && r.result.Pending == 0
	if r.err != nil {
		r.result.Error = r.err.Error()
	} else if !r.result.Passed {
		r.result.Error = "cluster did not finish and converge after healing"
	}
	return r.result
}
func (r *runner) schedule(at int64, kind string, fn func()) {
	r.seq++
	heap.Push(&r.q, item{at, r.seq, kind, fn})
}
func (r *runner) record(kind, node, detail string) {
	e := Trace{r.now, kind, node, detail}
	b, _ := json.Marshal(e)
	_, _ = r.traceHash.Write(b)
	_, _ = r.traceHash.Write([]byte{'\n'})
	if len(r.result.Trace) < 600 {
		r.result.Trace = append(r.result.Trace, e)
	}
}
func (r *runner) start(i int) {
	if r.nodes[i] != nil {
		_ = r.nodes[i].Close()
	}
	r.disks[i].failure = ""
	seed := r.random.Uint64()
	random := rand.New(rand.NewPCG(seed, seed^uint64(i+1)))
	n, err := raft.OpenWithRuntime(raft.Config{ID: r.peers[i].ID, ClusterID: "simulation", Peers: r.peers, ElectionMin: 400 * time.Millisecond, ElectionMax: 800 * time.Millisecond, Heartbeat: 50 * time.Millisecond, RPCTimeout: 300 * time.Millisecond}, nil, raft.Runtime{Manual: true, Now: func() time.Time { return time.UnixMilli(r.now).UTC() }, Int64N: random.Int64N, OpenJournal: r.disks[i].Open})
	if err != nil {
		r.err = err
		return
	}
	r.nodes[i] = n
	r.eventCursor[i] = 0
	r.record("RESTART", r.peers[i].ID, "")
}
func (r *runner) fault(f Fault) {
	r.record("FAULT", r.peers[f.Node].ID, f.Kind)
	switch f.Kind {
	case "crash":
		if r.nodes[f.Node] != nil {
			_ = r.nodes[f.Node].Close()
			r.nodes[f.Node] = nil
		}
	case "restart":
		r.start(f.Node)
	case "pause":
		if n := r.nodes[f.Node]; n != nil {
			n.SetPaused(true)
		}
	case "resume":
		if n := r.nodes[f.Node]; n != nil {
			n.SetPaused(false)
		}
	case "partition":
		r.isolated[f.Node] = true
	case "heal":
		r.isolated = make(map[int]bool)
	case "disk-before":
		r.disks[f.Node].failure = "before"
	case "disk-after":
		r.disks[f.Node].failure = "after"
	}
}
func (r *runner) try(op *operation) {
	if op.done || op.ticket != nil {
		return
	}
	op.retryAt = r.now + 150
	best := -1
	var term uint64
	for i, n := range r.nodes {
		if n == nil {
			continue
		}
		s := n.Status()
		if s.Healthy && s.Role == "leader" && (best < 0 || s.Term > term) {
			best = i
			term = s.Term
		}
	}
	if best < 0 {
		return
	}
	ticket, err := r.nodes[best].Submit(&op.command, nil)
	if err != nil {
		return
	}
	op.ticket = ticket
	op.source = r.nodes[best]
	r.record("CLIENT_SUBMIT", r.peers[best].ID, op.command.ClientID)
	r.schedule(r.now+1200, "client-timeout", func() {
		if op.ticket == ticket {
			ticket.Cancel()
			op.ticket = nil
			op.retryAt = r.now + 50
			r.record("CLIENT_TIMEOUT", r.peers[best].ID, op.command.ClientID)
		}
	})
}
func (r *runner) collect() {
	for _, op := range r.ops {
		if op.ticket == nil {
			continue
		}
		result, err, done := op.ticket.Poll()
		if !done {
			continue
		}
		if err == nil {
			a := op.source.Audit()
			if a.Status.CommitIndex < op.ticket.Index || !result.Applied {
				r.err = errors.New("acknowledgement before commit or wrong SET result")
				return
			}
			op.done = true
			r.result.Acknowledged++
			r.record("CLIENT_ACK", a.Status.NodeID, op.command.ClientID)
		}
		op.ticket = nil
		op.retryAt = r.now + 100
	}
}
func (r *runner) drain() {
	for i, n := range r.nodes {
		if n == nil {
			continue
		}
		for _, m := range n.DrainMessages() {
			to := -1
			for j, p := range r.peers {
				if p.ID == m.To {
					to = j
				}
			}
			if to >= 0 {
				r.send(i, to, n, m)
			}
		}
	}
}
func (r *runner) blocked(from, to int) bool {
	return from != to && (r.isolated[from] || r.isolated[to])
}
func (r *runner) delayMS() int64 { return int64(1 + r.random.IntN(r.delay+1)) }
func (r *runner) send(from, to int, source *raft.Node, m raft.Message) {
	completed := false
	finish := func(v raft.VoteResponse, a raft.AppendResponse, err error) {
		if completed || r.nodes[from] != source {
			return
		}
		completed = true
		if m.Vote != nil {
			source.DeliverVote(m, v, err)
		} else {
			source.DeliverAppend(m, a, err)
		}
	}
	r.schedule(r.now+300, "rpc-timeout", func() { finish(raft.VoteResponse{}, raft.AppendResponse{}, errors.New("virtual RPC timeout")) })
	if r.blocked(from, to) || r.random.IntN(100) < r.drop {
		r.record("MESSAGE_DROPPED", m.From, fmt.Sprintf("%s/%d", m.To, m.Sequence))
		return
	}
	deliver := func() {
		if r.blocked(from, to) || r.nodes[to] == nil {
			return
		}
		r.record("MESSAGE_DELIVERED", m.From, fmt.Sprintf("%s/%d", m.To, m.Sequence))
		var v raft.VoteResponse
		var a raft.AppendResponse
		var err error
		if m.Vote != nil {
			v, err = r.nodes[to].RequestVote(*m.Vote)
		} else {
			a, err = r.nodes[to].AppendEntries(*m.Append)
		}
		if r.random.IntN(100) < r.drop {
			return
		}
		r.schedule(r.now+r.delayMS(), "response", func() {
			if !r.blocked(to, from) {
				finish(v, a, err)
			}
		})
	}
	r.schedule(r.now+r.delayMS(), "message", deliver)
	if r.random.IntN(100) < r.s.DuplicatePercent {
		r.schedule(r.now+r.delayMS(), "duplicate-message", deliver)
	}
}
func (r *runner) check() {
	for i, n := range r.nodes {
		if n == nil {
			continue
		}
		a := n.Audit()
		s := a.Status
		for _, e := range n.Events(r.eventCursor[i]) {
			r.eventCursor[i] = e.ID
			r.record(e.Type, e.NodeID, fmt.Sprintf("%d/%d/%s/%s", e.Term, e.Index, e.Peer, e.MessageID))
		}
		if s.CommitIndex < r.previousCommit[i] || s.LastApplied < r.previousApplied[i] || s.LastApplied > s.CommitIndex {
			r.err = errors.New("commit/apply monotonicity violated")
			return
		}
		r.previousCommit[i] = s.CommitIndex
		r.previousApplied[i] = s.LastApplied
		if a.Vote != "" {
			key := fmt.Sprintf("%s/%d", s.NodeID, s.Term)
			if v := r.votes[key]; v != "" && v != a.Vote {
				r.err = errors.New("two votes in one term")
				return
			}
			r.votes[key] = a.Vote
		}
		if s.Role == "leader" {
			if id := r.leaders[s.Term]; id != "" && id != s.NodeID {
				r.err = errors.New("two leaders in one term")
				return
			}
			r.leaders[s.Term] = s.NodeID
		}
		prefix := ""
		for _, entry := range a.Log[1:] {
			h := raft.EntryHash(entry)
			chain := sha256.Sum256([]byte(prefix + h))
			prefix = hex.EncodeToString(chain[:])
			key := fmt.Sprintf("%d/%d", entry.Index, entry.Term)
			if old := r.logMatching[key]; old != "" && old != prefix {
				r.err = errors.New("log matching violated")
				return
			}
			r.logMatching[key] = prefix
			if entry.Index <= s.CommitIndex {
				if old := r.committed[entry.Index]; old != "" && old != h {
					r.err = errors.New("committed prefix changed")
					return
				}
				r.committed[entry.Index] = h
				// Leader completeness applies to entries committed before a
				// leader's term, not to the entry's own (possibly older) term.
				if _, seen := r.committedTerms[entry.Index]; !seen {
					r.committedTerms[entry.Index] = s.Term
				}
			}
		}
		if s.Role == "leader" {
			for index, h := range r.committed {
				if s.Term >= r.committedTerms[index] && (index >= uint64(len(a.Log)) || raft.EntryHash(a.Log[index]) != h) {
					r.err = fmt.Errorf("future leader lost committed prefix at index %d on %s", index, s.NodeID)
					return
				}
			}
		}
	}
}
func (r *runner) converged() bool {
	var digest string
	var index uint64
	for i, n := range r.nodes {
		if n == nil {
			return false
		}
		a := n.Audit()
		if !a.Status.Healthy || a.Status.CommitIndex != a.Status.LastLogIndex {
			return false
		}
		if i == 0 {
			digest = a.StateHash
			index = a.Status.LastApplied
		} else if digest != a.StateHash || index != a.Status.LastApplied {
			return false
		}
	}
	return true
}

// Minimize removes fault actions while preserving the same failure. This finds
// a single-deletion-minimal schedule within the run budget, not a global minimum.
func Minimize(ctx context.Context, s Scenario, fails func(Scenario) bool) Scenario {
	s.Faults = append([]Fault(nil), s.Faults...)
	if !fails(s) {
		return s
	}
	sort.SliceStable(s.Faults, func(i, j int) bool { return s.Faults[i].AtMS < s.Faults[j].AtMS })
	for i, attempts := 0, 0; i < len(s.Faults) && attempts < 100 && ctx.Err() == nil; {
		candidate := s
		candidate.Faults = append(append([]Fault{}, s.Faults[:i]...), s.Faults[i+1:]...)
		attempts++
		if fails(candidate) {
			s = candidate
			i = 0
		} else {
			i++
		}
	}
	return s
}
