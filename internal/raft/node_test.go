package raft

import (
	"context"
	"errors"
	"fmt"
	"strconv"
	"sync"
	"testing"
	"time"

	"github.com/Rushabh-dave/shardzero/internal/statemachine"
)

type testNetwork struct {
	mu      sync.Mutex
	nodes   map[string]*Node
	blocked map[string]bool
}
type testTransport struct {
	net  *testNetwork
	from string
}

func (t testTransport) target(ctx context.Context, id string) (*Node, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	t.net.mu.Lock()
	defer t.net.mu.Unlock()
	if t.net.blocked[t.from+"/"+id] {
		return nil, errors.New("partition")
	}
	n := t.net.nodes[id]
	if n == nil {
		return nil, errors.New("offline")
	}
	return n, nil
}
func (t testTransport) AppendEntries(ctx context.Context, p Peer, r AppendRequest) (AppendResponse, error) {
	n, err := t.target(ctx, p.ID)
	if err != nil {
		return AppendResponse{}, err
	}
	return n.AppendEntries(r)
}
func (t testTransport) RequestVote(ctx context.Context, p Peer, r VoteRequest) (VoteResponse, error) {
	n, err := t.target(ctx, p.ID)
	if err != nil {
		return VoteResponse{}, err
	}
	return n.RequestVote(r)
}

type testCluster struct {
	net     *testNetwork
	nodes   []*Node
	configs []Config
}

func newCluster(t testing.TB, count int, fixed bool) *testCluster {
	return newClusterConfigured(t, count, fixed, nil)
}
func newClusterConfigured(t testing.TB, count int, fixed bool, configure func(*Config)) *testCluster {
	t.Helper()
	c := &testCluster{net: &testNetwork{nodes: make(map[string]*Node), blocked: make(map[string]bool)}}
	var peers []Peer
	for i := 0; i < count; i++ {
		peers = append(peers, Peer{ID: fmt.Sprint(i), PeerURL: fmt.Sprintf("http://localhost:%d", 8000+i), ClientURL: fmt.Sprintf("http://localhost:%d", 7000+i)})
	}
	for i := 0; i < count; i++ {
		cfg := Config{ID: fmt.Sprint(i), ClusterID: "test", DataDir: t.TempDir(), Peers: peers, Heartbeat: 20 * time.Millisecond, ElectionMin: 200 * time.Millisecond, ElectionMax: 400 * time.Millisecond, RPCTimeout: 100 * time.Millisecond, RequestTimeout: 450 * time.Millisecond}
		if fixed {
			cfg.FixedLeader = "0"
		}
		if configure != nil {
			configure(&cfg)
		}
		c.configs = append(c.configs, cfg)
		c.nodes = append(c.nodes, nil)
		c.restart(t, i)
	}
	t.Cleanup(func() {
		for _, n := range c.nodes {
			if n != nil {
				_ = n.Close()
			}
		}
	})
	return c
}
func (c *testCluster) restart(t testing.TB, i int) {
	t.Helper()
	n, err := Open(c.configs[i], testTransport{c.net, fmt.Sprint(i)})
	if err != nil {
		t.Fatal(err)
	}
	c.nodes[i] = n
	c.net.mu.Lock()
	c.net.nodes[fmt.Sprint(i)] = n
	c.net.mu.Unlock()
}
func (c *testCluster) stop(i int) {
	c.net.mu.Lock()
	delete(c.net.nodes, fmt.Sprint(i))
	c.net.mu.Unlock()
	_ = c.nodes[i].Close()
	c.nodes[i] = nil
}
func (c *testCluster) isolate(i int) {
	c.net.mu.Lock()
	defer c.net.mu.Unlock()
	for j := range c.nodes {
		if i != j {
			c.net.blocked[fmt.Sprintf("%d/%d", i, j)] = true
			c.net.blocked[fmt.Sprintf("%d/%d", j, i)] = true
		}
	}
}
func (c *testCluster) heal() {
	c.net.mu.Lock()
	c.net.blocked = make(map[string]bool)
	c.net.mu.Unlock()
}
func eventually(t *testing.T, condition func() bool) {
	t.Helper()
	deadline := time.Now().Add(6 * time.Second)
	for time.Now().Before(deadline) {
		if condition() {
			return
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Fatal("condition did not become true")
}
func command(client string, id uint64, key, value string) statemachine.Command {
	return statemachine.Command{ClientID: client, RequestID: id, Operation: "SET", Key: key, Value: value}
}

func TestFixedLeaderMajorityAndRestart(t *testing.T) {
	c := newCluster(t, 3, true)
	n := c.nodes[0]
	if _, err := n.Execute(context.Background(), command("c", 1, "key", "one")); err != nil {
		t.Fatal(err)
	}
	c.stop(2)
	if _, err := n.Execute(context.Background(), command("c", 2, "key", "two")); err != nil {
		t.Fatal(err)
	}
	c.stop(1)
	if _, err := n.Execute(context.Background(), command("c", 3, "key", "uncertain")); !errors.Is(err, ErrNoQuorum) {
		t.Fatal("minority write", err)
	}
	if _, _, err := n.Get(context.Background(), "key"); !errors.Is(err, ErrNoQuorum) {
		t.Fatal("minority read", err)
	}
	c.restart(t, 1)
	c.restart(t, 2)
	if _, err := n.Execute(context.Background(), command("c", 4, "key", "final")); err != nil {
		t.Fatal(err)
	}
	eventually(t, func() bool {
		for _, node := range c.nodes {
			node.mu.Lock()
			v, ok := node.machine.Get("key")
			node.mu.Unlock()
			if !ok || v != "final" {
				return false
			}
		}
		return true
	})
	for i := range c.nodes {
		c.stop(i)
	}
	for i := range c.nodes {
		c.restart(t, i)
	}
	v, found, err := c.nodes[0].Get(context.Background(), "key")
	if err != nil || !found || v != "final" {
		t.Fatal(v, found, err)
	}
}

func TestSnapshotCompactsFullyReplicatedPrefixAndRestoresRetries(t *testing.T) {
	c := newClusterConfigured(t, 3, true, func(cfg *Config) { cfg.SnapshotEvery = 2 })
	for i := uint64(1); i <= 4; i++ {
		if _, err := c.nodes[0].Execute(context.Background(), command("writer", i, "score", strconv.FormatUint(i, 10))); err != nil {
			t.Fatal(err)
		}
	}
	eventually(t, func() bool { return c.nodes[0].Status().SnapshotIndex >= 2 })
	if c.nodes[0].Status().SnapshotIndex == 0 {
		t.Fatal("leader did not compact")
	}
	for i := range c.nodes {
		c.stop(i)
	}
	for i := range c.nodes {
		c.restart(t, i)
	}
	eventually(t, func() bool {
		for _, n := range c.nodes {
			v, ok := n.machine.Get("score")
			if !ok || v != "4" {
				return false
			}
		}
		return true
	})
	// The latest request identity is restored with the checkpointed machine.
	r, err := c.nodes[0].Execute(context.Background(), command("writer", 4, "score", "4"))
	if err != nil || !r.Applied || r.Value != "4" {
		t.Fatal(r, err)
	}
}

func TestOnlyCommittedEntriesReplay(t *testing.T) {
	c := newCluster(t, 3, true)
	c.stop(1)
	c.stop(2)
	_, _ = c.nodes[0].Execute(context.Background(), command("c", 1, "never-committed", "v"))
	c.stop(0)
	c.restart(t, 0)
	c.nodes[0].mu.Lock()
	_, exists := c.nodes[0].machine.Get("never-committed")
	c.nodes[0].mu.Unlock()
	if exists {
		t.Fatal("replayed an uncommitted write")
	}
}

func (c *testCluster) leader(t *testing.T, excluded int) int {
	t.Helper()
	found := -1
	eventually(t, func() bool {
		count := 0
		for i, n := range c.nodes {
			if i != excluded && n != nil && n.Status().Role == "leader" {
				found = i
				count++
			}
		}
		return count == 1
	})
	return found
}

func TestElectionAndRepeatedLeaderCrash(t *testing.T) {
	c := newCluster(t, 3, false)
	for round := 1; round <= 4; round++ {
		leader := c.leader(t, -1)
		if _, err := c.nodes[leader].Execute(context.Background(), command("writer", uint64(round), "value", fmt.Sprint(round))); err != nil {
			t.Fatal(err)
		}
		c.stop(leader)
		newLeader := c.leader(t, -1)
		value, found, err := c.nodes[newLeader].Get(context.Background(), "value")
		if err != nil || !found || value != fmt.Sprint(round) {
			t.Fatal(value, found, err)
		}
		c.restart(t, leader)
		eventually(t, func() bool { return c.nodes[leader].Status().CommitIndex >= c.nodes[newLeader].Status().CommitIndex })
	}
}

func TestVoteSurvivesRestartAndLogFreshness(t *testing.T) {
	c := newCluster(t, 3, false)
	c.stop(0)
	c.stop(1)
	c.stop(2)
	// Avoid timer activity while examining exact vote rules.
	c.configs[0].ElectionMin = time.Hour
	c.configs[0].ElectionMax = 2 * time.Hour
	c.restart(t, 0)
	req := VoteRequest{ClusterID: "test", Term: 10, CandidateID: "1"}
	r, err := c.nodes[0].RequestVote(req)
	if err != nil || !r.Granted {
		t.Fatal(r, err)
	}
	c.stop(0)
	c.restart(t, 0)
	req.CandidateID = "2"
	r, err = c.nodes[0].RequestVote(req)
	if err != nil || r.Granted {
		t.Fatal("voted twice", r, err)
	}
	entry := Entry{Index: 1, Term: 10, Command: ptrCommand(command("x", 1, "key", "value"))}
	_, err = c.nodes[0].AppendEntries(AppendRequest{ClusterID: "test", Term: 10, LeaderID: "1", Entries: []Entry{entry}, LeaderCommit: 1})
	if err != nil {
		t.Fatal(err)
	}
	req.Term = 11
	r, err = c.nodes[0].RequestVote(req)
	if err != nil || r.Granted || r.Term != 11 {
		t.Fatal("stale candidate won", r, err)
	}
	req.LastLogIndex = 1
	req.LastLogTerm = 10
	r, err = c.nodes[0].RequestVote(req)
	if err != nil || !r.Granted {
		t.Fatal(r, err)
	}
}
func ptrCommand(c statemachine.Command) *statemachine.Command { return &c }

func TestPartitionRepairsDivergentSuffixAndRejectsOldLeader(t *testing.T) {
	c := newCluster(t, 3, false)
	old := c.leader(t, -1)
	first := command("writer", 1, "saved", "yes")
	if _, err := c.nodes[old].Execute(context.Background(), first); err != nil {
		t.Fatal(err)
	}
	c.isolate(old)
	if _, err := c.nodes[old].Execute(context.Background(), command("orphan", 1, "orphan", "must-disappear")); !errors.Is(err, ErrNoQuorum) {
		t.Fatal(err)
	}
	newLeader := c.leader(t, old)
	if _, err := c.nodes[newLeader].Execute(context.Background(), command("writer", 2, "saved", "new")); err != nil {
		t.Fatal(err)
	}
	if _, _, err := c.nodes[old].Get(context.Background(), "saved"); !errors.Is(err, ErrNoQuorum) {
		t.Fatal("stale read allowed", err)
	}
	c.heal()
	eventually(t, func() bool {
		for _, node := range c.nodes {
			node.mu.Lock()
			v, ok := node.machine.Get("saved")
			_, orphan := node.machine.Get("orphan")
			node.mu.Unlock()
			if !ok || v != "new" || orphan {
				return false
			}
		}
		return c.nodes[old].Status().Role == "follower"
	})
	for i := range c.nodes {
		c.stop(i)
	}
	for i := range c.nodes {
		c.restart(t, i)
	}
	leader := c.leader(t, -1)
	v, ok, err := c.nodes[leader].Get(context.Background(), "saved")
	if err != nil || !ok || v != "new" {
		t.Fatal(v, ok, err)
	}
	_, ok, err = c.nodes[leader].Get(context.Background(), "orphan")
	if err != nil || ok {
		t.Fatal("orphan restored after restart", ok, err)
	}
}

func TestCommitRequiresCurrentTerm(t *testing.T) {
	c := newCluster(t, 3, false)
	for i := range c.nodes {
		c.stop(i)
	}
	c.configs[0].ElectionMin = time.Hour
	c.configs[0].ElectionMax = 2 * time.Hour
	c.restart(t, 0)
	n := c.nodes[0]
	n.mu.Lock()
	defer n.mu.Unlock()
	entries := []Entry{{Index: 1, Term: 1}, {Index: 2, Term: 1}}
	if !n.persistLocked(2, "0", 0, 1, entries) {
		t.Fatal(n.failed)
	}
	n.role = "leader"
	n.leader = "0"
	n.match["1"] = 2
	n.match["2"] = 2
	n.advanceCommitLocked()
	if n.store.state.commit != 0 {
		t.Fatal("committed previous term by replica count alone")
	}
	if !n.persistLocked(2, "0", 0, 3, []Entry{{Index: 3, Term: 2}}) {
		t.Fatal(n.failed)
	}
	n.match["1"] = 3
	n.advanceCommitLocked()
	if n.store.state.commit != 3 || n.applied != 3 {
		t.Fatal("current term did not commit inherited prefix")
	}
}

func TestHeartbeatDoesNotCommitOrDeleteUnmatchedSuffix(t *testing.T) {
	c := newCluster(t, 3, false)
	for i := range c.nodes {
		c.stop(i)
	}
	c.configs[0].ElectionMin = time.Hour
	c.configs[0].ElectionMax = 2 * time.Hour
	c.restart(t, 0)
	n := c.nodes[0]
	r, err := n.AppendEntries(AppendRequest{ClusterID: "test", Term: 1, LeaderID: "1", Entries: []Entry{{Index: 1, Term: 1}, {Index: 2, Term: 1, Command: ptrCommand(command("c", 1, "k", "old"))}}})
	if err != nil || !r.Success {
		t.Fatal(r, err)
	}
	r, err = n.AppendEntries(AppendRequest{ClusterID: "test", Term: 2, LeaderID: "2", PrevLogIndex: 1, PrevLogTerm: 1, LeaderCommit: 2})
	if err != nil || !r.Success {
		t.Fatal(r, err)
	}
	if s := n.Status(); s.CommitIndex != 1 || s.LastLogIndex != 2 {
		t.Fatal("heartbeat trusted unverified suffix", s)
	}
	r, err = n.AppendEntries(AppendRequest{ClusterID: "test", Term: 2, LeaderID: "2", PrevLogIndex: 1, PrevLogTerm: 1, Entries: []Entry{{Index: 2, Term: 2}}, LeaderCommit: 2})
	if err != nil || !r.Success {
		t.Fatal(r, err)
	}
	_, err = n.AppendEntries(AppendRequest{ClusterID: "test", Term: 3, LeaderID: "1", Entries: []Entry{{Index: 1, Term: 3}}})
	if err == nil {
		t.Fatal("committed prefix replacement accepted")
	}
	if n.Status().CommitIndex != 2 {
		t.Fatal("commit regressed")
	}
}

func TestConcurrentCASHistoryAndRetryAfterElection(t *testing.T) {
	c := newClusterConfigured(t, 3, false, func(cfg *Config) {
		cfg.RequestTimeout = 3 * time.Second
		cfg.ElectionMin = 600 * time.Millisecond
		cfg.ElectionMax = 1100 * time.Millisecond
	})
	leader := c.leader(t, -1)
	n := c.nodes[leader]
	if _, err := n.Execute(context.Background(), command("init", 1, "counter", "0")); err != nil {
		t.Fatal(err)
	}
	var wg sync.WaitGroup
	for i := 0; i < 12; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			id := uint64(1)
			for successes := 0; successes < 4; {
				v, ok, err := n.Get(context.Background(), "counter")
				if err != nil || !ok {
					t.Error(v, ok, err)
					return
				}
				number, err := strconv.Atoi(v)
				if err != nil {
					t.Error(err)
					return
				}
				cmd := statemachine.Command{ClientID: fmt.Sprintf("concurrent-%d", i), RequestID: id, Operation: "CAS", Key: "counter", Expected: v, Value: strconv.Itoa(number + 1)}
				r, err := n.Execute(context.Background(), cmd)
				if err != nil {
					t.Error(err)
					return
				}
				id++
				if r.Applied {
					successes++
				}
			}
		}(i)
	}
	wg.Wait()
	v, ok, err := n.Get(context.Background(), "counter")
	if err != nil || !ok || v != "48" {
		t.Fatal("lost or duplicated update", v, ok, err)
	}
	cas := statemachine.Command{ClientID: "retry", RequestID: 1, Operation: "CAS", Key: "counter", Expected: "48", Value: "49"}
	want, err := n.Execute(context.Background(), cas)
	if err != nil || !want.Applied {
		t.Fatal(want, err)
	}
	c.stop(leader)
	newLeader := c.leader(t, -1)
	got, err := c.nodes[newLeader].Execute(context.Background(), cas)
	if err != nil || got != want {
		t.Fatal("retry after election", got, err)
	}
	v, _, err = c.nodes[newLeader].Get(context.Background(), "counter")
	if err != nil || v != "49" {
		t.Fatal(v, err)
	}
}

type failingJournal struct{ journal }

func (f failingJournal) Append([]byte) error { return errors.New("injected disk failure") }
func TestDiskFailureCannotGrantVoteOrAcknowledgeAppend(t *testing.T) {
	for _, operation := range []string{"vote", "append"} {
		t.Run(operation, func(t *testing.T) {
			c := newCluster(t, 3, false)
			for i := range c.nodes {
				c.stop(i)
			}
			c.configs[0].ElectionMin = time.Hour
			c.configs[0].ElectionMax = 2 * time.Hour
			c.restart(t, 0)
			n := c.nodes[0]
			n.mu.Lock()
			n.store.wal = failingJournal{n.store.wal}
			n.mu.Unlock()
			if operation == "vote" {
				r, err := n.RequestVote(VoteRequest{ClusterID: "test", Term: 1, CandidateID: "1"})
				if err == nil || r.Granted {
					t.Fatal(r, err)
				}
			} else {
				r, err := n.AppendEntries(AppendRequest{ClusterID: "test", Term: 1, LeaderID: "1", Entries: []Entry{{Index: 1, Term: 1}}})
				if err == nil || r.Success {
					t.Fatal(r, err)
				}
			}
			if n.Status().Healthy {
				t.Fatal("disk failure did not stop node")
			}
		})
	}
}
