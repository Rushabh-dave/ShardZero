// Package raft implements a fixed-membership Raft group over durable journals.
package raft

import (
	"context"
	"errors"
	"time"

	"github.com/Rushabh-dave/shardzero/internal/statemachine"
)

var (
	ErrUnavailable = errors.New("node unavailable")
	ErrNoQuorum    = errors.New("quorum confirmation timed out; write outcome may be unknown")
)

type NotLeaderError struct{ LeaderID, LeaderURL string }

func (e *NotLeaderError) Error() string { return "request must be sent to the current leader" }

type Peer struct {
	ID        string `json:"id"`
	PeerURL   string `json:"peerUrl"`
	ClientURL string `json:"clientUrl"`
}

type Config struct {
	ID             string
	ClusterID      string
	DataDir        string
	Peers          []Peer // Includes this node. Membership cannot change after initialization.
	FixedLeader    string // Phase 3 teaching mode: disables elections on every node.
	ElectionMin    time.Duration
	ElectionMax    time.Duration
	Heartbeat      time.Duration
	RPCTimeout     time.Duration
	RequestTimeout time.Duration
	// SnapshotEvery compacts a fully replicated committed prefix after this
	// many entries. Zero keeps snapshots disabled (useful for protocol traces).
	SnapshotEvery uint64
}

type Entry struct {
	Index   uint64                `json:"index"`
	Term    uint64                `json:"term"`
	Command *statemachine.Command `json:"command,omitempty"` // nil means a barrier/no-op.
}

type VoteRequest struct {
	MessageID    string `json:"messageId,omitempty"`
	ClusterID    string `json:"clusterId"`
	Term         uint64 `json:"term"`
	CandidateID  string `json:"candidateId"`
	LastLogIndex uint64 `json:"lastLogIndex"`
	LastLogTerm  uint64 `json:"lastLogTerm"`
}
type VoteResponse struct {
	Term    uint64 `json:"term"`
	Granted bool   `json:"voteGranted"`
}

type AppendRequest struct {
	MessageID    string  `json:"messageId,omitempty"`
	ClusterID    string  `json:"clusterId"`
	Term         uint64  `json:"term"`
	LeaderID     string  `json:"leaderId"`
	PrevLogIndex uint64  `json:"prevLogIndex"`
	PrevLogTerm  uint64  `json:"prevLogTerm"`
	Entries      []Entry `json:"entries"`
	LeaderCommit uint64  `json:"leaderCommit"`
}
type AppendResponse struct {
	Term          uint64 `json:"term"`
	Success       bool   `json:"success"`
	MatchIndex    uint64 `json:"matchIndex"`
	ConflictIndex uint64 `json:"conflictIndex"`
}

type Transport interface {
	RequestVote(context.Context, Peer, VoteRequest) (VoteResponse, error)
	AppendEntries(context.Context, Peer, AppendRequest) (AppendResponse, error)
}

type Status struct {
	NodeID            string `json:"nodeId"`
	Mode              string `json:"mode"`
	Role              string `json:"role"`
	Term              uint64 `json:"term"`
	LeaderID          string `json:"leaderId"`
	LeaderURL         string `json:"leaderUrl"`
	LastLogIndex      uint64 `json:"lastLogIndex"`
	CommitIndex       uint64 `json:"commitIndex"`
	LastApplied       uint64 `json:"lastApplied"`
	Keys              int    `json:"keys"`
	AppliedOperations uint64 `json:"appliedOperations"`
	SnapshotIndex     uint64 `json:"snapshotIndex"`
	Healthy           bool   `json:"healthy"`
	Paused            bool   `json:"paused"`
	Peers             []Peer `json:"peers"`
}
