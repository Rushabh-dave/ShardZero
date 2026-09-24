package raft

import (
	"context"
	"errors"
	"fmt"
	"reflect"
)

func (n *Node) member(id, cluster string) bool {
	_, ok := n.peers[id]
	return ok && id != n.cfg.ID && cluster == n.cfg.ClusterID
}

func (n *Node) AppendEntries(req AppendRequest) (AppendResponse, error) {
	n.mu.Lock()
	defer n.mu.Unlock()
	response := func() AppendResponse {
		return AppendResponse{Term: n.store.state.term, ConflictIndex: n.lastLocked() + 1}
	}
	if !n.availableLocked() {
		return response(), ErrUnavailable
	}
	if !n.member(req.LeaderID, req.ClusterID) || n.cfg.FixedLeader != "" && req.LeaderID != n.cfg.FixedLeader {
		return response(), errors.New("unknown leader or cluster")
	}
	n.emitLocked("APPEND_RECEIVED", req.PrevLogIndex, req.LeaderID, req.MessageID, "")
	if req.Term < n.store.state.term {
		return response(), nil
	}
	if !n.stepDownLocked(req.Term, req.LeaderID) {
		return response(), ErrUnavailable
	}
	n.resetTimerLocked()
	if req.PrevLogIndex > n.lastLocked() {
		return response(), nil
	}
	if req.PrevLogIndex < n.store.state.base {
		r := response()
		r.ConflictIndex = n.store.state.base + 1
		return r, nil
	}
	if n.entryLocked(req.PrevLogIndex).Term != req.PrevLogTerm {
		conflict := req.PrevLogIndex
		for conflict > n.store.state.base && n.entryLocked(conflict-1).Term == n.entryLocked(req.PrevLogIndex).Term {
			conflict--
		}
		r := response()
		r.ConflictIndex = conflict
		return r, nil
	}
	var from uint64
	var entries []Entry
	for i, e := range req.Entries {
		if e.Index != req.PrevLogIndex+1+uint64(i) || e.Term > req.Term || e.Term == 0 {
			return response(), errors.New("malformed log entry")
		}
		if e.Index <= n.store.state.base {
			continue
		}
		if e.Index > n.lastLocked() || n.entryLocked(e.Index).Term != e.Term {
			from = e.Index
			entries = req.Entries[i:]
			break
		}
		if !reflect.DeepEqual(e, n.entryLocked(e.Index)) {
			return response(), errors.New("same index and term with different command")
		}
	}
	matched := req.PrevLogIndex + uint64(len(req.Entries))
	commit := n.store.state.commit
	if req.LeaderCommit > commit {
		commit = min(req.LeaderCommit, matched)
		commit = max(commit, n.store.state.commit)
	}
	if from != 0 && from <= n.store.state.commit {
		return response(), errors.New("attempt to overwrite committed entry")
	}
	if from != 0 || commit != n.store.state.commit {
		if from != 0 && from <= n.lastLocked() {
			n.emitLocked("LOG_REPAIRED", from, req.LeaderID, req.MessageID, "")
		}
		if !n.persistLocked(n.store.state.term, n.store.state.vote, commit, from, entries) {
			return response(), ErrUnavailable
		}
		n.applyLocked()
	}
	return AppendResponse{Term: n.store.state.term, Success: true, MatchIndex: matched}, nil
}

func (n *Node) RequestVote(req VoteRequest) (VoteResponse, error) {
	n.mu.Lock()
	defer n.mu.Unlock()
	response := func(granted bool) VoteResponse { return VoteResponse{Term: n.store.state.term, Granted: granted} }
	if !n.availableLocked() {
		return response(false), ErrUnavailable
	}
	if !n.member(req.CandidateID, req.ClusterID) {
		return response(false), errors.New("unknown candidate or cluster")
	}
	n.emitLocked("VOTE_RECEIVED", 0, req.CandidateID, req.MessageID, "")
	if n.cfg.FixedLeader != "" {
		return response(false), nil
	}
	if req.Term < n.store.state.term {
		return response(false), nil
	}
	if req.Term > n.store.state.term && !n.stepDownLocked(req.Term, "") {
		return response(false), ErrUnavailable
	}
	last := n.entryLocked(n.lastLocked())
	upToDate := req.LastLogTerm > last.Term || req.LastLogTerm == last.Term && req.LastLogIndex >= last.Index
	if upToDate && (n.store.state.vote == "" || n.store.state.vote == req.CandidateID) {
		if n.store.state.vote == "" && !n.persistLocked(req.Term, req.CandidateID, n.store.state.commit, 0, nil) {
			return response(false), ErrUnavailable
		}
		n.resetTimerLocked()
		n.emitLocked("VOTE_GRANTED", 0, req.CandidateID, req.MessageID, "")
		return response(true), nil
	}
	return response(false), nil
}

func (n *Node) replicator(peer Peer, wake <-chan struct{}) {
	defer n.wg.Done()
	for {
		select {
		case <-n.ctx.Done():
			return
		case <-wake:
			for n.replicateOnce(peer) {
				select {
				case <-n.ctx.Done():
					return
				default:
				}
			}
		}
	}
}

func (n *Node) replicateOnce(peer Peer) bool {
	n.mu.Lock()
	if !n.availableLocked() || n.role != "leader" {
		n.mu.Unlock()
		return false
	}
	req := n.prepareAppendLocked(peer)
	n.mu.Unlock()
	ctx, cancel := context.WithTimeout(n.ctx, n.cfg.RPCTimeout)
	resp, err := n.transport.AppendEntries(ctx, peer, req)
	cancel()
	n.mu.Lock()
	defer n.mu.Unlock()
	return n.acceptAppendLocked(peer, req, resp, err)
}

func (n *Node) prepareAppendLocked(peer Peer) AppendRequest {
	next := max(uint64(1), n.next[peer.ID])
	next = min(next, n.lastLocked()+1)
	// Batch small commands, accounting conservatively for sixfold JSON escaping.
	end := next
	bytes := 1024
	for end <= n.lastLocked() && end-next < 32 {
		size := 1024
		if c := n.entryLocked(end).Command; c != nil {
			size += 6 * (len(c.Key) + len(c.Value) + len(c.Expected) + len(c.ClientID))
		}
		if bytes+size > 15<<20 {
			break
		}
		bytes += size
		end++
	}
	prev := next - 1
	if prev < n.store.state.base {
		prev = n.store.state.base
		next = prev + 1
	}
	entries := make([]Entry, 0, end-next)
	for i := next; i < end; i++ {
		entries = append(entries, n.entryLocked(i))
	}
	req := AppendRequest{ClusterID: n.cfg.ClusterID, Term: n.store.state.term, LeaderID: n.cfg.ID, PrevLogIndex: prev, PrevLogTerm: n.entryLocked(prev).Term, Entries: cloneEntries(entries), LeaderCommit: n.store.state.commit}
	n.sequence++
	req.MessageID = fmt.Sprintf("%s/%d", n.bootID, n.sequence)
	n.emitLocked("APPEND_SENT", req.PrevLogIndex+uint64(len(req.Entries)), peer.ID, req.MessageID, "")
	return req
}

func (n *Node) acceptAppendLocked(peer Peer, req AppendRequest, resp AppendResponse, err error) bool {
	if !n.availableLocked() {
		return false
	}
	if err == nil {
		n.emitLocked("APPEND_RESPONSE", resp.MatchIndex, peer.ID, req.MessageID, "")
	} else {
		n.emitLocked("RPC_FAILED", 0, peer.ID, req.MessageID, "")
	}
	if err == nil && resp.Term > n.store.state.term {
		n.stepDownLocked(resp.Term, "")
		return false
	}
	if err != nil || n.role != "leader" || n.store.state.term != req.Term || resp.Term != req.Term {
		return false
	}
	if resp.Success {
		matched := req.PrevLogIndex + uint64(len(req.Entries))
		n.match[peer.ID] = max(n.match[peer.ID], matched)
		n.next[peer.ID] = n.match[peer.ID] + 1
		n.advanceCommitLocked()
		n.maybeSnapshotLocked()
		return n.next[peer.ID] <= n.lastLocked()
	}
	next := req.PrevLogIndex + 1
	if next <= 1 {
		return false
	}
	n.next[peer.ID] = max(uint64(1), min(next-1, resp.ConflictIndex))
	return true
}
