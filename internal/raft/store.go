package raft

import (
	"encoding/json"
	"errors"
	"fmt"

	"github.com/Rushabh-dave/shardzero/internal/statemachine"
	"github.com/Rushabh-dave/shardzero/internal/storage"
)

// A journal transaction records hard state, optional suffix replacement, and
// commit progress atomically. No committed prefix can be replaced. Unlike the
// standalone journal, merely persisting an entry does not apply it.
type record struct {
	Identity   string      `json:"identity"`
	Term       uint64      `json:"term"`
	Vote       string      `json:"votedFor"`
	Commit     uint64      `json:"commitIndex"`
	From       uint64      `json:"replaceFrom,omitempty"`
	Entries    []Entry     `json:"entries,omitempty"`
	Checkpoint *checkpoint `json:"checkpoint,omitempty"`
}

type checkpoint struct {
	Index    uint64                `json:"index"`
	Term     uint64                `json:"term"`
	Machine  statemachine.Snapshot `json:"machine"`
	RaftTerm uint64                `json:"raftTerm"`
	Vote     string                `json:"votedFor"`
	Commit   uint64                `json:"commitIndex"`
	Log      []Entry               `json:"log"`
}

type durableState struct {
	term   uint64
	vote   string
	commit uint64
	base   uint64
	log    []Entry
}
type journal interface {
	Append([]byte) error
	Close() error
}
type diskStore struct {
	wal      journal
	identity string
	state    durableState
	snapshot statemachine.Snapshot
}

func openStore(dir, identity string) (*diskStore, error) {
	return openStoreUsing(identity, func(replay func([]byte) error) (Journal, error) { return storage.OpenJournal(dir, "raft.wal", replay) })
}
func openStoreUsing(identity string, opener func(func([]byte) error) (Journal, error)) (*diskStore, error) {
	s := &diskStore{identity: identity, state: durableState{log: []Entry{{}}}}
	seen := false
	w, err := opener(func(b []byte) error {
		var r record
		if err := json.Unmarshal(b, &r); err != nil {
			return err
		}
		if err := s.validate(r); err != nil {
			return err
		}
		s.accept(r)
		seen = true
		return nil
	})
	if err != nil {
		return nil, err
	}
	s.wal = w
	if !seen {
		if err := s.save(0, "", 0, 0, nil); err != nil {
			_ = w.Close()
			return nil, err
		}
	}
	return s, nil
}

func (s *diskStore) validate(r record) error {
	if r.Checkpoint != nil {
		c := r.Checkpoint
		if r.Identity != s.identity || r.Term != 0 || r.Vote != "" || r.Commit != 0 || r.From != 0 || len(r.Entries) != 0 {
			return errors.New("invalid checkpoint record")
		}
		if c.Index < s.state.base || c.Commit < c.Index || c.RaftTerm < s.state.term || c.Commit < s.state.commit || len(c.Log) == 0 || c.Log[0].Index != c.Index || c.Log[0].Term != c.Term {
			return errors.New("invalid checkpoint state")
		}
		if err := statemachine.New().Restore(c.Machine); err != nil {
			return err
		}
		previousTerm := c.Term
		for i, e := range c.Log[1:] {
			if e.Index != c.Index+1+uint64(i) || e.Term == 0 || e.Term > c.RaftTerm || e.Term < previousTerm {
				return errors.New("invalid checkpoint log")
			}
			if e.Command != nil {
				if err := statemachine.Validate(*e.Command); err != nil {
					return err
				}
			}
			previousTerm = e.Term
		}
		if c.Commit > c.Index+uint64(len(c.Log)-1) {
			return errors.New("checkpoint commit exceeds log")
		}
		return nil
	}
	if r.Identity != s.identity {
		return errors.New("data belongs to another node, cluster, membership, or election mode")
	}
	if r.Term < s.state.term || r.Commit < s.state.commit {
		return errors.New("durable term or commit regressed")
	}
	if r.Term == s.state.term && s.state.vote != "" && r.Vote != s.state.vote {
		return errors.New("attempt to change vote within a term")
	}
	length := s.state.base + uint64(len(s.state.log))
	if r.From != 0 {
		if r.From <= s.state.commit || r.From > length {
			return errors.New("attempt to replace committed entries or create log gap")
		}
		length = r.From + uint64(len(r.Entries))
		previousTerm := s.entry(r.From - 1).Term
		for i, entry := range r.Entries {
			if entry.Index != r.From+uint64(i) || entry.Term == 0 || entry.Term > r.Term || entry.Term < previousTerm {
				return errors.New("invalid log index or term")
			}
			if entry.Command != nil {
				if err := statemachine.Validate(*entry.Command); err != nil {
					return err
				}
			}
			previousTerm = entry.Term
		}
	} else if len(r.Entries) != 0 {
		return errors.New("entries without replacement index")
	}
	if r.Commit >= length {
		return errors.New("commit exceeds log")
	}
	return nil
}

func cloneEntries(entries []Entry) []Entry {
	copyOf := append([]Entry(nil), entries...)
	for i := range copyOf {
		if copyOf[i].Command != nil {
			c := *copyOf[i].Command
			copyOf[i].Command = &c
		}
	}
	return copyOf
}

func (s *diskStore) accept(r record) {
	if r.Checkpoint != nil {
		c := r.Checkpoint
		s.state = durableState{term: c.RaftTerm, vote: c.Vote, commit: c.Commit, base: c.Index, log: cloneEntries(c.Log)}
		s.snapshot = c.Machine
		return
	}
	s.state.term, s.state.vote, s.state.commit = r.Term, r.Vote, r.Commit
	if r.From != 0 {
		offset := r.From - s.state.base
		s.state.log = append(s.state.log[:offset], cloneEntries(r.Entries)...)
	}
}

func (s *diskStore) save(term uint64, vote string, commit, from uint64, entries []Entry) error {
	r := record{Identity: s.identity, Term: term, Vote: vote, Commit: commit, From: from, Entries: entries}
	if err := s.validate(r); err != nil {
		return err
	}
	b, err := json.Marshal(r)
	if err != nil {
		return err
	}
	if err := s.wal.Append(b); err != nil {
		return fmt.Errorf("persist raft state: %w", err)
	}
	s.accept(r)
	return nil
}

func (s *diskStore) entry(index uint64) Entry { return s.state.log[index-s.state.base] }
func (s *diskStore) last() uint64             { return s.state.base + uint64(len(s.state.log)-1) }

func (s *diskStore) checkpoint(machine statemachine.Snapshot, index uint64) error {
	if index <= s.state.base || index > s.state.commit || index > s.last() {
		return errors.New("invalid checkpoint index")
	}
	offset := index - s.state.base
	c := checkpoint{Index: index, Term: s.state.log[offset].Term, Machine: machine, RaftTerm: s.state.term, Vote: s.state.vote, Commit: s.state.commit, Log: cloneEntries(s.state.log[offset:])}
	r := record{Identity: s.identity, Checkpoint: &c}
	if err := s.validate(r); err != nil {
		return err
	}
	b, err := json.Marshal(r)
	if err != nil {
		return err
	}
	replacer, ok := s.wal.(interface{ Replace([][]byte) error })
	if !ok {
		return errors.New("journal does not support compaction")
	}
	if err := replacer.Replace([][]byte{b}); err != nil {
		return fmt.Errorf("persist checkpoint: %w", err)
	}
	s.accept(r)
	return nil
}
