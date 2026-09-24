package raft

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/Rushabh-dave/shardzero/internal/storage"
)

func TestJournalReplayTermVoteCommitAndReplacedSuffix(t *testing.T) {
	dir := t.TempDir()
	s, err := openStore(dir, "test-identity")
	if err != nil {
		t.Fatal(err)
	}
	if err := s.save(1, "a", 1, 1, []Entry{{Index: 1, Term: 1}, {Index: 2, Term: 1}}); err != nil {
		t.Fatal(err)
	}
	if err := s.save(2, "b", 2, 2, []Entry{{Index: 2, Term: 2}}); err != nil {
		t.Fatal(err)
	}
	if err := s.save(2, "a", 2, 0, nil); err == nil {
		t.Fatal("changed persisted vote")
	}
	if err := s.save(2, "b", 2, 1, []Entry{{Index: 1, Term: 2}}); err == nil {
		t.Fatal("replaced committed entry")
	}
	if err := s.wal.Close(); err != nil {
		t.Fatal(err)
	}
	s, err = openStore(dir, "test-identity")
	if err != nil {
		t.Fatal(err)
	}
	if s.state.term != 2 || s.state.vote != "b" || s.state.commit != 2 || s.state.log[2].Term != 2 {
		t.Fatal(s.state)
	}
	_ = s.wal.Close()
	if other, err := openStore(dir, "wrong-node"); err == nil {
		_ = other.wal.Close()
		t.Fatal("reused another node's data")
	}
}

func TestRaftPartialTailRetainsPriorCommit(t *testing.T) {
	dir := t.TempDir()
	s, err := openStore(dir, "test")
	if err != nil {
		t.Fatal(err)
	}
	if err := s.save(1, "a", 1, 1, []Entry{{Index: 1, Term: 1}}); err != nil {
		t.Fatal(err)
	}
	_ = s.wal.Close()
	f, err := os.OpenFile(filepath.Join(dir, "raft.wal"), os.O_APPEND|os.O_WRONLY, 0600)
	if err != nil {
		t.Fatal(err)
	}
	_, err = f.Write([]byte{0, 0, 0, 100, 0, 0, 0, 0, '{'})
	if err != nil {
		t.Fatal(err)
	}
	_ = f.Close()
	s, err = openStore(dir, "test")
	if err != nil {
		t.Fatal(err)
	}
	defer s.wal.Close()
	if s.state.term != 1 || s.state.commit != 1 {
		t.Fatal(s.state)
	}
}

func TestStandaloneAndRaftDataCannotBeMixed(t *testing.T) {
	dir := t.TempDir()
	w, err := storage.Open(dir, func([]byte) error { return nil })
	if err != nil {
		t.Fatal(err)
	}
	_ = w.Close()
	if s, err := openStore(dir, "test"); err == nil {
		_ = s.wal.Close()
		t.Fatal("opened standalone directory as Raft")
	}
	dir = t.TempDir()
	s, err := openStore(dir, "test")
	if err != nil {
		t.Fatal(err)
	}
	_ = s.wal.Close()
	if w, err := storage.Open(dir, func([]byte) error { return nil }); err == nil {
		_ = w.Close()
		t.Fatal("opened Raft directory as standalone")
	}
}
