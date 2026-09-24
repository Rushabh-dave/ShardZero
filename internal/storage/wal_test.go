package storage

import (
	"encoding/binary"
	"errors"
	"hash/crc32"
	"os"
	"path/filepath"
	"testing"
)

func record(payload string) []byte {
	b := make([]byte, 8+len(payload))
	binary.BigEndian.PutUint32(b[:4], uint32(len(payload)))
	binary.BigEndian.PutUint32(b[4:8], crc32.ChecksumIEEE([]byte(payload)))
	copy(b[8:], payload)
	return b
}

func TestEveryIncompleteTailAndAppendAfterRecovery(t *testing.T) {
	first, tail := record("first"), record("unfinished")
	for cut := 1; cut < len(tail); cut++ {
		dir := t.TempDir()
		if err := os.WriteFile(filepath.Join(dir, "commands.wal"), append(append([]byte{}, first...), tail[:cut]...), 0600); err != nil {
			t.Fatal(err)
		}
		var entries []string
		w, err := Open(dir, func(b []byte) error { entries = append(entries, string(b)); return nil })
		if err != nil {
			t.Fatalf("cut %d: %v", cut, err)
		}
		if len(entries) != 1 || entries[0] != "first" {
			t.Fatal(entries)
		}
		if err := w.Append([]byte("next")); err != nil {
			t.Fatal(err)
		}
		_ = w.Close()
		entries = nil
		w, err = Open(dir, func(b []byte) error { entries = append(entries, string(b)); return nil })
		if err != nil {
			t.Fatal(err)
		}
		_ = w.Close()
		if len(entries) != 2 || entries[1] != "next" {
			t.Fatalf("cut %d: %+v", cut, entries)
		}
	}
}

func TestCorruptCompleteRecordRejected(t *testing.T) {
	dir := t.TempDir()
	data := record("value")
	data[8] ^= 1
	if err := os.WriteFile(filepath.Join(dir, "commands.wal"), data, 0600); err != nil {
		t.Fatal(err)
	}
	if w, err := Open(dir, func([]byte) error { return nil }); err == nil {
		_ = w.Close()
		t.Fatal("corruption accepted")
	}
}

func TestExclusiveDirectory(t *testing.T) {
	dir := t.TempDir()
	w, err := Open(dir, func([]byte) error { return nil })
	if err != nil {
		t.Fatal(err)
	}
	defer w.Close()
	if other, err := Open(dir, func([]byte) error { return nil }); err == nil {
		_ = other.Close()
		t.Fatal("second owner accepted")
	}
}

type failingFile struct {
	writes   int
	failSync bool
}

func (f *failingFile) Write(b []byte) (int, error) {
	f.writes++
	if f.failSync {
		return len(b), nil
	}
	return len(b) / 2, errors.New("disk full")
}
func (f *failingFile) Sync() error  { return errors.New("flush failed") }
func (f *failingFile) Close() error { return nil }

func TestFailurePoisonsJournal(t *testing.T) {
	for _, failSync := range []bool{false, true} {
		f := &failingFile{failSync: failSync}
		w := &WAL{file: f}
		if err := w.Append([]byte("first")); err == nil {
			t.Fatal("I/O failure ignored")
		}
		if err := w.Append([]byte("second")); err == nil {
			t.Fatal("poisoned journal accepted write")
		}
		if f.writes != 1 {
			t.Fatal("continued writing after failure")
		}
	}
}
