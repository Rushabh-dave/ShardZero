// Package storage provides a checksummed append-only command journal.
package storage

import (
	"encoding/binary"
	"errors"
	"fmt"
	"hash/crc32"
	"io"
	"os"
	"path/filepath"
)

const MaxRecordBytes = 16 << 20 // JSON escaping can expand a 1 MiB value sixfold.

type syncFile interface {
	io.Writer
	Sync() error
	Close() error
}

type WAL struct {
	file   syncFile
	lock   *os.File
	failed error
	dir    string
	path   string
}

// Open locks the journal for this process, replays complete records, and removes
// only an incomplete final record. A complete corrupt record fails recovery.
func Open(dir string, replay func([]byte) error) (*WAL, error) {
	return OpenJournal(dir, "commands.wal", replay)
}

// OpenJournal shares one directory lock across standalone and Raft modes.
// Their incompatible journals must never be silently opened as each other.
func OpenJournal(dir, name string, replay func([]byte) error) (*WAL, error) {
	if name != "commands.wal" && name != "raft.wal" {
		return nil, errors.New("unknown journal")
	}
	if err := os.MkdirAll(dir, 0700); err != nil {
		return nil, err
	}
	lock, err := os.OpenFile(filepath.Join(dir, "directory.lock"), os.O_CREATE|os.O_RDWR, 0600)
	if err != nil {
		return nil, err
	}
	if err := lockFile(lock); err != nil {
		_ = lock.Close()
		return nil, fmt.Errorf("data directory is already in use: %w", err)
	}
	other := "raft.wal"
	if name == other {
		other = "commands.wal"
	}
	if _, err := os.Stat(filepath.Join(dir, other)); !errors.Is(err, os.ErrNotExist) {
		_ = lock.Close()
		return nil, fmt.Errorf("incompatible data directory: %s exists or cannot be inspected; use a fresh directory", other)
	}
	f, err := os.OpenFile(filepath.Join(dir, name), os.O_CREATE|os.O_RDWR, 0600)
	if err != nil {
		_ = lock.Close()
		return nil, err
	}
	fail := func(err error) (*WAL, error) { _ = f.Close(); _ = lock.Close(); return nil, err }
	if err := lockFile(f); err != nil {
		return fail(fmt.Errorf("data directory is already in use or cannot be locked: %w", err))
	}
	var offset int64
	for {
		var header [8]byte
		_, err := io.ReadFull(f, header[:])
		if errors.Is(err, io.EOF) {
			break
		}
		if errors.Is(err, io.ErrUnexpectedEOF) {
			if err := truncateTail(f, offset); err != nil {
				return fail(err)
			}
			break
		}
		if err != nil {
			return fail(err)
		}
		length := binary.BigEndian.Uint32(header[:4])
		if length == 0 || length > MaxRecordBytes {
			return fail(fmt.Errorf("invalid WAL length at byte %d", offset))
		}
		payload := make([]byte, length)
		_, err = io.ReadFull(f, payload)
		if errors.Is(err, io.EOF) || errors.Is(err, io.ErrUnexpectedEOF) {
			if err := truncateTail(f, offset); err != nil {
				return fail(err)
			}
			break
		}
		if err != nil {
			return fail(err)
		}
		if crc32.ChecksumIEEE(payload) != binary.BigEndian.Uint32(header[4:]) {
			return fail(fmt.Errorf("WAL checksum mismatch at byte %d", offset))
		}
		if err := replay(payload); err != nil {
			return fail(fmt.Errorf("WAL replay at byte %d: %w", offset, err))
		}
		offset += int64(8 + length)
	}
	if _, err := f.Seek(0, io.SeekEnd); err != nil {
		return fail(err)
	}
	if err := f.Sync(); err != nil {
		return fail(err)
	}
	if err := syncDirectory(dir); err != nil {
		return fail(err)
	}
	return &WAL{file: f, lock: lock, dir: dir, path: filepath.Join(dir, name)}, nil
}

func truncateTail(f *os.File, offset int64) error {
	if err := f.Truncate(offset); err != nil {
		return err
	}
	return f.Sync()
}

// Append must be serialized by the owner. After any I/O failure the journal is
// poisoned: continuing could append behind a partial record and lose later data.
func (w *WAL) Append(payload []byte) error {
	if w.failed != nil {
		return w.failed
	}
	if len(payload) == 0 || len(payload) > MaxRecordBytes {
		return errors.New("invalid record size")
	}
	record := make([]byte, 8+len(payload))
	binary.BigEndian.PutUint32(record[:4], uint32(len(payload)))
	binary.BigEndian.PutUint32(record[4:8], crc32.ChecksumIEEE(payload))
	copy(record[8:], payload)
	n, err := w.file.Write(record)
	if err == nil && n != len(record) {
		err = io.ErrShortWrite
	}
	if err == nil {
		err = w.file.Sync()
	}
	if err != nil {
		w.failed = fmt.Errorf("WAL unavailable; restart required: %w", err)
	}
	return w.failed
}

func (w *WAL) Close() error {
	err := w.file.Close()
	if w.lock != nil {
		err = errors.Join(err, w.lock.Close())
	}
	return err
}

// Replace atomically installs a compacted sequence of already-framed logical
// records. The new file is synced before it replaces the old one. It is used by
// Raft checkpoints; normal command journals never call it.
func (w *WAL) Replace(payloads [][]byte) error {
	if w.failed != nil {
		return w.failed
	}
	old, ok := w.file.(*os.File)
	if !ok {
		return errors.New("WAL replacement unavailable for this file")
	}
	tmp := w.path + ".compact"
	f, err := os.OpenFile(tmp, os.O_CREATE|os.O_TRUNC|os.O_WRONLY, 0600)
	if err != nil {
		return err
	}
	fail := func(e error) error { _ = f.Close(); _ = os.Remove(tmp); return e }
	for _, payload := range payloads {
		if len(payload) == 0 || len(payload) > MaxRecordBytes {
			return fail(errors.New("invalid record size"))
		}
		record := make([]byte, 8+len(payload))
		binary.BigEndian.PutUint32(record[:4], uint32(len(payload)))
		binary.BigEndian.PutUint32(record[4:8], crc32.ChecksumIEEE(payload))
		copy(record[8:], payload)
		if n, e := f.Write(record); e != nil || n != len(record) {
			if e == nil {
				e = io.ErrShortWrite
			}
			return fail(e)
		}
	}
	if err := f.Sync(); err != nil {
		return fail(err)
	}
	if err := f.Close(); err != nil {
		_ = os.Remove(tmp)
		return err
	}
	if err := old.Close(); err != nil {
		_ = os.Remove(tmp)
		return err
	}
	if err := os.Rename(tmp, w.path); err != nil {
		return err
	}
	if err := syncDirectory(w.dir); err != nil {
		return err
	}
	reopened, err := os.OpenFile(w.path, os.O_RDWR|os.O_APPEND, 0600)
	if err != nil {
		return err
	}
	if err := lockFile(reopened); err != nil {
		_ = reopened.Close()
		return err
	}
	w.file = reopened
	return nil
}
