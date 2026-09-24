// Package database serializes durable commands and reads over a state machine.
package database

import (
	"encoding/json"
	"errors"
	"sync"

	"github.com/Rushabh-dave/shardzero/internal/statemachine"
	"github.com/Rushabh-dave/shardzero/internal/storage"
)

var ErrUnavailable = errors.New("database unavailable; restart required")

type Database struct {
	mu      sync.RWMutex
	machine *statemachine.Machine
	wal     *storage.WAL
	failed  bool
	closed  bool
}

type Stats struct {
	Keys              int    `json:"keys"`
	AppliedOperations uint64 `json:"appliedOperations"`
	Healthy           bool   `json:"healthy"`
}

func Open(dir string) (*Database, error) {
	d := &Database{machine: statemachine.New()}
	w, err := storage.Open(dir, func(payload []byte) error {
		var command statemachine.Command
		if err := json.Unmarshal(payload, &command); err != nil {
			return err
		}
		_, err := d.machine.Apply(command)
		return err
	})
	if err != nil {
		return nil, err
	}
	d.wal = w
	return d, nil
}

func (d *Database) Execute(c statemachine.Command) (statemachine.Result, error) {
	d.mu.Lock()
	defer d.mu.Unlock()
	if d.closed || d.failed {
		return statemachine.Result{}, ErrUnavailable
	}
	if result, duplicate, err := d.machine.Check(c); duplicate || err != nil {
		return result, err
	}
	payload, err := json.Marshal(c)
	if err != nil {
		return statemachine.Result{}, err
	}
	if err := d.wal.Append(payload); err != nil {
		d.failed = true
		return statemachine.Result{}, errors.Join(ErrUnavailable, err)
	}
	return d.machine.Apply(c)
}

func (d *Database) Get(key string) (string, bool, error) {
	d.mu.RLock()
	defer d.mu.RUnlock()
	if d.closed || d.failed {
		return "", false, ErrUnavailable
	}
	v, ok := d.machine.Get(key)
	return v, ok, nil
}

func (d *Database) Stats() Stats {
	d.mu.RLock()
	defer d.mu.RUnlock()
	keys, operations := d.machine.Stats()
	return Stats{Keys: keys, AppliedOperations: operations, Healthy: !d.closed && !d.failed}
}

func (d *Database) Close() error {
	d.mu.Lock()
	defer d.mu.Unlock()
	if d.closed {
		return nil
	}
	d.closed = true
	return d.wal.Close()
}
