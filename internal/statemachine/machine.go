// Package statemachine implements the ordered SET, DELETE and CAS commands.
package statemachine

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"sync"
	"unicode/utf8"
)

// Digest includes both values and retry sessions. JSON sorts string map keys.
func (m *Machine) Digest() string {
	m.mu.RLock()
	defer m.mu.RUnlock()
	b, _ := json.Marshal(struct {
		Values   map[string]string
		Sessions map[string]session
	}{m.values, m.sessions})
	sum := sha256.Sum256(b)
	return hex.EncodeToString(sum[:])
}

var (
	ErrInvalid = errors.New("invalid command")
	ErrStale   = errors.New("request ID is older than the last request for this client")
	ErrReused  = errors.New("request ID was already used for a different command")
)

const MaxValueBytes = 1 << 20

type Command struct {
	ClientID  string `json:"clientId"`
	RequestID uint64 `json:"requestId"`
	Operation string `json:"command"`
	Key       string `json:"key"`
	Value     string `json:"value,omitempty"`
	Expected  string `json:"expected,omitempty"`
}

type Result struct {
	Value    string `json:"value"`
	Found    bool   `json:"found"`
	Applied  bool   `json:"applied"`
	Conflict bool   `json:"conflict,omitempty"`
}

type session struct {
	Command Command
	Result  Result
}

// Snapshot is the complete deterministic state needed to resume a machine.
// It deliberately includes retry sessions: omitting them would make a retried
// acknowledged request behave differently after log compaction.
type Snapshot struct {
	Values   map[string]string  `json:"values"`
	Sessions map[string]Session `json:"sessions"`
	Count    uint64             `json:"count"`
}

type Session struct {
	Command Command `json:"command"`
	Result  Result  `json:"result"`
}

// Machine may be used on its own for an in-memory service. Durable callers must
// serialize Check -> persist -> Apply; Check never changes state.
type Machine struct {
	mu       sync.RWMutex
	values   map[string]string
	sessions map[string]session
	count    uint64
}

func New() *Machine {
	return &Machine{values: make(map[string]string), sessions: make(map[string]session)}
}

func Validate(c Command) error {
	for _, text := range []string{c.ClientID, c.Key, c.Value, c.Expected} {
		if !utf8.ValidString(text) {
			return fmt.Errorf("%w: text must be valid UTF-8", ErrInvalid)
		}
	}
	if c.ClientID == "" || len(c.ClientID) > 128 || c.RequestID == 0 {
		return fmt.Errorf("%w: clientId (1–128 bytes) and positive requestId required", ErrInvalid)
	}
	if c.Key == "" || len(c.Key) > 1024 {
		return fmt.Errorf("%w: key must contain 1–1024 bytes", ErrInvalid)
	}
	if len(c.Value) > MaxValueBytes || len(c.Expected) > MaxValueBytes {
		return fmt.Errorf("%w: value exceeds 1 MiB", ErrInvalid)
	}
	switch c.Operation {
	case "SET":
		if c.Expected != "" {
			return fmt.Errorf("%w: SET does not accept expected", ErrInvalid)
		}
	case "DELETE":
		if c.Value != "" || c.Expected != "" {
			return fmt.Errorf("%w: DELETE does not accept values", ErrInvalid)
		}
	case "CAS":
	default:
		return fmt.Errorf("%w: unknown operation", ErrInvalid)
	}
	return nil
}

func (m *Machine) check(c Command) (Result, bool, error) {
	if err := Validate(c); err != nil {
		return Result{}, false, err
	}
	if previous, ok := m.sessions[c.ClientID]; ok {
		if c.RequestID < previous.Command.RequestID {
			return Result{}, false, ErrStale
		}
		if c.RequestID == previous.Command.RequestID {
			if c != previous.Command {
				return Result{}, false, ErrReused
			}
			return previous.Result, true, nil
		}
	}
	return Result{}, false, nil
}

func (m *Machine) Check(c Command) (Result, bool, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()
	return m.check(c)
}

func (m *Machine) Apply(c Command) (Result, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if result, duplicate, err := m.check(c); duplicate || err != nil {
		return result, err
	}
	value, found := m.values[c.Key]
	result := Result{Value: value, Found: found}
	switch c.Operation {
	case "SET":
		m.values[c.Key] = c.Value
		result = Result{Value: c.Value, Found: true, Applied: true}
	case "DELETE":
		delete(m.values, c.Key)
		result.Applied = found
	case "CAS":
		if found && value == c.Expected {
			m.values[c.Key] = c.Value
			result = Result{Value: c.Value, Found: true, Applied: true}
		} else {
			result.Conflict = true
		}
	}
	m.sessions[c.ClientID] = session{Command: c, Result: result}
	m.count++
	return result, nil
}

func (m *Machine) Get(key string) (string, bool) {
	m.mu.RLock()
	defer m.mu.RUnlock()
	value, found := m.values[key]
	return value, found
}

func (m *Machine) Stats() (int, uint64) {
	m.mu.RLock()
	defer m.mu.RUnlock()
	return len(m.values), m.count
}

func (m *Machine) Snapshot() Snapshot {
	m.mu.RLock()
	defer m.mu.RUnlock()
	s := Snapshot{Values: make(map[string]string, len(m.values)), Sessions: make(map[string]Session, len(m.sessions)), Count: m.count}
	for k, v := range m.values {
		s.Values[k] = v
	}
	for id, v := range m.sessions {
		s.Sessions[id] = Session{Command: v.Command, Result: v.Result}
	}
	return s
}

// Restore replaces state only after validating the command identities retained
// for client retry semantics.
func (m *Machine) Restore(s Snapshot) error {
	values := make(map[string]string, len(s.Values))
	for k, v := range s.Values {
		if k == "" || len(k) > 1024 || !utf8.ValidString(k) || !utf8.ValidString(v) || len(v) > MaxValueBytes {
			return fmt.Errorf("%w: invalid snapshot value", ErrInvalid)
		}
		values[k] = v
	}
	sessions := make(map[string]session, len(s.Sessions))
	for id, v := range s.Sessions {
		if id != v.Command.ClientID {
			return fmt.Errorf("%w: invalid snapshot session", ErrInvalid)
		}
		if err := Validate(v.Command); err != nil {
			return err
		}
		sessions[id] = session{Command: v.Command, Result: v.Result}
	}
	m.mu.Lock()
	m.values, m.sessions, m.count = values, sessions, s.Count
	m.mu.Unlock()
	return nil
}
