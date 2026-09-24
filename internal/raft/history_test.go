package raft

import (
	"context"
	"fmt"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/Rushabh-dave/shardzero/internal/statemachine"
)

// This deliberately small checker covers one string register, with completed
// GET/SET/DELETE/CAS calls and no ambiguous failures. It searches for an order
// that obeys real-time precedence and an independent sequential specification.
// Unknown-outcome requests require a richer checker (the later simulation phase).
type historyCall struct {
	operation, expected, value string
	result                     statemachine.Result
	start, end                 uint64
}
type register struct {
	value string
	found bool
}

func stepRegister(s register, c historyCall) (register, statemachine.Result) {
	r := statemachine.Result{Value: s.value, Found: s.found}
	switch c.operation {
	case "GET":
	case "SET":
		s = register{c.value, true}
		r = statemachine.Result{Value: c.value, Found: true, Applied: true}
	case "DELETE":
		r.Applied = s.found
		s = register{}
	case "CAS":
		if s.found && s.value == c.expected {
			s = register{c.value, true}
			r = statemachine.Result{Value: c.value, Found: true, Applied: true}
		} else {
			r.Conflict = true
		}
	}
	return s, r
}
func linearizable(history []historyCall) bool {
	if len(history) > 32 {
		panic("checker scope exceeded")
	}
	predecessors := make([]uint64, len(history))
	for i, a := range history {
		for j, b := range history {
			if b.end < a.start {
				predecessors[i] |= uint64(1) << j
			}
		}
	}
	type searchState struct {
		mask  uint64
		state register
	}
	failed := make(map[searchState]bool)
	var search func(uint64, register) bool
	search = func(mask uint64, s register) bool {
		if mask == (uint64(1)<<len(history))-1 {
			return true
		}
		key := searchState{mask, s}
		if failed[key] {
			return false
		}
		for i, call := range history {
			bit := uint64(1) << i
			if mask&bit != 0 || predecessors[i]&mask != predecessors[i] {
				continue
			}
			next, result := stepRegister(s, call)
			if result == call.result && search(mask|bit, next) {
				return true
			}
		}
		failed[key] = true
		return false
	}
	return search(0, register{})
}

func TestHistoryCheckerRejectsImpossibleReads(t *testing.T) {
	set := historyCall{operation: "SET", value: "new", result: statemachine.Result{Value: "new", Found: true, Applied: true}, start: 1, end: 2}
	read := historyCall{operation: "GET", result: statemachine.Result{}, start: 3, end: 4}
	if linearizable([]historyCall{set, read}) {
		t.Fatal("accepted missing read after completed SET")
	}
	read.start = 1 // Concurrent read can legally precede the SET.
	if !linearizable([]historyCall{set, read}) {
		t.Fatal("rejected legal overlapping read")
	}
}

func TestConcurrentHistoryIsLinearizable(t *testing.T) {
	c := newClusterConfigured(t, 3, false, func(cfg *Config) {
		cfg.RequestTimeout = 3 * time.Second
		cfg.ElectionMin = 600 * time.Millisecond
		cfg.ElectionMax = 1100 * time.Millisecond
	})
	n := c.nodes[c.leader(t, -1)]
	var clock atomic.Uint64
	var mu sync.Mutex
	var history []historyCall
	var wg sync.WaitGroup
	for client := 0; client < 4; client++ {
		wg.Add(1)
		go func(client int) {
			defer wg.Done()
			for i, op := range []string{"SET", "GET", "CAS", "GET", "DELETE", "GET"} {
				call := historyCall{operation: op, value: fmt.Sprint(client), expected: fmt.Sprint((client + 1) % 4)}
				call.start = clock.Add(1)
				var err error
				if op == "GET" {
					call.result.Value, call.result.Found, err = n.Get(context.Background(), "register")
				} else {
					cmd := statemachine.Command{ClientID: fmt.Sprint(client), RequestID: uint64(i + 1), Operation: op, Key: "register"}
					if op != "DELETE" {
						cmd.Value = call.value
					}
					if op == "CAS" {
						cmd.Expected = call.expected
					}
					call.result, err = n.Execute(context.Background(), cmd)
				}
				call.end = clock.Add(1)
				if err != nil {
					t.Error(err)
					return
				}
				mu.Lock()
				history = append(history, call)
				mu.Unlock()
			}
		}(client)
	}
	wg.Wait()
	if len(history) != 24 || !linearizable(history) {
		t.Fatalf("no legal sequential order: %+v", history)
	}
}
