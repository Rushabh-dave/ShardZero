package statemachine

import (
	"errors"
	"fmt"
	"sync"
	"sync/atomic"
	"testing"
)

func TestCommandsAndDeduplication(t *testing.T) {
	m := New()
	set := Command{ClientID: "c", RequestID: 1, Operation: "SET", Key: "score", Value: "950"}
	if r, err := m.Apply(set); err != nil || !r.Applied {
		t.Fatalf("SET: %+v %v", r, err)
	}
	cas := Command{ClientID: "c", RequestID: 2, Operation: "CAS", Key: "score", Expected: "950", Value: "1000"}
	want, err := m.Apply(cas)
	if err != nil || !want.Applied {
		t.Fatal(want, err)
	}
	if got, err := m.Apply(cas); got != want || err != nil {
		t.Fatalf("duplicate: %+v %v", got, err)
	}
	changed := cas
	changed.Value = "999"
	if _, err := m.Apply(changed); !errors.Is(err, ErrReused) {
		t.Fatal(err)
	}
	if _, err := m.Apply(set); !errors.Is(err, ErrStale) {
		t.Fatal(err)
	}
	cas.RequestID = 3
	if r, err := m.Apply(cas); err != nil || !r.Conflict {
		t.Fatal(r, err)
	}
	deleteCommand := Command{ClientID: "c", RequestID: 4, Operation: "DELETE", Key: "score"}
	if r, err := m.Apply(deleteCommand); err != nil || !r.Applied {
		t.Fatal(r, err)
	}
	if _, found := m.Get("score"); found {
		t.Fatal("deleted key exists")
	}
	cas.RequestID = 5
	cas.Expected = ""
	if r, _ := m.Apply(cas); !r.Conflict {
		t.Fatal("CAS must not create missing key")
	}
}

func TestConcurrentCASHasOneWinner(t *testing.T) {
	m := New()
	_, _ = m.Apply(Command{ClientID: "init", RequestID: 1, Operation: "SET", Key: "k", Value: "start"})
	var winners atomic.Int32
	var wg sync.WaitGroup
	for i := 0; i < 100; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			r, err := m.Apply(Command{ClientID: fmt.Sprint(i), RequestID: 1, Operation: "CAS", Key: "k", Expected: "start", Value: "end"})
			if err != nil {
				t.Error(err)
			}
			if r.Applied {
				winners.Add(1)
			}
			m.Get("k")
		}(i)
	}
	wg.Wait()
	if winners.Load() != 1 {
		t.Fatalf("winners = %d", winners.Load())
	}
}

func TestInvalidUTF8CannotDivergeDuringJSONReplication(t *testing.T) {
	c := Command{ClientID: "client", RequestID: 1, Operation: "SET", Key: "key", Value: string([]byte{0xff})}
	if err := Validate(c); !errors.Is(err, ErrInvalid) {
		t.Fatal("invalid UTF-8 accepted", err)
	}
}
