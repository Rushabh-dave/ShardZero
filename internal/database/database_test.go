package database

import (
	"errors"
	"testing"

	"github.com/Rushabh-dave/shardzero/internal/statemachine"
)

func TestRestartRestoresValuesAndRequestResults(t *testing.T) {
	dir := t.TempDir()
	db, err := Open(dir)
	if err != nil {
		t.Fatal(err)
	}
	commands := []statemachine.Command{
		{ClientID: "a", RequestID: 1, Operation: "SET", Key: "score", Value: "950"},
		{ClientID: "a", RequestID: 2, Operation: "CAS", Key: "score", Expected: "950", Value: "1000"},
		{ClientID: "b", RequestID: 1, Operation: "CAS", Key: "score", Expected: "wrong", Value: "no"},
		{ClientID: "c", RequestID: 1, Operation: "SET", Key: "gone", Value: ""},
		{ClientID: "c", RequestID: 2, Operation: "DELETE", Key: "gone"},
	}
	var results []statemachine.Result
	for _, c := range commands {
		r, err := db.Execute(c)
		if err != nil {
			t.Fatal(err)
		}
		results = append(results, r)
	}
	if err := db.Close(); err != nil {
		t.Fatal(err)
	}
	db, err = Open(dir)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	if v, ok, err := db.Get("score"); err != nil || !ok || v != "1000" {
		t.Fatal(v, ok, err)
	}
	if _, ok, _ := db.Get("gone"); ok {
		t.Fatal("deleted value resurrected")
	}
	for _, i := range []int{1, 2, 4} {
		r, err := db.Execute(commands[i])
		if err != nil || r != results[i] {
			t.Fatal(r, err)
		}
	}
	if db.Stats().AppliedOperations != uint64(len(commands)) {
		t.Fatal("retries appended commands")
	}
	if _, err := db.Execute(commands[0]); !errors.Is(err, statemachine.ErrStale) {
		t.Fatal(err)
	}
}

func TestInvalidCommandDoesNotEnterJournal(t *testing.T) {
	db, err := Open(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	if _, err := db.Execute(statemachine.Command{Operation: "SET", Key: "k"}); !errors.Is(err, statemachine.ErrInvalid) {
		t.Fatal(err)
	}
	if db.Stats().AppliedOperations != 0 {
		t.Fatal("invalid command applied")
	}
}
