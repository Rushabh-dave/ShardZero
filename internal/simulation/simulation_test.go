package simulation

import (
	"context"
	"encoding/json"
	"os"
	"testing"
)

func TestRecordedFaultTrace(t *testing.T) {
	b, err := os.ReadFile("testdata/seed-728391.scenario.json")
	if err != nil {
		t.Fatal(err)
	}
	var s Scenario
	if err := json.Unmarshal(b, &s); err != nil {
		t.Fatal(err)
	}
	r := Run(context.Background(), s)
	if !r.Passed || r.TraceHash != "a0c9092a704639f3d5b772d900ac309b7c68d2ca8f890174c88280d7ce23d359" {
		t.Fatalf("recorded schedule changed: passed=%v hash=%s error=%s", r.Passed, r.TraceHash, r.Error)
	}
}

func TestHealingPausedNodeWithDiskFailure(t *testing.T) {
	b, err := os.ReadFile("testdata/paused-disk-failure.scenario.json")
	if err != nil {
		t.Fatal(err)
	}
	var s Scenario
	if err := json.Unmarshal(b, &s); err != nil {
		t.Fatal(err)
	}
	if r := Run(context.Background(), s); !r.Passed {
		t.Fatal(r.Error)
	}
}

func TestSameSeedReproducesEntireResult(t *testing.T) {
	s := Default(728391, 3, 45)
	a := Run(context.Background(), s)
	b := Run(context.Background(), s)
	if !a.Passed || !b.Passed {
		t.Fatal(a.Error, b.Error)
	}
	x, _ := json.Marshal(a)
	y, _ := json.Marshal(b)
	if string(x) != string(y) {
		t.Fatalf("seed is nondeterministic: %s != %s", a.TraceHash, b.TraceHash)
	}
}
func TestSeededFaultScenarios(t *testing.T) {
	for seed := uint64(1); seed <= 12; seed++ {
		s := Default(seed, 3+int(seed%2)*2, 30)
		if seed%3 == 0 {
			s.Faults = append(s.Faults, Fault{AtMS: 1700, Kind: "disk-before", Node: 2}, Fault{AtMS: 2250, Kind: "restart", Node: 2})
		}
		r := Run(context.Background(), s)
		if !r.Passed {
			t.Fatalf("seed %d: %s", seed, r.Error)
		}
	}
}
func TestFaultScheduleMinimization(t *testing.T) {
	s := Default(1, 3, 10)
	s.Faults = append(s.Faults, Fault{AtMS: 1, Kind: "disk-before"})
	small := Minimize(context.Background(), s, func(x Scenario) bool {
		for _, f := range x.Faults {
			if f.Kind == "disk-before" {
				return true
			}
		}
		return false
	})
	if len(small.Faults) != 1 || small.Faults[0].Kind != "disk-before" {
		t.Fatal(small.Faults)
	}
}
func TestCanceledSimulationStops(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if r := Run(ctx, Default(1, 3, 20)); r.Passed || r.Error == "" {
		t.Fatal(r)
	}
}
