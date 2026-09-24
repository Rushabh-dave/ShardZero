package main

import (
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"os"
	"path/filepath"

	"github.com/Rushabh-dave/shardzero/internal/simulation"
)

func main() {
	if err := run(); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}
func run() error {
	seed := flag.Uint64("seed", 728391, "deterministic seed")
	nodes := flag.Int("nodes", 3, "3 to 5 nodes")
	ops := flag.Int("operations", 100, "write operations")
	scenario := flag.String("scenario", "", "replay a saved JSON scenario")
	out := flag.String("out", "artifacts/simulation", "report directory")
	runs := flag.Int("runs", 1, "consecutive seeds")
	minimize := flag.Bool("minimize", false, "reduce fault schedule on failure")
	flag.Parse()
	if *runs < 1 || *runs > 10000 {
		return fmt.Errorf("runs must be 1–10000")
	}
	s := simulation.Default(*seed, *nodes, *ops)
	if *scenario != "" {
		b, err := os.ReadFile(*scenario)
		if err != nil {
			return err
		}
		if err := json.Unmarshal(b, &s); err != nil {
			return err
		}
	}
	if err := os.MkdirAll(*out, 0700); err != nil {
		return err
	}
	for i := 0; i < *runs; i++ {
		current := s
		current.Seed += uint64(i)
		r := simulation.Run(context.Background(), current)
		name := filepath.Join(*out, fmt.Sprintf("seed-%d", current.Seed))
		if err := write(name+".scenario.json", current); err != nil {
			return err
		}
		if err := write(name+".report.json", r); err != nil {
			return err
		}
		fmt.Printf("seed=%d passed=%t acknowledged=%d/%d converged=%t steps=%d trace=%s\n", current.Seed, r.Passed, r.Acknowledged, current.Operations, r.Converged, r.Steps, r.TraceHash)
		if !r.Passed {
			if *minimize {
				small := simulation.Minimize(context.Background(), current, func(x simulation.Scenario) bool { return simulation.Run(context.Background(), x).Error == r.Error })
				if err := write(name+".minimal.json", small); err != nil {
					return err
				}
			}
			return fmt.Errorf("scenario failed: %s (replay %s.scenario.json)", r.Error, name)
		}
	}
	return nil
}
func write(path string, value any) error {
	b, err := json.MarshalIndent(value, "", "  ")
	if err != nil {
		return err
	}
	return os.WriteFile(path, append(b, '\n'), 0600)
}
