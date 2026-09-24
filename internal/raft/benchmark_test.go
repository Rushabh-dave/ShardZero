package raft

import (
	"context"
	"fmt"
	"testing"
)

// BenchmarkReplicatedWrite reports end-to-end in-process Raft write cost. It
// uses three nodes and therefore includes quorum replication, commit, and
// state-machine application; it is a repeatable comparison tool, not a claim
// about production network throughput.
func BenchmarkReplicatedWrite(b *testing.B) {
	c := newCluster(b, 3, true)
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if _, err := c.nodes[0].Execute(context.Background(), command("benchmark", uint64(i+1), "key", fmt.Sprint(i))); err != nil { b.Fatal(err) }
	}
}
