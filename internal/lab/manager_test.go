package lab

import (
	"bufio"
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"net"
	"net/http"
	"net/http/httptest"
	"os/exec"
	"path/filepath"
	"runtime"
	"strings"
	"testing"
	"time"
)

func TestLiveControllerRecoveryAndAccessBoundary(t *testing.T) {
	binary := filepath.Join(t.TempDir(), "node")
	if runtime.GOOS == "windows" {
		binary += ".exe"
	}
	build := exec.Command("go", "build", "-o", binary, "./cmd/node")
	build.Dir = "../.."
	if output, err := build.CombinedOutput(); err != nil {
		t.Fatalf("build: %v\n%s", err, output)
	}
	m, err := New(Config{Nodes: 3, BasePort: availableBase(t), DataDir: t.TempDir(), NodeBinary: binary})
	if err != nil {
		t.Fatal(err)
	}
	defer m.Close()
	waitLab(t, m, func(s Snapshot) bool { return s.Converged && leaderID(s) != "" })
	srv := httptest.NewServer(m.Handler())
	defer srv.Close()
	// A cross-origin page cannot retrieve the token or mutate the local cluster.
	for _, tc := range []struct{ method, path, origin, token, host string }{
		{"GET", "/api/session", "https://other.example", "", ""},
		{"POST", "/api/chaos", "", "", ""},
		{"POST", "/api/chaos", "https://other.example", m.token, ""},
		{"GET", "/api/session", "", "", "other.example:9100"},
	} {
		req, _ := http.NewRequest(tc.method, srv.URL+tc.path, strings.NewReader(`{"action":"heal"}`))
		req.Header.Set("Origin", tc.origin)
		req.Header.Set("X-ShardZero-Control", tc.token)
		if tc.host != "" {
			req.Host = tc.host
		}
		r, e := srv.Client().Do(req)
		if e != nil {
			t.Fatal(e)
		}
		r.Body.Close()
		if r.StatusCode != 403 {
			t.Fatalf("boundary accepted %+v: %d", tc, r.StatusCode)
		}
	}
	resp, err := srv.Client().Get(srv.URL + "/api/events")
	if err != nil {
		t.Fatal(err)
	}
	reader := bufio.NewReader(resp.Body)
	line, err := reader.ReadString('\n')
	if err != nil || line != "event: state\n" {
		t.Fatalf("SSE: %q %v", line, err)
	}
	line, err = reader.ReadString('\n')
	resp.Body.Close()
	var streamed Snapshot
	if err != nil || json.Unmarshal([]byte(strings.TrimPrefix(line, "data: ")), &streamed) != nil || len(streamed.Nodes) != 3 {
		t.Fatal("SSE did not expose actual members")
	}
	command := func(c CommandRequest, duration time.Duration) CommandResponse {
		t.Helper()
		ctx, cancel := context.WithTimeout(context.Background(), duration)
		defer cancel()
		r, e := m.Command(ctx, c)
		if e != nil {
			t.Fatal(e)
		}
		return r
	}
	set := CommandRequest{Operation: "SET", Key: "lab-test", Value: "0", ClientID: "test", RequestID: 1}
	if r := command(set, 5*time.Second); r.Status != 200 {
		t.Fatal(r)
	}
	// Network controls must actually prevent acknowledgement without a majority.
	if err := m.SetFaults([]string{"node-1", "node-2", "node-3"}, 0, 0); err != nil {
		t.Fatal(err)
	}
	cas := CommandRequest{Operation: "CAS", Key: "lab-test", Expected: "0", Value: "1", ClientID: "test", RequestID: 2}
	if r := command(cas, 700*time.Millisecond); r.Status != 503 {
		t.Fatalf("isolated write acknowledged: %+v", r)
	}
	if err := m.SetFaults(nil, 0, 0); err != nil {
		t.Fatal(err)
	}
	first := command(cas, 7*time.Second)
	if first.Status != 200 || !bytes.Contains(first.Body, []byte(`"applied":true`)) {
		t.Fatalf("heal/retry: %+v", first)
	}
	leader := leaderID(waitLab(t, m, func(s Snapshot) bool { return s.Converged && leaderID(s) != "" }))
	if err := m.Pause(context.Background(), leader, true); err != nil {
		t.Fatal(err)
	}
	waitLab(t, m, func(s Snapshot) bool { id := leaderID(s); return id != "" && id != leader })
	if err := m.Pause(context.Background(), leader, false); err != nil {
		t.Fatal(err)
	}
	s := waitLab(t, m, func(s Snapshot) bool { return s.Converged && leaderID(s) != "" })
	leader = leaderID(s)
	if err := m.Crash(leader); err != nil {
		t.Fatal(err)
	}
	waitLab(t, m, func(s Snapshot) bool {
		for _, n := range s.Nodes {
			if n.ID == leader {
				return !n.Running && !n.Online
			}
		}
		return false
	})
	duplicate := command(cas, 7*time.Second)
	if duplicate.Status != 200 || !bytes.Equal(first.Body, duplicate.Body) {
		t.Fatalf("dedup after crash: %+v != %+v", first, duplicate)
	}
	if err := m.Start(leader); err != nil {
		t.Fatal(err)
	}
	waitLab(t, m, func(s Snapshot) bool { return s.Converged })
	get := command(CommandRequest{Operation: "GET", Key: "lab-test"}, 5*time.Second)
	if get.Status != 200 || !bytes.Contains(get.Body, []byte(`"value":"1"`)) {
		t.Fatal(get)
	}
}

func leaderID(s Snapshot) string {
	var id string
	var term uint64
	for _, n := range s.Nodes {
		if n.Online && n.Healthy && !n.Paused && n.Role == "leader" && n.Term >= term {
			id = n.ID
			term = n.Term
		}
	}
	return id
}
func waitLab(t *testing.T, m *Manager, predicate func(Snapshot) bool) Snapshot {
	t.Helper()
	deadline := time.Now().Add(12 * time.Second)
	for time.Now().Before(deadline) {
		s := m.Snapshot()
		if predicate(s) {
			return s
		}
		time.Sleep(50 * time.Millisecond)
	}
	t.Fatalf("lab did not recover: %+v", m.Snapshot().Nodes)
	return Snapshot{}
}
func availableBase(t *testing.T) int {
	t.Helper()
	for base := 22000; base < 60000; base += 503 {
		var listeners []net.Listener
		ok := true
		for group := 1; group <= 4 && ok; group++ {
			for node := 1; node <= 3; node++ {
				l, e := net.Listen("tcp", fmt.Sprintf("127.0.0.1:%d", base+100*group+node))
				if e != nil {
					ok = false
					break
				}
				listeners = append(listeners, l)
			}
		}
		for _, l := range listeners {
			l.Close()
		}
		if ok {
			return base
		}
	}
	t.Fatal("no free test ports")
	return 0
}
