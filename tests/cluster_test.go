package tests

import (
	"bufio"
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/url"
	"os"
	"os/exec"
	"testing"
	"time"

	"github.com/Rushabh-dave/shardzero/internal/api"
	"github.com/Rushabh-dave/shardzero/internal/cluster"
	"github.com/Rushabh-dave/shardzero/internal/raft"
	"github.com/Rushabh-dave/shardzero/internal/transport"
)

func TestClusterProcessHelper(t *testing.T) {
	encoded := os.Getenv("SHARDZERO_CLUSTER_HELPER")
	if encoded == "" {
		return
	}
	var cfg raft.Config
	if err := json.Unmarshal([]byte(encoded), &cfg); err != nil {
		panic(err)
	}
	var self raft.Peer
	for _, p := range cfg.Peers {
		if p.ID == cfg.ID {
			self = p
		}
	}
	clientURL, _ := url.Parse(self.ClientURL)
	peerURL, _ := url.Parse(self.PeerURL)
	clientListener, err := net.Listen("tcp", clientURL.Host)
	if err != nil {
		panic(err)
	}
	peerListener, err := net.Listen("tcp", peerURL.Host)
	if err != nil {
		panic(err)
	}
	node, err := raft.Open(cfg, transport.New())
	if err != nil {
		panic(err)
	}
	go func() {
		if err := http.Serve(peerListener, transport.Handler(node)); err != nil {
			panic(err)
		}
	}()
	fmt.Println("ready")
	if err := http.Serve(clientListener, api.ClusterHandler(node)); err != nil {
		panic(err)
	}
}

type processCluster struct {
	configs []raft.Config
	stops   []func()
	client  *http.Client
}

func newProcessCluster(t *testing.T, fixed bool) *processCluster {
	t.Helper()
	c := &processCluster{client: &http.Client{Timeout: 5 * time.Second, CheckRedirect: func(*http.Request, []*http.Request) error { return http.ErrUseLastResponse }}}
	var peers []raft.Peer
	var reservations []net.Listener
	for i := 0; i < 3; i++ {
		addresses := []string{}
		for j := 0; j < 2; j++ {
			l, err := net.Listen("tcp", "127.0.0.1:0")
			if err != nil {
				t.Fatal(err)
			}
			reservations = append(reservations, l)
			addresses = append(addresses, "http://"+l.Addr().String())
		}
		peers = append(peers, raft.Peer{ID: fmt.Sprintf("node-%d", i), PeerURL: addresses[0], ClientURL: addresses[1]})
	}
	for _, l := range reservations {
		_ = l.Close()
	}
	for i := 0; i < 3; i++ {
		cfg := raft.Config{ID: peers[i].ID, ClusterID: "process-test", Peers: peers, DataDir: t.TempDir(), RequestTimeout: time.Second}
		if fixed {
			cfg.FixedLeader = peers[0].ID
		}
		c.configs = append(c.configs, cfg)
		c.stops = append(c.stops, nil)
		c.start(t, i)
	}
	return c
}
func (c *processCluster) start(t *testing.T, i int) {
	t.Helper()
	encoded, _ := json.Marshal(c.configs[i])
	cmd := exec.Command(os.Args[0], "-test.run=^TestClusterProcessHelper$")
	cmd.Env = append(os.Environ(), "SHARDZERO_CLUSTER_HELPER="+string(encoded))
	cmd.Stderr = os.Stderr
	stdout, err := cmd.StdoutPipe()
	if err != nil {
		t.Fatal(err)
	}
	if err := cmd.Start(); err != nil {
		t.Fatal(err)
	}
	stopped := false
	stop := func() {
		if !stopped {
			stopped = true
			_ = cmd.Process.Kill()
			_ = cmd.Wait()
		}
	}
	t.Cleanup(stop)
	c.stops[i] = stop
	ready := make(chan bool, 1)
	go func() { s := bufio.NewScanner(stdout); ready <- s.Scan() && s.Text() == "ready" }()
	select {
	case ok := <-ready:
		if !ok {
			t.Fatal("child failed to start")
		}
	case <-time.After(20 * time.Second):
		t.Fatal("child startup timed out")
	}
}
func (c *processCluster) request(t *testing.T, i int, method, path, body string) (int, []byte) {
	t.Helper()
	req, err := http.NewRequest(method, c.configs[i].Peers[i].ClientURL+path, bytes.NewBufferString(body))
	if err != nil {
		t.Fatal(err)
	}
	resp, err := c.client.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	b, err := io.ReadAll(resp.Body)
	if err != nil {
		t.Fatal(err)
	}
	return resp.StatusCode, b
}
func TestFixedLeaderAcrossProcesses(t *testing.T) {
	c := newProcessCluster(t, true)
	set := func(id int, want int) {
		t.Helper()
		code, b := c.request(t, 0, "PUT", "/v1/kv/score", fmt.Sprintf(`{"clientId":"process","requestId":%d,"value":"%d"}`, id, id))
		if code != want {
			t.Fatalf("%d %s", code, b)
		}
	}
	set(1, 200)
	c.stops[2]()
	set(2, 200)
	c.stops[1]()
	set(3, 503)
	code, _ := c.request(t, 0, "GET", "/v1/kv/score", "")
	if code != 503 {
		t.Fatal("isolated leader served read", code)
	}
	c.start(t, 1)
	c.start(t, 2)
	set(4, 200)
	code, b := c.request(t, 1, "GET", "/v1/kv/score", "")
	if code != 307 {
		t.Fatalf("follower read: %d %s", code, b)
	}
}

func (c *processCluster) leader(t *testing.T, excluded int) int {
	t.Helper()
	deadline := time.Now().Add(10 * time.Second)
	for time.Now().Before(deadline) {
		for i := range c.configs {
			if i == excluded {
				continue
			}
			resp, err := c.client.Get(c.configs[i].Peers[i].ClientURL + "/v1/status")
			if err != nil {
				continue
			}
			var status raft.Status
			err = json.NewDecoder(resp.Body).Decode(&status)
			_ = resp.Body.Close()
			if err == nil && status.Role == "leader" {
				return i
			}
		}
		time.Sleep(30 * time.Millisecond)
	}
	t.Fatal("no leader elected")
	return -1
}

func TestAutomaticFailoverWithLeaderAwareClient(t *testing.T) {
	c := newProcessCluster(t, false)
	old := c.leader(t, -1)
	var seeds []string
	for _, p := range c.configs[0].Peers {
		seeds = append(seeds, p.ClientURL)
	}
	client, err := cluster.New(seeds)
	if err != nil {
		t.Fatal(err)
	}
	do := func(method, path, body string) []byte {
		t.Helper()
		r, err := client.Do(context.Background(), method, path, []byte(body))
		if err != nil || r.StatusCode != 200 {
			t.Fatalf("client: %+v %v", r, err)
		}
		return r.Body
	}
	do("PUT", "/v1/kv/score", `{"clientId":"failover","requestId":1,"value":"950"}`)
	cas := `{"clientId":"failover","requestId":2,"expected":"950","value":"1000"}`
	want := do("POST", "/v1/kv/score/cas", cas)
	c.stops[old]()
	newLeader := c.leader(t, old)
	got := do("POST", "/v1/kv/score/cas", cas)
	if string(want) != string(got) {
		t.Fatalf("duplicate changed outcome: %s / %s", want, got)
	}
	var result struct {
		Value string `json:"value"`
	}
	if err := json.Unmarshal(do("GET", "/v1/kv/score", ""), &result); err != nil || result.Value != "1000" {
		t.Fatal(result, err)
	}
	c.start(t, old)
	do("PUT", "/v1/kv/after-restart", `{"clientId":"failover","requestId":3,"value":"saved"}`)
	// A complete crash/restart retains acknowledged commands and client sessions.
	for _, stop := range c.stops {
		stop()
	}
	for i := range c.configs {
		c.start(t, i)
	}
	_ = c.leader(t, -1)
	if err := json.Unmarshal(do("GET", "/v1/kv/after-restart", ""), &result); err != nil || result.Value != "saved" {
		t.Fatal(result, err)
	}
	_ = newLeader
}
