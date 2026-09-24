// Package lab is a separate local control plane. It owns only its child processes
// and peer proxies; it cannot alter Raft state or a node's durable data.
package lab

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"net"
	"net/http"
	"os"
	"os/exec"
	"path/filepath"
	"sort"
	"sync"
	"time"

	"github.com/Rushabh-dave/shardzero/internal/cluster"
	"github.com/Rushabh-dave/shardzero/internal/events"
	"github.com/Rushabh-dave/shardzero/internal/raft"
)

type Config struct {
	Nodes      int
	BasePort   int
	DataDir    string
	NodeBinary string
	Assets     string
}
type Member struct {
	ID           string
	ClientURL    string
	PeerAddress  string
	AdminURL     string
	ProxyAddress string
	cmd          *exec.Cmd
	running      bool
	logs         *os.File
}
type NodeView struct {
	raft.Inspection
	ID      string    `json:"id"`
	Online  bool      `json:"online"`
	Running bool      `json:"running"`
	SeenAt  time.Time `json:"seenAt"`
}
type FaultState struct {
	Isolated    []string `json:"isolated"`
	DelayMS     int      `json:"delayMs"`
	DropPercent int      `json:"dropPercent"`
}
type Metrics struct {
	Requests  int     `json:"requests"`
	Succeeded int     `json:"succeeded"`
	Conflicts int     `json:"conflicts"`
	Failed    int     `json:"failed"`
	P50MS     float64 `json:"p50Ms"`
	P95MS     float64 `json:"p95Ms"`
	Rate      float64 `json:"rate"`
	Workload  bool    `json:"workload"`
}
type Snapshot struct {
	Time        time.Time      `json:"time"`
	Nodes       []NodeView     `json:"nodes"`
	Faults      FaultState     `json:"faults"`
	Events      []events.Event `json:"events"`
	Timeline    []events.Event `json:"timeline"`
	Metrics     Metrics        `json:"metrics"`
	Converged   bool           `json:"converged"`
	Consistency string         `json:"consistency"`
	EventGaps   int            `json:"eventGaps"`
	BeforeFault *Observation   `json:"beforeFault,omitempty"`
	Observed    Observation    `json:"observed"`
}

type ReplicaObservation struct {
	ID      string `json:"id"`
	Online  bool   `json:"online"`
	Healthy bool   `json:"healthy"`
	Applied uint64 `json:"applied"`
	Hash    string `json:"hash"`
}
type Observation struct {
	Time     time.Time            `json:"time"`
	Matched  bool                 `json:"matched"`
	Replicas []ReplicaObservation `json:"replicas"`
}
type sample struct {
	time time.Time
	ms   float64
}
type Manager struct {
	mu          sync.Mutex
	cfg         Config
	members     []*Member
	views       map[string]NodeView
	ctx         context.Context
	cancel      context.CancelFunc
	wg          sync.WaitGroup
	closing     bool
	configPath  string
	token       string
	http        *http.Client
	client      *cluster.Client
	events      *events.Buffer
	timeline    *events.Buffer
	proxies     []*http.Server
	isolated    map[string]bool
	delayMS     int
	drop        int
	packet      uint64
	metrics     Metrics
	samples     []sample
	workCancel  context.CancelFunc
	beforeFault *Observation
	eventGaps   int
}

func New(cfg Config) (*Manager, error) {
	if cfg.Nodes != 3 && cfg.Nodes != 5 {
		return nil, errors.New("lab requires 3 or 5 nodes")
	}
	if cfg.BasePort < 1024 || cfg.BasePort > 64000 {
		return nil, errors.New("invalid base port")
	}
	// Refuse occupied client/admin/peer ports before starting any child. This
	// prevents accidentally observing or sending commands to an unrelated node.
	var probes []net.Listener
	defer func() {
		for _, probe := range probes {
			_ = probe.Close()
		}
	}()
	for group := 1; group <= 3; group++ {
		for i := 1; i <= cfg.Nodes; i++ {
			address := fmt.Sprintf("127.0.0.1:%d", cfg.BasePort+100*group+i)
			probe, err := net.Listen("tcp", address)
			if err != nil {
				return nil, fmt.Errorf("lab port %s unavailable: %w", address, err)
			}
			probes = append(probes, probe)
		}
	}
	if err := os.MkdirAll(cfg.DataDir, 0700); err != nil {
		return nil, err
	}
	var secret [32]byte
	if _, err := rand.Read(secret[:]); err != nil {
		return nil, err
	}
	ctx, cancel := context.WithCancel(context.Background())
	m := &Manager{cfg: cfg, ctx: ctx, cancel: cancel, views: make(map[string]NodeView), isolated: make(map[string]bool), token: hex.EncodeToString(secret[:]), http: &http.Client{Timeout: time.Second}, events: events.New(400), timeline: events.New(200)}
	m.configPath = filepath.Join(cfg.DataDir, "membership.json")
	membership := raft.Membership{ClusterID: "shardzero-lab"}
	var seeds []string
	for i := 1; i <= cfg.Nodes; i++ {
		member := &Member{ID: fmt.Sprintf("node-%d", i), ClientURL: fmt.Sprintf("http://127.0.0.1:%d", cfg.BasePort+100+i), PeerAddress: fmt.Sprintf("127.0.0.1:%d", cfg.BasePort+200+i), AdminURL: fmt.Sprintf("http://127.0.0.1:%d", cfg.BasePort+300+i), ProxyAddress: fmt.Sprintf("127.0.0.1:%d", cfg.BasePort+400+i)}
		m.members = append(m.members, member)
		membership.Peers = append(membership.Peers, raft.Peer{ID: member.ID, ClientURL: member.ClientURL, PeerURL: "http://" + member.ProxyAddress})
		seeds = append(seeds, member.ClientURL)
	}
	b, _ := json.MarshalIndent(membership, "", "  ")
	if err := os.WriteFile(m.configPath, b, 0600); err != nil {
		cancel()
		return nil, err
	}
	m.client, _ = cluster.New(seeds)
	for _, member := range m.members {
		listener, err := net.Listen("tcp", member.ProxyAddress)
		if err != nil {
			m.Close()
			return nil, err
		}
		server := &http.Server{Handler: m.proxy(member), ReadHeaderTimeout: 3 * time.Second}
		m.proxies = append(m.proxies, server)
		m.wg.Add(1)
		go func() { defer m.wg.Done(); _ = server.Serve(listener) }()
	}
	for _, probe := range probes {
		_ = probe.Close()
	}
	probes = nil
	for _, member := range m.members {
		if err := m.Start(member.ID); err != nil {
			m.Close()
			return nil, err
		}
	}
	m.wg.Add(1)
	go m.pollLoop()
	return m, nil
}
func (m *Manager) find(id string) *Member {
	for _, n := range m.members {
		if n.ID == id {
			return n
		}
	}
	return nil
}
func (m *Manager) Start(id string) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.closing {
		return errors.New("lab closed")
	}
	member := m.find(id)
	if member == nil {
		return errors.New("unknown node")
	}
	if member.running {
		return nil
	}
	log, err := os.OpenFile(filepath.Join(m.cfg.DataDir, id+".log"), os.O_CREATE|os.O_APPEND|os.O_WRONLY, 0600)
	if err != nil {
		return err
	}
	clientAddress := member.ClientURL[len("http://"):]
	adminAddress := member.AdminURL[len("http://"):]
	cmd := exec.Command(m.cfg.NodeBinary, "--id="+id, "--cluster="+m.configPath, "--listen="+clientAddress, "--peer-listen="+member.PeerAddress, "--admin-listen="+adminAddress, "--data="+filepath.Join(m.cfg.DataDir, id))
	cmd.Env = append(os.Environ(), "SHARDZERO_ADMIN_TOKEN="+m.token)
	cmd.Stdout = log
	cmd.Stderr = log
	if err := cmd.Start(); err != nil {
		_ = log.Close()
		return err
	}
	member.cmd = cmd
	member.logs = log
	member.running = true
	m.publish(events.Event{Time: time.Now(), NodeID: id, Type: "PROCESS_STARTED", Detail: fmt.Sprintf("PID %d", cmd.Process.Pid)})
	m.wg.Add(1)
	go func() {
		defer m.wg.Done()
		err := cmd.Wait()
		_ = log.Close()
		m.mu.Lock()
		if member.cmd == cmd {
			member.running = false
		}
		m.mu.Unlock()
		detail := "stopped"
		if err != nil {
			detail = err.Error()
		}
		m.publish(events.Event{Time: time.Now(), NodeID: id, Type: "PROCESS_EXITED", Detail: detail})
	}()
	return nil
}
func (m *Manager) Crash(id string) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	member := m.find(id)
	if member == nil {
		return errors.New("unknown node")
	}
	if !member.running {
		return nil
	}
	observation := m.observationLocked()
	m.beforeFault = &observation
	return member.cmd.Process.Kill()
}
func (m *Manager) Pause(ctx context.Context, id string, paused bool) error {
	m.mu.Lock()
	member := m.find(id)
	if paused && member != nil {
		observation := m.observationLocked()
		m.beforeFault = &observation
	}
	m.mu.Unlock()
	if member == nil {
		return errors.New("unknown node")
	}
	action := "resume"
	if paused {
		action = "pause"
	}
	req, err := http.NewRequestWithContext(ctx, "POST", member.AdminURL+"/admin/"+action, nil)
	if err != nil {
		return err
	}
	req.Header.Set("X-ShardZero-Admin", m.token)
	resp, err := m.http.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	if resp.StatusCode != 200 {
		return fmt.Errorf("admin returned %d", resp.StatusCode)
	}
	return nil
}
func (m *Manager) SetFaults(isolated []string, delay, drop int) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	if delay < 0 || delay > 2000 || drop < 0 || drop > 100 {
		return errors.New("invalid delay/drop range")
	}
	blocked := make(map[string]bool)
	for _, id := range isolated {
		if m.find(id) == nil {
			return errors.New("unknown node")
		}
		blocked[id] = true
	}
	if len(blocked) > 0 || delay > 0 || drop > 0 {
		observation := m.observationLocked()
		m.beforeFault = &observation
	}
	m.isolated = blocked
	m.delayMS = delay
	m.drop = drop
	m.publish(events.Event{Time: time.Now(), Type: "NETWORK_CHANGED", Detail: fmt.Sprintf("isolated=%v delay=%dms drop=%d%%", isolated, delay, drop)})
	return nil
}
func (m *Manager) Close() {
	m.mu.Lock()
	if m.closing {
		m.mu.Unlock()
		return
	}
	m.closing = true
	m.cancel()
	if m.workCancel != nil {
		m.workCancel()
	}
	for _, member := range m.members {
		if member.running {
			_ = member.cmd.Process.Kill()
		}
	}
	m.mu.Unlock()
	for _, server := range m.proxies {
		_ = server.Close()
	}
	m.wg.Wait()
}
func (m *Manager) pollLoop() {
	defer m.wg.Done()
	timer := time.NewTicker(400 * time.Millisecond)
	defer timer.Stop()
	for {
		m.poll()
		select {
		case <-m.ctx.Done():
			return
		case <-timer.C:
		}
	}
}
func (m *Manager) poll() {
	var wg sync.WaitGroup
	for _, member := range m.members {
		wg.Add(1)
		go func(member *Member) {
			defer wg.Done()
			ctx, cancel := context.WithTimeout(m.ctx, 300*time.Millisecond)
			defer cancel()
			req, _ := http.NewRequestWithContext(ctx, "GET", member.ClientURL+"/v1/inspect", nil)
			resp, err := m.http.Do(req)
			var inspect raft.Inspection
			ok := false
			if err == nil {
				ok = resp.StatusCode == 200 && json.NewDecoder(resp.Body).Decode(&inspect) == nil
				_ = resp.Body.Close()
			}
			m.mu.Lock()
			defer m.mu.Unlock()
			prior := m.views[member.ID]
			view := prior
			view.ID = member.ID
			view.Online = ok
			view.Running = member.running
			if ok {
				view.Inspection = inspect
				view.SeenAt = time.Now()
				var last uint64
				if prior.BootID == inspect.BootID && len(prior.Events) > 0 {
					last = prior.Events[len(prior.Events)-1].ID
				}
				if len(inspect.Events) > 0 && last > 0 && inspect.Events[0].ID > last+1 {
					m.eventGaps++
				}
				for _, e := range inspect.Events {
					if e.ID > last {
						m.publish(e)
					}
				}
			}
			m.views[member.ID] = view
		}(member)
	}
	wg.Wait()
}
func (m *Manager) Snapshot() Snapshot {
	m.mu.Lock()
	defer m.mu.Unlock()
	s := Snapshot{Time: time.Now(), Nodes: make([]NodeView, 0), Faults: FaultState{Isolated: make([]string, 0), DelayMS: m.delayMS, DropPercent: m.drop}, Events: m.events.After(0), Metrics: m.metrics, EventGaps: m.eventGaps}
	s.BeforeFault = m.beforeFault
	s.Timeline = m.timeline.After(0)
	s.Observed = m.observationLocked()
	var digest string
	var applied uint64
	s.Converged = true
	for i, member := range m.members {
		v := m.views[member.ID]
		v.ID = member.ID
		v.Running = member.running
		v.Events = nil
		s.Nodes = append(s.Nodes, v)
		if !v.Online || !v.Healthy {
			s.Converged = false
		}
		if i == 0 {
			digest = v.StateHash
			applied = v.LastApplied
		} else if v.StateHash != digest || v.LastApplied != applied {
			s.Converged = false
		}
	}
	for id := range m.isolated {
		s.Faults.Isolated = append(s.Faults.Isolated, id)
	}
	sort.Strings(s.Faults.Isolated)
	s.Consistency = "Waiting for all replicas to reach the same applied state"
	if s.Converged {
		s.Consistency = "All observed replicas have the same applied index and state hash"
	}
	var latencies []float64
	count := 0
	for _, sample := range m.samples {
		latencies = append(latencies, sample.ms)
		if time.Since(sample.time) < 10*time.Second {
			count++
		}
	}
	sort.Float64s(latencies)
	if len(latencies) > 0 {
		s.Metrics.P50MS = latencies[(len(latencies)-1)/2]
		s.Metrics.P95MS = latencies[(len(latencies)-1)*95/100]
	}
	s.Metrics.Rate = float64(count) / 10
	return s
}

func (m *Manager) observationLocked() Observation {
	o := Observation{Time: time.Now(), Matched: true, Replicas: make([]ReplicaObservation, 0, len(m.members))}
	for _, member := range m.members {
		v := m.views[member.ID]
		r := ReplicaObservation{member.ID, v.Online, v.Healthy, v.LastApplied, v.StateHash}
		o.Replicas = append(o.Replicas, r)
		first := o.Replicas[0]
		if !r.Online || !r.Healthy || r.Applied != first.Applied || r.Hash != first.Hash {
			o.Matched = false
		}
	}
	return o
}

func (m *Manager) publish(e events.Event) {
	m.events.Publish(e)
	switch e.Type {
	case "PROCESS_STARTED", "PROCESS_EXITED", "NETWORK_CHANGED", "PAUSED", "RESUMED", "DISK_ERROR", "ELECTION_STARTED", "LEADER_ELECTED", "STEPPED_DOWN", "VOTE_GRANTED", "LOG_REPAIRED":
		m.timeline.Publish(e)
	}
}

type CommandRequest struct {
	Operation string `json:"operation"`
	Key       string `json:"key"`
	Value     string `json:"value"`
	Expected  string `json:"expected"`
	ClientID  string `json:"clientId"`
	RequestID uint64 `json:"requestId"`
}
type CommandResponse struct {
	Status    int             `json:"status"`
	Body      json.RawMessage `json:"body"`
	LatencyMS float64         `json:"latencyMs"`
	Server    string          `json:"server"`
}

func (m *Manager) Command(ctx context.Context, request CommandRequest) (CommandResponse, error) {
	method, path, payload, err := commandHTTP(request)
	if err != nil {
		return CommandResponse{}, err
	}
	start := time.Now()
	resp, err := m.client.Do(ctx, method, path, payload)
	ms := float64(time.Since(start).Microseconds()) / 1000
	m.mu.Lock()
	m.metrics.Requests++
	if err != nil || resp.StatusCode >= 500 {
		m.metrics.Failed++
	} else if resp.StatusCode == 409 {
		m.metrics.Conflicts++
	} else if resp.StatusCode < 300 {
		m.metrics.Succeeded++
	}
	m.samples = append(m.samples, sample{time.Now(), ms})
	if len(m.samples) > 300 {
		m.samples = m.samples[len(m.samples)-300:]
	}
	m.mu.Unlock()
	body := resp.Body
	if !json.Valid(body) {
		body = []byte(`{}`)
	}
	if err != nil {
		body, _ = json.Marshal(map[string]string{"error": err.Error()})
		resp.StatusCode = 503
	}
	return CommandResponse{resp.StatusCode, body, ms, resp.Server}, nil
}
func (m *Manager) Workload(enabled bool) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.closing {
		return
	}
	if m.workCancel != nil {
		m.workCancel()
		m.workCancel = nil
	}
	m.metrics.Workload = enabled
	if !enabled {
		return
	}
	ctx, cancel := context.WithCancel(m.ctx)
	m.workCancel = cancel
	m.wg.Add(1)
	go func() {
		defer m.wg.Done()
		client := fmt.Sprintf("workload-%d", time.Now().UnixNano())
		id := uint64(1)
		ticker := time.NewTicker(200 * time.Millisecond)
		defer ticker.Stop()
		for {
			select {
			case <-ctx.Done():
				return
			case <-ticker.C:
				req := CommandRequest{Operation: "SET", Key: "lab:counter", Value: fmt.Sprint(id), ClientID: client, RequestID: id}
				r, _ := m.Command(ctx, req)
				if r.Status == 200 {
					id++
				}
			}
		}
	}()
}
