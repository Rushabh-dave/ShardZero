package lab

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/url"
	"strings"
	"time"

	"github.com/Rushabh-dave/shardzero/internal/simulation"
	"github.com/Rushabh-dave/shardzero/internal/statemachine"
)

func commandHTTP(c CommandRequest) (string, string, []byte, error) {
	c.Operation = strings.ToUpper(c.Operation)
	if c.Key == "" || len(c.Key) > 1024 {
		return "", "", nil, errors.New("key must contain 1–1024 bytes")
	}
	path := "/v1/kv/" + url.PathEscape(c.Key)
	if c.Operation == "GET" {
		return "GET", path, nil, nil
	}
	command := statemachine.Command{ClientID: c.ClientID, RequestID: c.RequestID, Operation: c.Operation, Key: c.Key, Value: c.Value, Expected: c.Expected}
	if err := statemachine.Validate(command); err != nil {
		return "", "", nil, err
	}
	body := map[string]any{"clientId": c.ClientID, "requestId": c.RequestID}
	method := ""
	switch c.Operation {
	case "SET":
		method = "PUT"
		body["value"] = c.Value
	case "DELETE":
		method = "DELETE"
	case "CAS":
		method = "POST"
		path += "/cas"
		body["value"] = c.Value
		body["expected"] = c.Expected
	}
	b, err := json.Marshal(body)
	return method, path, b, err
}
func (m *Manager) Handler() http.Handler {
	mux := http.NewServeMux()
	simGate := make(chan struct{}, 1)
	mux.HandleFunc("GET /api/session", func(w http.ResponseWriter, r *http.Request) { jsonReply(w, 200, map[string]string{"token": m.token}) })
	mux.HandleFunc("GET /api/state", func(w http.ResponseWriter, r *http.Request) { jsonReply(w, 200, m.Snapshot()) })
	mux.HandleFunc("GET /api/events", func(w http.ResponseWriter, r *http.Request) {
		flusher, ok := w.(http.Flusher)
		if !ok {
			http.Error(w, "streaming unavailable", 500)
			return
		}
		w.Header().Set("Content-Type", "text/event-stream")
		w.Header().Set("Cache-Control", "no-cache")
		w.Header().Set("X-Accel-Buffering", "no")
		tick := time.NewTicker(500 * time.Millisecond)
		defer tick.Stop()
		for {
			b, _ := json.Marshal(m.Snapshot())
			if _, err := fmt.Fprintf(w, "event: state\ndata: %s\n\n", b); err != nil {
				return
			}
			flusher.Flush()
			select {
			case <-r.Context().Done():
				return
			case <-m.ctx.Done():
				return
			case <-tick.C:
			}
		}
	})
	mux.HandleFunc("POST /api/command", func(w http.ResponseWriter, r *http.Request) {
		var c CommandRequest
		if !readJSON(w, r, &c) {
			return
		}
		ctx, cancel := context.WithTimeout(r.Context(), 10*time.Second)
		defer cancel()
		result, err := m.Command(ctx, c)
		if err != nil {
			jsonReply(w, 400, map[string]string{"error": err.Error()})
			return
		}
		jsonReply(w, 200, result)
	})
	mux.HandleFunc("POST /api/chaos", func(w http.ResponseWriter, r *http.Request) {
		var request struct {
			Action      string   `json:"action"`
			Node        string   `json:"node"`
			Isolated    []string `json:"isolated"`
			DelayMS     int      `json:"delayMs"`
			DropPercent int      `json:"dropPercent"`
		}
		if !readJSON(w, r, &request) {
			return
		}
		var err error
		switch request.Action {
		case "crash":
			err = m.Crash(request.Node)
		case "restart":
			err = m.Start(request.Node)
		case "pause":
			err = m.Pause(r.Context(), request.Node, true)
		case "resume":
			err = m.Pause(r.Context(), request.Node, false)
		case "network":
			err = m.SetFaults(request.Isolated, request.DelayMS, request.DropPercent)
		case "heal":
			err = m.SetFaults(nil, 0, 0)
		case "reset":
			err = m.SetFaults(nil, 0, 0)
			for _, n := range m.members {
				if e := m.Start(n.ID); e != nil {
					err = errors.Join(err, e)
				}
				_ = m.Pause(r.Context(), n.ID, false)
			}
		default:
			err = errors.New("unknown action")
		}
		if err != nil {
			jsonReply(w, 400, map[string]string{"error": err.Error()})
			return
		}
		jsonReply(w, 200, map[string]bool{"ok": true})
	})
	mux.HandleFunc("POST /api/workload", func(w http.ResponseWriter, r *http.Request) {
		var request struct {
			Enabled bool `json:"enabled"`
		}
		if !readJSON(w, r, &request) {
			return
		}
		m.Workload(request.Enabled)
		jsonReply(w, 200, map[string]bool{"enabled": request.Enabled})
	})
	mux.HandleFunc("POST /api/simulate", func(w http.ResponseWriter, r *http.Request) {
		select {
		case simGate <- struct{}{}:
			defer func() { <-simGate }()
		default:
			jsonReply(w, 409, map[string]string{"error": "a simulation is already running"})
			return
		}
		var request struct {
			Seed       uint64               `json:"seed"`
			Nodes      int                  `json:"nodes"`
			Operations int                  `json:"operations"`
			Scenario   *simulation.Scenario `json:"scenario,omitempty"`
		}
		if !readJSON(w, r, &request) {
			return
		}
		s := simulation.Default(request.Seed, request.Nodes, request.Operations)
		if request.Scenario != nil {
			s = *request.Scenario
		}
		if s.Operations > 500 {
			jsonReply(w, 400, map[string]string{"error": "dashboard simulation limit is 500 operations; use the CLI for larger runs"})
			return
		}
		if err := s.Validate(); err != nil {
			jsonReply(w, 400, map[string]string{"error": err.Error()})
			return
		}
		ctx, cancel := context.WithTimeout(r.Context(), 45*time.Second)
		defer cancel()
		result := simulation.Run(ctx, s)
		jsonReply(w, 200, result)
	})
	mux.Handle("/", http.FileServer(http.Dir(m.cfg.Assets)))
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		host, _, err := net.SplitHostPort(r.Host)
		if err != nil || (host != "127.0.0.1" && host != "localhost" && host != "::1") {
			http.Error(w, "loopback host required", 403)
			return
		}
		if origin := r.Header.Get("Origin"); origin != "" && origin != "http://"+r.Host {
			http.Error(w, "same origin required", 403)
			return
		}
		if r.Method != "GET" && r.Method != "HEAD" && r.Header.Get("X-ShardZero-Control") != m.token {
			http.Error(w, "control token required", 403)
			return
		}
		w.Header().Set("X-Content-Type-Options", "nosniff")
		mux.ServeHTTP(w, r)
	})
}
func readJSON(w http.ResponseWriter, r *http.Request, value any) bool {
	defer r.Body.Close()
	r.Body = http.MaxBytesReader(w, r.Body, 2<<20)
	d := json.NewDecoder(r.Body)
	d.DisallowUnknownFields()
	if err := d.Decode(value); err != nil {
		jsonReply(w, 400, map[string]string{"error": "invalid request JSON"})
		return false
	}
	if err := d.Decode(new(any)); !errors.Is(err, io.EOF) {
		jsonReply(w, 400, map[string]string{"error": "trailing request data"})
		return false
	}
	return true
}
func jsonReply(w http.ResponseWriter, status int, value any) {
	w.Header().Set("Content-Type", "application/json")
	w.Header().Set("Cache-Control", "no-store")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(value)
}
