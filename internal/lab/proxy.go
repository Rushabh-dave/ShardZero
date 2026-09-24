package lab

import (
	"github.com/Rushabh-dave/shardzero/internal/events"
	"io"
	"net/http"
	"time"
)

func (m *Manager) proxy(target *Member) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != "POST" || (r.URL.Path != "/raft/vote" && r.URL.Path != "/raft/append") {
			w.WriteHeader(404)
			return
		}
		from := r.Header.Get("X-ShardZero-From")
		message := r.Header.Get("X-ShardZero-Message")
		m.mu.Lock()
		known := m.find(from) != nil
		m.packet++
		drop := m.packet%100 < uint64(m.drop)
		blocked := m.isolated[from] || m.isolated[target.ID]
		delay := m.delayMS
		m.mu.Unlock()
		if !known {
			w.WriteHeader(403)
			return
		}
		if blocked || drop {
			m.events.Publish(events.Event{Time: time.Now(), NodeID: from, Peer: target.ID, Type: "MESSAGE_DROPPED", MessageID: message})
			http.Error(w, "injected network loss", 503)
			return
		}
		if delay > 0 {
			m.events.Publish(events.Event{Time: time.Now(), NodeID: from, Peer: target.ID, Type: "MESSAGE_DELAYED", MessageID: message})
			timer := time.NewTimer(time.Duration(delay) * time.Millisecond)
			defer timer.Stop()
			select {
			case <-r.Context().Done():
				return
			case <-timer.C:
			}
		}
		// Re-check partitions at delivery, after any intentional delay.
		m.mu.Lock()
		blocked = m.isolated[from] || m.isolated[target.ID]
		m.mu.Unlock()
		if blocked {
			w.WriteHeader(503)
			return
		}
		r.Body = http.MaxBytesReader(w, r.Body, 16<<20)
		req, err := http.NewRequestWithContext(r.Context(), "POST", "http://"+target.PeerAddress+r.URL.Path, r.Body)
		if err != nil {
			w.WriteHeader(502)
			return
		}
		req.Header.Set("Content-Type", "application/json")
		resp, err := m.http.Do(req)
		if err != nil {
			w.WriteHeader(503)
			return
		}
		defer resp.Body.Close()
		m.mu.Lock()
		blocked = m.isolated[from] || m.isolated[target.ID]
		m.mu.Unlock()
		if blocked {
			w.WriteHeader(503)
			return
		}
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(resp.StatusCode)
		_, _ = io.Copy(w, io.LimitReader(resp.Body, 1<<20))
	})
}
