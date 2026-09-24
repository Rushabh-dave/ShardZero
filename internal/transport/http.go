// Package transport carries typed Raft RPCs on a separate peer HTTP listener.
package transport

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"

	"github.com/Rushabh-dave/shardzero/internal/raft"
)

type HTTP struct{ Client *http.Client }

func New() *HTTP {
	return &HTTP{Client: &http.Client{CheckRedirect: func(*http.Request, []*http.Request) error { return http.ErrUseLastResponse }}}
}
func (h *HTTP) call(ctx context.Context, peer raft.Peer, path string, input, output any) error {
	b, err := json.Marshal(input)
	if err != nil {
		return err
	}
	req, err := http.NewRequestWithContext(ctx, "POST", peer.PeerURL+path, bytes.NewReader(b))
	if err != nil {
		return err
	}
	req.Header.Set("Content-Type", "application/json")
	switch r := input.(type) {
	case raft.VoteRequest:
		req.Header.Set("X-ShardZero-From", r.CandidateID)
		req.Header.Set("X-ShardZero-Message", r.MessageID)
	case raft.AppendRequest:
		req.Header.Set("X-ShardZero-From", r.LeaderID)
		req.Header.Set("X-ShardZero-Message", r.MessageID)
	}
	resp, err := h.Client.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	if resp.StatusCode != 200 {
		return fmt.Errorf("peer returned %s", resp.Status)
	}
	return json.NewDecoder(io.LimitReader(resp.Body, 1<<20)).Decode(output)
}
func (h *HTTP) RequestVote(ctx context.Context, p raft.Peer, r raft.VoteRequest) (raft.VoteResponse, error) {
	var out raft.VoteResponse
	err := h.call(ctx, p, "/raft/vote", r, &out)
	return out, err
}
func (h *HTTP) AppendEntries(ctx context.Context, p raft.Peer, r raft.AppendRequest) (raft.AppendResponse, error) {
	var out raft.AppendResponse
	err := h.call(ctx, p, "/raft/append", r, &out)
	return out, err
}

func Handler(node *raft.Node) http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("POST /raft/vote", func(w http.ResponseWriter, r *http.Request) {
		var req raft.VoteRequest
		if !decode(w, r, &req) {
			return
		}
		out, err := node.RequestVote(req)
		reply(w, out, err)
	})
	mux.HandleFunc("POST /raft/append", func(w http.ResponseWriter, r *http.Request) {
		var req raft.AppendRequest
		if !decode(w, r, &req) {
			return
		}
		out, err := node.AppendEntries(req)
		reply(w, out, err)
	})
	return mux
}
func decode(w http.ResponseWriter, r *http.Request, v any) bool {
	defer r.Body.Close()
	r.Body = http.MaxBytesReader(w, r.Body, 16<<20)
	d := json.NewDecoder(r.Body)
	d.DisallowUnknownFields()
	if err := d.Decode(v); err != nil {
		http.Error(w, "invalid RPC", 400)
		return false
	}
	if err := d.Decode(new(any)); !errors.Is(err, io.EOF) {
		http.Error(w, "trailing RPC data", 400)
		return false
	}
	return true
}
func reply(w http.ResponseWriter, out any, err error) {
	w.Header().Set("Content-Type", "application/json")
	if err != nil {
		w.WriteHeader(503)
		_ = json.NewEncoder(w).Encode(map[string]string{"error": err.Error()})
		return
	}
	_ = json.NewEncoder(w).Encode(out)
}
