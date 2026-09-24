package api

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"strconv"

	"github.com/Rushabh-dave/shardzero/internal/database"
	"github.com/Rushabh-dave/shardzero/internal/raft"
	"github.com/Rushabh-dave/shardzero/internal/statemachine"
)

type writeRequest struct {
	ClientID  string  `json:"clientId"`
	RequestID uint64  `json:"requestId"`
	Value     *string `json:"value"`
	Expected  *string `json:"expected"`
}

func Handler(db *database.Database, nodeID string) http.Handler {
	return handler(func(_ context.Context, c statemachine.Command) (statemachine.Result, error) { return db.Execute(c) }, func(_ context.Context, k string) (string, bool, error) { return db.Get(k) }, func() any {
		return struct {
			NodeID string `json:"nodeId"`
			Mode   string `json:"mode"`
			database.Stats
		}{nodeID, "standalone", db.Stats()}
	})
}

func ClusterHandler(node *raft.Node) http.Handler {
	mux := http.NewServeMux()
	mux.Handle("/", handler(node.Execute, node.Get, func() any { return node.Status() }))
	mux.HandleFunc("GET /v1/inspect", func(w http.ResponseWriter, r *http.Request) {
		after, _ := strconv.ParseUint(r.URL.Query().Get("after"), 10, 64)
		respond(w, 200, node.Inspect(after))
	})
	return mux
}

// AdminHandler is served only on an explicitly enabled loopback listener.
func AdminHandler(node *raft.Node, token string) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if token == "" || r.Header.Get("X-ShardZero-Admin") != token {
			apiError(w, 403, "forbidden", "admin token required")
			return
		}
		if r.Method != "POST" {
			w.WriteHeader(405)
			return
		}
		switch r.URL.Path {
		case "/admin/pause":
			node.SetPaused(true)
		case "/admin/resume":
			node.SetPaused(false)
		default:
			w.WriteHeader(404)
			return
		}
		respond(w, 200, node.Status())
	})
}

func handler(execute func(context.Context, statemachine.Command) (statemachine.Result, error), get func(context.Context, string) (string, bool, error), status func() any) http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("GET /health", func(w http.ResponseWriter, r *http.Request) {
		respond(w, http.StatusOK, map[string]string{"status": "alive"})
	})
	mux.HandleFunc("GET /v1/status", func(w http.ResponseWriter, r *http.Request) {
		respond(w, http.StatusOK, status())
	})
	mux.HandleFunc("GET /v1/kv/{key}", func(w http.ResponseWriter, r *http.Request) {
		key := r.PathValue("key")
		if len(key) > 1024 {
			apiError(w, http.StatusBadRequest, "invalid_key", "key exceeds 1024 bytes")
			return
		}
		value, found, err := get(r.Context(), key)
		if err != nil {
			handleRequestError(w, r, err)
			return
		}
		status := http.StatusOK
		if !found {
			status = http.StatusNotFound
		}
		respond(w, status, statemachine.Result{Value: value, Found: found})
	})
	write := func(operation string) http.HandlerFunc {
		return func(w http.ResponseWriter, r *http.Request) {
			defer r.Body.Close()
			r.Body = http.MaxBytesReader(w, r.Body, 16<<20)
			decoder := json.NewDecoder(r.Body)
			decoder.DisallowUnknownFields()
			var body writeRequest
			if err := decoder.Decode(&body); err != nil {
				apiError(w, http.StatusBadRequest, "invalid_json", "expected a JSON command body")
				return
			}
			if err := decoder.Decode(new(any)); !errors.Is(err, io.EOF) {
				apiError(w, http.StatusBadRequest, "invalid_json", "body must contain exactly one JSON object")
				return
			}
			if (operation == "SET" || operation == "CAS") && body.Value == nil || operation == "CAS" && body.Expected == nil {
				apiError(w, http.StatusBadRequest, "invalid_command", "value is required; CAS also requires expected")
				return
			}
			if operation == "DELETE" && (body.Value != nil || body.Expected != nil) || operation == "SET" && body.Expected != nil {
				apiError(w, http.StatusBadRequest, "invalid_command", "unexpected value or expected field")
				return
			}
			command := statemachine.Command{Operation: operation, Key: r.PathValue("key"), ClientID: body.ClientID, RequestID: body.RequestID}
			if body.Value != nil {
				command.Value = *body.Value
			}
			if body.Expected != nil {
				command.Expected = *body.Expected
			}
			result, err := execute(r.Context(), command)
			if err != nil {
				handleRequestError(w, r, err)
				return
			}
			status := http.StatusOK
			if result.Conflict {
				status = http.StatusConflict
			}
			respond(w, status, result)
		}
	}
	mux.HandleFunc("PUT /v1/kv/{key}", write("SET"))
	mux.HandleFunc("DELETE /v1/kv/{key}", write("DELETE"))
	mux.HandleFunc("POST /v1/kv/{key}/cas", write("CAS"))
	return mux
}

func handleRequestError(w http.ResponseWriter, r *http.Request, err error) {
	var notLeader *raft.NotLeaderError
	if errors.As(err, &notLeader) {
		if notLeader.LeaderURL != "" {
			w.Header().Set("Location", notLeader.LeaderURL+r.URL.RequestURI())
			respond(w, 307, map[string]string{"error": "not_leader", "leaderId": notLeader.LeaderID, "leaderUrl": notLeader.LeaderURL})
		} else {
			apiError(w, 503, "leader_unknown", "leader election in progress; retry the same request")
		}
		return
	}
	if errors.Is(err, raft.ErrNoQuorum) {
		apiError(w, 503, "no_quorum", err.Error())
		return
	}
	handleError(w, err)
}

func handleError(w http.ResponseWriter, err error) {
	switch {
	case errors.Is(err, statemachine.ErrInvalid):
		apiError(w, 400, "invalid_command", err.Error())
	case errors.Is(err, statemachine.ErrStale):
		apiError(w, 409, "stale_request", err.Error())
	case errors.Is(err, statemachine.ErrReused):
		apiError(w, 409, "request_id_reused", err.Error())
	default:
		apiError(w, 503, "storage_unavailable", "storage unavailable; outcome may be unknown, restart and retry the same request")
	}
}

func apiError(w http.ResponseWriter, status int, code, message string) {
	respond(w, status, map[string]string{"error": code, "message": message})
}
func respond(w http.ResponseWriter, status int, value any) {
	w.Header().Set("Content-Type", "application/json")
	w.Header().Set("Cache-Control", "no-store")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(value)
}
