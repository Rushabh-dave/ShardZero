package api

import (
	"bytes"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"sync"
	"sync/atomic"
	"testing"

	"github.com/Rushabh-dave/shardzero/internal/database"
)

func TestHTTPContract(t *testing.T) {
	db, err := database.Open(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	h := Handler(db, "test")
	tests := []struct {
		method, path, body string
		status             int
	}{
		{"GET", "/health", "", 200},
		{"GET", "/v1/status", "", 200},
		{"GET", "/v1/kv/score", "", 404},
		{"PUT", "/v1/kv/score", `{"clientId":"a","requestId":1,"value":"950"}`, 200},
		{"PUT", "/v1/kv/score", `{"clientId":"a","requestId":1,"value":"950"}`, 200},
		{"PUT", "/v1/kv/score", `{"clientId":"a","requestId":1,"value":"other"}`, 409},
		{"POST", "/v1/kv/score/cas", `{"clientId":"a","requestId":2,"value":"1000","expected":"950"}`, 200},
		{"POST", "/v1/kv/score/cas", `{"clientId":"a","requestId":3,"value":"1","expected":"950"}`, 409},
		{"GET", "/v1/kv/score", "", 200},
		{"PUT", "/v1/kv/score", `{"clientId":"a","requestId":4}`, 400},
		{"PUT", "/v1/kv/score", `{"clientId":"a","requestId":4,"value":"x","extra":true}`, 400},
		{"PUT", "/v1/kv/score", `{"clientId":"a","requestId":4,"value":"x"} {}`, 400},
		{"PUT", "/v1/kv/score", `null`, 400},
		{"POST", "/v1/kv/score/cas", `{"clientId":"a","requestId":4,"value":"x"}`, 400},
		{"DELETE", "/v1/kv/score", `{"clientId":"a","requestId":4}`, 200},
		{"GET", "/v1/kv/score", "", 404},
		{"PUT", "/v1/kv/empty", `{"clientId":"a","requestId":5,"value":""}`, 200},
		{"GET", "/v1/kv/empty", "", 200},
		{"PATCH", "/v1/kv/empty", `{}`, 405},
	}
	for _, tt := range tests {
		r := httptest.NewRequest(tt.method, tt.path, bytes.NewBufferString(tt.body))
		w := httptest.NewRecorder()
		h.ServeHTTP(w, r)
		if w.Code != tt.status {
			t.Errorf("%s %s %s: got %d %s", tt.method, tt.path, tt.body, w.Code, w.Body.String())
		}
	}
}

func TestConcurrentHTTPCompareAndSet(t *testing.T) {
	db, err := database.Open(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	s := httptest.NewServer(Handler(db, "test"))
	defer s.Close()
	put := httptest.NewRecorder()
	Handler(db, "test").ServeHTTP(put, httptest.NewRequest("PUT", "/v1/kv/score", bytes.NewBufferString(`{"clientId":"initial","requestId":1,"value":"0"}`)))
	if put.Code != 200 {
		t.Fatal(put.Body.String())
	}
	var winners atomic.Int32
	var wg sync.WaitGroup
	for i := 0; i < 32; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			payload, _ := json.Marshal(map[string]any{"clientId": fmt.Sprint(i), "requestId": 1, "expected": "0", "value": "1"})
			resp, err := http.Post(s.URL+"/v1/kv/score/cas", "application/json", bytes.NewReader(payload))
			if err != nil {
				t.Error(err)
				return
			}
			defer resp.Body.Close()
			if resp.StatusCode == 200 {
				winners.Add(1)
			} else if resp.StatusCode != 409 {
				t.Error(resp.Status)
			}
		}(i)
	}
	wg.Wait()
	if winners.Load() != 1 {
		t.Fatalf("CAS winners: %d", winners.Load())
	}
}
