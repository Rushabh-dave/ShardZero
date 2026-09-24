package cluster

import (
	"context"
	"io"
	"net/http"
	"net/http/httptest"
	"sync/atomic"
	"testing"
	"time"
)

func TestRedirectRetryKeepsCommandAndRequestIdentity(t *testing.T) {
	body := []byte(`{"clientId":"same-client","requestId":42,"value":"data"}`)
	var attempts atomic.Int32
	leader := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/v1/status" {
			_, _ = w.Write([]byte(`{"peers":[]}`))
			return
		}
		b, _ := io.ReadAll(r.Body)
		if string(b) != string(body) || r.Method != "PUT" {
			t.Errorf("retry changed request: %s %s", r.Method, b)
		}
		if attempts.Add(1) == 1 {
			connection, _, err := w.(http.Hijacker).Hijack()
			if err != nil {
				t.Error(err)
				return
			}
			_ = connection.Close()
			return
		}
		_, _ = w.Write([]byte(`{"applied":true}`))
	}))
	defer leader.Close()
	follower := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Location", leader.URL+r.URL.RequestURI())
		w.WriteHeader(307)
	}))
	defer follower.Close()
	c, err := New([]string{follower.URL, leader.URL})
	if err != nil {
		t.Fatal(err)
	}
	resp, err := c.Do(context.Background(), "PUT", "/v1/kv/key", body)
	if err != nil || resp.StatusCode != 200 || attempts.Load() != 2 {
		t.Fatal(resp, err, attempts.Load())
	}
}

func TestRetriesAreBoundedAndApplicationErrorsAreTerminal(t *testing.T) {
	var attempts atomic.Int32
	s := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { attempts.Add(1); w.WriteHeader(409) }))
	defer s.Close()
	c, _ := New([]string{s.URL})
	r, err := c.Do(context.Background(), "POST", "/v1/kv/key/cas", nil)
	if err != nil || r.StatusCode != 409 || attempts.Load() != 1 {
		t.Fatal(r, err, attempts.Load())
	}
	unavailable := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { w.WriteHeader(503) }))
	defer unavailable.Close()
	c, _ = New([]string{unavailable.URL})
	ctx, cancel := context.WithTimeout(context.Background(), 180*time.Millisecond)
	defer cancel()
	start := time.Now()
	_, err = c.Do(ctx, "GET", "/v1/kv/key", nil)
	if err == nil || time.Since(start) > time.Second {
		t.Fatal("request did not honor deadline", err)
	}
}

func TestDiscoverOtherServersFromLiveSeed(t *testing.T) {
	leader := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { _, _ = w.Write([]byte(`{"value":"yes"}`)) }))
	defer leader.Close()
	seed := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/v1/status" {
			_, _ = w.Write([]byte(`{"peers":[{"clientUrl":"` + leader.URL + `"}]}`))
			return
		}
		w.WriteHeader(503)
	}))
	defer seed.Close()
	c, _ := New([]string{seed.URL})
	r, err := c.Do(context.Background(), "GET", "/v1/kv/key", nil)
	if err != nil || r.StatusCode != 200 || r.Server != leader.URL {
		t.Fatal(r, err)
	}
}

func TestSingleSeedLearnsPeersBeforeThatSeedFails(t *testing.T) {
	other := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { _, _ = w.Write([]byte(`{"value":"current"}`)) }))
	defer other.Close()
	seed := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/v1/status" {
			_, _ = w.Write([]byte(`{"peers":[{"clientUrl":"` + other.URL + `"}]}`))
			return
		}
		_, _ = w.Write([]byte(`{"value":"current"}`))
	}))
	defer seed.Close()
	c, _ := New([]string{seed.URL})
	if _, err := c.Do(context.Background(), "GET", "/v1/kv/key", nil); err != nil {
		t.Fatal(err)
	}
	seed.Close()
	r, err := c.Do(context.Background(), "GET", "/v1/kv/key", nil)
	if err != nil || r.Server != other.URL {
		t.Fatal(r, err)
	}
}
