// Package cluster provides bounded retries and leader discovery for the client API.
package cluster

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strings"
	"sync"
	"time"
)

type Response struct {
	StatusCode int
	Body       []byte
	Server     string
}
type Client struct {
	mu         sync.Mutex
	servers    []string
	preferred  string
	discovered map[string]bool
	http       *http.Client
}

func New(servers []string) (*Client, error) {
	c := &Client{discovered: make(map[string]bool), http: &http.Client{Timeout: 4 * time.Second, CheckRedirect: func(*http.Request, []*http.Request) error { return http.ErrUseLastResponse }}}
	for _, server := range servers {
		server = strings.TrimRight(strings.TrimSpace(server), "/")
		if !validOrigin(server) {
			return nil, fmt.Errorf("invalid server URL %q", server)
		}
		c.add(server)
	}
	if len(c.servers) == 0 {
		return nil, errors.New("at least one server is required")
	}
	return c, nil
}
func validOrigin(server string) bool {
	u, err := url.Parse(server)
	return err == nil && (u.Scheme == "http" || u.Scheme == "https") && u.Host != "" && u.User == nil && u.Path == "" && u.RawQuery == "" && u.Fragment == ""
}
func (c *Client) add(server string) {
	c.mu.Lock()
	defer c.mu.Unlock()
	for _, s := range c.servers {
		if s == server {
			return
		}
	}
	c.servers = append(c.servers, server)
}
func (c *Client) addresses() []string {
	c.mu.Lock()
	defer c.mu.Unlock()
	out := append([]string(nil), c.servers...)
	if c.preferred != "" {
		for i, s := range out {
			if s == c.preferred {
				out[0], out[i] = out[i], out[0]
				break
			}
		}
	}
	return out
}

// Do retries connection failures and 503s with the exact same serialized body.
// The caller must reuse clientId/requestId on a later retry after timeout.
func (c *Client) Do(parent context.Context, method, path string, body []byte) (Response, error) {
	ctx, cancel := context.WithTimeout(parent, 12*time.Second)
	defer cancel()
	var last error
	var response Response
	addresses := c.addresses()
	target := addresses[0]
	cursor := 0
	for attempt := 0; attempt < 40; attempt++ {
		if err := ctx.Err(); err != nil {
			return response, fmt.Errorf("request deadline; outcome may be unknown: %w", err)
		}
		req, err := http.NewRequestWithContext(ctx, method, target+path, bytes.NewReader(body))
		if err != nil {
			return response, err
		}
		req.Header.Set("Content-Type", "application/json")
		resp, err := c.http.Do(req)
		redirect := ""
		if err == nil {
			b, readErr := io.ReadAll(io.LimitReader(resp.Body, 16<<20))
			_ = resp.Body.Close()
			if readErr == nil {
				response = Response{resp.StatusCode, b, target}
				if resp.StatusCode == 307 || resp.StatusCode == 308 {
					location, parseErr := url.Parse(resp.Header.Get("Location"))
					if parseErr == nil {
						origin := location.Scheme + "://" + location.Host
						if validOrigin(origin) {
							redirect = origin
							c.add(origin)
						}
					}
				} else if resp.StatusCode != 503 {
					c.mu.Lock()
					c.preferred = target
					c.mu.Unlock()
					if resp.StatusCode < 300 {
						c.discover(ctx, target)
					}
					return response, nil
				}
				last = fmt.Errorf("server %s returned HTTP %d", target, resp.StatusCode)
			} else {
				last = readErr
			}
		} else {
			last = err
		}
		// Status comes from an explicitly selected/trusted server. It lets a single
		// live seed teach the client all other endpoints before a future failover.
		c.discover(ctx, target)
		if redirect != "" && redirect != target && attempt%2 == 0 {
			target = redirect
		} else {
			addresses = c.addresses()
			cursor++
			target = addresses[cursor%len(addresses)]
		}
		timer := time.NewTimer(75 * time.Millisecond)
		select {
		case <-ctx.Done():
			timer.Stop()
			return response, fmt.Errorf("request deadline; outcome may be unknown: %w", ctx.Err())
		case <-timer.C:
		}
	}
	return response, fmt.Errorf("retry limit reached; outcome may be unknown: %w", last)
}

func (c *Client) discover(parent context.Context, server string) {
	c.mu.Lock()
	already := c.discovered[server]
	c.mu.Unlock()
	if already {
		return
	}
	ctx, cancel := context.WithTimeout(parent, 250*time.Millisecond)
	defer cancel()
	req, err := http.NewRequestWithContext(ctx, "GET", server+"/v1/status", nil)
	if err != nil {
		return
	}
	resp, err := c.http.Do(req)
	if err != nil {
		return
	}
	defer resp.Body.Close()
	if resp.StatusCode != 200 {
		return
	}
	var status struct {
		Peers []struct {
			ClientURL string `json:"clientUrl"`
		} `json:"peers"`
	}
	if json.NewDecoder(io.LimitReader(resp.Body, 1<<20)).Decode(&status) != nil {
		return
	}
	for _, peer := range status.Peers {
		if validOrigin(peer.ClientURL) {
			c.add(peer.ClientURL)
		}
	}
	c.mu.Lock()
	c.discovered[server] = true
	c.mu.Unlock()
}
