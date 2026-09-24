package main

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"net/url"
	"os"
	"strings"
	"time"

	"github.com/Rushabh-dave/shardzero/internal/cluster"
)

func main() {
	if err := run(); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}
func run() error {
	server := flag.String("server", "http://127.0.0.1:7001", "node URL")
	servers := flag.String("servers", "", "comma-separated seed URLs for failover")
	timeout := flag.Duration("timeout", 12*time.Second, "overall request deadline")
	clientID := flag.String("client", "", "stable client ID for retries (default: random)")
	requestID := flag.Uint64("request", 1, "positive request ID; increase per client")
	flag.Parse()
	args := flag.Args()
	if len(args) == 0 {
		return errors.New("usage: client [flags] SET key value | GET key | DELETE key | CAS key expected value | STATUS")
	}
	op := strings.ToUpper(args[0])
	method, path := "GET", "/v1/status"
	var body []byte
	if op != "STATUS" {
		arity := map[string]int{"GET": 2, "SET": 3, "DELETE": 2, "CAS": 4}
		if arity[op] == 0 || len(args) != arity[op] {
			return errors.New("invalid command or argument count")
		}
		path = "/v1/kv/" + url.PathEscape(args[1])
		if op != "GET" {
			if *clientID == "" {
				var id [16]byte
				if _, err := rand.Read(id[:]); err != nil {
					return err
				}
				*clientID = hex.EncodeToString(id[:])
			}
			payload := map[string]any{"clientId": *clientID, "requestId": *requestID}
			switch op {
			case "SET":
				method = "PUT"
				payload["value"] = args[2]
			case "DELETE":
				method = "DELETE"
			case "CAS":
				method = "POST"
				path += "/cas"
				payload["expected"] = args[2]
				payload["value"] = args[3]
			}
			encoded, err := json.Marshal(payload)
			if err != nil {
				return err
			}
			body = encoded
			fmt.Fprintf(os.Stderr, "clientId=%s requestId=%d (reuse both for retry)\n", *clientID, *requestID)
		}
	} else if len(args) != 1 {
		return errors.New("STATUS takes no arguments")
	}
	seeds := []string{*server}
	if *servers != "" {
		seeds = strings.Split(*servers, ",")
	}
	client, err := cluster.New(seeds)
	if err != nil {
		return err
	}
	ctx, cancel := context.WithTimeout(context.Background(), *timeout)
	defer cancel()
	resp, err := client.Do(ctx, method, path, body)
	if err != nil {
		return fmt.Errorf("request failed; write outcome may be unknown: %w", err)
	}
	if _, err := os.Stdout.Write(resp.Body); err != nil {
		return err
	}
	if resp.StatusCode >= 300 {
		return fmt.Errorf("HTTP %d", resp.StatusCode)
	}
	return nil
}
