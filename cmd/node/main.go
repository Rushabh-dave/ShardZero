package main

import (
	"context"
	"errors"
	"flag"
	"log/slog"
	"net"
	"net/http"
	"os"
	"os/signal"
	"path/filepath"
	"syscall"
	"time"

	"github.com/Rushabh-dave/shardzero/internal/api"
	"github.com/Rushabh-dave/shardzero/internal/database"
	"github.com/Rushabh-dave/shardzero/internal/raft"
	"github.com/Rushabh-dave/shardzero/internal/transport"
)

func main() {
	if err := run(); err != nil {
		slog.Error("node stopped", "error", err)
		os.Exit(1)
	}
}

func run() error {
	id := flag.String("id", "node-1", "node identifier")
	listen := flag.String("listen", "127.0.0.1:7001", "HTTP listen address")
	dir := flag.String("data", "", "exclusive data directory (default: data/<id> or data/raft/<id>)")
	clusterFile := flag.String("cluster", "", "cluster membership JSON; omit for standalone mode")
	peerListen := flag.String("peer-listen", "127.0.0.1:8001", "internal Raft listen address")
	fixedLeader := flag.String("fixed-leader", "", "Phase 3 mode: same fixed leader ID on every member")
	snapshotEvery := flag.Uint64("snapshot-every", 128, "compact a fully replicated committed prefix every N entries (0 disables)")
	adminListen := flag.String("admin-listen", "", "optional loopback-only lab controls; requires SHARDZERO_ADMIN_TOKEN")
	flag.Parse()
	slog.SetDefault(slog.New(slog.NewJSONHandler(os.Stderr, nil)))
	mode := "standalone"
	var handler http.Handler
	var peerServer *http.Server
	var peerListener net.Listener
	var adminServer *http.Server
	var adminListener net.Listener
	if *dir == "" {
		*dir = filepath.Join("data", *id)
		if *clusterFile != "" {
			*dir = filepath.Join("data", "raft", *id)
		}
	}
	if *clusterFile == "" {
		if *fixedLeader != "" {
			return errors.New("--fixed-leader requires --cluster")
		}
		db, err := database.Open(*dir)
		if err != nil {
			return err
		}
		defer db.Close()
		handler = api.Handler(db, *id)
	} else {
		membership, err := raft.LoadMembership(*clusterFile)
		if err != nil {
			return err
		}
		peerListener, err = net.Listen("tcp", *peerListen)
		if err != nil {
			return err
		}
		defer peerListener.Close()
		node, err := raft.Open(raft.Config{ID: *id, ClusterID: membership.ClusterID, DataDir: *dir, Peers: membership.Peers, FixedLeader: *fixedLeader, SnapshotEvery: *snapshotEvery}, transport.New())
		if err != nil {
			return err
		}
		defer node.Close()
		handler = api.ClusterHandler(node)
		if *adminListen != "" {
			host, _, err := net.SplitHostPort(*adminListen)
			if err != nil || net.ParseIP(host) == nil || !net.ParseIP(host).IsLoopback() {
				return errors.New("admin listener must use a loopback IP")
			}
			token := os.Getenv("SHARDZERO_ADMIN_TOKEN")
			if token == "" {
				return errors.New("admin token required")
			}
			adminListener, err = net.Listen("tcp", *adminListen)
			if err != nil {
				return err
			}
			defer adminListener.Close()
			adminServer = newServer(*adminListen, api.AdminHandler(node, token))
			defer adminServer.Close()
		}
		peerServer = newServer(*peerListen, transport.Handler(node))
		mode = node.Status().Mode
	}
	listener, err := net.Listen("tcp", *listen)
	if err != nil {
		return err
	}
	defer listener.Close()
	server := newServer(*listen, handler)
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	done := make(chan error, 3)
	go func() { done <- server.Serve(listener) }()
	defer server.Close()
	if peerServer != nil {
		go func() { done <- peerServer.Serve(peerListener) }()
		defer peerServer.Close()
	}
	if adminServer != nil {
		go func() { done <- adminServer.Serve(adminListener) }()
	}
	slog.Info("starting node", "nodeId", *id, "mode", mode, "listen", *listen, "peerListen", *peerListen)
	select {
	case err := <-done:
		if errors.Is(err, http.ErrServerClosed) {
			return nil
		}
		return err
	case <-ctx.Done():
		shutdown, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		if err := server.Shutdown(shutdown); err != nil {
			_ = server.Close()
			return err
		}
		if peerServer != nil {
			if err := peerServer.Shutdown(shutdown); err != nil {
				return err
			}
		}
		if adminServer != nil {
			return adminServer.Shutdown(shutdown)
		}
		return nil
	}
}

func newServer(address string, handler http.Handler) *http.Server {
	return &http.Server{Addr: address, Handler: handler, ReadHeaderTimeout: 5 * time.Second, ReadTimeout: 15 * time.Second, WriteTimeout: 30 * time.Second, IdleTimeout: 60 * time.Second, MaxHeaderBytes: 16 << 10}
}
