// Command lab runs the local dashboard, fault proxies, and independent nodes.
package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"github.com/Rushabh-dave/shardzero/internal/lab"
	"net"
	"net/http"
	"os"
	"os/exec"
	"os/signal"
	"path/filepath"
	"runtime"
	"syscall"
	"time"
)

func main() {
	if err := run(); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}
func run() error {
	listen := flag.String("listen", "127.0.0.1:9100", "dashboard loopback address")
	base := flag.Int("base-port", 17000, "node/proxy port range base")
	nodes := flag.Int("nodes", 3, "3 or 5 independent node processes")
	data := flag.String("data", "data/lab", "lab-only data directory")
	buildUI := flag.Bool("build-ui", true, "build dashboard before starting")
	flag.Parse()
	host, _, err := net.SplitHostPort(*listen)
	if err != nil || net.ParseIP(host) == nil || !net.ParseIP(host).IsLoopback() {
		return errors.New("lab must bind a loopback IP")
	}
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	if *buildUI {
		if _, err := os.Stat("dashboard/node_modules"); os.IsNotExist(err) {
			if err := npm(ctx, "ci"); err != nil {
				return err
			}
		}
		if err := npm(ctx, "run", "build"); err != nil {
			return err
		}
	}
	if _, err := os.Stat("dashboard/dist/index.html"); err != nil {
		return errors.New("dashboard missing: run npm --prefix dashboard ci and npm --prefix dashboard run build")
	}
	if err := os.MkdirAll("bin", 0700); err != nil {
		return err
	}
	name := "lab-node"
	if runtime.GOOS == "windows" {
		name += ".exe"
	}
	binary, _ := filepath.Abs(filepath.Join("bin", name))
	build := exec.CommandContext(ctx, "go", "build", "-o", binary, "./cmd/node")
	build.Stdout = os.Stdout
	build.Stderr = os.Stderr
	if err := build.Run(); err != nil {
		return err
	}
	listener, err := net.Listen("tcp", *listen)
	if err != nil {
		return err
	}
	defer listener.Close()
	manager, err := lab.New(lab.Config{Nodes: *nodes, BasePort: *base, DataDir: *data, NodeBinary: binary, Assets: "dashboard/dist"})
	if err != nil {
		return err
	}
	defer manager.Close()
	server := &http.Server{Handler: manager.Handler(), ReadHeaderTimeout: 5 * time.Second, IdleTimeout: 60 * time.Second}
	defer server.Close()
	done := make(chan error, 1)
	go func() { done <- server.Serve(listener) }()
	fmt.Printf("ShardZero lab: http://%s\n%d nodes; isolated data: %s\nPress Ctrl+C to stop the lab and its node processes.\n", listener.Addr(), *nodes, *data)
	select {
	case <-ctx.Done():
		return nil
	case err := <-done:
		return err
	}
}
func npm(ctx context.Context, args ...string) error {
	var cmd *exec.Cmd
	if runtime.GOOS == "windows" {
		cmd = exec.CommandContext(ctx, "cmd", append([]string{"/c", "npm.cmd"}, args...)...)
	} else {
		cmd = exec.CommandContext(ctx, "npm", args...)
	}
	cmd.Dir = "dashboard"
	cmd.Stdout = os.Stdout
	cmd.Stderr = os.Stderr
	return cmd.Run()
}
