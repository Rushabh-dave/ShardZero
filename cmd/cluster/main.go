// Command cluster starts separate local node processes using configs/local.json.
package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"net/url"
	"os"
	"os/exec"
	"os/signal"
	"path/filepath"
	"runtime"
	"strings"
	"syscall"
	"time"

	"github.com/Rushabh-dave/shardzero/internal/raft"
)

func main() {
	if err := run(); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}
func run() error {
	config := flag.String("config", "configs/local.json", "local cluster membership JSON")
	data := flag.String("data", "data/raft", "parent of per-node data directories")
	fixed := flag.String("fixed-leader", "", "Phase 3 teaching mode (use a separate --data directory)")
	snapshotEvery := flag.Uint64("snapshot-every", 128, "compact fully replicated entries every N commits (0 disables)")
	flag.Parse()
	m, err := raft.LoadMembership(*config)
	if err != nil {
		return err
	}
	if len(m.Peers) == 0 {
		return errors.New("empty cluster")
	}
	for _, p := range m.Peers {
		if p.ID == "" || p.ID == "." || p.ID == ".." || strings.ContainsAny(p.ID, "/\\:") {
			return errors.New("node IDs must be simple directory names")
		}
		for _, address := range []string{p.PeerURL, p.ClientURL} {
			u, err := url.Parse(address)
			if err != nil || u.Scheme != "http" || u.Port() == "" || (u.Hostname() != "127.0.0.1" && u.Hostname() != "localhost") {
				return errors.New("local launcher requires HTTP loopback addresses with explicit ports")
			}
		}
	}
	if err := os.MkdirAll("bin", 0700); err != nil {
		return err
	}
	name := "shardzero-node"
	if runtime.GOOS == "windows" {
		name += ".exe"
	}
	binary, err := filepath.Abs(filepath.Join("bin", name))
	if err != nil {
		return err
	}
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	build := exec.CommandContext(ctx, "go", "build", "-o", binary, "./cmd/node")
	build.Stdout = os.Stdout
	build.Stderr = os.Stderr
	if err := build.Run(); err != nil {
		return fmt.Errorf("build node: %w", err)
	}
	type child struct {
		cmd  *exec.Cmd
		done chan error
	}
	var children []child
	defer func() {
		for _, c := range children {
			if runtime.GOOS != "windows" {
				_ = c.cmd.Process.Signal(syscall.SIGTERM)
			} else {
				_ = c.cmd.Process.Kill()
			}
		}
		for _, c := range children {
			select {
			case <-c.done:
			case <-time.After(5 * time.Second):
				_ = c.cmd.Process.Kill()
				<-c.done
			}
		}
	}()
	failures := make(chan error, len(m.Peers))
	for _, p := range m.Peers {
		clientURL, _ := url.Parse(p.ClientURL)
		peerURL, _ := url.Parse(p.PeerURL)
		args := []string{"--id=" + p.ID, "--cluster=" + *config, "--listen=" + clientURL.Host, "--peer-listen=" + peerURL.Host, "--data=" + filepath.Join(*data, p.ID)}
		if *fixed != "" {
			args = append(args, "--fixed-leader="+*fixed)
		}
		args = append(args, fmt.Sprintf("--snapshot-every=%d", *snapshotEvery))
		cmd := exec.Command(binary, args...)
		cmd.Stdout = os.Stdout
		cmd.Stderr = os.Stderr
		if err := cmd.Start(); err != nil {
			return err
		}
		done := make(chan error, 1)
		children = append(children, child{cmd, done})
		go func(id string) { err := cmd.Wait(); done <- err; failures <- fmt.Errorf("%s exited: %v", id, err) }(p.ID)
		fmt.Printf("%s PID=%d client=%s peer=%s\n", p.ID, cmd.Process.Pid, p.ClientURL, p.PeerURL)
	}
	fmt.Println("Cluster processes started. Wait for an election; press Ctrl+C to stop them.")
	remaining := len(children)
	for {
		select {
		case <-ctx.Done():
			return nil
		case err := <-failures:
			remaining--
			fmt.Fprintln(os.Stderr, err)
			if remaining == 0 {
				return errors.New("all cluster processes exited")
			}
			fmt.Fprintf(os.Stderr, "%d node process(es) still running; the majority can continue.\n", remaining)
		}
	}
}
