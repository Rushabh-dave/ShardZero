package tests

import (
	"bufio"
	"bytes"
	"encoding/json"
	"fmt"
	"net"
	"net/http"
	"os"
	"os/exec"
	"strings"
	"testing"
	"time"

	"github.com/Rushabh-dave/shardzero/internal/api"
	"github.com/Rushabh-dave/shardzero/internal/database"
	"github.com/Rushabh-dave/shardzero/internal/statemachine"
)

// Run a real HTTP node in a child process so Kill bypasses graceful shutdown,
// database Close, and all deferred functions.
func TestCrashHelper(t *testing.T) {
	if os.Getenv("SHARDZERO_CRASH_HELPER") != "1" {
		return
	}
	db, err := database.Open(os.Getenv("SHARDZERO_TEST_DATA"))
	if err != nil {
		panic(err)
	}
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		panic(err)
	}
	fmt.Println("http://" + listener.Addr().String())
	if err := http.Serve(listener, api.Handler(db, "crash-test")); err != nil {
		panic(err)
	}
}

func startNode(t *testing.T, dir string) (string, func()) {
	t.Helper()
	cmd := exec.Command(os.Args[0], "-test.run=^TestCrashHelper$")
	cmd.Env = append(os.Environ(), "SHARDZERO_CRASH_HELPER=1", "SHARDZERO_TEST_DATA="+dir)
	stdout, err := cmd.StdoutPipe()
	if err != nil {
		t.Fatal(err)
	}
	cmd.Stderr = os.Stderr
	if err := cmd.Start(); err != nil {
		t.Fatal(err)
	}
	stopped := false
	stop := func() {
		if !stopped {
			stopped = true
			_ = cmd.Process.Kill()
			_ = cmd.Wait()
		}
	}
	t.Cleanup(stop)
	ready := make(chan string, 1)
	go func() {
		scanner := bufio.NewScanner(stdout)
		if scanner.Scan() {
			ready <- scanner.Text()
		} else {
			ready <- ""
		}
	}()
	select {
	case address := <-ready:
		if !strings.HasPrefix(address, "http://") {
			t.Fatalf("child did not start: %q", address)
		}
		return address, stop
	case <-time.After(20 * time.Second):
		t.Fatal("child startup timed out")
		return "", stop
	}
}

func TestAcknowledgedWritesSurviveForcedKill(t *testing.T) {
	dir := t.TempDir()
	url, kill := startNode(t, dir)
	client := &http.Client{Timeout: 10 * time.Second}
	write := func(method, path, body string, expected int) statemachine.Result {
		t.Helper()
		req, err := http.NewRequest(method, url+path, bytes.NewBufferString(body))
		if err != nil {
			t.Fatal(err)
		}
		resp, err := client.Do(req)
		if err != nil {
			t.Fatal(err)
		}
		defer resp.Body.Close()
		var result statemachine.Result
		if err := json.NewDecoder(resp.Body).Decode(&result); err != nil {
			t.Fatal(err)
		}
		if resp.StatusCode != expected {
			t.Fatalf("status=%d result=%+v", resp.StatusCode, result)
		}
		return result
	}
	for i := 0; i < 40; i++ {
		write("PUT", fmt.Sprintf("/v1/kv/key-%d", i), fmt.Sprintf(`{"clientId":"writer","requestId":%d,"value":"saved-%d"}`, i+1, i), 200)
	}
	casBody := `{"clientId":"cas-client","requestId":1,"expected":"saved-0","value":"changed"}`
	want := write("POST", "/v1/kv/key-0/cas", casBody, 200)
	kill()
	url, _ = startNode(t, dir)
	for i := 0; i < 40; i++ {
		result := write("GET", fmt.Sprintf("/v1/kv/key-%d", i), "", 200)
		expected := fmt.Sprintf("saved-%d", i)
		if i == 0 {
			expected = "changed"
		}
		if !result.Found || result.Value != expected {
			t.Fatalf("lost acknowledged key %d: %+v", i, result)
		}
	}
	if got := write("POST", "/v1/kv/key-0/cas", casBody, 200); got != want {
		t.Fatalf("retry after crash: %+v != %+v", got, want)
	}
}
