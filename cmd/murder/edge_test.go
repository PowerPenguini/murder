package main

import (
	"context"
	"crypto/tls"
	"fmt"
	"io"
	"log/slog"
	"net"
	"net/http"
	"net/http/httptest"
	"net/netip"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"syscall"
	"testing"
	"time"

	"github.com/PowerPenguini/murder/internal/api"
	"github.com/PowerPenguini/murder/internal/edge"
	"github.com/PowerPenguini/murder/internal/spec"
	"github.com/PowerPenguini/murder/internal/store"
)

func freePort(t *testing.T) string {
	t.Helper()
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer listener.Close()
	return listener.Addr().String()
}

func readPID(path string) int {
	data, err := os.ReadFile(path)
	if err != nil {
		return 0
	}
	pid, _ := strconv.Atoi(strings.TrimSpace(string(data)))
	return pid
}

// An edge replaced while it serves drops no request: new connections reach
// one of the two processes all along, and a request under way when the
// old one hands over is finished by it.
func TestEdgeUpgradeDropsNothing(t *testing.T) {
	if testing.Short() {
		t.Skip("builds and runs the binary")
	}
	t.Run("plain", func(t *testing.T) { testEdgeUpgrade(t, false) })
	// The new edge loads the certificate before it serves.
	t.Run("tls", func(t *testing.T) { testEdgeUpgrade(t, true) })
}

func testEdgeUpgrade(t *testing.T, secure bool) {
	dir := t.TempDir()
	binary := filepath.Join(dir, "murder")
	if output, err := exec.Command("go", "build", "-o", binary, ".").CombinedOutput(); err != nil {
		t.Fatalf("build: %v\n%s", err, output)
	}
	backend := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/slow" {
			time.Sleep(1500 * time.Millisecond)
		}
		_, _ = io.WriteString(w, "ok")
	}))
	defer backend.Close()
	routes := filepath.Join(dir, "edge-routes.json")
	if err := edge.SaveTable(routes, edge.NewTable(map[string]edge.Route{"acme": {Backend: strings.TrimPrefix(backend.URL, "http://")}})); err != nil {
		t.Fatal(err)
	}
	address, plainAddress := freePort(t), freePort(t)
	pidFile := filepath.Join(dir, "edge.pid")
	configPath := filepath.Join(dir, "agent.yaml")
	// No agent runs: the edge serves the saved routes.
	edgeConfig := fmt.Sprintf(`{http: %q, routes: %s, pidFile: %s, drain: 10s}`, address, routes, pidFile)
	scheme := "http"
	if secure {
		scheme = "https"
		edgeConfig = fmt.Sprintf(`{http: %q, https: %q, tls: {mode: internal, storage: %s}, routes: %s, pidFile: %s, drain: 10s}`, plainAddress, address, filepath.Join(dir, "certs"), routes, pidFile)
	}
	config := fmt.Sprintf(`baseDomain: example.test
socket: %s
firewall: "off"
edge: %s
`, filepath.Join(dir, "agent.sock"), edgeConfig)
	if err := os.WriteFile(configPath, []byte(config), 0o600); err != nil {
		t.Fatal(err)
	}
	first := exec.Command(binary, "edge", "-config", configPath)
	// A file, not a pipe: the new edge inherits it, and Wait would wait for
	// a pipe to close.
	logFile, err := os.Create(filepath.Join(dir, "edge.log"))
	if err != nil {
		t.Fatal(err)
	}
	defer logFile.Close()
	first.Stdout, first.Stderr = logFile, logFile
	output := logOutput(logFile.Name())
	if err := first.Start(); err != nil {
		t.Fatal(err)
	}
	exited := make(chan error, 1)
	go func() { exited <- first.Wait() }()
	defer func() {
		// The new edge is not this test's child: wait for it to drain and
		// go, before the backend closes.
		if pid := readPID(pidFile); pid != 0 && pid != first.Process.Pid {
			_ = syscall.Kill(pid, syscall.SIGTERM)
			for range 300 {
				if syscall.Kill(pid, 0) != nil {
					break
				}
				time.Sleep(50 * time.Millisecond)
			}
		}
		_ = first.Process.Kill()
	}()
	deadline := time.Now().Add(30 * time.Second)
	for readPID(pidFile) != first.Process.Pid {
		if time.Now().After(deadline) {
			t.Fatalf("the edge did not get ready:\n%s", output())
		}
		time.Sleep(20 * time.Millisecond)
	}

	get := func(client *http.Client, path string) error {
		request, _ := http.NewRequest("GET", scheme+"://"+address+path, nil)
		request.Host = "acme.example.test"
		response, err := client.Do(request)
		if err != nil {
			return err
		}
		defer response.Body.Close()
		body, _ := io.ReadAll(response.Body)
		if response.StatusCode != 200 || string(body) != "ok" {
			return fmt.Errorf("%s: %q", response.Status, body)
		}
		return nil
	}
	// New connections every time, and kept-alive ones.
	insecure := &tls.Config{InsecureSkipVerify: true, ServerName: "acme.example.test"}
	fresh := &http.Client{Transport: &http.Transport{DisableKeepAlives: true, TLSClientConfig: insecure}, Timeout: 10 * time.Second}
	kept := &http.Client{Transport: &http.Transport{TLSClientConfig: insecure}, Timeout: 10 * time.Second}
	ctx, stop := context.WithCancel(context.Background())
	var wg sync.WaitGroup
	var served atomic.Int64
	failures := make(chan error, 100)
	for i := range 4 {
		client, kind := fresh, "new connection"
		if i%2 == 1 {
			client, kind = kept, "kept-alive connection"
		}
		wg.Add(1)
		go func() {
			defer wg.Done()
			for ctx.Err() == nil {
				if err := get(client, "/"); err != nil {
					failures <- fmt.Errorf("%s at %s: %w", kind, time.Now().Format("15:04:05.000"), err)
					return
				}
				served.Add(1)
			}
		}()
	}
	slow := make(chan error, 1)
	go func() { slow <- get(fresh, "/slow") }()
	time.Sleep(300 * time.Millisecond)

	t.Logf("SIGHUP at %s", time.Now().Format("15:04:05.000"))
	if err := first.Process.Signal(syscall.SIGHUP); err != nil {
		t.Fatal(err)
	}
	for readPID(pidFile) == first.Process.Pid {
		if time.Now().After(deadline.Add(30 * time.Second)) {
			t.Fatalf("no new edge took over:\n%s", output())
		}
		time.Sleep(20 * time.Millisecond)
	}
	select {
	case <-exited:
	case <-time.After(30 * time.Second):
		t.Fatalf("the old edge did not exit:\n%s", output())
	}
	before := served.Load()
	time.Sleep(500 * time.Millisecond) // served by the new edge alone
	stop()
	wg.Wait()
	close(failures)
	for err := range failures {
		t.Errorf("a request failed during the upgrade: %v", err)
	}
	if err := <-slow; err != nil {
		t.Errorf("the request under way failed: %v", err)
	}
	if served.Load() <= before || before == 0 {
		t.Errorf("served %d before the old edge left, %d in all", before, served.Load())
	}
	if t.Failed() {
		t.Logf("edge output:\n%s", output())
	}
}

func logOutput(path string) func() string {
	return func() string {
		data, _ := os.ReadFile(path)
		return string(data)
	}
}

func TestTheEdgeFollowsTheAgentsRoutes(t *testing.T) {
	dir := t.TempDir()
	database := filepath.Join(dir, "murder.db")
	if err := store.Init(database, "node-1"); err != nil {
		t.Fatal(err)
	}
	st, err := store.Open(database, store.Options{NetworkPool: netip.MustParsePrefix("10.240.0.0/16"), Sealer: func(string, string, spec.Secret) (string, error) { return "x", nil }})
	if err != nil {
		t.Fatal(err)
	}
	defer st.Close()
	app := spec.Tenant{Image: "sha256:" + strings.Repeat("a", 64), HealthPath: "/health", User: "1000:1000"}
	if _, err := st.PutApp(store.App{Name: "web", Spec: app}, "test"); err != nil {
		t.Fatal(err)
	}
	log := slog.New(slog.NewTextHandler(io.Discard, nil))
	agent := httptest.NewServer(api.NewServer(st, nil, nil, nil, log).Handler())
	routes := filepath.Join(dir, "edge-routes.json")
	table := &edge.LiveTable{}
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() {
		defer close(done)
		followRoutes(ctx, api.NewClientURL(agent.URL), table, routes, log)
	}()
	defer func() { cancel(); <-done }()
	waitFor := func(what string, check func() bool) {
		t.Helper()
		deadline := time.Now().Add(10 * time.Second)
		for !check() {
			if time.Now().After(deadline) {
				t.Fatalf("timed out waiting until %s", what)
			}
			time.Sleep(10 * time.Millisecond)
		}
	}
	waitFor("the first routes", func() bool { return table.Load().Version != "" })
	if _, err := st.Create(store.NewTenant{Name: "acme", App: "web"}, "test"); err != nil {
		t.Fatal(err)
	}
	waitFor("acme is routed", func() bool { _, ok := table.Route("acme"); return ok })
	saved, ok, err := edge.LoadTable(routes)
	if err != nil || !ok || saved.Routes["acme"].Backend != "10.240.0.2:8080" {
		t.Fatalf("saved %+v, %v %v", saved, ok, err)
	}
	// The agent goes away: the edge keeps routing.
	agent.Close()
	time.Sleep(200 * time.Millisecond)
	if _, ok := table.Route("acme"); !ok {
		t.Fatal("the edge forgot its routes when the agent went away")
	}
}
