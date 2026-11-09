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
	"github.com/PowerPenguini/murder/internal/apiserver"
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
	// No agent runs: the first edge serves the saved routes once it has
	// held requests for routesHold; the edge that takes over holds none.
	edgeConfig := fmt.Sprintf(`{http: %q, routes: %s, routesHold: 1s, pidFile: %s, drain: 10s}`, address, routes, pidFile)
	scheme := "http"
	if secure {
		scheme = "https"
		edgeConfig = fmt.Sprintf(`{http: %q, https: %q, tls: {mode: internal, storage: %s}, routes: %s, routesHold: 1s, pidFile: %s, drain: 10s}`, plainAddress, address, filepath.Join(dir, "certs"), routes, pidFile)
	}
	config := fmt.Sprintf(`baseDomain: example.test
socket: %s
firewall: "off"
edge: %s
`, filepath.Join(socketDir(t), "agent.sock"), edgeConfig)
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
	agent := httptest.NewServer(apiserver.NewServer(st, nil, nil, nil, log).Handler())
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

func TestSlowStartsAfterIdle(t *testing.T) {
	dir := t.TempDir()
	for value, want := range map[string]bool{"1\n": true, "0\n": false} {
		path := filepath.Join(dir, strings.TrimSpace(value))
		if err := os.WriteFile(path, []byte(value), 0o644); err != nil {
			t.Fatal(err)
		}
		if got := slowStartsAfterIdle(path); got != want {
			t.Errorf("setting %q: slow starts %v, want %v", value, got, want)
		}
	}
	if slowStartsAfterIdle(filepath.Join(dir, "absent")) {
		t.Error("a setting that cannot be read is reported")
	}
}

// The edge's servers give up on a request whose body stopped coming, over
// HTTP/1.1 and HTTP/2; a body that keeps coming, however slowly, and a
// response that streams on after the body are not cut off.
func TestTheEdgeGivesUpOnABodyThatStopped(t *testing.T) {
	defer func(was time.Duration) { uploadIdle = was }(uploadIdle)
	uploadIdle = 200 * time.Millisecond
	read := make(chan error, 10)
	handler := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		n, err := io.Copy(io.Discard, r.Body)
		read <- err
		if err != nil {
			return
		}
		// Then a stream, longer than the body may stop.
		for range 5 {
			fmt.Fprintf(w, "%d\n", n)
			http.NewResponseController(w).Flush()
			time.Sleep(100 * time.Millisecond)
		}
	})
	for _, http2 := range []bool{false, true} {
		server := httptest.NewUnstartedServer(nil)
		server.Config = publicServer(handler)
		server.EnableHTTP2 = http2
		server.StartTLS()
		client := server.Client()
		post := func(body io.Reader) (string, error) {
			response, err := client.Post(server.URL, "text/plain", body)
			if err != nil {
				return "", err
			}
			defer response.Body.Close()
			got, err := io.ReadAll(response.Body)
			return string(got), err
		}
		// One byte, then nothing.
		stalled, stall := io.Pipe()
		go func() { _, _ = stall.Write([]byte("x")) }()
		go func() { _, _ = post(stalled) }()
		select {
		case err := <-read:
			if err == nil {
				t.Errorf("HTTP/2 %v: a body that stopped was read whole", http2)
			}
		case <-time.After(5 * time.Second):
			t.Errorf("HTTP/2 %v: a body that stopped is still waited for", http2)
		}
		stall.Close()
		// Ten bytes over a second, then a stream of half a second.
		slow, write := io.Pipe()
		go func() {
			for range 10 {
				time.Sleep(100 * time.Millisecond)
				_, _ = write.Write([]byte("x"))
			}
			write.Close()
		}()
		if got, err := post(slow); err != nil || got != strings.Repeat("10\n", 5) {
			t.Errorf("HTTP/2 %v: a slow body: %q, %v", http2, got, err)
		}
		if err := <-read; err != nil {
			t.Errorf("HTTP/2 %v: a slow body: %v", http2, err)
		}
		server.Close()
	}
}

// An edge started with saved routes asks the agent for its own at once,
// not for a change from them: requests wait for them. The same routes
// confirm the saved ones.
func TestTheEdgeConfirmsItsSavedRoutesAtOnce(t *testing.T) {
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
	log := slog.New(slog.NewTextHandler(io.Discard, nil))
	server := apiserver.NewServer(st, nil, nil, nil, log)
	agent := httptest.NewServer(server.Handler())
	// The long poll under way ends with the agent.
	defer agent.Close()
	defer server.Stop()
	table := &edge.LiveTable{}
	table.Restore(edge.NewTable(nil)) // what the agent has too
	_, confirmed, unconfirmed := table.Unconfirmed()
	if !unconfirmed {
		t.Fatal("the saved routes count as confirmed")
	}
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() {
		defer close(done)
		followRoutes(ctx, api.NewClientURL(agent.URL), table, filepath.Join(dir, "edge-routes.json"), log)
	}()
	defer func() { cancel(); <-done }()
	select {
	case <-confirmed:
	case <-time.After(5 * time.Second):
		t.Fatal("the agent did not confirm the saved routes at once")
	}
}

// Restarted under socket activation, the edge refuses no connection: the
// socket systemd holds (here, the test) takes them while no edge runs, and
// the next edge serves those waiting. That edge still hands over on SIGHUP.
func TestEdgeRestartUnderSocketActivationRefusesNothing(t *testing.T) {
	if testing.Short() {
		t.Skip("builds and runs the binary")
	}
	dir := t.TempDir()
	binary := filepath.Join(dir, "murder")
	if output, err := exec.Command("go", "build", "-o", binary, ".").CombinedOutput(); err != nil {
		t.Fatalf("build: %v\n%s", err, output)
	}
	backend := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { _, _ = io.WriteString(w, "ok") }))
	defer backend.Close()
	routes := filepath.Join(dir, "edge-routes.json")
	if err := edge.SaveTable(routes, edge.NewTable(map[string]edge.Route{"acme": {Backend: strings.TrimPrefix(backend.URL, "http://")}})); err != nil {
		t.Fatal(err)
	}
	// What murder-edge.socket does: hold the port across the edges.
	held, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer held.Close()
	socket, err := held.(*net.TCPListener).File()
	if err != nil {
		t.Fatal(err)
	}
	defer socket.Close()
	address := held.Addr().String()
	pidFile := filepath.Join(dir, "edge.pid")
	configPath := filepath.Join(dir, "agent.yaml")
	config := fmt.Sprintf("baseDomain: example.test\nsocket: %s\nfirewall: \"off\"\nedge: {http: %q, routes: %s, routesHold: 0s, pidFile: %s, drain: 5s}\n",
		filepath.Join(socketDir(t), "agent.sock"), address, routes, pidFile)
	if err := os.WriteFile(configPath, []byte(config), 0o600); err != nil {
		t.Fatal(err)
	}
	logFile, err := os.Create(filepath.Join(dir, "edge.log"))
	if err != nil {
		t.Fatal(err)
	}
	defer logFile.Close()
	output := logOutput(logFile.Name())
	// systemd sets LISTEN_PID to the pid it starts: the shell's, which
	// exec keeps.
	start := func() *exec.Cmd {
		edge := exec.Command("/bin/sh", "-c", `LISTEN_PID=$$ exec "$0" "$@"`, binary, "edge", "-config", configPath)
		edge.Env = append(os.Environ(), "LISTEN_FDS=1", "LISTEN_FDNAMES=edge")
		edge.ExtraFiles = []*os.File{socket}
		edge.Stdout, edge.Stderr = logFile, logFile
		if err := edge.Start(); err != nil {
			t.Fatal(err)
		}
		deadline := time.Now().Add(30 * time.Second)
		for readPID(pidFile) != edge.Process.Pid {
			if time.Now().After(deadline) {
				t.Fatalf("the edge did not get ready:\n%s", output())
			}
			time.Sleep(20 * time.Millisecond)
		}
		return edge
	}
	stopEdge := func(pid int) {
		_ = syscall.Kill(pid, syscall.SIGTERM)
		for range 300 {
			if syscall.Kill(pid, 0) != nil {
				return
			}
			time.Sleep(50 * time.Millisecond)
		}
		t.Fatalf("edge %d did not stop:\n%s", pid, output())
	}

	first := start()
	firstDone := make(chan struct{})
	go func() { _ = first.Wait(); close(firstDone) }()
	ctx, stop := context.WithCancel(context.Background())
	var wg sync.WaitGroup
	var served atomic.Int64
	failures := make(chan error, 100)
	client := &http.Client{Transport: &http.Transport{DisableKeepAlives: true}, Timeout: 20 * time.Second}
	for range 4 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for ctx.Err() == nil {
				request, _ := http.NewRequest("GET", "http://"+address+"/", nil)
				request.Host = "acme.example.test"
				response, err := client.Do(request)
				if err != nil {
					failures <- fmt.Errorf("at %s: %w", time.Now().Format("15:04:05.000"), err)
					return
				}
				body, _ := io.ReadAll(response.Body)
				response.Body.Close()
				if response.StatusCode != 200 || string(body) != "ok" {
					failures <- fmt.Errorf("%s: %q", response.Status, body)
					return
				}
				served.Add(1)
				time.Sleep(5 * time.Millisecond)
			}
		}()
	}
	time.Sleep(300 * time.Millisecond)
	// A restart: the edge stops, and only then does the next one start.
	stopEdge(first.Process.Pid)
	<-firstDone
	before := served.Load()
	second := start()
	secondDone := make(chan struct{})
	go func() { _ = second.Wait(); close(secondDone) }()
	time.Sleep(300 * time.Millisecond)
	afterRestart := served.Load()
	// A reload hands over to a child, which gets the socket from the edge
	// (the variables naming it are gone): it binds nothing anew, which
	// would fail beside the held socket.
	if err := second.Process.Signal(syscall.SIGHUP); err != nil {
		t.Fatal(err)
	}
	select {
	case <-secondDone:
	case <-time.After(30 * time.Second):
		t.Fatalf("the edge did not hand over:\n%s", output())
	}
	child := readPID(pidFile)
	time.Sleep(300 * time.Millisecond)
	stop()
	wg.Wait()
	close(failures)
	for err := range failures {
		t.Errorf("a request failed: %v", err)
	}
	if before == 0 || afterRestart <= before || served.Load() <= afterRestart {
		t.Errorf("served %d before the restart, %d after it, %d after the handover", before, afterRestart, served.Load())
	}
	if child != 0 && child != second.Process.Pid {
		stopEdge(child)
	}
	if !strings.Contains(output(), `"socketActivated":true`) {
		t.Errorf("the edge did not serve the socket it was passed:\n%s", output())
	}
	if t.Failed() {
		t.Logf("edge output:\n%s", output())
	}
}
