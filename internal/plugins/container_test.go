package plugins

import (
	"fmt"
	"io"
	"log/slog"
	"net"
	"net/http"
	"net/http/httptest"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"strings"
	"sync"
	"testing"
	"time"
)

var (
	buildOnce sync.Once
	built     map[string]string
	buildErr  error
)

// testPlugin builds a plugin of this repository's for the tests, as a
// program: the test plugin, or the Anthropic example.
func testPlugin(t *testing.T, name string) string {
	t.Helper()
	buildOnce.Do(func() {
		dir, err := os.MkdirTemp("", "plugins-")
		if err != nil {
			buildErr = err
			return
		}
		built = map[string]string{}
		for name, pkg := range map[string]string{"test": "./testdata/plugin", "anthropic": "../../examples/plugins/anthropic"} {
			out := filepath.Join(dir, name)
			command := exec.Command(filepath.Join(runtime.GOROOT(), "bin", "go"), "build", "-o", out, pkg)
			command.Env = append(os.Environ(), "CGO_ENABLED=0", "GOFLAGS=")
			if output, err := command.CombinedOutput(); err != nil {
				buildErr = fmt.Errorf("build %s: %v\n%s", pkg, err, output)
				return
			}
			built[name] = out
		}
	})
	if buildErr != nil {
		t.Fatal(buildErr)
	}
	return built[name]
}

// manifestOf reads a plugin's manifest as its image's label would hold
// it: as the program prints it.
func manifestOf(t *testing.T, program string) Manifest {
	t.Helper()
	printed, err := exec.Command(program, "-manifest").Output()
	if err != nil {
		t.Fatal(err)
	}
	manifest, err := ParseManifest(string(printed))
	if err != nil {
		t.Fatal(err)
	}
	return manifest
}

// socketRoot is a directory for plugins' sockets, short enough for a unix
// socket's address.
func socketRoot(t *testing.T) string {
	t.Helper()
	root, err := os.MkdirTemp("/tmp", "mp")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { os.RemoveAll(root) })
	return root
}

// startPlugin runs a plugin as its container would, on the socket of the
// container of plugin p's image below root, and waits for it to listen.
func startPlugin(t *testing.T, program, root, image string) *exec.Cmd {
	t.Helper()
	dir := filepath.Join(root, ContainerID("p", image))
	if err := os.MkdirAll(dir, 0o700); err != nil {
		t.Fatal(err)
	}
	socket := filepath.Join(dir, SocketName)
	command := exec.Command(program)
	command.Env = append(os.Environ(), "MURDER_PLUGIN_SOCKET="+socket)
	command.Stderr = os.Stderr
	if err := command.Start(); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { command.Process.Kill(); command.Wait() })
	for deadline := time.Now().Add(10 * time.Second); time.Now().Before(deadline); time.Sleep(10 * time.Millisecond) {
		if conn, err := net.Dial("unix", socket); err == nil {
			conn.Close()
			return command
		}
	}
	t.Fatal("the plugin does not listen on its socket")
	return nil
}

// testImage is the image the tests' plugins run as, by its digest.
var testImage = "example.com/test@sha256:" + strings.Repeat("a", 64)

func TestAPluginsManifestIsReadFromWhatItPrints(t *testing.T) {
	manifest := manifestOf(t, testPlugin(t, "test"))
	if manifest.ABI != ABI || manifest.Name != "test" || manifest.Key.Header != "X-Key" || len(manifest.Metrics) != 2 || manifest.RequestBody != 1<<20 {
		t.Fatalf("manifest %+v", manifest)
	}
	if metrics := MetricsOf(Spec{Kind: KindContainer}, &manifest); strings.Join(metrics, " ") != "requests widgets bytes" {
		t.Fatalf("metrics %v", metrics)
	}
	if anthropic := manifestOf(t, testPlugin(t, "anthropic")); anthropic.Name != "anthropic" || anthropic.Key.Header != "x-api-key" {
		t.Fatalf("anthropic %+v", anthropic)
	}
	// A label written by hand, with empty lists, says the same as the
	// container that leaves them out.
	byHand, err := ParseManifest(`{"abi":2,"name":"x","hosts":["a.example.com"],"key":{"header":"X-Key"},"metrics":[],"ui":{"metrics":[],"config":[]}}`)
	if err != nil {
		t.Fatal(err)
	}
	if said := (Manifest{ABI: ABI, Name: "x", Hosts: []string{"a.example.com"}, Key: KeyPlace{Header: "X-Key"}, UI: &UI{}}); !SameManifest(byHand, said) || SameManifest(byHand, Manifest{ABI: ABI, Name: "y"}) {
		t.Fatalf("%+v is not the same as what its container says", byHand)
	}
	for name, label := range map[string]string{
		"not JSON":         "#!/bin/sh",
		"an unknown field": `{"abi":2,"name":"x","hosts":["a.example.com"],"key":{"header":"X-Key"},"run":"rm -rf /"}`,
		"two values":       `{"abi":2,"name":"x","hosts":["a.example.com"],"key":{"header":"X-Key"}} {}`,
		"another ABI":      `{"abi":1,"name":"x","hosts":["a.example.com"],"key":{"header":"X-Key"}}`,
	} {
		if _, err := ParseManifest(label); err == nil {
			t.Errorf("%s was taken", name)
		}
	}
}

func TestAManifestIsHeldToWhatAPluginMay(t *testing.T) {
	good := Manifest{ABI: ABI, Name: "x", Hosts: []string{"api.example.com"}, Key: KeyPlace{Header: "Authorization", Prefix: "Bearer "}, Metrics: []string{"tokens.input"}}
	if err := good.Validate(); err != nil {
		t.Fatal(err)
	}
	for name, change := range map[string]func(*Manifest){
		"another ABI":       func(m *Manifest) { m.ABI = 1 },
		"no hosts":          func(m *Manifest) { m.Hosts = nil },
		"a bad host":        func(m *Manifest) { m.Hosts = []string{"*."} },
		"no key":            func(m *Manifest) { m.Key = KeyPlace{} },
		"two keys":          func(m *Manifest) { m.Key.Query = "key" },
		"the key in Host":   func(m *Manifest) { m.Key = KeyPlace{Header: "Host"} },
		"the key in Cookie": func(m *Manifest) { m.Key = KeyPlace{Header: "cookie"} },
		"a prefix in query": func(m *Manifest) { m.Key = KeyPlace{Query: "key", Prefix: "x"} },
		"a bad metric":      func(m *Manifest) { m.Metrics = []string{"Tokens!"} },
		"a body too large":  func(m *Manifest) { m.RequestBody = MaxRequestBody + 1 },
		"a bad name":        func(m *Manifest) { m.Name = "My Plugin" },
	} {
		manifest := good
		manifest.Hosts = append([]string(nil), good.Hosts...)
		change(&manifest)
		if manifest.Validate() == nil {
			t.Errorf("%s was taken", name)
		}
	}
}

func TestASpecOfKindContainerNamesItsImagePinned(t *testing.T) {
	spec, err := Parse([]byte("kind: container\nimage: " + testImage + "\nconfig: {allow: [a]}\nlimits:\n  - {metric: widgets, per: day, amount: 3}\n"))
	if err != nil {
		t.Fatal(err)
	}
	manifest := Manifest{Metrics: []string{"widgets"}}
	if err := spec.ValidateWith(&manifest); err != nil {
		t.Fatal(err)
	}
	if err := spec.ValidateWith(&Manifest{Metrics: []string{"bytes"}}); err == nil {
		t.Error("a limit of a metric the plugin does not count was taken")
	}
	// A file may name a tag, which the agent pins.
	tagged, err := Decode([]byte("kind: container\nimage: ghcr.io/acme/plugin:1.0\n"))
	if err != nil || tagged.ValidateUnpinned() != nil || tagged.Validate() == nil {
		t.Fatalf("a tag: %v, unpinned %v, pinned %v", err, tagged.ValidateUnpinned(), tagged.Validate())
	}
	for _, bad := range []string{
		"kind: container\n",
		"kind: container\nimage: ghcr.io/acme/plugin:1.0\n",
		"kind: container\nimage: " + testImage + "\nupstream: https://x.example\n",
		"kind: openai\nimage: " + testImage + "\n",
		"kind: here\nconfig: {a: 1}\n",
		"kind: wasm\nmodule: plugin.wasm\n",
	} {
		if _, err := Parse([]byte(bad)); err == nil {
			t.Errorf("%q was taken", bad)
		}
	}
}

// containerFixture serves a plugin p of the test plugin, run on its
// socket, to tenants acme, at 127.0.0.1, and beta, at 127.0.0.2.
type containerFixture struct {
	*fixture
	containers *Containers
	root       string
	plugin     *exec.Cmd
	seen       chan *http.Request
}

func newContainerFixture(t *testing.T, upstream http.HandlerFunc, change func(*Spec)) *containerFixture {
	t.Helper()
	program := testPlugin(t, "test")
	manifest := manifestOf(t, program)
	w := &containerFixture{seen: make(chan *http.Request, 64), root: socketRoot(t)}
	server := httptest.NewServer(http.HandlerFunc(func(rw http.ResponseWriter, r *http.Request) {
		body, _ := io.ReadAll(r.Body)
		r.Body = io.NopCloser(strings.NewReader(string(body)))
		w.seen <- r
		upstream(rw, r)
	}))
	t.Cleanup(server.Close)
	spec := Spec{Kind: KindContainer, Image: testImage, Config: map[string]any{"allow": []any{"a"}}}
	if change != nil {
		change(&spec)
	}
	w.fixture = newFixture(t, spec, Grant{}, server)
	plugin := w.config.Plugins["p"]
	plugin.Manifest = &manifest
	w.config.Plugins["p"] = plugin
	w.config.Tenants["beta"] = Tenant{Address: "127.0.0.2", Grants: map[string]Grant{"p": {}}}
	w.plugin = startPlugin(t, program, w.root, testImage)
	w.containers = NewContainers(w.root, slog.New(slog.DiscardHandler))
	w.proxy.WithContainers(w.containers)
	w.containers.Prepare(w.config)
	return w
}

// as sends a request as the tenant at address.
func (w *containerFixture) as(t *testing.T, address string, request *http.Request) *http.Response {
	t.Helper()
	client := &http.Client{Transport: &http.Transport{DialContext: (&net.Dialer{LocalAddr: &net.TCPAddr{IP: net.ParseIP(address)}}).DialContext}}
	response, err := client.Do(request)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { response.Body.Close() })
	return response
}

func (w *containerFixture) get(t *testing.T, address, path string) (*http.Response, string) {
	t.Helper()
	request, _ := http.NewRequest(http.MethodGet, w.server.URL+"/p"+path, nil)
	response := w.as(t, address, request)
	body, _ := io.ReadAll(response.Body)
	return response, string(body)
}

func (w *containerFixture) upstream(t *testing.T) *http.Request {
	t.Helper()
	select {
	case r := <-w.seen:
		return r
	case <-time.After(5 * time.Second):
		t.Fatal("the upstream got no request")
		return nil
	}
}

func ok(rw http.ResponseWriter, _ *http.Request) { rw.Write([]byte("fine")) }

func TestAContainerPassesTheRequestOnWithTheKeyWhereItSays(t *testing.T) {
	w := newContainerFixture(t, ok, nil)
	request, _ := http.NewRequest(http.MethodGet, w.server.URL+"/p/v1/things?q=1&x-key=mine", nil)
	request.Header.Set("Authorization", "Bearer tenant-key")
	request.Header.Set("X-Key", "tenant-key")
	response := w.as(t, "127.0.0.1", request)
	if response.StatusCode != http.StatusOK {
		body, _ := io.ReadAll(response.Body)
		t.Fatalf("status %d: %s", response.StatusCode, body)
	}
	seen := w.upstream(t)
	if seen.Host != "api.example.com" || seen.URL.Path != "/v1/things" || seen.URL.RawQuery != "q=1&x-key=mine" {
		t.Fatalf("went to %s%s?%s", seen.Host, seen.URL.Path, seen.URL.RawQuery)
	}
	if seen.Header.Get("X-Key") != "Token operator-key" || seen.Header.Get("Authorization") != "" {
		t.Fatalf("key %q, authorization %q", seen.Header.Get("X-Key"), seen.Header.Get("Authorization"))
	}
	// The container saw no credentials of the tenant's.
	if seen.Header.Get("X-Saw-Key") != "" || seen.Header.Get("X-Tenant") != "acme" {
		t.Fatalf("the container saw %q, as tenant %q", seen.Header.Get("X-Saw-Key"), seen.Header.Get("X-Tenant"))
	}
	if got := w.counter.Used("acme", "p", MetricRequests, PerDay, time.Now()); got != 1 {
		t.Fatalf("requests %d", got)
	}
	if report, _ := w.counter.Next(); len(report.Counts) != 1 || report.Counts[0].Detail != "svc" {
		t.Fatalf("report %+v", report)
	}
	// Its config came with the request, as JSON.
	if _, body := w.get(t, "127.0.0.1", "/config"); body != "fine" {
		t.Fatal(body)
	}
	if config := w.upstream(t).Header.Get("X-Config"); config != `{"allow":["a"]}` {
		t.Fatalf("config %q", config)
	}
	// A wildcard host of its manifest is reached.
	if response, body := w.get(t, "127.0.0.1", "/tile"); response.StatusCode != http.StatusOK {
		t.Fatalf("tile: %d %s", response.StatusCode, body)
	}
	if seen := w.upstream(t); seen.Host != "a.tiles.example.com" {
		t.Fatalf("tile went to %s", seen.Host)
	}
}

func TestAContainerCountsWhatItReadsInTheAnswer(t *testing.T) {
	w := newContainerFixture(t, func(rw http.ResponseWriter, r *http.Request) {
		rw.Header().Set("Content-Type", "application/json")
		rw.Write([]byte(`{"widgets": 7}`))
	}, nil)
	response, body := w.get(t, "127.0.0.1", "/count")
	if response.StatusCode != http.StatusOK || body != `{"widgets": 7}` {
		t.Fatalf("%d %s", response.StatusCode, body)
	}
	w.upstream(t)
	if got := w.used("widgets"); got != 7 {
		t.Fatalf("widgets %d", got)
	}
	if got := w.used("bytes"); got != int64(len(body)) {
		t.Fatalf("bytes %d", got)
	}
	report, _ := w.counter.Next()
	details := map[string]string{}
	for _, count := range report.Counts {
		details[count.Metric] = count.Detail
	}
	if details["widgets"] != "blue" || details["bytes"] != "whole" {
		t.Fatalf("details %v", details)
	}
}

func TestAContainerRewritesTheBodyItRead(t *testing.T) {
	w := newContainerFixture(t, ok, nil)
	request, _ := http.NewRequest(http.MethodPost, w.server.URL+"/p/rewrite", strings.NewReader("hello"))
	if response := w.as(t, "127.0.0.1", request); response.StatusCode != http.StatusOK {
		t.Fatalf("status %d", response.StatusCode)
	}
	seen := w.upstream(t)
	if body, _ := io.ReadAll(seen.Body); string(body) != "HELLO" || seen.ContentLength != 5 {
		t.Fatalf("body %q, %d", body, seen.ContentLength)
	}
	// A body past what the manifest reads is refused.
	request, _ = http.NewRequest(http.MethodPost, w.server.URL+"/p/rewrite", strings.NewReader(strings.Repeat("x", 2<<20)))
	if response := w.as(t, "127.0.0.1", request); response.StatusCode != http.StatusRequestEntityTooLarge {
		t.Fatalf("a large body: %d", response.StatusCode)
	}
}

func TestWhatAContainerMayNotDoFailsTheRequest(t *testing.T) {
	previous := callRequest
	callRequest = 500 * time.Millisecond
	t.Cleanup(func() { callRequest = previous })
	w := newContainerFixture(t, ok, nil)
	for path, want := range map[string]int{
		"/sleep":     http.StatusBadGateway,
		"/panic":     http.StatusBadGateway,
		"/elsewhere": http.StatusBadGateway,
		"/plain":     http.StatusBadGateway,
		"/header":    http.StatusBadGateway,
		"/refuse":    http.StatusTeapot,
	} {
		response, body := w.get(t, "127.0.0.1", path)
		if response.StatusCode != want {
			t.Errorf("%s: %d %s", path, response.StatusCode, body)
		}
		if path == "/refuse" && !strings.Contains(body, "no tea for acme") {
			t.Errorf("refusal %s", body)
		}
	}
	select {
	case r := <-w.seen:
		t.Fatalf("the upstream got %s", r.URL)
	default:
	}
	// Each failure was counted, and the container serves on.
	if failures := w.proxy.failures["p"]; failures != 5 {
		t.Fatalf("failures %d", failures)
	}
	if response, _ := w.get(t, "127.0.0.1", "/fine"); response.StatusCode != http.StatusOK {
		t.Fatalf("after the failures: %d", response.StatusCode)
	}
}

func TestACountAContainerMayNotMakeIsNotCountedButTheAnswerPasses(t *testing.T) {
	w := newContainerFixture(t, ok, nil)
	response, body := w.get(t, "127.0.0.1", "/badcount")
	if response.StatusCode != http.StatusOK || body != "fine" {
		t.Fatalf("%d %s", response.StatusCode, body)
	}
	if got := w.used(MetricRequests); got != 1 {
		t.Fatalf("requests %d: the container counted some", got)
	}
	if w.proxy.failures["p"] != 1 {
		t.Fatalf("failures %d", w.proxy.failures["p"])
	}
}

// One container serves every tenant, many requests at once, each
// counted for its own tenant.
func TestAContainerServesManyRequestsAtOnceEachCountedForItsTenant(t *testing.T) {
	w := newContainerFixture(t, func(rw http.ResponseWriter, r *http.Request) {
		rw.Header().Set("Content-Type", "application/json")
		rw.Write([]byte(`{"widgets": 7}`))
	}, nil)
	var group sync.WaitGroup
	for i := range 40 {
		address := "127.0.0.1"
		if i%2 == 1 {
			address = "127.0.0.2"
		}
		group.Go(func() {
			request, _ := http.NewRequest(http.MethodGet, w.server.URL+"/p/count", nil)
			client := &http.Client{Transport: &http.Transport{DialContext: (&net.Dialer{LocalAddr: &net.TCPAddr{IP: net.ParseIP(address)}}).DialContext}}
			response, err := client.Do(request)
			if err != nil {
				t.Error(err)
				return
			}
			io.Copy(io.Discard, response.Body)
			response.Body.Close()
		})
	}
	group.Wait()
	for _, tenant := range []string{"acme", "beta"} {
		if got := w.counter.Used(tenant, "p", "widgets", PerDay, time.Now()); got != 20*7 {
			t.Errorf("%s's widgets %d, not %d", tenant, got, 20*7)
		}
	}
	if w.proxy.failures["p"] != 0 {
		t.Fatalf("failures %d", w.proxy.failures["p"])
	}
}

// A plugin whose container is not there yet answers 503, and is served
// once it is; one whose container says another manifest than the plugin
// was installed with is not served at all.
func TestAContainerIsUsedOnlyOnceItSaysItsPluginsManifest(t *testing.T) {
	previous := recheck
	recheck = 0
	t.Cleanup(func() { recheck = previous })
	w := newContainerFixture(t, ok, nil)
	w.plugin.Process.Kill()
	w.plugin.Wait()
	w.containers = NewContainers(w.root, slog.New(slog.DiscardHandler))
	w.proxy.WithContainers(w.containers)
	response, body := w.get(t, "127.0.0.1", "/x")
	if response.StatusCode != http.StatusServiceUnavailable || !strings.Contains(body, "murder_plugin_unavailable") || !strings.Contains(body, "not running") {
		t.Fatalf("no container: %d %s", response.StatusCode, body)
	}
	startPlugin(t, testPlugin(t, "test"), w.root, testImage)
	if response, body := w.get(t, "127.0.0.1", "/x"); response.StatusCode != http.StatusOK {
		t.Fatalf("once it runs: %d %s", response.StatusCode, body)
	}
	w.upstream(t)
	// The plugin installed with another manifest: the container is not
	// used, whatever it would do.
	w.mu.Lock()
	plugin := w.config.Plugins["p"]
	other := *plugin.Manifest
	other.Hosts = []string{"evil.example.org"}
	plugin.Manifest = &other
	w.config.Plugins["p"] = plugin
	w.mu.Unlock()
	w.containers = NewContainers(w.root, slog.New(slog.DiscardHandler))
	w.proxy.WithContainers(w.containers)
	if response, body := w.get(t, "127.0.0.1", "/x"); response.StatusCode != http.StatusServiceUnavailable || !strings.Contains(body, "another manifest") {
		t.Fatalf("another manifest: %d %s", response.StatusCode, body)
	}
}

func TestTheAnthropicExampleCountsAStreamsUsage(t *testing.T) {
	program := testPlugin(t, "anthropic")
	manifest := manifestOf(t, program)
	stream := "event: message_start\n" +
		`data: {"type":"message_start","message":{"model":"claude-opus-5-5","usage":{"input_tokens":10,"cache_creation_input_tokens":3,"cache_read_input_tokens":20,"output_tokens":1}}}` + "\n\n" +
		"event: content_block_delta\n" + `data: {"type":"content_block_delta","index":0,"delta":{"type":"text_delta","text":"Hi"}}` + "\n\n" +
		"event: message_delta\n" + `data: {"type":"message_delta","delta":{"stop_reason":"end_turn"},"usage":{"output_tokens":12}}` + "\n\n" +
		"event: message_stop\n" + `data: {"type":"message_stop"}` + "\n\n"
	var seen http.Header
	server := httptest.NewServer(http.HandlerFunc(func(rw http.ResponseWriter, r *http.Request) {
		seen = r.Header.Clone()
		if r.URL.Path != "/v1/messages" || r.Host != "api.anthropic.com" {
			http.NotFound(rw, r)
			return
		}
		rw.Header().Set("Content-Type", "text/event-stream")
		for _, part := range strings.SplitAfter(stream, "\n") {
			io.WriteString(rw, part)
			rw.(http.Flusher).Flush()
		}
	}))
	defer server.Close()
	f := newFixture(t, Spec{Kind: KindContainer, Image: testImage}, Grant{}, server)
	plugin := f.config.Plugins["p"]
	plugin.Manifest = &manifest
	f.config.Plugins["p"] = plugin
	root := socketRoot(t)
	startPlugin(t, program, root, testImage)
	f.proxy.WithContainers(NewContainers(root, slog.New(slog.DiscardHandler)))
	request, _ := http.NewRequest(http.MethodPost, f.server.URL+"/p/v1/messages", strings.NewReader(`{"model":"claude-opus-5-5","stream":true,"max_tokens":64,"messages":[]}`))
	request.Header.Set("x-api-key", "tenant")
	response, err := http.DefaultClient.Do(request)
	if err != nil {
		t.Fatal(err)
	}
	body, _ := io.ReadAll(response.Body)
	response.Body.Close()
	if response.StatusCode != http.StatusOK || string(body) != stream {
		t.Fatalf("%d %q", response.StatusCode, body)
	}
	if seen.Get("X-Api-Key") != "operator-key" {
		t.Fatalf("key %q", seen.Get("X-Api-Key"))
	}
	for metric, want := range map[string]int64{"tokens.input": 10, "tokens.output": 12, "tokens.cache_write": 3, "tokens.cache_read": 20, "tokens": 45} {
		if got := f.used(metric); got != want {
			t.Errorf("%s %d, not %d", metric, got, want)
		}
	}
	// Batches keep what a tenant sends: not passed on.
	request, _ = http.NewRequest(http.MethodPost, f.server.URL+"/p/v1/messages/batches", strings.NewReader(`{}`))
	if response, err := http.DefaultClient.Do(request); err != nil || response.StatusCode != http.StatusForbidden {
		t.Fatalf("batches: %v %v", response.Status, err)
	}
}

func TestAKeyInTheQueryReplacesTheTenantsAndLeavesNoTrace(t *testing.T) {
	h := &containerHandler{x: &exchange{key: "k&y"}, manifest: Manifest{Key: KeyPlace{Query: "appid"}}}
	out, _ := http.NewRequest(http.MethodGet, "https://api.example.com/v1/x?a=1;b&APPID=mine", nil)
	h.authorize(out)
	if out.URL.RawQuery != "a=1;b&appid=k%26y" {
		t.Fatalf("query %q", out.URL.RawQuery)
	}
	// An error that says the key is answered without it.
	response := &http.Response{StatusCode: 401, Header: http.Header{}, Body: io.NopCloser(strings.NewReader(`{"error":"bad key k%26y in ?appid=k%26y"}`)), ContentLength: -1}
	h.meter = false
	h.response(response)
	if body, _ := io.ReadAll(response.Body); strings.Contains(string(body), "k%26y") {
		t.Fatalf("body %s", body)
	}
}

func TestAPathThatClimbsGetsPastNoContainer(t *testing.T) {
	w := newContainerFixture(t, ok, nil)
	for _, path := range []string{"/v1/models/..%2Fmessages%2Fbatches", "/v1/../batches", "/v1/%2e%2e/batches", "/v1/./x", "/v1%5c..%5cx"} {
		request, _ := http.NewRequest(http.MethodGet, w.server.URL+"/p"+path, nil)
		request.URL.Opaque = "/p" + path // as written, not cleaned
		response := w.as(t, "127.0.0.1", request)
		body, _ := io.ReadAll(response.Body)
		if response.StatusCode != http.StatusBadRequest || !strings.Contains(string(body), "not one to pass on") {
			t.Errorf("%s: %d %s", path, response.StatusCode, body)
		}
	}
	select {
	case r := <-w.seen:
		t.Fatalf("the upstream got %s", r.URL)
	default:
	}
}

func TestAHardLimitOfAContainersMetricRefusesTheNextRequest(t *testing.T) {
	w := newContainerFixture(t, func(rw http.ResponseWriter, r *http.Request) {
		rw.Header().Set("Content-Type", "application/json")
		rw.Write([]byte(`{"widgets": 3}`))
	}, nil)
	w.mu.Lock()
	tenant := w.config.Tenants["acme"]
	tenant.Grants["p"] = Grant{Limits: []Limit{{Metric: "widgets", Per: PerDay, Amount: 3, Mode: ModeHard}}}
	w.mu.Unlock()
	if response, _ := w.get(t, "127.0.0.1", "/count"); response.StatusCode != http.StatusOK {
		t.Fatalf("first: %d", response.StatusCode)
	}
	response, body := w.get(t, "127.0.0.1", "/count")
	if response.StatusCode != http.StatusTooManyRequests || !strings.Contains(body, "widgets") || response.Header.Get("Retry-After") == "" {
		t.Fatalf("past the limit: %d %s", response.StatusCode, body)
	}
}

func TestACompressedErrorCannotCarryAKeyInTheQuery(t *testing.T) {
	h := &containerHandler{x: &exchange{key: "k"}, manifest: Manifest{Key: KeyPlace{Query: "appid"}}}
	out, _ := http.NewRequest(http.MethodGet, "https://api.example.com/x", nil)
	out.Header.Set("Accept-Encoding", "gzip")
	h.authorize(out)
	if out.Header.Get("Accept-Encoding") != "identity" {
		t.Fatal("an error could come compressed, the key in it")
	}
	// One that comes compressed all the same, or too long to read, is not
	// passed on.
	for _, response := range []*http.Response{
		{StatusCode: 400, Header: http.Header{"Content-Encoding": {"gzip"}}, Body: io.NopCloser(strings.NewReader("\x1f\x8bk")), ContentLength: -1},
		{StatusCode: 400, Header: http.Header{}, Body: io.NopCloser(strings.NewReader("k" + strings.Repeat("x", 2<<20))), ContentLength: -1},
		{StatusCode: 400, Header: http.Header{"Content-Encoding": {"identity", "gzip"}}, Body: io.NopCloser(strings.NewReader("k")), ContentLength: -1},
	} {
		scrubErrorBody(response, "k")
		body, _ := io.ReadAll(response.Body)
		if !strings.Contains(string(body), "murder_provider_error") || response.Header.Get("Content-Encoding") != "" || response.ContentLength != int64(len(body)) {
			t.Fatalf("an error not read: %s %v", body, response.Header)
		}
	}
}

// A manifest's UI says how the panel shows its plugin: what it leaves
// out reads as the metrics' names, and what it says of metrics it does
// not count is refused.
func TestAManifestsUISaysHowThePanelShowsIt(t *testing.T) {
	manifest := manifestOf(t, testPlugin(t, "anthropic"))
	ui := UIOf(Spec{Kind: KindContainer}, &manifest)
	if ui.Title != "Anthropic" || ui.Detail != "model" || ui.Metrics[0].Name != "tokens" || ui.Metric("tokens.cache_read").Label != "Read from the cache" || len(ui.Config) != 1 {
		t.Fatalf("ui %+v", ui)
	}
	bare := Manifest{Name: "bare", Metrics: []string{"widgets"}}
	if ui := UIOf(Spec{Kind: KindContainer}, &bare); ui.Title != "bare" || len(ui.Metrics) != 2 || ui.Metrics[0].Name != "requests" || ui.Metrics[1].Label != "widgets" || ui.Detail != "detail" {
		t.Fatalf("a plugin without a UI: %+v", ui)
	}
	if ui := UIOf(Spec{Kind: KindOpenAI}, nil); ui.Title != "OpenAI" || ui.Metrics[0].Name != MetricTokens || len(ui.Metrics) != len(Metrics[KindOpenAI]) {
		t.Fatalf("openai's: %+v", ui)
	}
	good := Manifest{ABI: ABI, Name: "x", Hosts: []string{"api.example.com"}, Key: KeyPlace{Header: "X-Key"}, Metrics: []string{"widgets"}}
	for name, ui := range map[string]UI{
		"a metric it does not count": {Metrics: []MetricUI{{Name: "gadgets"}}},
		"a title too long":           {Title: strings.Repeat("x", 65)},
		"a line break":               {Description: "a\nb"},
		"a config field unnamed":     {Config: []ConfigUI{{Label: "x"}}},
	} {
		manifest := good
		manifest.UI = &ui
		if manifest.Validate() == nil {
			t.Errorf("%s was taken", name)
		}
	}
	good.UI = &UI{Metrics: []MetricUI{{Name: "requests", Label: "Calls"}, {Name: "widgets", Unit: "widgets"}}}
	if err := good.Validate(); err != nil {
		t.Fatal(err)
	}
}
