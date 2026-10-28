package main

import (
	"bytes"
	"encoding/json"
	"flag"
	"io"
	"maps"
	"net"
	"net/http"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/PowerPenguini/corral/internal/api"
	"github.com/PowerPenguini/corral/internal/reconcile"
	"github.com/PowerPenguini/corral/internal/spec"
)

func TestFlagsMayFollowArguments(t *testing.T) {
	flags := flag.NewFlagSet("t", flag.ContinueOnError)
	file := flags.String("file", "", "")
	yes := flags.Bool("yes", false, "")
	positional, err := parseInterspersed(flags, []string{"acme", "-file", "app.yaml", "extra", "-yes"})
	if err != nil {
		t.Fatal(err)
	}
	if !slices.Equal(positional, []string{"acme", "extra"}) || *file != "app.yaml" || !*yes {
		t.Fatalf("positional=%v file=%q yes=%v", positional, *file, *yes)
	}
}

// fakeAgent answers the API calls the CLI makes, over a unix socket.
type fakeAgent struct {
	mu      sync.Mutex
	tenants map[string]api.Tenant
	calls   []string
	bodies  map[string][]byte
}

func (a *fakeAgent) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	a.mu.Lock()
	defer a.mu.Unlock()
	call := r.Method + " " + r.URL.Path
	a.calls = append(a.calls, call)
	body, _ := io.ReadAll(r.Body)
	a.bodies[call] = body
	switch {
	case r.Method == http.MethodGet && r.URL.Path == "/v1/tenants":
		var views []api.Tenant
		for _, name := range slices.Sorted(maps.Keys(a.tenants)) {
			views = append(views, a.tenants[name])
		}
		_ = json.NewEncoder(w).Encode(views)
	case r.Method == http.MethodGet && strings.HasPrefix(r.URL.Path, "/v1/tenants/"):
		// "slow" never becomes healthy; everything else already is.
		name := strings.TrimPrefix(r.URL.Path, "/v1/tenants/")
		tenant, ok := a.tenants[name]
		if !ok {
			w.WriteHeader(http.StatusNotFound)
			_, _ = w.Write([]byte(`{"error":"not found"}`))
			return
		}
		if !tenant.ArchivedAt.IsZero() {
			_ = json.NewEncoder(w).Encode(tenant)
			return
		}
		tenant.Status = &reconcile.Status{Phase: reconcile.PhaseRunning, Healthy: name != "slow", Message: "health check: HTTP 500", CheckedAt: time.Now()}
		_ = json.NewEncoder(w).Encode(tenant)
	case r.Method == http.MethodPut && strings.HasPrefix(r.URL.Path, "/v1/apps/"):
		name := strings.TrimPrefix(r.URL.Path, "/v1/apps/")
		var app spec.Tenant
		_ = json.Unmarshal(body, &app)
		_ = json.NewEncoder(w).Encode(api.AppApplied{App: api.App{Name: name, Spec: app}, Created: true, Changed: true})
	case r.Method == http.MethodPut && strings.HasPrefix(r.URL.Path, "/v1/tenants/"):
		name := strings.TrimPrefix(r.URL.Path, "/v1/tenants/")
		var request api.TenantRequest
		_ = json.Unmarshal(body, &request)
		tenant := a.tenants[name]
		changed := tenant.App != request.App || !tenant.Overrides.Equal(request.Overrides)
		tenant.App, tenant.Overrides = request.App, request.Overrides
		a.tenants[name] = tenant
		_ = json.NewEncoder(w).Encode(api.Applied{Tenant: tenant, Changed: changed})
	case r.Method == http.MethodPost && r.URL.Path == "/v1/tenants":
		var request api.TenantRequest
		_ = json.Unmarshal(body, &request)
		tenant := api.Tenant{Spec: spec.Tenant{Name: request.Name}, App: request.App, Overrides: request.Overrides}
		a.tenants[request.Name] = tenant
		w.WriteHeader(http.StatusCreated)
		_ = json.NewEncoder(w).Encode(api.Applied{Tenant: tenant, Changed: true})
	default:
		w.WriteHeader(http.StatusNotFound)
		_, _ = w.Write([]byte(`{"error":"unexpected call"}`))
	}
}

func startAgent(t *testing.T, tenants ...api.Tenant) (*fakeAgent, string) {
	t.Helper()
	socket := filepath.Join(t.TempDir(), "agent.sock")
	listener, err := net.Listen("unix", socket)
	if err != nil {
		t.Fatal(err)
	}
	agent := &fakeAgent{tenants: map[string]api.Tenant{}, bodies: map[string][]byte{}}
	for _, tenant := range tenants {
		agent.tenants[tenant.Spec.Name] = tenant
	}
	server := &http.Server{Handler: agent}
	go server.Serve(listener)
	t.Cleanup(func() { _ = server.Close() })
	return agent, socket
}

func capture(t *testing.T) *bytes.Buffer {
	t.Helper()
	var out bytes.Buffer
	stdout = &out
	t.Cleanup(func() { stdout = os.Stdout })
	return &out
}

func writeFile(t *testing.T, dir, name, content string) string {
	t.Helper()
	path := filepath.Join(dir, name)
	if err := os.WriteFile(path, []byte(content), 0o600); err != nil {
		t.Fatal(err)
	}
	return path
}

// Every file is read before anything is applied; a broken one stops all.
func TestAppApplyReadsEveryFileFirst(t *testing.T) {
	agent, socket := startAgent(t)
	dir := t.TempDir()
	good := writeFile(t, dir, "web.yaml", "image: nginx:1.27\nuser: \"101:101\"\nsecrets: [TOKEN]\n")
	typo := writeFile(t, dir, "shop.yaml", "image: nginx:1.27\nuser: \"101:101\"\nhelthPath: /x\n")
	badName := writeFile(t, dir, "Bad_Name.yaml", "image: nginx:1.27\nuser: \"101:101\"\n")
	rooted := writeFile(t, dir, "rooted.yaml", "image: nginx:1.27\nuser: \"0:0\"\n")
	capture(t)
	for _, files := range [][]string{{good, typo}, {good, badName}, {good, good}, {good, rooted}} {
		if err := runApp(append([]string{"apply", "-socket", socket}, files...)); err == nil {
			t.Errorf("apply %v succeeded", files)
		}
	}
	if len(agent.calls) != 0 {
		t.Fatalf("a broken apply called the agent: %v", agent.calls)
	}
	out := capture(t)
	if err := runApp([]string{"apply", good, "-socket", socket}); err != nil {
		t.Fatal(err)
	}
	var sent spec.Tenant
	if err := json.Unmarshal(agent.bodies["PUT /v1/apps/web"], &sent); err != nil || sent.Image != "nginx:1.27" || sent.Secrets["TOKEN"].Kind != "hex" {
		t.Fatalf("sent app = %+v, %v", sent, err)
	}
	if !strings.Contains(out.String(), "web                  created") {
		t.Fatalf("output:\n%s", out)
	}
}

// One unhealthy tenant fails the apply but does not use up the wait of the
// others; every tenant of the app is waited for, changed or not.
func TestAppApplyWaitsForEveryTenantSideBySide(t *testing.T) {
	tenant := func(name, app string) api.Tenant { return api.Tenant{Spec: spec.Tenant{Name: name}, App: app} }
	_, socket := startAgent(t, tenant("slow", "web"), tenant("a", "web"), tenant("b", "web"), tenant("other", "shop"))
	file := writeFile(t, t.TempDir(), "web.yaml", "image: nginx:1.27\nuser: \"101:101\"\n")
	out := capture(t)
	err := runApp([]string{"apply", file, "-socket", socket, "-wait", "2s"})
	if err == nil || !strings.Contains(err.Error(), "1 problem(s)") || !strings.Contains(err.Error(), "slow did not become healthy within 2s: health check: HTTP 500") {
		t.Fatalf("apply = %v", err)
	}
	if !strings.Contains(out.String(), "a is healthy") || !strings.Contains(out.String(), "b is healthy") || strings.Contains(out.String(), "other") {
		t.Fatalf("output:\n%s", out)
	}
}

// set starts from the tenant's overrides and changes only what it is told.
func TestTenantSetChangesOnlyWhatItIsTold(t *testing.T) {
	current := api.Tenant{Spec: spec.Tenant{Name: "acme"}, App: "web", Overrides: spec.Overrides{
		Image: "web@sha256:canary", Env: map[string]string{"TZ": "UTC", "MODE": "x"}, Secrets: spec.Secrets{"OLD": {Kind: "hex"}}, Limits: spec.Limits{MemoryMB: 768, CPUs: 2},
	}}
	agent, socket := startAgent(t, current)
	capture(t)
	if err := runTenant([]string{"set", "acme", "-socket", socket, "-env", "MODE=prod", "-unset-env", "TZ", "-secret", "NEW=password", "-unset-secret", "OLD", "-memory", "0", "-app-image"}); err != nil {
		t.Fatal(err)
	}
	var sent api.TenantRequest
	if err := json.Unmarshal(agent.bodies["PUT /v1/tenants/acme"], &sent); err != nil {
		t.Fatal(err)
	}
	want := api.TenantRequest{App: "web", Overrides: spec.Overrides{Env: map[string]string{"MODE": "prod"}, Secrets: spec.Secrets{"NEW": {Kind: "password"}}, Limits: spec.Limits{CPUs: 2}}}
	if !sent.Overrides.Equal(want.Overrides) || sent.App != "web" || sent.Name != "" {
		t.Fatalf("sent %+v, want %+v", sent, want)
	}
	for _, args := range [][]string{
		{"set", "acme", "-env", "NOEQUALS"},
		{"set", "acme", "-image", "x:1", "-app-image"},
		{"create", "beta"},
		{"create", "beta", "-app", "web", "-unset-env", "TZ"},
	} {
		if err := runTenant(append(args, "-socket", socket)); err == nil {
			t.Errorf("%v succeeded", args)
		}
	}
	if err := runTenant([]string{"create", "beta", "-app", "web", "-image", "web:2", "-cpus", "0.5", "-socket", socket}); err != nil {
		t.Fatal(err)
	}
	if err := json.Unmarshal(agent.bodies["POST /v1/tenants"], &sent); err != nil || sent.Name != "beta" || sent.Overrides.Image != "web:2" || sent.Overrides.Limits.CPUs != 0.5 {
		t.Fatalf("created %+v, %v", sent, err)
	}
}

// set works on an archived tenant too, and sends the generation it read.
func TestTenantSetOnAnArchivedTenant(t *testing.T) {
	agent, socket := startAgent(t, api.Tenant{Spec: spec.Tenant{Name: "old"}, App: "web", Generation: 7, ArchivedAt: time.Now(), Overrides: spec.Overrides{Env: map[string]string{"A": "1"}}})
	capture(t)
	if err := runTenant([]string{"set", "old", "-env", "B=2", "-socket", socket}); err != nil {
		t.Fatal(err)
	}
	var sent api.TenantRequest
	if err := json.Unmarshal(agent.bodies["PUT /v1/tenants/old"], &sent); err != nil || sent.Generation != 7 || sent.App != "web" || sent.Overrides.Env["A"] != "1" || sent.Overrides.Env["B"] != "2" {
		t.Fatalf("sent %+v, %v", sent, err)
	}
	// A no-op on an archived tenant does not wait for it to run.
	out := capture(t)
	if err := runTenant([]string{"set", "old", "-env", "A=1", "-wait", "5s", "-socket", socket}); err != nil || !strings.Contains(out.String(), "old unchanged; it is archived") {
		t.Fatalf("no-op set on an archived tenant = %v:\n%s", err, out)
	}
	if err := runTenant([]string{"set", "nobody", "-env", "B=2", "-socket", socket}); err == nil {
		t.Fatal("set on a missing tenant succeeded")
	}
}

func TestExampleAppIsValid(t *testing.T) {
	name, app, err := loadApp(filepath.Join("..", "..", "examples", "apps", "web.yaml"))
	if err != nil {
		t.Fatal(err)
	}
	if name != "web" {
		t.Fatalf("name = %q", name)
	}
	if err := spec.Compose(app, "acme", spec.Overrides{}).ValidateUnpinned(); err != nil {
		t.Fatal(err)
	}
}

// list shows archived tenants with the others.
func TestTenantListShowsArchivedTenants(t *testing.T) {
	_, socket := startAgent(t,
		api.Tenant{Spec: spec.Tenant{Name: "acme"}, App: "web"},
		api.Tenant{Spec: spec.Tenant{Name: "old"}, App: "web", ArchivedAt: time.Now()})
	out := capture(t)
	if err := runTenant([]string{"list", "-socket", socket}); err != nil {
		t.Fatal(err)
	}
	lines := strings.Split(strings.TrimSpace(out.String()), "\n")
	if len(lines) != 3 || !strings.HasPrefix(lines[1], "acme") || !strings.Contains(lines[1], "pending") || !strings.HasPrefix(lines[2], "old") || !strings.Contains(lines[2], "archived") {
		t.Fatalf("list:\n%s", out)
	}
}
