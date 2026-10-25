package main

import (
	"bytes"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"flag"
	"io"
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

// fakeAgent answers the API calls apply makes, over a unix socket.
type fakeAgent struct {
	mu       sync.Mutex
	tenants  map[string]spec.Tenant
	archived []string
	calls    []string
}

func (a *fakeAgent) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	a.mu.Lock()
	defer a.mu.Unlock()
	a.calls = append(a.calls, r.Method+" "+r.URL.Path)
	switch {
	case r.Method == http.MethodGet && r.URL.Path == "/v1/tenants":
		var views []api.Tenant
		for _, tenant := range a.tenants {
			views = append(views, api.Tenant{Spec: tenant})
		}
		_ = json.NewEncoder(w).Encode(views)
	case r.Method == http.MethodGet && r.URL.Path == "/v1/archive":
		var views []api.ArchivedTenant
		for _, name := range a.archived {
			views = append(views, api.ArchivedTenant{Name: name})
		}
		_ = json.NewEncoder(w).Encode(views)
	case r.Method == http.MethodPost && r.URL.Path == "/v1/images/resolve":
		var request api.Resolved
		_ = json.NewDecoder(r.Body).Decode(&request)
		_ = json.NewEncoder(w).Encode(api.Resolved{Image: pin(request.Image)})
	case r.Method == http.MethodPost && r.URL.Path == "/v1/tenants":
		var tenant spec.Tenant
		_ = json.NewDecoder(r.Body).Decode(&tenant)
		a.tenants[tenant.Name] = tenant
		w.WriteHeader(http.StatusCreated)
		_ = json.NewEncoder(w).Encode(api.Applied{Tenant: api.Tenant{Spec: tenant}, Changed: true})
	case r.Method == http.MethodGet && strings.HasPrefix(r.URL.Path, "/v1/tenants/"):
		// "slow" never becomes healthy; everything else already is.
		name := strings.TrimPrefix(r.URL.Path, "/v1/tenants/")
		status := &reconcile.Status{Phase: reconcile.PhaseRunning, Healthy: name != "slow", Message: "health check: HTTP 500", CheckedAt: time.Now()}
		_ = json.NewEncoder(w).Encode(api.Tenant{Spec: a.tenants[name], Status: status})
	case r.Method == http.MethodPut && strings.HasPrefix(r.URL.Path, "/v1/tenants/"):
		var tenant spec.Tenant
		_ = json.NewDecoder(r.Body).Decode(&tenant)
		tenant = tenant.WithDefaults()
		changed := !tenant.Equal(a.tenants[tenant.Name])
		a.tenants[tenant.Name] = tenant
		_ = json.NewEncoder(w).Encode(api.Applied{Tenant: api.Tenant{Spec: tenant}, Changed: changed})
	default:
		w.WriteHeader(http.StatusNotFound)
		_, _ = w.Write([]byte(`{"error":"unexpected call"}`))
	}
}

// pin stands in for the agent pinning a tag to a digest.
func pin(image string) string {
	sum := sha256.Sum256([]byte(image))
	return image[:strings.LastIndex(image, ":")] + "@sha256:" + hex.EncodeToString(sum[:])
}

func TestApplyCreatesUpdatesAndNeverRemoves(t *testing.T) {
	dir := t.TempDir()
	socket := filepath.Join(dir, "agent.sock")
	listener, err := net.Listen("unix", socket)
	if err != nil {
		t.Fatal(err)
	}
	agent := &fakeAgent{tenants: map[string]spec.Tenant{
		"same":     spec.Tenant{Name: "same", Image: pin("app:1"), User: "1:1"}.WithDefaults(),
		"changed":  {Name: "changed", Image: "app:0", User: "1:1"},
		"leftover": {Name: "leftover", Image: "app:1", User: "1:1"},
	}, archived: []string{"old"}}
	server := &http.Server{Handler: agent}
	go server.Serve(listener)
	defer server.Close()
	fleetFile := filepath.Join(dir, "fleet.yaml")
	if err := os.WriteFile(fleetFile, []byte(`
apps:
  app: {image: "app:1", user: "1:1"}
tenants:
  same: {app: app}
  changed: {app: app}
  fresh: {app: app}
  old: {app: app}
`), 0o600); err != nil {
		t.Fatal(err)
	}
	var out bytes.Buffer
	stdout = &out
	defer func() { stdout = os.Stdout }()
	if err := runApply([]string{fleetFile, "-socket", socket}); err != nil {
		t.Fatal(err)
	}
	report := out.String()
	for _, want := range []string{"same                 unchanged", "changed              updated", "fresh                created", "old                  archived", "leftover             not in the fleet file"} {
		if !strings.Contains(report, want) {
			t.Errorf("report lacks %q:\n%s", want, report)
		}
	}
	resolves := 0
	for _, call := range agent.calls {
		if call == "POST /v1/images/resolve" {
			resolves++
		}
		if strings.HasPrefix(call, "DELETE") || strings.Contains(call, "archive/old") || strings.HasSuffix(call, "/leftover") {
			t.Errorf("apply made a call it must never make: %s", call)
		}
	}
	if resolves != 1 {
		t.Errorf("one image was resolved %d times", resolves)
	}
	if fresh, ok := agent.tenants["fresh"]; !ok || fresh.Image != pin("app:1") {
		t.Fatalf("fresh tenant = %+v, %v", fresh, ok)
	}
}

// One unhealthy tenant fails the apply but does not use up the wait of the
// others, and unchanged tenants are waited for too.
func TestApplyWaitsForEveryTenantSideBySide(t *testing.T) {
	dir := t.TempDir()
	socket := filepath.Join(dir, "agent.sock")
	listener, err := net.Listen("unix", socket)
	if err != nil {
		t.Fatal(err)
	}
	agent := &fakeAgent{tenants: map[string]spec.Tenant{
		"slow": spec.Tenant{Name: "slow", Image: pin("app:1"), User: "1:1"}.WithDefaults(),
	}}
	server := &http.Server{Handler: agent}
	go server.Serve(listener)
	defer server.Close()
	fleetFile := filepath.Join(dir, "fleet.yaml")
	if err := os.WriteFile(fleetFile, []byte(`
apps:
  app: {image: "app:1", user: "1:1"}
tenants:
  slow: {app: app}
  a: {app: app}
  b: {app: app}
`), 0o600); err != nil {
		t.Fatal(err)
	}
	var out bytes.Buffer
	stdout = &out
	defer func() { stdout = os.Stdout }()
	err = runApply([]string{fleetFile, "-socket", socket, "-wait", "2s"})
	if err == nil || !strings.Contains(err.Error(), "1 problem(s)") || !strings.Contains(err.Error(), "slow did not become healthy within 2s: health check: HTTP 500") {
		t.Fatalf("apply = %v", err)
	}
	if !strings.Contains(out.String(), "a is healthy") || !strings.Contains(out.String(), "b is healthy") {
		t.Fatalf("output:\n%s", out.String())
	}
}

// A broken spec stops the apply before it talks to the agent at all.
func TestApplyChecksTheFileFirst(t *testing.T) {
	dir := t.TempDir()
	socket := filepath.Join(dir, "agent.sock")
	listener, err := net.Listen("unix", socket)
	if err != nil {
		t.Fatal(err)
	}
	agent := &fakeAgent{tenants: map[string]spec.Tenant{}}
	server := &http.Server{Handler: agent}
	go server.Serve(listener)
	defer server.Close()
	fleetFile := filepath.Join(dir, "fleet.yaml")
	if err := os.WriteFile(fleetFile, []byte(`
apps:
  app: {image: "app:1", user: "0:0"}
tenants:
  acme: {app: app}
`), 0o600); err != nil {
		t.Fatal(err)
	}
	stdout = io.Discard
	defer func() { stdout = os.Stdout }()
	if err := runApply([]string{fleetFile, "-socket", socket}); err == nil || !strings.Contains(err.Error(), "acme:") {
		t.Fatalf("apply = %v", err)
	}
	if len(agent.calls) != 0 {
		t.Fatalf("apply called the agent: %v", agent.calls)
	}
}

func TestExampleFleetIsValid(t *testing.T) {
	if err := runApply([]string{filepath.Join("..", "..", "examples", "fleet.yaml"), "-socket", filepath.Join(t.TempDir(), "none.sock")}); err == nil || !strings.Contains(err.Error(), "reach the corral agent") {
		t.Fatalf("the example fleet should parse and only fail to reach an agent: %v", err)
	}
}
