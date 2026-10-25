package main

import (
	"bytes"
	"encoding/json"
	"flag"
	"net"
	"net/http"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"sync"
	"testing"

	"github.com/PowerPenguini/corral/internal/api"
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
	case r.Method == http.MethodPost && r.URL.Path == "/v1/tenants":
		var tenant spec.Tenant
		_ = json.NewDecoder(r.Body).Decode(&tenant)
		a.tenants[tenant.Name] = tenant
		w.WriteHeader(http.StatusCreated)
		_ = json.NewEncoder(w).Encode(api.Applied{Tenant: api.Tenant{Spec: tenant}, Changed: true})
	case r.Method == http.MethodPut && strings.HasPrefix(r.URL.Path, "/v1/tenants/"):
		var tenant spec.Tenant
		_ = json.NewDecoder(r.Body).Decode(&tenant)
		changed := !tenant.Equal(a.tenants[tenant.Name])
		a.tenants[tenant.Name] = tenant
		_ = json.NewEncoder(w).Encode(api.Applied{Tenant: api.Tenant{Spec: tenant}, Changed: changed})
	default:
		w.WriteHeader(http.StatusNotFound)
		_, _ = w.Write([]byte(`{"error":"unexpected call"}`))
	}
}

func TestApplyCreatesUpdatesAndNeverRemoves(t *testing.T) {
	dir := t.TempDir()
	socket := filepath.Join(dir, "agent.sock")
	listener, err := net.Listen("unix", socket)
	if err != nil {
		t.Fatal(err)
	}
	agent := &fakeAgent{tenants: map[string]spec.Tenant{
		"same":     {Name: "same", Image: "app:1", User: "1:1"},
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
	for _, call := range agent.calls {
		if strings.HasPrefix(call, "DELETE") || strings.Contains(call, "archive/old") || strings.HasSuffix(call, "/leftover") {
			t.Errorf("apply made a call it must never make: %s", call)
		}
	}
	if _, ok := agent.tenants["fresh"]; !ok {
		t.Fatal("fresh tenant was not created")
	}
}

func TestExampleFleetIsValid(t *testing.T) {
	if err := runApply([]string{filepath.Join("..", "..", "examples", "fleet.yaml"), "-socket", filepath.Join(t.TempDir(), "none.sock")}); err == nil || !strings.Contains(err.Error(), "reach the corral agent") {
		t.Fatalf("the example fleet should parse and only fail to reach an agent: %v", err)
	}
}
