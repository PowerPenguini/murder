package main

import (
	"encoding/json"
	"io"
	"net"
	"net/http"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/PowerPenguini/murder/internal/api"
	"github.com/PowerPenguini/murder/internal/reconcile"
	"github.com/PowerPenguini/murder/internal/spec"
	"github.com/PowerPenguini/murder/internal/store"
)

const (
	imageV1 = "ghcr.io/example/web@sha256:1111111111111111111111111111111111111111111111111111111111111111"
	imageV2 = "ghcr.io/example/web@sha256:2222222222222222222222222222222222222222222222222222222222222222"
)

// serveAgent answers the CLI with handler over a unix socket, and records
// each call with its query.
func serveAgent(t *testing.T, handler func(call string, w http.ResponseWriter, r *http.Request)) (string, func() []string) {
	t.Helper()
	socket := filepath.Join(t.TempDir(), "agent.sock")
	listener, err := net.Listen("unix", socket)
	if err != nil {
		t.Fatal(err)
	}
	var mu sync.Mutex
	var calls []string
	server := &http.Server{Handler: http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		call := r.Method + " " + r.URL.Path
		mu.Lock()
		if r.URL.RawQuery != "" {
			calls = append(calls, call+"?"+r.URL.RawQuery)
		} else {
			calls = append(calls, call)
		}
		mu.Unlock()
		handler(call, w, r)
	})}
	go server.Serve(listener)
	t.Cleanup(func() { _ = server.Close() })
	return socket, func() []string {
		mu.Lock()
		defer mu.Unlock()
		return append([]string(nil), calls...)
	}
}

func TestAppCheckAsksAboutEveryAppGivenATag(t *testing.T) {
	updated := time.Date(2026, 11, 3, 10, 0, 0, 0, time.UTC)
	apps := []api.App{
		{Name: "web", Tag: "ghcr.io/example/web:stable", Revision: 3, Spec: spec.Tenant{Image: imageV1}, Latest: imageV2, UpdatedAt: updated},
		{Name: "shop", Tag: "ghcr.io/example/shop:1", Revision: 1, Spec: spec.Tenant{Image: imageV1}, UpdatedAt: updated},
		{Name: "pinned", Spec: spec.Tenant{Image: imageV1}, UpdatedAt: updated},
	}
	socket, calls := serveAgent(t, func(call string, w http.ResponseWriter, _ *http.Request) {
		switch call {
		case "GET /v1/apps":
			_ = json.NewEncoder(w).Encode(apps)
		case "POST /v1/apps/web/check":
			_ = json.NewEncoder(w).Encode(api.AppCheck{App: apps[0], Image: imageV2, Newer: true})
		case "POST /v1/apps/shop/check":
			w.WriteHeader(http.StatusBadGateway)
			_, _ = w.Write([]byte(`{"error":"ask the registry about ghcr.io/example/shop:1: denied"}`))
		default:
			w.WriteHeader(http.StatusNotFound)
			_, _ = w.Write([]byte(`{"error":"unexpected call"}`))
		}
	})
	out := capture(t)
	err := runApp([]string{"check", "-socket", socket})
	if err == nil || !strings.Contains(err.Error(), "denied") {
		t.Fatalf("a failed check: %v", err)
	}
	if got := strings.Join(calls(), ","); got != "GET /v1/apps,POST /v1/apps/web/check,POST /v1/apps/shop/check" {
		t.Fatalf("calls %s", got)
	}
	if !strings.Contains(out.String(), "web                  newer image ghcr.io/example/web@222222222222 of ghcr.io/example/web:stable (runs ghcr.io/example/web@111111111111): murder app update web") {
		t.Fatalf("output:\n%s", out)
	}
	out = capture(t)
	if err := runApp([]string{"list", "-socket", socket}); err != nil {
		t.Fatal(err)
	}
	lines := strings.Split(out.String(), "\n")
	if !strings.Contains(lines[0], "REVISION") || !strings.Contains(lines[1], "ghcr.io/example/web:stable") || !strings.Contains(lines[1], "ghcr.io/example/web@222222222222") || !strings.HasPrefix(lines[3], "pinned  ") {
		t.Fatalf("list:\n%s", out)
	}
}

func TestAppUpdateRollbackAndHistory(t *testing.T) {
	updated := time.Date(2026, 11, 3, 10, 0, 0, 0, time.UTC)
	app := api.App{Name: "web", Tag: "ghcr.io/example/web:stable", Revision: 3, Spec: spec.Tenant{Image: imageV2}, UpdatedAt: updated}
	revisions := []store.Revision{
		{Revision: 3, Spec: spec.Tenant{Image: imageV2}, Tag: app.Tag, Actor: "alice", CreatedAt: updated},
		{Revision: 2, Spec: spec.Tenant{Image: imageV1}, Tag: app.Tag, Actor: "bob", CreatedAt: updated.Add(-time.Hour)},
	}
	socket, calls := serveAgent(t, func(call string, w http.ResponseWriter, _ *http.Request) {
		switch call {
		case "GET /v1/apps/web":
			_ = json.NewEncoder(w).Encode(app)
		case "GET /v1/apps/web/revisions":
			_ = json.NewEncoder(w).Encode(revisions)
		case "POST /v1/apps/web/update":
			next := app
			next.Revision = 4
			_ = json.NewEncoder(w).Encode(api.AppApplied{App: next, Changed: true, Tenants: []api.Tenant{{Spec: spec.Tenant{Name: "acme"}}}})
		case "POST /v1/apps/web/revisions/2/restore", "POST /v1/apps/web/revisions/1/restore":
			next := app
			next.Revision, next.Spec.Image = 4, imageV1
			_ = json.NewEncoder(w).Encode(api.AppApplied{App: next, Changed: true})
		default:
			w.WriteHeader(http.StatusNotFound)
			_, _ = w.Write([]byte(`{"error":"unexpected call"}`))
		}
	})
	out := capture(t)
	if err := runApp([]string{"update", "web", "-socket", socket}); err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(out.String(), "revision 4: ghcr.io/example/web@222222222222") || !strings.Contains(out.String(), "rolls out to acme") {
		t.Fatalf("update output:\n%s", out)
	}
	if err := runApp([]string{"rollback", "web", "-socket", socket}); err != nil {
		t.Fatal(err)
	}
	if err := runApp([]string{"rollback", "web", "-to", "1", "-socket", socket}); err != nil {
		t.Fatal(err)
	}
	stamp := "if=2026-11-03T10%3A00%3A00Z"
	want := []string{
		"POST /v1/apps/web/update",
		"GET /v1/apps/web", "GET /v1/apps/web/revisions", "POST /v1/apps/web/revisions/2/restore?" + stamp,
		"GET /v1/apps/web", "POST /v1/apps/web/revisions/1/restore?" + stamp,
	}
	if got := strings.Join(calls(), "\n"); got != strings.Join(want, "\n") {
		t.Fatalf("calls:\n%s\nwant:\n%s", got, strings.Join(want, "\n"))
	}
	out = capture(t)
	if err := runApp([]string{"history", "web", "-socket", socket}); err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(out.String(), "3 (current)") || !strings.Contains(out.String(), "bob") {
		t.Fatalf("history:\n%s", out)
	}
}

func TestAppApplyWithUpdateAsksForTheNewestImage(t *testing.T) {
	socket, calls := serveAgent(t, func(_ string, w http.ResponseWriter, r *http.Request) {
		var app spec.Tenant
		_ = json.NewDecoder(r.Body).Decode(&app)
		_ = json.NewEncoder(w).Encode(api.AppApplied{App: api.App{Name: "web", Spec: app}, Changed: true})
	})
	file := writeFile(t, t.TempDir(), "web.yaml", "image: ghcr.io/example/web:stable\nuser: \"101:101\"\n")
	capture(t)
	if err := runApp([]string{"apply", file, "-update", "-socket", socket}); err != nil {
		t.Fatal(err)
	}
	if err := runApp([]string{"apply", file, "-socket", socket}); err != nil {
		t.Fatal(err)
	}
	if got := strings.Join(calls(), ","); got != "PUT /v1/apps/web?update=true,PUT /v1/apps/web" {
		t.Fatalf("calls %s", got)
	}
}

func TestApplyWaitsForTheWavesAndRolloutActs(t *testing.T) {
	rolloutPoll = 10 * time.Millisecond
	defer func() { rolloutPoll = 5 * time.Second }()
	var mu sync.Mutex
	asked := 0
	state := store.RolloutRunning
	changed := true
	socket, calls := serveAgent(t, func(call string, w http.ResponseWriter, r *http.Request) {
		mu.Lock()
		defer mu.Unlock()
		switch {
		case call == "PUT /v1/apps/web":
			var app spec.Tenant
			_ = json.NewDecoder(r.Body).Decode(&app)
			_ = json.NewEncoder(w).Encode(api.AppApplied{App: api.App{Name: "web", Spec: app}, Changed: changed, Rollout: changed && r.URL.Query().Get("rollout") != "now",
				Tenants: []api.Tenant{{Spec: spec.Tenant{Name: "acme"}, App: "web"}}})
		case call == "GET /v1/apps/web":
			// Running twice, then as the test says.
			asked++
			current := store.RolloutRunning
			if asked > 2 {
				current = state
			}
			_ = json.NewEncoder(w).Encode(api.App{Name: "web", Revision: 2, Rollout: &store.Rollout{App: "web", ToRevision: 2, State: current, Wave: asked, Reason: "acme did not come up"}})
		case call == "GET /v1/tenants":
			_ = json.NewEncoder(w).Encode([]api.Tenant{{Spec: spec.Tenant{Name: "acme"}, App: "web"}})
		case strings.HasPrefix(call, "GET /v1/tenants/"):
			_ = json.NewEncoder(w).Encode(api.Tenant{Spec: spec.Tenant{Name: "acme"}, App: "web", Status: &reconcile.Status{Phase: reconcile.PhaseRunning, Healthy: true, CheckedAt: time.Now()}})
		case call == "POST /v1/apps/web/rollout/pause":
			_ = json.NewEncoder(w).Encode(api.App{Name: "web"})
		default:
			w.WriteHeader(http.StatusNotFound)
			_, _ = w.Write([]byte(`{"error":"unexpected call"}`))
		}
	})
	file := writeFile(t, t.TempDir(), "web.yaml", "image: ghcr.io/example/web:stable\nuser: \"101:101\"\n")
	mu.Lock()
	state = store.RolloutDone
	mu.Unlock()
	out := capture(t)
	if err := runApp([]string{"apply", file, "-wait", "10s", "-socket", socket}); err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(out.String(), "rolls out in waves to acme (murder app rollout web)") || !strings.Contains(out.String(), "web                  rolled out") || !strings.Contains(out.String(), "acme is healthy") {
		t.Fatalf("output:\n%s", out)
	}
	// A rollout that pauses fails the wait, saying why.
	mu.Lock()
	asked, state = 0, store.RolloutPaused
	mu.Unlock()
	capture(t)
	if err := runApp([]string{"apply", file, "-wait", "10s", "-socket", socket}); err == nil || !strings.Contains(err.Error(), "the rollout is paused: acme did not come up") {
		t.Fatalf("a paused rollout: %v", err)
	}
	// So does a retry that changes nothing: it waits for what the first
	// began.
	mu.Lock()
	asked, changed = 0, false
	mu.Unlock()
	if err := runApp([]string{"apply", file, "-wait", "10s", "-socket", socket}); err == nil || !strings.Contains(err.Error(), "the rollout is paused") {
		t.Fatalf("an unchanged retry: %v", err)
	}
	mu.Lock()
	changed = true
	mu.Unlock()
	if err := runApp([]string{"apply", file, "-now", "-socket", socket}); err != nil {
		t.Fatal(err)
	}
	if err := runApp([]string{"rollout", "pause", "web", "-socket", socket}); err != nil {
		t.Fatal(err)
	}
	got := strings.Join(calls(), "\n")
	if !strings.Contains(got, "PUT /v1/apps/web?rollout=now") || !strings.Contains(got, "POST /v1/apps/web/rollout/pause") {
		t.Fatalf("calls:\n%s", got)
	}
}

// A restore says what it loses and does nothing without -yes; with it, it
// takes the tenants named back with their data.
func TestRolloutRestoreAsksFirst(t *testing.T) {
	var body []byte
	socket, calls := serveAgent(t, func(call string, w http.ResponseWriter, r *http.Request) {
		switch call {
		case "GET /v1/tenants":
			_ = json.NewEncoder(w).Encode([]api.Tenant{
				{Spec: spec.Tenant{Name: "acme"}, App: "web", BeforeRevision: 1},
				{Spec: spec.Tenant{Name: "beta"}, App: "web", AppRevision: 1},
				{Spec: spec.Tenant{Name: "other"}, App: "blog", BeforeRevision: 3},
			})
		case "POST /v1/apps/web/rollout/restore":
			body, _ = io.ReadAll(r.Body)
			_ = json.NewEncoder(w).Encode(api.RolloutRestored{App: api.App{Name: "web"}, Tenants: []string{"acme"}})
		case "GET /v1/apps/web":
			_ = json.NewEncoder(w).Encode(api.App{Name: "web", Revision: 2, Rollout: &store.Rollout{App: "web", ToRevision: 2, State: store.RolloutPaused, Wave: 1, Reason: "acme went back"}})
		default:
			w.WriteHeader(http.StatusNotFound)
			_, _ = w.Write([]byte(`{"error":"unexpected call"}`))
		}
	})
	err := runApp([]string{"rollout", "restore", "web", "-socket", socket})
	if err == nil || !strings.Contains(err.Error(), "takes acme (to revision 1) back") || !strings.Contains(err.Error(), "writes to these tenants since they moved are lost") || !strings.Contains(err.Error(), "-yes") {
		t.Fatalf("without -yes: %v", err)
	}
	if err := runApp([]string{"rollout", "restore", "web", "-tenant", "acme", "-socket", socket}); err == nil || !strings.Contains(err.Error(), "takes acme back") {
		t.Fatalf("one named, without -yes: %v", err)
	}
	if got := strings.Join(calls(), "\n"); strings.Contains(got, "POST") {
		t.Fatalf("restored without -yes:\n%s", got)
	}
	out := capture(t)
	if err := runApp([]string{"rollout", "restore", "web", "-tenant", "acme", "-yes", "-socket", socket}); err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(string(body), `"tenants":["acme"]`) || !strings.Contains(out.String(), "acme go back to the revision before, their data too") {
		t.Fatalf("body %s, output:\n%s", body, out)
	}
	if err := runApp([]string{"rollout", "pause", "web", "-yes", "-socket", socket}); err == nil {
		t.Fatal("-yes taken by another action")
	}
}
