package panel

import (
	"context"
	"fmt"
	"sync"
	"time"

	"github.com/PowerPenguini/murder/internal/api"
	"github.com/PowerPenguini/murder/internal/reconcile"
	"github.com/PowerPenguini/murder/internal/store"
)

// DemoCluster is the demo's cluster, its tenants changing as they are
// acted on, the audit log growing: for trying the panel's actions.
type DemoCluster struct {
	Demo
	mu      sync.Mutex
	tenants map[string]api.Tenant
	events  []store.Event
	nodes   demoNodes
	apps    map[string]api.App
	// revisions are each app's revisions, oldest first.
	revisions map[string][]store.Revision
	// Pace is how long the demo's slow work takes, as an upgrade's install
	// or a backup: for watching jobs; none in tests.
	Pace time.Duration
}

func NewDemoCluster() *DemoCluster {
	d := &DemoCluster{tenants: map[string]api.Tenant{}, nodes: demoNodes{binaries: map[string]string{}, removed: map[string]bool{}, failed: map[string]time.Time{}, started: time.Now().Add(-3 * 24 * time.Hour)}, Pace: time.Second}
	tenants, _ := d.Demo.List(context.Background())
	for _, t := range tenants {
		d.tenants[t.Spec.Name] = t
	}
	d.events, _ = d.Demo.Events(context.Background(), store.EventQuery{})
	d.demoApps()
	return d
}

// As is the cluster acted on by actor, as the agent's client is.
func (d *DemoCluster) As(actor string) ActionAgent { return &demoActor{d, actor} }

func (d *DemoCluster) List(context.Context) ([]api.Tenant, error) {
	d.mu.Lock()
	defer d.mu.Unlock()
	var out []api.Tenant
	for name, t := range d.tenants {
		d.tenants[name] = settle(t)
		out = append(out, d.tenants[name])
	}
	return out, nil
}

func (d *DemoCluster) Get(_ context.Context, name string) (api.Tenant, error) {
	d.mu.Lock()
	defer d.mu.Unlock()
	t, ok := d.tenants[name]
	if !ok {
		return t, &api.Error{Status: 404, Message: fmt.Sprintf("tenant %s: not found", name)}
	}
	d.tenants[name] = settle(t)
	return d.tenants[name], nil
}

func (d *DemoCluster) Events(_ context.Context, query store.EventQuery) ([]store.Event, error) {
	d.mu.Lock()
	defer d.mu.Unlock()
	var out []store.Event
	for _, e := range d.events {
		if (query.Tenant == "" || e.Tenant == query.Tenant) && (query.App == "" || e.App == query.App) && (query.Before == 0 || e.ID < query.Before) {
			out = append(out, e)
		}
		if query.Limit > 0 && len(out) == query.Limit {
			break
		}
	}
	return out, nil
}

type demoActor struct {
	*DemoCluster
	actor string
}

func (a *demoActor) record(action, tenant, detail string) {
	id := int64(1)
	if len(a.events) > 0 {
		id = a.events[0].ID + 1
	}
	a.events = append([]store.Event{{ID: id, At: time.Now(), Actor: a.actor, Action: action, Tenant: tenant, App: "web", Detail: detail}}, a.events...)
}

func (a *demoActor) change(name, action, detail string, change func(*api.Tenant) error) (api.Tenant, error) {
	a.mu.Lock()
	defer a.mu.Unlock()
	t, ok := a.tenants[name]
	if !ok {
		return t, &api.Error{Status: 404, Message: "tenant " + name + ": not found"}
	}
	if err := change(&t); err != nil {
		return t, err
	}
	t.Generation++
	// A new container starts, and settles a few seconds later (settle).
	if _, starts := operations[action]; starts && t.Status != nil && !t.Spec.Suspended {
		restarts := t.Status.Restarts
		t.Status = &reconcile.Status{Phase: "starting", Generation: t.Generation, Restarts: restarts, CheckedAt: time.Now()}
	}
	a.tenants[name] = t
	a.record(action, name, detail)
	return t, nil
}

// demoStart is how long the demo's new containers take to start.
const demoStart = 6 * time.Second

// settle has a demo tenant's starting container run, a few seconds after
// it started: healthy, but for baltic-freight, which runs out of memory
// again.
func settle(t api.Tenant) api.Tenant {
	if t.Status == nil || t.Status.Phase != "starting" || time.Since(t.Status.CheckedAt) < demoStart {
		return t
	}
	status := *t.Status
	status.Phase, status.Healthy, status.CheckedAt = "running", true, time.Now()
	if t.Spec.Name == "baltic-freight" {
		status.Healthy, status.Restarts, status.LastExit = false, status.Restarts+1, "exit 137 (most likely out of memory) just now"
	}
	t.Status = &status
	return t
}

func (a *demoActor) Create(ctx context.Context, request api.TenantRequest) (api.Applied, error) {
	node := request.Node
	switch node {
	case "":
		node = "fsn-1"
	case store.Auto:
		nodes, records := a.demoPlacement(ctx)
		node = store.LeastLoaded(nodes, records, "fsn-1")
	}
	a.mu.Lock()
	defer a.mu.Unlock()
	if _, ok := a.tenants[request.Name]; ok {
		return api.Applied{}, &api.Error{Status: 409, Message: "tenant " + request.Name + " exists already"}
	}
	t := api.Tenant{App: request.App, Node: node, Host: request.Name + ".example.com", URL: "https://" + request.Name + ".example.com", Generation: 1, CreatedAt: time.Now(),
		Status: &reconcile.Status{Phase: "starting", CheckedAt: time.Now()}}
	t.Spec.Name, t.Spec.Image, t.Spec.User = request.Name, "sha256:c23c323553d0a47eedff01b1cfb4a83a8f9b1b03f7f75a454c9ebf1391201156", "1000:1000"
	a.tenants[request.Name] = t
	a.record("tenant.created", request.Name, "on node "+node)
	return api.Applied{Tenant: t, Changed: true}, nil
}

func (a *demoActor) Suspend(_ context.Context, name string, suspended bool) (api.Tenant, error) {
	action := "tenant.resumed"
	if suspended {
		action = "tenant.suspended"
	}
	return a.change(name, action, "", func(t *api.Tenant) error {
		t.Spec.Suspended = suspended
		phase := "running"
		if suspended {
			phase = "stopped"
		}
		t.Status = &reconcile.Status{Phase: phase, Healthy: !suspended, CheckedAt: time.Now()}
		return nil
	})
}

func (a *demoActor) Roll(_ context.Context, name string) (api.Tenant, error) {
	return a.change(name, "tenant.rolled", "", func(*api.Tenant) error { return nil })
}

func (a *demoActor) Move(_ context.Context, name, to string) (api.Tenant, error) {
	return a.change(name, "tenant.moved", "", func(t *api.Tenant) error {
		if t.Replica == to {
			t.Replica = ""
		}
		t.Node = to
		return nil
	})
}

func (a *demoActor) Archive(_ context.Context, name string) error {
	_, err := a.change(name, "tenant.archived", "", func(t *api.Tenant) error {
		t.ArchivedAt, t.Status = time.Now(), nil
		return nil
	})
	return err
}

func (a *demoActor) Restore(_ context.Context, name string) (api.Tenant, error) {
	return a.change(name, "tenant.restored", "", func(t *api.Tenant) error {
		t.ArchivedAt = time.Time{}
		t.Status = &reconcile.Status{Phase: "running", Healthy: true, CheckedAt: time.Now()}
		return nil
	})
}

func (a *demoActor) Purge(_ context.Context, name string) error {
	a.mu.Lock()
	defer a.mu.Unlock()
	t, ok := a.tenants[name]
	if !ok || t.ArchivedAt.IsZero() {
		return &api.Error{Status: 409, Message: "tenant " + name + " is not archived"}
	}
	delete(a.tenants, name)
	a.record("tenant.purged", name, "")
	return nil
}

func (a *demoActor) Logs(_ context.Context, name string, tail int) ([]byte, error) {
	now := time.Now().UTC()
	var lines []byte
	if name == "baltic-freight" {
		// Its container was started again 4m ago, after the kill: its
		// current run's lines begin there, and its memory climbs again.
		start := now.Add(-4 * time.Minute)
		lines = fmt.Appendf(lines, "%s INFO listening on :8080 version=3\n", start.Add(2*time.Second).Format(time.RFC3339))
		lines = fmt.Appendf(lines, "%s INFO cache warmed entries=48210\n", start.Add(9*time.Second).Format(time.RFC3339))
	}
	for i := 12; i > 0; i-- {
		lines = fmt.Appendf(lines, "%s INFO request method=GET path=/api/freights status=200 took=%dms\n", now.Add(-time.Duration(i)*7*time.Second).Format(time.RFC3339), 3+i%5)
	}
	lines = fmt.Appendf(lines, "%s WARN \x1b[33mslow query\x1b[0m table=fuelings took=412ms\n", now.Add(-5*time.Second).Format(time.RFC3339))
	if name == "baltic-freight" {
		lines = fmt.Appendf(lines, "%s WARN heap at 241 MB of 256 MB: caches trimmed\n", now.Add(-3*time.Second).Format(time.RFC3339))
	}
	return lines, nil
}

// PreviousLogs: baltic-freight's node kept the lines of the container the
// kernel killed 4m ago; no other tenant's container went while the agent
// kept them.
func (a *demoActor) PreviousLogs(_ context.Context, name string) (api.PreviousLogs, error) {
	if name != "baltic-freight" {
		return api.PreviousLogs{}, &api.Error{Status: 404, Message: "tenant " + name + ": no previous container's lines are kept"}
	}
	exited := time.Now().UTC().Add(-4 * time.Minute)
	var lines []byte
	line := func(before time.Duration, text string) {
		lines = fmt.Appendf(lines, "%s %s\n", exited.Add(-before).Format(time.RFC3339), text)
	}
	for i := 18; i > 6; i-- {
		line(time.Duration(i)*9*time.Second, fmt.Sprintf("INFO request method=GET path=/api/freights status=200 took=%dms", 4+i%6))
	}
	line(52*time.Second, "INFO export started path=/api/freights/export rows=184220")
	line(40*time.Second, "WARN heap at 231 MB of 256 MB: caches trimmed")
	line(31*time.Second, "WARN slow query table=freights took=2210ms")
	line(22*time.Second, "WARN heap at 247 MB of 256 MB: caches trimmed")
	line(14*time.Second, "WARN gc pause 840ms heap=251MB")
	line(6*time.Second, "ERROR export failed: cannot allocate memory path=/api/freights/export rows=184220")
	line(2*time.Second, "WARN heap at 255 MB of 256 MB")
	return api.PreviousLogs{Container: "4f1c0e9a77b2", Image: "ghcr.io/example/web@sha256:c23c323553d0a47eedff01b1cfb4a83a8f9b1b03f7f75a454c9ebf1391201156",
		Why: "exited", ExitedAt: exited, ExitKnown: true, ExitCode: 137, MemoryMB: 256, At: exited.Add(time.Second), Lines: string(lines)}, nil
}
