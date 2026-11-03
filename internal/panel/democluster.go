package panel

import (
	"context"
	"fmt"
	"maps"
	"slices"
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
	return slices.Collect(maps.Values(d.tenants)), nil
}

func (d *DemoCluster) Get(_ context.Context, name string) (api.Tenant, error) {
	d.mu.Lock()
	defer d.mu.Unlock()
	t, ok := d.tenants[name]
	if !ok {
		return t, &api.Error{Status: 404, Message: fmt.Sprintf("tenant %s: not found", name)}
	}
	return t, nil
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
	a.events = append([]store.Event{{ID: id, At: time.Now(), Actor: a.actor, Action: action, Tenant: tenant, App: "addhoc", Detail: detail}}, a.events...)
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
	a.tenants[name] = t
	a.record(action, name, detail)
	return t, nil
}

func (a *demoActor) Create(_ context.Context, request api.TenantRequest) (api.Applied, error) {
	a.mu.Lock()
	defer a.mu.Unlock()
	if _, ok := a.tenants[request.Name]; ok {
		return api.Applied{}, &api.Error{Status: 409, Message: "tenant " + request.Name + " exists already"}
	}
	t := api.Tenant{App: request.App, Node: "fsn-1", Host: request.Name + ".example.com", URL: "https://" + request.Name + ".example.com", Generation: 1, CreatedAt: time.Now(),
		Status: &reconcile.Status{Phase: "starting", CheckedAt: time.Now()}}
	t.Spec.Name, t.Spec.Image, t.Spec.User = request.Name, "sha256:c23c323553d0a47eedff01b1cfb4a83a8f9b1b03f7f75a454c9ebf1391201156", "1000:1000"
	a.tenants[request.Name] = t
	a.record("tenant.created", request.Name, "on node fsn-1")
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
	for i := 12; i > 0; i-- {
		lines = fmt.Appendf(lines, "%s INFO request method=GET path=/api/freights status=200 took=%dms\n", now.Add(-time.Duration(i)*7*time.Second).Format(time.RFC3339), 3+i%5)
	}
	lines = fmt.Appendf(lines, "%s WARN \x1b[33mslow query\x1b[0m table=fuelings took=412ms\n", now.Format(time.RFC3339))
	return lines, nil
}
