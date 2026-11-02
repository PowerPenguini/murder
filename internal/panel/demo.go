package panel

import (
	"context"
	"fmt"
	"net/netip"
	"strings"
	"time"

	"github.com/PowerPenguini/murder/internal/api"
	"github.com/PowerPenguini/murder/internal/reconcile"
	"github.com/PowerPenguini/murder/internal/spec"
	"github.com/PowerPenguini/murder/internal/store"
)

// Demo is a made-up cluster, for trying the panel and testing it: three
// nodes, one of them down, and tenants in every state.
type Demo struct{ Now time.Time }

func (d Demo) now() time.Time {
	if d.Now.IsZero() {
		return time.Now()
	}
	return d.Now
}

func (d Demo) Nodes(context.Context) (api.Nodes, error) {
	now := d.now()
	node := func(name, private, public string, answers bool) api.NodeState {
		host := private[strings.LastIndex(private, ".")+1:]
		end := &store.Tunnel{Address: netip.MustParseAddr("10.77.0." + host), Key: "pnQ8Rn0r6k0yBqI3m3eH1vO8y7cWvCq1h8mJv0q3b2Y=", Port: 51820}
		return api.NodeState{Node: store.Node{Name: name, Address: private + ":7400", CreatedAt: now.Add(-40 * 24 * time.Hour), Public: []netip.Addr{netip.MustParseAddr(public)}, Tunnel: end}, Answers: answers}
	}
	n3 := node("hel-1", "10.0.1.3", "204.168.230.236", false)
	n3.FailedAt = now.Add(-6 * time.Minute)
	return api.Nodes{Cluster: "4NUMVTJKAY45MV4ANV47PA6COB", Self: "fsn-1", Leader: "fsn-1", Nodes: []api.NodeState{
		node("fsn-1", "10.0.1.1", "2.28.108.222", true),
		node("nbg-1", "10.0.1.2", "167.235.105.216", true),
		n3,
	}}, nil
}

func (d Demo) List(ctx context.Context) ([]api.Tenant, error) {
	now := d.now()
	running := func(restarts int) *reconcile.Status {
		return &reconcile.Status{Phase: "running", Healthy: true, Restarts: restarts, Generation: 3, CheckedAt: now.Add(-4 * time.Second)}
	}
	tenant := func(name, app, node, replica string, ready bool, status *reconcile.Status) api.Tenant {
		return api.Tenant{
			Spec: spec.Tenant{Name: name, Image: "sha256:c23c323553d0a47eedff01b1cfb4a83a8f9b1b03f7f75a454c9ebf1391201156", Port: 8080, HealthPath: "/health", DataPath: "/data", User: "1000:1000",
				Env: map[string]string{"DATA_DIR": "/data", "TZ": "Europe/Warsaw"}, Secrets: spec.Secrets{"SESSION_KEY": {Kind: "random", Bytes: 32}},
				Limits: spec.Limits{MemoryMB: 256, CPUs: 1, PIDs: 256, DiskGB: 10}},
			App: app, Node: node, Replica: replica, ReplicaReady: ready, ReplicaSyncedAt: now.Add(-9 * time.Second),
			Host: name + ".iqtms.io", URL: "https://" + name + ".iqtms.io", Generation: 3,
			CreatedAt: now.Add(-20 * 24 * time.Hour), UpdatedAt: now.Add(-2 * time.Hour), Status: status,
		}
	}
	moving := tenant("kowalski-trans", "addhoc", "nbg-1", "fsn-1", true, running(0))
	moving.ReplicaNext = "hel-1"
	unhealthy := tenant("baltic-freight", "addhoc", "nbg-1", "fsn-1", true, &reconcile.Status{Phase: "running", Healthy: false, Restarts: 3, LastExit: "exit 137 (most likely out of memory) 4m ago", Generation: 2, CheckedAt: now.Add(-3 * time.Second)})
	syncing := tenant("nordwind", "addhoc", "fsn-1", "nbg-1", false, running(0))
	suspended := tenant("test-staging", "addhoc", "fsn-1", "nbg-1", true, &reconcile.Status{Phase: "stopped", Generation: 5, CheckedAt: now.Add(-6 * time.Second)})
	suspended.Spec.Suspended = true
	archived := tenant("old-demo", "addhoc", "nbg-1", "", false, nil)
	archived.ArchivedAt = now.Add(-9 * 24 * time.Hour)
	return []api.Tenant{
		tenant("iqtms", "addhoc", "fsn-1", "nbg-1", true, running(0)),
		tenant("acme-logistics", "addhoc", "fsn-1", "nbg-1", true, running(1)),
		unhealthy, moving, syncing, suspended, archived,
		tenant("vistula-cargo", "addhoc", "nbg-1", "fsn-1", true, running(0)),
	}, nil
}

func (d Demo) Get(ctx context.Context, name string) (api.Tenant, error) {
	tenants, _ := d.List(ctx)
	for _, t := range tenants {
		if t.Spec.Name == name {
			return t, nil
		}
	}
	return api.Tenant{}, &api.Error{Status: 404, Message: fmt.Sprintf("tenant %s: not found", name)}
}

func (d Demo) Events(_ context.Context, query store.EventQuery) ([]store.Event, error) {
	now := d.now()
	all := []store.Event{
		{ID: 214, At: now.Add(-50 * time.Second), Actor: "murder", Action: "tenant.replica", Tenant: "kowalski-trans", App: "addhoc", Detail: "copy moves from fsn-1 to hel-1, which keeps fewer"},
		{ID: 213, At: now.Add(-5 * time.Minute), Actor: "murder (failover)", Action: "node.failed-over", Detail: "hel-1: 2 tenants run from their replicas"},
		{ID: 212, At: now.Add(-5 * time.Minute), Actor: "murder (failover)", Action: "tenant.failed-over", Tenant: "vistula-cargo", App: "addhoc", Detail: "hel-1 → nbg-1"},
		{ID: 211, At: now.Add(-5 * time.Minute), Actor: "murder (failover)", Action: "tenant.failed-over", Tenant: "nordwind", App: "addhoc", Detail: "hel-1 → fsn-1"},
		{ID: 210, At: now.Add(-48 * time.Minute), Actor: "jan via murder-panel", Action: "tenant.suspended", Tenant: "test-staging", App: "addhoc"},
		{ID: 209, At: now.Add(-3 * time.Hour), Actor: "root", Action: "app.updated", App: "addhoc", Detail: "image sha256:c23c323553d0"},
		{ID: 208, At: now.Add(-3 * time.Hour), Actor: "root", Action: "tenant.rolled", Tenant: "iqtms", App: "addhoc"},
		{ID: 207, At: now.Add(-26 * time.Hour), Actor: "root", Action: "node.upgraded", Detail: "fsn-1 to a10e3bfb8220"},
		{ID: 206, At: now.Add(-9 * 24 * time.Hour), Actor: "anna via murder-panel", Action: "tenant.archived", Tenant: "old-demo", App: "addhoc"},
	}
	var out []store.Event
	for _, e := range all {
		if (query.Tenant == "" || e.Tenant == query.Tenant) && (query.Before == 0 || e.ID < query.Before) {
			out = append(out, e)
		}
		if query.Limit > 0 && len(out) == query.Limit {
			break
		}
	}
	return out, nil
}
