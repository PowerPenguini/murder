package panel

import (
	"context"
	"fmt"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/PowerPenguini/murder/internal/access"
	"github.com/PowerPenguini/murder/internal/api"
	"github.com/PowerPenguini/murder/internal/reconcile"
	"github.com/PowerPenguini/murder/internal/spec"
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
	// limitChanges are the changes of each tenant's memory limit, oldest
	// first: its use before one is measured against the limit then.
	limitChanges map[string][]limitChange
	// Pace is how long the demo's slow work takes, as an upgrade's install
	// or a backup: for watching jobs; none in tests.
	Pace time.Duration
	// diskKept is the disk limit, in GB, a tenant's volume keeps while its
	// node refuses the one its spec gives (below its data), as ZFS does;
	// data is its data's size, when not demoData (FillDisk).
	diskKept map[string]int
	data     map[string]int64
}

// diskOf is a tenant's data's size and the disk limit its volume has, in
// bytes, as its node reports them.
func (d *DemoCluster) diskOf(t api.Tenant) (used, limit int64) {
	d.mu.Lock()
	defer d.mu.Unlock()
	used, limit = demoData, int64(t.Spec.Limits.Or(demoDefaults).DiskGB)<<30
	if n, ok := d.data[t.Spec.Name]; ok {
		used = n
	}
	if gb, ok := d.diskKept[t.Spec.Name]; ok {
		limit = int64(gb) << 30
	}
	return used, limit
}

// FillDisk has a tenant's data grow to its disk limit, as data grows into
// a limit set long ago: the demo of a tenant that cannot write. It fails
// for a tenant without a disk limit.
func (d *DemoCluster) FillDisk(name string) error {
	t, err := d.Get(context.Background(), name)
	if err != nil {
		return err
	}
	_, limit := d.diskOf(t)
	if limit == 0 {
		return &api.Error{Status: 409, Message: "tenant " + name + " has no disk limit to fill"}
	}
	d.mu.Lock()
	defer d.mu.Unlock()
	if d.data == nil {
		d.data = map[string]int64{}
	}
	d.data[name] = limit
	return nil
}

func NewDemoCluster() *DemoCluster {
	d := &DemoCluster{tenants: map[string]api.Tenant{}, nodes: demoNodes{binaries: map[string]string{}, removed: map[string]bool{}, failed: map[string]time.Time{}, rebooted: map[string]time.Time{}, down: map[string]time.Time{}, started: time.Now().Add(-3 * 24 * time.Hour)}, Pace: time.Second}
	tenants, _ := d.Demo.List(context.Background())
	for _, t := range tenants {
		d.tenants[t.Spec.Name] = t
	}
	d.events, _ = d.Demo.Events(context.Background(), store.EventQuery{})
	// hel-1 failed once, before its tenants failed over: a fixed time, as
	// the events are, not one recomputed as the demo runs.
	for _, e := range d.events {
		if e.Action == "node.failed-over" && strings.HasPrefix(e.Detail, "hel-1") {
			d.nodes.failed["hel-1"] = e.At.Add(-30 * time.Second)
		}
	}
	d.demoApps()
	d.demoDomains()
	return d
}

// As is the cluster acted on by actor, as the agent's client is.
func (d *DemoCluster) As(actor string) ActionAgent { return &demoActor{DemoCluster: d, actor: actor} }

func (d *DemoCluster) List(context.Context) ([]api.Tenant, error) {
	d.mu.Lock()
	defer d.mu.Unlock()
	d.current()
	var out []api.Tenant
	for _, t := range d.tenants {
		out = append(out, withLimits(t))
	}
	return out, nil
}

// demoDefaults are the demo nodes' default limits, as a node's
// configuration has them by default: no disk limit.
var demoDefaults = spec.Limits{MemoryMB: 512, CPUs: 1, PIDs: 512}

// withLimits says, as a node's status does, the limits a tenant's
// container runs with and the node's defaults among them.
func withLimits(t api.Tenant) api.Tenant {
	if t.Status != nil && !t.Archived() {
		status := *t.Status
		status.Limits, status.Defaults = t.Spec.Limits.Or(demoDefaults), demoDefaults
		t.Status = &status
	}
	return t
}

func (d *DemoCluster) Get(_ context.Context, name string) (api.Tenant, error) {
	d.mu.Lock()
	defer d.mu.Unlock()
	d.current()
	t, ok := d.tenants[name]
	if !ok {
		return t, &api.Error{Status: 404, Message: fmt.Sprintf("tenant %s: not found", name)}
	}
	return withLimits(t), nil
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
	// atOnce has an app change move every tenant now, not in waves; first
	// names tenants that move at once, as its first wave.
	atOnce bool
	first  []string
}

// AppsFirst is the actor, but an app change in waves it makes moves the
// tenants named first, as the agent's client's First.
func (a *demoActor) AppsFirst(names []string) AppAgent {
	copied := *a
	copied.first = names
	return &copied
}

// AppsAtOnce is the actor, but an app change it makes moves every tenant
// at once, as the agent's client's AtOnce.
func (a *demoActor) AppsAtOnce() AppAgent {
	copied := *a
	copied.atOnce = true
	return &copied
}

func (a *demoActor) record(action, tenant, detail string) {
	id := int64(1)
	if len(a.events) > 0 {
		id = a.events[0].ID + 1
	}
	// As the agent records it: the panel's system user signs for the
	// person (apiserver's actor).
	a.events = append([]store.Event{{ID: id, At: time.Now(), Actor: a.actor + " via murder-panel", Action: action, Tenant: tenant, App: "web", Detail: detail}}, a.events...)
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
		// A new container: its restarts are its own.
		t.Status = &reconcile.Status{Phase: "starting", Generation: t.Generation, CheckedAt: time.Now(), DataOwner: dataOwnerOf(t.Status)}
	}
	a.tenants[name] = t
	a.record(action, name, detail)
	return t, nil
}

// demoStart is how long the demo's new containers take to start.
const demoStart = 6 * time.Second

// settle has a demo tenant's starting container run, a few seconds after
// it started: healthy, but for baltic-freight on 256 MB, which runs out of
// memory again, as the agent records it. The caller holds d.mu.
func (d *DemoCluster) settle(t api.Tenant) api.Tenant {
	if t.Status == nil || t.Status.Phase != "starting" || time.Since(t.Status.CheckedAt) < demoStart {
		return t
	}
	status := *t.Status
	status.Phase, status.Healthy, status.CheckedAt = "running", true, time.Now()
	// On the app's 256 MB its export runs out of memory again; given more,
	// it stays up.
	if limit := t.Spec.Limits.Or(spec.Limits{MemoryMB: 256}).MemoryMB; t.Spec.Name == "baltic-freight" && limit <= 256 {
		status.Healthy, status.Restarts = false, status.Restarts+1
		status.LastExit = "killed (SIGKILL) at " + time.Now().UTC().Format(time.RFC3339) + ", most likely for using more than its " + strconv.Itoa(limit) + " MB of memory"
		d.note("murder", "tenant.restarted", t.Spec.Name, t.App, "exit 137: killed (SIGKILL), most likely for using more than its "+strconv.Itoa(limit)+" MB of memory")
	}
	t.Status = &status
	return t
}

// note records an event, as the cluster does; the caller holds d.mu.
func (d *DemoCluster) note(actor, action, tenant, app, detail string) {
	id := int64(1)
	if len(d.events) > 0 {
		id = d.events[0].ID + 1
	}
	d.events = append([]store.Event{{ID: id, At: time.Now(), Actor: actor, Action: action, Tenant: tenant, App: app, Detail: detail}}, d.events...)
}

// current settles the tenants and moves the rollouts on, as time passes;
// the caller holds d.mu.
func (d *DemoCluster) current() {
	for name, t := range d.tenants {
		t = d.settleDomains(d.settle(t))
		// The agent checks a running tenant's health every pass.
		if s := t.Status; s != nil && s.Phase == "running" && time.Since(s.CheckedAt) > 15*time.Second {
			status := *s
			status.CheckedAt = time.Now().Add(-4 * time.Second)
			t.Status = &status
		}
		d.tenants[name] = t
	}
	d.drive()
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
		t.Status = &reconcile.Status{Phase: phase, Healthy: !suspended, CheckedAt: time.Now(), DataOwner: dataOwnerOf(t.Status)}
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
		t.Status = &reconcile.Status{Phase: "running", Healthy: true, CheckedAt: time.Now(), DataOwner: dataOwnerOf(t.Status)}
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

// RecordAccess records an access in the demo's audit log, as the agent
// does in the register.
func (a *demoActor) RecordAccess(_ context.Context, request access.Request) (access.Entry, error) {
	if err := request.Normalize(); err != nil {
		return access.Entry{}, &api.Error{Status: 403, Message: err.Error()}
	}
	a.mu.Lock()
	defer a.mu.Unlock()
	a.record(request.Kind.Action(), request.Tenant, request.Detail())
	a.events[0].App = ""
	entry, _ := access.FromEvent(a.events[0])
	return entry, nil
}

// demoGranted refuses a read of the demo's logs without an access.
func demoGranted(proof access.Proof) error {
	if proof.Grant == 0 {
		return &api.Error{Status: 403, Message: "not recorded in the access register"}
	}
	return nil
}

func (a *demoActor) Logs(_ context.Context, name string, tail int, proof access.Proof) ([]byte, error) {
	if err := demoGranted(proof); err != nil {
		return nil, err
	}
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
func (a *demoActor) PreviousLogs(_ context.Context, name string, proof access.Proof) (api.PreviousLogs, error) {
	if err := demoGranted(proof); err != nil {
		return api.PreviousLogs{}, err
	}
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
