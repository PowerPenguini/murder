package replicate

import (
	"context"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net/netip"
	"path/filepath"
	"slices"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/PowerPenguini/murder/internal/metrics"
	"github.com/PowerPenguini/murder/internal/spec"
	"github.com/PowerPenguini/murder/internal/store"
	"github.com/PowerPenguini/murder/internal/volume"
)

// memoryVolumes is one node's ZFS, in memory: each tenant's data is a
// version number, each snapshot remembers the version it took.
type memoryVolumes struct {
	mu sync.Mutex
	// data is each tenant's live data or copy.
	data map[string]*memoryData
	// aside are the datasets put aside.
	aside   []string
	clock   func() time.Time
	failRcv error
}

type memoryData struct {
	version   int
	replica   bool
	mounted   bool
	snapshots map[string]int // short name → version
}

func newVolumes(clock func() time.Time) *memoryVolumes {
	return &memoryVolumes{data: map[string]*memoryData{}, clock: clock}
}

// write changes a tenant's live data.
func (v *memoryVolumes) write(tenant string) {
	v.mu.Lock()
	defer v.mu.Unlock()
	d, ok := v.data[tenant]
	if !ok {
		d = &memoryData{mounted: true, snapshots: map[string]int{}}
		v.data[tenant] = d
	}
	d.version++
}

func (v *memoryVolumes) Snapshot(_ context.Context, tenant, reason string) (string, error) {
	v.mu.Lock()
	defer v.mu.Unlock()
	d, ok := v.data[tenant]
	if !ok {
		return "", fmt.Errorf("no data of %s", tenant)
	}
	name := "murder-" + reason + "-" + v.clock().UTC().Format("20060102T150405.000Z")
	d.snapshots[name] = d.version
	return "tank/" + tenant + "@" + name, nil
}

func (v *memoryVolumes) Snapshots(_ context.Context, tenant, reason string) ([]string, error) {
	v.mu.Lock()
	defer v.mu.Unlock()
	d, ok := v.data[tenant]
	if !ok {
		return nil, nil
	}
	var names []string
	for name := range d.snapshots {
		if strings.HasPrefix(name, "murder-"+reason+"-") {
			names = append(names, name)
		}
	}
	slices.Sort(names)
	return names, nil
}

func (v *memoryVolumes) Written(_ context.Context, tenant, snapshot string) (int64, error) {
	v.mu.Lock()
	defer v.mu.Unlock()
	d := v.data[tenant]
	taken, ok := d.snapshots[snapshot]
	if !ok {
		return 0, fmt.Errorf("no snapshot %s", snapshot)
	}
	return int64(d.version - taken), nil
}

func (v *memoryVolumes) Prune(_ context.Context, tenant, reason string, keep int) error {
	names, _ := v.Snapshots(context.Background(), tenant, reason)
	v.mu.Lock()
	defer v.mu.Unlock()
	for _, name := range names[:max(0, len(names)-keep)] {
		delete(v.data[tenant].snapshots, name)
	}
	return nil
}

func (v *memoryVolumes) DestroySnapshot(_ context.Context, tenant, snapshot string) error {
	v.mu.Lock()
	defer v.mu.Unlock()
	delete(v.data[tenant].snapshots, snapshot)
	return nil
}

// SendSnapshot writes "from|to|version".
func (v *memoryVolumes) SendSnapshot(_ context.Context, tenant, from, to string, w io.Writer) error {
	v.mu.Lock()
	d := v.data[tenant]
	version, ok := d.snapshots[to]
	_, hasFrom := d.snapshots[from]
	v.mu.Unlock()
	if !ok || (from != "" && !hasFrom) {
		return fmt.Errorf("send %s→%s: no such snapshot", from, to)
	}
	_, err := fmt.Fprintf(w, "%s|%s|%d", from, to, version)
	return err
}

func (v *memoryVolumes) ReceiveReplica(_ context.Context, tenant string, whole bool, r io.Reader, keep int) error {
	stream, err := io.ReadAll(r)
	if err != nil {
		return err
	}
	fields := strings.Split(string(stream), "|")
	if len(fields) != 3 {
		return fmt.Errorf("stream %q", stream)
	}
	from, to := fields[0], fields[1]
	var version int
	if _, err := fmt.Sscanf(fields[2], "%d", &version); err != nil {
		return err
	}
	v.mu.Lock()
	defer v.mu.Unlock()
	if v.failRcv != nil {
		return v.failRcv
	}
	d, exists := v.data[tenant]
	if exists && !d.replica {
		return errors.New("not a copy")
	}
	switch {
	case whole:
		if from != "" {
			return errors.New("a whole stream with a base")
		}
		d = &memoryData{replica: true, snapshots: map[string]int{}}
		v.data[tenant] = d
	case !exists:
		return errors.New("no copy to add to")
	default:
		if _, ok := d.snapshots[from]; !ok {
			return fmt.Errorf("the copy has no snapshot %s", from)
		}
	}
	d.snapshots[to] = version
	d.version = version
	names := slices.Sorted(func(yield func(string) bool) {
		for name := range d.snapshots {
			if !yield(name) {
				return
			}
		}
	})
	for _, name := range names[:max(0, len(names)-keep)] {
		delete(d.snapshots, name)
	}
	return nil
}

func (v *memoryVolumes) Datasets(context.Context) ([]volume.Dataset, error) {
	v.mu.Lock()
	defer v.mu.Unlock()
	var datasets []volume.Dataset
	for name, d := range v.data {
		datasets = append(datasets, volume.Dataset{Tenant: name, Replica: d.replica, Mounted: d.mounted})
	}
	return datasets, nil
}

func (v *memoryVolumes) DropReplica(_ context.Context, tenant string) error {
	v.mu.Lock()
	defer v.mu.Unlock()
	if d := v.data[tenant]; d == nil || !d.replica {
		return errors.New("not a copy")
	}
	delete(v.data, tenant)
	return nil
}

func (v *memoryVolumes) IsReplica(_ context.Context, tenant string) (bool, error) {
	v.mu.Lock()
	defer v.mu.Unlock()
	d := v.data[tenant]
	return d != nil && d.replica, nil
}

func (v *memoryVolumes) RollbackReplica(context.Context, string) error { return nil }

func (v *memoryVolumes) PutAside(_ context.Context, tenant, why string) (string, error) {
	v.mu.Lock()
	defer v.mu.Unlock()
	delete(v.data, tenant)
	name := "murder-failed-" + tenant + "-" + why
	v.aside = append(v.aside, name)
	return name, nil
}

// node is one node of a test cluster: its state and its data.
type node struct {
	name     string
	store    *store.SQLite
	volumes  *memoryVolumes
	receiver *Receiver
}

// peer reaches the nodes of a test cluster as the node named from.
type peer struct {
	from  string
	nodes map[string]*node
	mu    sync.Mutex
	sends []string
}

func (p *peer) Snapshots(ctx context.Context, to, tenant string) ([]string, error) {
	return p.nodes[to].receiver.Snapshots(ctx, p.from, tenant)
}

func (p *peer) Send(ctx context.Context, to, tenant string, whole bool, data io.Reader) error {
	p.mu.Lock()
	p.sends = append(p.sends, fmt.Sprintf("%s→%s whole=%v", tenant, to, whole))
	p.mu.Unlock()
	return p.nodes[to].receiver.Receive(ctx, p.from, tenant, whole, data)
}

type fixture struct {
	clock time.Time
	nodes map[string]*node
}

func (f *fixture) now() time.Time { return f.clock }

// newFixture makes nodes n1, n2 and n3, each with its own copy of the same
// state: n1 runs tenant acme, whose replica is n2.
func newFixture(t *testing.T) *fixture {
	t.Helper()
	f := &fixture{clock: time.Date(2026, 11, 2, 10, 0, 0, 0, time.UTC), nodes: map[string]*node{}}
	for _, name := range []string{"n1", "n2", "n3"} {
		path := filepath.Join(t.TempDir(), "murder.db")
		if err := store.Init(path, name); err != nil {
			t.Fatal(err)
		}
		st, err := store.Open(path, store.Options{NetworkPool: netip.MustParsePrefix("10.240.0.0/16"), Sealer: func(string, string, spec.Secret) (string, error) { return "sealed", nil }})
		if err != nil {
			t.Fatal(err)
		}
		t.Cleanup(func() { st.Close() })
		volumes := newVolumes(f.now)
		f.nodes[name] = &node{name: name, store: st, volumes: volumes, receiver: &Receiver{Store: st, Volumes: volumes}}
	}
	f.change(t, func(st *store.SQLite) {
		for i, name := range []string{"n1", "n2", "n3"} {
			if err := st.PutNode(store.Node{Name: name, Address: fmt.Sprintf("10.0.0.%d:7400", i+1)}, "test"); err != nil {
				t.Fatal(err)
			}
		}
		app := spec.Tenant{Image: "sha256:" + strings.Repeat("a", 64), HealthPath: "/health", User: "1000:1000"}
		if _, err := st.PutApp(store.App{Name: "web", Spec: app}, "test"); err != nil {
			t.Fatal(err)
		}
		if _, err := st.Create(store.NewTenant{Name: "acme", App: "web", Node: "n1"}, "test"); err != nil {
			t.Fatal(err)
		}
		if _, err := st.PlaceReplicas("test"); err != nil {
			t.Fatal(err)
		}
	})
	if record, _ := f.nodes["n1"].store.Get("acme"); record.Replica != "n2" {
		t.Fatalf("acme's replica: %q", record.Replica)
	}
	return f
}

// change makes the same change on every node, as the cluster's log would.
func (f *fixture) change(t *testing.T, change func(*store.SQLite)) {
	t.Helper()
	for _, name := range []string{"n1", "n2", "n3"} {
		change(f.nodes[name].store)
	}
}

func (f *fixture) replicator(name string) (*Replicator, *peer) {
	p := &peer{from: name, nodes: f.nodes}
	n := f.nodes[name]
	return &Replicator{Store: n.store, Volumes: n.volumes, Peer: p, Log: slog.New(slog.NewTextHandler(io.Discard, nil)), Now: f.now}, p
}

// A tenant's copy is sent whole first, then only the changes, and nothing
// while nothing changed; each side keeps only its newest snapshots.
func TestACopyFollowsTheData(t *testing.T) {
	f := newFixture(t)
	n1, n2 := f.nodes["n1"], f.nodes["n2"]
	n1.volumes.write("acme")
	r, p := f.replicator("n1")
	ctx := context.Background()
	for i := range 5 {
		f.clock = f.clock.Add(15 * time.Second)
		if i != 2 {
			n1.volumes.write("acme")
		}
		r.Pass(ctx)
	}
	want := []string{"acme→n2 whole=true", "acme→n2 whole=false", "acme→n2 whole=false", "acme→n2 whole=false"}
	if !slices.Equal(p.sends, want) {
		t.Fatalf("sends = %v, want %v", p.sends, want)
	}
	copy := n2.volumes.data["acme"]
	if copy == nil || !copy.replica || copy.version != n1.volumes.data["acme"].version {
		t.Fatalf("the copy: %+v, the data at version %d", copy, n1.volumes.data["acme"].version)
	}
	if len(copy.snapshots) > 3 || len(n1.volumes.data["acme"].snapshots) > 3 {
		t.Fatalf("snapshots kept: copy %d, data %d", len(copy.snapshots), len(n1.volumes.data["acme"].snapshots))
	}
	// Ready once the whole copy is there, as every node learns from the log.
	if record, _ := n1.store.Get("acme"); !record.ReplicaReady {
		t.Fatal("the copy is not ready")
	}
	if synced := r.Synced("acme"); !synced.Equal(f.clock) {
		t.Fatalf("synced at %v, want %v", synced, f.clock)
	}
	var page strings.Builder
	r.WriteMetrics(metrics.NewWriter(&page))
	if !strings.Contains(page.String(), `murder_tenant_replica_lag_seconds{tenant="acme"} 0`) || !strings.Contains(page.String(), `murder_tenant_replica_failing{tenant="acme"} 0`) {
		t.Fatalf("metrics:\n%s", page.String())
	}
}

// A copy whose base the data no longer has gets the whole data again; a
// send that fails leaves the copy's base among the snapshots kept.
func TestACopyWithoutABaseIsSentWhole(t *testing.T) {
	f := newFixture(t)
	n1, n2 := f.nodes["n1"], f.nodes["n2"]
	n1.volumes.write("acme")
	r, p := f.replicator("n1")
	ctx := context.Background()
	r.Pass(ctx)
	// Failures: the snapshots they took go, the base stays.
	n2.volumes.failRcv = errors.New("disk full")
	for range 5 {
		f.clock = f.clock.Add(15 * time.Second)
		n1.volumes.write("acme")
		r.Pass(ctx)
	}
	theirs, _ := n2.receiver.Snapshots(ctx, "n1", "acme")
	base := theirs[len(theirs)-1]
	if _, ok := n1.volumes.data["acme"].snapshots[base]; !ok {
		t.Fatalf("the copy's base %s is gone from the data", base)
	}
	var page strings.Builder
	r.WriteMetrics(metrics.NewWriter(&page))
	if !strings.Contains(page.String(), `murder_tenant_replica_failing{tenant="acme"} 1`) {
		t.Fatalf("metrics:\n%s", page.String())
	}
	n2.volumes.failRcv = nil
	f.clock = f.clock.Add(15 * time.Second)
	r.Pass(ctx)
	if last := p.sends[len(p.sends)-1]; last != "acme→n2 whole=false" {
		t.Fatalf("after the failures: %s", last)
	}
	// The data lost its snapshots, as if pruned by hand: whole again.
	n1.volumes.data["acme"].snapshots = map[string]int{}
	n1.volumes.write("acme")
	f.clock = f.clock.Add(15 * time.Second)
	r.Pass(ctx)
	if last := p.sends[len(p.sends)-1]; last != "acme→n2 whole=true" {
		t.Fatalf("without a base: %s", last)
	}
	if n2.volumes.data["acme"].version != n1.volumes.data["acme"].version {
		t.Fatal("the copy does not match the data")
	}
}

// A node takes a copy only from the node that runs the tenant, only if it
// is the tenant's replica, and not while the tenant moves: a node that lost
// its tenant never writes over the copy that took over.
func TestACopyIsTakenOnlyFromTheTenantsNode(t *testing.T) {
	f := newFixture(t)
	ctx := context.Background()
	n2 := f.nodes["n2"]
	stream := func() io.Reader { return strings.NewReader("|murder-replica-x|1") }
	if err := n2.receiver.Receive(ctx, "n3", "acme", true, stream()); err == nil {
		t.Fatal("a copy taken from a node that does not run the tenant")
	}
	if err := f.nodes["n3"].receiver.Receive(ctx, "n1", "acme", true, stream()); err == nil {
		t.Fatal("a copy taken by a node that is not the replica")
	}
	if err := f.nodes["n1"].receiver.Receive(ctx, "n1", "acme", true, stream()); err == nil {
		t.Fatal("a node took a copy of its own tenant")
	}
	if _, err := n2.receiver.Snapshots(ctx, "n3", "acme"); err == nil {
		t.Fatal("a node that does not run the tenant was told of its copy")
	}
	if err := n2.receiver.Receive(ctx, "n1", "acme", true, stream()); err != nil {
		t.Fatal(err)
	}
	// StartMove is the owner's to write; the others learn it from the log.
	n1 := f.nodes["n1"]
	n1.volumes.write("acme")
	r, p := f.replicator("n1")
	if _, err := n1.store.StartMove("acme", "n2", "test"); err != nil {
		t.Fatal(err)
	}
	r.Pass(ctx)
	if len(p.sends) != 0 {
		t.Fatalf("a tenant moving onto its replica's node was sent: %v", p.sends)
	}
	if _, err := n1.store.CancelMove("acme", "test"); err != nil {
		t.Fatal(err)
	}
	// Moving elsewhere, its copy keeps up: it may yet take over.
	if _, err := n1.store.StartMove("acme", "n3", "test"); err != nil {
		t.Fatal(err)
	}
	f.clock = f.clock.Add(15 * time.Second)
	r.Pass(ctx)
	if len(p.sends) != 1 {
		t.Fatalf("a tenant moving elsewhere was not sent: %v", p.sends)
	}
}

// The sweep drops copies a node no longer keeps and puts aside the data of
// tenants it no longer runs; its own data, copies it keeps and data a move
// handles stay. Nothing happens on a state that may be behind.
func TestTheSweepDropsCopiesAndPutsStaleDataAside(t *testing.T) {
	f := newFixture(t)
	ctx := context.Background()
	n2 := f.nodes["n2"]
	n2.volumes.data["acme"] = &memoryData{replica: true, snapshots: map[string]int{}}  // its copy
	n2.volumes.data["gone"] = &memoryData{replica: true, snapshots: map[string]int{}}  // of a purged tenant
	n2.volumes.data["stale"] = &memoryData{mounted: true, snapshots: map[string]int{}} // runs elsewhere now
	n2.volumes.data["moving"] = &memoryData{mounted: true, snapshots: map[string]int{}}
	f.change(t, func(st *store.SQLite) {
		for _, name := range []string{"stale", "moving"} {
			if _, err := st.Create(store.NewTenant{Name: name, App: "web", Node: "n3"}, "test"); err != nil {
				t.Fatal(err)
			}
		}
		if _, err := st.Create(store.NewTenant{Name: "mine", App: "web", Node: "n2"}, "test"); err != nil {
			t.Fatal(err)
		}
	})
	n2.volumes.data["mine"] = &memoryData{mounted: true, snapshots: map[string]int{}}
	r, _ := f.replicator("n2")
	r.CatchUp = func() error { return errors.New("no leader") }
	if err := r.Sweep(ctx); err == nil || len(n2.volumes.data) != 5 {
		t.Fatalf("a sweep on a state that may be behind: %v, %d datasets", err, len(n2.volumes.data))
	}
	r.CatchUp = nil
	r.Moving = func(tenant string) bool { return tenant == "moving" }
	if err := r.Sweep(ctx); err != nil {
		t.Fatal(err)
	}
	var left []string
	for name := range n2.volumes.data {
		left = append(left, name)
	}
	slices.Sort(left)
	if !slices.Equal(left, []string{"acme", "mine", "moving"}) || !slices.Equal(n2.volumes.aside, []string{"murder-failed-stale-elsewhere"}) {
		t.Fatalf("left %v, aside %v", left, n2.volumes.aside)
	}
}

// A node sends nothing before it has caught up with the cluster: back from
// the dead, its old state names it the node of tenants that moved away.
func TestNothingIsSentBeforeCatchingUp(t *testing.T) {
	f := newFixture(t)
	f.nodes["n1"].volumes.write("acme")
	r, p := f.replicator("n1")
	behind := errors.New("no leader")
	r.CatchUp = func() error { return behind }
	r.step(context.Background())
	if len(p.sends) != 0 {
		t.Fatalf("sent before catching up: %v", p.sends)
	}
	r.CatchUp = func() error { return nil }
	r.step(context.Background())
	if !slices.Equal(p.sends, []string{"acme→n2 whole=true"}) {
		t.Fatalf("sends = %v", p.sends)
	}
	// Once caught up, a later lag does not stop the sends; the sweep checks
	// for itself.
	r.CatchUp = func() error { return behind }
	f.nodes["n1"].volumes.write("acme")
	r.step(context.Background())
	if len(p.sends) != 2 {
		t.Fatalf("sends = %v", p.sends)
	}
}

// A replica that has not yet applied the leader's choice of it catches up
// before it refuses a copy.
func TestAReplicaCatchesUpBeforeRefusing(t *testing.T) {
	f := newFixture(t)
	f.change(t, func(st *store.SQLite) {
		if _, err := st.Create(store.NewTenant{Name: "late", App: "web", Node: "n1"}, "test"); err != nil {
			t.Fatal(err)
		}
	})
	n1, n3 := f.nodes["n1"], f.nodes["n3"]
	// The leader gives it its replica; n3 has not applied that yet.
	if _, err := n1.store.PlaceReplicas("test"); err != nil {
		t.Fatal(err)
	}
	if record, _ := n1.store.Get("late"); record.Replica != "n3" {
		t.Fatalf("late's replica: %q", record.Replica)
	}
	n1.volumes.write("late")
	r, _ := f.replicator("n1")
	if err := r.Replicate(context.Background(), "late", "n3"); err == nil {
		t.Fatal("a replica took a copy its state does not give it")
	}
	n3.receiver.CatchUp = func() error {
		_, err := n3.store.PlaceReplicas("test")
		return err
	}
	if err := r.Replicate(context.Background(), "late", "n3"); err != nil {
		t.Fatal(err)
	}
	if copy := n3.volumes.data["late"]; copy == nil || !copy.replica {
		t.Fatalf("the copy on n3: %+v", copy)
	}
}

// While an upgrade freezes changes the leader gives no replicas, and logs
// nothing for it; it does once changes thaw.
func TestPlacementWaitsForTheThaw(t *testing.T) {
	f := newFixture(t)
	n1 := f.nodes["n1"]
	f.change(t, func(st *store.SQLite) {
		if _, err := st.Create(store.NewTenant{Name: "later", App: "web", Node: "n1"}, "test"); err != nil {
			t.Fatal(err)
		}
		if err := st.Freeze("upgrade", "test"); err != nil {
			t.Fatal(err)
		}
	})
	var logged strings.Builder
	r, _ := f.replicator("n1")
	r.Log = slog.New(slog.NewTextHandler(&logged, &slog.HandlerOptions{Level: slog.LevelWarn}))
	r.Leader = func() bool { return true }
	r.step(context.Background())
	if record, _ := n1.store.Get("later"); record.Replica != "" || strings.Contains(logged.String(), "replicas") {
		t.Fatalf("placed while frozen: %q; logged %s", record.Replica, logged.String())
	}
	if err := n1.store.Thaw("test"); err != nil {
		t.Fatal(err)
	}
	r.step(context.Background())
	if record, _ := n1.store.Get("later"); record.Replica == "" {
		t.Fatal("not placed once thawed")
	}
}

// A copy moving to another node is sent there whole while the replica
// keeps getting the changes; once whole it is the replica, and the old
// node drops its copy.
func TestACopyMovesToAnotherNode(t *testing.T) {
	f := newFixture(t)
	ctx := context.Background()
	n1, n2, n3 := f.nodes["n1"], f.nodes["n2"], f.nodes["n3"]
	// While n3 was away, every copy of n1's tenants went to n2.
	f.change(t, func(st *store.SQLite) {
		if err := st.RemoveNode("n3", "test"); err != nil {
			t.Fatal(err)
		}
		for _, name := range []string{"b", "c"} {
			if _, err := st.Create(store.NewTenant{Name: name, App: "web", Node: "n1"}, "test"); err != nil {
				t.Fatal(err)
			}
		}
		if _, err := st.PlaceReplicas("test"); err != nil {
			t.Fatal(err)
		}
	})
	for _, name := range []string{"acme", "b", "c"} {
		n1.volumes.write(name)
	}
	r, p := f.replicator("n1")
	r.Pass(ctx)
	f.change(t, func(st *store.SQLite) {
		for _, name := range []string{"acme", "b", "c"} {
			if err := st.ReplicaReady(name, "n2"); err != nil {
				t.Fatal(err)
			}
		}
		if err := st.PutNode(store.Node{Name: "n3", Address: "10.0.0.3:7400"}, "test"); err != nil {
			t.Fatal(err)
		}
		if placed, err := st.PlaceReplicas("test"); err != nil || placed != 1 {
			t.Fatalf("moved %d copies, %v", placed, err)
		}
	})
	if record, _ := n1.store.Get("acme"); record.ReplicaNext != "n3" {
		t.Fatalf("acme: %+v", record)
	}
	p.sends = nil
	n1.volumes.write("acme")
	r.Pass(ctx)
	if !slices.Equal(p.sends, []string{"acme→n2 whole=false", "acme→n3 whole=true"}) {
		t.Fatalf("sends = %v", p.sends)
	}
	record, _ := n1.store.Get("acme")
	if record.Replica != "n3" || !record.ReplicaReady || record.ReplicaNext != "" {
		t.Fatalf("after the copy moved: %+v", record)
	}
	if copy := n3.volumes.data["acme"]; copy == nil || !copy.replica || copy.version != n1.volumes.data["acme"].version {
		t.Fatalf("the copy on n3: %+v", copy)
	}
	// The old node keeps its copy until it learns of the switch, then
	// drops it; the new one keeps its own.
	r2, _ := f.replicator("n2")
	if err := r2.Sweep(ctx); err != nil || n2.volumes.data["acme"] == nil {
		t.Fatalf("dropped before the switch: %v", err)
	}
	for _, st := range []*store.SQLite{n2.store, n3.store} {
		if _, err := st.SwitchReplica("acme", "n3"); err != nil {
			t.Fatal(err)
		}
	}
	if err := r2.Sweep(ctx); err != nil || n2.volumes.data["acme"] != nil {
		t.Fatalf("the old copy stays: %v", err)
	}
	r3, _ := f.replicator("n3")
	if err := r3.Sweep(ctx); err != nil || n3.volumes.data["acme"] == nil {
		t.Fatalf("the new copy went: %v", err)
	}
	// Its metrics are the replica's: synced at the last send.
	if synced := r.Synced("acme"); synced.IsZero() {
		t.Fatal("no sync time")
	}
}

// A copy that never gets to the node it moves to stops moving after a few
// tries, so the other copies may move.
func TestAStuckCopyMoveIsGivenUp(t *testing.T) {
	f := newFixture(t)
	ctx := context.Background()
	n1, n3 := f.nodes["n1"], f.nodes["n3"]
	f.change(t, func(st *store.SQLite) {
		if err := st.RemoveNode("n3", "test"); err != nil {
			t.Fatal(err)
		}
		for _, name := range []string{"b", "c"} {
			if _, err := st.Create(store.NewTenant{Name: name, App: "web", Node: "n1"}, "test"); err != nil {
				t.Fatal(err)
			}
		}
		if _, err := st.PlaceReplicas("test"); err != nil {
			t.Fatal(err)
		}
	})
	for _, name := range []string{"acme", "b", "c"} {
		n1.volumes.write(name)
	}
	r, _ := f.replicator("n1")
	r.Pass(ctx)
	f.change(t, func(st *store.SQLite) {
		for _, name := range []string{"acme", "b", "c"} {
			if err := st.ReplicaReady(name, "n2"); err != nil {
				t.Fatal(err)
			}
		}
		if err := st.PutNode(store.Node{Name: "n3", Address: "10.0.0.3:7400"}, "test"); err != nil {
			t.Fatal(err)
		}
		if _, err := st.PlaceReplicas("test"); err != nil {
			t.Fatal(err)
		}
	})
	// n3 refuses every copy.
	n3.volumes.failRcv = errors.New("pool full")
	for i := range moveGiveUp {
		if record, _ := n1.store.Get("acme"); record.ReplicaNext != "n3" {
			t.Fatalf("given up after %d tries: %+v", i, record)
		}
		r.Pass(ctx)
	}
	record, _ := n1.store.Get("acme")
	if record.ReplicaNext != "" || record.Replica != "n2" || !record.ReplicaReady {
		t.Fatalf("after %d failed sends: %+v", moveGiveUp, record)
	}
	if events, _ := n1.store.Events(store.EventQuery{Tenant: "acme"}); len(events) == 0 || !strings.Contains(events[0].Detail, "no longer moves to n3") {
		t.Fatalf("events: %+v", events)
	}
	// No copy moves to n3 again for a while: none would get there.
	if placed, err := n1.store.PlaceReplicas("test"); err != nil || placed != 0 {
		t.Fatalf("moved %d copies to the node that refused one, %v", placed, err)
	}
}
