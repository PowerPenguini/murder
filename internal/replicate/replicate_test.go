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
	// failSend fails this node's sends.
	failSend error
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
	if v.failSend != nil {
		return v.failSend
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
	r.step(context.Background()).Wait()
	if len(p.sends) != 0 {
		t.Fatalf("sent before catching up: %v", p.sends)
	}
	r.CatchUp = func() error { return nil }
	r.step(context.Background()).Wait()
	if !slices.Equal(p.sends, []string{"acme→n2 whole=true"}) {
		t.Fatalf("sends = %v", p.sends)
	}
	// Once caught up, a later lag does not stop the sends; the sweep checks
	// for itself.
	r.CatchUp = func() error { return behind }
	f.nodes["n1"].volumes.write("acme")
	r.step(context.Background()).Wait()
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
	r.step(context.Background()).Wait()
	if record, _ := n1.store.Get("later"); record.Replica != "" || strings.Contains(logged.String(), "replicas") {
		t.Fatalf("placed while frozen: %q; logged %s", record.Replica, logged.String())
	}
	if err := n1.store.Thaw("test"); err != nil {
		t.Fatal(err)
	}
	r.step(context.Background()).Wait()
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
			if err := markReady(st, name, "n2"); err != nil {
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
	f.clock = f.clock.Add(15 * time.Second)
	n1.volumes.write("acme")
	r.Pass(ctx)
	// The two go side by side: the copy moving holds up no change.
	slices.Sort(p.sends)
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
		if _, err := st.SwitchReplica("acme", "n3", "n1"); err != nil {
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
			if err := markReady(st, name, "n2"); err != nil {
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
	for i := range copyGiveUp {
		if record, _ := n1.store.Get("acme"); record.ReplicaNext != "n3" {
			t.Fatalf("given up after %d tries: %+v", i, record)
		}
		r.Pass(ctx)
	}
	record, _ := n1.store.Get("acme")
	if record.ReplicaNext != "" || record.Replica != "n2" || !record.ReplicaReady {
		t.Fatalf("after %d failed sends: %+v", copyGiveUp, record)
	}
	if events, _ := n1.store.Events(store.EventQuery{Tenant: "acme"}); len(events) == 0 || !strings.Contains(events[0].Detail, "no copy is kept on n3") {
		t.Fatalf("events: %+v", events)
	}
	// No copy moves to n3 again for a while: none would get there.
	if placed, err := n1.store.PlaceReplicas("test"); err != nil || placed != 0 {
		t.Fatalf("moved %d copies to the node that refused one, %v", placed, err)
	}
}

// A first copy that never gets to the replica is given up, after a few
// tries, and placement gives the tenant another node; a send failing on
// this node's side counts for nothing against the other.
func TestAReplicaThatNeverGetsItsCopyIsReplaced(t *testing.T) {
	f := newFixture(t)
	ctx := context.Background()
	n1, n2 := f.nodes["n1"], f.nodes["n2"]
	n1.volumes.write("acme")
	r, _ := f.replicator("n1")
	// This node's own failures: n2 is not to blame.
	n1.volumes.failSend = errors.New("pool busy")
	for range copyGiveUp + 1 {
		r.Pass(ctx)
	}
	if record, _ := n1.store.Get("acme"); record.Replica != "n2" {
		t.Fatalf("a replica given up for this node's failures: %+v", record)
	}
	n1.volumes.failSend = nil
	n2.volumes.failRcv = errors.New("pool full")
	for range copyGiveUp {
		r.Pass(ctx)
	}
	record, _ := n1.store.Get("acme")
	if record.Replica != "" {
		t.Fatalf("after %d refused copies: %+v", copyGiveUp, record)
	}
	if _, err := n1.store.PlaceReplicas("test"); err != nil {
		t.Fatal(err)
	}
	if record, _ := n1.store.Get("acme"); record.Replica != "n3" {
		t.Fatalf("placed on %q, not on the other node", record.Replica)
	}
	// Back from a failure, a node takes copies again at once.
	if _, err := n1.store.MarkFailed("n2", "test"); err != nil {
		t.Fatal(err)
	}
	if err := n1.store.NodeBack("n2", "test"); err != nil {
		t.Fatal(err)
	}
	for _, node := range n1.store.Nodes() {
		if node.Name == "n2" && !node.CopiesRefusedUntil.IsZero() {
			t.Fatalf("n2 still refused: %+v", node)
		}
	}
}

// heldPeer holds sends to one node until released, as a whole copy that
// takes hours would; entered, buffered, learns of the first.
type heldPeer struct {
	*peer
	node     string
	entered  chan struct{}
	released chan struct{}
}

func (p *heldPeer) Send(ctx context.Context, to, tenant string, whole bool, data io.Reader) error {
	if to == p.node {
		select {
		case p.entered <- struct{}{}:
		default:
		}
		select {
		case <-p.released:
		case <-ctx.Done():
			return ctx.Err()
		}
	}
	return p.peer.Send(ctx, to, tenant, whole, data)
}

// A copy moving to another node, for hours, holds up no other send: the
// other tenants' replicas and the tenant's own keep getting their changes
// every pass, and the snapshot the copy is sent from stays for the
// changes that follow it.
func TestACopyMovingHoldsUpNoOtherSend(t *testing.T) {
	f := newFixture(t)
	ctx := context.Background()
	n1, n2, n3 := f.nodes["n1"], f.nodes["n2"], f.nodes["n3"]
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
			if err := markReady(st, name, "n2"); err != nil {
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
	if record, _ := n1.store.Get("acme"); record.ReplicaNext != "n3" {
		t.Fatalf("acme: %+v", record)
	}
	held := &heldPeer{peer: p, node: "n3", entered: make(chan struct{}, 1), released: make(chan struct{})}
	r.Peer = held
	f.clock = f.clock.Add(15 * time.Second)
	moving := r.start(ctx)
	<-held.entered
	// Only the copy moving is left under way.
	for deadline := time.Now().Add(10 * time.Second); ; time.Sleep(time.Millisecond) {
		r.mu.Lock()
		left := len(r.busy)
		r.mu.Unlock()
		if left == 1 {
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("%d sends still under way", left)
		}
	}
	for range 5 {
		f.clock = f.clock.Add(15 * time.Second)
		for _, name := range []string{"acme", "b", "c"} {
			n1.volumes.write(name)
		}
		r.start(ctx).Wait()
		for _, name := range []string{"acme", "b", "c"} {
			if got, want := n2.volumes.data[name].version, n1.volumes.data[name].version; got != want {
				t.Fatalf("%s's replica is at version %d, its data at %d, while acme's copy moves", name, got, want)
			}
		}
	}
	if kept := len(n1.volumes.data["acme"].snapshots); kept <= r.keep() {
		t.Fatalf("acme's snapshots pruned to %d while its copy moved", kept)
	}
	close(held.released)
	moving.Wait()
	record, _ := n1.store.Get("acme")
	if record.Replica != "n3" || !record.ReplicaReady {
		t.Fatalf("after the copy moved: %+v", record)
	}
	// The new replica's first changes go on the snapshot it was sent.
	p.sends = nil
	f.clock = f.clock.Add(15 * time.Second)
	n1.volumes.write("acme")
	r.Pass(ctx)
	if !slices.Contains(p.sends, "acme→n3 whole=false") {
		t.Fatalf("sends = %v", p.sends)
	}
	if got, want := n3.volumes.data["acme"].version, n1.volumes.data["acme"].version; got != want {
		t.Fatalf("the new replica is at version %d, the data at %d", got, want)
	}
	if kept := len(n1.volumes.data["acme"].snapshots); kept > r.keep() {
		t.Fatalf("%d snapshots kept once the copy moved", kept)
	}
}

// stalledPeer takes no byte of a stream, nor answers, until the sender
// gives up; its node still answers otherwise.
type stalledPeer struct{ *peer }

func (p stalledPeer) Send(ctx context.Context, to, tenant string, whole bool, data io.Reader) error {
	<-ctx.Done()
	return ctx.Err()
}

// A send whose stream stops moving fails, rather than holding the copy up
// for ever, and counts against the node that stalled it.
func TestAStalledSendFails(t *testing.T) {
	f := newFixture(t)
	n1 := f.nodes["n1"]
	n1.volumes.write("acme")
	r, p := f.replicator("n1")
	r.Peer = stalledPeer{p}
	r.Stall = 20 * time.Millisecond
	for range copyGiveUp {
		done := make(chan struct{})
		go func() {
			r.Pass(context.Background())
			close(done)
		}()
		select {
		case <-done:
		case <-time.After(10 * time.Second):
			t.Fatal("a stalled send is still waited for")
		}
	}
	r.mu.Lock()
	why := r.failing["acme"]
	r.mu.Unlock()
	if !strings.Contains(why, "moved nothing") {
		t.Fatalf("failing: %q", why)
	}
	// The first copy never got there: the replica is given up.
	if record, _ := n1.store.Get("acme"); record.Replica != "" {
		t.Fatalf("after %d stalled sends: %+v", copyGiveUp, record)
	}
}

// relocationFixture gives acme, b and c their whole copies on n2, then has
// placement move acme's copy to n3, which joined later.
func relocationFixture(t *testing.T) (*fixture, *Replicator, *peer) {
	t.Helper()
	f := newFixture(t)
	n1 := f.nodes["n1"]
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
	r.Pass(context.Background())
	f.change(t, func(st *store.SQLite) {
		for _, name := range []string{"acme", "b", "c"} {
			if err := markReady(st, name, "n2"); err != nil {
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
	if record, _ := n1.store.Get("acme"); record.ReplicaNext != "n3" {
		t.Fatalf("acme: %+v", record)
	}
	p.sends = nil
	return f, r, p
}

// A copy that took long to move gets the changes the replica got meanwhile
// before it takes the replica's place: a failover right after the switch
// loses none of them.
func TestAMovedCopyCatchesUpBeforeItIsTheReplica(t *testing.T) {
	f, r, p := relocationFixture(t)
	ctx := context.Background()
	n1, n2, n3 := f.nodes["n1"], f.nodes["n2"], f.nodes["n3"]
	held := &heldPeer{peer: p, node: "n3", entered: make(chan struct{}, 1), released: make(chan struct{})}
	r.Peer = held
	f.clock = f.clock.Add(15 * time.Second)
	n1.volumes.write("acme")
	record, _ := n1.store.Get("acme")
	moved := make(chan struct{})
	go func() {
		r.relocate(ctx, record)
		close(moved)
	}()
	<-held.entered
	// Hours of changes reach the replica while the whole copy goes.
	for range 5 {
		f.clock = f.clock.Add(15 * time.Second)
		n1.volumes.write("acme")
		r.update(ctx, record)
	}
	if got, want := n2.volumes.data["acme"].version, n1.volumes.data["acme"].version; got != want {
		t.Fatalf("the replica is at version %d, the data at %d", got, want)
	}
	close(held.released)
	<-moved
	record, _ = n1.store.Get("acme")
	if record.Replica != "n3" || !record.ReplicaReady {
		t.Fatalf("after the copy moved: %+v", record)
	}
	if got, want := n3.volumes.data["acme"].version, n2.volumes.data["acme"].version; got != want {
		t.Fatalf("the new replica is at version %d, the old one was at %d", got, want)
	}
	if !slices.Contains(p.sends, "acme→n3 whole=false") {
		t.Fatalf("no changes followed the whole copy: %v", p.sends)
	}
}

// fallingBehind makes every send to a node take long enough for the
// replica to get newer changes meanwhile.
type fallingBehind struct {
	*peer
	node string
	r    *Replicator
	f    *fixture
}

func (p *fallingBehind) Send(ctx context.Context, to, tenant string, whole bool, data io.Reader) error {
	err := p.peer.Send(ctx, to, tenant, whole, data)
	if to == p.node {
		p.f.clock = p.f.clock.Add(time.Minute)
		p.f.nodes["n1"].volumes.write(tenant)
		p.r.markSynced(tenant, p.f.clock)
	}
	return err
}

// A moving copy whose sends never catch up with the replica does not take
// its place; it does once a send of its changes is quick.
func TestAMovingCopyBehindTheReplicaDoesNotSwitch(t *testing.T) {
	f, r, p := relocationFixture(t)
	ctx := context.Background()
	n1, n3 := f.nodes["n1"], f.nodes["n3"]
	r.Peer = &fallingBehind{peer: p, node: "n3", r: r, f: f}
	f.clock = f.clock.Add(15 * time.Second)
	record, _ := n1.store.Get("acme")
	r.relocate(ctx, record)
	if record, _ := n1.store.Get("acme"); record.Replica != "n2" || record.ReplicaNext != "n3" {
		t.Fatalf("a copy behind its replica took its place: %+v", record)
	}
	if len(p.sends) < 2 {
		t.Fatalf("sends = %v", p.sends)
	}
	r.Peer = p
	f.clock = f.clock.Add(time.Second)
	r.relocate(ctx, record)
	record, _ = n1.store.Get("acme")
	if record.Replica != "n3" || record.ReplicaNext != "" {
		t.Fatalf("a copy caught up did not take over: %+v", record)
	}
	if got, want := n3.volumes.data["acme"].version, n1.volumes.data["acme"].version; got != want {
		t.Fatalf("the new replica is at version %d, the data at %d", got, want)
	}
}

// A send to the replica that waited for its slot while the copy moved
// sends nothing to the old node, and prunes nothing the new one needs.
func TestASendToAReplicaThatMovedIsDropped(t *testing.T) {
	f, r, p := relocationFixture(t)
	ctx := context.Background()
	n1, n3 := f.nodes["n1"], f.nodes["n3"]
	stale, _ := n1.store.Get("acme")
	f.clock = f.clock.Add(15 * time.Second)
	n1.volumes.write("acme")
	r.relocate(ctx, stale)
	if record, _ := n1.store.Get("acme"); record.Replica != "n3" {
		t.Fatalf("after the copy moved: %+v", record)
	}
	theirs, _ := n3.receiver.Snapshots(ctx, "n1", "acme")
	base := theirs[len(theirs)-1]
	p.sends = nil
	for range 5 {
		f.clock = f.clock.Add(15 * time.Second)
		n1.volumes.write("acme")
		r.update(ctx, stale)
	}
	if len(p.sends) != 0 {
		t.Fatalf("sent to the node that is no longer the replica: %v", p.sends)
	}
	if _, ok := n1.volumes.data["acme"].snapshots[base]; !ok {
		t.Fatalf("the new replica's base %s was pruned", base)
	}
}

// While a copy moves, as the state says, the replica's sends prune
// nothing, even between the moving copy's sends: the copy's base stays.
func TestNothingIsPrunedWhileACopyMoves(t *testing.T) {
	f, r, _ := relocationFixture(t)
	ctx := context.Background()
	n1, n3 := f.nodes["n1"], f.nodes["n3"]
	f.clock = f.clock.Add(15 * time.Second)
	n1.volumes.write("acme")
	if _, err := r.replicate(ctx, "acme", "n3", false); err != nil {
		t.Fatal(err)
	}
	theirs, _ := n3.receiver.Snapshots(ctx, "n1", "acme")
	base := theirs[len(theirs)-1]
	record, _ := n1.store.Get("acme")
	for range 5 {
		f.clock = f.clock.Add(15 * time.Second)
		n1.volumes.write("acme")
		r.update(ctx, record)
	}
	if _, ok := n1.volumes.data["acme"].snapshots[base]; !ok {
		t.Fatalf("the moving copy's base %s was pruned", base)
	}
}

// markReady reports name's copy on replica whole, as its node would.
func markReady(st interface {
	Get(string) (store.Record, error)
	ReplicaReady(string, string, string) error
}, name, replica string) error {
	record, err := st.Get(name)
	if err != nil {
		return err
	}
	return st.ReplicaReady(name, replica, record.Node)
}

// blockingPeer is peer whose sends wait, once begun, for unblock.
type blockingPeer struct {
	*peer
	once    sync.Once
	begun   chan struct{}
	unblock chan struct{}
}

func newBlockingPeer(p *peer) *blockingPeer {
	return &blockingPeer{peer: p, begun: make(chan struct{}), unblock: make(chan struct{})}
}

func (b *blockingPeer) Send(ctx context.Context, to, tenant string, whole bool, data io.Reader) error {
	b.once.Do(func() { close(b.begun) })
	<-b.unblock
	return b.peer.Send(ctx, to, tenant, whole, data)
}

// A change of the tenant while its copy is sent, to its containers here,
// leaves its data as it is: the send still reports the copy whole.
func TestASendReportsTheCopyWholeThroughChangesOfTheTenant(t *testing.T) {
	f := newFixture(t)
	n1 := f.nodes["n1"]
	n1.volumes.write("acme")
	r, p := f.replicator("n1")
	blocking := newBlockingPeer(p)
	r.Peer = blocking
	ctx := context.Background()
	done := make(chan struct{})
	go func() { r.Pass(ctx); close(done) }()
	<-blocking.begun
	f.change(t, func(st *store.SQLite) {
		if _, err := st.Roll("acme", "test"); err != nil {
			t.Fatal(err)
		}
	})
	close(blocking.unblock)
	<-done
	if record, _ := n1.store.Get("acme"); !record.ReplicaReady {
		t.Fatal("a send was not reported for a change that left the data")
	}
}

// A replacement of the data holds the tenant's sends: it waits for the one
// under way, and its report, before the copy is marked stale, and none
// runs until it ends; the next send then makes the copy whole again.
func TestASendBegunBeforeAReplacementReportsNothingAfter(t *testing.T) {
	f := newFixture(t)
	n1 := f.nodes["n1"]
	n1.volumes.write("acme")
	r, p := f.replicator("n1")
	blocking := newBlockingPeer(p)
	r.Peer = blocking
	ctx := context.Background()
	done := make(chan struct{})
	go func() { r.Pass(ctx); close(done) }()
	<-blocking.begun
	// A caller that gives up waiting holds nothing.
	gaveUp, cancel := context.WithCancel(ctx)
	cancel()
	if _, err := r.Hold(gaveUp, "acme"); !errors.Is(err, context.Canceled) {
		t.Fatalf("a hold given up: %v", err)
	}
	held := make(chan func(), 1)
	go func() {
		release, err := r.Hold(ctx, "acme")
		if err != nil {
			t.Error(err)
			release = func() {}
		}
		held <- release
	}()
	select {
	case <-held:
		t.Fatal("held while a send was under way")
	case <-time.After(50 * time.Millisecond):
	}
	close(blocking.unblock)
	<-done
	release := <-held
	// The replacement: the copy no longer matches, and the data is new,
	// with no snapshot in common with the copy.
	if err := n1.store.ReplicaStale("acme", "its data is replaced", "test"); err != nil {
		t.Fatal(err)
	}
	n1.volumes.mu.Lock()
	n1.volumes.data["acme"] = &memoryData{version: 100, mounted: true, snapshots: map[string]int{}}
	n1.volumes.mu.Unlock()
	if record, _ := n1.store.Get("acme"); record.ReplicaReady {
		t.Fatal("the copy of the data replaced is whole")
	}
	p.sends = nil
	r.Pass(ctx)
	if err := r.Replicate(ctx, "acme", "n2"); !errors.Is(err, ErrHeld) {
		t.Fatalf("a send while held: %v", err)
	}
	if record, _ := n1.store.Get("acme"); len(p.sends) != 0 || record.ReplicaReady {
		t.Fatalf("held: sends %v, ready %v", p.sends, record.ReplicaReady)
	}
	release()
	release() // twice is once
	r.Pass(ctx)
	if !slices.Equal(p.sends, []string{"acme→n2 whole=true"}) {
		t.Fatalf("sends after the replacement: %v", p.sends)
	}
	if record, _ := n1.store.Get("acme"); !record.ReplicaReady || f.nodes["n2"].volumes.data["acme"].version != 100 {
		t.Fatal("the copy of the new data is not whole")
	}
}

// A hold covers a copy moving to another node too: none is sent, nor
// switched, while the tenant is held, and a hold taken while one is sent
// waits for it, and its switch, to end.
func TestAHoldCoversACopyMoving(t *testing.T) {
	f := newFixture(t)
	ctx := context.Background()
	n1 := f.nodes["n1"]
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
			if err := markReady(st, name, "n2"); err != nil {
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
	if record, _ := n1.store.Get("acme"); record.ReplicaNext != "n3" {
		t.Fatalf("acme: %+v", record)
	}
	// Held first: the copy neither moves nor switches.
	release, err := r.Hold(ctx, "acme")
	if err != nil {
		t.Fatal(err)
	}
	p.sends = nil
	f.clock = f.clock.Add(15 * time.Second)
	n1.volumes.write("acme")
	r.Pass(ctx)
	for _, send := range p.sends {
		if strings.HasPrefix(send, "acme") {
			t.Fatalf("held: sends %v", p.sends)
		}
	}
	if record, _ := n1.store.Get("acme"); record.Replica != "n2" || record.ReplicaNext != "n3" {
		t.Fatalf("held: %+v", record)
	}
	release()
	// Held while the copy is sent: the hold waits for the send and its
	// switch.
	held := &heldPeer{peer: p, node: "n3", entered: make(chan struct{}, 1), released: make(chan struct{})}
	r.Peer = held
	moving := r.start(ctx)
	<-held.entered
	holding := make(chan func(), 1)
	go func() {
		release, err := r.Hold(ctx, "acme")
		if err != nil {
			t.Error(err)
			release = func() {}
		}
		holding <- release
	}()
	select {
	case <-holding:
		t.Fatal("held while a copy moving was sent")
	case <-time.After(50 * time.Millisecond):
	}
	close(held.released)
	release = <-holding
	moving.Wait()
	if record, _ := n1.store.Get("acme"); record.Replica != "n3" {
		t.Fatalf("the copy sent before the hold did not switch: %+v", record)
	}
	release()
}
