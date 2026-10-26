package reconcile

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net"
	"net/http"
	"net/netip"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/PowerPenguini/corral/internal/runtime"
	"github.com/PowerPenguini/corral/internal/secrets"
	"github.com/PowerPenguini/corral/internal/spec"
	"github.com/PowerPenguini/corral/internal/store"
	"github.com/PowerPenguini/corral/internal/volume"
)

const (
	imageA = "sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"
	imageB = "sha256:bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb"
)

// fakeRuntime is an in-memory container engine.
type fakeRuntime struct {
	mu         sync.Mutex
	images     map[string]bool
	containers map[string]*fakeContainer
	networks   map[string]map[string]string // name → labels
	pulled     []string
	next       int
	events     []string
	failCreate error
	failPull   error
	failRemove error
	// exitBeforeInspect makes the next Pid call see the container gone, as
	// if it exited between List and the inspection.
	exitBeforeInspect bool
}

type fakeContainer struct {
	runtime.Container
	spec runtime.ContainerSpec
}

func newFakeRuntime() *fakeRuntime {
	return &fakeRuntime{images: map[string]bool{imageA: true}, containers: map[string]*fakeContainer{}, networks: map[string]map[string]string{}}
}

func (f *fakeRuntime) record(event string) { f.events = append(f.events, event) }

func (f *fakeRuntime) HasImage(_ context.Context, ref string) (bool, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.images[ref], nil
}

func (f *fakeRuntime) PullImage(_ context.Context, ref string) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.pulled = append(f.pulled, ref)
	if f.failPull != nil {
		return f.failPull
	}
	f.images[ref] = true
	return nil
}

func (f *fakeRuntime) EnsureNetwork(_ context.Context, name, _ string, labels map[string]string) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.networks[name] = labels
	return nil
}

func (f *fakeRuntime) RemoveNetwork(_ context.Context, name string) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	if _, ok := f.networks[name]; ok {
		f.record("remove-network " + name)
	}
	delete(f.networks, name)
	return nil
}

func (f *fakeRuntime) ListNetworks(context.Context) ([]runtime.Network, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	var out []runtime.Network
	for name, labels := range f.networks {
		out = append(out, runtime.Network{Name: name, Labels: labels})
	}
	return out, nil
}

func (f *fakeRuntime) List(context.Context) ([]runtime.Container, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	var out []runtime.Container
	for _, container := range f.containers {
		out = append(out, container.Container)
	}
	slices.SortFunc(out, func(a, b runtime.Container) int { return strings.Compare(a.ID, b.ID) })
	return out, nil
}

func (f *fakeRuntime) Create(_ context.Context, spec runtime.ContainerSpec) (string, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.failCreate != nil {
		return "", f.failCreate
	}
	if !f.images[spec.Image] {
		return "", fmt.Errorf("image %s not present", spec.Image)
	}
	f.next++
	id := fmt.Sprintf("c%d", f.next)
	f.containers[id] = &fakeContainer{Container: runtime.Container{ID: id, Name: spec.Name, State: "created", Labels: spec.Labels}, spec: spec}
	f.record("create " + id)
	return id, nil
}

func (f *fakeRuntime) Start(_ context.Context, id string) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.containers[id].Running, f.containers[id].State = true, "running"
	f.record("start " + id)
	return nil
}

func (f *fakeRuntime) Stop(_ context.Context, id string, _ time.Duration) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.containers[id].Running, f.containers[id].State = false, "exited"
	f.record("stop " + id)
	return nil
}

func (f *fakeRuntime) Remove(_ context.Context, id string) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.failRemove != nil {
		return f.failRemove
	}
	delete(f.containers, id)
	f.record("remove " + id)
	return nil
}

func (f *fakeRuntime) Logs(context.Context, string, int) ([]byte, error) { return nil, nil }

func (f *fakeRuntime) Resolve(_ context.Context, ref string) (string, string, error) {
	return ref, "", nil
}

func (f *fakeRuntime) Pid(_ context.Context, id string) (int, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	if container, ok := f.containers[id]; !ok || !container.Running || f.exitBeforeInspect {
		f.exitBeforeInspect = false
		return 0, fmt.Errorf("container %s: %w", id, runtime.ErrNotRunning)
	}
	return 4242, nil
}

func (f *fakeRuntime) only(t *testing.T) *fakeContainer {
	t.Helper()
	f.mu.Lock()
	defer f.mu.Unlock()
	if len(f.containers) != 1 {
		t.Fatalf("want exactly one container, have %d", len(f.containers))
	}
	for _, container := range f.containers {
		return container
	}
	return nil
}

func (f *fakeRuntime) count() int {
	f.mu.Lock()
	defer f.mu.Unlock()
	return len(f.containers)
}

func (f *fakeRuntime) takeEvents() []string {
	f.mu.Lock()
	defer f.mu.Unlock()
	events := f.events
	f.events = nil
	return events
}

// fakeVolumes records snapshots and keeps directories below a temp root.
type fakeVolumes struct {
	volume.Dir
	snapshots    []string
	removed      []string
	failSnapshot error
	mountNext    bool
	notServing   bool
}

func (f *fakeVolumes) LateMounts() bool { return true }

func (f *fakeVolumes) Serving(context.Context, string, int) (bool, error) {
	serving := !f.notServing
	f.notServing = false
	return serving, nil
}

func (f *fakeVolumes) Ensure(ctx context.Context, tenant string, uid, gid int) (string, bool, error) {
	path, _, err := f.Dir.Ensure(ctx, tenant, uid, gid)
	mounted := f.mountNext
	f.mountNext = false
	return path, mounted, err
}

func (f *fakeVolumes) Snapshot(_ context.Context, tenant, reason string) (string, error) {
	if f.failSnapshot != nil {
		return "", f.failSnapshot
	}
	f.snapshots = append(f.snapshots, tenant+"@"+reason)
	return tenant + "@" + reason, nil
}

func (f *fakeVolumes) Remove(ctx context.Context, tenant string) error {
	f.removed = append(f.removed, tenant)
	return f.Dir.Remove(ctx, tenant)
}

type fixture struct {
	store      *store.SQLite
	storePath  string
	runtime    *fakeRuntime
	volumes    *fakeVolumes
	key        *secrets.Key
	reconciler *Reconciler
}

// newFixture serves a health endpoint on the single host port the store can
// assign, so the reconciler's health check reaches it.
func newFixture(t *testing.T) *fixture {
	t.Helper()
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	port := listener.Addr().(*net.TCPAddr).Port
	server := &http.Server{Handler: http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/health" {
			w.WriteHeader(http.StatusNotFound)
			return
		}
		_, _ = io.WriteString(w, "ok")
	})}
	go server.Serve(listener)
	t.Cleanup(func() { _ = server.Close() })
	keyPath := filepath.Join(t.TempDir(), "master.key")
	if err := secrets.CreateKey(keyPath); err != nil {
		t.Fatal(err)
	}
	key, err := secrets.LoadKey(keyPath)
	if err != nil {
		t.Fatal(err)
	}
	statePath := filepath.Join(t.TempDir(), "corral.db")
	if err := store.Init(statePath, "test-node"); err != nil {
		t.Fatal(err)
	}
	// Every secret gets the same known value, so tests can look for it.
	sealer := func(tenant, name string, _ spec.Secret) (string, error) {
		return key.Seal(secrets.Context(tenant, name), "the-secret")
	}
	st, err := store.Open(statePath, store.Options{PortFrom: port, PortTo: port, NetworkPool: netip.MustParsePrefix("10.240.0.0/16"), Sealer: sealer})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = st.Close() })
	rt := newFakeRuntime()
	volumes := &fakeVolumes{Dir: volume.Dir{Root: filepath.Join(t.TempDir(), "tenants")}}
	log := slog.New(slog.NewTextHandler(io.Discard, nil))
	reconciler := New(st, rt, volumes, key, Config{BaseDomain: "example.test", DefaultLimits: spec.Limits{MemoryMB: 256, CPUs: 0.5, PIDs: 128}}, log)
	return &fixture{store: st, storePath: statePath, runtime: rt, volumes: volumes, key: key, reconciler: reconciler}
}

// create adds a tenant running an app of its own name, so tests change one
// tenant by changing its app.
func (f *fixture) create(t *testing.T, name string) store.Record {
	t.Helper()
	app := spec.Tenant{
		Image: imageA, HealthPath: "/health",
		User:    fmt.Sprintf("%d:%d", os.Getuid(), os.Getgid()),
		Env:     map[string]string{"PUBLIC_URL": "{{url}}", "TRUSTED_PROXIES": "{{network}}"},
		Secrets: spec.Secrets{"SESSION_SECRET": {}},
	}
	if _, err := f.store.PutApp(store.App{Name: name, Spec: app}, "test"); err != nil {
		t.Fatal(err)
	}
	record, err := f.store.Create(store.NewTenant{Name: name, App: name}, "test")
	if err != nil {
		t.Fatal(err)
	}
	return record
}

func (f *fixture) updateApp(t *testing.T, name string, change func(*spec.Tenant)) {
	t.Helper()
	app, err := f.store.App(name)
	if err != nil {
		t.Fatal(err)
	}
	change(&app.Spec)
	if _, err := f.store.PutApp(app, "test"); err != nil {
		t.Fatal(err)
	}
}

func (f *fixture) suspend(t *testing.T, name string, suspended bool) {
	t.Helper()
	if _, err := f.store.SetSuspended(name, suspended, "test"); err != nil {
		t.Fatal(err)
	}
}

func (f *fixture) reconcile(t *testing.T) {
	t.Helper()
	if err := f.reconciler.Reconcile(context.Background()); err != nil {
		t.Fatal(err)
	}
}

func TestNewTenantGetsAHealthyContainer(t *testing.T) {
	f := newFixture(t)
	record := f.create(t, "acme")
	f.reconcile(t)
	container := f.runtime.only(t)
	if !container.Running || container.Name != "corral-acme" || container.spec.Network != "corral-acme" {
		t.Fatalf("container = %+v", container)
	}
	env := strings.Join(container.spec.Env, "\n")
	for _, want := range []string{"PUBLIC_URL=https://acme.example.test", "TRUSTED_PROXIES=" + record.Subnet, "SESSION_SECRET=the-secret"} {
		if !strings.Contains(env, want) {
			t.Errorf("env lacks %q:\n%s", want, env)
		}
	}
	if container.spec.MemoryBytes != 256<<20 || container.spec.NanoCPUs != 5e8 || container.spec.PIDs != 128 {
		t.Errorf("default limits not applied: %+v", container.spec)
	}
	if container.spec.Mounts[0].Target != "/data" || container.spec.Mounts[0].Source != filepath.Join(f.volumes.Root, "acme") {
		t.Errorf("mount = %+v", container.spec.Mounts)
	}
	status, ok := f.reconciler.Status("acme")
	if !ok || status.Phase != PhaseRunning || !status.Healthy || status.Generation != 1 {
		t.Fatalf("status = %+v", status)
	}
	if got, _ := f.store.Get("acme"); got.DataImage != imageA {
		t.Fatalf("data image after the first start = %q", got.DataImage)
	}
	if len(f.volumes.snapshots) != 0 {
		t.Fatalf("the first start took a snapshot: %v", f.volumes.snapshots)
	}
}

// An agent restart must adopt running containers, not restart them.
func TestUnchangedTenantIsLeftAlone(t *testing.T) {
	f := newFixture(t)
	f.create(t, "acme")
	f.reconcile(t)
	f.runtime.takeEvents()
	restarted := New(f.store, f.runtime, f.volumes, f.key, f.reconciler.config, f.reconciler.log)
	if err := restarted.Reconcile(context.Background()); err != nil {
		t.Fatal(err)
	}
	if events := f.runtime.takeEvents(); len(events) != 0 {
		t.Fatalf("a reconcile with nothing changed touched containers: %v", events)
	}
}

func TestImageChangeStopsSnapshotsAndReplaces(t *testing.T) {
	f := newFixture(t)
	f.create(t, "acme")
	f.reconcile(t)
	old := f.runtime.only(t).ID
	f.updateApp(t, "acme", func(app *spec.Tenant) { app.Image = imageB })
	f.runtime.takeEvents()
	f.reconcile(t)
	replacement := f.runtime.only(t)
	if replacement.ID == old || replacement.spec.Image != imageB || !replacement.Running {
		t.Fatalf("container after image change = %+v", replacement)
	}
	if !slices.Equal(f.volumes.snapshots, []string{"acme@image-change"}) || !slices.Equal(f.runtime.pulled, []string{imageB}) {
		t.Fatalf("snapshots = %v, pulled = %v", f.volumes.snapshots, f.runtime.pulled)
	}
	events := strings.Join(f.runtime.takeEvents(), ",")
	if events != fmt.Sprintf("stop %s,remove %s,create %s,start %s", old, old, replacement.ID, replacement.ID) {
		t.Fatalf("events = %v", events)
	}
	if got, _ := f.store.Get("acme"); got.DataImage != imageB {
		t.Fatalf("data image = %q", got.DataImage)
	}
	// An env change replaces the container without a snapshot.
	f.updateApp(t, "acme", func(app *spec.Tenant) { app.Env["EXTRA"] = "1" })
	f.reconcile(t)
	if len(f.volumes.snapshots) != 1 {
		t.Fatalf("an env change took a snapshot: %v", f.volumes.snapshots)
	}
}

// A new image that cannot be fetched must not take the running tenant down.
func TestFailedPullKeepsTheOldContainer(t *testing.T) {
	f := newFixture(t)
	f.create(t, "acme")
	f.reconcile(t)
	old := f.runtime.only(t).ID
	f.runtime.failPull = errors.New("manifest unknown")
	f.updateApp(t, "acme", func(app *spec.Tenant) { app.Image = imageB })
	f.runtime.takeEvents()
	for range 3 {
		if err := f.reconciler.Reconcile(context.Background()); err == nil || !strings.Contains(err.Error(), "manifest unknown") {
			t.Fatalf("Reconcile = %v", err)
		}
	}
	if container := f.runtime.only(t); container.ID != old || !container.Running {
		t.Fatalf("the old container did not keep serving: %+v", container)
	}
	if events := f.runtime.takeEvents(); len(events) != 0 {
		t.Fatalf("a failed pull touched containers: %v", events)
	}
	if len(f.volumes.snapshots) != 0 {
		t.Fatalf("snapshots taken for a change that never happened: %v", f.volumes.snapshots)
	}
}

// A failing snapshot blocks the change without ever stopping the old
// container, however many passes it takes.
func TestFailedSnapshotBlocksTheChangeWithoutAnOutage(t *testing.T) {
	f := newFixture(t)
	f.create(t, "acme")
	f.reconcile(t)
	old := f.runtime.only(t).ID
	f.volumes.failSnapshot = errors.New("pool is full")
	f.updateApp(t, "acme", func(app *spec.Tenant) { app.Image = imageB })
	f.runtime.takeEvents()
	for range 3 {
		if err := f.reconciler.Reconcile(context.Background()); err == nil || !strings.Contains(err.Error(), "pool is full") {
			t.Fatalf("Reconcile = %v", err)
		}
	}
	if events := f.runtime.takeEvents(); len(events) != 0 {
		t.Fatalf("a failing snapshot touched the running container: %v", events)
	}
	if container := f.runtime.only(t); container.ID != old || !container.Running || container.spec.Image != imageA {
		t.Fatalf("after a failed snapshot = %+v", container)
	}
	if got, _ := f.store.Get("acme"); got.DataImage != imageA {
		t.Fatalf("data image moved without a snapshot: %q", got.DataImage)
	}
	f.volumes.failSnapshot = nil
	f.reconcile(t)
	if f.runtime.only(t).spec.Image != imageB || len(f.volumes.snapshots) != 1 {
		t.Fatal("the change did not go ahead once the snapshot worked")
	}
}

// The snapshot follows the data, not the containers that happen to exist.
func TestImageChangedWhileSuspendedIsSnapshottedOnResume(t *testing.T) {
	f := newFixture(t)
	f.create(t, "acme")
	f.reconcile(t)
	f.suspend(t, "acme", true)
	f.reconcile(t)
	if f.runtime.count() != 0 {
		t.Fatal("suspended tenant still has containers")
	}
	if status, _ := f.reconciler.Status("acme"); status.Phase != PhaseSuspended {
		t.Fatalf("status = %+v", status)
	}
	f.updateApp(t, "acme", func(app *spec.Tenant) { app.Image = imageB })
	f.suspend(t, "acme", false)
	f.reconcile(t)
	if f.runtime.only(t).spec.Image != imageB || !slices.Equal(f.volumes.snapshots, []string{"acme@image-change"}) {
		t.Fatalf("resume with a new image: snapshots = %v", f.volumes.snapshots)
	}
}

// A container started before its dataset was mounted sees the empty
// directory underneath; it must be replaced, not adopted.
func TestContainerIsRecreatedWhenItsDataWasMountedLate(t *testing.T) {
	f := newFixture(t)
	f.create(t, "acme")
	f.reconcile(t)
	old := f.runtime.only(t).ID
	f.volumes.mountNext = true
	f.reconcile(t)
	if container := f.runtime.only(t); container.ID == old || !container.Running {
		t.Fatalf("container after a late mount = %+v", container)
	}
}

// A container that sees something other than the tenant's data at its data
// path (its dataset was mounted after it started) is replaced.
func TestContainerNotServingTheDataIsReplaced(t *testing.T) {
	f := newFixture(t)
	f.create(t, "acme")
	f.reconcile(t)
	old := f.runtime.only(t).ID
	f.volumes.notServing = true
	f.reconcile(t)
	if container := f.runtime.only(t); container.ID == old || !container.Running {
		t.Fatalf("container after a mount check failure = %+v", container)
	}
}

func TestContainerThatExitsDuringThePassIsStarted(t *testing.T) {
	f := newFixture(t)
	f.create(t, "acme")
	f.reconcile(t)
	container := f.runtime.only(t)
	f.runtime.exitBeforeInspect = true
	f.runtime.takeEvents()
	f.reconcile(t)
	if events := f.runtime.takeEvents(); !slices.Equal(events, []string{"start " + container.ID}) {
		t.Fatalf("events = %v", events)
	}
}

// A spec stored under older, looser rules is reported, not run.
func TestInvalidStoredSpecIsNotRun(t *testing.T) {
	f := newFixture(t)
	f.create(t, "old")
	if err := f.store.Close(); err != nil {
		t.Fatal(err)
	}
	db, err := sql.Open("sqlite", "file:"+f.storePath)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := db.Exec(`UPDATE apps SET spec = json_set(spec, '$.user', '')`); err != nil {
		t.Fatal(err)
	}
	_ = db.Close()
	st, err := store.Open(f.storePath, store.Options{PortFrom: 20000, PortTo: 20010, NetworkPool: netip.MustParsePrefix("10.240.0.0/16"), Sealer: func(string, string, spec.Secret) (string, error) { return "", nil }})
	if err != nil {
		t.Fatal(err)
	}
	defer st.Close()
	reconciler := New(st, f.runtime, f.volumes, f.key, f.reconciler.config, f.reconciler.log)
	if err := reconciler.Reconcile(context.Background()); err == nil || !strings.Contains(err.Error(), "no longer valid") {
		t.Fatalf("Reconcile = %v", err)
	}
	if f.runtime.count() != 0 {
		t.Fatal("a container was created for an invalid stored spec")
	}
}

func TestStoppedContainerIsStartedAgain(t *testing.T) {
	f := newFixture(t)
	f.create(t, "acme")
	f.reconcile(t)
	container := f.runtime.only(t)
	_ = f.runtime.Stop(context.Background(), container.ID, 0)
	f.runtime.takeEvents()
	f.reconcile(t)
	if events := f.runtime.takeEvents(); !slices.Equal(events, []string{"start " + container.ID}) {
		t.Fatalf("events = %v", events)
	}
}

func TestArchiveClearsTheRuntimeAndKeepsTheData(t *testing.T) {
	f := newFixture(t)
	f.create(t, "acme")
	f.reconcile(t)
	if err := f.reconciler.Archive(context.Background(), "acme", "test"); err != nil {
		t.Fatal(err)
	}
	if f.runtime.count() != 0 || f.runtime.networks["corral-acme"] != nil {
		t.Fatalf("left behind: %d containers, networks %v", f.runtime.count(), f.runtime.networks)
	}
	if len(f.volumes.removed) != 0 {
		t.Fatalf("archive removed data: %v", f.volumes.removed)
	}
	if _, err := f.store.Get("acme"); !errors.Is(err, store.ErrNotFound) {
		t.Fatalf("record after archiving = %v", err)
	}
	if archived, ok := f.store.ArchivedTenant("acme"); !ok || archived.Record.Spec.Suspended {
		t.Fatalf("archive entry = %+v, %v", archived, ok)
	}
	if _, ok := f.reconciler.Status("acme"); ok {
		t.Fatal("status kept for an archived tenant")
	}
	// Restored, it runs again on the same data.
	if _, err := f.store.Restore("acme", "test"); err != nil {
		t.Fatal(err)
	}
	f.reconcile(t)
	if !f.runtime.only(t).Running {
		t.Fatal("restored tenant is not running")
	}
}

// A failure leaves the tenant suspended and its record (with its port and
// subnet) in place, so the archive can be repeated and nothing is reused early.
func TestFailedArchiveCanBeRepeated(t *testing.T) {
	f := newFixture(t)
	f.create(t, "acme")
	f.reconcile(t)
	f.runtime.failRemove = errors.New("device busy")
	if err := f.reconciler.Archive(context.Background(), "acme", "test"); err == nil {
		t.Fatal("Archive reported success while the container remained")
	}
	record, err := f.store.Get("acme")
	if err != nil || !record.Spec.Suspended {
		t.Fatalf("record after a failed archive = %+v, %v", record, err)
	}
	f.runtime.failRemove = nil
	if err := f.reconciler.Archive(context.Background(), "acme", "test"); err != nil {
		t.Fatal(err)
	}
	// The archive remembers the tenant as running, as it was before the
	// first attempt, not as the suspension that attempt left behind.
	if archived, _ := f.store.ArchivedTenant("acme"); archived.Record.Spec.Suspended || archived.Record.SuspendedBeforeArchive != nil {
		t.Fatalf("archived record = %+v", archived.Record)
	}
}

func TestOrphanNetworkOfAMissingTenantIsRemoved(t *testing.T) {
	f := newFixture(t)
	f.create(t, "acme")
	f.reconcile(t)
	f.suspend(t, "acme", true)
	f.reconcile(t)
	if f.runtime.networks["corral-acme"] == nil {
		t.Fatal("a suspended tenant keeps its network")
	}
	// The record disappears without Archive, as after restoring an older
	// state file: the next pass still removes the network.
	if err := f.store.Archive("acme", "test"); err != nil {
		t.Fatal(err)
	}
	f.reconcile(t)
	if f.runtime.networks["corral-acme"] != nil {
		t.Fatal("the network of a tenant without a record was left behind")
	}
}

func TestPurgeOnlyArchivedTenants(t *testing.T) {
	f := newFixture(t)
	f.create(t, "acme")
	f.reconcile(t)
	if err := f.reconciler.Purge(context.Background(), "acme", "test"); err == nil {
		t.Fatal("purged a live tenant")
	}
	if err := f.reconciler.Archive(context.Background(), "acme", "test"); err != nil {
		t.Fatal(err)
	}
	if exists, _ := f.reconciler.DataExists(context.Background(), "acme"); !exists {
		t.Fatal("archive removed the data")
	}
	if err := f.reconciler.Purge(context.Background(), "acme", "test"); err != nil {
		t.Fatal(err)
	}
	if exists, _ := f.reconciler.DataExists(context.Background(), "acme"); exists {
		t.Fatal("Purge left the data")
	}
	if _, ok := f.store.ArchivedTenant("acme"); ok {
		t.Fatal("Purge left the archive entry")
	}
}

func TestFailuresAreReportedPerTenant(t *testing.T) {
	f := newFixture(t)
	f.create(t, "acme")
	f.runtime.failCreate = errors.New("port is already allocated")
	err := f.reconciler.Reconcile(context.Background())
	if err == nil || !strings.Contains(err.Error(), "tenant acme") || !strings.Contains(err.Error(), "port is already allocated") {
		t.Fatalf("Reconcile = %v", err)
	}
	status, _ := f.reconciler.Status("acme")
	if status.Phase != PhaseFailed || !strings.Contains(status.Message, "port is already allocated") {
		t.Fatalf("status = %+v", status)
	}
}

func TestSpecHashIsStable(t *testing.T) {
	f := newFixture(t)
	record := f.create(t, "acme")
	first, err := f.reconciler.containerSpec(record, "/srv/acme")
	if err != nil {
		t.Fatal(err)
	}
	for range 20 {
		again, _ := f.reconciler.containerSpec(record, "/srv/acme")
		if again.Labels[runtime.LabelHash] != first.Labels[runtime.LabelHash] {
			t.Fatal("the same spec hashed differently")
		}
	}
	record.Spec.Tmpfs = nil
	none, _ := f.reconciler.containerSpec(record, "/srv/acme")
	record.Spec.Tmpfs = map[string]string{}
	empty, _ := f.reconciler.containerSpec(record, "/srv/acme")
	if empty.Labels[runtime.LabelHash] != none.Labels[runtime.LabelHash] || none.Labels[runtime.LabelHash] == first.Labels[runtime.LabelHash] {
		t.Fatal("an empty tmpfs map hashed differently from none, or like the default /tmp")
	}
	record.Subnet = "10.240.0.32/28"
	moved, _ := f.reconciler.containerSpec(record, "/srv/acme")
	if moved.Labels[runtime.LabelHash] == first.Labels[runtime.LabelHash] {
		t.Fatal("a different subnet kept the same hash")
	}
}
