package reconcile

import (
	"context"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net"
	"net/http"
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
	networks   map[string]bool
	pulled     []string
	next       int
	events     []string
	failCreate error
}

type fakeContainer struct {
	runtime.Container
	spec runtime.ContainerSpec
}

func newFakeRuntime() *fakeRuntime {
	return &fakeRuntime{images: map[string]bool{imageA: true}, containers: map[string]*fakeContainer{}, networks: map[string]bool{}}
}

func (f *fakeRuntime) HasImage(_ context.Context, ref string) (bool, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.images[ref], nil
}

func (f *fakeRuntime) PullImage(_ context.Context, ref string) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.pulled = append(f.pulled, ref)
	f.images[ref] = true
	return nil
}

func (f *fakeRuntime) EnsureNetwork(_ context.Context, name string, _ map[string]string) (string, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.networks[name] = true
	return "172.30.0.0/16", nil
}

func (f *fakeRuntime) RemoveNetwork(_ context.Context, name string) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	delete(f.networks, name)
	f.events = append(f.events, "remove-network "+name)
	return nil
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
	f.events = append(f.events, "create "+id)
	return id, nil
}

func (f *fakeRuntime) Start(_ context.Context, id string) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.containers[id].Running, f.containers[id].State = true, "running"
	f.events = append(f.events, "start "+id)
	return nil
}

func (f *fakeRuntime) Stop(_ context.Context, id string, _ time.Duration) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.containers[id].Running, f.containers[id].State = false, "exited"
	f.events = append(f.events, "stop "+id)
	return nil
}

func (f *fakeRuntime) Remove(_ context.Context, id string) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	delete(f.containers, id)
	f.events = append(f.events, "remove "+id)
	return nil
}

func (f *fakeRuntime) Logs(context.Context, string, int) ([]byte, error) { return nil, nil }

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
	snapshots []string
	removed   []string
}

func (f *fakeVolumes) Snapshot(_ context.Context, tenant, reason string) (string, error) {
	f.snapshots = append(f.snapshots, tenant+"@"+reason)
	return tenant + "@" + reason, nil
}

func (f *fakeVolumes) Remove(ctx context.Context, tenant string) error {
	f.removed = append(f.removed, tenant)
	return f.Dir.Remove(ctx, tenant)
}

type fixture struct {
	store      *store.File
	runtime    *fakeRuntime
	volumes    *fakeVolumes
	key        *secrets.Key
	reconciler *Reconciler
	health     net.Listener
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
	st, err := store.OpenFile(filepath.Join(t.TempDir(), "state.json"), port, port)
	if err != nil {
		t.Fatal(err)
	}
	keyPath := filepath.Join(t.TempDir(), "master.key")
	if err := secrets.CreateKey(keyPath); err != nil {
		t.Fatal(err)
	}
	key, err := secrets.LoadKey(keyPath)
	if err != nil {
		t.Fatal(err)
	}
	rt := newFakeRuntime()
	volumes := &fakeVolumes{Dir: volume.Dir{Root: filepath.Join(t.TempDir(), "tenants")}}
	log := slog.New(slog.NewTextHandler(io.Discard, nil))
	reconciler := New(st, rt, volumes, key, Config{BaseDomain: "example.test", DefaultLimits: spec.Limits{MemoryMB: 256, CPUs: 0.5, PIDs: 128}}, log)
	return &fixture{store: st, runtime: rt, volumes: volumes, key: key, reconciler: reconciler, health: listener}
}

func (f *fixture) create(t *testing.T, name string) store.Record {
	t.Helper()
	tenant := spec.Tenant{
		Name: name, Image: imageA, Port: 8080, HealthPath: "/health", DataPath: "/data",
		DataUID: os.Getuid(), DataGID: os.Getgid(),
		Env:     map[string]string{"PUBLIC_URL": "{{url}}", "TRUSTED_PROXIES": "{{network}}"},
		Secrets: map[string]spec.Secret{"SESSION_SECRET": {Kind: "hex", Bytes: 32}},
	}
	sealed, err := f.key.Seal(secrets.Context(name, "SESSION_SECRET"), "the-secret")
	if err != nil {
		t.Fatal(err)
	}
	record, err := f.store.Create(store.Record{Spec: tenant, Sealed: map[string]string{"SESSION_SECRET": sealed}})
	if err != nil {
		t.Fatal(err)
	}
	return record
}

func (f *fixture) reconcile(t *testing.T) {
	t.Helper()
	if err := f.reconciler.Reconcile(context.Background()); err != nil {
		t.Fatal(err)
	}
}

func TestNewTenantGetsAHealthyContainer(t *testing.T) {
	f := newFixture(t)
	f.create(t, "acme")
	f.reconcile(t)
	container := f.runtime.only(t)
	if !container.Running || container.Name != "corral-acme" || container.spec.Network != "corral-acme" {
		t.Fatalf("container = %+v", container)
	}
	env := strings.Join(container.spec.Env, "\n")
	for _, want := range []string{"PUBLIC_URL=https://acme.example.test", "TRUSTED_PROXIES=172.30.0.0/16", "SESSION_SECRET=the-secret"} {
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

func TestImageChangeSnapshotsAndReplaces(t *testing.T) {
	f := newFixture(t)
	f.create(t, "acme")
	f.reconcile(t)
	old := f.runtime.only(t).ID
	if _, err := f.store.Update("acme", func(r *store.Record) error { r.Spec.Image = imageB; return nil }); err != nil {
		t.Fatal(err)
	}
	f.runtime.takeEvents()
	f.reconcile(t)
	replacement := f.runtime.only(t)
	if replacement.ID == old || replacement.spec.Image != imageB || !replacement.Running {
		t.Fatalf("container after image change = %+v", replacement)
	}
	if !slices.Equal(f.volumes.snapshots, []string{"acme@image-change"}) {
		t.Fatalf("snapshots = %v", f.volumes.snapshots)
	}
	if !slices.Equal(f.runtime.pulled, []string{imageB}) {
		t.Fatalf("pulled = %v", f.runtime.pulled)
	}
	events := f.runtime.takeEvents()
	if strings.Join(events, ",") != fmt.Sprintf("stop %s,remove %s,create %s,start %s", old, old, replacement.ID, replacement.ID) {
		t.Fatalf("events = %v", events)
	}
	// An env change replaces the container without a snapshot.
	if _, err := f.store.Update("acme", func(r *store.Record) error { r.Spec.Env["EXTRA"] = "1"; return nil }); err != nil {
		t.Fatal(err)
	}
	f.reconcile(t)
	if len(f.volumes.snapshots) != 1 {
		t.Fatalf("an env change took a snapshot: %v", f.volumes.snapshots)
	}
}

func TestSuspendStopsAndResumeStarts(t *testing.T) {
	f := newFixture(t)
	f.create(t, "acme")
	f.reconcile(t)
	if _, err := f.store.Update("acme", func(r *store.Record) error { r.Spec.Suspended = true; return nil }); err != nil {
		t.Fatal(err)
	}
	f.reconcile(t)
	if containers, _ := f.runtime.List(context.Background()); len(containers) != 0 {
		t.Fatalf("suspended tenant still has containers: %v", containers)
	}
	if status, _ := f.reconciler.Status("acme"); status.Phase != PhaseSuspended {
		t.Fatalf("status = %+v", status)
	}
	if _, err := f.store.Update("acme", func(r *store.Record) error { r.Spec.Suspended = false; return nil }); err != nil {
		t.Fatal(err)
	}
	f.reconcile(t)
	if !f.runtime.only(t).Running {
		t.Fatal("resumed tenant is not running")
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

func TestDeletedTenantIsCleanedUpAndPurged(t *testing.T) {
	f := newFixture(t)
	f.create(t, "acme")
	f.reconcile(t)
	if err := f.reconciler.Purge(context.Background(), "acme"); err == nil {
		t.Fatal("purged a tenant that still exists")
	}
	if err := f.store.Delete("acme"); err != nil {
		t.Fatal(err)
	}
	if err := f.reconciler.Purge(context.Background(), "acme"); err != nil {
		t.Fatal(err)
	}
	if containers, _ := f.runtime.List(context.Background()); len(containers) != 0 {
		t.Fatalf("containers left: %v", containers)
	}
	if f.runtime.networks["corral-acme"] {
		t.Fatal("network left behind")
	}
	if !slices.Equal(f.volumes.removed, []string{"acme"}) {
		t.Fatalf("removed volumes = %v", f.volumes.removed)
	}
	if _, ok := f.reconciler.Status("acme"); ok {
		t.Fatal("status kept for a deleted tenant")
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
	first, err := f.reconciler.containerSpec(record, "/srv/acme", "172.30.0.0/16")
	if err != nil {
		t.Fatal(err)
	}
	for range 20 {
		again, _ := f.reconciler.containerSpec(record, "/srv/acme", "172.30.0.0/16")
		if again.Labels[runtime.LabelHash] != first.Labels[runtime.LabelHash] {
			t.Fatal("the same spec hashed differently")
		}
	}
	moved, _ := f.reconciler.containerSpec(record, "/srv/acme", "172.31.0.0/16")
	if moved.Labels[runtime.LabelHash] == first.Labels[runtime.LabelHash] {
		t.Fatal("a different network range kept the same hash")
	}
}
