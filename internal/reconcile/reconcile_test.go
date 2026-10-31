package reconcile

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"maps"
	"net"
	"net/http"
	"net/http/httptest"
	"net/netip"
	"os"
	"path/filepath"
	"slices"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/PowerPenguini/murder/internal/runtime"
	"github.com/PowerPenguini/murder/internal/secrets"
	"github.com/PowerPenguini/murder/internal/spec"
	"github.com/PowerPenguini/murder/internal/store"
	"github.com/PowerPenguini/murder/internal/volume"
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
	subnets    map[string]string            // name → subnet
	pulled     []string
	next       int
	events     []string
	failCreate error
	failPull   error
	failRemove error
	// removedImages are the images RemoveImage deleted; aliases gives a
	// name the ID of an image known by another name.
	removedImages []string
	aliases       map[string]string
	// onImageID runs when an image's ID is looked up, as a write through the
	// API could meanwhile.
	onImageID func(ref string)
	// restartAfterList makes the next List report every container stopped
	// and then starts them, as a restart policy would during a pass.
	restartAfterList bool
	// exitBeforeInspect makes the next Inspect call see the container's
	// process gone, as if it exited between List and the inspection.
	exitBeforeInspect bool
	// state is what Inspect reports beyond the PID: restarts and the last
	// exit. onStop runs when Stop is called.
	state  runtime.ContainerState
	onStop func()
}

type fakeContainer struct {
	runtime.Container
	spec runtime.ContainerSpec
}

func newFakeRuntime() *fakeRuntime {
	return &fakeRuntime{images: map[string]bool{imageA: true}, containers: map[string]*fakeContainer{}, networks: map[string]map[string]string{}, subnets: map[string]string{}}
}

func (f *fakeRuntime) record(event string) { f.events = append(f.events, event) }

func (f *fakeRuntime) HasImage(_ context.Context, ref string) (bool, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.images[ref], nil
}

// imageID is the ID of a present image: its alias, or the name itself.
func (f *fakeRuntime) imageID(ref string) string {
	if !f.images[ref] {
		return ""
	}
	if id, ok := f.aliases[ref]; ok {
		return id
	}
	return ref
}

func (f *fakeRuntime) ImageID(_ context.Context, ref string) (string, error) {
	if f.onImageID != nil {
		f.onImageID(ref)
	}
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.imageID(ref), nil
}

// RemoveImage refuses, like the engine, an image a container uses, and
// removes every name of the image.
func (f *fakeRuntime) RemoveImage(_ context.Context, id string) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	for _, container := range f.containers {
		if f.imageID(container.spec.Image) == id {
			return fmt.Errorf("image %s: %w", id, runtime.ErrInUse)
		}
	}
	for _, ref := range slices.Sorted(maps.Keys(f.images)) {
		if f.imageID(ref) == id {
			delete(f.images, ref)
			f.removedImages = append(f.removedImages, ref)
		}
	}
	return nil
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

// EnsureNetwork refuses, like Docker, a subnet another network holds, and
// reports a network of the name with another subnet.
func (f *fakeRuntime) EnsureNetwork(_ context.Context, name, subnet string, labels map[string]string) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	if existing, ok := f.subnets[name]; ok {
		if existing != subnet {
			return fmt.Errorf("%w: %s has %s", runtime.ErrNetworkMismatch, name, existing)
		}
		return nil
	}
	for other, taken := range f.subnets {
		if taken == subnet {
			return fmt.Errorf("Pool overlaps with other one on this address space (%s)", other)
		}
	}
	f.networks[name], f.subnets[name] = labels, subnet
	f.record("create-network " + name + " " + subnet)
	return nil
}

func (f *fakeRuntime) RemoveNetwork(_ context.Context, name string) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	if _, ok := f.networks[name]; ok {
		f.record("remove-network " + name)
	}
	delete(f.networks, name)
	delete(f.subnets, name)
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
		if f.restartAfterList {
			listed := container.Container
			listed.Running = false
			out = append(out, listed)
			container.Running = true
			continue
		}
		out = append(out, container.Container)
	}
	f.restartAfterList = false
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

func (f *fakeRuntime) Stop(ctx context.Context, id string, _ time.Duration) error {
	if f.onStop != nil {
		f.onStop()
		if ctx.Err() != nil {
			return ctx.Err()
		}
	}
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

func (f *fakeRuntime) Inspect(_ context.Context, id string) (runtime.ContainerState, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	container, ok := f.containers[id]
	if !ok {
		return runtime.ContainerState{}, fmt.Errorf("container %s: %w", id, runtime.ErrNotFound)
	}
	state := f.state
	if container.Running && !state.Restarting && !f.exitBeforeInspect {
		state.Pid = 4242
	}
	f.exitBeforeInspect = false
	return state, nil
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
	failPrune    error
	pruned       []string
	notServing   bool
	diskGB       map[string]int
	limitErr     error
}

func (f *fakeVolumes) LateMounts() bool { return true }

func (f *fakeVolumes) Serving(context.Context, string, int) (bool, error) {
	serving := !f.notServing
	f.notServing = false
	return serving, nil
}

func (f *fakeVolumes) Ensure(ctx context.Context, tenant string, uid, gid, diskGB int) (string, error) {
	if f.diskGB == nil {
		f.diskGB = map[string]int{}
	}
	f.diskGB[tenant] = diskGB
	path, err := f.Dir.Ensure(ctx, tenant, uid, gid, diskGB)
	if err == nil {
		err = f.limitErr
	}
	return path, err
}

func (f *fakeVolumes) Snapshot(_ context.Context, tenant, reason string) (string, error) {
	if f.failSnapshot != nil {
		return "", f.failSnapshot
	}
	f.snapshots = append(f.snapshots, tenant+"@"+reason)
	return tenant + "@" + reason, nil
}

func (f *fakeVolumes) Prune(_ context.Context, tenant, reason string, keep int) error {
	f.pruned = append(f.pruned, fmt.Sprintf("%s@%s keep %d", tenant, reason, keep))
	return f.failPrune
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

// newFixture serves a health endpoint the reconciler checks every tenant at.
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
	statePath := filepath.Join(t.TempDir(), "murder.db")
	if err := store.Init(statePath, "test-node"); err != nil {
		t.Fatal(err)
	}
	// Every secret gets the same known value, so tests can look for it.
	sealer := func(tenant, name string, _ spec.Secret) (string, error) {
		return key.Seal(secrets.Context(tenant, name), "the-secret")
	}
	st, err := store.Open(statePath, store.Options{NetworkPool: netip.MustParsePrefix("10.240.0.0/16"), Sealer: sealer})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = st.Close() })
	rt := newFakeRuntime()
	volumes := &fakeVolumes{Dir: volume.Dir{Root: filepath.Join(t.TempDir(), "tenants")}}
	log := slog.New(slog.NewTextHandler(io.Discard, nil))
	reconciler := New(st, rt, volumes, key, Config{BaseDomain: "example.test", DefaultLimits: spec.Limits{MemoryMB: 256, CPUs: 0.5, PIDs: 128, DiskGB: 7}}, log)
	// Every tenant's health is checked at the test server.
	reconciler.backend = func(store.Record) (string, error) { return "127.0.0.1:" + strconv.Itoa(port), nil }
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

// reconcile runs passes until no pull they started is left to use, as Run
// does when a pull ends.
func (f *fixture) reconcile(t *testing.T) {
	t.Helper()
	for {
		if err := f.reconciler.Reconcile(context.Background()); err != nil {
			t.Fatal(err)
		}
		f.reconciler.pulls.wait()
		select {
		case <-f.reconciler.pulls.done:
		default:
			return
		}
	}
}

func TestNewTenantGetsAHealthyContainer(t *testing.T) {
	f := newFixture(t)
	record := f.create(t, "acme")
	f.reconcile(t)
	container := f.runtime.only(t)
	if !container.Running || container.Name != "murder-acme" || container.spec.Network != "murder-acme" {
		t.Fatalf("container = %+v", container)
	}
	env := strings.Join(container.spec.Env, "\n")
	for _, want := range []string{"PUBLIC_URL=https://acme.example.test", "TRUSTED_PROXIES=" + record.Subnet, "SESSION_SECRET=the-secret"} {
		if !strings.Contains(env, want) {
			t.Errorf("env lacks %q:\n%s", want, env)
		}
	}
	if container.spec.MemoryBytes != 256<<20 || container.spec.NanoCPUs != 5e8 || container.spec.PIDs != 128 || f.volumes.diskGB["acme"] != 7 {
		t.Errorf("default limits not applied: %+v, disk %d GB", container.spec, f.volumes.diskGB["acme"])
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
	restarted.backend = f.reconciler.backend
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
	if !slices.Equal(f.volumes.pruned, []string{"acme@image-change keep 5"}) {
		t.Fatalf("pruned = %v", f.volumes.pruned)
	}
	// imageA is a local image, which could not be pulled again: it stays.
	if len(f.runtime.removedImages) != 0 {
		t.Fatalf("removed images = %v", f.runtime.removedImages)
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

// A failed prune is logged; the image change goes on.
func TestFailedPruneDoesNotBlockTheChange(t *testing.T) {
	f := newFixture(t)
	f.create(t, "acme")
	f.reconcile(t)
	f.volumes.failPrune = errors.New("dataset is busy")
	f.updateApp(t, "acme", func(app *spec.Tenant) { app.Image = imageB })
	f.reconcile(t)
	if f.runtime.only(t).spec.Image != imageB {
		t.Fatal("a failed prune blocked the image change")
	}
}

// A registry image no tenant needs is removed: tenants need the image they
// run, the one their data has and the one before, for a rollback. A local
// image is never removed, and an image a tenant needs under another name
// stays.
func TestUnusedRegistryImagesAreRemoved(t *testing.T) {
	release := func(n int) string {
		return "registry.test/app@sha256:" + strings.Repeat(strconv.Itoa(n), 64)
	}
	f := newFixture(t)
	for n := 1; n <= 4; n++ {
		f.runtime.images[release(n)] = true
	}
	for _, name := range []string{"acme", "beta"} {
		f.create(t, name)
		f.updateApp(t, name, func(app *spec.Tenant) { app.Image = release(1) })
	}
	f.reconcile(t)
	// imageA, which the tenants first got, is local: it stays.
	for _, name := range []string{"acme", "beta"} {
		f.updateApp(t, name, func(app *spec.Tenant) { app.Image = release(2) })
	}
	f.reconcile(t)
	if len(f.runtime.removedImages) != 0 || !f.runtime.images[imageA] {
		t.Fatalf("removed %v, though release1 is every tenant's previous image", f.runtime.removedImages)
	}
	// acme moves on; beta still keeps release1 as its previous image.
	f.updateApp(t, "acme", func(app *spec.Tenant) { app.Image = release(3) })
	f.reconcile(t)
	if len(f.runtime.removedImages) != 0 {
		t.Fatalf("removed %v that beta keeps for a rollback", f.runtime.removedImages)
	}
	f.updateApp(t, "beta", func(app *spec.Tenant) { app.Image = release(3) })
	f.reconcile(t)
	if !slices.Equal(f.runtime.removedImages, []string{release(1)}) || !f.runtime.images[imageA] {
		t.Fatalf("removed images = %v, want release1 only", f.runtime.removedImages)
	}
	// release2 is known to acme under another name too: it stays.
	f.runtime.aliases = map[string]string{release(2): "id-2", release(4): "id-2"}
	f.updateApp(t, "acme", func(app *spec.Tenant) { app.Image = release(4) })
	f.updateApp(t, "beta", func(app *spec.Tenant) { app.Image = release(4) })
	f.reconcile(t)
	f.updateApp(t, "acme", func(app *spec.Tenant) { app.Image = release(4) })
	if !f.runtime.images[release(4)] || !f.runtime.images[release(2)] {
		t.Fatalf("removed an image a tenant runs under another name: %v", f.runtime.removedImages)
	}
	// Purged tenants need nothing: their images go with the next pass.
	for _, name := range []string{"acme", "beta"} {
		f.archive(t, name)
		f.reconcile(t)
		if err := f.reconciler.Purge(context.Background(), name, "test"); err != nil {
			t.Fatal(err)
		}
	}
	f.reconcile(t)
	if f.runtime.images[release(3)] || f.runtime.images[release(4)] || !f.runtime.images[imageA] {
		t.Fatalf("after the purge: removed %v, left %v", f.runtime.removedImages, f.runtime.images)
	}
	// release2 was held back as release4's other name; once its delay is
	// up, the next pass finds it gone and stops tracking it.
	clear(f.reconciler.imageRetryAt)
	f.reconcile(t)
	if left := f.store.Images(); len(left) != 0 {
		t.Fatalf("still tracked: %v", left)
	}
}

// An image a tenant is given while the sweep runs is not removed.
func TestImageGivenDuringTheSweepStays(t *testing.T) {
	const old = "registry.test/app@sha256:1111111111111111111111111111111111111111111111111111111111111111"
	f := newFixture(t)
	f.runtime.images[old] = true
	f.create(t, "acme")
	f.updateApp(t, "acme", func(app *spec.Tenant) { app.Image = old })
	f.reconcile(t)
	f.updateApp(t, "acme", func(app *spec.Tenant) { app.Image = imageB })
	f.reconcile(t)
	f.updateApp(t, "acme", func(app *spec.Tenant) { app.Image = imageA })
	f.runtime.onImageID = func(ref string) {
		if ref == old {
			f.runtime.onImageID = nil
			f.updateApp(t, "acme", func(app *spec.Tenant) { app.Image = old }) // a rollback
		}
	}
	if err := f.reconciler.Reconcile(context.Background()); err != nil {
		t.Fatal(err)
	}
	if !f.runtime.images[old] {
		t.Fatal("removed the image a rollback was just given")
	}
}

// An image a container outside Murder uses is kept and tried again later,
// not on every pass.
func TestImageInUseIsRetriedLater(t *testing.T) {
	const old = "registry.test/app@sha256:1111111111111111111111111111111111111111111111111111111111111111"
	f := newFixture(t)
	f.runtime.images[old] = true
	f.create(t, "acme")
	f.updateApp(t, "acme", func(app *spec.Tenant) { app.Image = old })
	f.reconcile(t)
	f.updateApp(t, "acme", func(app *spec.Tenant) { app.Image = imageB })
	f.reconcile(t)
	f.updateApp(t, "acme", func(app *spec.Tenant) { app.Image = imageA })
	f.runtime.containers["outside"] = &fakeContainer{spec: runtime.ContainerSpec{Image: old}}
	f.reconcile(t)
	if !f.runtime.images[old] || f.reconciler.imageRetryAt[old].IsZero() {
		t.Fatalf("an image in use: present %v, retry at %v", f.runtime.images[old], f.reconciler.imageRetryAt[old])
	}
	delete(f.runtime.containers, "outside")
	f.reconcile(t)
	if !f.runtime.images[old] {
		t.Fatal("retried before the delay was up")
	}
	f.reconciler.imageRetryAt[old] = time.Time{}
	f.reconcile(t)
	if f.runtime.images[old] {
		t.Fatal("not removed once free")
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
	// The first pass starts the pull and reports the tenant pulling, still
	// served by its container.
	if err := f.reconciler.Reconcile(context.Background()); err != nil {
		t.Fatal(err)
	}
	if status, _ := f.reconciler.Status("acme"); status.Phase != PhasePulling || status.ContainerID != old {
		t.Fatalf("status while pulling = %+v", status)
	}
	f.reconciler.pulls.wait()
	// Later passes report the failure without pulling again until the
	// retry is due.
	for range 3 {
		if err := f.reconciler.Reconcile(context.Background()); err == nil || !strings.Contains(err.Error(), "manifest unknown") {
			t.Fatalf("Reconcile = %v", err)
		}
	}
	if status, _ := f.reconciler.Status("acme"); status.Phase != PhaseFailed || !strings.Contains(status.Message, "attempt 1") || status.ContainerID != old {
		t.Fatalf("status after a failed pull = %+v", status)
	}
	if len(f.runtime.pulled) != 1 {
		t.Fatalf("pulled %d times before the retry was due", len(f.runtime.pulled))
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
	// "murder reconcile" retries at once and reports how the pull went.
	if err := f.reconciler.ReconcileNow(context.Background()); err == nil || !strings.Contains(err.Error(), "manifest unknown") || len(f.runtime.pulled) != 2 {
		t.Fatalf("ReconcileNow with the registry still failing = %v, pulled %v", err, f.runtime.pulled)
	}
	f.runtime.mu.Lock()
	f.runtime.failPull = nil // say, the credentials were fixed
	f.runtime.mu.Unlock()
	if err := f.reconciler.ReconcileNow(context.Background()); err != nil {
		t.Fatal(err)
	}
	if container := f.runtime.only(t); len(f.runtime.pulled) != 3 || container.spec.Image != imageB {
		t.Fatalf("after a retry: pulled %v, container %+v", f.runtime.pulled, container)
	}
}

// Once the old container is gone, a failed replacement names no container
// that no longer exists.
func TestFailedCreateNamesNoRemovedContainer(t *testing.T) {
	f := newFixture(t)
	f.create(t, "acme")
	f.reconcile(t)
	f.runtime.images[imageB] = true
	f.runtime.failCreate = errors.New("bad mount")
	f.updateApp(t, "acme", func(app *spec.Tenant) { app.Image = imageB })
	if err := f.reconciler.Reconcile(context.Background()); err == nil {
		t.Fatal("a failed create reported success")
	}
	if status, _ := f.reconciler.Status("acme"); status.Phase != PhaseFailed || status.ContainerID != "" {
		t.Fatalf("status = %+v", status)
	}
}

// An image whose pull runs while no tenant needs it any more stays tracked,
// and is removed once it arrives.
func TestImageStillPullingStaysTracked(t *testing.T) {
	const wanted = "registry.test/app@sha256:1111111111111111111111111111111111111111111111111111111111111111"
	f := newFixture(t)
	f.create(t, "acme")
	f.reconcile(t)
	release := make(chan struct{})
	pull := f.reconciler.pulls.pull
	f.reconciler.pulls.pull = func(ctx context.Context, image string) error {
		<-release
		return pull(ctx, image)
	}
	f.updateApp(t, "acme", func(app *spec.Tenant) { app.Image = wanted })
	if err := f.reconciler.Reconcile(context.Background()); err != nil {
		t.Fatal(err)
	}
	f.updateApp(t, "acme", func(app *spec.Tenant) { app.Image = imageA })
	if err := f.reconciler.Reconcile(context.Background()); err != nil {
		t.Fatal(err)
	}
	if !slices.Contains(f.store.Images(), wanted) {
		t.Fatal("forgot an image whose pull still runs")
	}
	close(release)
	f.reconcile(t)
	if f.runtime.images[wanted] || slices.Contains(f.store.Images(), wanted) {
		t.Fatalf("the pulled image no tenant needs stayed: tracked %v", f.store.Images())
	}
}

// A slow pull holds up neither other tenants nor work under the pass lock.
func TestSlowPullHoldsUpNothingElse(t *testing.T) {
	f := newFixture(t)
	f.create(t, "acme")
	f.reconcile(t)
	old := f.runtime.only(t).ID
	release := make(chan struct{})
	pull := f.reconciler.pulls.pull
	f.reconciler.pulls.pull = func(ctx context.Context, image string) error {
		<-release
		return pull(ctx, image)
	}
	f.updateApp(t, "acme", func(app *spec.Tenant) { app.Image = imageB })
	f.create(t, "beta")
	if err := f.reconciler.Reconcile(context.Background()); err != nil {
		t.Fatal(err)
	}
	if status, _ := f.reconciler.Status("beta"); status.Phase != PhaseRunning {
		t.Fatalf("beta waited for acme's pull: %+v", status)
	}
	if status, _ := f.reconciler.Status("acme"); status.Phase != PhasePulling || status.ContainerID != old || !status.Healthy {
		t.Fatalf("acme while pulling, served by its old container = %+v", status)
	}
	locked := make(chan error, 1)
	go func() { locked <- f.reconciler.Exclusive(func() error { return nil }) }()
	select {
	case <-locked:
	case <-time.After(5 * time.Second):
		t.Fatal("the pass lock was held during the pull")
	}
	close(release)
	f.reconcile(t)
	if status, _ := f.reconciler.Status("acme"); status.Phase != PhaseRunning || status.ContainerID == old {
		t.Fatalf("acme after the pull = %+v", status)
	}
}

// Health checks run side by side: each request here answers only once
// every tenant's request has arrived, which one at a time would never do
// within the timeout.
func TestHealthChecksRunSideBySide(t *testing.T) {
	f := newFixture(t)
	names := []string{"acme", "beta", "gamma"}
	var arrived atomic.Int32
	all := make(chan struct{})
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		if arrived.Add(1) == int32(len(names)) {
			close(all)
		}
		select {
		case <-all:
		case <-time.After(5 * time.Second):
		}
		_, _ = io.WriteString(w, "ok")
	}))
	t.Cleanup(server.Close)
	f.reconciler.backend = func(store.Record) (string, error) { return strings.TrimPrefix(server.URL, "http://"), nil }
	for _, name := range names {
		f.create(t, name)
	}
	f.reconcile(t)
	for _, name := range names {
		if status, _ := f.reconciler.Status(name); !status.Healthy {
			t.Fatalf("%s = %+v", name, status)
		}
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
	f.runtime.images[imageB] = true // already pulled
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

// A disk limit that cannot be applied is a warning; the tenant runs.
func TestDiskLimitProblemIsAWarning(t *testing.T) {
	f := newFixture(t)
	f.create(t, "acme")
	f.volumes.limitErr = fmt.Errorf("%w: 7 GB: size is less than current used", volume.ErrDiskLimit)
	f.reconcile(t)
	status, _ := f.reconciler.Status("acme")
	if status.Phase != PhaseRunning || !status.Healthy || !strings.Contains(status.Warning, "less than current used") || !f.runtime.only(t).Running {
		t.Fatalf("status = %+v", status)
	}
	f.volumes.limitErr = nil
	f.reconcile(t)
	if status, _ := f.reconciler.Status("acme"); status.Warning != "" {
		t.Fatalf("the warning outlived its cause: %+v", status)
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

// A container listed as stopped that its restart policy starts during the
// pass, before its dataset is mounted, is checked and replaced too.
func TestContainerRestartedDuringThePassIsCheckedForItsData(t *testing.T) {
	f := newFixture(t)
	f.create(t, "acme")
	f.reconcile(t)
	old := f.runtime.only(t).ID
	f.runtime.restartAfterList = true
	f.volumes.notServing = true
	f.reconcile(t)
	if container := f.runtime.only(t); container.ID == old || !container.Running {
		t.Fatalf("container restarted on the empty directory = %+v", container)
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
	st, err := store.Open(f.storePath, store.Options{NetworkPool: netip.MustParsePrefix("10.240.0.0/16"), Sealer: func(string, string, spec.Secret) (string, error) { return "", nil }})
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

func (f *fixture) archive(t *testing.T, name string) {
	t.Helper()
	if err := f.store.Archive(name, "test"); err != nil {
		t.Fatal(err)
	}
}

func TestArchiveClearsTheRuntimeAndKeepsTheData(t *testing.T) {
	f := newFixture(t)
	f.create(t, "acme")
	f.reconcile(t)
	f.archive(t, "acme")
	f.reconcile(t)
	if f.runtime.count() != 0 || f.runtime.networks["murder-acme"] != nil {
		t.Fatalf("left behind: %d containers, networks %v", f.runtime.count(), f.runtime.networks)
	}
	if len(f.volumes.removed) != 0 {
		t.Fatalf("archive removed data: %v", f.volumes.removed)
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

// An archived tenant keeps its subnet until it is purged, so a new tenant is
// never routed to a container still running at that address, even one a
// pass could not remove.
func TestArchivedTenantKeepsItsSubnetUntilPurged(t *testing.T) {
	f := newFixture(t)
	old := f.create(t, "acme")
	f.reconcile(t)
	f.runtime.failRemove = errors.New("device busy")
	f.archive(t, "acme")
	if err := f.reconciler.Reconcile(context.Background()); err == nil {
		t.Fatal("a pass that could not remove a container reported success")
	}
	if beta := f.create(t, "beta"); beta.Subnet == old.Subnet {
		t.Fatal("a new tenant got the subnet of a container still running")
	}
	f.runtime.failRemove = nil
	if err := f.reconciler.Purge(context.Background(), "acme", "test"); err != nil {
		t.Fatal(err)
	}
	containers, _ := f.runtime.List(context.Background())
	if f.runtime.networks["murder-acme"] != nil || slices.ContainsFunc(containers, func(c runtime.Container) bool { return c.Labels[runtime.LabelTenant] == "acme" }) {
		t.Fatal("a purge left the tenant's runtime")
	}
	if gamma := f.create(t, "gamma"); gamma.Subnet != old.Subnet {
		t.Fatalf("gamma got %s, not the purged %s", gamma.Subnet, old.Subnet)
	}
	f.reconcile(t)
}

// A tenant restored before a pass removed its runtime returns to its own
// network and container.
func TestQuickRestoreKeepsTheNetwork(t *testing.T) {
	f := newFixture(t)
	f.create(t, "acme")
	f.reconcile(t)
	container := f.runtime.only(t).ID
	f.archive(t, "acme")
	if _, err := f.store.Restore("acme", "test"); err != nil {
		t.Fatal(err)
	}
	f.runtime.takeEvents()
	f.reconcile(t)
	if f.runtime.only(t).ID != container || slices.ContainsFunc(f.runtime.takeEvents(), func(e string) bool { return strings.Contains(e, "network") }) {
		t.Fatal("a quick restore rebuilt the tenant's runtime")
	}
}

// A network found with another subnet (say, after restoring an older
// database) is rebuilt.
func TestOutdatedNetworkIsRebuilt(t *testing.T) {
	f := newFixture(t)
	record := f.create(t, "acme")
	f.runtime.networks["murder-acme"] = map[string]string{runtime.LabelTenant: "acme"}
	f.runtime.subnets["murder-acme"] = "10.240.9.0/28"
	f.reconcile(t)
	if f.runtime.subnets["murder-acme"] != record.Subnet {
		t.Fatalf("network subnet = %s, want %s", f.runtime.subnets["murder-acme"], record.Subnet)
	}
	if status, _ := f.reconciler.Status("acme"); status.Phase != PhaseRunning || !status.Healthy {
		t.Fatalf("status = %+v", status)
	}
}

func TestContainerGetsTheFixedAddress(t *testing.T) {
	f := newFixture(t)
	record := f.create(t, "acme")
	f.reconcile(t)
	_, address, _ := runtime.Addresses(record.Subnet)
	if got := f.runtime.only(t).spec.Address; got != address.String() {
		t.Fatalf("address = %s, want %s", got, address)
	}
	backend, err := Backend(record)
	if err != nil || backend != address.String()+":8080" {
		t.Fatalf("Backend = %s, %v", backend, err)
	}
}

func TestOrphanNetworkOfAMissingTenantIsRemoved(t *testing.T) {
	f := newFixture(t)
	f.create(t, "acme")
	f.reconcile(t)
	f.suspend(t, "acme", true)
	f.reconcile(t)
	if f.runtime.networks["murder-acme"] == nil {
		t.Fatal("a suspended tenant keeps its network")
	}
	f.archive(t, "acme")
	f.reconcile(t)
	if f.runtime.networks["murder-acme"] != nil {
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
	f.archive(t, "acme")
	if exists, _ := f.reconciler.DataExists(context.Background(), "acme"); !exists {
		t.Fatal("archive removed the data")
	}
	if err := f.reconciler.Purge(context.Background(), "acme", "test"); err != nil {
		t.Fatal(err)
	}
	if exists, _ := f.reconciler.DataExists(context.Background(), "acme"); exists {
		t.Fatal("Purge left the data")
	}
	if _, err := f.store.Get("acme"); !errors.Is(err, store.ErrNotFound) {
		t.Fatalf("Purge left the archived tenant: %v", err)
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

// Stopping the agent while a container is replaced lets the replacement
// finish, so the tenant is not left without a container, and stops the
// pass before the next tenant.
func TestCancelledPassFinishesTheReplacementUnderWay(t *testing.T) {
	f := newFixture(t)
	f.create(t, "acme")
	f.create(t, "beta")
	f.reconcile(t)
	before := map[string]string{}
	for _, container := range f.runtime.containers {
		before[container.Labels[runtime.LabelTenant]] = container.ID
	}
	statusBefore, _ := f.reconciler.Status("acme")
	passBefore, _ := f.reconciler.LastPass()
	if passBefore.IsZero() {
		t.Fatal("a whole pass left no time")
	}
	for _, name := range []string{"acme", "beta"} {
		f.updateApp(t, name, func(app *spec.Tenant) { app.Env["NEW"] = "1" })
	}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	f.runtime.onStop = cancel // the signal arrives while the old one stops
	if err := f.reconciler.Reconcile(ctx); !errors.Is(err, context.Canceled) {
		t.Fatalf("Reconcile = %v, want context.Canceled", err)
	}
	after := map[string]*fakeContainer{}
	for _, container := range f.runtime.containers {
		after[container.Labels[runtime.LabelTenant]] = container
	}
	if acme := after["acme"]; acme == nil || acme.ID == before["acme"] || !acme.Running {
		t.Fatalf("acme after the cancelled pass = %+v, want its replacement running", acme)
	}
	if beta := after["beta"]; beta == nil || beta.ID != before["beta"] || !beta.Running {
		t.Fatalf("beta after the cancelled pass = %+v, want it untouched", beta)
	}
	if status, _ := f.reconciler.Status("acme"); status != statusBefore {
		t.Fatalf("status after an interrupted pass = %+v, want the last whole pass's %+v", status, statusBefore)
	}
	if pass, _ := f.reconciler.LastPass(); pass != passBefore {
		t.Fatalf("an interrupted pass counts as whole: %v, want %v", pass, passBefore)
	}
}

// A container the engine is restarting after a crash is left to it, and
// its status tells why it exited.
func TestRestartingContainerReportsWhyItExited(t *testing.T) {
	f := newFixture(t)
	f.create(t, "acme")
	f.reconcile(t)
	exited := time.Date(2026, 10, 29, 15, 2, 0, 0, time.UTC)
	f.runtime.state = runtime.ContainerState{Restarting: true, Restarts: 3, ExitKnown: true, OOMKilled: true, ExitCode: 137, ExitedAt: exited}
	f.runtime.takeEvents()
	f.reconcile(t)
	if events := f.runtime.takeEvents(); len(events) != 0 {
		t.Fatalf("events = %v, want the engine left to restart it", events)
	}
	status, _ := f.reconciler.Status("acme")
	if status.Phase != PhaseRestarting || status.Healthy || status.Restarts != 3 ||
		status.LastExit != "killed for using more than its 256 MB of memory, at 2026-10-29T15:02:00Z" {
		t.Fatalf("status = %+v", status)
	}
	// Running again, it still tells how often it exited, and why if the
	// engine's event log knew.
	f.runtime.state = runtime.ContainerState{Restarts: 4, ExitedAt: exited, ExitKnown: true, ExitCode: 2}
	f.reconcile(t)
	status, _ = f.reconciler.Status("acme")
	if status.Phase != PhaseRunning || !status.Healthy || status.Restarts != 4 || status.LastExit != "exited with code 2 at 2026-10-29T15:02:00Z" {
		t.Fatalf("status = %+v", status)
	}
	f.runtime.state = runtime.ContainerState{Restarts: 5, ExitedAt: exited}
	f.reconcile(t)
	if status, _ = f.reconciler.Status("acme"); status.LastExit != "exited at 2026-10-29T15:02:00Z" {
		t.Fatalf("status = %+v", status)
	}
}

// A container Murder starts again (someone stopped it) has no restarts of
// the app's to report.
func TestStartedContainerReportsNoRestarts(t *testing.T) {
	f := newFixture(t)
	f.create(t, "acme")
	f.reconcile(t)
	f.runtime.state = runtime.ContainerState{Restarts: 3, ExitedAt: time.Now(), ExitKnown: true, ExitCode: 143}
	f.runtime.exitBeforeInspect = true
	f.reconcile(t)
	if status, _ := f.reconciler.Status("acme"); status.Restarts != 0 || status.LastExit != "" {
		t.Fatalf("status = %+v", status)
	}
}

// Stopping the reconciler stops a pass begun through the API too, after the
// tenant step under way.
func TestStoppingEndsAPassBegunElsewhere(t *testing.T) {
	f := newFixture(t)
	f.create(t, "acme")
	f.create(t, "beta")
	f.reconcile(t)
	before := map[string]string{}
	for _, container := range f.runtime.containers {
		before[container.Labels[runtime.LabelTenant]] = container.ID
	}
	for _, name := range []string{"acme", "beta"} {
		f.updateApp(t, name, func(app *spec.Tenant) { app.Env["NEW"] = "1" })
	}
	f.runtime.onStop = f.reconciler.stop // the agent stops meanwhile
	if err := f.reconciler.Reconcile(context.Background()); !errors.Is(err, context.Canceled) {
		t.Fatalf("Reconcile = %v, want context.Canceled", err)
	}
	for _, container := range f.runtime.containers {
		switch tenant := container.Labels[runtime.LabelTenant]; {
		case tenant == "acme" && (container.ID == before["acme"] || !container.Running):
			t.Errorf("acme = %+v, want its replacement running", container)
		case tenant == "beta" && container.ID != before["beta"]:
			t.Errorf("beta = %+v, want it untouched", container)
		}
	}
}

// Run returns only once a pass begun elsewhere has let go.
func TestRunWaitsForAPassBegunElsewhere(t *testing.T) {
	f := newFixture(t)
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	f.reconciler.pass.Lock()
	returned := make(chan struct{})
	go func() {
		f.reconciler.Run(ctx, time.Hour)
		close(returned)
	}()
	select {
	case <-returned:
		t.Fatal("Run returned while a pass was under way")
	case <-time.After(100 * time.Millisecond):
	}
	if f.reconciler.life.Err() == nil {
		t.Fatal("the pass under way was not told to stop")
	}
	f.reconciler.pass.Unlock()
	select {
	case <-returned:
	case <-time.After(5 * time.Second):
		t.Fatal("Run did not return after the pass")
	}
	// Work queued behind it is refused, not run while the agent stops.
	if err := f.reconciler.Exclusive(func() error { return nil }); !errors.Is(err, ErrStopping) {
		t.Fatalf("Exclusive after stop = %v", err)
	}
	if err := f.reconciler.Purge(context.Background(), "acme", "test"); !errors.Is(err, ErrStopping) {
		t.Fatalf("Purge after stop = %v", err)
	}
}

// An exit the container was not restarted after (Murder or the host
// stopped it) is not reported as the app's.
func TestExitWithoutRestartIsNotReported(t *testing.T) {
	f := newFixture(t)
	f.create(t, "acme")
	f.reconcile(t)
	f.runtime.state = runtime.ContainerState{ExitCode: 143, ExitedAt: time.Now()}
	f.reconcile(t)
	if status, _ := f.reconciler.Status("acme"); status.LastExit != "" || status.Restarts != 0 {
		t.Fatalf("status = %+v", status)
	}
}

func TestANodeRunsOnlyItsOwnTenants(t *testing.T) {
	f := newFixture(t)
	f.create(t, "acme")
	if err := f.store.PutNode(store.Node{Name: "other-node", Address: "10.0.0.2:7400"}, "test"); err != nil {
		t.Fatal(err)
	}
	app := spec.Tenant{Image: imageB, HealthPath: "/health", User: fmt.Sprintf("%d:%d", os.Getuid(), os.Getgid())}
	if _, err := f.store.PutApp(store.App{Name: "away", Spec: app}, "test"); err != nil {
		t.Fatal(err)
	}
	if _, err := f.store.Create(store.NewTenant{Name: "away", App: "away", Node: "other-node"}, "test"); err != nil {
		t.Fatal(err)
	}
	f.reconcile(t)
	if f.runtime.count() != 1 || f.runtime.only(t).spec.Labels[runtime.LabelTenant] != "acme" {
		t.Fatalf("containers: %d", f.runtime.count())
	}
	if _, ok := f.reconciler.Status("away"); ok {
		t.Error("this node reports a tenant another node runs")
	}
	if exists, _ := f.volumes.Exists(context.Background(), "away"); exists {
		t.Error("this node made data for a tenant another node runs")
	}
	// Its image is not this node's to keep.
	if f.reconciler.needed(imageB) {
		t.Error("another node's image counts as needed here")
	}
	if err := f.store.Archive("away", "test"); err != nil {
		t.Fatal(err)
	}
	if err := f.reconciler.Purge(context.Background(), "away", "test"); !errors.Is(err, ErrElsewhere) {
		t.Fatalf("purge a tenant another node runs: %v", err)
	}
}
