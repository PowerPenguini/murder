package apiserver

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net"
	"net/http"
	"net/http/httptest"
	"net/netip"
	"os"
	"os/user"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/PowerPenguini/murder/internal/api"
	"github.com/PowerPenguini/murder/internal/cluster"
	"github.com/PowerPenguini/murder/internal/reconcile"
	"github.com/PowerPenguini/murder/internal/runtime"
	"github.com/PowerPenguini/murder/internal/secrets"
	"github.com/PowerPenguini/murder/internal/spec"
	"github.com/PowerPenguini/murder/internal/store"
	"github.com/PowerPenguini/murder/internal/volume"
)

// memoryRuntime runs nothing; it only remembers containers.
type memoryRuntime struct {
	mu         sync.Mutex
	containers []runtime.Container
	// latest is what the registry holds under each tag, if not what
	// Resolve pins it to.
	latest map[string]string
}

func (m *memoryRuntime) HasImage(context.Context, string) (bool, error) { return true, nil }

func (m *memoryRuntime) PullImage(context.Context, string) error { return nil }

func (m *memoryRuntime) RemoveImage(context.Context, string) error { return nil }

func (m *memoryRuntime) ImageID(context.Context, string) (string, error) { return "", nil }

func (m *memoryRuntime) EnsureNetwork(context.Context, string, string, map[string]string) error {
	return nil
}

func (m *memoryRuntime) RemoveNetwork(context.Context, string) error { return nil }

func (m *memoryRuntime) ListNetworks(context.Context) ([]runtime.Network, error) {
	return nil, nil
}

func (m *memoryRuntime) List(context.Context) ([]runtime.Container, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	return append([]runtime.Container(nil), m.containers...), nil
}

func (m *memoryRuntime) Create(_ context.Context, s runtime.ContainerSpec) (string, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.containers = append(m.containers, runtime.Container{ID: "c-" + s.Labels[runtime.LabelTenant], Name: s.Name, Labels: s.Labels})
	return "c-" + s.Labels[runtime.LabelTenant], nil
}

func (m *memoryRuntime) Start(_ context.Context, id string) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	for i := range m.containers {
		if m.containers[i].ID == id {
			m.containers[i].Running = true
		}
	}
	return nil
}

func (m *memoryRuntime) Stop(context.Context, string, time.Duration) error { return nil }

func (m *memoryRuntime) Remove(_ context.Context, id string) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	for i := range m.containers {
		if m.containers[i].ID == id {
			m.containers = append(m.containers[:i], m.containers[i+1:]...)
			break
		}
	}
	return nil
}

func (m *memoryRuntime) Inspect(context.Context, string) (runtime.ContainerState, error) {
	return runtime.ContainerState{Pid: 1}, nil
}

func (m *memoryRuntime) SetRestartPolicy(context.Context, string, string) error { return nil }

// Resolve pins tags to a digest derived from the tag; "missing" tags fail.
func (m *memoryRuntime) Resolve(_ context.Context, ref string) (string, string, error) {
	if spec.Pinned(ref) {
		return ref, "", nil
	}
	if strings.Contains(ref, "missing") {
		return "", "", errors.New("manifest unknown")
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	if pinned, ok := m.latest[ref]; ok && pinned != "" {
		return pinned, "", nil
	}
	return pinnedTag(ref), "", nil
}

func (m *memoryRuntime) Latest(_ context.Context, tag string) (string, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if strings.Contains(tag, "missing") {
		return "", errors.New("manifest unknown")
	}
	if pinned, ok := m.latest[tag]; ok {
		if pinned == "" {
			return "", errors.New("the registry did not answer")
		}
		return pinned, nil
	}
	return pinnedTag(tag), nil
}

func (m *memoryRuntime) setLatest(tag, pinned string) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.latest == nil {
		m.latest = map[string]string{}
	}
	m.latest[tag] = pinned
}

func (m *memoryRuntime) Logs(_ context.Context, id string, tail int) ([]byte, error) {
	return []byte("logs of " + id + "\n"), nil
}

type apiFixture struct {
	client     *api.Client
	store      *store.SQLite
	reconciler *reconcile.Reconciler
	handler    http.Handler
	dataRoot   string
	server     *Server
	runtime    *memoryRuntime
}

func newAPIFixture(t *testing.T) *apiFixture {
	t.Helper()
	keyPath := filepath.Join(t.TempDir(), "master.key")
	if err := secrets.CreateKey(keyPath); err != nil {
		t.Fatal(err)
	}
	key, _ := secrets.LoadKey(keyPath)
	statePath := filepath.Join(t.TempDir(), "murder.db")
	if err := store.Init(statePath, "test-node"); err != nil {
		t.Fatal(err)
	}
	st, err := store.Open(statePath, store.Options{NetworkPool: netip.MustParsePrefix("10.240.0.0/16"), Sealer: key.NewSealed})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = st.Close() })
	rt := &memoryRuntime{}
	dataRoot := filepath.Join(t.TempDir(), "tenants")
	log := slog.New(slog.NewTextHandler(io.Discard, nil))
	reconciler := reconcile.New(st, rt, volume.Dir{Root: dataRoot}, key, reconcile.Config{BaseDomain: "example.test", HealthTimeout: 100 * time.Millisecond}, log)
	served := NewServer(st, reconciler, rt, key, log)
	handler := served.Handler()
	server := httptest.NewServer(handler)
	t.Cleanup(server.Close)
	return &apiFixture{client: api.NewClientURL(server.URL), store: st, reconciler: reconciler, handler: handler, dataRoot: dataRoot, server: served, runtime: rt}
}

func appSpec() spec.Tenant {
	return spec.Tenant{
		Image: "ghcr.io/example/app:1.4", HealthPath: "/health",
		User:    fmt.Sprintf("%d:%d", os.Getuid(), os.Getgid()),
		Secrets: spec.Secrets{"SESSION_SECRET": {}},
	}
}

// withApp is a fixture with app "web" stored.
func withApp(t *testing.T) *apiFixture {
	t.Helper()
	f := newAPIFixture(t)
	if _, err := f.client.PutApp(context.Background(), "web", appSpec()); err != nil {
		t.Fatal(err)
	}
	return f
}

func tenantOf(name string) api.TenantRequest { return api.TenantRequest{Name: name, App: "web"} }

// pinnedTag is how memoryRuntime pins a tag.
func pinnedTag(ref string) string {
	repository := ref[:strings.LastIndex(ref, ":")]
	sum := sha256.Sum256([]byte(ref))
	return repository + "@sha256:" + hex.EncodeToString(sum[:])
}

func checkStatus(t *testing.T, name string, err error, status int) {
	t.Helper()
	apiErr, ok := err.(*api.Error)
	if !ok || apiErr.Status != status {
		t.Errorf("%s: error %v, want HTTP %d", name, err, status)
	}
}

func TestAppsArePinnedAndRolledOutToTheirTenants(t *testing.T) {
	f := newAPIFixture(t)
	ctx := context.Background()
	created, err := f.client.PutApp(ctx, "web", appSpec())
	if err != nil || !created.Created || !created.Changed || created.App.Spec.Image != pinnedTag("ghcr.io/example/app:1.4") || created.App.Spec.Port != 8080 {
		t.Fatalf("new app = %+v, %v", created, err)
	}
	if _, err := f.client.Create(ctx, tenantOf("acme")); err != nil {
		t.Fatal(err)
	}
	beta := tenantOf("beta")
	beta.Overrides = spec.Overrides{Env: map[string]string{"TZ": "UTC"}}
	if _, err := f.client.Create(ctx, beta); err != nil {
		t.Fatal(err)
	}
	same, err := f.client.PutApp(ctx, "web", appSpec())
	if err != nil || same.Changed || len(same.Tenants) != 0 {
		t.Fatalf("an unchanged app = %+v, %v", same, err)
	}
	next := appSpec()
	next.Image = "ghcr.io/example/app:1.5"
	moved, err := f.client.PutApp(ctx, "web", next)
	if err != nil || !moved.Changed || len(moved.Tenants) != 2 || moved.Tenants[0].Spec.Image != pinnedTag("ghcr.io/example/app:1.5") || moved.Tenants[0].Generation != 2 {
		t.Fatalf("app upgrade = %+v, %v", moved, err)
	}
	if moved.Tenants[1].Spec.Env["TZ"] != "UTC" {
		t.Fatal("an upgrade dropped a tenant's override")
	}
	apps, err := f.client.Apps(ctx)
	if err != nil || len(apps) != 1 || strings.Join(apps[0].Tenants, ",") != "acme,beta" {
		t.Fatalf("apps = %+v, %v", apps, err)
	}
	checkStatus(t, "delete an app in use", f.client.DeleteApp(ctx, "web"), 409)
	if _, err := f.client.PutApp(ctx, "spare", appSpec()); err != nil {
		t.Fatal(err)
	}
	if err := f.client.DeleteApp(ctx, "spare"); err != nil {
		t.Fatal(err)
	}
	_, err = f.client.App(ctx, "spare")
	checkStatus(t, "a deleted app", err, 404)
}

func TestCreatePinsDefaultsAndHidesSecrets(t *testing.T) {
	f := withApp(t)
	ctx := context.Background()
	request := tenantOf("acme")
	request.Overrides.Image = "ghcr.io/example/app:canary"
	applied, err := f.client.Create(ctx, request)
	if err != nil {
		t.Fatal(err)
	}
	created := applied.Tenant
	if !applied.Changed || created.URL != "https://acme.example.test" || created.Address != "10.240.0.2:8080" || created.Generation != 1 || created.App != "web" {
		t.Fatalf("created = %+v", applied)
	}
	if created.Spec.Image != pinnedTag("ghcr.io/example/app:canary") || created.Overrides.Image != created.Spec.Image {
		t.Fatalf("stored image = %q, want the pinned canary", created.Spec.Image)
	}
	if created.Spec.Port != 8080 || created.Spec.DataPath != "/data" || created.Spec.Tmpfs["/tmp"] == "" || created.Spec.WritableRoot {
		t.Fatalf("defaults were not stored: %+v", created.Spec)
	}
	secret, err := f.client.Secret(ctx, "acme", "SESSION_SECRET")
	if err != nil || len(secret) != 64 {
		t.Fatalf("secret = %q, %v", secret, err)
	}
	listed, err := f.client.List(ctx)
	if err != nil {
		t.Fatal(err)
	}
	encoded, _ := json.Marshal(listed)
	if strings.Contains(string(encoded), secret) {
		t.Fatal("the tenant list exposes a secret value")
	}
	record, _ := f.store.Get("acme")
	if strings.Contains(record.Sealed["SESSION_SECRET"], secret) {
		t.Fatal("the store holds the secret in the clear")
	}
}

func TestErrorsMapToStatusCodes(t *testing.T) {
	f := withApp(t)
	ctx := context.Background()
	if _, err := f.client.Create(ctx, tenantOf("acme")); err != nil {
		t.Fatal(err)
	}
	_, err := f.client.Create(ctx, tenantOf("acme"))
	checkStatus(t, "duplicate", err, 409)
	_, err = f.client.Get(ctx, "nobody")
	checkStatus(t, "missing", err, 404)
	if !api.IsNotFound(err) {
		t.Error("IsNotFound missed a 404")
	}
	bad := tenantOf("bad")
	bad.Overrides.Env = map[string]string{"BAD-NAME": "x"}
	_, err = f.client.Create(ctx, bad)
	checkStatus(t, "invalid override", err, 400)
	_, err = f.client.Create(ctx, tenantOf("www"))
	checkStatus(t, "reserved name", err, 400)
	_, err = f.client.Create(ctx, api.TenantRequest{Name: "orphan", App: "nope"})
	checkStatus(t, "unknown app", err, 404)
	unpinnable := tenantOf("nopull")
	unpinnable.Overrides.Image = "ghcr.io/example/missing:1"
	_, err = f.client.Create(ctx, unpinnable)
	checkStatus(t, "image that cannot be pinned", err, 422)
	brokenAndUnpinnable := unpinnable
	brokenAndUnpinnable.Overrides.Env = map[string]string{"SESSION_SECRET": "clash"}
	_, err = f.client.Create(ctx, brokenAndUnpinnable)
	checkStatus(t, "an invalid tenant is refused before its image is pinned", err, 400)
	malformed := tenantOf("malformed")
	malformed.Overrides.Image = "Not An Image"
	_, err = f.client.Create(ctx, malformed)
	checkStatus(t, "malformed image", err, 400)
	_, err = f.client.Secret(ctx, "acme", "NOPE")
	checkStatus(t, "missing secret", err, 404)
	_, err = f.client.Configure(ctx, "other", api.TenantRequest{App: "web"})
	checkStatus(t, "configure missing", err, 404)
	huge := api.TenantRequest{App: "web", Overrides: spec.Overrides{Secrets: spec.Secrets{"HUGE": {Kind: "password", Bytes: 3_000_000}}}}
	_, err = f.client.Configure(ctx, "acme", huge)
	checkStatus(t, "oversized secret", err, 400)
	checkStatus(t, "purge a live tenant", f.client.Purge(ctx, "acme"), 409)
	_, err = f.client.Restore(ctx, "acme")
	checkStatus(t, "restore a live tenant", err, 409)
	checkStatus(t, "purge a name with nothing to purge", f.client.Purge(ctx, "acmee"), 404)
	_, err = f.client.Rotate(ctx, "acme", "NOPE")
	checkStatus(t, "rotate a secret outside the spec", err, 404)

	root := appSpec()
	root.User = "0:0"
	_, err = f.client.PutApp(ctx, "rooted", root)
	checkStatus(t, "root app", err, 400)
	named := appSpec()
	named.Name = "acme"
	_, err = f.client.PutApp(ctx, "named", named)
	checkStatus(t, "app spec naming a tenant", err, 400)
	_, err = f.client.PutApp(ctx, "Bad_Name", appSpec())
	checkStatus(t, "invalid app name", err, 400)
	missingImage := appSpec()
	missingImage.Image = "ghcr.io/example/missing:1"
	_, err = f.client.PutApp(ctx, "nopull", missingImage)
	checkStatus(t, "app image that cannot be pinned", err, 422)
	clash := appSpec()
	clash.Env = map[string]string{"SESSION_SECRET": "x"}
	_, err = f.client.PutApp(ctx, "web", clash)
	checkStatus(t, "app change that breaks a tenant", err, 400)
	checkStatus(t, "delete a missing app", f.client.DeleteApp(ctx, "nope"), 404)
	asleep := appSpec()
	asleep.Suspended = true
	_, err = f.client.PutApp(ctx, "asleep", asleep)
	checkStatus(t, "suspended app", err, 400)
	// A change that breaks a tenant is refused before its image is pulled.
	acmeSecret := api.TenantRequest{App: "web", Overrides: spec.Overrides{Secrets: spec.Secrets{"TOKEN": {}}}}
	if _, err := f.client.Configure(ctx, "acme", acmeSecret); err != nil {
		t.Fatal(err)
	}
	breaking := appSpec()
	breaking.Image = "ghcr.io/example/missing:2"
	breaking.Env = map[string]string{"TOKEN": "x"}
	_, err = f.client.PutApp(ctx, "web", breaking)
	checkStatus(t, "an app change that breaks a tenant, with an image that cannot be pulled", err, 400)
	// A change based on an outdated read is refused.
	current, _ := f.client.Get(ctx, "acme")
	if _, err := f.client.Configure(ctx, "acme", api.TenantRequest{App: "web", Generation: current.Generation}); err != nil {
		t.Fatal(err)
	}
	_, err = f.client.Configure(ctx, "acme", api.TenantRequest{App: "web", Overrides: acmeSecret.Overrides, Generation: current.Generation})
	checkStatus(t, "an outdated change", err, 409)
	_, err = f.client.Events(ctx, store.EventQuery{Limit: 5000})
	checkStatus(t, "event limit", err, 400)
}

// An empty tmpfs map means no tmpfs and must survive the trip to the agent.
func TestEmptyTmpfsIsKept(t *testing.T) {
	f := newAPIFixture(t)
	app := appSpec()
	app.WritableRoot = true
	app.Tmpfs = map[string]string{}
	created, err := f.client.PutApp(context.Background(), "web", app)
	if err != nil {
		t.Fatal(err)
	}
	if created.App.Spec.Tmpfs == nil || len(created.App.Spec.Tmpfs) != 0 {
		t.Fatalf("tmpfs = %v, want none", created.App.Spec.Tmpfs)
	}
}

func TestConfigureKeepsSecretsSuspensionAndSkipsNoOps(t *testing.T) {
	f := withApp(t)
	ctx := context.Background()
	if _, err := f.client.Create(ctx, tenantOf("acme")); err != nil {
		t.Fatal(err)
	}
	before, _ := f.client.Secret(ctx, "acme", "SESSION_SECRET")
	same, err := f.client.Configure(ctx, "acme", api.TenantRequest{App: "web"})
	if err != nil || same.Changed || same.Tenant.Generation != 1 {
		t.Fatalf("an unchanged tenant = %+v, %v", same, err)
	}
	if _, err := f.client.Suspend(ctx, "acme", true); err != nil {
		t.Fatal(err)
	}
	next := api.TenantRequest{App: "web", Overrides: spec.Overrides{Env: map[string]string{"MODE": "prod"}, Secrets: spec.Secrets{"ADMIN_PASSWORD": {Kind: "password"}}}}
	replaced, err := f.client.Configure(ctx, "acme", next)
	if err != nil || !replaced.Changed || replaced.Tenant.Spec.Env["MODE"] != "prod" {
		t.Fatalf("configure = %+v, %v", replaced, err)
	}
	if !replaced.Tenant.Spec.Suspended {
		t.Fatal("configuring a tenant resumed it")
	}
	if after, _ := f.client.Secret(ctx, "acme", "SESSION_SECRET"); after != before {
		t.Fatal("configure regenerated an existing secret")
	}
	password, err := f.client.Secret(ctx, "acme", "ADMIN_PASSWORD")
	if err != nil || len(password) != 20 {
		t.Fatalf("new secret = %q, %v", password, err)
	}
	// A secret removed and added back keeps its value.
	if _, err := f.client.Configure(ctx, "acme", api.TenantRequest{App: "web"}); err != nil {
		t.Fatal(err)
	}
	if _, err := f.client.Configure(ctx, "acme", next); err != nil {
		t.Fatal(err)
	}
	if again, err := f.client.Secret(ctx, "acme", "ADMIN_PASSWORD"); err != nil || again != password {
		t.Fatalf("a secret removed and added back changed its value: %v", err)
	}
	rotated, err := f.client.Rotate(ctx, "acme", "ADMIN_PASSWORD")
	if err != nil || rotated.Generation <= replaced.Tenant.Generation {
		t.Fatalf("rotate = %+v, %v", rotated, err)
	}
	if fresh, _ := f.client.Secret(ctx, "acme", "ADMIN_PASSWORD"); fresh == password || len(fresh) != 20 {
		t.Fatal("rotate kept the old value")
	}
}

func TestArchiveRestorePurge(t *testing.T) {
	f := withApp(t)
	ctx := context.Background()
	if _, err := f.client.Create(ctx, tenantOf("acme")); err != nil {
		t.Fatal(err)
	}
	_ = f.client.Reconcile(ctx)
	if err := os.WriteFile(filepath.Join(f.dataRoot, "acme", "data.db"), []byte("customer data"), 0o600); err != nil {
		t.Fatal(err)
	}
	before, _ := f.client.Secret(ctx, "acme", "SESSION_SECRET")
	if err := f.client.Archive(ctx, "acme"); err != nil {
		t.Fatal(err)
	}
	if routes := (Routes{Store: f.store}); func() bool { _, ok := routes.Route("acme"); return ok }() {
		t.Fatal("an archived tenant still has a route")
	}
	list, err := f.client.List(ctx)
	if err != nil || len(list) != 1 || list[0].Spec.Name != "acme" || list[0].App != "web" || list[0].ArchivedAt.IsZero() || list[0].Address != "" {
		t.Fatalf("list = %+v, %v", list, err)
	}
	archived := list[0]
	if shown, err := f.client.Get(ctx, "acme"); err != nil || shown.ArchivedAt.IsZero() {
		t.Fatalf("show an archived tenant = %+v, %v", shown, err)
	}
	// What acts on a running tenant refuses an archived one.
	_, suspendErr := f.client.Suspend(ctx, "acme", true)
	_, secretErr := f.client.Secret(ctx, "acme", "SESSION_SECRET")
	_, logsErr := f.client.Logs(ctx, "acme", 10)
	for what, err := range map[string]error{"suspend": suspendErr, "secret": secretErr, "logs": logsErr} {
		if apiErr, ok := err.(*api.Error); !ok || apiErr.Status != 409 || !strings.Contains(apiErr.Message, "archived") {
			t.Fatalf("%s an archived tenant = %v", what, err)
		}
	}
	_, err = f.client.Create(ctx, tenantOf("acme"))
	if apiErr, ok := err.(*api.Error); !ok || apiErr.Status != 409 || !strings.Contains(apiErr.Message, "archived") {
		t.Fatalf("create over an archived name = %v", err)
	}
	// An archived tenant can be reconfigured; restore runs the result.
	archivedConfig := api.TenantRequest{App: "web", Overrides: spec.Overrides{Env: map[string]string{"TZ": "UTC"}}, Generation: archived.Generation}
	if changed, err := f.client.Configure(ctx, "acme", archivedConfig); err != nil || !changed.Changed || changed.Tenant.ArchivedAt.IsZero() {
		t.Fatalf("configure an archived tenant = %+v, %v", changed, err)
	}
	restored, err := f.client.Restore(ctx, "acme")
	if err != nil || !restored.ArchivedAt.IsZero() || restored.Spec.Suspended || restored.Spec.Env["TZ"] != "UTC" {
		t.Fatalf("restore = %+v, %v", restored, err)
	}
	if after, _ := f.client.Secret(ctx, "acme", "SESSION_SECRET"); after != before {
		t.Fatal("restore did not bring back the tenant's secrets")
	}
	if data, err := os.ReadFile(filepath.Join(f.dataRoot, "acme", "data.db")); err != nil || string(data) != "customer data" {
		t.Fatalf("data after restore = %q, %v", data, err)
	}
	if err := f.client.Archive(ctx, "acme"); err != nil {
		t.Fatal(err)
	}
	if err := f.client.Purge(ctx, "acme"); err != nil {
		t.Fatal(err)
	}
	if _, err := os.Stat(filepath.Join(f.dataRoot, "acme")); !os.IsNotExist(err) {
		t.Fatal("purge left the data")
	}
	if _, err := f.client.Get(ctx, "acme"); !api.IsNotFound(err) {
		t.Fatalf("purge left the tenant: %v", err)
	}
	fresh, err := f.client.Create(ctx, tenantOf("acme"))
	if err != nil {
		t.Fatalf("create after purge = %v", err)
	}
	if after, _ := f.client.Secret(ctx, "acme", "SESSION_SECRET"); after == before {
		t.Fatal("a fresh tenant reused a purged tenant's secret")
	}
	if record, _ := f.store.Get(fresh.Tenant.Spec.Name); record.DataImage != "" {
		t.Fatalf("a fresh tenant inherited the data image %q", record.DataImage)
	}
	events, err := f.client.Events(ctx, store.EventQuery{Tenant: "acme"})
	if err != nil {
		t.Fatal(err)
	}
	var actions []string
	for _, e := range events {
		actions = append(actions, e.Action)
	}
	if strings.Join(actions, ",") != "tenant.created,tenant.purged,tenant.archived,tenant.restored,tenant.updated,tenant.archived,tenant.created" {
		t.Fatalf("events = %v", actions)
	}
}

// Data no archive entry accounts for blocks a create until it is purged.
func TestLeftoverDataBlocksCreateUntilPurged(t *testing.T) {
	f := withApp(t)
	ctx := context.Background()
	if err := os.MkdirAll(filepath.Join(f.dataRoot, "acme"), 0o700); err != nil {
		t.Fatal(err)
	}
	if _, err := f.client.Create(ctx, tenantOf("acme")); err == nil || !strings.Contains(err.Error(), "purge it first") {
		t.Fatalf("create over leftover data = %v", err)
	}
	if err := f.client.Purge(ctx, "acme"); err != nil {
		t.Fatal(err)
	}
	if _, err := f.client.Create(ctx, tenantOf("acme")); err != nil {
		t.Fatal(err)
	}
}

func TestSuspendLogsAndRoutes(t *testing.T) {
	f := withApp(t)
	ctx := context.Background()
	if _, err := f.client.Create(ctx, tenantOf("acme")); err != nil {
		t.Fatal(err)
	}
	if suspended, err := f.client.Suspend(ctx, "acme", true); err != nil || !suspended.Spec.Suspended {
		t.Fatalf("suspend = %+v, %v", suspended, err)
	}
	if resumed, err := f.client.Suspend(ctx, "acme", false); err != nil || resumed.Spec.Suspended {
		t.Fatalf("resume = %+v, %v", resumed, err)
	}
	// Reconcile reports the unreachable health endpoint but still records the
	// container, which logs needs.
	_ = f.client.Reconcile(ctx)
	if logs, err := f.client.Logs(ctx, "acme", 10); err != nil || string(logs) != "logs of c-acme\n" {
		t.Fatalf("logs = %q, %v", logs, err)
	}
	route, ok := (Routes{Store: f.store}).Route("acme")
	if !ok || route.Backend != "10.240.0.2:8080" || route.Suspended {
		t.Fatalf("route = %+v, %v", route, ok)
	}
	if _, ok := (Routes{Store: f.store}).Route("nobody"); ok {
		t.Fatal("route for a missing tenant")
	}

	// A tenant another node runs is shown, but neither routed nor read here.
	if err := f.store.PutNode(store.Node{Name: "other-node", Address: "10.0.0.2:7400"}, "test"); err != nil {
		t.Fatal(err)
	}
	if _, err := f.store.Create(store.NewTenant{Name: "away", App: "web", Node: "other-node"}, "test"); err != nil {
		t.Fatal(err)
	}
	if _, ok := (Routes{Store: f.store}).Route("away"); ok {
		t.Fatal("outside a cluster's relay, this node routes a tenant another node runs")
	}
	if route, ok := (Routes{Store: f.store, RelayPort: 7402}).Route("away"); !ok || route.Relay != "10.0.0.2:7402" || route.Backend != "" {
		t.Fatalf("route to a tenant of another node = %+v, %v", route, ok)
	}
	if away, err := f.client.Get(ctx, "away"); err != nil || away.Node != "other-node" || !away.Elsewhere || away.Status != nil || away.Address != "" {
		t.Fatalf("away = %+v, %v", away, err)
	}
	if _, err := f.client.Logs(ctx, "away", 10); err == nil || !strings.Contains(err.Error(), "runs on node other-node") {
		t.Fatalf("logs of a tenant elsewhere: %v", err)
	}
}

// statusCluster answers for the tenants of other nodes as those nodes
// would; a node missing from statuses does not answer.
type statusCluster struct {
	Cluster
	statuses map[string]reconcile.Status
}

func (c statusCluster) StatusOn(ctx context.Context, node, tenant string, out any) error {
	status, ok := c.statuses[node+"/"+tenant]
	if !ok {
		<-ctx.Done()
		return ctx.Err()
	}
	*out.(*reconcile.Status) = status
	return nil
}

// Waiting for a tenant, or listing them, works from any node of a cluster:
// the status of a tenant another node runs is asked of that node, and a
// node that does not answer leaves it unknown without holding the answer.
func TestStatusOfTenantsOfOtherNodes(t *testing.T) {
	f := newAPIFixture(t)
	ctx := context.Background()
	if _, err := f.client.PutApp(ctx, "web", appSpec()); err != nil {
		t.Fatal(err)
	}
	for i, node := range []string{"n2", "n3"} {
		if err := f.store.PutNode(store.Node{Name: node, Address: fmt.Sprintf("10.0.0.%d:7400", i+2)}, "test"); err != nil {
			t.Fatal(err)
		}
		if _, err := f.store.Create(store.NewTenant{Name: "on-" + node, App: "web", Node: node}, "test"); err != nil {
			t.Fatal(err)
		}
	}
	healthy := reconcile.Status{Phase: reconcile.PhaseRunning, Healthy: true, Generation: 1}
	f.server.WithCluster(statusCluster{statuses: map[string]reconcile.Status{"n2/on-n2": healthy}})

	tenant, err := f.client.Get(ctx, "on-n2")
	if err != nil || tenant.Status == nil || !tenant.Status.Healthy || tenant.Status.Phase != reconcile.PhaseRunning {
		t.Fatalf("on-n2 = %+v, %v", tenant, err)
	}
	// Its copy, and whether it can take over.
	if _, err := f.store.PlaceReplicas("test"); err != nil {
		t.Fatal(err)
	}
	if tenant, err = f.client.Get(ctx, "on-n2"); err != nil || tenant.Replica == "" || tenant.ReplicaReady {
		t.Fatalf("on-n2 with a replica = %+v, %v", tenant, err)
	}
	if err := f.store.ReplicaReady("on-n2", tenant.Replica); err != nil {
		t.Fatal(err)
	}
	if tenant, err = f.client.Get(ctx, "on-n2"); err != nil || !tenant.ReplicaReady {
		t.Fatalf("on-n2 with a ready replica = %+v, %v", tenant, err)
	}
	start := time.Now()
	tenants, err := f.client.List(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if took := time.Since(start); took > remoteStatusTimeout+time.Second {
		t.Errorf("listing waited %s for a node that does not answer", took)
	}
	for _, tenant := range tenants {
		switch tenant.Spec.Name {
		case "on-n2":
			if tenant.Status == nil || !tenant.Status.Healthy {
				t.Errorf("listed on-n2 = %+v", tenant)
			}
		case "on-n3":
			if tenant.Status != nil {
				t.Errorf("a node that does not answer has a status: %+v", tenant.Status)
			}
		}
	}
}

// Over the unix socket the audit log names the user at the other end, and
// the person a client says it acts for next to it.
func TestEventsNameTheSocketUser(t *testing.T) {
	f := newAPIFixture(t)
	socket := filepath.Join(t.TempDir(), "agent.sock")
	listener, err := net.Listen("unix", socket)
	if err != nil {
		t.Fatal(err)
	}
	server := &http.Server{Handler: f.handler, ConnContext: ConnContext}
	go server.Serve(listener)
	t.Cleanup(func() { _ = server.Close() })
	me, err := user.Current()
	if err != nil {
		t.Fatal(err)
	}
	client := api.NewClient(socket)
	ctx := context.Background()
	if _, err := client.PutApp(ctx, "web", appSpec()); err != nil {
		t.Fatal(err)
	}
	client.Actor = "  alice "
	if _, err := client.Create(ctx, tenantOf("acme")); err != nil {
		t.Fatal(err)
	}
	events, err := client.Events(ctx, store.EventQuery{Limit: 2})
	if err != nil || len(events) != 2 {
		t.Fatalf("events = %+v, %v", events, err)
	}
	if events[0].Actor != "alice via "+me.Username || events[0].Action != "tenant.created" {
		t.Errorf("newest event = %+v", events[0])
	}
	if events[1].Actor != me.Username || events[1].Action != "app.created" || events[1].Detail != "image "+pinnedTag("ghcr.io/example/app:1.4")+" of ghcr.io/example/app:1.4" {
		t.Errorf("first event = %+v", events[1])
	}
	if cleanActor("bo\x01b\u200b") != "bob" || len(cleanActor(strings.Repeat("x", 100))) != 64 {
		t.Error("cleanActor keeps unprintable or overlong names")
	}
	// Over TCP nobody is known.
	if _, err := f.client.Suspend(ctx, "acme", true); err != nil {
		t.Fatal(err)
	}
	if latest, _ := client.Events(ctx, store.EventQuery{Limit: 1}); latest[0].Actor != "unknown" {
		t.Errorf("TCP event = %+v", latest[0])
	}
}

// pingCluster answers pings from the nodes listed.
type pingCluster struct {
	Cluster
	up     map[string]bool
	leader string
}

func (c pingCluster) Ping(_ context.Context, node string) error {
	if c.up[node] {
		return nil
	}
	return errors.New("no answer")
}

func (c pingCluster) Status() (cluster.Status, error) {
	return cluster.Status{Leader: c.leader}, nil
}

// The panel's view of the nodes: each with whether it answers now.
func TestNodesSayWhichAnswer(t *testing.T) {
	f := newAPIFixture(t)
	ctx := context.Background()
	// A node in no cluster sees itself alone.
	view, err := f.client.Nodes(ctx)
	if err != nil || len(view.Nodes) != 1 || view.Nodes[0].Name != view.Self || !view.Nodes[0].Answers {
		t.Fatalf("alone: %+v, %v", view, err)
	}
	self := view.Self
	for i, node := range []string{self, "n2", "n3"} {
		if err := f.store.PutNode(store.Node{Name: node, Address: fmt.Sprintf("10.0.0.%d:7400", i+1)}, "test"); err != nil {
			t.Fatal(err)
		}
	}
	f.server.WithCluster(pingCluster{up: map[string]bool{"n2": true}, leader: "n2"})
	view, err = f.client.Nodes(ctx)
	if err != nil || view.Leader != "n2" || len(view.Nodes) != 3 {
		t.Fatalf("view: %+v, %v", view, err)
	}
	for _, node := range view.Nodes {
		if want := node.Name != "n3"; node.Answers != want {
			t.Errorf("node %s answers %v, want %v", node.Name, node.Answers, want)
		}
	}
}

// catchUpCluster cannot catch up, as a node cut off from the leader.
type catchUpCluster struct {
	Cluster
	err error
}

func (c catchUpCluster) CatchUp() error { return c.err }

// The devices are listed as the cluster has them: a node that cannot
// catch up says so, and its panel then lets no device in.
func TestDevicesAreReadCaughtUp(t *testing.T) {
	f := newAPIFixture(t)
	ctx := context.Background()
	if err := f.store.PutNode(store.Node{Name: f.store.Node(), Address: "10.0.0.1:7400"}, "test"); err != nil {
		t.Fatal(err)
	}
	if err := f.store.NameCluster("TESTCLUSTER", "test"); err != nil {
		t.Fatal(err)
	}
	f.server.WithCluster(catchUpCluster{err: errors.New("no leader")})
	if _, err := f.client.Devices(ctx); err == nil {
		t.Fatal("devices listed by a node that cannot catch up")
	}
	f.server.WithCluster(catchUpCluster{})
	if devices, err := f.client.Devices(ctx); err != nil || len(devices) != 0 {
		t.Fatalf("devices %+v, %v", devices, err)
	}
}

// The panel's people through the agent's API: an invitation, a passkey
// added with it, a session, and the secret every panel shares.
func TestThePanelsPeopleThroughTheAPI(t *testing.T) {
	f := newAPIFixture(t)
	ctx := context.Background()
	hash := strings.Repeat("ab", 32)
	user, err := f.client.InvitePanelUser(ctx, "jan", api.InviteRequest{Hash: hash, Expires: time.Now().Add(time.Hour)})
	if err != nil || user.Name != "jan" {
		t.Fatalf("invite: %+v, %v", user, err)
	}
	if e, err := f.client.Enrollment(ctx, hash); err != nil || e.User != "jan" {
		t.Fatalf("enrollment %+v, %v", e, err)
	}
	if err := f.client.AddPasskey(ctx, api.PasskeyRequest{Hash: hash, Passkey: store.Passkey{ID: "k1", User: "jan", Data: "{}", Label: "laptop"}}); err != nil {
		t.Fatal(err)
	}
	if _, err := f.client.Enrollment(ctx, hash); !api.IsNotFound(err) {
		t.Fatalf("a used invitation: %v", err)
	}
	if u, err := f.client.PanelUserByHandle(ctx, user.Handle); err != nil || len(u.Passkeys) != 1 {
		t.Fatalf("by handle: %+v, %v", u, err)
	}
	session := strings.Repeat("cd", 32)
	if err := f.client.StartPanelSession(ctx, api.SessionRequest{Hash: session, User: "jan", Passkey: "k1", Challenge: "c1", ExpiresAt: time.Now().Add(time.Hour)}); err != nil {
		t.Fatal(err)
	}
	if s, err := f.client.PanelSession(ctx, session, true); err != nil || s.User != "jan" {
		t.Fatalf("session %+v, %v", s, err)
	}
	secret, err := f.client.PanelSecret(ctx, "ceremonies")
	if err != nil || len(secret) != 64 {
		t.Fatalf("secret %q, %v", secret, err)
	}
	if err := f.client.WithActor("anna (panel)").RemovePanelUser(ctx, "jan"); err != nil {
		t.Fatal(err)
	}
	if _, err := f.client.PanelSession(ctx, session, false); !api.IsNotFound(err) {
		t.Fatalf("a removed person's session: %v", err)
	}
	events, _ := f.client.Events(ctx, store.EventQuery{Limit: 1})
	if len(events) != 1 || !strings.HasPrefix(events[0].Actor, "anna (panel)") {
		t.Fatalf("the removal's actor: %+v", events)
	}
}

// A conditional store of an app is refused, with a 409, when the app is
// not as the client read it.
func TestStoringAnAppOnlyAsRead(t *testing.T) {
	f := newAPIFixture(t)
	ctx := context.Background()
	if _, err := f.client.PutAppIf(ctx, "web", appSpec(), store.AppIf{Absent: true}); err != nil {
		t.Fatal(err)
	}
	_, err := f.client.PutAppIf(ctx, "web", appSpec(), store.AppIf{Absent: true})
	checkStatus(t, "create over an app", err, 409)
	read, err := f.client.App(ctx, "web")
	if err != nil {
		t.Fatal(err)
	}
	next := appSpec()
	next.HealthPath = "/ready"
	_, err = f.client.PutAppIf(ctx, "web", next, store.AppIf{UpdatedAt: read.UpdatedAt.Add(-time.Millisecond)})
	checkStatus(t, "store over a newer app", err, 409)
	if changed, err := f.client.PutAppIf(ctx, "web", next, store.AppIf{UpdatedAt: read.UpdatedAt}); err != nil || !changed.Changed {
		t.Fatalf("store over the app as read = %+v, %v", changed, err)
	}
	recorder := httptest.NewRecorder()
	f.handler.ServeHTTP(recorder, httptest.NewRequest(http.MethodPut, "/v1/apps/web?if=yesterday", strings.NewReader(`{"image":"x","user":"1:1"}`)))
	if recorder.Code != http.StatusBadRequest || !strings.Contains(recorder.Body.String(), "if must be") {
		t.Fatalf("a bad if: %d %s", recorder.Code, recorder.Body)
	}
}

// createCluster creates tenants on other nodes as they would, refusing
// those whose name has data left there.
type createCluster struct {
	Cluster
	store store.Store
	data  map[string]bool
}

func (c createCluster) CreateOn(_ context.Context, node string, tenant store.NewTenant, actor string) (store.Record, error) {
	if c.data[node+"/"+tenant.Name] {
		return store.Record{}, &cluster.PeerError{Status: http.StatusConflict, Message: "data of an earlier tenant " + tenant.Name + " is on node " + node + " but not in the archive"}
	}
	return c.store.Create(tenant, actor)
}

func TestNewTenantsGoWhereTheyAreAskedTo(t *testing.T) {
	f := newAPIFixture(t)
	ctx := context.Background()
	if _, err := f.client.PutApp(ctx, "web", appSpec()); err != nil {
		t.Fatal(err)
	}
	self := f.store.Node()
	for i, node := range []string{self, "n2", "n3"} {
		if err := f.store.PutNode(store.Node{Name: node, Address: fmt.Sprintf("10.0.0.%d:7400", i+1)}, "test"); err != nil {
			t.Fatal(err)
		}
	}
	f.server.WithCluster(createCluster{store: f.store, data: map[string]bool{"n3/stale": true}})
	create := func(name, node string) (api.Applied, error) {
		return f.client.Create(ctx, api.TenantRequest{Name: name, App: "web", Node: node})
	}
	// Here by default; auto spreads them, this node first among equals.
	for _, c := range []struct{ name, node, want string }{
		{"a", "", self}, {"b", store.Auto, "n2"}, {"c", store.Auto, "n3"}, {"d", store.Auto, self}, {"e", "n2", "n2"},
	} {
		applied, err := create(c.name, c.node)
		if err != nil || applied.Tenant.Node != c.want {
			t.Fatalf("%s on %q: %+v, %v", c.name, c.node, applied.Tenant.Node, err)
		}
	}
	if _, err := create("x", "nowhere"); err == nil || !strings.Contains(err.Error(), "not in the cluster") {
		t.Fatalf("an unknown node: %v", err)
	}
	// What a failure left on the node it would go to stops it.
	if _, err := create("stale", "n3"); err == nil || !strings.Contains(err.Error(), "on node n3 but not in the archive") {
		t.Fatalf("leftover data elsewhere: %v", err)
	}
	if _, err := f.store.Get("stale"); err == nil {
		t.Fatal("the tenant was created anyway")
	}
	// The plan to balance them: none needed now.
	moves, err := f.client.BalanceMoves(ctx)
	if err != nil || len(moves) != 0 {
		t.Fatalf("moves %+v, %v", moves, err)
	}
}

// Creates at once through one node spread over the nodes.
func TestNewTenantsAtOnceSpread(t *testing.T) {
	f := newAPIFixture(t)
	ctx := context.Background()
	if _, err := f.client.PutApp(ctx, "web", appSpec()); err != nil {
		t.Fatal(err)
	}
	self := f.store.Node()
	for i, node := range []string{self, "n2", "n3"} {
		if err := f.store.PutNode(store.Node{Name: node, Address: fmt.Sprintf("10.0.0.%d:7400", i+1)}, "test"); err != nil {
			t.Fatal(err)
		}
	}
	f.server.WithCluster(createCluster{store: f.store})
	var wg sync.WaitGroup
	for i := range 9 {
		wg.Go(func() {
			if _, err := f.client.Create(ctx, api.TenantRequest{Name: fmt.Sprintf("t%d", i), App: "web", Node: store.Auto}); err != nil {
				t.Error(err)
			}
		})
	}
	wg.Wait()
	counts := map[string]int{}
	for _, record := range f.store.List() {
		counts[record.Node]++
	}
	if counts[self] != 3 || counts["n2"] != 3 || counts["n3"] != 3 {
		t.Fatalf("placed %v", counts)
	}
}
