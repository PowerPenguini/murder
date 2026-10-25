package api

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net/http/httptest"
	"net/netip"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/PowerPenguini/corral/internal/reconcile"
	"github.com/PowerPenguini/corral/internal/runtime"
	"github.com/PowerPenguini/corral/internal/secrets"
	"github.com/PowerPenguini/corral/internal/spec"
	"github.com/PowerPenguini/corral/internal/store"
	"github.com/PowerPenguini/corral/internal/volume"
)

// memoryRuntime runs nothing; it only remembers containers.
type memoryRuntime struct {
	mu         sync.Mutex
	containers []runtime.Container
}

func (m *memoryRuntime) HasImage(context.Context, string) (bool, error) { return true, nil }
func (m *memoryRuntime) PullImage(context.Context, string) error        { return nil }
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
func (m *memoryRuntime) Pid(context.Context, string) (int, error) { return 1, nil }

// Resolve pins tags to a digest derived from the tag; "missing" tags fail.
func (m *memoryRuntime) Resolve(_ context.Context, ref string) (string, string, error) {
	if spec.Pinned(ref) {
		return ref, "", nil
	}
	if strings.Contains(ref, "missing") {
		return "", "", errors.New("manifest unknown")
	}
	return pinnedTag(ref), "", nil
}
func (m *memoryRuntime) Logs(_ context.Context, id string, tail int) ([]byte, error) {
	return []byte("logs of " + id + "\n"), nil
}

type apiFixture struct {
	client     *Client
	store      *store.File
	reconciler *reconcile.Reconciler
	dataRoot   string
}

func newAPIFixture(t *testing.T) *apiFixture {
	t.Helper()
	statePath := filepath.Join(t.TempDir(), "state.json")
	if err := store.InitFile(statePath, "test-node"); err != nil {
		t.Fatal(err)
	}
	st, err := store.OpenFile(statePath, store.Options{PortFrom: 20000, PortTo: 20100, NetworkPool: netip.MustParsePrefix("10.240.0.0/16")})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = st.Close() })
	keyPath := filepath.Join(t.TempDir(), "master.key")
	if err := secrets.CreateKey(keyPath); err != nil {
		t.Fatal(err)
	}
	key, _ := secrets.LoadKey(keyPath)
	rt := &memoryRuntime{}
	dataRoot := filepath.Join(t.TempDir(), "tenants")
	log := slog.New(slog.NewTextHandler(io.Discard, nil))
	reconciler := reconcile.New(st, rt, volume.Dir{Root: dataRoot}, key, reconcile.Config{BaseDomain: "example.test", HealthTimeout: 100 * time.Millisecond}, log)
	server := httptest.NewServer(NewServer(st, reconciler, rt, key, log).Handler())
	t.Cleanup(server.Close)
	return &apiFixture{client: NewClientURL(server.URL), store: st, reconciler: reconciler, dataRoot: dataRoot}
}

func appSpec(name string) spec.Tenant {
	return spec.Tenant{
		Name: name, Image: "ghcr.io/example/app:1.4", HealthPath: "/health",
		User:    fmt.Sprintf("%d:%d", os.Getuid(), os.Getgid()),
		Secrets: spec.Secrets{"SESSION_SECRET": {}},
	}
}

// pinnedTag is how memoryRuntime pins a tag.
func pinnedTag(ref string) string {
	repository := ref[:strings.LastIndex(ref, ":")]
	sum := sha256.Sum256([]byte(ref))
	return repository + "@sha256:" + hex.EncodeToString(sum[:])
}

func TestCreatePinsDefaultsAndHidesSecrets(t *testing.T) {
	f := newAPIFixture(t)
	ctx := context.Background()
	applied, err := f.client.Create(ctx, appSpec("acme"))
	if err != nil {
		t.Fatal(err)
	}
	created := applied.Tenant
	if !applied.Changed || created.URL != "https://acme.example.test" || created.HostPort != 20000 || created.Generation != 1 {
		t.Fatalf("created = %+v", applied)
	}
	if created.Spec.Image != pinnedTag("ghcr.io/example/app:1.4") {
		t.Fatalf("stored image = %q, want the pinned digest", created.Spec.Image)
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
	f := newAPIFixture(t)
	ctx := context.Background()
	if _, err := f.client.Create(ctx, appSpec("acme")); err != nil {
		t.Fatal(err)
	}
	check := func(name string, err error, status int) {
		t.Helper()
		apiErr, ok := err.(*Error)
		if !ok || apiErr.Status != status {
			t.Errorf("%s: error %v, want HTTP %d", name, err, status)
		}
	}
	_, err := f.client.Create(ctx, appSpec("acme"))
	check("duplicate", err, 409)
	if err != nil && !strings.Contains(err.Error(), "already exists") {
		t.Errorf("duplicate create explains %q", err)
	}
	_, err = f.client.Get(ctx, "nobody")
	check("missing", err, 404)
	if !IsNotFound(err) {
		t.Error("IsNotFound missed a 404")
	}
	bad := appSpec("bad")
	bad.Port = 70000
	_, err = f.client.Create(ctx, bad)
	check("invalid spec", err, 400)
	_, err = f.client.Create(ctx, appSpec("www"))
	check("reserved name", err, 400)
	unpinnable := appSpec("nopull")
	unpinnable.Image = "ghcr.io/example/missing:1"
	_, err = f.client.Create(ctx, unpinnable)
	check("image that cannot be pinned", err, 422)
	malformed := appSpec("malformed")
	malformed.Image = "Not An Image"
	_, err = f.client.Create(ctx, malformed)
	check("malformed image", err, 400)
	_, err = f.client.Secret(ctx, "acme", "NOPE")
	check("missing secret", err, 404)
	_, err = f.client.Replace(ctx, appSpec("other"))
	check("replace missing", err, 404)
	huge := appSpec("acme")
	huge.Secrets["HUGE"] = spec.Secret{Kind: "password", Bytes: 3_000_000}
	_, err = f.client.Replace(ctx, huge)
	check("oversized secret", err, 400)
	root := appSpec("acme")
	root.User = "0:0"
	_, err = f.client.Replace(ctx, root)
	check("root user", err, 400)
	err = f.client.Purge(ctx, "acme")
	check("purge a live tenant", err, 409)
	_, err = f.client.Restore(ctx, "acme")
	check("restore a tenant that is not archived", err, 404)
	err = f.client.Purge(ctx, "acmee")
	check("purge a name with nothing to purge", err, 404)
	brokenAndUnpinnable := appSpec("broken")
	brokenAndUnpinnable.Image = "ghcr.io/example/missing:1"
	brokenAndUnpinnable.User = "0:0"
	_, err = f.client.Create(ctx, brokenAndUnpinnable)
	check("an invalid spec is refused before its image is pinned", err, 400)
	resolved, err := f.client.Resolve(ctx, "ghcr.io/example/app:1.4")
	if err != nil || resolved.Image != pinnedTag("ghcr.io/example/app:1.4") {
		t.Errorf("resolve = %+v, %v", resolved, err)
	}
	_, err = f.client.Resolve(ctx, "ghcr.io/example/missing:1")
	check("resolve an image that cannot be pinned", err, 422)
	_, err = f.client.Resolve(ctx, "Not An Image")
	check("resolve a malformed image", err, 400)
}

// An empty tmpfs map means no tmpfs and must survive the trip to the agent.
func TestEmptyTmpfsIsKept(t *testing.T) {
	f := newAPIFixture(t)
	tenant := appSpec("acme")
	tenant.WritableRoot = true
	tenant.Tmpfs = map[string]string{}
	created, err := f.client.Create(context.Background(), tenant)
	if err != nil {
		t.Fatal(err)
	}
	if created.Tenant.Spec.Tmpfs == nil || len(created.Tenant.Spec.Tmpfs) != 0 {
		t.Fatalf("tmpfs = %v, want none", created.Tenant.Spec.Tmpfs)
	}
}

func TestReplaceKeepsSecretsSuspensionAndSkipsNoOps(t *testing.T) {
	f := newAPIFixture(t)
	ctx := context.Background()
	if _, err := f.client.Create(ctx, appSpec("acme")); err != nil {
		t.Fatal(err)
	}
	before, _ := f.client.Secret(ctx, "acme", "SESSION_SECRET")
	same, err := f.client.Replace(ctx, appSpec("acme"))
	if err != nil || same.Changed || same.Tenant.Generation != 1 {
		t.Fatalf("an unchanged spec = %+v, %v", same, err)
	}
	if _, err := f.client.Suspend(ctx, "acme", true); err != nil {
		t.Fatal(err)
	}
	next := appSpec("acme")
	next.Secrets["ADMIN_PASSWORD"] = spec.Secret{Kind: "password"}
	next.Env = map[string]string{"MODE": "prod"}
	replaced, err := f.client.Replace(ctx, next)
	if err != nil || !replaced.Changed || replaced.Tenant.Spec.Env["MODE"] != "prod" {
		t.Fatalf("replace = %+v, %v", replaced, err)
	}
	if !replaced.Tenant.Spec.Suspended {
		t.Fatal("applying an app spec resumed a suspended tenant")
	}
	if after, _ := f.client.Secret(ctx, "acme", "SESSION_SECRET"); after != before {
		t.Fatal("replace regenerated an existing secret")
	}
	if password, err := f.client.Secret(ctx, "acme", "ADMIN_PASSWORD"); err != nil || len(password) != 20 {
		t.Fatalf("new secret = %q, %v", password, err)
	}
	// A secret removed from the spec keeps its value, so a mistaken edit
	// that is reverted gets the same value back.
	password, _ := f.client.Secret(ctx, "acme", "ADMIN_PASSWORD")
	delete(next.Secrets, "ADMIN_PASSWORD")
	if _, err := f.client.Replace(ctx, next); err != nil {
		t.Fatal(err)
	}
	next.Secrets["ADMIN_PASSWORD"] = spec.Secret{Kind: "password"}
	if _, err := f.client.Replace(ctx, next); err != nil {
		t.Fatal(err)
	}
	if again, err := f.client.Secret(ctx, "acme", "ADMIN_PASSWORD"); err != nil || again != password {
		t.Fatalf("a secret removed and added back changed its value: %v", err)
	}
	// Rotation is the way to a new value, and it rolls the container.
	rotated, err := f.client.Rotate(ctx, "acme", "ADMIN_PASSWORD")
	if err != nil || rotated.Generation <= replaced.Tenant.Generation {
		t.Fatalf("rotate = %+v, %v", rotated, err)
	}
	if fresh, _ := f.client.Secret(ctx, "acme", "ADMIN_PASSWORD"); fresh == password || len(fresh) != 20 {
		t.Fatal("rotate kept the old value")
	}
	if _, err := f.client.Rotate(ctx, "acme", "NOT_IN_SPEC"); !IsNotFound(err) {
		t.Fatalf("rotating a secret outside the spec = %v", err)
	}
	// A new tag means a new pinned image.
	upgraded := appSpec("acme")
	upgraded.Image = "ghcr.io/example/app:1.5"
	moved, err := f.client.Replace(ctx, upgraded)
	if err != nil || !moved.Changed || moved.Tenant.Spec.Image != pinnedTag("ghcr.io/example/app:1.5") {
		t.Fatalf("tag change = %+v, %v", moved, err)
	}
}

func TestArchiveRestorePurge(t *testing.T) {
	f := newAPIFixture(t)
	ctx := context.Background()
	if _, err := f.client.Create(ctx, appSpec("acme")); err != nil {
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
	archived, err := f.client.ListArchived(ctx)
	if err != nil || len(archived) != 1 || archived[0].Name != "acme" {
		t.Fatalf("archive = %+v, %v", archived, err)
	}
	_, err = f.client.Create(ctx, appSpec("acme"))
	if apiErr, ok := err.(*Error); !ok || apiErr.Status != 409 || !strings.Contains(apiErr.Message, "archived") {
		t.Fatalf("create over an archived name = %v", err)
	}
	restored, err := f.client.Restore(ctx, "acme")
	if err != nil || restored.Spec.Suspended {
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
	if archived, _ := f.client.ListArchived(ctx); len(archived) != 0 {
		t.Fatalf("purge left the archive entry: %+v", archived)
	}
	fresh, err := f.client.Create(ctx, appSpec("acme"))
	if err != nil {
		t.Fatalf("create after purge = %v", err)
	}
	if after, _ := f.client.Secret(ctx, "acme", "SESSION_SECRET"); after == before {
		t.Fatal("a fresh tenant reused a purged tenant's secret")
	}
	if record, _ := f.store.Get(fresh.Tenant.Spec.Name); record.DataImage != "" {
		t.Fatalf("a fresh tenant inherited the data image %q", record.DataImage)
	}
}

// Data no archive entry accounts for blocks a create until it is purged.
func TestLeftoverDataBlocksCreateUntilPurged(t *testing.T) {
	f := newAPIFixture(t)
	ctx := context.Background()
	if err := os.MkdirAll(filepath.Join(f.dataRoot, "acme"), 0o700); err != nil {
		t.Fatal(err)
	}
	if _, err := f.client.Create(ctx, appSpec("acme")); err == nil || !strings.Contains(err.Error(), "purge it first") {
		t.Fatalf("create over leftover data = %v", err)
	}
	if err := f.client.Purge(ctx, "acme"); err != nil {
		t.Fatal(err)
	}
	if _, err := f.client.Create(ctx, appSpec("acme")); err != nil {
		t.Fatal(err)
	}
}

func TestSuspendLogsAndRoutes(t *testing.T) {
	f := newAPIFixture(t)
	ctx := context.Background()
	if _, err := f.client.Create(ctx, appSpec("acme")); err != nil {
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
	if !ok || route.Backend != "127.0.0.1:20000" || route.Suspended {
		t.Fatalf("route = %+v, %v", route, ok)
	}
	if _, ok := (Routes{Store: f.store}).Route("nobody"); ok {
		t.Fatal("route for a missing tenant")
	}
}
