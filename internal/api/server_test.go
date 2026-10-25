package api

import (
	"context"
	"encoding/json"
	"io"
	"log/slog"
	"net/http/httptest"
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

const image = "sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"

// memoryRuntime runs nothing; it only remembers containers.
type memoryRuntime struct {
	mu         sync.Mutex
	containers []runtime.Container
}

func (m *memoryRuntime) HasImage(context.Context, string) (bool, error) { return true, nil }
func (m *memoryRuntime) PullImage(context.Context, string) error        { return nil }
func (m *memoryRuntime) EnsureNetwork(context.Context, string, map[string]string) (string, error) {
	return "172.30.0.0/16", nil
}
func (m *memoryRuntime) RemoveNetwork(context.Context, string) error { return nil }
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
	st, err := store.OpenFile(filepath.Join(t.TempDir(), "state.json"), 20000, 20100)
	if err != nil {
		t.Fatal(err)
	}
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
		Name: name, Image: image, Port: 8080, HealthPath: "/health", DataPath: "/data",
		DataUID: os.Getuid(), DataGID: os.Getgid(),
		Secrets: map[string]spec.Secret{"SESSION_SECRET": {Kind: "hex", Bytes: 32}},
	}
}

func TestCreateGeneratesSecretsWithoutShowingThem(t *testing.T) {
	f := newAPIFixture(t)
	ctx := context.Background()
	created, err := f.client.Create(ctx, appSpec("acme"))
	if err != nil {
		t.Fatal(err)
	}
	if created.URL != "https://acme.example.test" || created.HostPort != 20000 || created.Generation != 1 {
		t.Fatalf("created = %+v", created)
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
	_, err = f.client.Get(ctx, "nobody")
	check("missing", err, 404)
	if !IsNotFound(err) {
		t.Error("IsNotFound missed a 404")
	}
	bad := appSpec("bad")
	bad.Port = 0
	_, err = f.client.Create(ctx, bad)
	check("invalid spec", err, 400)
	_, err = f.client.Create(ctx, appSpec("www"))
	check("reserved name", err, 400)
	_, err = f.client.SetImage(ctx, "acme", "nginx:latest")
	check("unpinned image", err, 400)
	_, err = f.client.Secret(ctx, "acme", "NOPE")
	check("missing secret", err, 404)
	renamed := appSpec("other")
	_, err = f.client.Replace(ctx, renamed)
	check("replace missing", err, 404)
}

func TestReplaceKeepsAndAddsSecrets(t *testing.T) {
	f := newAPIFixture(t)
	ctx := context.Background()
	if _, err := f.client.Create(ctx, appSpec("acme")); err != nil {
		t.Fatal(err)
	}
	before, _ := f.client.Secret(ctx, "acme", "SESSION_SECRET")
	next := appSpec("acme")
	next.Secrets["ADMIN_PASSWORD"] = spec.Secret{Kind: "password", Bytes: 20}
	next.Env = map[string]string{"MODE": "prod"}
	replaced, err := f.client.Replace(ctx, next)
	if err != nil || replaced.Generation != 2 || replaced.Spec.Env["MODE"] != "prod" {
		t.Fatalf("replace = %+v, %v", replaced, err)
	}
	after, _ := f.client.Secret(ctx, "acme", "SESSION_SECRET")
	if after != before {
		t.Fatal("replace regenerated an existing secret")
	}
	if password, err := f.client.Secret(ctx, "acme", "ADMIN_PASSWORD"); err != nil || len(password) != 20 {
		t.Fatalf("new secret = %q, %v", password, err)
	}
	delete(next.Secrets, "ADMIN_PASSWORD")
	if _, err := f.client.Replace(ctx, next); err != nil {
		t.Fatal(err)
	}
	if _, err := f.client.Secret(ctx, "acme", "ADMIN_PASSWORD"); !IsNotFound(err) {
		t.Fatalf("a removed secret is still readable: %v", err)
	}
}

func TestImageSuspendLogsAndDeleteWithPurge(t *testing.T) {
	f := newAPIFixture(t)
	ctx := context.Background()
	if _, err := f.client.Create(ctx, appSpec("acme")); err != nil {
		t.Fatal(err)
	}
	changed, err := f.client.SetImage(ctx, "acme", "sha256:bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb")
	if err != nil || changed.Generation != 2 {
		t.Fatalf("set image = %+v, %v", changed, err)
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
	if _, err := os.Stat(filepath.Join(f.dataRoot, "acme")); err != nil {
		t.Fatal("the reconcile pass did not create the data directory")
	}
	if err := f.client.Delete(ctx, "acme", true); err != nil {
		t.Fatal(err)
	}
	if _, err := os.Stat(filepath.Join(f.dataRoot, "acme")); !os.IsNotExist(err) {
		t.Fatal("purge left the data directory")
	}
	if err := f.client.Delete(ctx, "acme", false); !IsNotFound(err) {
		t.Fatalf("second delete = %v", err)
	}
}

func TestRoutesFollowTheStore(t *testing.T) {
	f := newAPIFixture(t)
	if _, err := f.client.Create(context.Background(), appSpec("acme")); err != nil {
		t.Fatal(err)
	}
	routes := Routes{Store: f.store}
	route, ok := routes.Route("acme")
	if !ok || route.Backend != "127.0.0.1:20000" || route.Suspended {
		t.Fatalf("route = %+v, %v", route, ok)
	}
	if _, ok := routes.Route("nobody"); ok {
		t.Fatal("route for a missing tenant")
	}
}
