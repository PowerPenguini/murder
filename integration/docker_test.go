//go:build docker

// Package integration runs Murder's components against a real Docker
// engine. Run with: go test -tags docker ./integration
package integration

import (
	"context"
	"crypto/rand"
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
	"net/url"
	"os"
	"os/exec"
	"path/filepath"
	"slices"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/PowerPenguini/murder/internal/access"
	"github.com/PowerPenguini/murder/internal/api"
	"github.com/PowerPenguini/murder/internal/apiserver"
	"github.com/PowerPenguini/murder/internal/edge"
	"github.com/PowerPenguini/murder/internal/pluginhost"
	"github.com/PowerPenguini/murder/internal/plugins"
	"github.com/PowerPenguini/murder/internal/reconcile"
	"github.com/PowerPenguini/murder/internal/rollout"
	"github.com/PowerPenguini/murder/internal/runtime"
	"github.com/PowerPenguini/murder/internal/secrets"
	"github.com/PowerPenguini/murder/internal/spec"
	"github.com/PowerPenguini/murder/internal/store"
	"github.com/PowerPenguini/murder/internal/volume"
)

const baseDomain = "murder.test"

func dockerSocket() string {
	if socket := os.Getenv("MURDER_DOCKER_SOCKET"); socket != "" {
		return socket
	}
	return "/var/run/docker.sock"
}

// buildTestImage builds testapp into a scratch image tagged tag (a local
// image only, so pinning falls back to its ID) and returns that ID.
func buildTestImage(t *testing.T, variant, tag string) string {
	t.Helper()
	dir := t.TempDir()
	build := exec.Command("go", "build", "-o", filepath.Join(dir, "testapp"), "./testapp")
	build.Env = append(os.Environ(), "CGO_ENABLED=0", "GOOS=linux")
	if output, err := build.CombinedOutput(); err != nil {
		t.Fatalf("build testapp: %v\n%s", err, output)
	}
	dockerfile := "FROM scratch\nCOPY testapp /testapp\nENV VARIANT=" + variant + "\nENTRYPOINT [\"/testapp\"]\n"
	if err := os.WriteFile(filepath.Join(dir, "Dockerfile"), []byte(dockerfile), 0o600); err != nil {
		t.Fatal(err)
	}
	output, err := exec.Command("docker", "build", "-q", "-t", tag, dir).CombinedOutput()
	if err != nil {
		t.Fatalf("docker build: %v\n%s", err, output)
	}
	id := strings.TrimSpace(string(output))
	t.Cleanup(func() { _ = exec.Command("docker", "image", "rm", "-f", tag, id).Run() })
	return id
}

// buildMurdersPlugin builds one of Murder's plugins into an image as its
// release does, from plugins/Dockerfile, with its manifest as its label,
// and returns its ID.
func buildMurdersPlugin(t *testing.T, plugin, tag string) string {
	t.Helper()
	manifest, err := exec.Command("go", "run", "../plugins/"+plugin, "-manifest").Output()
	if err != nil {
		t.Fatalf("%s's manifest: %v", plugin, err)
	}
	build := exec.Command("docker", "build", "-q", "-f", "../plugins/Dockerfile", "--build-arg", "PLUGIN="+plugin,
		"--label", plugins.ManifestLabel+"="+strings.TrimSpace(string(manifest)), "-t", tag, "..")
	build.Env = append(os.Environ(), "DOCKER_BUILDKIT=1")
	output, err := build.CombinedOutput()
	if err != nil {
		t.Fatalf("docker build %s: %v\n%s", plugin, err, output)
	}
	id := strings.TrimSpace(string(output))
	t.Cleanup(func() { _ = exec.Command("docker", "image", "rm", "-f", tag, id).Run() })
	return id
}

// buildPluginImage builds the test plugin into an image with its
// manifest as its label, and returns its ID.
func buildPluginImage(t *testing.T, tag string) string {
	t.Helper()
	dir := t.TempDir()
	build := exec.Command("go", "build", "-o", filepath.Join(dir, "plugin"), "../internal/plugins/testdata/plugin")
	build.Env = append(os.Environ(), "CGO_ENABLED=0", "GOOS=linux")
	if output, err := build.CombinedOutput(); err != nil {
		t.Fatalf("build the test plugin: %v\n%s", err, output)
	}
	manifest, err := exec.Command(filepath.Join(dir, "plugin"), "-manifest").Output()
	if err != nil {
		t.Fatal(err)
	}
	dockerfile := "FROM scratch\nCOPY plugin /plugin\nENTRYPOINT [\"/plugin\"]\n"
	if err := os.WriteFile(filepath.Join(dir, "Dockerfile"), []byte(dockerfile), 0o600); err != nil {
		t.Fatal(err)
	}
	output, err := exec.Command("docker", "build", "-q", "--label", plugins.ManifestLabel+"="+strings.TrimSpace(string(manifest)), "-t", tag, dir).CombinedOutput()
	if err != nil {
		t.Fatalf("docker build: %v\n%s", err, output)
	}
	id := strings.TrimSpace(string(output))
	t.Cleanup(func() { _ = exec.Command("docker", "image", "rm", "-f", tag, id).Run() })
	return id
}

type node struct {
	store      *store.SQLite
	docker     *runtime.Docker
	volumes    volume.Dir
	key        *secrets.Key
	log        *slog.Logger
	reconciler *reconcile.Reconciler
	client     *api.Client
	edgeURL    string
	// pluginsPort is where TestATenantReachesAPluginOnDocker serves
	// plugins; 0 for none.
	pluginsPort int
}

func newNode(t *testing.T, name string) *node {
	t.Helper()
	dir := t.TempDir()
	keyPath := filepath.Join(dir, "master.key")
	if err := secrets.CreateKey(keyPath); err != nil {
		t.Fatal(err)
	}
	key, err := secrets.LoadKey(keyPath)
	if err != nil {
		t.Fatal(err)
	}
	statePath := filepath.Join(dir, "murder.db")
	if err := store.Init(statePath, "test-node"); err != nil {
		t.Fatal(err)
	}
	// A pool unlikely to collide with the host's networks.
	st, err := store.Open(statePath, store.Options{NetworkPool: netip.MustParsePrefix("10.253.0.0/24"), Sealer: key.NewSealed})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = st.Close() })
	// A node name of its own keeps this test away from any agent running on
	// the same engine, and that agent away from this test's objects.
	n := &node{store: st, docker: runtime.NewDocker(runtime.DockerOptions{Socket: dockerSocket(), Node: name + "-it"}), volumes: volume.Dir{Root: filepath.Join(dir, "tenants")}, key: key, log: slog.New(slog.NewTextHandler(os.Stderr, &slog.HandlerOptions{Level: slog.LevelWarn}))}
	n.reconciler = n.newReconciler()
	apiServer := httptest.NewServer(apiserver.NewServer(st, n.reconciler, n.docker, key, n.log).Handler())
	t.Cleanup(apiServer.Close)
	n.client = api.NewClientURL(apiServer.URL)
	edgeServer := httptest.NewServer(edge.NewProxy(baseDomain, apiserver.Routes{Store: st}, n.log))
	t.Cleanup(edgeServer.Close)
	n.edgeURL = edgeServer.URL
	return n
}

func (n *node) newReconciler() *reconcile.Reconciler {
	return reconcile.New(n.store, n.docker, n.volumes, n.key, reconcile.Config{BaseDomain: baseDomain, Scheme: "http", StopTimeout: 5 * time.Second, HealthTimeout: time.Second, PluginsPort: n.pluginsPort}, n.log)
}

// rollOut drives app's rollout to its end, reconciling as it goes, with
// waves that soak for a second.
func (n *node) rollOut(t *testing.T, app string) {
	t.Helper()
	driver := &rollout.Driver{Store: n.store, Log: n.log, CanarySoak: time.Second, Soak: time.Second, FailAfter: time.Minute,
		Status: func(_ context.Context, record store.Record) (reconcile.Status, bool) {
			return n.reconciler.Status(record.Spec.Name)
		}}
	deadline := time.Now().Add(90 * time.Second)
	for time.Now().Before(deadline) {
		driver.Step(context.Background())
		if err := n.reconciler.Reconcile(context.Background()); err != nil {
			t.Logf("reconcile: %v", err)
		}
		found, err := n.store.Rollout(app)
		if err != nil {
			t.Fatal(err)
		}
		switch found.State {
		case store.RolloutDone:
			return
		case store.RolloutRunning:
		default:
			t.Fatalf("rollout %+v", found)
		}
		time.Sleep(300 * time.Millisecond)
	}
	t.Fatalf("the rollout of %s did not end", app)
}

// waitHealthy reconciles until the tenant is running and healthy.
func (n *node) waitHealthy(t *testing.T, reconciler *reconcile.Reconciler, tenant string) reconcile.Status {
	t.Helper()
	deadline := time.Now().Add(60 * time.Second)
	var last reconcile.Status
	for time.Now().Before(deadline) {
		err := reconciler.Reconcile(context.Background())
		last, _ = reconciler.Status(tenant)
		if err == nil && last.Phase == reconcile.PhaseRunning && last.Healthy {
			return last
		}
		time.Sleep(300 * time.Millisecond)
	}
	t.Fatalf("tenant %s did not become healthy: %+v", tenant, last)
	return last
}

func (n *node) request(t *testing.T, method, tenant, path, body string) (int, string) {
	t.Helper()
	request, err := http.NewRequest(method, n.edgeURL+path, strings.NewReader(body))
	if err != nil {
		t.Fatal(err)
	}
	request.Host = tenant + "." + baseDomain
	response, err := http.DefaultClient.Do(request)
	if err != nil {
		t.Fatal(err)
	}
	defer response.Body.Close()
	data, _ := io.ReadAll(response.Body)
	return response.StatusCode, string(data)
}

func startedAt(t *testing.T, id string) string {
	t.Helper()
	output, err := exec.Command("docker", "inspect", "-f", "{{.State.StartedAt}}", id).Output()
	if err != nil {
		t.Fatal(err)
	}
	return strings.TrimSpace(string(output))
}

func cleanupTenant(t *testing.T, tenant string) {
	t.Cleanup(func() {
		output, _ := exec.Command("docker", "ps", "-aq", "--filter", "label="+runtime.LabelTenant+"="+tenant).Output()
		for _, id := range strings.Fields(string(output)) {
			_ = exec.Command("docker", "rm", "-f", id).Run()
		}
		_ = exec.Command("docker", "network", "rm", reconcile.NetworkName(tenant)).Run()
	})
}

func TestTenantLifecycleOnDocker(t *testing.T) {
	if _, err := os.Stat(dockerSocket()); err != nil {
		t.Skipf("no Docker socket: %v", err)
	}
	suffix := make([]byte, 3)
	_, _ = rand.Read(suffix)
	tenant := "it-" + hex.EncodeToString(suffix)
	repository := "murder-it/" + tenant
	imageV1, imageV2 := buildTestImage(t, "v1", repository+":v1"), buildTestImage(t, "v2", repository+":v2")
	cleanupTenant(t, tenant)
	n := newNode(t, tenant)
	ctx := context.Background()

	uid, gid := os.Getuid(), os.Getgid()
	// A minimal spec: everything else is a default, and the tag is pinned.
	app := spec.Tenant{
		Image: repository + ":v1", HealthPath: "/health",
		User:    strconv.Itoa(uid) + ":" + strconv.Itoa(gid),
		Env:     map[string]string{"PUBLIC_URL": "{{url}}", "DATA_DIR": "/data"},
		Secrets: spec.Secrets{"APP_SECRET": {}},
		Limits:  spec.Limits{MemoryMB: 64, CPUs: 0.5, PIDs: 64},
	}
	stored, err := n.client.PutApp(ctx, "it-app", app)
	if err != nil {
		t.Fatal(err)
	}
	// A local-only tag cannot be pulled, so the agent pins the local image:
	// by ID, or by its content digest where the engine reports one.
	if !pinnedTo(stored.App.Spec.Image, imageV1) || !strings.Contains(stored.Note, "local image") {
		t.Fatalf("pinned image = %q (note %q), want the local image %s", stored.App.Spec.Image, stored.Note, imageV1)
	}
	applied, err := n.client.Create(ctx, api.TenantRequest{Name: tenant, App: "it-app"})
	if err != nil {
		t.Fatal(err)
	}
	created := applied.Tenant
	first := n.waitHealthy(t, n.reconciler, tenant)

	status, body := n.request(t, http.MethodGet, tenant, "/info", "")
	var info struct {
		Host, ForwardedFor, Proto, PublicURL, Variant string
		SecretLength, UID                             int
	}
	if status != http.StatusOK || json.Unmarshal([]byte(body), &info) != nil {
		t.Fatalf("/info = %d %s", status, body)
	}
	if info.Host != tenant+"."+baseDomain || info.Proto != "http" || info.ForwardedFor == "" || info.PublicURL != created.URL || info.SecretLength != 64 || info.UID != uid || info.Variant != "v1" {
		t.Fatalf("the app saw %+v", info)
	}
	if _, writable := n.request(t, http.MethodGet, tenant, "/root-writable", ""); writable != "false" {
		t.Fatal("the container's root filesystem is writable")
	}
	if status, _ := n.request(t, http.MethodPut, tenant, "/value", "kept"); status != http.StatusNoContent {
		t.Fatalf("write = %d", status)
	}

	// A new image of the app replaces the container and keeps the data.
	app.Image = repository + ":v2"
	// It rolls out in waves: the tenant waits on v1 until the driver moves
	// it, then the rollout is through once it is healthy on v2.
	upgraded, err := n.client.PutApp(ctx, "it-app", app)
	if err != nil || !upgraded.Changed || !upgraded.Rollout || len(upgraded.Tenants) != 1 || pinnedTo(upgraded.Tenants[0].Spec.Image, imageV2) {
		t.Fatalf("upgrade = %+v, %v", upgraded, err)
	}
	if same, err := n.client.PutApp(ctx, "it-app", app); err != nil || same.Changed {
		t.Fatalf("applying the same app again = %+v, %v", same, err)
	}
	n.rollOut(t, "it-app")
	second := n.waitHealthy(t, n.reconciler, tenant)
	if second.ContainerID == first.ContainerID {
		t.Fatal("the image change kept the old container")
	}
	if _, body := n.request(t, http.MethodGet, tenant, "/info", ""); !strings.Contains(body, `"variant":"v2"`) {
		t.Fatalf("after the image change the app reports %s", body)
	}
	if _, value := n.request(t, http.MethodGet, tenant, "/value", ""); value != "kept" {
		t.Fatalf("data after the image change = %q", value)
	}

	// A restarted agent adopts the running container without restarting it.
	started := startedAt(t, second.ContainerID)
	adopted := n.waitHealthy(t, n.newReconciler(), tenant)
	if adopted.ContainerID != second.ContainerID || startedAt(t, second.ContainerID) != started {
		t.Fatal("a restarted agent replaced or restarted a healthy container")
	}

	// Suspend stops the tenant and answers 503; resume brings the data back.
	if _, err := n.client.Suspend(ctx, tenant, true); err != nil {
		t.Fatal(err)
	}
	if err := n.reconciler.Reconcile(ctx); err != nil {
		t.Fatal(err)
	}
	if status, _ := n.request(t, http.MethodGet, tenant, "/health", ""); status != http.StatusServiceUnavailable {
		t.Fatalf("suspended tenant answered %d", status)
	}
	if containers, _ := n.docker.List(ctx); hasTenant(containers, tenant) {
		t.Fatal("a suspended tenant still has a container")
	}
	if _, err := n.client.Suspend(ctx, tenant, false); err != nil {
		t.Fatal(err)
	}
	n.waitHealthy(t, n.reconciler, tenant)
	if _, value := n.request(t, http.MethodGet, tenant, "/value", ""); value != "kept" {
		t.Fatalf("data after resume = %q", value)
	}
	if _, err := n.client.Logs(ctx, tenant, 20, access.Proof{}); err == nil {
		t.Fatal("logs read without a reason")
	}
	if _, err := n.client.Logs(ctx, tenant, 20, access.Proof{Purpose: "an integration test", Ticket: "incident"}); err != nil {
		t.Fatalf("logs: %v", err)
	}
	if entries, err := n.client.Accesses(ctx, tenant, time.Time{}); err != nil || len(entries) != 1 || entries[0].Kind != access.Logs {
		t.Fatalf("the access register: %+v %v", entries, err)
	}

	// Archive removes the container and network and keeps the data;
	// restore brings the tenant back with it.
	if err := n.client.Archive(ctx, tenant); err != nil {
		t.Fatal(err)
	}
	if status, _ := n.request(t, http.MethodGet, tenant, "/health", ""); status != http.StatusNotFound {
		t.Fatalf("archived tenant answered %d before its container was removed", status)
	}
	// The pass the archive starts removes the container and network.
	if err := n.reconciler.Reconcile(ctx); err != nil {
		t.Fatal(err)
	}
	if containers, _ := n.docker.List(ctx); hasTenant(containers, tenant) {
		t.Fatal("container left after archive")
	}
	if err := exec.Command("docker", "network", "inspect", reconcile.NetworkName(tenant)).Run(); err == nil {
		t.Fatal("network left after archive")
	}
	if status, _ := n.request(t, http.MethodGet, tenant, "/health", ""); status != http.StatusNotFound {
		t.Fatalf("archived tenant answered %d", status)
	}
	if _, err := n.client.Restore(ctx, tenant); err != nil {
		t.Fatal(err)
	}
	n.waitHealthy(t, n.reconciler, tenant)
	if _, value := n.request(t, http.MethodGet, tenant, "/value", ""); value != "kept" {
		t.Fatalf("data after restore = %q", value)
	}

	// Purge deletes an archived tenant and its data.
	if err := n.client.Archive(ctx, tenant); err != nil {
		t.Fatal(err)
	}
	if err := n.client.Purge(ctx, tenant); err != nil {
		t.Fatal(err)
	}
	if _, err := os.Stat(filepath.Join(n.volumes.Root, tenant)); !os.IsNotExist(err) {
		t.Fatalf("data left after purge: %v", err)
	}
	fmt.Fprintln(os.Stderr, "lifecycle passed for", tenant)
}

// pinnedTo reports whether pinned names the local image id.
func pinnedTo(pinned, id string) bool {
	if pinned == id {
		return true
	}
	output, err := exec.Command("docker", "image", "inspect", "-f", "{{.Id}}", pinned).Output()
	return err == nil && strings.TrimSpace(string(output)) == id
}

func hasTenant(containers []runtime.Container, tenant string) bool {
	for _, container := range containers {
		if container.Labels[runtime.LabelTenant] == tenant {
			return true
		}
	}
	return false
}

// Tenants on one node cannot reach each other, while the host (the edge
// proxy) reaches every tenant at its address.
func TestTenantsCannotReachEachOther(t *testing.T) {
	if _, err := os.Stat(dockerSocket()); err != nil {
		t.Skipf("no Docker socket: %v", err)
	}
	suffix := make([]byte, 3)
	_, _ = rand.Read(suffix)
	first, second := "iso-a-"+hex.EncodeToString(suffix), "iso-b-"+hex.EncodeToString(suffix)
	repository := "murder-it/" + first
	buildTestImage(t, "v1", repository+":v1")
	cleanupTenant(t, first)
	cleanupTenant(t, second)
	n := newNode(t, first)
	ctx := context.Background()
	app := spec.Tenant{Image: repository + ":v1", HealthPath: "/health", User: strconv.Itoa(os.Getuid()) + ":" + strconv.Itoa(os.Getgid()), Env: map[string]string{"DATA_DIR": "/data"}}
	if _, err := n.client.PutApp(ctx, "iso", app); err != nil {
		t.Fatal(err)
	}
	var addresses []string
	for _, name := range []string{first, second} {
		created, err := n.client.Create(ctx, api.TenantRequest{Name: name, App: "iso"})
		if err != nil {
			t.Fatal(err)
		}
		n.waitHealthy(t, n.reconciler, name)
		addresses = append(addresses, created.Tenant.Address)
	}
	// The host reaches both directly.
	for _, address := range addresses {
		response, err := http.Get("http://" + address + "/health")
		if err != nil {
			t.Fatalf("the host cannot reach %s: %v", address, err)
		}
		response.Body.Close()
	}
	// Neither reaches the other.
	for i, name := range []string{first, second} {
		other := addresses[1-i]
		_, body := n.request(t, http.MethodGet, name, "/dial?addr="+other, "")
		if body == "connected" || !strings.HasPrefix(body, "refused") {
			t.Fatalf("%s reached the other tenant at %s: %s", name, other, body)
		}
	}
}

// The engine keeps an image a container uses; once it is gone, RemoveImage
// deletes the image with its tags.
func TestRemoveImageOnDocker(t *testing.T) {
	if _, err := os.Stat(dockerSocket()); err != nil {
		t.Skip("no Docker engine")
	}
	ctx := context.Background()
	docker := runtime.NewDocker(runtime.DockerOptions{Socket: dockerSocket(), Node: "gc-test"})
	tag := "murder-it/gc:" + strconv.FormatInt(time.Now().UnixNano(), 10)
	id := buildTestImage(t, "gc", tag)
	output, err := exec.Command("docker", "create", tag).Output()
	if err != nil {
		t.Fatal(err)
	}
	container := strings.TrimSpace(string(output))
	t.Cleanup(func() { _ = exec.Command("docker", "rm", "-f", container).Run() })
	if got, err := docker.ImageID(ctx, tag); err != nil || got != id {
		t.Fatalf("ImageID = %q, %v; want %s", got, err, id)
	}
	if err := docker.RemoveImage(ctx, id); !errors.Is(err, runtime.ErrInUse) {
		t.Fatalf("remove an image a container uses = %v", err)
	}
	if err := exec.Command("docker", "rm", container).Run(); err != nil {
		t.Fatal(err)
	}
	if err := docker.RemoveImage(ctx, id); err != nil {
		t.Fatal(err)
	}
	if got, err := docker.ImageID(ctx, tag); err != nil || got != "" {
		t.Fatalf("the tag after removal = %q, %v", got, err)
	}
	if err := docker.RemoveImage(ctx, id); err != nil {
		t.Fatalf("remove a missing image = %v", err)
	}
}

// An app that goes over its memory limit is killed and restarted by the
// engine; once it serves again, its status still says why it exited.
func TestOutOfMemoryIsReportedOnDocker(t *testing.T) {
	if _, err := os.Stat(dockerSocket()); err != nil {
		t.Skipf("no Docker socket: %v", err)
	}
	suffix := make([]byte, 3)
	_, _ = rand.Read(suffix)
	tenant := "it-oom-" + hex.EncodeToString(suffix)
	image := buildTestImage(t, "oom", "murder-it/"+tenant+":oom")
	cleanupTenant(t, tenant)
	n := newNode(t, tenant)
	ctx := context.Background()
	app := spec.Tenant{
		Image: image, HealthPath: "/health",
		User:   strconv.Itoa(os.Getuid()) + ":" + strconv.Itoa(os.Getgid()),
		Env:    map[string]string{"DATA_DIR": "/data"},
		Limits: spec.Limits{MemoryMB: 16, CPUs: 0.5, PIDs: 64},
	}
	if _, err := n.client.PutApp(ctx, "it-oom", app); err != nil {
		t.Fatal(err)
	}
	if _, err := n.client.Create(ctx, api.TenantRequest{Name: tenant, App: "it-oom"}); err != nil {
		t.Fatal(err)
	}
	deadline := time.Now().Add(60 * time.Second)
	var status reconcile.Status
	for time.Now().Before(deadline) {
		_ = n.reconciler.Reconcile(ctx)
		status, _ = n.reconciler.Status(tenant)
		// The engine may log the out-of-memory kill just after the exit.
		if status.Phase == reconcile.PhaseRunning && status.Healthy && status.Restarts == 1 && strings.Contains(status.LastExit, "more than its 16 MB of memory") {
			return
		}
		time.Sleep(300 * time.Millisecond)
	}
	// What the engine logged and reports, to tell a missing oom event from
	// a late one.
	for _, args := range [][]string{
		{"version", "--format", "{{.Server.Version}}"},
		{"info", "--format", "cgroup {{.CgroupVersion}} {{.CgroupDriver}}, kernel {{.KernelVersion}}"},
		{"events", "--since", time.Now().Add(-90 * time.Second).Format(time.RFC3339), "--until", time.Now().Format(time.RFC3339), "--filter", "container=" + status.ContainerID, "--format", "{{.TimeNano}} {{.Action}} {{json .Actor.Attributes}}"},
		{"inspect", "--format", "{{json .State}}", status.ContainerID},
	} {
		out, err := exec.Command("docker", args...).CombinedOutput()
		t.Logf("docker %s: %s %v", args[0], out, err)
	}
	t.Fatalf("status = %+v, want it serving again after one restart, killed for its memory", status)
}

// A tenant's container reaches a plugin its app lists at its network's
// gateway, as {{plugin.NAME.url}} says, from its own address: the edge
// knows it by that, passes the request on with the operator's key, and
// counts it.
func TestATenantReachesAPluginOnDocker(t *testing.T) {
	if _, err := os.Stat(dockerSocket()); err != nil {
		t.Skipf("no Docker socket: %v", err)
	}
	suffix := make([]byte, 3)
	_, _ = rand.Read(suffix)
	name := "plug-" + hex.EncodeToString(suffix)
	repository := "murder-it/" + name
	buildTestImage(t, "v1", repository+":v1")
	cleanupTenant(t, name)
	var auth, moduleKey, moduleHost string
	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.Host {
		case "api.example.com":
			moduleKey, moduleHost = r.Header.Get("X-Key"), r.Host
		case "api.openai.com":
			auth = r.Header.Get("Authorization")
		}
		w.Header().Set("Content-Type", "application/json")
		io.WriteString(w, `{"object":"list","data":[]}`)
	}))
	defer upstream.Close()
	// A plugin of one's own: the test plugin, in an image whose label
	// holds its manifest, as it prints it.
	pluginImage := buildPluginImage(t, repository+"-plugin:v1")
	// Murder's openai, as its release builds it.
	buildMurdersPlugin(t, "openai", repository+"-openai:v1")
	// Plugins on every address, as the edge serves them: on a port the
	// host's own firewall, if any, lets containers reach.
	port := os.Getenv("MURDER_IT_PLUGINS_PORT")
	if port == "" {
		port = "0"
	}
	listener, err := net.Listen("tcp4", "0.0.0.0:"+port)
	if err != nil {
		t.Fatal(err)
	}
	n := newNode(t, name)
	n.pluginsPort = listener.Addr().(*net.TCPAddr).Port
	n.reconciler = n.newReconciler()
	counter := plugins.NewCounter("edge-it")
	proxy := plugins.NewProxy(func() plugins.Config { return apiserver.Routes{Store: n.store}.Table().Plugins }, n.key.Open, counter, n.log)
	// The agent runs the plugin's container, its socket below a root of
	// the test's, and the edge reaches it there, as murder edge does.
	root, err := os.MkdirTemp("/tmp", "mp")
	if err != nil {
		t.Fatal(err)
	}
	defer os.RemoveAll(root)
	host := &pluginhost.Host{Runtime: n.docker, Plugins: n.store.Plugins, Root: root, Log: n.log}
	defer func() {
		containers, _ := n.docker.ListPlugins(context.Background())
		for _, container := range containers {
			_ = n.docker.Stop(context.Background(), container.ID, time.Second)
			_ = n.docker.Remove(context.Background(), container.ID)
		}
	}()
	proxy.WithContainers(plugins.NewContainers(root, n.log))
	// The plugin's host, over HTTPS, is the test's upstream.
	upstreamURL, _ := url.Parse(upstream.URL)
	proxy.Transport = toTestUpstream{host: upstreamURL.Host}
	server := &http.Server{Handler: proxy, ReadHeaderTimeout: 10 * time.Second}
	go server.Serve(listener)
	defer server.Close()
	ctx := context.Background()
	if _, err := n.client.PutPlugin(ctx, "openai", plugins.Spec{Kind: plugins.KindContainer, Image: repository + "-openai:v1"}); err != nil {
		t.Fatal(err)
	}
	if err := n.client.SetPluginKey(ctx, "openai", "", "sk-operator"); err != nil {
		t.Fatal(err)
	}
	installed, err := n.client.PutPlugin(ctx, "mine", plugins.Spec{Kind: plugins.KindContainer, Image: repository + "-plugin:v1"})
	if err != nil {
		t.Fatal(err)
	}
	if installed.Plugin.Manifest == nil || installed.Plugin.Manifest.Name != "test" || installed.Plugin.Spec.Image != pluginImage {
		t.Fatalf("installed %+v", installed)
	}
	if err := host.Sync(ctx); err != nil {
		t.Fatal(err)
	}
	if err := n.client.SetPluginKey(ctx, "mine", "", "mine-key"); err != nil {
		t.Fatal(err)
	}
	app := spec.Tenant{Image: repository + ":v1", HealthPath: "/health", User: strconv.Itoa(os.Getuid()) + ":" + strconv.Itoa(os.Getgid()), Plugins: []string{"openai", "mine"},
		Env: map[string]string{"DATA_DIR": "/data", "OPENAI_BASE_URL": "{{plugin.openai.url}}/v1", "OPENAI_API_KEY": "murder", "MINE_URL": "{{plugin.mine.url}}"}}
	if _, err := n.client.PutApp(ctx, "plug", app); err != nil {
		t.Fatal(err)
	}
	if _, err := n.client.Create(ctx, api.TenantRequest{Name: name, App: "plug"}); err != nil {
		t.Fatal(err)
	}
	n.waitHealthy(t, n.reconciler, name)
	// The tenants' reconciling leaves the plugin's container alone: still
	// there, still started again by the engine.
	if err := n.reconciler.Reconcile(ctx); err != nil {
		t.Fatal(err)
	}
	if err := n.reconciler.SetRestartPolicies(ctx); err != nil {
		t.Fatal(err)
	}
	containers, err := n.docker.ListPlugins(ctx)
	if err != nil || len(containers) != 2 || !containers[0].Running || !containers[1].Running {
		t.Fatalf("the plugins' containers after a pass: %+v %v", containers, err)
	}
	for _, container := range containers {
		if state, err := n.docker.Inspect(ctx, container.ID); err != nil || state.RestartPolicy != "unless-stopped" {
			t.Fatalf("its restart policy: %+v %v", state, err)
		}
	}
	logsOf := func(plugin string) string {
		for _, container := range containers {
			if container.Labels[runtime.LabelPlugin] == plugin {
				logs, _ := n.docker.Logs(ctx, container.ID, 50)
				return string(logs)
			}
		}
		return ""
	}
	if tenants, _ := n.docker.List(ctx); slices.ContainsFunc(tenants, func(c runtime.Container) bool { return c.Labels[runtime.LabelPlugin] != "" }) {
		t.Fatal("the plugin's container is listed as a tenant's")
	}

	// Murder's openai: its container may still be starting.
	var body string
	for deadline := time.Now().Add(30 * time.Second); time.Now().Before(deadline); time.Sleep(200 * time.Millisecond) {
		if _, body = n.request(t, http.MethodGet, name, "/fetch?env=OPENAI_BASE_URL&path=/models", ""); !strings.HasPrefix(body, "503 ") {
			break
		}
	}
	if strings.Contains(body, "i/o timeout") && port == "0" {
		t.Skipf("the host's own firewall drops containers' connections to it (%s): set MURDER_IT_PLUGINS_PORT to a port it lets in", body)
	}
	if !strings.HasPrefix(body, "200 ") || auth != "Bearer sk-operator" {
		t.Fatalf("the tenant got %q; the provider saw %q; the container said:\n%s", body, auth, logsOf("openai"))
	}
	if used := counter.Used(name, "openai", plugins.MetricRequests, plugins.PerDay, time.Now()); used != 1 {
		t.Fatalf("%d requests counted", used)
	}
	// The container's plugin: the edge asked its container on its socket,
	// and put the key where its manifest says. The container may still be
	// starting.
	var mine string
	for deadline := time.Now().Add(30 * time.Second); time.Now().Before(deadline); time.Sleep(200 * time.Millisecond) {
		if _, mine = n.request(t, http.MethodGet, name, "/fetch?env=MINE_URL&path=/v1/things", ""); !strings.HasPrefix(mine, "503 ") {
			break
		}
	}
	if !strings.HasPrefix(mine, "200 ") || moduleKey != "Token mine-key" || moduleHost != "api.example.com" {
		t.Fatalf("the tenant got %q; the plugin's provider saw %q at %q; the container said:\n%s", mine, moduleKey, moduleHost, logsOf("mine"))
	}
	if used := counter.Used(name, "mine", plugins.MetricRequests, plugins.PerDay, time.Now()); used != 1 {
		t.Fatalf("%d requests of the container's plugin counted", used)
	}
}

// toTestUpstream sends every request to the test's upstream, over plain
// HTTP, with the host it was for.
type toTestUpstream struct{ host string }

func (u toTestUpstream) RoundTrip(r *http.Request) (*http.Response, error) {
	r = r.Clone(r.Context())
	r.Host, r.URL.Scheme, r.URL.Host = r.URL.Host, "http", u.host
	return http.DefaultTransport.RoundTrip(r)
}
