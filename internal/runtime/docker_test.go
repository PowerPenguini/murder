package runtime

import (
	"context"
	"encoding/base64"
	"encoding/binary"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

// fakeEngine records requests and answers like the Docker Engine API.
type fakeEngine struct {
	t        *testing.T
	requests []string
	created  map[string]any
	handlers map[string]http.HandlerFunc
}

func newFakeEngine(t *testing.T) (*fakeEngine, *Docker) {
	engine := &fakeEngine{t: t, handlers: map[string]http.HandlerFunc{}}
	server := httptest.NewServer(engine)
	t.Cleanup(server.Close)
	return engine, newDockerForURL(server.URL, server.Client(), "node-1")
}

func (e *fakeEngine) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	path := strings.TrimPrefix(r.URL.Path, "/"+apiVersion)
	key := r.Method + " " + path
	e.requests = append(e.requests, key+"?"+r.URL.RawQuery)
	if handler, ok := e.handlers[key]; ok {
		handler(w, r)
		return
	}
	w.WriteHeader(http.StatusNotFound)
	_, _ = w.Write([]byte(`{"message":"no such thing"}`))
}

func TestImagePresenceAndPull(t *testing.T) {
	engine, docker := newFakeEngine(t)
	ctx := context.Background()
	engine.handlers["GET /images/sha256:present/json"] = func(w http.ResponseWriter, _ *http.Request) { _, _ = w.Write([]byte(`{}`)) }
	if ok, err := docker.HasImage(ctx, "sha256:present"); !ok || err != nil {
		t.Fatalf("present image: %v, %v", ok, err)
	}
	if ok, err := docker.HasImage(ctx, "sha256:missing"); ok || err != nil {
		t.Fatalf("missing image: %v, %v", ok, err)
	}
	engine.handlers["POST /images/create"] = func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Query().Get("fromImage") != "ghcr.io/x/app" || r.URL.Query().Get("tag") != "sha256:abc" {
			t.Errorf("pull query = %s", r.URL.RawQuery)
		}
		_, _ = w.Write([]byte(`{"status":"Pulling"}` + "\n" + `{"error":"denied: requires authentication"}` + "\n"))
	}
	err := docker.PullImage(ctx, "ghcr.io/x/app@sha256:abc")
	if err == nil || !strings.Contains(err.Error(), "requires authentication") {
		t.Fatalf("a pull whose stream reports an error = %v", err)
	}
	if err := docker.PullImage(ctx, "ghcr.io/x/app:latest"); err == nil {
		t.Fatal("pulled a mutable tag")
	}
}

func TestCreateSendsHardenedContainer(t *testing.T) {
	engine, docker := newFakeEngine(t)
	engine.handlers["POST /containers/create"] = func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Query().Get("name") != "corral-acme" {
			t.Errorf("container name = %q", r.URL.Query().Get("name"))
		}
		_ = json.NewDecoder(r.Body).Decode(&engine.created)
		_, _ = w.Write([]byte(`{"Id":"c1"}`))
	}
	id, err := docker.Create(context.Background(), ContainerSpec{
		Name: "corral-acme", Image: "sha256:img", Env: []string{"A=1"}, User: "10001", Network: "corral-acme",
		ContainerPort: 8080, HostPort: 20001, Mounts: []Mount{{Source: "/srv/acme", Target: "/data"}},
		MemoryBytes: 512 << 20, NanoCPUs: 1e9, PIDs: 256, ReadOnlyRoot: true, Tmpfs: map[string]string{"/tmp": "size=64m"},
		Labels: map[string]string{LabelTenant: "acme"},
	})
	if err != nil || id != "c1" {
		t.Fatalf("Create = %q, %v", id, err)
	}
	host := engine.created["HostConfig"].(map[string]any)
	bindings := host["PortBindings"].(map[string]any)["8080/tcp"].([]any)[0].(map[string]any)
	if bindings["HostIp"] != "127.0.0.1" || bindings["HostPort"] != "20001" {
		t.Errorf("port binding = %v, want 127.0.0.1:20001 only", bindings)
	}
	if host["ReadonlyRootfs"] != true || host["NetworkMode"] != "corral-acme" || host["PidsLimit"] != float64(256) {
		t.Errorf("host config = %v", host)
	}
	if capDrop := host["CapDrop"].([]any); len(capDrop) != 1 || capDrop[0] != "ALL" {
		t.Errorf("CapDrop = %v", capDrop)
	}
	if opts := host["SecurityOpt"].([]any); len(opts) != 1 || opts[0] != "no-new-privileges" {
		t.Errorf("SecurityOpt = %v", opts)
	}
	if mount := host["Mounts"].([]any)[0].(map[string]any); mount["Source"] != "/srv/acme" || mount["Target"] != "/data" || mount["Type"] != "bind" {
		t.Errorf("mount = %v", mount)
	}
	labels := engine.created["Labels"].(map[string]any)
	if labels[LabelManaged] != "true" || labels[LabelNode] != "node-1" || labels[LabelTenant] != "acme" {
		t.Errorf("labels = %v", labels)
	}
}

func TestListStopRemoveAndNetwork(t *testing.T) {
	engine, docker := newFakeEngine(t)
	ctx := context.Background()
	engine.handlers["GET /containers/json"] = func(w http.ResponseWriter, r *http.Request) {
		filters := r.URL.Query().Get("filters")
		if !strings.Contains(filters, LabelManaged+"=true") || !strings.Contains(filters, LabelNode+"=node-1") || r.URL.Query().Get("all") != "true" {
			t.Errorf("list query = %s", r.URL.RawQuery)
		}
		_, _ = w.Write([]byte(`[{"Id":"c1","Names":["/corral-acme"],"State":"running","Labels":{"io.corral.tenant":"acme"}},{"Id":"c2","Names":["/corral-beta"],"State":"exited","Labels":{}}]`))
	}
	containers, err := docker.List(ctx)
	if err != nil || len(containers) != 2 || !containers[0].Running || containers[1].Running || containers[0].Name != "corral-acme" {
		t.Fatalf("List = %+v, %v", containers, err)
	}
	// Stopping or removing something already gone is not an error.
	if err := docker.Stop(ctx, "gone", time.Second); err != nil {
		t.Fatal(err)
	}
	if err := docker.Remove(ctx, "gone"); err != nil {
		t.Fatal(err)
	}
	if err := docker.RemoveNetwork(ctx, "gone"); err != nil {
		t.Fatal(err)
	}
	created := false
	engine.handlers["GET /networks/corral-acme"] = func(w http.ResponseWriter, _ *http.Request) {
		if !created {
			w.WriteHeader(http.StatusNotFound)
			return
		}
		_, _ = w.Write([]byte(`{"IPAM":{"Config":[{"Subnet":"fd00::/64"},{"Subnet":"10.240.0.16/28"}]}}`))
	}
	engine.handlers["POST /networks/create"] = func(w http.ResponseWriter, r *http.Request) {
		var body map[string]any
		_ = json.NewDecoder(r.Body).Decode(&body)
		subnet := body["IPAM"].(map[string]any)["Config"].([]any)[0].(map[string]any)["Subnet"]
		labels := body["Labels"].(map[string]any)
		if body["Name"] != "corral-acme" || subnet != "10.240.0.16/28" || labels[LabelNode] != "node-1" || body["Options"].(map[string]any)["com.docker.network.bridge.enable_icc"] != "false" {
			t.Errorf("network create = %v", body)
		}
		created = true
		w.WriteHeader(http.StatusCreated)
		_, _ = w.Write([]byte(`{"Id":"n1"}`))
	}
	if err := docker.EnsureNetwork(ctx, "corral-acme", "10.240.0.16/28", map[string]string{LabelTenant: "acme"}); err != nil {
		t.Fatal(err)
	}
	if err := docker.EnsureNetwork(ctx, "corral-acme", "10.240.0.16/28", nil); err != nil {
		t.Fatalf("an existing network with the right subnet = %v", err)
	}
	if err := docker.EnsureNetwork(ctx, "corral-acme", "10.240.0.32/28", nil); err == nil {
		t.Fatal("an existing network with another subnet was accepted")
	}
	engine.handlers["GET /networks"] = func(w http.ResponseWriter, r *http.Request) {
		if !strings.Contains(r.URL.Query().Get("filters"), LabelNode+"=node-1") {
			t.Errorf("network list filters = %s", r.URL.Query().Get("filters"))
		}
		_, _ = w.Write([]byte(`[{"Name":"corral-acme","Labels":{"io.corral.tenant":"acme"}}]`))
	}
	networks, err := docker.ListNetworks(ctx)
	if err != nil || len(networks) != 1 || networks[0].Labels[LabelTenant] != "acme" {
		t.Fatalf("ListNetworks = %+v, %v", networks, err)
	}
	engine.handlers["DELETE /containers/c9"] = func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Query().Get("v") != "true" || r.URL.Query().Get("force") != "true" {
			t.Errorf("remove query = %s", r.URL.RawQuery)
		}
		w.WriteHeader(http.StatusNoContent)
	}
	if err := docker.Remove(ctx, "c9"); err != nil {
		t.Fatal(err)
	}
}

func TestPullSendsRegistryCredentials(t *testing.T) {
	engine, docker := newFakeEngine(t)
	authFile := filepath.Join(t.TempDir(), "config.json")
	credentials := base64.StdEncoding.EncodeToString([]byte("robot:s3cret"))
	if err := os.WriteFile(authFile, []byte(`{"auths":{"https://ghcr.io":{"auth":"`+credentials+`"}}}`), 0o600); err != nil {
		t.Fatal(err)
	}
	docker.authFile = authFile
	var sent string
	engine.handlers["POST /images/create"] = func(w http.ResponseWriter, r *http.Request) {
		sent = r.Header.Get("X-Registry-Auth")
		_, _ = w.Write([]byte(`{"status":"done"}`))
	}
	if err := docker.PullImage(context.Background(), "ghcr.io/org/app@sha256:abc"); err != nil {
		t.Fatal(err)
	}
	decoded, err := base64.URLEncoding.DecodeString(sent)
	if err != nil {
		t.Fatalf("X-Registry-Auth %q is not base64url: %v", sent, err)
	}
	var auth map[string]string
	if err := json.Unmarshal(decoded, &auth); err != nil || auth["username"] != "robot" || auth["password"] != "s3cret" || auth["serveraddress"] != "ghcr.io" {
		t.Fatalf("auth = %v, %v", auth, err)
	}
	sent = "unchanged"
	if err := docker.PullImage(context.Background(), "quay.io/org/app@sha256:abc"); err != nil {
		t.Fatal(err)
	}
	if sent != "" {
		t.Fatalf("credentials for ghcr.io were sent to quay.io: %q", sent)
	}
}

func TestEngineErrorsAreTyped(t *testing.T) {
	engine, docker := newFakeEngine(t)
	engine.handlers["POST /containers/c1/start"] = func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusInternalServerError)
		_, _ = w.Write([]byte(`{"message":"port is already allocated"}`))
	}
	err := docker.Start(context.Background(), "c1")
	var apiErr *apiError
	if !errors.As(err, &apiErr) || apiErr.Status != 500 || !strings.Contains(err.Error(), "port is already allocated") {
		t.Fatalf("Start error = %v", err)
	}
	if err := docker.Start(context.Background(), "c2"); !errors.Is(err, ErrNotFound) {
		t.Fatalf("missing container = %v", err)
	}
}

func frame(kind byte, text string) []byte {
	header := make([]byte, 8)
	header[0] = kind
	binary.BigEndian.PutUint32(header[4:], uint32(len(text)))
	return append(header, text...)
}

func TestLogsAreDemultiplexed(t *testing.T) {
	engine, docker := newFakeEngine(t)
	engine.handlers["GET /containers/c1/logs"] = func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Query().Get("tail") != "50" {
			t.Errorf("tail = %q", r.URL.Query().Get("tail"))
		}
		_, _ = w.Write(append(frame(1, "out line\n"), frame(2, "err line\n")...))
	}
	logs, err := docker.Logs(context.Background(), "c1", 50)
	if err != nil || string(logs) != "out line\nerr line\n" {
		t.Fatalf("Logs = %q, %v", logs, err)
	}
	if got := string(demultiplex([]byte("plain tty output"))); got != "plain tty output" {
		t.Fatalf("unframed output = %q", got)
	}
}
