package runtime

import (
	"context"
	"encoding/base64"
	"encoding/binary"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strconv"
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
	// A tag next to the digest is only a label; the digest is pulled.
	if err := docker.PullImage(ctx, "ghcr.io/x/app:1.4@sha256:abc"); err == nil || !strings.Contains(err.Error(), "requires authentication") {
		t.Fatalf("repo:tag@digest = %v", err)
	}
	if err := docker.PullImage(ctx, "ghcr.io/x/app:latest"); err == nil {
		t.Fatal("pulled a mutable tag")
	}
}

// The volumes an image declares are read from its configuration, and a
// container whose image declares one that nothing covers is never made.
func TestCreateRefusesAnUncoveredImageVolume(t *testing.T) {
	engine, docker := newFakeEngine(t)
	engine.handlers["GET /images/sha256:img/json"] = func(w http.ResponseWriter, _ *http.Request) {
		_, _ = w.Write([]byte(`{"Config":{"Volumes":{"/data/child":{},"/tmp":{},"/data":{}}}}`))
	}
	engine.handlers["POST /containers/create"] = func(w http.ResponseWriter, r *http.Request) {
		_ = json.NewDecoder(r.Body).Decode(&engine.created)
		_, _ = w.Write([]byte(`{"Id":"c1"}`))
	}
	ctx := context.Background()
	if volumes, err := docker.ImageVolumes(ctx, "sha256:img"); err != nil || strings.Join(volumes, ",") != "/data,/data/child,/tmp" {
		t.Fatalf("ImageVolumes = %v, %v", volumes, err)
	}
	spec := ContainerSpec{Name: "murder-acme", Image: "sha256:img", Mounts: []Mount{{Source: "/srv/acme", Target: "/data"}}, Tmpfs: map[string]string{"/tmp": "size=64m"}}
	if _, err := docker.Create(ctx, spec); !errors.Is(err, ErrUncoveredVolume) || !strings.Contains(err.Error(), "/data/child") {
		t.Fatalf("Create = %v", err)
	}
	if engine.created != nil {
		t.Fatal("the container was made")
	}
	spec.Mounts = append(spec.Mounts, Mount{Source: "/srv/acme/child", Target: "/data/child"})
	if id, err := docker.Create(ctx, spec); err != nil || id != "c1" {
		t.Fatalf("Create = %q, %v", id, err)
	}
}

func TestCreateSendsHardenedContainer(t *testing.T) {
	engine, docker := newFakeEngine(t)
	engine.handlers["POST /containers/create"] = func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Query().Get("name") != "murder-acme" {
			t.Errorf("container name = %q", r.URL.Query().Get("name"))
		}
		_ = json.NewDecoder(r.Body).Decode(&engine.created)
		_, _ = w.Write([]byte(`{"Id":"c1"}`))
	}
	id, err := docker.Create(context.Background(), ContainerSpec{
		Name: "murder-acme", Image: "sha256:img", Env: []string{"A=1"}, User: "10001", Network: "murder-acme",
		Address: "10.240.0.18", Mounts: []Mount{{Source: "/srv/acme", Target: "/data"}},
		MemoryBytes: 512 << 20, NanoCPUs: 1e9, PIDs: 256, ReadOnlyRoot: true, Tmpfs: map[string]string{"/tmp": "size=64m"},
		Labels: map[string]string{LabelTenant: "acme"},
	})
	if err != nil || id != "c1" {
		t.Fatalf("Create = %q, %v", id, err)
	}
	host := engine.created["HostConfig"].(map[string]any)
	if _, published := host["PortBindings"]; published || engine.created["ExposedPorts"] != nil {
		t.Errorf("the container publishes ports: %v", engine.created)
	}
	endpoint := engine.created["NetworkingConfig"].(map[string]any)["EndpointsConfig"].(map[string]any)["murder-acme"].(map[string]any)
	if endpoint["IPAMConfig"].(map[string]any)["IPv4Address"] != "10.240.0.18" {
		t.Errorf("endpoint = %v, want the fixed address", endpoint)
	}
	if host["Memory"] != float64(512<<20) || host["MemorySwap"] != float64(512<<20) {
		t.Errorf("memory = %v, swap = %v; want 512 MB and no swap beyond it", host["Memory"], host["MemorySwap"])
	}
	if host["ReadonlyRootfs"] != true || host["NetworkMode"] != "murder-acme" || host["PidsLimit"] != float64(256) {
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
		_, _ = w.Write([]byte(`[{"Id":"c1","Names":["/murder-acme"],"State":"running","Labels":{"io.murder.tenant":"acme"}},{"Id":"c2","Names":["/murder-beta"],"State":"exited","Labels":{}}]`))
	}
	containers, err := docker.List(ctx)
	if err != nil || len(containers) != 2 || !containers[0].Running || containers[1].Running || containers[0].Name != "murder-acme" {
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
	engine.handlers["GET /networks/murder-acme"] = func(w http.ResponseWriter, _ *http.Request) {
		if !created {
			w.WriteHeader(http.StatusNotFound)
			return
		}
		_, _ = w.Write([]byte(`{"Labels":{"io.murder.managed":"true","io.murder.node":"node-1"},"IPAM":{"Config":[{"Subnet":"10.240.0.16/28"}]}}`))
	}
	// A network with IPv6 (the engine's defaults can turn it on) is refused:
	// the tenant firewall covers IPv4 only.
	engine.handlers["GET /networks/murder-v6"] = func(w http.ResponseWriter, _ *http.Request) {
		_, _ = w.Write([]byte(`{"EnableIPv6":true,"Labels":{"io.murder.managed":"true","io.murder.node":"node-1"},"IPAM":{"Config":[{"Subnet":"fd00::/64"},{"Subnet":"10.240.0.32/28"}]}}`))
	}
	engine.handlers["GET /networks/murder-foreign"] = func(w http.ResponseWriter, _ *http.Request) {
		_, _ = w.Write([]byte(`{"Labels":{"io.murder.managed":"true","io.murder.node":"node-2"},"IPAM":{"Config":[{"Subnet":"10.240.0.16/28"}]}}`))
	}
	engine.handlers["POST /networks/create"] = func(w http.ResponseWriter, r *http.Request) {
		var body map[string]any
		_ = json.NewDecoder(r.Body).Decode(&body)
		ipam := body["IPAM"].(map[string]any)["Config"].([]any)[0].(map[string]any)
		labels := body["Labels"].(map[string]any)
		if body["Name"] != "murder-acme" || ipam["Subnet"] != "10.240.0.16/28" || ipam["Gateway"] != "10.240.0.17" || labels[LabelNode] != "node-1" || body["Options"].(map[string]any)["com.docker.network.bridge.enable_icc"] != "false" || body["EnableIPv6"] != false {
			t.Errorf("network create = %v", body)
		}
		created = true
		w.WriteHeader(http.StatusCreated)
		_, _ = w.Write([]byte(`{"Id":"n1"}`))
	}
	if err := docker.EnsureNetwork(ctx, "murder-acme", "10.240.0.16/28", map[string]string{LabelTenant: "acme"}); err != nil {
		t.Fatal(err)
	}
	if err := docker.EnsureNetwork(ctx, "murder-acme", "10.240.0.16/28", nil); err != nil {
		t.Fatalf("an existing network with the right subnet = %v", err)
	}
	if err := docker.EnsureNetwork(ctx, "murder-v6", "10.240.0.32/28", nil); err == nil || !strings.Contains(err.Error(), "IPv6") {
		t.Fatalf("a network with IPv6 = %v", err)
	}
	if err := docker.EnsureNetwork(ctx, "murder-acme", "10.240.0.32/28", nil); !errors.Is(err, ErrNetworkMismatch) {
		t.Fatalf("an existing network with another subnet = %v", err)
	}
	// Another node's network is neither adopted nor removed.
	if err := docker.EnsureNetwork(ctx, "murder-foreign", "10.240.0.16/28", nil); err == nil || !strings.Contains(err.Error(), "not owned") {
		t.Fatalf("adopting another node's network = %v", err)
	}
	if err := docker.RemoveNetwork(ctx, "murder-foreign"); !errors.Is(err, ErrNotOwned) {
		t.Fatalf("removing another node's network = %v, want ErrNotOwned", err)
	}
	for _, request := range engine.requests {
		if strings.HasPrefix(request, "DELETE /networks/murder-foreign") {
			t.Fatal("removed another node's network")
		}
	}
	engine.handlers["GET /networks"] = func(w http.ResponseWriter, r *http.Request) {
		if !strings.Contains(r.URL.Query().Get("filters"), LabelNode+"=node-1") {
			t.Errorf("network list filters = %s", r.URL.Query().Get("filters"))
		}
		_, _ = w.Write([]byte(`[{"Name":"murder-acme","Labels":{"io.murder.tenant":"acme"}}]`))
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

func TestPullSignsInWithTheConfiguredRegistries(t *testing.T) {
	engine, docker := newFakeEngine(t)
	dir := t.TempDir()
	ghcrToken, hubToken := filepath.Join(dir, "ghcr.token"), filepath.Join(dir, "hub.token")
	if err := os.WriteFile(ghcrToken, []byte("ghp_first\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(hubToken, []byte("dckr_pat"), 0o600); err != nil {
		t.Fatal(err)
	}
	// A login comes before the auth file's entry for the same registry.
	authFile := filepath.Join(dir, "config.json")
	if err := os.WriteFile(authFile, []byte(`{"auths":{"ghcr.io":{"auth":"`+base64.StdEncoding.EncodeToString([]byte("old:old"))+`"},"quay.io":{"auth":"`+base64.StdEncoding.EncodeToString([]byte("q:q"))+`"}}}`), 0o600); err != nil {
		t.Fatal(err)
	}
	docker.authFile = authFile
	docker.registries = []RegistryLogin{{Host: "GHCR.io", Username: "bot", TokenFile: ghcrToken}, {Host: "docker.io", Username: "hubber", TokenFile: hubToken}}
	var sent string
	engine.handlers["POST /images/create"] = func(w http.ResponseWriter, r *http.Request) {
		sent = r.Header.Get("X-Registry-Auth")
		_, _ = w.Write([]byte(`{"status":"done"}`))
	}
	login := func(ref string) map[string]string {
		t.Helper()
		sent = ""
		if err := docker.PullImage(context.Background(), ref); err != nil {
			t.Fatal(err)
		}
		if sent == "" {
			return nil
		}
		decoded, _ := base64.URLEncoding.DecodeString(sent)
		var auth map[string]string
		_ = json.Unmarshal(decoded, &auth)
		return auth
	}
	if auth := login("ghcr.io/org/app@sha256:abc"); auth["username"] != "bot" || auth["password"] != "ghp_first" || auth["serveraddress"] != "ghcr.io" {
		t.Fatalf("ghcr.io login = %v", auth)
	}
	// A new token in the file is used at the next pull.
	if err := os.WriteFile(ghcrToken, []byte("ghp_second"), 0o600); err != nil {
		t.Fatal(err)
	}
	if auth := login("ghcr.io/org/app@sha256:abc"); auth["password"] != "ghp_second" {
		t.Fatalf("after a new token = %v", auth)
	}
	for _, ref := range []string{"postgres@sha256:abc", "library/postgres@sha256:abc", "docker.io/library/postgres@sha256:abc", "index.docker.io/x/y@sha256:abc"} {
		if auth := login(ref); auth["username"] != "hubber" || auth["serveraddress"] != "docker.io" {
			t.Errorf("Docker Hub login for %s = %v", ref, auth)
		}
	}
	// The auth file still serves the registries no login names.
	if auth := login("quay.io/org/app@sha256:abc"); auth["username"] != "q" {
		t.Fatalf("quay.io from the auth file = %v", auth)
	}
	if auth := login("registry.example.com/app@sha256:abc"); auth != nil {
		t.Fatalf("credentials sent to a registry with none: %v", auth)
	}
	// A token that cannot be read fails the pull rather than pulling
	// without it.
	if err := os.Remove(ghcrToken); err != nil {
		t.Fatal(err)
	}
	if err := docker.PullImage(context.Background(), "ghcr.io/org/app@sha256:abc"); err == nil || !strings.Contains(err.Error(), "read the token for GHCR.io") {
		t.Fatalf("pull without its token = %v", err)
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

func TestLatestAsksTheRegistryWithoutPulling(t *testing.T) {
	engine, docker := newFakeEngine(t)
	ctx := context.Background()
	const digest = "sha256:1111111111111111111111111111111111111111111111111111111111111111"
	authFile := filepath.Join(t.TempDir(), "config.json")
	if err := os.WriteFile(authFile, []byte(`{"auths":{"ghcr.io":{"auth":"`+base64.StdEncoding.EncodeToString([]byte("bot:secret"))+`"}}}`), 0o600); err != nil {
		t.Fatal(err)
	}
	docker.authFile = authFile
	engine.handlers["POST /images/create"] = func(http.ResponseWriter, *http.Request) {
		t.Error("asking about a tag pulled it")
	}
	engine.handlers["GET /distribution/ghcr.io/org/app:stable/json"] = func(w http.ResponseWriter, r *http.Request) {
		auth, _ := base64.URLEncoding.DecodeString(r.Header.Get("X-Registry-Auth"))
		if !strings.Contains(string(auth), `"username":"bot"`) {
			t.Errorf("registry auth = %s", auth)
		}
		_, _ = w.Write([]byte(`{"Descriptor":{"mediaType":"application/vnd.oci.image.index.v1+json","digest":"` + digest + `","size":9226},"Platforms":[]}`))
	}
	if pinned, err := docker.Latest(ctx, "ghcr.io/org/app:stable"); err != nil || pinned != "ghcr.io/org/app@"+digest {
		t.Fatalf("Latest = %q, %v", pinned, err)
	}
	// Docker Hub's names come back short, as Resolve pins them.
	engine.handlers["GET /distribution/docker.io/library/postgres:latest/json"] = func(w http.ResponseWriter, _ *http.Request) {
		_, _ = w.Write([]byte(`{"Descriptor":{"digest":"` + digest + `"}}`))
	}
	if pinned, err := docker.Latest(ctx, "docker.io/library/postgres"); err != nil || pinned != "postgres@"+digest {
		t.Fatalf("Latest(hub) = %q, %v", pinned, err)
	}
	engine.handlers["GET /distribution/ghcr.io/org/gone:1/json"] = func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusForbidden)
		_, _ = w.Write([]byte(`{"message":"denied"}`))
	}
	if _, err := docker.Latest(ctx, "ghcr.io/org/gone:1"); err == nil || !strings.Contains(err.Error(), "denied") {
		t.Fatalf("a refused question: %v", err)
	}
	engine.handlers["GET /distribution/ghcr.io/org/odd:1/json"] = func(w http.ResponseWriter, _ *http.Request) {
		_, _ = w.Write([]byte(`{"Descriptor":{"digest":"md5:abc"}}`))
	}
	if _, err := docker.Latest(ctx, "ghcr.io/org/odd:1"); err == nil {
		t.Fatal("an odd digest was taken")
	}
	if _, err := docker.Latest(ctx, "ghcr.io/org/app@"+digest); err == nil {
		t.Fatal("a pinned image was asked about")
	}
}

func TestResolvePinsTags(t *testing.T) {
	engine, docker := newFakeEngine(t)
	ctx := context.Background()
	const digest = "sha256:1111111111111111111111111111111111111111111111111111111111111111"
	if pinned, _, err := docker.Resolve(ctx, "ghcr.io/org/app@"+digest); err != nil || pinned != "ghcr.io/org/app@"+digest {
		t.Fatalf("a pinned reference = %q, %v", pinned, err)
	}
	pulls := 0
	engine.handlers["POST /images/create"] = func(w http.ResponseWriter, r *http.Request) {
		pulls++
		if r.URL.Query().Get("fromImage") == "local/app" {
			_, _ = w.Write([]byte(`{"error":"pull access denied for local/app"}`))
			return
		}
		if r.URL.Query().Get("tag") != "1.4" && r.URL.Query().Get("fromImage") == "ghcr.io/org/app" {
			t.Errorf("pull query = %s", r.URL.RawQuery)
		}
		_, _ = w.Write([]byte(`{"status":"done"}`))
	}
	engine.handlers["GET /images/ghcr.io/org/app:1.4/json"] = func(w http.ResponseWriter, _ *http.Request) {
		_, _ = w.Write([]byte(`{"Id":"sha256:2222222222222222222222222222222222222222222222222222222222222222","RepoDigests":["other/app@sha256:9999999999999999999999999999999999999999999999999999999999999999","ghcr.io/org/app@` + digest + `"]}`))
	}
	pinned, note, err := docker.Resolve(ctx, "ghcr.io/org/app:1.4")
	if err != nil || pinned != "ghcr.io/org/app@"+digest || note != "" {
		t.Fatalf("Resolve(tag) = %q, %q, %v", pinned, note, err)
	}
	// The engine reports Docker Hub digests in their short form; every
	// spelling of the name pins to the same short reference.
	engine.handlers["POST /images/create"] = func(w http.ResponseWriter, _ *http.Request) {
		_, _ = w.Write([]byte(`{"status":"done"}`))
	}
	hub := func(w http.ResponseWriter, _ *http.Request) {
		_, _ = w.Write([]byte(`{"Id":"sha256:4444444444444444444444444444444444444444444444444444444444444444","RepoDigests":["postgres@` + digest + `"]}`))
	}
	for _, ref := range []string{"postgres:17", "library/postgres:17", "docker.io/library/postgres:17", "index.docker.io/library/postgres:17"} {
		engine.handlers["GET /images/"+ref+"/json"] = hub
		if pinned, _, err := docker.Resolve(ctx, ref); err != nil || pinned != "postgres@"+digest {
			t.Errorf("Resolve(%s) = %q, %v", ref, pinned, err)
		}
	}
	engine.handlers["POST /images/create"] = func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Query().Get("fromImage") == "local/app" {
			_, _ = w.Write([]byte(`{"error":"pull access denied for local/app"}`))
			return
		}
		_, _ = w.Write([]byte(`{"status":"done"}`))
	}
	// A local image that cannot be pulled is pinned to its ID, with a note.
	// With containerd's image store a local build has a repository digest
	// too; it names no registry's image.
	engine.handlers["GET /images/local/app:latest/json"] = func(w http.ResponseWriter, _ *http.Request) {
		_, _ = w.Write([]byte(`{"Id":"sha256:3333333333333333333333333333333333333333333333333333333333333333","RepoDigests":["local/app@sha256:3333333333333333333333333333333333333333333333333333333333333333"]}`))
	}
	pinned, note, err = docker.Resolve(ctx, "local/app")
	if err != nil || pinned != "sha256:3333333333333333333333333333333333333333333333333333333333333333" || !strings.Contains(note, "local image") {
		t.Fatalf("Resolve(local) = %q, %q, %v", pinned, note, err)
	}
	// A registry that only failed to answer keeps the digest other nodes
	// can pull.
	engine.handlers["POST /images/create"] = func(w http.ResponseWriter, _ *http.Request) {
		_, _ = w.Write([]byte(`{"error":"received unexpected HTTP status: 503 Service Unavailable"}`))
	}
	engine.handlers["GET /images/postgres:17/json"] = hub
	if pinned, note, err := docker.Resolve(ctx, "postgres:17"); err != nil || pinned != "postgres@"+digest || !strings.Contains(note, "local image") {
		t.Fatalf("Resolve while the registry fails = %q, %q, %v", pinned, note, err)
	}
	// Neither pullable nor present: the pull error comes back.
	engine.handlers["POST /images/create"] = func(w http.ResponseWriter, _ *http.Request) {
		_, _ = w.Write([]byte(`{"error":"manifest unknown"}`))
	}
	if _, _, err := docker.Resolve(ctx, "ghcr.io/org/missing:1"); err == nil || !strings.Contains(err.Error(), "manifest unknown") {
		t.Fatalf("Resolve(missing) = %v", err)
	}
	if pulls == 0 {
		t.Fatal("no pull was attempted")
	}
	for ref, want := range map[string][2]string{
		"nginx":                        {"nginx", "latest"},
		"nginx:1.27":                   {"nginx", "1.27"},
		"registry.local:5000/team/app": {"registry.local:5000/team/app", "latest"},
		"registry.local:5000/app:v2":   {"registry.local:5000/app", "v2"},
	} {
		if repository, tag := splitTag(ref); repository != want[0] || tag != want[1] {
			t.Errorf("splitTag(%q) = %q, %q", ref, repository, tag)
		}
	}
}

func TestAddresses(t *testing.T) {
	gateway, container, err := Addresses("10.240.0.16/28")
	if err != nil || gateway.String() != "10.240.0.17" || container.String() != "10.240.0.18" {
		t.Fatalf("Addresses = %v, %v, %v", gateway, container, err)
	}
	for _, bad := range []string{"", "10.240.0.17/28", "10.240.0.16/30", "fd00::/64", "nonsense"} {
		if _, _, err := Addresses(bad); err == nil {
			t.Errorf("Addresses(%q) accepted", bad)
		}
	}
}

// ImageID finds an image's ID; RemoveImage deletes by ID, so the tags go
// with the digest, and reports an image the engine keeps.
func TestImageIDAndRemoveImage(t *testing.T) {
	engine, docker := newFakeEngine(t)
	ctx := context.Background()
	engine.handlers["GET /images/ghcr.io/x/app@sha256:old/json"] = func(w http.ResponseWriter, _ *http.Request) {
		_, _ = w.Write([]byte(`{"Id":"sha256:id-old"}`))
	}
	if id, err := docker.ImageID(ctx, "ghcr.io/x/app@sha256:old"); err != nil || id != "sha256:id-old" {
		t.Fatalf("ImageID = %q, %v", id, err)
	}
	if id, err := docker.ImageID(ctx, "ghcr.io/x/app@sha256:gone"); err != nil || id != "" {
		t.Fatalf("ImageID of a missing image = %q, %v", id, err)
	}
	engine.handlers["DELETE /images/sha256:id-old"] = func(w http.ResponseWriter, _ *http.Request) { _, _ = w.Write([]byte(`[]`)) }
	if err := docker.RemoveImage(ctx, "sha256:id-old"); err != nil {
		t.Fatal(err)
	}
	if err := docker.RemoveImage(ctx, "sha256:id-gone"); err != nil {
		t.Fatalf("a missing image = %v", err)
	}
	engine.handlers["DELETE /images/sha256:id-used"] = func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusConflict)
		_, _ = w.Write([]byte(`{"message":"conflict: unable to delete (cannot be forced) - image is being used by running container"}`))
	}
	if err := docker.RemoveImage(ctx, "sha256:id-used"); !errors.Is(err, ErrInUse) {
		t.Fatalf("an image in use = %v", err)
	}
	for _, request := range engine.requests {
		if strings.HasPrefix(request, "DELETE") && strings.Contains(request, "force") {
			t.Fatalf("forced a removal: %s", request)
		}
	}
}

func TestInspectReportsRestartsAndTheLastExit(t *testing.T) {
	engine, docker := newFakeEngine(t)
	engine.handlers["GET /containers/crashing/json"] = func(w http.ResponseWriter, _ *http.Request) {
		_, _ = w.Write([]byte(`{"RestartCount":3,"State":{"Pid":0,"Restarting":true,"OOMKilled":true,"ExitCode":137,"FinishedAt":"2026-10-29T15:02:00.123Z"}}`))
	}
	engine.handlers["GET /containers/fresh/json"] = func(w http.ResponseWriter, _ *http.Request) {
		_, _ = w.Write([]byte(`{"RestartCount":0,"State":{"Pid":4242,"FinishedAt":"0001-01-01T00:00:00Z"}}`))
	}
	state, err := docker.Inspect(context.Background(), "crashing")
	want := ContainerState{Restarting: true, Restarts: 3, ExitKnown: true, OOMKilled: true, ExitCode: 137, ExitedAt: time.Date(2026, 10, 29, 15, 2, 0, 123e6, time.UTC)}
	if err != nil || state != want {
		t.Fatalf("Inspect = %+v, %v; want %+v", state, err, want)
	}
	if state, err := docker.Inspect(context.Background(), "fresh"); err != nil || state != (ContainerState{Pid: 4242}) {
		t.Fatalf("Inspect = %+v, %v; want only the PID", state, err)
	}
	if _, err := docker.Inspect(context.Background(), "gone"); !errors.Is(err, ErrNotFound) {
		t.Fatalf("Inspect of a missing container = %v", err)
	}
}

// A process that runs again has its last exit looked up in the engine's
// event log, once per exit.
func TestInspectFindsTheLastExitInTheEventLog(t *testing.T) {
	engine, docker := newFakeEngine(t)
	exited := time.Date(2026, 10, 29, 15, 2, 0, 0, time.UTC)
	engine.handlers["GET /containers/leaky/json"] = func(w http.ResponseWriter, _ *http.Request) {
		_, _ = w.Write([]byte(`{"RestartCount":1,"State":{"Pid":4242,"OOMKilled":false,"ExitCode":0,"FinishedAt":"2026-10-29T15:02:00Z"}}`))
	}
	lookups := 0
	engine.handlers["GET /events"] = func(w http.ResponseWriter, r *http.Request) {
		lookups++
		since, _ := strconv.ParseFloat(r.URL.Query().Get("since"), 64)
		until, _ := strconv.ParseFloat(r.URL.Query().Get("until"), 64)
		if since > float64(exited.Unix()) || until < float64(exited.Unix()) || until > float64(time.Now().Unix()+1) || !strings.Contains(r.URL.Query().Get("filters"), `"leaky"`) {
			t.Errorf("events query = %v", r.URL.Query())
		}
		at := exited.UnixNano()
		fmt.Fprintf(w, `{"Action":"die","timeNano":%d,"Actor":{"Attributes":{"exitCode":"1"}}}`+"\n", at-int64(3*time.Second))
		fmt.Fprintf(w, `{"Action":"oom","timeNano":%d,"Actor":{"Attributes":{}}}`+"\n", at-int64(time.Millisecond))
		fmt.Fprintf(w, `{"Action":"die","timeNano":%d,"Actor":{"Attributes":{"exitCode":"137"}}}`+"\n", at)
	}
	for range 2 {
		state, err := docker.Inspect(context.Background(), "leaky")
		if err != nil || !state.ExitKnown || state.ExitCode != 137 || !state.OOMKilled || state.Pid != 4242 || state.Restarts != 1 {
			t.Fatalf("Inspect = %+v, %v", state, err)
		}
	}
	if lookups != 1 {
		t.Fatalf("the event log was read %d times for one exit", lookups)
	}
}

// An out-of-memory kill belongs to the exit right after it, not to a later
// one.
func TestInspectDoesNotBlameAnEarlierOOM(t *testing.T) {
	engine, docker := newFakeEngine(t)
	engine.handlers["GET /containers/flaky/json"] = func(w http.ResponseWriter, _ *http.Request) {
		_, _ = w.Write([]byte(`{"RestartCount":2,"State":{"Pid":4242,"FinishedAt":"2026-10-29T15:02:00.3Z"}}`))
	}
	engine.handlers["GET /events"] = func(w http.ResponseWriter, _ *http.Request) {
		killed := time.Date(2026, 10, 29, 15, 2, 0, 0, time.UTC).UnixNano()
		fmt.Fprintf(w, `{"Action":"oom","timeNano":%d}`+"\n", killed)
		fmt.Fprintf(w, `{"Action":"die","timeNano":%d,"Actor":{"Attributes":{"exitCode":"137"}}}`+"\n", killed+int64(time.Millisecond))
		fmt.Fprintf(w, `{"Action":"die","timeNano":%d,"Actor":{"Attributes":{"exitCode":"1"}}}`+"\n", killed+int64(300*time.Millisecond))
	}
	state, err := docker.Inspect(context.Background(), "flaky")
	if err != nil || !state.ExitKnown || state.ExitCode != 1 || state.OOMKilled {
		t.Fatalf("Inspect = %+v, %v; want exit code 1, not killed", state, err)
	}
}

// The kernel's out-of-memory notice and the exit race: an "oom" event
// logged just after the exit is that exit's, and an exit looked up before
// the event is logged is looked up again.
func TestInspectTakesALateOOMEvent(t *testing.T) {
	engine, docker := newFakeEngine(t)
	exited := time.Now().UTC().Add(-time.Second)
	engine.handlers["GET /containers/leaky/json"] = func(w http.ResponseWriter, _ *http.Request) {
		fmt.Fprintf(w, `{"RestartCount":1,"State":{"Pid":4242,"FinishedAt":%q}}`, exited.Format(time.RFC3339Nano))
	}
	logged := false
	engine.handlers["GET /events"] = func(w http.ResponseWriter, _ *http.Request) {
		fmt.Fprintf(w, `{"Action":"die","timeNano":%d,"Actor":{"Attributes":{"exitCode":"137"}}}`+"\n", exited.UnixNano())
		if logged {
			fmt.Fprintf(w, `{"Action":"oom","timeNano":%d}`+"\n", exited.UnixNano()+int64(2*time.Millisecond))
		}
	}
	if state, err := docker.Inspect(context.Background(), "leaky"); err != nil || state.ExitCode != 137 || state.OOMKilled {
		t.Fatalf("before the oom event: %+v, %v", state, err)
	}
	logged = true
	if state, err := docker.Inspect(context.Background(), "leaky"); err != nil || state.ExitCode != 137 || !state.OOMKilled {
		t.Fatalf("after the oom event: %+v, %v", state, err)
	}
}

// A container's restart policy is read with its state and changed in
// place; new containers get the policy that leaves starts at boot to the
// agent.
func TestTheRestartPolicyIsReadAndChanged(t *testing.T) {
	engine, docker := newFakeEngine(t)
	engine.handlers["GET /containers/old/json"] = func(w http.ResponseWriter, _ *http.Request) {
		_, _ = w.Write([]byte(`{"State":{"Pid":42},"HostConfig":{"RestartPolicy":{"Name":"unless-stopped"}}}`))
	}
	var update map[string]map[string]string
	engine.handlers["POST /containers/old/update"] = func(w http.ResponseWriter, r *http.Request) {
		if err := json.NewDecoder(r.Body).Decode(&update); err != nil {
			t.Error(err)
		}
		_, _ = w.Write([]byte(`{"Warnings":[]}`))
	}
	state, err := docker.Inspect(context.Background(), "old")
	if err != nil || state.RestartPolicy != "unless-stopped" {
		t.Fatalf("Inspect = %+v, %v", state, err)
	}
	if err := docker.SetRestartPolicy(context.Background(), "old", RestartPolicy); err != nil || update["RestartPolicy"]["Name"] != "no" {
		t.Fatalf("update = %v, %v", update, err)
	}
}
