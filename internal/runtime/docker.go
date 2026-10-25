package runtime

import (
	"bytes"
	"context"
	"encoding/base64"
	"encoding/binary"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"maps"
	"net"
	"net/http"
	"net/url"
	"os"
	"strconv"
	"strings"
	"time"
)

// apiVersion is the oldest Engine API that every supported Docker serves
// (Docker 25 and later) and that has every field used here.
const apiVersion = "v1.44"

// Call deadlines for engine requests whose context has none.
const (
	callTimeout = time.Minute
	pullTimeout = 15 * time.Minute
)

// DockerOptions configures the Docker runtime.
type DockerOptions struct {
	// Socket is the engine's unix socket.
	Socket string
	// Node labels every object this agent creates and limits List and
	// ListNetworks to them.
	Node string
	// RegistryAuthFile, if set, is a Docker config.json whose "auths" entries
	// hold registry credentials ("auth" is base64 of user:password).
	// Credential helpers are not supported.
	RegistryAuthFile string
}

// Docker talks to the Docker Engine API over its unix socket.
type Docker struct {
	client   *http.Client
	base     string
	node     string
	authFile string
}

// NewDocker returns a client for the engine described by options.
func NewDocker(options DockerOptions) *Docker {
	transport := &http.Transport{
		DialContext: func(ctx context.Context, _, _ string) (net.Conn, error) {
			var dialer net.Dialer
			return dialer.DialContext(ctx, "unix", options.Socket)
		},
	}
	return &Docker{client: &http.Client{Transport: transport}, base: "http://docker/" + apiVersion, node: options.Node, authFile: options.RegistryAuthFile}
}

// newDockerForURL points the client at an HTTP server; tests use it.
func newDockerForURL(base string, client *http.Client, node string) *Docker {
	return &Docker{client: client, base: strings.TrimSuffix(base, "/") + "/" + apiVersion, node: node}
}

// apiError is a non-2xx Engine API answer.
type apiError struct {
	Status  int
	Message string
}

func (e *apiError) Error() string { return fmt.Sprintf("docker: %d %s", e.Status, e.Message) }

type request struct {
	method, path string
	query        url.Values
	body         any
	header       http.Header
	timeout      time.Duration
}

func (d *Docker) do(ctx context.Context, call request, out any) error {
	if _, ok := ctx.Deadline(); !ok {
		timeout := call.timeout
		if timeout == 0 {
			timeout = callTimeout
		}
		var cancel context.CancelFunc
		ctx, cancel = context.WithTimeout(ctx, timeout)
		defer cancel()
	}
	var reader io.Reader
	if call.body != nil {
		encoded, err := json.Marshal(call.body)
		if err != nil {
			return err
		}
		reader = bytes.NewReader(encoded)
	}
	target := d.base + call.path
	if len(call.query) > 0 {
		target += "?" + call.query.Encode()
	}
	httpRequest, err := http.NewRequestWithContext(ctx, call.method, target, reader)
	if err != nil {
		return err
	}
	maps.Copy(httpRequest.Header, call.header)
	if call.body != nil {
		httpRequest.Header.Set("Content-Type", "application/json")
	}
	response, err := d.client.Do(httpRequest)
	if err != nil {
		return err
	}
	defer response.Body.Close()
	if response.StatusCode == http.StatusNotModified {
		return nil
	}
	if response.StatusCode < 200 || response.StatusCode > 299 {
		var payload struct {
			Message string `json:"message"`
		}
		data, _ := io.ReadAll(io.LimitReader(response.Body, 1<<16))
		if json.Unmarshal(data, &payload) != nil || payload.Message == "" {
			payload.Message = strings.TrimSpace(string(data))
		}
		if response.StatusCode == http.StatusNotFound {
			return fmt.Errorf("%w: %s", ErrNotFound, payload.Message)
		}
		return &apiError{Status: response.StatusCode, Message: payload.Message}
	}
	switch out := out.(type) {
	case nil:
		_, err = io.Copy(io.Discard, response.Body)
		return err
	case *[]byte:
		*out, err = io.ReadAll(response.Body)
		return err
	default:
		return json.NewDecoder(response.Body).Decode(out)
	}
}

// ownLabels adds the managed and node labels to labels.
func (d *Docker) ownLabels(labels map[string]string) map[string]string {
	owned := maps.Clone(labels)
	if owned == nil {
		owned = map[string]string{}
	}
	owned[LabelManaged] = "true"
	owned[LabelNode] = d.node
	return owned
}

func (d *Docker) ownFilter() string {
	filters, _ := json.Marshal(map[string][]string{"label": {LabelManaged + "=true", LabelNode + "=" + d.node}})
	return string(filters)
}

func (d *Docker) HasImage(ctx context.Context, ref string) (bool, error) {
	err := d.do(ctx, request{method: http.MethodGet, path: "/images/" + url.PathEscape(ref) + "/json"}, nil)
	if errors.Is(err, ErrNotFound) {
		return false, nil
	}
	return err == nil, err
}

// PullImage pulls a digest-pinned reference. The engine streams progress as
// JSON lines and reports a failed pull inside that stream.
func (d *Docker) PullImage(ctx context.Context, ref string) error {
	repository, digest, ok := strings.Cut(ref, "@")
	if !ok {
		return fmt.Errorf("pull %s: only digest-pinned references can be pulled; a local image ID must already be present", ref)
	}
	header := http.Header{}
	if auth, err := d.registryAuth(repository); err != nil {
		return fmt.Errorf("pull %s: %w", ref, err)
	} else if auth != "" {
		header.Set("X-Registry-Auth", auth)
	}
	var stream []byte
	call := request{method: http.MethodPost, path: "/images/create", query: url.Values{"fromImage": {repository}, "tag": {digest}}, header: header, timeout: pullTimeout}
	if err := d.do(ctx, call, &stream); err != nil {
		return fmt.Errorf("pull %s: %w", ref, err)
	}
	decoder := json.NewDecoder(bytes.NewReader(stream))
	for {
		var message struct {
			Error string `json:"error"`
		}
		if err := decoder.Decode(&message); err != nil {
			if errors.Is(err, io.EOF) {
				return nil
			}
			return fmt.Errorf("pull %s: read progress: %w", ref, err)
		}
		if message.Error != "" {
			return fmt.Errorf("pull %s: %s", ref, message.Error)
		}
	}
}

// registryAuth returns the X-Registry-Auth value for repository's registry,
// or "" when no credentials are configured for it. The engine does not read
// the credentials "docker login" saves; the client must send them.
func (d *Docker) registryAuth(repository string) (string, error) {
	if d.authFile == "" {
		return "", nil
	}
	data, err := os.ReadFile(d.authFile)
	if err != nil {
		return "", fmt.Errorf("read registry auth: %w", err)
	}
	var config struct {
		Auths map[string]struct {
			Auth string `json:"auth"`
		} `json:"auths"`
	}
	if err := json.Unmarshal(data, &config); err != nil {
		return "", fmt.Errorf("parse registry auth %s: %w", d.authFile, err)
	}
	registry := "docker.io"
	if first, _, ok := strings.Cut(repository, "/"); ok && (strings.ContainsAny(first, ".:") || first == "localhost") {
		registry = first
	}
	for host, entry := range config.Auths {
		host = strings.TrimSuffix(strings.TrimPrefix(strings.TrimPrefix(host, "https://"), "http://"), "/")
		if host == registry || (registry == "docker.io" && strings.HasPrefix(host, "index.docker.io")) {
			decoded, err := base64.StdEncoding.DecodeString(entry.Auth)
			if err != nil {
				return "", fmt.Errorf("registry auth for %s is not base64", host)
			}
			user, password, ok := strings.Cut(string(decoded), ":")
			if !ok {
				return "", fmt.Errorf("registry auth for %s is not user:password", host)
			}
			encoded, _ := json.Marshal(map[string]string{"username": user, "password": password, "serveraddress": registry})
			return base64.URLEncoding.EncodeToString(encoded), nil
		}
	}
	return "", nil
}

func (d *Docker) EnsureNetwork(ctx context.Context, name, subnet string, labels map[string]string) error {
	existing, owned, err := d.inspectNetwork(ctx, name)
	switch {
	case err == nil && !owned:
		return fmt.Errorf("network %s exists but is %w %s", name, ErrNotOwned, d.node)
	case err == nil && existing == subnet:
		return nil
	case err == nil:
		return fmt.Errorf("network %s has subnet %s, want %s", name, existing, subnet)
	case !errors.Is(err, ErrNotFound):
		return err
	}
	create := map[string]any{
		"Name":           name,
		"Driver":         "bridge",
		"CheckDuplicate": true,
		"Labels":         d.ownLabels(labels),
		// Corral assigns each tenant a small subnet from its own pool, so the
		// engine's default pools (about thirty networks) are never used up.
		"IPAM": map[string]any{"Driver": "default", "Config": []map[string]string{{"Subnet": subnet}}},
		// Containers on one tenant network cannot reach each other; the edge
		// proxy reaches the tenant through its published loopback port.
		"Options": map[string]string{"com.docker.network.bridge.enable_icc": "false"},
	}
	if err := d.do(ctx, request{method: http.MethodPost, path: "/networks/create", body: create}, nil); err != nil {
		return fmt.Errorf("create network %s: %w", name, err)
	}
	return nil
}

// inspectNetwork returns a network's IPv4 subnet ("" if it has none) and
// whether this node owns it.
func (d *Docker) inspectNetwork(ctx context.Context, name string) (string, bool, error) {
	var network struct {
		Labels map[string]string `json:"Labels"`
		IPAM   struct {
			Config []struct {
				Subnet string `json:"Subnet"`
			} `json:"Config"`
		} `json:"IPAM"`
	}
	if err := d.do(ctx, request{method: http.MethodGet, path: "/networks/" + url.PathEscape(name)}, &network); err != nil {
		return "", false, err
	}
	owned := network.Labels[LabelManaged] == "true" && network.Labels[LabelNode] == d.node
	for _, config := range network.IPAM.Config {
		if config.Subnet != "" && !strings.Contains(config.Subnet, ":") {
			return config.Subnet, owned, nil
		}
	}
	return "", owned, nil
}

func (d *Docker) RemoveNetwork(ctx context.Context, name string) error {
	_, owned, err := d.inspectNetwork(ctx, name)
	if errors.Is(err, ErrNotFound) {
		return nil
	}
	if err == nil && !owned {
		return fmt.Errorf("network %s is %w %s; left in place", name, ErrNotOwned, d.node)
	}
	if err != nil {
		return err
	}
	err = d.do(ctx, request{method: http.MethodDelete, path: "/networks/" + url.PathEscape(name)}, nil)
	if errors.Is(err, ErrNotFound) {
		return nil
	}
	return err
}

func (d *Docker) ListNetworks(ctx context.Context) ([]Network, error) {
	var listed []struct {
		Name   string            `json:"Name"`
		Labels map[string]string `json:"Labels"`
	}
	if err := d.do(ctx, request{method: http.MethodGet, path: "/networks", query: url.Values{"filters": {d.ownFilter()}}}, &listed); err != nil {
		return nil, err
	}
	networks := make([]Network, 0, len(listed))
	for _, item := range listed {
		networks = append(networks, Network{Name: item.Name, Labels: item.Labels})
	}
	return networks, nil
}

func (d *Docker) List(ctx context.Context) ([]Container, error) {
	var listed []struct {
		ID     string            `json:"Id"`
		Names  []string          `json:"Names"`
		State  string            `json:"State"`
		Labels map[string]string `json:"Labels"`
	}
	if err := d.do(ctx, request{method: http.MethodGet, path: "/containers/json", query: url.Values{"all": {"true"}, "filters": {d.ownFilter()}}}, &listed); err != nil {
		return nil, err
	}
	containers := make([]Container, 0, len(listed))
	for _, item := range listed {
		name := ""
		if len(item.Names) > 0 {
			name = strings.TrimPrefix(item.Names[0], "/")
		}
		containers = append(containers, Container{ID: item.ID, Name: name, State: item.State, Running: item.State == "running", Labels: item.Labels})
	}
	return containers, nil
}

func (d *Docker) Create(ctx context.Context, spec ContainerSpec) (string, error) {
	port := strconv.Itoa(spec.ContainerPort) + "/tcp"
	mounts := make([]map[string]any, 0, len(spec.Mounts))
	for _, mount := range spec.Mounts {
		mounts = append(mounts, map[string]any{"Type": "bind", "Source": mount.Source, "Target": mount.Target})
	}
	host := map[string]any{
		"PortBindings":   map[string]any{port: []map[string]string{{"HostIp": "127.0.0.1", "HostPort": strconv.Itoa(spec.HostPort)}}},
		"Mounts":         mounts,
		"Memory":         spec.MemoryBytes,
		"NanoCpus":       spec.NanoCPUs,
		"PidsLimit":      spec.PIDs,
		"ReadonlyRootfs": spec.ReadOnlyRoot,
		"Tmpfs":          spec.Tmpfs,
		"RestartPolicy":  map[string]any{"Name": "unless-stopped"},
		"SecurityOpt":    []string{"no-new-privileges"},
		"CapDrop":        []string{"ALL"},
		"NetworkMode":    spec.Network,
		"LogConfig":      map[string]any{"Type": "json-file", "Config": map[string]string{"max-size": "10m", "max-file": "3"}},
	}
	body := map[string]any{
		"Image":            spec.Image,
		"Env":              spec.Env,
		"User":             spec.User,
		"Labels":           d.ownLabels(spec.Labels),
		"ExposedPorts":     map[string]any{port: map[string]any{}},
		"HostConfig":       host,
		"NetworkingConfig": map[string]any{"EndpointsConfig": map[string]any{spec.Network: map[string]any{}}},
	}
	var created struct {
		ID string `json:"Id"`
	}
	if err := d.do(ctx, request{method: http.MethodPost, path: "/containers/create", query: url.Values{"name": {spec.Name}}, body: body}, &created); err != nil {
		return "", fmt.Errorf("create container %s: %w", spec.Name, err)
	}
	return created.ID, nil
}

func (d *Docker) Start(ctx context.Context, id string) error {
	return d.do(ctx, request{method: http.MethodPost, path: "/containers/" + url.PathEscape(id) + "/start"}, nil)
}

func (d *Docker) Stop(ctx context.Context, id string, timeout time.Duration) error {
	seconds := strconv.Itoa(int(timeout.Seconds()))
	err := d.do(ctx, request{method: http.MethodPost, path: "/containers/" + url.PathEscape(id) + "/stop", query: url.Values{"t": {seconds}}, timeout: timeout + callTimeout}, nil)
	if errors.Is(err, ErrNotFound) {
		return nil
	}
	return err
}

// Remove deletes the container and the anonymous volumes Docker made for the
// image's VOLUME paths, which would otherwise outlive every replacement.
func (d *Docker) Remove(ctx context.Context, id string) error {
	err := d.do(ctx, request{method: http.MethodDelete, path: "/containers/" + url.PathEscape(id), query: url.Values{"force": {"true"}, "v": {"true"}}}, nil)
	if errors.Is(err, ErrNotFound) {
		return nil
	}
	return err
}

func (d *Docker) Pid(ctx context.Context, id string) (int, error) {
	var inspected struct {
		State struct {
			Pid int `json:"Pid"`
		} `json:"State"`
	}
	if err := d.do(ctx, request{method: http.MethodGet, path: "/containers/" + url.PathEscape(id) + "/json"}, &inspected); err != nil {
		return 0, err
	}
	if inspected.State.Pid <= 0 {
		return 0, fmt.Errorf("container %s: %w", id, ErrNotRunning)
	}
	return inspected.State.Pid, nil
}

func (d *Docker) Logs(ctx context.Context, id string, tail int) ([]byte, error) {
	var stream []byte
	query := url.Values{"stdout": {"true"}, "stderr": {"true"}, "tail": {strconv.Itoa(tail)}}
	if err := d.do(ctx, request{method: http.MethodGet, path: "/containers/" + url.PathEscape(id) + "/logs", query: query}, &stream); err != nil {
		return nil, err
	}
	return demultiplex(stream), nil
}

// demultiplex strips the 8-byte frame headers the engine puts on the output
// of containers without a TTY. Output that is not framed is returned as is.
func demultiplex(stream []byte) []byte {
	var out bytes.Buffer
	for len(stream) >= 8 {
		kind := stream[0]
		if (kind != 0 && kind != 1 && kind != 2) || stream[1] != 0 || stream[2] != 0 || stream[3] != 0 {
			out.Write(stream)
			return out.Bytes()
		}
		size := int(binary.BigEndian.Uint32(stream[4:8]))
		if 8+size > len(stream) {
			out.Write(stream[8:])
			return out.Bytes()
		}
		out.Write(stream[8 : 8+size])
		stream = stream[8+size:]
	}
	out.Write(stream)
	return out.Bytes()
}
