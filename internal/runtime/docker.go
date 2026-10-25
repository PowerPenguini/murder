package runtime

import (
	"bytes"
	"context"
	"encoding/binary"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"time"
)

// apiVersion is the oldest Engine API that every supported Docker serves
// (Docker 25 and later) and that has every field used here.
const apiVersion = "v1.44"

// Docker talks to the Docker Engine API over its unix socket.
type Docker struct {
	client *http.Client
	base   string
}

// NewDocker returns a client for the engine listening on socket.
func NewDocker(socket string) *Docker {
	transport := &http.Transport{
		DialContext: func(ctx context.Context, _, _ string) (net.Conn, error) {
			var dialer net.Dialer
			return dialer.DialContext(ctx, "unix", socket)
		},
	}
	return &Docker{client: &http.Client{Transport: transport}, base: "http://docker/" + apiVersion}
}

// newDockerForURL points the client at an HTTP server; tests use it.
func newDockerForURL(base string, client *http.Client) *Docker {
	return &Docker{client: client, base: strings.TrimSuffix(base, "/") + "/" + apiVersion}
}

// apiError is a non-2xx Engine API answer.
type apiError struct {
	Status  int
	Message string
}

func (e *apiError) Error() string { return fmt.Sprintf("docker: %d %s", e.Status, e.Message) }

func (d *Docker) do(ctx context.Context, method, path string, query url.Values, body any, out any) error {
	var reader io.Reader
	if body != nil {
		encoded, err := json.Marshal(body)
		if err != nil {
			return err
		}
		reader = bytes.NewReader(encoded)
	}
	target := d.base + path
	if len(query) > 0 {
		target += "?" + query.Encode()
	}
	request, err := http.NewRequestWithContext(ctx, method, target, reader)
	if err != nil {
		return err
	}
	if body != nil {
		request.Header.Set("Content-Type", "application/json")
	}
	response, err := d.client.Do(request)
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

func (d *Docker) HasImage(ctx context.Context, ref string) (bool, error) {
	err := d.do(ctx, http.MethodGet, "/images/"+url.PathEscape(ref)+"/json", nil, nil, nil)
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
		return fmt.Errorf("pull %s: only digest-pinned references can be pulled", ref)
	}
	var stream []byte
	if err := d.do(ctx, http.MethodPost, "/images/create", url.Values{"fromImage": {repository}, "tag": {digest}}, nil, &stream); err != nil {
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

func (d *Docker) EnsureNetwork(ctx context.Context, name string, labels map[string]string) (string, error) {
	subnet, err := d.networkSubnet(ctx, name)
	if err == nil {
		return subnet, nil
	}
	if !errors.Is(err, ErrNotFound) {
		return "", err
	}
	create := map[string]any{
		"Name":           name,
		"Driver":         "bridge",
		"CheckDuplicate": true,
		"Labels":         labels,
		// Containers on one tenant network cannot reach each other; the edge
		// proxy reaches the tenant through its published loopback port.
		"Options": map[string]string{"com.docker.network.bridge.enable_icc": "false"},
	}
	if err := d.do(ctx, http.MethodPost, "/networks/create", nil, create, nil); err != nil {
		return "", fmt.Errorf("create network %s: %w", name, err)
	}
	return d.networkSubnet(ctx, name)
}

func (d *Docker) networkSubnet(ctx context.Context, name string) (string, error) {
	var network struct {
		IPAM struct {
			Config []struct {
				Subnet string `json:"Subnet"`
			} `json:"Config"`
		} `json:"IPAM"`
	}
	if err := d.do(ctx, http.MethodGet, "/networks/"+url.PathEscape(name), nil, nil, &network); err != nil {
		return "", err
	}
	for _, config := range network.IPAM.Config {
		if config.Subnet != "" && !strings.Contains(config.Subnet, ":") {
			return config.Subnet, nil
		}
	}
	return "", fmt.Errorf("network %s has no IPv4 subnet", name)
}

func (d *Docker) RemoveNetwork(ctx context.Context, name string) error {
	err := d.do(ctx, http.MethodDelete, "/networks/"+url.PathEscape(name), nil, nil, nil)
	if errors.Is(err, ErrNotFound) {
		return nil
	}
	return err
}

func (d *Docker) List(ctx context.Context) ([]Container, error) {
	filters, _ := json.Marshal(map[string][]string{"label": {LabelManaged + "=true"}})
	var listed []struct {
		ID     string            `json:"Id"`
		Names  []string          `json:"Names"`
		State  string            `json:"State"`
		Labels map[string]string `json:"Labels"`
	}
	if err := d.do(ctx, http.MethodGet, "/containers/json", url.Values{"all": {"true"}, "filters": {string(filters)}}, nil, &listed); err != nil {
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
		"Labels":           spec.Labels,
		"ExposedPorts":     map[string]any{port: map[string]any{}},
		"HostConfig":       host,
		"NetworkingConfig": map[string]any{"EndpointsConfig": map[string]any{spec.Network: map[string]any{}}},
	}
	var created struct {
		ID string `json:"Id"`
	}
	if err := d.do(ctx, http.MethodPost, "/containers/create", url.Values{"name": {spec.Name}}, body, &created); err != nil {
		return "", fmt.Errorf("create container %s: %w", spec.Name, err)
	}
	return created.ID, nil
}

func (d *Docker) Start(ctx context.Context, id string) error {
	return d.do(ctx, http.MethodPost, "/containers/"+url.PathEscape(id)+"/start", nil, nil, nil)
}

func (d *Docker) Stop(ctx context.Context, id string, timeout time.Duration) error {
	seconds := strconv.Itoa(int(timeout.Seconds()))
	err := d.do(ctx, http.MethodPost, "/containers/"+url.PathEscape(id)+"/stop", url.Values{"t": {seconds}}, nil, nil)
	if errors.Is(err, ErrNotFound) {
		return nil
	}
	return err
}

func (d *Docker) Remove(ctx context.Context, id string) error {
	err := d.do(ctx, http.MethodDelete, "/containers/"+url.PathEscape(id), url.Values{"force": {"true"}}, nil, nil)
	if errors.Is(err, ErrNotFound) {
		return nil
	}
	return err
}

func (d *Docker) Logs(ctx context.Context, id string, tail int) ([]byte, error) {
	var stream []byte
	query := url.Values{"stdout": {"true"}, "stderr": {"true"}, "tail": {strconv.Itoa(tail)}}
	if err := d.do(ctx, http.MethodGet, "/containers/"+url.PathEscape(id)+"/logs", query, nil, &stream); err != nil {
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
