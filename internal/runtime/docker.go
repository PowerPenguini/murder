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
	"slices"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/PowerPenguini/murder/internal/spec"
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

	// exits holds what the event log said about each container's last
	// exit.
	exitsMu sync.Mutex
	exits   map[string]exit
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

func (d *Docker) ImageID(ctx context.Context, ref string) (string, error) {
	var image struct {
		ID string `json:"Id"`
	}
	err := d.do(ctx, request{method: http.MethodGet, path: "/images/" + url.PathEscape(ref) + "/json"}, &image)
	if errors.Is(err, ErrNotFound) {
		return "", nil
	}
	return image.ID, err
}

// RemoveImage removes the image by its ID, so its tags in the repository go
// too; the engine refuses, with 409, an image a container uses or one named
// in several repositories.
func (d *Docker) RemoveImage(ctx context.Context, id string) error {
	err := d.do(ctx, request{method: http.MethodDelete, path: "/images/" + url.PathEscape(id)}, nil)
	var apiErr *apiError
	switch {
	case errors.Is(err, ErrNotFound):
		return nil
	case errors.As(err, &apiErr) && apiErr.Status == http.StatusConflict:
		return fmt.Errorf("image %s: %w: %s", id, ErrInUse, apiErr.Message)
	}
	return err
}

// PullImage pulls a digest-pinned reference.
func (d *Docker) PullImage(ctx context.Context, ref string) error {
	repository, digest, ok := strings.Cut(ref, "@")
	if !ok {
		return fmt.Errorf("pull %s: only digest-pinned references can be pulled; a local image ID must already be present", ref)
	}
	// In repo:tag@digest the digest decides; the tag is only a label.
	repository, _ = splitTag(repository)
	return d.pull(ctx, repository, digest)
}

// pull fetches repository at tag (a tag or a digest). The engine streams
// progress as JSON lines and reports a failed pull inside that stream.
func (d *Docker) pull(ctx context.Context, repository, tag string) error {
	ref := repository + ":" + tag
	if strings.HasPrefix(tag, "sha256:") {
		ref = repository + "@" + tag
	}
	header := http.Header{}
	if auth, err := d.registryAuth(repository); err != nil {
		return fmt.Errorf("pull %s: %w", ref, err)
	} else if auth != "" {
		header.Set("X-Registry-Auth", auth)
	}
	var stream []byte
	call := request{method: http.MethodPost, path: "/images/create", query: url.Values{"fromImage": {repository}, "tag": {tag}}, header: header, timeout: pullTimeout}
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

// absent reports a pull the registry refused because it has no such image
// for this client, rather than one that failed.
func absent(err error) bool {
	message := strings.ToLower(err.Error())
	for _, sign := range []string{"not found", "pull access denied", "repository does not exist", "manifest unknown", "name unknown"} {
		if strings.Contains(message, sign) {
			return true
		}
	}
	return false
}

func (d *Docker) Resolve(ctx context.Context, ref string) (string, string, error) {
	if spec.Pinned(ref) {
		return ref, "", nil
	}
	repository, tag := splitTag(ref)
	pullErr := d.pull(ctx, repository, tag)
	if pullErr != nil && ctx.Err() != nil {
		return "", "", pullErr
	}
	var image struct {
		ID          string   `json:"Id"`
		RepoDigests []string `json:"RepoDigests"`
	}
	if err := d.do(ctx, request{method: http.MethodGet, path: "/images/" + url.PathEscape(repository+":"+tag) + "/json"}, &image); err != nil {
		if pullErr != nil {
			return "", "", pullErr
		}
		return "", "", fmt.Errorf("inspect %s: %w", ref, err)
	}
	// An image the registry says it does not have is local only: its ID
	// pins it, as one that cannot be pulled again. With containerd's image
	// store a local build has a repository digest too, which names no
	// registry's image. A registry that only failed to answer keeps its
	// digest, which other nodes can pull.
	if pullErr != nil && absent(pullErr) {
		return image.ID, fmt.Sprintf("using the local image %s:%s (%v)", repository, tag, pullErr), nil
	}
	note := ""
	if pullErr != nil {
		note = fmt.Sprintf("using the local image %s:%s (%v)", repository, tag, pullErr)
	}
	for _, repoDigest := range image.RepoDigests {
		if name, digest, ok := strings.Cut(repoDigest, "@"); ok && familiar(name) == familiar(repository) {
			return familiar(repository) + "@" + digest, note, nil
		}
	}
	return image.ID, note, nil
}

func (d *Docker) Latest(ctx context.Context, tag string) (string, error) {
	if spec.Pinned(tag) {
		return "", fmt.Errorf("%s is pinned, not a tag", tag)
	}
	repository, name := splitTag(tag)
	header := http.Header{}
	if auth, err := d.registryAuth(repository); err != nil {
		return "", fmt.Errorf("ask the registry about %s: %w", tag, err)
	} else if auth != "" {
		header.Set("X-Registry-Auth", auth)
	}
	var answer struct {
		Descriptor struct {
			Digest string `json:"digest"`
		} `json:"Descriptor"`
	}
	// The engine asks the registry for the tag's manifest, the image index
	// of a multi-platform image, whose digest a pull records.
	if err := d.do(ctx, request{method: http.MethodGet, path: "/distribution/" + repository + ":" + name + "/json", header: header}, &answer); err != nil {
		return "", fmt.Errorf("ask the registry about %s: %w", tag, err)
	}
	pinned := familiar(repository) + "@" + answer.Descriptor.Digest
	if !spec.RegistryPinned(pinned) {
		return "", fmt.Errorf("ask the registry about %s: it answered digest %q", tag, answer.Descriptor.Digest)
	}
	return pinned, nil
}

// familiar shortens a Docker Hub repository the way the engine reports it:
// docker.io/library/postgres and index.docker.io/library/postgres are
// postgres.
func familiar(repository string) string {
	for _, prefix := range []string{"docker.io/", "index.docker.io/"} {
		if rest, ok := strings.CutPrefix(repository, prefix); ok {
			repository = rest
			break
		}
	}
	if rest, ok := strings.CutPrefix(repository, "library/"); ok && !strings.Contains(rest, "/") {
		return rest
	}
	return repository
}

// splitTag splits "registry:5000/repo:tag" into repository and tag; a
// reference without a tag means "latest".
func splitTag(ref string) (string, string) {
	slash := strings.LastIndex(ref, "/")
	if colon := strings.LastIndex(ref, ":"); colon > slash {
		return ref[:colon], ref[colon+1:]
	}
	return ref, "latest"
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
	existing, err := d.inspectNetwork(ctx, name)
	switch {
	case err == nil && !existing.owned:
		return fmt.Errorf("network %s exists but is %w (node %s)", name, ErrNotOwned, d.node)
	case err == nil && existing.ipv6:
		return ipv6Error(name)
	case err == nil && existing.subnet == subnet:
		return nil
	case err == nil:
		return fmt.Errorf("%w: network %s has subnet %s, want %s", ErrNetworkMismatch, name, existing.subnet, subnet)
	case !errors.Is(err, ErrNotFound):
		return err
	}
	gateway, _, err := Addresses(subnet)
	if err != nil {
		return err
	}
	create := map[string]any{
		"Name":           name,
		"Driver":         "bridge",
		"CheckDuplicate": true,
		// IPv4 only: the tenant firewall has no IPv6 rules.
		"EnableIPv6": false,
		"Labels":     d.ownLabels(labels),
		// Murder assigns each tenant a small subnet from its own pool, so the
		// engine's default pools (about thirty networks) are never used up.
		"IPAM": map[string]any{"Driver": "default", "Config": []map[string]string{{"Subnet": subnet, "Gateway": gateway.String()}}},
		// Containers on one tenant network cannot reach each other, and
		// networks are isolated from each other; the host (the edge proxy)
		// reaches the tenant at its address.
		"Options": map[string]string{"com.docker.network.bridge.enable_icc": "false"},
	}
	if err := d.do(ctx, request{method: http.MethodPost, path: "/networks/create", body: create}, nil); err != nil {
		return fmt.Errorf("create network %s: %w", name, err)
	}
	// The engine's default-network-opts may still have turned IPv6 on.
	created, err := d.inspectNetwork(ctx, name)
	if err != nil {
		return err
	}
	if created.ipv6 {
		_ = d.do(ctx, request{method: http.MethodDelete, path: "/networks/" + url.PathEscape(name)}, nil)
		return ipv6Error(name)
	}
	return nil
}

func ipv6Error(name string) error {
	return fmt.Errorf("network %s has IPv6, which the tenant firewall does not cover; turn IPv6 off in Docker's default-network-opts", name)
}

// network is what Murder reads of a network.
type network struct {
	// subnet is the IPv4 subnet, "" if there is none.
	subnet string
	owned  bool
	ipv6   bool
}

func (d *Docker) inspectNetwork(ctx context.Context, name string) (network, error) {
	var inspected struct {
		Labels     map[string]string `json:"Labels"`
		EnableIPv6 bool              `json:"EnableIPv6"`
		IPAM       struct {
			Config []struct {
				Subnet string `json:"Subnet"`
			} `json:"Config"`
		} `json:"IPAM"`
	}
	if err := d.do(ctx, request{method: http.MethodGet, path: "/networks/" + url.PathEscape(name)}, &inspected); err != nil {
		return network{}, err
	}
	result := network{
		owned: inspected.Labels[LabelManaged] == "true" && inspected.Labels[LabelNode] == d.node,
		ipv6:  inspected.EnableIPv6,
	}
	for _, config := range inspected.IPAM.Config {
		if config.Subnet == "" {
			continue
		}
		if strings.Contains(config.Subnet, ":") {
			result.ipv6 = true
		} else if result.subnet == "" {
			result.subnet = config.Subnet
		}
	}
	return result, nil
}

func (d *Docker) RemoveNetwork(ctx context.Context, name string) error {
	existing, err := d.inspectNetwork(ctx, name)
	if errors.Is(err, ErrNotFound) {
		return nil
	}
	if err == nil && !existing.owned {
		return fmt.Errorf("network %s is %w (node %s); left in place", name, ErrNotOwned, d.node)
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
	mounts := make([]map[string]any, 0, len(spec.Mounts))
	for _, mount := range spec.Mounts {
		mounts = append(mounts, map[string]any{"Type": "bind", "Source": mount.Source, "Target": mount.Target})
	}
	host := map[string]any{
		"Mounts":         mounts,
		"Memory":         spec.MemoryBytes,
		"NanoCpus":       spec.NanoCPUs,
		"PidsLimit":      spec.PIDs,
		"ReadonlyRootfs": spec.ReadOnlyRoot,
		"Tmpfs":          spec.Tmpfs,
		"RestartPolicy":  map[string]any{"Name": RestartPolicy},
		"SecurityOpt":    []string{"no-new-privileges"},
		"CapDrop":        []string{"ALL"},
		"NetworkMode":    spec.Network,
		"LogConfig":      map[string]any{"Type": "json-file", "Config": map[string]string{"max-size": "10m", "max-file": "3"}},
	}
	// No swap on top of the memory limit (the engine would allow as much
	// again): an app over its limit is killed and says so, rather than
	// slowing the whole host down.
	if spec.MemoryBytes > 0 {
		host["MemorySwap"] = spec.MemoryBytes
	}
	body := map[string]any{
		"Image":            spec.Image,
		"Env":              spec.Env,
		"User":             spec.User,
		"Labels":           d.ownLabels(spec.Labels),
		"HostConfig":       host,
		"NetworkingConfig": map[string]any{"EndpointsConfig": map[string]any{spec.Network: map[string]any{"IPAMConfig": map[string]string{"IPv4Address": spec.Address}}}},
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
		err = nil
	}
	if err == nil {
		d.exitsMu.Lock()
		delete(d.exits, id)
		d.exitsMu.Unlock()
	}
	return err
}

func (d *Docker) SetRestartPolicy(ctx context.Context, id, policy string) error {
	body := map[string]any{"RestartPolicy": map[string]any{"Name": policy}}
	return d.do(ctx, request{method: http.MethodPost, path: "/containers/" + url.PathEscape(id) + "/update", body: body}, nil)
}

func (d *Docker) Inspect(ctx context.Context, id string) (ContainerState, error) {
	var inspected struct {
		RestartCount int `json:"RestartCount"`
		State        struct {
			Pid        int       `json:"Pid"`
			Restarting bool      `json:"Restarting"`
			OOMKilled  bool      `json:"OOMKilled"`
			ExitCode   int       `json:"ExitCode"`
			FinishedAt time.Time `json:"FinishedAt"`
		} `json:"State"`
		HostConfig struct {
			RestartPolicy struct {
				Name string `json:"Name"`
			} `json:"RestartPolicy"`
		} `json:"HostConfig"`
	}
	if err := d.do(ctx, request{method: http.MethodGet, path: "/containers/" + url.PathEscape(id) + "/json"}, &inspected); err != nil {
		return ContainerState{}, err
	}
	state := ContainerState{
		Pid:           max(inspected.State.Pid, 0),
		Restarting:    inspected.State.Restarting,
		Restarts:      inspected.RestartCount,
		RestartPolicy: inspected.HostConfig.RestartPolicy.Name,
	}
	// The engine reports "never" as the zero time, 0001-01-01T00:00:00Z.
	if inspected.State.FinishedAt.Year() <= 1 {
		return state, nil
	}
	state.ExitedAt = inspected.State.FinishedAt.UTC()
	if state.Pid == 0 {
		state.ExitKnown, state.ExitCode, state.OOMKilled = true, inspected.State.ExitCode, inspected.State.OOMKilled
		return state, nil
	}
	state.ExitKnown, state.ExitCode, state.OOMKilled = d.exitFromEvents(ctx, id, state.ExitedAt)
	return state, nil
}

// exitFromEvents finds how a container's process that runs again last
// exited, at exitedAt: the engine's "die" event carries the exit code, an
// "oom" event comes just before it, or, as the kernel's notice and the
// exit race, just after. The answer is kept per container, so the log is
// read once per exit, once a late "oom" event can no longer come.
func (d *Docker) exitFromEvents(ctx context.Context, id string, exitedAt time.Time) (known bool, code int, oom bool) {
	d.exitsMu.Lock()
	cached, ok := d.exits[id]
	d.exitsMu.Unlock()
	if ok && cached.at.Equal(exitedAt) {
		return cached.known, cached.code, cached.oom
	}
	until := exitedAt.Add(exitWindow)
	if now := time.Now(); until.After(now) {
		until = now // a time to come would make the engine wait for events until then
	}
	filters, _ := json.Marshal(map[string][]string{"type": {"container"}, "container": {id}, "event": {"die", "oom"}})
	query := url.Values{
		"since":   {engineTime(exitedAt.Add(-exitWindow))},
		"until":   {engineTime(until)},
		"filters": {string(filters)},
	}
	var data []byte
	if err := d.do(ctx, request{method: http.MethodGet, path: "/events", query: query}, &data); err != nil {
		return false, 0, false // unknown this time; asked again next time
	}
	// The die event nearest the exit is the one; an oom event belongs to
	// it if it came after the die before.
	type logged struct {
		die  bool
		at   time.Time
		code int
	}
	var events []logged
	decoder := json.NewDecoder(bytes.NewReader(data))
	for {
		var event struct {
			Action   string `json:"Action"`
			TimeNano int64  `json:"timeNano"`
			Actor    struct {
				Attributes map[string]string `json:"Attributes"`
			} `json:"Actor"`
		}
		if decoder.Decode(&event) != nil {
			break
		}
		at := time.Unix(0, event.TimeNano)
		switch event.Action {
		case "die":
			if exitCode, err := strconv.Atoi(event.Actor.Attributes["exitCode"]); err == nil {
				events = append(events, logged{die: true, at: at, code: exitCode})
			}
		case "oom":
			events = append(events, logged{at: at})
		}
	}
	slices.SortStableFunc(events, func(a, b logged) int { return a.at.Compare(b.at) })
	var die time.Time
	for _, e := range events {
		if e.die && (!known || e.at.Sub(exitedAt).Abs() < die.Sub(exitedAt).Abs()) {
			known, code, die = true, e.code, e.at
		}
	}
	// The exit's "oom" event is one after the exit before it and before
	// the next, at most lateOOM after its own.
	var previous, next time.Time
	for _, e := range events {
		switch {
		case !known || !e.die:
		case e.at.Before(die):
			previous = e.at
		case e.at.After(die) && next.IsZero():
			next = e.at
		}
	}
	for _, e := range events {
		if known && !e.die && e.at.After(previous) && (next.IsZero() || e.at.Before(next)) && !e.at.After(die.Add(lateOOM)) {
			oom = true
		}
	}
	// Without its "oom" event yet, the answer may still change.
	if !oom && time.Now().Before(exitedAt.Add(exitWindow)) {
		return known, code, oom
	}
	d.exitsMu.Lock()
	if d.exits == nil {
		d.exits = map[string]exit{}
	}
	d.exits[id] = exit{at: exitedAt, known: known, code: code, oom: oom}
	d.exitsMu.Unlock()
	return known, code, oom
}

// engineTime writes t as the engine's event filters take it: seconds and
// nanoseconds.
func engineTime(t time.Time) string { return fmt.Sprintf("%d.%09d", t.Unix(), t.Nanosecond()) }

// exitWindow is how far from the exit time the event log is searched.
const exitWindow = 5 * time.Second

// lateOOM is how long after its exit an "oom" event still belongs to it.
const lateOOM = time.Second

// exit is what exitFromEvents found for one exit.
type exit struct {
	at    time.Time
	known bool
	code  int
	oom   bool
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
