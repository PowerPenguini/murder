package plugins

import (
	"bytes"
	"context"
	"crypto/rand"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"maps"
	"net"
	"net/http"
	"net/textproto"
	"net/url"
	"path/filepath"
	"reflect"
	"slices"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/PowerPenguini/murder/internal/metrics"
)

// SocketRoot is where the agent keeps the socket of each plugin's
// container, in a directory of its own (see ContainerID), a small tmpfs
// the edge reaches and the container writes; SocketName is the socket's
// name in it, and SocketDir the directory as the container has it.
const (
	SocketRoot = "/run/murder-plugins"
	SocketName = "plugin.sock"
	SocketDir  = "/run/murder-plugin"
)

// ContainerID names the container of a plugin's image, and its socket's
// directory: short, so that the socket's path stays within the 108 bytes
// a unix socket's address holds.
func ContainerID(plugin, image string) string {
	sum := sha256.Sum256([]byte(plugin + "\x00" + image))
	return hex.EncodeToString(sum[:6])
}

// How long each call to a plugin's container may take; what a tenant
// stops waiting for is counted all the same.
var callRequest = 10 * time.Second

// recheck is how soon a container that could not be reached, or said
// another manifest, is asked again.
var recheck = time.Second

const (
	callManifest = 2 * time.Second
	callResponse = time.Second
	callBody     = time.Second
	callEnd      = 10 * time.Second
	// maxAnswer is the most a container answers to one call, but for a
	// request's (whose body it may replace).
	maxAnswer = 1 << 20
	// maxCounts is the most counts one answer holds.
	maxCounts = 64
	// maxCalls is how many calls the edge has under way to one container.
	maxCalls = 256
)

// Containers reaches the containers of plugins of kind container, each on
// its socket below a root (SocketRoot). The agent runs them; the edge only
// calls them, once a container said the manifest its plugin was installed
// with.
type Containers struct {
	root string
	log  *slog.Logger

	mu      sync.Mutex
	reached map[string]*container
}

// NewContainers reaches plugins' containers on the sockets below root.
func NewContainers(root string, log *slog.Logger) *Containers {
	return &Containers{root: root, log: log, reached: map[string]*container{}}
}

// container is the edge's way to one plugin's container.
type container struct {
	id     string
	client *http.Client

	// verified is set once the container said its plugin's manifest, and
	// unset when it cannot be reached: it may have been replaced. mu has
	// one request at a time ask it again.
	verified  atomic.Bool
	mu        sync.Mutex
	checkedAt time.Time
	err       error
}

// Prepare lets go of the containers config's plugins no longer run.
func (c *Containers) Prepare(config Config) {
	wanted := map[string]bool{}
	for name, plugin := range config.Plugins {
		if plugin.Spec.Kind == KindContainer {
			wanted[ContainerID(name, plugin.Spec.Image)] = true
		}
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	for id, reached := range c.reached {
		if !wanted[id] {
			reached.client.CloseIdleConnections()
			delete(c.reached, id)
		}
	}
}

// of is the way to the container of a plugin's image, once it said the
// manifest the plugin was installed with.
func (c *Containers) of(name string, plugin Plugin) (*container, error) {
	id := ContainerID(name, plugin.Spec.Image)
	c.mu.Lock()
	reached := c.reached[id]
	if reached == nil {
		socket := filepath.Join(c.root, id, SocketName)
		transport := &http.Transport{
			DialContext: func(ctx context.Context, _, _ string) (net.Conn, error) {
				var dialer net.Dialer
				return dialer.DialContext(ctx, "unix", socket)
			},
			MaxIdleConnsPerHost: 64,
			MaxConnsPerHost:     maxCalls,
			IdleConnTimeout:     time.Minute,
			DisableCompression:  true,
		}
		reached = &container{id: id, client: &http.Client{Transport: transport}}
		c.reached[id] = reached
	}
	c.mu.Unlock()
	return reached, reached.verify(*plugin.Manifest)
}

// verify asks the container for its manifest, unless it said it already:
// one that says another than its plugin was installed with is not used.
func (c *container) verify(want Manifest) error {
	if c.verified.Load() {
		return nil
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.verified.Load() {
		return nil
	}
	if c.err != nil && time.Since(c.checkedAt) < recheck {
		return c.err
	}
	c.checkedAt = time.Now()
	c.err = func() error {
		answer, err := c.call(callManifest, http.MethodGet, "/manifest", "", nil, maxAnswer)
		if err != nil {
			return fmt.Errorf("its container is not running on this node, or not yet: %w", err)
		}
		var said Manifest
		if err := json.Unmarshal(answer, &said); err != nil {
			return fmt.Errorf("its container says no manifest: %w", err)
		}
		if !reflect.DeepEqual(said, want) {
			return errors.New("its container says another manifest than its image's label, which it was installed with")
		}
		return nil
	}()
	c.verified.Store(c.err == nil)
	return c.err
}

// unreachable has the container's manifest asked again before its next
// request: it may have been replaced.
func (c *container) unreachable() { c.verified.Store(false) }

// call calls the container, and reads at most limit bytes of its answer.
// It runs for at most timeout, whatever becomes of the request.
func (c *container) call(timeout time.Duration, method, path, session string, body []byte, limit int64) ([]byte, error) {
	ctx, cancel := context.WithTimeout(context.Background(), timeout)
	defer cancel()
	request, err := http.NewRequestWithContext(ctx, method, "http://plugin"+path, bytes.NewReader(body))
	if err != nil {
		return nil, err
	}
	if session != "" {
		request.Header.Set("Murder-Session", session)
	}
	response, err := c.client.Do(request)
	if err != nil {
		if ctx.Err() != nil {
			return nil, fmt.Errorf("%s took more than %s", path, timeout)
		}
		var opError *net.OpError
		if errors.As(err, &opError) && opError.Op == "dial" {
			c.unreachable()
		}
		return nil, fmt.Errorf("%s: %w", path, err)
	}
	defer response.Body.Close()
	answer, err := io.ReadAll(io.LimitReader(response.Body, limit+1))
	switch {
	case err != nil:
		if ctx.Err() != nil {
			return nil, fmt.Errorf("%s took more than %s", path, timeout)
		}
		return nil, fmt.Errorf("%s: %w", path, err)
	case int64(len(answer)) > limit:
		return nil, fmt.Errorf("%s answered more than %d bytes", path, limit)
	case response.StatusCode != http.StatusOK:
		said := strings.TrimSpace(string(answer))
		if len(said) > 512 {
			said = said[:512]
		}
		return nil, fmt.Errorf("%s answered %d: %s", path, response.StatusCode, said)
	}
	return answer, nil
}

// newSession names a request to a container, for its calls that follow.
func newSession() string {
	var b [16]byte
	_, _ = rand.Read(b[:])
	return hex.EncodeToString(b[:])
}

// The JSON a container takes and gives (see docs/plugins.md).
type (
	moduleRequest struct {
		Tenant string              `json:"tenant"`
		Plugin string              `json:"plugin"`
		Method string              `json:"method"`
		Path   string              `json:"path"`
		Query  string              `json:"query,omitempty"`
		Header map[string][]string `json:"header"`
		Config map[string]any      `json:"config,omitempty"`
		Body   []byte              `json:"body,omitempty"`
	}
	moduleForward struct {
		URL    string              `json:"url"`
		Header map[string][]string `json:"header"`
		Drop   []string            `json:"drop"`
		// ReplaceBody sends Body in place of the request's body.
		ReplaceBody bool   `json:"replaceBody"`
		Body        []byte `json:"body"`
		Detail      string `json:"detail"`
		Meter       bool   `json:"meter"`
		Refuse      *struct {
			Status  int    `json:"status"`
			Message string `json:"message"`
		} `json:"refuse"`
		Error string `json:"error"`
	}
	moduleResponse struct {
		Status int                 `json:"status"`
		Header map[string][]string `json:"header"`
	}
	moduleMetered struct {
		Counts []moduleCount `json:"counts"`
		Body   bool          `json:"body"`
		Error  string        `json:"error"`
	}
	moduleChunk struct {
		More  bool   `json:"more"`
		Error string `json:"error"`
	}
	moduleCount struct {
		Metric string `json:"metric"`
		Detail string `json:"detail"`
		Amount int64  `json:"amount"`
	}
	moduleEnd struct {
		Complete bool `json:"complete"`
	}
)

// WithContainers serves plugins of kind container with their
// containers.
func (p *Proxy) WithContainers(containers *Containers) *Proxy {
	p.containers = containers
	return p
}

// containerHandler passes a request on as a plugin's container says, and
// counts what the container reads in its answer.
type containerHandler struct {
	x         *exchange
	manifest  Manifest
	hosts     []string
	container *container
	session   string

	mu      sync.Mutex
	failed  bool
	meter   bool
	reading bool
	ended   bool
}

func (p *Proxy) containerHandler(x *exchange, plugin Plugin) (handler, error) {
	if p.containers == nil || plugin.Manifest == nil {
		return nil, statusError{code: http.StatusServiceUnavailable, message: fmt.Sprintf("this edge runs no containers for plugin %s", x.plugin), kind: "murder_plugin_unavailable"}
	}
	reached, err := p.containers.of(x.plugin, plugin)
	if err != nil {
		p.log.Warn("a plugin's container serves no request", "plugin", x.plugin, "tenant", x.tenant, "error", err)
		return nil, statusError{code: http.StatusServiceUnavailable, message: fmt.Sprintf("plugin %s: %v", x.plugin, err), kind: "murder_plugin_unavailable"}
	}
	return &containerHandler{x: x, manifest: *plugin.Manifest, hosts: hostsOf(plugin.Spec, *plugin.Manifest), container: reached, session: newSession()}, nil
}

// failure is what a request is answered when the container failed before
// it went on.
var failure = statusError{code: http.StatusBadGateway, message: "the plugin's container failed; the edge's log says how", kind: "murder_plugin_failed"}

// fail says why the container failed.
func (h *containerHandler) fail(stage string, err error) {
	h.failed = true
	h.x.proxy.containerFailed(h.x.plugin)
	h.x.proxy.log.Error("a plugin's container failed; what it was to count after is not counted", "plugin", h.x.plugin, "tenant", h.x.tenant, "stage", stage,
		"error", h.scrub(err.Error()))
}

// scrub takes the key out of what a container says.
func (h *containerHandler) scrub(text string) string {
	if h.x.key == "" {
		return text
	}
	return strings.ReplaceAll(strings.ReplaceAll(text, h.x.key, "[key]"), url.QueryEscape(h.x.key), "[key]")
}

// credentials are the headers a tenant's credentials come in, never shown
// to a container.
var credentials = []string{"Authorization", "Api-Key", "X-Api-Key", "Cookie", "Proxy-Authorization", "Openai-Organization", "Openai-Project", "X-Forwarded-For"}

func (h *containerHandler) request(r *http.Request, rest string) (*url.URL, error) {
	h.mu.Lock()
	defer h.mu.Unlock()
	// A path that climbs, written as it is or escaped, would get past the
	// endpoints a container passes on.
	if !plainPath(rest) {
		return nil, statusError{code: http.StatusBadRequest, message: "the path is not one to pass on"}
	}
	var body []byte
	if h.manifest.RequestBody > 0 && r.Body != nil && r.Body != http.NoBody {
		var err error
		if body, err = h.x.readBodyUpTo(r, int(h.manifest.RequestBody)); err != nil {
			return nil, err
		}
	}
	header := r.Header.Clone()
	for _, name := range append(slices.Clip(credentials), h.manifest.Key.Header) {
		if name != "" {
			header.Del(name)
		}
	}
	query := r.URL.RawQuery
	if h.manifest.Key.Query != "" {
		query = withoutKey(query, h.manifest.Key.Query)
	}
	input, err := json.Marshal(moduleRequest{Tenant: h.x.tenant, Plugin: h.x.plugin, Method: r.Method, Path: "/" + rest, Query: query, Header: header, Config: h.x.spec.Config, Body: body})
	if err != nil {
		return nil, err
	}
	answer, err := h.container.call(callRequest, http.MethodPost, "/request", h.session, input, MaxRequestBody*4/3+maxAnswer)
	if err != nil {
		h.fail("request", err)
		return nil, failure
	}
	var forward moduleForward
	if err := json.Unmarshal(answer, &forward); err != nil {
		h.fail("request", fmt.Errorf("/request answered no JSON of a request: %w", err))
		return nil, failure
	}
	if forward.Error != "" {
		h.fail("request", errors.New(forward.Error))
		return nil, failure
	}
	if refusal := forward.Refuse; refusal != nil {
		status := refusal.Status
		if status < 400 || status > 599 {
			status = http.StatusBadRequest
		}
		message := refusal.Message
		if len(message) > 1024 {
			message = message[:1024]
		}
		return nil, statusError{code: status, message: h.scrub(message), kind: "murder_plugin_refused"}
	}
	target, err := h.target(forward.URL)
	if err == nil {
		err = h.headers(r.Header, forward)
	}
	if err == nil && forward.ReplaceBody {
		if len(forward.Body) > MaxRequestBody {
			err = fmt.Errorf("the container replaces the request's body with %d bytes, more than %d", len(forward.Body), MaxRequestBody)
		} else {
			replaced := forward.Body
			r.Body = io.NopCloser(bytes.NewReader(replaced))
			r.ContentLength = int64(len(replaced))
			r.Header.Set("Content-Length", fmt.Sprint(len(replaced)))
			r.GetBody = func() (io.ReadCloser, error) { return io.NopCloser(bytes.NewReader(replaced)), nil }
		}
	}
	if err != nil {
		h.fail("request", err)
		return nil, failure
	}
	h.x.detail = cleanDetail(forward.Detail)
	h.meter = forward.Meter
	return target, nil
}

// target checks where a container sends a request: over HTTPS, to a host
// of the plugin's, which the key may go to.
func (h *containerHandler) target(raw string) (*url.URL, error) {
	target, err := url.Parse(raw)
	if err != nil {
		return nil, fmt.Errorf("the container sends the request to %q, not a URL", raw)
	}
	host := target.Hostname()
	switch {
	case target.Scheme != "https":
		return nil, fmt.Errorf("the container sends the request over %q: only https carries the key", target.Scheme)
	case target.User != nil || target.Opaque != "" || target.Fragment != "":
		return nil, errors.New("the container sends the request to a URL with a user, or a fragment")
	case target.Port() != "" && target.Port() != "443":
		return nil, fmt.Errorf("the container sends the request to port %s: only 443 carries the key", target.Port())
	case host != strings.ToLower(host) || hostRuleIn(h.hosts, host) == "":
		return nil, fmt.Errorf("the container sends the request to %s, which is not one of the plugin's hosts (%s)", host, strings.Join(h.hosts, ", "))
	}
	if !plainPath(target.EscapedPath()) {
		return nil, fmt.Errorf("the container sends the request to a path that climbs: %s", target.EscapedPath())
	}
	return &url.URL{Scheme: "https", Host: target.Host, Path: target.Path, RawPath: target.RawPath, RawQuery: target.RawQuery}, nil
}

// plainPath reports whether an escaped path neither climbs nor stays, as
// in /../ or /./, written as it is or escaped, and escapes no slash.
func plainPath(escaped string) bool {
	lower := strings.ToLower(escaped)
	if strings.Contains(lower, "%2f") || strings.Contains(lower, "%5c") || strings.Contains(escaped, "\\") {
		return false
	}
	unescaped, err := url.PathUnescape(escaped)
	if err != nil {
		return false
	}
	padded := "/" + unescaped + "/"
	return !strings.Contains(padded, "/../") && !strings.Contains(padded, "/./")
}

// headers changes the request's headers as the container says: never
// those that say how it is carried, nor the key's.
func (h *containerHandler) headers(header http.Header, forward moduleForward) error {
	check := func(name string) error {
		if !headerToken.MatchString(name) || reservedHeader(name) || strings.EqualFold(name, h.manifest.Key.Header) {
			return fmt.Errorf("the container may not set header %q", name)
		}
		return nil
	}
	for _, name := range forward.Drop {
		if err := check(name); err != nil {
			return err
		}
		header.Del(name)
	}
	for name, values := range forward.Header {
		if err := check(name); err != nil {
			return err
		}
		for _, value := range values {
			if strings.ContainsAny(value, "\r\n\x00") {
				return fmt.Errorf("the container sets header %s to a value with a line break", name)
			}
		}
		header[textproto.CanonicalMIMEHeaderKey(name)] = slices.Clone(values)
	}
	return nil
}

// authorize puts the key where the plugin's manifest says.
func (h *containerHandler) authorize(out *http.Request) {
	if key := h.manifest.Key; key.Header != "" {
		out.Header.Set(key.Header, key.Prefix+h.x.key)
	} else {
		out.URL.RawQuery = withKey(out.URL.RawQuery, key.Query, h.x.key)
	}
	if h.meter || h.manifest.Key.Query != "" {
		// The container reads the answer uncompressed, and an error is read
		// so to take a key in the query out of it.
		out.Header.Set("Accept-Encoding", "identity")
	}
}

func (h *containerHandler) response(response *http.Response) {
	h.mu.Lock()
	defer h.mu.Unlock()
	if h.manifest.Key.Query != "" {
		scrubErrorBody(response, h.x.key)
	}
	if !h.meter || h.failed {
		return
	}
	input, err := json.Marshal(moduleResponse{Status: response.StatusCode, Header: response.Header})
	if err != nil {
		h.fail("response", err)
		return
	}
	metered, err := h.metered("/response", callResponse, input)
	if err != nil {
		h.fail("response", err)
		return
	}
	if metered.Body {
		h.reading = true
		response.Body = &containerBody{h: h, body: response.Body}
	}
}

// metered makes a call that answers counts, and counts them.
func (h *containerHandler) metered(name string, timeout time.Duration, input []byte) (moduleMetered, error) {
	answer, err := h.container.call(timeout, http.MethodPost, name, h.session, input, maxAnswer)
	if err != nil {
		return moduleMetered{}, err
	}
	var metered moduleMetered
	if err := json.Unmarshal(answer, &metered); err != nil {
		return moduleMetered{}, fmt.Errorf("%s answered no JSON of counts: %w", name, err)
	}
	if metered.Error != "" {
		return moduleMetered{}, errors.New(metered.Error)
	}
	if len(metered.Counts) > maxCounts {
		return moduleMetered{}, fmt.Errorf("%s answered %d counts, more than %d", name, len(metered.Counts), maxCounts)
	}
	metrics := MetricsOf(Spec{Kind: KindContainer}, &h.manifest)
	for _, count := range metered.Counts {
		switch {
		case count.Metric == MetricRequests || !slices.Contains(metrics, count.Metric):
			return metered, fmt.Errorf("%s counts %q, which is not one of the metrics its manifest says (%s)", name, count.Metric, strings.Join(h.manifest.Metrics, ", "))
		case count.Amount < 0 || count.Amount > MaxCount:
			return metered, fmt.Errorf("%s counts %d of %s, not an amount from 0 to 2^40", name, count.Amount, count.Metric)
		}
	}
	for _, count := range metered.Counts {
		if count.Amount > 0 {
			h.x.count(count.Metric, cleanDetail(count.Detail), count.Amount)
		}
	}
	return metered, nil
}

// chunk shows the container a chunk of the answer's body; it reports
// whether the container wants more.
func (h *containerHandler) chunk(data []byte) bool {
	h.mu.Lock()
	defer h.mu.Unlock()
	if h.failed || h.ended {
		return false
	}
	answer, err := h.container.call(callBody, http.MethodPost, "/body", h.session, data, maxAnswer)
	var said moduleChunk
	if err == nil {
		if err = json.Unmarshal(answer, &said); err != nil {
			err = fmt.Errorf("/body answered no JSON: %w", err)
		} else if said.Error != "" {
			err = errors.New(said.Error)
		}
	}
	if err != nil {
		h.fail("body", err)
		return false
	}
	return said.More
}

// end tells the container the answer ended, its body read whole or not,
// and counts what it says. A container that meters the request is told
// so even when no body was read, so that it lets go of the request.
func (h *containerHandler) end(complete bool) {
	h.mu.Lock()
	defer h.mu.Unlock()
	if h.ended || h.failed || !h.meter {
		return
	}
	h.ended = true
	input, _ := json.Marshal(moduleEnd{Complete: complete && h.reading})
	if _, err := h.metered("/end", callEnd, input); err != nil {
		h.fail("end", err)
	}
}

func (h *containerHandler) close() { h.end(false) }

// containerBody shows a container the answer's body as it passes.
type containerBody struct {
	h      *containerHandler
	body   io.ReadCloser
	done   bool
	enough bool
}

func (b *containerBody) Read(buffer []byte) (int, error) {
	n, err := b.body.Read(buffer)
	if n > 0 && !b.enough {
		b.enough = !b.h.chunk(buffer[:n])
	}
	if err != nil && !b.done {
		b.done = true
		b.h.end(err == io.EOF)
	}
	return n, err
}

func (b *containerBody) Close() error {
	if !b.done {
		b.done = true
		b.h.end(false)
	}
	return b.body.Close()
}

// containerFailed counts a failure of a plugin's container.
func (p *Proxy) containerFailed(plugin string) {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.failures[plugin]++
}

// WriteMetrics writes how often each plugin's container failed.
func (p *Proxy) WriteMetrics(out *metrics.Writer) {
	p.mu.Lock()
	failures := maps.Clone(p.failures)
	p.mu.Unlock()
	if len(failures) == 0 {
		return
	}
	out.Family("murder_plugin_failures_total", "counter", "How often a plugin's container failed, since the edge started: a request it failed before is answered 502, one after passes without what was left to count.")
	for _, plugin := range slices.Sorted(maps.Keys(failures)) {
		out.Sample("murder_plugin_failures_total", float64(failures[plugin]), "plugin", plugin)
	}
}
