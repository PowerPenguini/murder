package plugins

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net"
	"net/http"
	"net/http/httputil"
	"net/netip"
	"net/url"
	"strconv"
	"strings"
	"sync"
	"time"
)

// Opener opens a key sealed with the master key in context.
type Opener func(context, sealed string) (string, error)

// Proxy serves the plugins to the tenants of its node: a request to
// /<plugin>/... from a tenant's network goes on to the provider with the
// plugin's key, unless a hard limit of the tenant's is reached, and what it
// used is counted.
type Proxy struct {
	config  func() Config
	open    Opener
	counter *Counter
	log     *slog.Logger
	// Transport reaches the providers.
	Transport http.RoundTripper
	// now is the clock, for tests.
	now func() time.Time
	// containers serves plugins of kind container; none without.
	containers *Containers

	mu   sync.Mutex
	keys map[string]string
	// active counts each tenant's requests under way; buffered the bytes
	// of request bodies read before they are passed on, in all and by
	// tenant.
	active   map[string]int
	buffered int64
	byTenant map[string]int64
	// failures counts the failures of each plugin's container.
	failures map[string]int64
}

// MaxPerTenant is how many requests to plugins a tenant has under way at
// once. MaxBuffered is how many bytes of request bodies the edge holds
// before passing them on, for every tenant together, and
// MaxBufferedPerTenant for one: a tenant cannot take it all.
const (
	MaxPerTenant         = 64
	MaxBuffered          = 512 << 20
	MaxBufferedPerTenant = 2 * MaxBody
)

// enter takes a place for a tenant's request, if it has one free.
func (p *Proxy) enter(tenant string) (func(), bool) {
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.active[tenant] >= MaxPerTenant {
		return nil, false
	}
	p.active[tenant]++
	return func() {
		p.mu.Lock()
		defer p.mu.Unlock()
		if p.active[tenant]--; p.active[tenant] <= 0 {
			delete(p.active, tenant)
		}
	}, true
}

// reserve takes room for size more bytes of a tenant's request body, if
// the edge has it, and the tenant's share does.
func (p *Proxy) reserve(tenant string, size int64) bool {
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.buffered+size > MaxBuffered || p.byTenant[tenant]+size > MaxBufferedPerTenant {
		return false
	}
	p.buffered += size
	p.byTenant[tenant] += size
	return true
}

func (p *Proxy) unreserve(tenant string, size int64) {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.buffered -= size
	if p.byTenant[tenant] -= size; p.byTenant[tenant] <= 0 {
		delete(p.byTenant, tenant)
	}
}

// NewProxy serves the plugins config says, opening their keys with open,
// counting into counter.
func NewProxy(config func() Config, open Opener, counter *Counter, log *slog.Logger) *Proxy {
	return &Proxy{config: config, open: open, counter: counter, log: log, Transport: NewTransport(), now: time.Now, keys: map[string]string{}, active: map[string]int{}, byTenant: map[string]int64{}, failures: map[string]int64{}}
}

// NewTransport reaches providers: HTTP/2 where they speak it, bodies as
// they come (the meters read them uncompressed: see the kinds).
func NewTransport() *http.Transport {
	return &http.Transport{
		Proxy:                 http.ProxyFromEnvironment,
		DialContext:           (&net.Dialer{Timeout: 10 * time.Second, KeepAlive: 30 * time.Second}).DialContext,
		ForceAttemptHTTP2:     true,
		MaxIdleConnsPerHost:   64,
		IdleConnTimeout:       90 * time.Second,
		TLSHandshakeTimeout:   10 * time.Second,
		ResponseHeaderTimeout: 10 * time.Minute,
		ExpectContinueTimeout: time.Second,
		DisableCompression:    true,
	}
}

// MaxBody is the largest request body a meter reads before passing it on.
const MaxBody = 64 << 20

// refuse answers a request that does not go on, as the providers' APIs
// answer errors: JSON with an error's message and type.
func refuse(w http.ResponseWriter, status int, kind, message string) {
	w.Header().Set("Content-Type", "application/json")
	w.Header().Set("X-Murder-Plugin-Error", kind)
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(map[string]any{"error": map[string]string{"message": message, "type": kind, "code": kind}})
}

func (p *Proxy) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	address, err := netip.ParseAddrPort(r.RemoteAddr)
	if err != nil {
		refuse(w, http.StatusForbidden, "murder_not_a_tenant", "plugins answer the tenants of this node only")
		return
	}
	config := p.config()
	tenantName, tenant, ok := config.tenantAt(address.Addr().Unmap())
	if !ok {
		refuse(w, http.StatusForbidden, "murder_not_a_tenant", "plugins answer the tenants of this node only, from their own network")
		return
	}
	pluginName, rest, _ := strings.Cut(strings.TrimPrefix(r.URL.EscapedPath(), "/"), "/")
	plugin, ok := config.Plugins[pluginName]
	if !ok {
		refuse(w, http.StatusNotFound, "murder_no_plugin", fmt.Sprintf("no plugin %q is installed", pluginName))
		return
	}
	grant, ok := tenant.Grants[pluginName]
	if !ok {
		refuse(w, http.StatusForbidden, "murder_plugin_not_granted", fmt.Sprintf("tenant %s may not use plugin %s: its app does not list it under plugins", tenantName, pluginName))
		return
	}
	// A protocol switch (the Realtime API's WebSockets) would pass
	// unmetered.
	if r.Header.Get("Upgrade") != "" || r.Method == http.MethodConnect {
		refuse(w, http.StatusBadRequest, "murder_bad_request", "plugins pass no protocol switch, such as a WebSocket, on: what it used would not be counted")
		return
	}
	release, ok := p.enter(tenantName)
	if !ok {
		refuse(w, http.StatusTooManyRequests, "murder_too_many_requests", fmt.Sprintf("tenant %s has %d requests to plugins under way, the most at once", tenantName, MaxPerTenant))
		return
	}
	defer release()
	now := p.now()
	for _, limit := range grant.Limits {
		if !limit.Hard() {
			continue
		}
		if used := p.counter.Used(tenantName, pluginName, limit.Metric, limit.Per, now); used >= limit.Amount {
			retry := max(1, int(PeriodEnd(limit.Per, now).Sub(now).Seconds()))
			w.Header().Set("Retry-After", strconv.Itoa(retry))
			refuse(w, http.StatusTooManyRequests, "murder_limit_reached", fmt.Sprintf("tenant %s used %d of its %d %s of plugin %s this %s", tenantName, used, limit.Amount, limit.Metric, pluginName, limit.Per))
			return
		}
	}
	sealed, context := plugin.Key, KeyContext(pluginName)
	if grant.Key != "" {
		sealed, context = grant.Key, TenantKeyContext(pluginName, tenantName)
	}
	if sealed == "" {
		refuse(w, http.StatusServiceUnavailable, "murder_plugin_without_key", fmt.Sprintf("plugin %s has no key: murder plugin key %s", pluginName, pluginName))
		return
	}
	key, err := p.key(context, sealed)
	if err != nil {
		p.log.Error("open a plugin's key", "plugin", pluginName, "tenant", tenantName, "error", err)
		refuse(w, http.StatusServiceUnavailable, "murder_plugin_without_key", fmt.Sprintf("the edge cannot open plugin %s's key", pluginName))
		return
	}
	x := &exchange{proxy: p, tenant: tenantName, plugin: pluginName, spec: plugin.Spec, key: key, started: now}
	// Until the request is answered: its body is held until then.
	defer x.releaseBody()
	var target *url.URL
	x.handler, err = p.handlerFor(r.Context(), x, plugin)
	if err == nil {
		defer x.handler.close()
		target, err = x.handler.request(r, rest)
	}
	if err != nil {
		var status statusError
		if !errors.As(err, &status) {
			status = statusError{code: http.StatusBadRequest, message: err.Error()}
		}
		kind := status.kind
		if kind == "" {
			kind = "murder_bad_request"
		}
		refuse(w, status.code, kind, status.message)
		return
	}
	x.count(MetricRequests, x.detail, 1)
	proxy := &httputil.ReverseProxy{
		Rewrite: func(out *httputil.ProxyRequest) {
			out.Out.URL = target
			out.Out.Host = target.Host
			x.authorize(out.Out)
		},
		Transport:      p.Transport,
		FlushInterval:  -1,
		ModifyResponse: x.response,
		ErrorHandler:   x.failed,
		ErrorLog:       nil,
	}
	proxy.ServeHTTP(w, r)
}

// key opens a sealed key, once.
func (p *Proxy) key(context, sealed string) (string, error) {
	p.mu.Lock()
	defer p.mu.Unlock()
	if key, ok := p.keys[context+"\x00"+sealed]; ok {
		return key, nil
	}
	if p.open == nil {
		return "", errors.New("the edge has no master key")
	}
	key, err := p.open(context, sealed)
	if err != nil {
		return "", err
	}
	if len(p.keys) > 1024 {
		clear(p.keys)
	}
	p.keys[context+"\x00"+sealed] = key
	return key, nil
}

// handler passes one request of a plugin's kind on, and meters it.
type handler interface {
	// request checks the tenant's request for /<plugin>/<rest>, reading
	// its body if it must, and says where it goes.
	request(r *http.Request, rest string) (*url.URL, error)
	// authorize puts the key on the request as it goes out, the tenant's
	// credentials taken off it already.
	authorize(out *http.Request)
	// response meters the provider's answer as it passes, its headers
	// cleared of the key already.
	response(response *http.Response)
	// close lets go of what the handler holds, once the request was
	// answered.
	close()
}

// handlerFor makes the handler of a request of plugin's kind.
func (p *Proxy) handlerFor(ctx context.Context, x *exchange, plugin Plugin) (handler, error) {
	switch plugin.Spec.Kind {
	case KindOpenAI:
		return &openAIHandler{x: x}, nil
	case KindHERE:
		return &hereHandler{x: x}, nil
	case KindContainer:
		return p.containerHandler(x, plugin)
	}
	return nil, statusError{code: http.StatusNotImplemented, message: fmt.Sprintf("this edge does not know plugins of kind %q: upgrade it", plugin.Spec.Kind)}
}

// statusError refuses a request with a status, and a kind of error
// (murder_bad_request when empty).
type statusError struct {
	code    int
	message string
	kind    string
}

func (e statusError) Error() string { return e.message }

// exchange is one request passed on, and what it used.
type exchange struct {
	proxy   *Proxy
	tenant  string
	plugin  string
	spec    Spec
	key     string
	started time.Time
	// detail is what the use is for: the service, for HERE. OpenAI's
	// requests are counted with none, and their tokens by the model that
	// answered: what a tenant sends is not trusted to name one.
	detail string
	// reserved is the room taken for the request's body.
	reserved int64
	// handler passes it on as the plugin's kind does.
	handler handler
}

// bodyChunk is how much of a body is read at a time, each in room taken
// as it comes: a body that says it is long takes no room it did not send.
const bodyChunk = 256 << 10

// readBody reads a request's body to pass it on, in the room the edge
// has for bodies: at most MaxBody.
func (x *exchange) readBody(r *http.Request) ([]byte, error) { return x.readBodyUpTo(r, MaxBody) }

// readBodyUpTo reads a request's body of at most limit bytes.
func (x *exchange) readBodyUpTo(r *http.Request, limit int) ([]byte, error) {
	var body []byte
	chunk := make([]byte, bodyChunk)
	for {
		if !x.proxy.reserve(x.tenant, bodyChunk) {
			return nil, statusError{code: http.StatusServiceUnavailable, message: "the edge holds too many request bodies at once; try again"}
		}
		x.reserved += bodyChunk
		n, err := io.ReadFull(r.Body, chunk)
		body = append(body, chunk[:n]...)
		if len(body) > limit {
			return nil, statusError{code: http.StatusRequestEntityTooLarge, message: fmt.Sprintf("a body is at most %d MB", limit>>20)}
		}
		if err == io.EOF || err == io.ErrUnexpectedEOF {
			break
		}
		if err != nil {
			return nil, statusError{code: http.StatusBadRequest, message: "the request's body could not be read"}
		}
	}
	r.Body = io.NopCloser(bytes.NewReader(body))
	r.ContentLength = int64(len(body))
	r.Header.Set("Content-Length", strconv.Itoa(len(body)))
	r.GetBody = func() (io.ReadCloser, error) { return io.NopCloser(bytes.NewReader(body)), nil }
	return body, nil
}

// releaseBody gives the room for the request's body back.
func (x *exchange) releaseBody() {
	if x.reserved > 0 {
		x.proxy.unreserve(x.tenant, x.reserved)
		x.reserved = 0
	}
}

// count adds to what the tenant used today.
func (x *exchange) count(metric, detail string, amount int64) {
	x.proxy.counter.Add(Count{Tenant: x.tenant, Plugin: x.plugin, Day: Day(x.started), Metric: metric, Detail: detail, Amount: amount})
}

// authorize takes the tenant's credentials off the request, and puts the
// key on it.
func (x *exchange) authorize(out *http.Request) {
	for _, header := range []string{"Authorization", "Api-Key", "X-Api-Key", "Cookie", "Proxy-Authorization", "Openai-Organization", "Openai-Project"} {
		out.Header.Del(header)
	}
	out.Header.Del("X-Forwarded-For")
	x.handler.authorize(out)
}

// response meters the provider's answer as it passes.
func (x *exchange) response(response *http.Response) error {
	// A redirect would say where with the key in it.
	scrubHeaders(response.Header, x.key)
	x.handler.response(response)
	// Trailers, which come after the body, are not passed on: the
	// transport fills them in as the body ends, and the proxy passes on
	// what is there once the body is closed.
	response.Trailer = nil
	response.Body = &withoutTrailers{ReadCloser: response.Body, response: response}
	return nil
}

// withoutTrailers takes a response's trailers off as its body is closed.
type withoutTrailers struct {
	io.ReadCloser
	response *http.Response
}

func (b *withoutTrailers) Close() error {
	err := b.ReadCloser.Close()
	b.response.Trailer = nil
	return err
}

// failed answers a request the provider did not answer. The error is
// logged without the URL: it holds HERE's key.
func (x *exchange) failed(w http.ResponseWriter, r *http.Request, err error) {
	var urlErr *url.Error
	if errors.As(err, &urlErr) {
		err = fmt.Errorf("%s: %w", urlErr.Op, urlErr.Err)
	}
	message := strings.ReplaceAll(err.Error(), x.key, "[key]")
	if errors.Is(err, context.Canceled) {
		return // the tenant went away
	}
	x.proxy.log.Warn("a plugin's provider did not answer", "plugin", x.plugin, "tenant", x.tenant, "error", message)
	refuse(w, http.StatusBadGateway, "murder_provider_unreachable", fmt.Sprintf("plugin %s's provider did not answer: %s", x.plugin, message))
}

// withoutKey takes the parameters named param, in any case, out of a
// query, leaving the rest as it was written.
func withoutKey(query, param string) string {
	var kept []string
	for part := range strings.SplitSeq(query, "&") {
		name, _, _ := strings.Cut(part, "=")
		if unescaped, err := url.QueryUnescape(name); err != nil || strings.EqualFold(unescaped, param) {
			continue
		}
		if part != "" {
			kept = append(kept, part)
		}
	}
	return strings.Join(kept, "&")
}

// withKey adds the key to a query as param.
func withKey(query, param, key string) string {
	if query = withoutKey(query, param); query != "" {
		query += "&"
	}
	return query + url.QueryEscape(param) + "=" + url.QueryEscape(key)
}

// scrubErrorBody takes the key out of an error's body, for a key that goes
// in the query: the provider may say what was asked, key and all. An error
// that cannot be read to take it out, compressed or of more than a MB, is
// not passed on: the tenant is told only that there was one.
func scrubErrorBody(response *http.Response, key string) {
	if response.StatusCode < 400 {
		return
	}
	// Read only as it came: no coding, or identity alone.
	encodings := response.Header.Values("Content-Encoding")
	plain := len(encodings) == 0 || len(encodings) == 1 && (strings.TrimSpace(encodings[0]) == "" || strings.EqualFold(strings.TrimSpace(encodings[0]), "identity"))
	var body []byte
	var err error
	if plain && response.ContentLength <= 1<<20 {
		body, err = io.ReadAll(io.LimitReader(response.Body, 1<<20+1))
	}
	_ = response.Body.Close()
	response.Header.Del("Content-Encoding")
	if body == nil || err != nil || len(body) > 1<<20 {
		body, _ = json.Marshal(map[string]any{"error": map[string]string{"message": fmt.Sprintf("the provider answered %d; what it said is not shown, as it could hold the plugin's key", response.StatusCode), "type": "murder_provider_error", "code": "murder_provider_error"}})
		response.Header.Set("Content-Type", "application/json")
	}
	body = bytes.ReplaceAll(bytes.ReplaceAll(body, []byte(key), []byte("[key]")), []byte(url.QueryEscape(key)), []byte("[key]"))
	response.Body = io.NopCloser(bytes.NewReader(body))
	response.ContentLength = int64(len(body))
	response.Header.Del("Content-Length")
}

// cleanDetail keeps a detail such as a model's name if it is one: at most
// 64 letters, digits and ._:/-; otherwise it is "other".
func cleanDetail(detail string) string {
	if len(detail) > 64 {
		return "other"
	}
	for _, c := range detail {
		if !(c >= 'a' && c <= 'z' || c >= 'A' && c <= 'Z' || c >= '0' && c <= '9' || strings.ContainsRune("._:/-", c)) {
			return "other"
		}
	}
	return detail
}

// scrubHeaders takes key out of what the provider answers.
func scrubHeaders(header http.Header, key string) {
	if key == "" {
		return
	}
	escaped := url.QueryEscape(key)
	for name, values := range header {
		for i, value := range values {
			if strings.Contains(value, key) || strings.Contains(value, escaped) {
				values[i] = strings.ReplaceAll(strings.ReplaceAll(value, key, "[key]"), escaped, "[key]")
			}
		}
		header[name] = values
	}
}

// meteredBody passes a response's body on, calls each with what it read,
// and done once, at its end or when it is closed.
type meteredBody struct {
	body io.ReadCloser
	each func([]byte)
	done func()
	once sync.Once
}

func (b *meteredBody) Read(buffer []byte) (int, error) {
	n, err := b.body.Read(buffer)
	if n > 0 && b.each != nil {
		b.each(buffer[:n])
	}
	if err != nil {
		b.once.Do(b.done)
	}
	return n, err
}

func (b *meteredBody) Close() error {
	b.once.Do(b.done)
	return b.body.Close()
}
