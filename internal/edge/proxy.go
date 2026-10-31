// Package edge is the public entry point: it maps each request's host to a
// tenant and proxies it to that tenant's container.
package edge

import (
	"bufio"
	"context"
	"crypto/tls"
	"errors"
	"io"
	"log/slog"
	"net"
	"net/http"
	"net/http/httputil"
	"net/url"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/PowerPenguini/murder/internal/metrics"
)

// spoofable are client-settable headers that proxies and frameworks use to
// pass on the client's address or the public URL.
var spoofable = []string{
	"X-Real-Ip", "True-Client-Ip", "X-Client-Ip", "Cf-Connecting-Ip", "Fastly-Client-Ip", "X-Cluster-Client-Ip",
	"X-Forwarded", "Forwarded-For", "X-Forwarded-Port", "X-Forwarded-Prefix", "X-Forwarded-Ssl", "X-Forwarded-Scheme",
	"X-Forwarded-Server", "X-Forwarded-Uri", "X-Forwarded-Path", "X-Original-Url", "X-Original-Host", "X-Rewrite-Url", "X-Url-Scheme",
	"Client-Ip", "Front-End-Https", "X-Arr-Ssl", "X-Arr-Log-Id", "X-Proxyuser-Ip", "Wl-Proxy-Client-Ip",
}

// hsts tells a browser that reached a tenant over HTTPS to use nothing else
// for a year.
const hsts = "max-age=31536000"

// Route says where a tenant's traffic goes.
type Route struct {
	// Backend is the tenant container's address and port, which only the
	// host reaches; set when this node runs the tenant.
	Backend string `json:"backend,omitempty"`
	// Relay is the edge of the node that runs the tenant, on the private
	// network: set when another node runs it.
	Relay     string `json:"relay,omitempty"`
	Suspended bool   `json:"suspended,omitempty"`
	// Moving holds requests: the tenant is moving to another node, and
	// for a few seconds runs nowhere.
	Moving bool `json:"moving,omitempty"`
}

// Routes looks tenants up by name.
type Routes interface {
	Route(tenant string) (Route, bool)
}

// Proxy routes "<tenant>.<BaseDomain>" to the tenant's backend, or to the
// edge of the node that runs it. Its reverse proxies are made once and
// shared by every request, with copy buffers from a pool: a request costs
// the proxy little more than the bytes it passes on.
type Proxy struct {
	baseDomain string
	routes     Routes
	log        *slog.Logger
	stats      *metrics.Edge
	// hold is how long a request for a tenant that is moving waits for it.
	hold time.Duration
	// local passes requests to tenants on this node, forward to other
	// nodes' edges (nil outside a cluster), relayed takes requests other
	// edges passed on.
	local, forward, relayed *httputil.ReverseProxy
	// forwardRelayed passes a relayed request on once more, to the node a
	// tenant moved to while the request waited for it.
	forwardRelayed *httputil.ReverseProxy
	relayTransport *http.Transport
}

// HoldMoving is how long a request for a moving tenant waits before it is
// answered that the tenant is not available.
const HoldMoving = 60 * time.Second

// watcher is a set of routes that says when it changes.
type watcher interface {
	Watch() <-chan struct{}
}

// newTransport keeps connections open for the next request: to a tenant,
// one per request at once, or to another edge. It passes bodies on as
// they are: decompressing what a client did not ask compressed is the
// app's choice, not work for the edge.
func newTransport(tlsConfig *tls.Config) *http.Transport {
	return &http.Transport{
		DialContext:           (&net.Dialer{Timeout: 5 * time.Second, KeepAlive: 30 * time.Second}).DialContext,
		TLSClientConfig:       tlsConfig,
		MaxIdleConns:          0, // no limit across tenants
		MaxIdleConnsPerHost:   128,
		IdleConnTimeout:       90 * time.Second,
		ResponseHeaderTimeout: 2*time.Minute + HoldMoving,
		ExpectContinueTimeout: time.Second,
		DisableCompression:    true,
		// WebSockets and other upgrades need HTTP/1.1 on every hop.
		ForceAttemptHTTP2: false,
		WriteBufferSize:   32 << 10,
		ReadBufferSize:    32 << 10,
	}
}

// buffers lends the reverse proxies their copy buffers.
var buffers = &bufferPool{pool: sync.Pool{New: func() any { buffer := make([]byte, 32<<10); return &buffer }}}

type bufferPool struct{ pool sync.Pool }

func (b *bufferPool) Get() []byte { return *b.pool.Get().(*[]byte) }

func (b *bufferPool) Put(buffer []byte) {
	if cap(buffer) == 32<<10 {
		b.pool.Put(&buffer)
	}
}

// hop is what one request needs from the shared reverse proxies.
type hop struct {
	tenant   string
	target   *url.URL
	secure   bool
	recorder *recorder
	// hops is how many edges passed the request on, this one included,
	// when it goes to another edge; 0 when it goes to a tenant.
	hops int
}

// A request is passed on between edges twice at most: to the node its
// tenant runs on, and, if the tenant moved while the request waited there,
// on to the node it moved to. hopsHeader counts the edges it went through;
// only edges of the cluster reach the relay port, and the edge a client
// reaches sets it anew, so a client cannot.
const (
	hopsHeader = "X-Murder-Hops"
	maxHops    = 2
)

// hopsOf is how many edges passed a relayed request on: one if the edge
// before did not say, as an earlier release does not.
func hopsOf(r *http.Request) int {
	if hops, err := strconv.Atoi(r.Header.Get(hopsHeader)); err == nil && hops > 0 {
		return hops
	}
	return 1
}

type hopKey struct{}

func hopOf(r *http.Request) *hop { return r.Context().Value(hopKey{}).(*hop) }

// NewProxy returns a proxy for tenants below baseDomain.
func NewProxy(baseDomain string, routes Routes, log *slog.Logger) *Proxy {
	p := &Proxy{baseDomain: strings.ToLower(baseDomain), routes: routes, log: log, stats: metrics.NewEdge(), hold: HoldMoving}
	p.local = p.reverseProxy(newTransport(nil), false)
	p.relayed = p.reverseProxy(newTransport(nil), true)
	return p
}

// Connections to other edges cross the private network, where an answer
// takes milliseconds: a node that does not answer within relayDialTimeout
// is down. One that was switched off answers nothing on a connection
// opened before either, so a request sent on it would wait for its response
// for as long as a slow app may take; the kernel drops such a connection
// once what was sent stays unacknowledged for relayUserTimeout, and the
// request fails within seconds. A live node acknowledges at once, however
// long its tenant takes to answer.
const (
	relayDialTimeout = 2 * time.Second
	relayUserTimeout = 5 * time.Second
)

// WithRelay passes requests for tenants of other nodes on to their edges,
// with the TLS config that proves this node belongs to the cluster.
func (p *Proxy) WithRelay(config *tls.Config) *Proxy {
	config = config.Clone()
	// A connection to another edge opened again resumes its TLS session.
	config.ClientSessionCache = tls.NewLRUClientSessionCache(64)
	p.relayTransport = newTransport(config)
	p.relayTransport.DialContext = (&net.Dialer{
		Timeout: relayDialTimeout, KeepAlive: 15 * time.Second, Control: userTimeout(relayUserTimeout),
	}).DialContext
	p.forward = p.reverseProxy(p.relayTransport, false)
	p.forwardRelayed = p.reverseProxy(p.relayTransport, true)
	return p
}

// Warm opens, or keeps open, a connection to each of relays, so the first
// request for a tenant of another node waits for no TLS handshake. It
// asks for nothing: the relay answers a request with no tenant at once.
func (p *Proxy) Warm(ctx context.Context, relays []string) {
	if p.relayTransport == nil {
		return
	}
	var group sync.WaitGroup
	for _, relay := range relays {
		group.Go(func() {
			ctx, cancel := context.WithTimeout(ctx, 5*time.Second)
			defer cancel()
			request, err := http.NewRequestWithContext(ctx, http.MethodHead, "https://"+relay+"/", nil)
			if err != nil {
				return
			}
			request.Header.Set(warmHeader, "1")
			response, err := p.relayTransport.RoundTrip(request)
			if err != nil {
				return
			}
			_, _ = io.Copy(io.Discard, response.Body)
			_ = response.Body.Close()
		})
	}
	group.Wait()
}

// Relays lists the other nodes' edges the routes lead to.
func Relays(table Table) []string {
	seen := map[string]bool{}
	var relays []string
	for _, route := range table.Routes {
		if route.Relay != "" && !seen[route.Relay] {
			seen[route.Relay] = true
			relays = append(relays, route.Relay)
		}
	}
	return relays
}

// reverseProxy passes requests on to the hop's target. A request the
// client reached this edge with gets this edge's view of the client; one
// another edge relayed keeps that edge's.
func (p *Proxy) reverseProxy(transport http.RoundTripper, relayed bool) *httputil.ReverseProxy {
	return &httputil.ReverseProxy{
		Rewrite: func(out *httputil.ProxyRequest) {
			h := hopOf(out.In)
			if relayed {
				// The edge the client reached observed these.
				for _, name := range forwarded {
					if value := out.In.Header.Get(name); value != "" {
						out.Out.Header.Set(name, value)
					}
				}
			} else {
				// The app sees only what this proxy observed: Rewrite
				// drops Forwarded and X-Forwarded-For/-Host/-Proto, and
				// every other header apps read a client address or URL
				// from is dropped here.
				for _, name := range spoofable {
					out.Out.Header.Del(name)
				}
				// CGI-style servers map "X_Forwarded_For" to the same
				// variable as "X-Forwarded-For"; no legitimate client
				// needs underscores.
				for name := range out.Out.Header {
					if strings.IndexByte(name, '_') >= 0 {
						out.Out.Header.Del(name)
					}
				}
				out.SetXForwarded()
			}
			if h.hops > 0 {
				out.Out.Header.Set(hopsHeader, strconv.Itoa(h.hops))
			} else {
				out.Out.Header.Del(hopsHeader)
			}
			out.SetURL(h.target)
			out.Out.Host = out.In.Host
		},
		Transport:  transport,
		BufferPool: buffers,
		// An app's own Strict-Transport-Security wins.
		ModifyResponse: func(response *http.Response) error {
			if hopOf(response.Request).secure && response.Header.Get("Strict-Transport-Security") == "" {
				response.Header.Set("Strict-Transport-Security", hsts)
			}
			return nil
		},
		// Streams (server-sent events, responses of unknown length) are
		// flushed as they come; others go out in full buffers, in fewer
		// writes.
		FlushInterval: 0,
		ErrorHandler: func(w http.ResponseWriter, r *http.Request, err error) {
			h := hopOf(r)
			// Once the connection is taken over, a failure is the client's,
			// and nothing can be written back.
			if errors.Is(err, http.ErrAbortHandler) || r.Context().Err() != nil || h.recorder.hijacked {
				return
			}
			p.stats.BackendError(h.tenant)
			p.log.Warn("proxy to tenant", "tenant", h.tenant, "error", err)
			w.Header().Set("Retry-After", "10")
			http.Error(w, "this workspace is not available right now", http.StatusBadGateway)
		},
	}
}

// forwarded are the headers an edge passes on to the edge it relays to.
var forwarded = []string{"X-Forwarded-For", "X-Forwarded-Proto", "X-Forwarded-Host"}

// Stats returns the proxy's request counters.
func (p *Proxy) Stats() *metrics.Edge { return p.stats }

// Tenant returns the tenant a host name belongs to: exactly one label in
// front of the base domain.
func (p *Proxy) Tenant(host string) (string, bool) {
	if h, _, err := net.SplitHostPort(host); err == nil {
		host = h
	}
	host = strings.TrimSuffix(strings.ToLower(host), ".")
	label, ok := strings.CutSuffix(host, "."+p.baseDomain)
	if !ok || label == "" || strings.Contains(label, ".") {
		return "", false
	}
	return label, true
}

func (p *Proxy) ServeHTTP(w http.ResponseWriter, r *http.Request) { p.serve(w, r, false) }

// Relay serves other nodes' edges passing requests on to tenants this node
// runs. The client's address and scheme are theirs to say: the TLS
// handshake proved the peer belongs to the cluster. A tenant this node does
// not run, say one that moved away while the request waited for it, is
// passed on once more, to the node it runs on, and no further.
func (p *Proxy) Relay() http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		// Another edge keeping its connection warm: nothing to count.
		if r.Method == http.MethodHead && r.Header.Get(warmHeader) != "" {
			w.WriteHeader(http.StatusNoContent)
			return
		}
		p.serve(w, r, true)
	})
}

// warmHeader marks a request that only keeps a relay connection open.
const warmHeader = "X-Murder-Warm"

// route looks a tenant up, waiting while it moves.
func (p *Proxy) route(r *http.Request, tenant string) (Route, bool) {
	route, ok := p.routes.Route(tenant)
	watch, watched := p.routes.(watcher)
	if !ok || !route.Moving || !watched {
		return route, ok
	}
	deadline := time.NewTimer(p.hold)
	defer deadline.Stop()
	for route.Moving {
		changed := watch.Watch()
		if route, ok = p.routes.Route(tenant); !ok || !route.Moving {
			break
		}
		select {
		case <-changed:
		case <-deadline.C:
			return route, ok
		case <-r.Context().Done():
			return route, ok
		}
	}
	return route, ok
}

func (p *Proxy) serve(w http.ResponseWriter, r *http.Request, relayed bool) {
	tenant, ok := p.Tenant(r.Host)
	if !ok {
		p.stats.Unrouted()
		http.Error(w, "unknown host", http.StatusNotFound)
		return
	}
	route, ok := p.route(r, tenant)
	if !ok {
		p.stats.Unrouted()
		// An edge that has no routes from its agent yet, such as on a node
		// that just joined, does not know which tenants exist.
		if ready, ok := p.routes.(interface{ Ready() bool }); ok && !ready.Ready() {
			w.Header().Set("Retry-After", "5")
			http.Error(w, "this server is starting; try again shortly", http.StatusServiceUnavailable)
			return
		}
		http.Error(w, "unknown tenant", http.StatusNotFound)
		return
	}
	recorder := &recorder{ResponseWriter: w, start: time.Now()}
	w = recorder
	// A request the client gave up before any response is not counted.
	defer func() {
		if recorder.status != 0 {
			p.stats.Request(tenant, recorder.status, recorder.wait)
		}
	}()
	if route.Suspended {
		w.Header().Set("Retry-After", "3600")
		http.Error(w, "this workspace is suspended", http.StatusServiceUnavailable)
		return
	}
	unavailable := func() {
		w.Header().Set("Retry-After", "10")
		http.Error(w, "this workspace is not available right now", http.StatusServiceUnavailable)
	}
	h := &hop{tenant: tenant, recorder: recorder, secure: r.TLS != nil && !relayed}
	var proxy *httputil.ReverseProxy
	switch {
	case route.Moving:
		unavailable() // it did not arrive in time
		return
	case route.Backend != "":
		h.target, proxy = &url.URL{Scheme: "http", Host: route.Backend}, p.local
		if relayed {
			proxy = p.relayed
		}
	case route.Relay != "" && p.forward != nil && !relayed:
		h.target, h.hops, proxy = &url.URL{Scheme: "https", Host: route.Relay}, 1, p.forward
	case route.Relay != "" && p.forward != nil && hopsOf(r) < maxHops:
		// The tenant moved away while the request waited for it here.
		h.target, h.hops, proxy = &url.URL{Scheme: "https", Host: route.Relay}, hopsOf(r)+1, p.forwardRelayed
	default:
		p.stats.BackendError(tenant)
		p.log.Warn("no way to the tenant from this node", "tenant", tenant, "relayed", relayed)
		unavailable()
		return
	}
	proxy.ServeHTTP(w, r.WithContext(context.WithValue(r.Context(), hopKey{}, h)))
}

// RedirectToHTTPS sends plain HTTP requests for tenant hosts to HTTPS once
// ready reports that the HTTPS listener serves; until then it answers 503
// instead of redirecting to a port that does not answer.
func (p *Proxy) RedirectToHTTPS(ready func() bool) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if _, ok := p.Tenant(r.Host); !ok {
			http.Error(w, "unknown host", http.StatusNotFound)
			return
		}
		if !ready() {
			w.Header().Set("Retry-After", "60")
			http.Error(w, "the secure endpoint is starting; try again shortly", http.StatusServiceUnavailable)
			return
		}
		host := r.Host
		if h, _, err := net.SplitHostPort(host); err == nil {
			host = h
		}
		http.Redirect(w, r, "https://"+host+r.URL.RequestURI(), http.StatusPermanentRedirect)
	})
}

// recorder notes a response's status and when its headers were written.
// Unwrap lets the proxy flush streamed responses and take over upgraded
// connections through it.
type recorder struct {
	http.ResponseWriter
	start    time.Time
	status   int
	wait     time.Duration
	hijacked bool
}

func (r *recorder) WriteHeader(status int) {
	// Informational responses come before the final one, except a
	// protocol switch, which is final.
	if r.status == 0 && (status >= 200 || status == http.StatusSwitchingProtocols) {
		r.status, r.wait = status, time.Since(r.start)
	}
	r.ResponseWriter.WriteHeader(status)
}

func (r *recorder) Write(data []byte) (int, error) {
	if r.status == 0 {
		r.status, r.wait = http.StatusOK, time.Since(r.start)
	}
	return r.ResponseWriter.Write(data)
}

func (r *recorder) Unwrap() http.ResponseWriter { return r.ResponseWriter }

// Hijack takes over the connection for a protocol switch, whose response
// the proxy then writes to it directly.
func (r *recorder) Hijack() (net.Conn, *bufio.ReadWriter, error) {
	conn, buffered, err := http.NewResponseController(r.ResponseWriter).Hijack()
	if err == nil {
		r.hijacked = true
		if r.status == 0 {
			r.status, r.wait = http.StatusSwitchingProtocols, time.Since(r.start)
		}
	}
	return conn, buffered, err
}
