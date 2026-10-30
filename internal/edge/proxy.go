// Package edge is the public entry point: it maps each request's host to a
// tenant and proxies it to that tenant's container.
package edge

import (
	"bufio"
	"crypto/tls"
	"errors"
	"log/slog"
	"net"
	"net/http"
	"net/http/httputil"
	"net/url"
	"strings"
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
// edge of the node that runs it.
type Proxy struct {
	baseDomain string
	routes     Routes
	log        *slog.Logger
	transport  http.RoundTripper
	// relay reaches other nodes' edges; nil outside a cluster.
	relay http.RoundTripper
	stats *metrics.Edge
	// hold is how long a request for a tenant that is moving waits for it.
	hold time.Duration
}

// HoldMoving is how long a request for a moving tenant waits before it is
// answered that the tenant is not available.
const HoldMoving = 60 * time.Second

// watcher is a set of routes that says when it changes.
type watcher interface {
	Watch() <-chan struct{}
}

// WithRelay passes requests for tenants of other nodes on to their edges,
// with the TLS config that proves this node belongs to the cluster.
func (p *Proxy) WithRelay(config *tls.Config) *Proxy {
	p.relay = &http.Transport{
		TLSClientConfig:       config,
		DialContext:           (&net.Dialer{Timeout: 5 * time.Second, KeepAlive: 30 * time.Second}).DialContext,
		MaxIdleConnsPerHost:   32,
		IdleConnTimeout:       90 * time.Second,
		ResponseHeaderTimeout: 2*time.Minute + HoldMoving,
		ForceAttemptHTTP2:     false,
	}
	return p
}

// NewProxy returns a proxy for tenants below baseDomain.
func NewProxy(baseDomain string, routes Routes, log *slog.Logger) *Proxy {
	transport := &http.Transport{
		DialContext:           (&net.Dialer{Timeout: 5 * time.Second, KeepAlive: 30 * time.Second}).DialContext,
		MaxIdleConnsPerHost:   32,
		IdleConnTimeout:       90 * time.Second,
		ResponseHeaderTimeout: 2 * time.Minute,
		ExpectContinueTimeout: time.Second,
	}
	return &Proxy{baseDomain: strings.ToLower(baseDomain), routes: routes, log: log, transport: transport, stats: metrics.NewEdge(), hold: HoldMoving}
}

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
// not run is not passed on again.
func (p *Proxy) Relay() http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { p.serve(w, r, true) })
}

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
	target, transport := &url.URL{Scheme: "http", Host: route.Backend}, p.transport
	switch {
	case route.Moving:
		unavailable() // it did not arrive in time
		return
	case route.Backend != "":
	case route.Relay != "" && p.relay != nil && !relayed:
		target, transport = &url.URL{Scheme: "https", Host: route.Relay}, p.relay
	default:
		p.stats.BackendError(tenant)
		p.log.Warn("no way to the tenant from this node", "tenant", tenant, "relayed", relayed)
		unavailable()
		return
	}
	secure := r.TLS != nil && !relayed
	proxy := &httputil.ReverseProxy{
		Rewrite: func(out *httputil.ProxyRequest) {
			if relayed {
				// The edge the client reached observed these.
				for _, name := range []string{"X-Forwarded-For", "X-Forwarded-Proto", "X-Forwarded-Host"} {
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
					if strings.Contains(name, "_") {
						out.Out.Header.Del(name)
					}
				}
				out.SetXForwarded()
			}
			out.SetURL(target)
			out.Out.Host = out.In.Host
		},
		Transport: transport,
		// An app's own Strict-Transport-Security wins.
		ModifyResponse: func(response *http.Response) error {
			if secure && response.Header.Get("Strict-Transport-Security") == "" {
				response.Header.Set("Strict-Transport-Security", hsts)
			}
			return nil
		},
		// Stream responses as they come, for server-sent events.
		FlushInterval: -1,
		ErrorHandler: func(w http.ResponseWriter, r *http.Request, err error) {
			// Once the connection is taken over, a failure is the client's,
			// and nothing can be written back.
			if errors.Is(err, http.ErrAbortHandler) || r.Context().Err() != nil || recorder.hijacked {
				return
			}
			p.stats.BackendError(tenant)
			p.log.Warn("proxy to tenant", "tenant", tenant, "error", err)
			w.Header().Set("Retry-After", "10")
			http.Error(w, "this workspace is not available right now", http.StatusBadGateway)
		},
	}
	proxy.ServeHTTP(w, r)
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
