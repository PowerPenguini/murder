// Package edge is the public entry point: it maps each request's host to a
// tenant and proxies it to that tenant's container.
package edge

import (
	"errors"
	"log/slog"
	"net"
	"net/http"
	"net/http/httputil"
	"net/url"
	"strings"
	"time"
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
	// host reaches.
	Backend   string
	Suspended bool
}

// Routes looks tenants up by name.
type Routes interface {
	Route(tenant string) (Route, bool)
}

// Proxy routes "<tenant>.<BaseDomain>" to the tenant's backend.
type Proxy struct {
	baseDomain string
	routes     Routes
	log        *slog.Logger
	transport  http.RoundTripper
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
	return &Proxy{baseDomain: strings.ToLower(baseDomain), routes: routes, log: log, transport: transport}
}

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

func (p *Proxy) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	tenant, ok := p.Tenant(r.Host)
	if !ok {
		http.Error(w, "unknown host", http.StatusNotFound)
		return
	}
	route, ok := p.routes.Route(tenant)
	if !ok {
		http.Error(w, "unknown tenant", http.StatusNotFound)
		return
	}
	if route.Suspended {
		w.Header().Set("Retry-After", "3600")
		http.Error(w, "this workspace is suspended", http.StatusServiceUnavailable)
		return
	}
	backend := &url.URL{Scheme: "http", Host: route.Backend}
	secure := r.TLS != nil
	proxy := &httputil.ReverseProxy{
		// The app sees only what this proxy observed: Rewrite drops
		// Forwarded and X-Forwarded-For/-Host/-Proto, and every other header
		// apps read a client address or URL from is dropped here.
		Rewrite: func(out *httputil.ProxyRequest) {
			for _, name := range spoofable {
				out.Out.Header.Del(name)
			}
			// CGI-style servers map "X_Forwarded_For" to the same variable
			// as "X-Forwarded-For"; no legitimate client needs underscores.
			for name := range out.Out.Header {
				if strings.Contains(name, "_") {
					out.Out.Header.Del(name)
				}
			}
			out.SetURL(backend)
			out.SetXForwarded()
			out.Out.Host = out.In.Host
		},
		Transport: p.transport,
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
			if errors.Is(err, http.ErrAbortHandler) || r.Context().Err() != nil {
				return
			}
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
