package edge

import (
	"bufio"
	"bytes"
	"context"
	"crypto/tls"
	"crypto/x509"
	"io"
	"log/slog"
	"net"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/PowerPenguini/murder/internal/metrics"
	"go.uber.org/zap"
)

type staticRoutes map[string]Route

func (s staticRoutes) Route(tenant string) (Route, bool) {
	route, ok := s[tenant]
	return route, ok
}

func quietLog() *slog.Logger { return slog.New(slog.NewTextHandler(io.Discard, nil)) }

func TestTenantFromHost(t *testing.T) {
	proxy := NewProxy("Example.com", nil, quietLog())
	for host, want := range map[string]string{"acme.example.com": "acme", "ACME.example.com:443": "acme", "acme.example.com.": "acme"} {
		if got, ok := proxy.Tenant(host); !ok || got != want {
			t.Errorf("Tenant(%q) = %q, %v", host, got, ok)
		}
	}
	for _, host := range []string{"example.com", "a.b.example.com", "acme.example.org", "acmeexample.com", ".example.com"} {
		if got, ok := proxy.Tenant(host); ok {
			t.Errorf("Tenant(%q) = %q, want no tenant", host, got)
		}
	}
}

func TestProxyRoutesAndForwardsHeaders(t *testing.T) {
	backend := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("X-Seen-Host", r.Host)
		w.Header().Set("X-Seen-For", r.Header.Get("X-Forwarded-For"))
		w.Header().Set("X-Seen-Proto", r.Header.Get("X-Forwarded-Proto"))
		for name := range r.Header {
			if r.Header.Get(name) == "spoofed" {
				w.Header().Add("X-Leaked", name)
			}
		}
		_, _ = io.WriteString(w, "hello from "+r.URL.Path)
	}))
	defer backend.Close()
	routes := staticRoutes{
		"acme":  {Backend: strings.TrimPrefix(backend.URL, "http://")},
		"sleep": {Suspended: true},
		"down":  {Backend: "127.0.0.1:1"},
	}
	proxy := httptest.NewServer(NewProxy("example.com", routes, quietLog()))
	defer proxy.Close()

	request, _ := http.NewRequest(http.MethodGet, proxy.URL+"/api/me", nil)
	request.Host = "acme.example.com"
	request.Header.Set("X-Forwarded-For", "6.6.6.6")
	for _, name := range spoofable {
		request.Header.Set(name, "spoofed")
	}
	request.Header["X_Forwarded_For"] = []string{"spoofed"}
	response, err := http.DefaultClient.Do(request)
	if err != nil {
		t.Fatal(err)
	}
	body, _ := io.ReadAll(response.Body)
	response.Body.Close()
	if response.StatusCode != http.StatusOK || string(body) != "hello from /api/me" {
		t.Fatalf("proxied response = %d %q", response.StatusCode, body)
	}
	if response.Header.Get("X-Seen-Host") != "acme.example.com" || response.Header.Get("X-Seen-Proto") != "http" {
		t.Errorf("backend saw host %q proto %q", response.Header.Get("X-Seen-Host"), response.Header.Get("X-Seen-Proto"))
	}
	if leaked := response.Header.Values("X-Leaked"); len(leaked) > 0 {
		t.Errorf("client-sent proxy headers reached the app: %v", leaked)
	}
	if seen := response.Header.Get("X-Seen-For"); seen != "127.0.0.1" {
		t.Errorf("backend saw X-Forwarded-For %q; a client-supplied value must be replaced", seen)
	}

	for host, want := range map[string]int{"sleep.example.com": 503, "down.example.com": 502, "nobody.example.com": 404, "example.com": 404} {
		request, _ := http.NewRequest(http.MethodGet, proxy.URL+"/", nil)
		request.Host = host
		response, err := http.DefaultClient.Do(request)
		if err != nil {
			t.Fatal(err)
		}
		response.Body.Close()
		if response.StatusCode != want {
			t.Errorf("%s answered %d, want %d", host, response.StatusCode, want)
		}
	}
}

// Server-sent events must reach the client as they are written, not when
// the response ends.
func TestProxyStreamsEvents(t *testing.T) {
	release := make(chan struct{})
	backend := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "text/event-stream")
		_, _ = io.WriteString(w, "data: first\n\n")
		w.(http.Flusher).Flush()
		<-release
		_, _ = io.WriteString(w, "data: second\n\n")
	}))
	defer backend.Close()
	defer close(release)
	proxy := httptest.NewServer(NewProxy("example.com", staticRoutes{"acme": {Backend: strings.TrimPrefix(backend.URL, "http://")}}, quietLog()))
	defer proxy.Close()
	request, _ := http.NewRequest(http.MethodGet, proxy.URL+"/events", nil)
	request.Host = "acme.example.com"
	response, err := http.DefaultClient.Do(request)
	if err != nil {
		t.Fatal(err)
	}
	defer response.Body.Close()
	line := make(chan string, 1)
	go func() {
		text, _ := bufio.NewReader(response.Body).ReadString('\n')
		line <- text
	}()
	select {
	case text := <-line:
		if text != "data: first\n" {
			t.Fatalf("first event = %q", text)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("the first event was buffered instead of streamed")
	}
}

func TestRedirectToHTTPS(t *testing.T) {
	ready := false
	handler := NewProxy("example.com", nil, quietLog()).RedirectToHTTPS(func() bool { return ready })
	recorder := httptest.NewRecorder()
	handler.ServeHTTP(recorder, httptest.NewRequest(http.MethodGet, "http://acme.example.com/login", nil))
	if recorder.Code != http.StatusServiceUnavailable {
		t.Fatalf("before HTTPS is ready = %d, want 503 rather than a redirect to a closed port", recorder.Code)
	}
	ready = true
	recorder = httptest.NewRecorder()
	handler.ServeHTTP(recorder, httptest.NewRequest(http.MethodGet, "http://acme.example.com:80/login?next=%2F", nil))
	if recorder.Code != http.StatusPermanentRedirect || recorder.Header().Get("Location") != "https://acme.example.com/login?next=%2F" {
		t.Fatalf("redirect = %d %q", recorder.Code, recorder.Header().Get("Location"))
	}
	recorder = httptest.NewRecorder()
	handler.ServeHTTP(recorder, httptest.NewRequest(http.MethodGet, "http://evil.org/", nil))
	if recorder.Code != http.StatusNotFound {
		t.Fatalf("foreign host = %d", recorder.Code)
	}
}

func TestInternalTLSServesTenantsAndKeepsItsCA(t *testing.T) {
	dir := t.TempDir()
	config := TLSConfig{Mode: TLSInternal, Storage: dir}
	certificates, err := NewTLS(context.Background(), config, "example.com", quietLog())
	if err != nil {
		t.Fatal(err)
	}
	if left := time.Until(certificates.NotAfter()); left < 89*24*time.Hour || left > 90*24*time.Hour {
		t.Fatalf("the certificate expires in %s, want 90 days", left)
	}
	tlsConfig := certificates.Config
	caPEM, err := os.ReadFile(filepath.Join(dir, "ca.pem"))
	if err != nil {
		t.Fatal(err)
	}
	listener, err := tls.Listen("tcp", "127.0.0.1:0", tlsConfig)
	if err != nil {
		t.Fatal(err)
	}
	server := &http.Server{Handler: http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) { _, _ = io.WriteString(w, "secure") })}
	go server.Serve(listener)
	defer server.Close()
	pool := x509.NewCertPool()
	pool.AppendCertsFromPEM(caPEM)
	client := &http.Client{Transport: &http.Transport{
		TLSClientConfig: &tls.Config{RootCAs: pool, ServerName: "acme.example.com"},
		DialContext: func(ctx context.Context, network, _ string) (net.Conn, error) {
			return (&net.Dialer{}).DialContext(ctx, network, listener.Addr().String())
		},
	}}
	response, err := client.Get("https://acme.example.com/")
	if err != nil {
		t.Fatalf("a tenant host does not verify against the internal CA: %v", err)
	}
	response.Body.Close()
	// A second start reuses the CA, so clients keep trusting it.
	if _, err := NewTLS(context.Background(), config, "example.com", quietLog()); err != nil {
		t.Fatal(err)
	}
	again, _ := os.ReadFile(filepath.Join(dir, "ca.pem"))
	if string(again) != string(caPEM) {
		t.Fatal("the internal CA was replaced on restart")
	}
	if info, _ := os.Stat(filepath.Join(dir, "ca-key.pem")); info.Mode().Perm() != 0o600 {
		t.Fatalf("CA key mode = %v", info.Mode().Perm())
	}
}

func TestTLSConfigRejectsIncompleteACME(t *testing.T) {
	if _, err := NewTLS(context.Background(), TLSConfig{Mode: TLSACME}, "example.com", quietLog()); err == nil {
		t.Fatal("acme without email and storage accepted")
	}
	tokenFile := filepath.Join(t.TempDir(), "token")
	_ = os.WriteFile(tokenFile, []byte("no-colon\n"), 0o600)
	if _, err := dnsProvider("namecheap", tokenFile); err == nil {
		t.Fatal("namecheap token without user accepted")
	}
	if _, err := dnsProvider("route53", tokenFile); err == nil {
		t.Fatal("unknown provider accepted")
	}
	if provider, err := dnsProvider("hetzner", tokenFile); err != nil || provider == nil {
		t.Fatalf("hetzner provider = %v, %v", provider, err)
	}
	if _, err := NewTLS(context.Background(), TLSConfig{Mode: "magic"}, "example.com", quietLog()); err == nil {
		t.Fatal("unknown mode accepted")
	}
}

// Over HTTPS the edge adds Strict-Transport-Security unless the app sets
// its own; over plain HTTP it adds nothing.
func TestProxyAddsHSTSOverHTTPS(t *testing.T) {
	backend := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/own" {
			w.Header().Set("Strict-Transport-Security", "max-age=60")
		}
	}))
	defer backend.Close()
	proxy := NewProxy("example.com", staticRoutes{"acme": {Backend: strings.TrimPrefix(backend.URL, "http://")}}, quietLog())
	for target, want := range map[string]string{
		"https://acme.example.com/":    "max-age=31536000",
		"https://acme.example.com/own": "max-age=60",
		"http://acme.example.com/":     "",
	} {
		recorder := httptest.NewRecorder()
		proxy.ServeHTTP(recorder, httptest.NewRequest(http.MethodGet, target, nil))
		if got := recorder.Header().Values("Strict-Transport-Security"); strings.Join(got, ",") != want {
			t.Errorf("%s: Strict-Transport-Security = %q, want %q", target, got, want)
		}
	}
}

// The proxy counts responses per tenant, including the protocol switch of
// an upgraded connection, which still works through the counter.
func TestProxyCountsRequests(t *testing.T) {
	backend := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/fail":
			w.WriteHeader(http.StatusInternalServerError)
		case "/upgrade":
			conn, buffered, err := http.NewResponseController(w).Hijack()
			if err != nil {
				t.Error(err)
				return
			}
			defer conn.Close()
			_, _ = buffered.WriteString("HTTP/1.1 101 Switching Protocols\r\nConnection: Upgrade\r\nUpgrade: echo\r\n\r\n")
			_ = buffered.Flush()
			line, _ := buffered.ReadString('\n')
			_, _ = conn.Write([]byte(line))
		default:
			_, _ = io.WriteString(w, "ok")
		}
	}))
	defer backend.Close()
	proxy := NewProxy("example.com", staticRoutes{
		"acme": {Backend: strings.TrimPrefix(backend.URL, "http://")},
		"down": {Backend: "127.0.0.1:1"},
	}, quietLog())
	server := httptest.NewServer(proxy)
	defer server.Close()
	get := func(host, path string) {
		request, _ := http.NewRequest(http.MethodGet, server.URL+path, nil)
		request.Host = host
		response, err := http.DefaultClient.Do(request)
		if err != nil {
			t.Fatal(err)
		}
		_, _ = io.Copy(io.Discard, response.Body)
		response.Body.Close()
	}
	get("acme.example.com", "/")
	get("acme.example.com", "/")
	get("acme.example.com", "/fail")
	get("down.example.com", "/")
	get("nobody.example.com", "/")
	get("example.org", "/")

	conn, err := net.Dial("tcp", strings.TrimPrefix(server.URL, "http://"))
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()
	_, _ = io.WriteString(conn, "GET /upgrade HTTP/1.1\r\nHost: acme.example.com\r\nConnection: Upgrade\r\nUpgrade: echo\r\n\r\n")
	reader := bufio.NewReader(conn)
	response, err := http.ReadResponse(reader, nil)
	if err != nil || response.StatusCode != http.StatusSwitchingProtocols {
		t.Fatalf("upgrade = %v, %v", response, err)
	}
	_, _ = io.WriteString(conn, "ping\n")
	if line, err := reader.ReadString('\n'); err != nil || line != "ping\n" {
		t.Fatalf("echo through the upgraded connection = %q, %v", line, err)
	}
	conn.Close()

	var out strings.Builder
	deadline := time.Now().Add(5 * time.Second)
	for {
		out.Reset()
		writer := metrics.NewWriter(&out)
		proxy.Stats().Write(writer)
		// The upgraded connection is counted once the proxy lets it go.
		if strings.Contains(out.String(), `murder_edge_requests_total{tenant="acme",code="1xx"} 1`) || time.Now().After(deadline) {
			break
		}
		time.Sleep(20 * time.Millisecond)
	}
	for _, want := range []string{
		`murder_edge_requests_total{tenant="acme",code="1xx"} 1`,
		`murder_edge_requests_total{tenant="acme",code="2xx"} 2`,
		`murder_edge_requests_total{tenant="acme",code="5xx"} 1`,
		`murder_edge_requests_total{tenant="down",code="5xx"} 1`,
		`murder_edge_response_seconds_count{tenant="acme"} 4`,
		`murder_edge_response_seconds_bucket{tenant="acme",le="+Inf"} 4`,
		`murder_edge_backend_errors_total{tenant="acme"} 0`,
		`murder_edge_backend_errors_total{tenant="down"} 1`,
		`murder_edge_unrouted_requests_total 2`,
	} {
		if !strings.Contains(out.String(), want+"\n") {
			t.Errorf("missing %s in:\n%s", want, out.String())
		}
	}
}

// An agent running for months keeps a valid internal certificate: a new one
// is signed, by the same CA, once less than a third of its life is left.
func TestInternalCertificateIsRenewed(t *testing.T) {
	now := time.Now()
	issuer := &internalIssuer{dir: t.TempDir(), names: []string{"*.example.com", "example.com"}, now: func() time.Time { return now }, create: true}
	first, err := issuer.certificate()
	if err != nil {
		t.Fatal(err)
	}
	now = now.Add(59 * 24 * time.Hour)
	if same, _ := issuer.certificate(); same != first {
		t.Fatal("renewed with two thirds of its life left")
	}
	now = now.Add(2 * 24 * time.Hour)
	renewed, err := issuer.certificate()
	if err != nil {
		t.Fatal(err)
	}
	if renewed == first || !renewed.Leaf.NotAfter.After(first.Leaf.NotAfter) || issuer.notAfter() != renewed.Leaf.NotAfter {
		t.Fatalf("not renewed: %v then %v", first.Leaf.NotAfter, renewed.Leaf.NotAfter)
	}
	if !bytes.Equal(renewed.Certificate[1], first.Certificate[1]) {
		t.Fatal("the renewed certificate has another CA")
	}
}

// certmagic's logs reach the agent's logger with their fields.
func TestCertmagicLogsGoToTheAgentLog(t *testing.T) {
	var out bytes.Buffer
	log := slog.New(slog.NewTextHandler(&out, &slog.HandlerOptions{Level: slog.LevelInfo}))
	logger := zap.New(slogCore{log: log}).Named("obtain").With(zap.String("identifier", "*.example.com"))
	logger.Debug("hidden")
	logger.Error("could not get certificate", zap.Int("attempt", 3))
	text := out.String()
	if strings.Contains(text, "hidden") || !strings.Contains(text, `level=ERROR msg="could not get certificate" logger=obtain attempt=3 identifier=*.example.com`) {
		t.Fatalf("log:\n%s", text)
	}
}

// A renewal never makes a new CA, which clients would not trust, and a
// failed one waits a minute before the next attempt.
func TestInternalRenewalKeepsTheCA(t *testing.T) {
	dir := t.TempDir()
	now := time.Now()
	issuer := &internalIssuer{dir: dir, names: []string{"*.example.com", "example.com"}, now: func() time.Time { return now }, create: true}
	first, err := issuer.certificate()
	if err != nil {
		t.Fatal(err)
	}
	caPEM, _ := os.ReadFile(filepath.Join(dir, "ca.pem"))
	if err := os.Rename(filepath.Join(dir, "ca.pem"), filepath.Join(dir, "ca.pem.away")); err != nil {
		t.Fatal(err)
	}
	now = now.Add(61 * 24 * time.Hour)
	if same, err := issuer.certificate(); err != nil || same != first {
		t.Fatalf("a failed renewal = %v, %v; want the current certificate", same, err)
	}
	if _, err := os.Stat(filepath.Join(dir, "ca.pem")); !os.IsNotExist(err) {
		t.Fatalf("a renewal made a new CA: %v", err)
	}
	if err := os.Rename(filepath.Join(dir, "ca.pem.away"), filepath.Join(dir, "ca.pem")); err != nil {
		t.Fatal(err)
	}
	now = now.Add(30 * time.Second)
	if same, _ := issuer.certificate(); same != first {
		t.Fatal("renewal tried again within a minute")
	}
	now = now.Add(time.Minute)
	renewed, err := issuer.certificate()
	if err != nil || renewed == first {
		t.Fatalf("renewal after the wait = %v, %v", renewed, err)
	}
	if again, _ := os.ReadFile(filepath.Join(dir, "ca.pem")); !bytes.Equal(again, caPEM) {
		t.Fatal("the CA changed")
	}
}

// An edge handing over waits, within its drain, for connections taken over
// for a protocol switch (WebSockets), which its servers do not wait for.
func TestAHandoverWaitsForUpgradedConnections(t *testing.T) {
	backend := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		conn, buffered, err := http.NewResponseController(w).Hijack()
		if err != nil {
			return
		}
		defer conn.Close()
		_, _ = buffered.WriteString("HTTP/1.1 101 Switching Protocols\r\nConnection: Upgrade\r\nUpgrade: echo\r\n\r\n")
		_ = buffered.Flush()
		_, _ = io.Copy(conn, buffered) // echo until the client goes
	}))
	defer backend.Close()
	proxy := NewProxy("example.com", staticRoutes{"acme": {Backend: strings.TrimPrefix(backend.URL, "http://")}}, quietLog())
	server := httptest.NewServer(proxy)
	defer server.Close()
	conn, err := net.Dial("tcp", strings.TrimPrefix(server.URL, "http://"))
	if err != nil {
		t.Fatal(err)
	}
	_, _ = io.WriteString(conn, "GET / HTTP/1.1\r\nHost: acme.example.com\r\nConnection: Upgrade\r\nUpgrade: echo\r\n\r\n")
	if response, err := http.ReadResponse(bufio.NewReader(conn), nil); err != nil || response.StatusCode != http.StatusSwitchingProtocols {
		t.Fatalf("upgrade = %v, %v", response, err)
	}
	short, cancel := context.WithTimeout(context.Background(), 200*time.Millisecond)
	defer cancel()
	start := time.Now()
	proxy.WaitUpgraded(short)
	if time.Since(start) < 150*time.Millisecond {
		t.Fatal("did not wait for the open connection")
	}
	conn.Close()
	done := make(chan struct{})
	go func() {
		proxy.WaitUpgraded(context.Background())
		close(done)
	}()
	select {
	case <-done:
	case <-time.After(5 * time.Second):
		t.Fatal("still waiting after the connection closed")
	}
}

// A process beside the edge serves the edge's internal certificate and
// never creates the CA itself: two creating it at once could make two.
func TestSharedTLSWaitsForTheEdgesCA(t *testing.T) {
	dir := t.TempDir()
	config := TLSConfig{Mode: TLSInternal, Storage: dir}
	log := slog.New(slog.NewTextHandler(io.Discard, nil))
	if _, err := SharedTLS(context.Background(), config, "example.com", log); err == nil {
		t.Fatal("the CA was made beside the edge")
	}
	if _, err := os.Stat(filepath.Join(dir, "ca.pem")); err == nil {
		t.Fatal("a CA file was written")
	}
	if _, err := NewTLS(context.Background(), config, "example.com", log); err != nil {
		t.Fatal(err)
	}
	if _, err := SharedTLS(context.Background(), config, "example.com", log); err != nil {
		t.Fatalf("with the edge's CA: %v", err)
	}
}
