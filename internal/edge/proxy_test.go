package edge

import (
	"bufio"
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
	handler := NewProxy("example.com", nil, quietLog()).RedirectToHTTPS()
	recorder := httptest.NewRecorder()
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
	tlsConfig, err := NewTLS(context.Background(), config, "example.com", quietLog())
	if err != nil {
		t.Fatal(err)
	}
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
