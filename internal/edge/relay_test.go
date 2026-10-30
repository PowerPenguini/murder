package edge

import (
	"bytes"
	"crypto/tls"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/PowerPenguini/murder/internal/cluster"
)

func credentials(t *testing.T, seed byte) *cluster.Credentials {
	t.Helper()
	c, err := cluster.NewCredentials(bytes.Repeat([]byte{seed}, 32), "edge", time.Now())
	if err != nil {
		t.Fatal(err)
	}
	return c
}

// relayPair is two nodes' edges: acme runs on the second, the first passes
// its requests on.
type relayPair struct {
	front, back *httptest.Server
	seen        chan http.Header
}

func newRelayPair(t *testing.T) relayPair {
	t.Helper()
	seen := make(chan http.Header, 10)
	backend := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		seen <- r.Header.Clone()
		_, _ = io.WriteString(w, "acme on node 2")
	}))
	t.Cleanup(backend.Close)
	creds := credentials(t, 7)
	// The second node runs acme; it would pass beta on, but a relayed
	// request goes no further.
	backRoutes := staticRoutes{
		"acme": {Backend: strings.TrimPrefix(backend.URL, "http://")},
		"beta": {Relay: "127.0.0.1:1"},
	}
	backProxy := NewProxy("example.com", backRoutes, quietLog()).WithRelay(creds.RelayClient())
	back := httptest.NewUnstartedServer(backProxy.Relay())
	back.TLS = creds.RelayServer()
	back.StartTLS()
	t.Cleanup(back.Close)
	frontRoutes := staticRoutes{"acme": {Relay: strings.TrimPrefix(back.URL, "https://")}}
	front := httptest.NewServer(NewProxy("example.com", frontRoutes, quietLog()).WithRelay(credentials(t, 7).RelayClient()))
	t.Cleanup(front.Close)
	return relayPair{front: front, back: back, seen: seen}
}

func TestRequestsReachTenantsOfOtherNodes(t *testing.T) {
	pair := newRelayPair(t)
	request, _ := http.NewRequest("GET", pair.front.URL+"/path", nil)
	request.Host = "acme.example.com"
	request.Header.Set("X-Forwarded-For", "6.6.6.6")
	request.Header.Set("X-Real-Ip", "6.6.6.6")
	response, err := http.DefaultClient.Do(request)
	if err != nil {
		t.Fatal(err)
	}
	body, _ := io.ReadAll(response.Body)
	response.Body.Close()
	if response.StatusCode != 200 || string(body) != "acme on node 2" {
		t.Fatalf("%s %q", response.Status, body)
	}
	headers := <-pair.seen
	// The app sees the client the first edge saw, not what it claimed, nor
	// the first edge as its client.
	if headers.Get("X-Forwarded-For") != "127.0.0.1" || headers.Get("X-Real-Ip") != "" || headers.Get("X-Forwarded-Host") != "acme.example.com" || headers.Get("X-Forwarded-Proto") != "http" {
		t.Fatalf("headers at the app: %v", headers)
	}
}

func TestTheRelayTakesOnlyTheCluster(t *testing.T) {
	pair := newRelayPair(t)
	address := strings.TrimPrefix(pair.back.URL, "https://")
	// Without a certificate of the cluster's authority, no handshake.
	for name, config := range map[string]*tls.Config{
		"no certificate":     {InsecureSkipVerify: true},
		"another master key": credentials(t, 8).RelayClient(),
	} {
		conn, err := tls.Dial("tcp", address, config)
		if err == nil {
			_, err = conn.Write([]byte("GET / HTTP/1.1\r\nHost: acme.example.com\r\n\r\n"))
			if err == nil {
				_, err = conn.Read(make([]byte, 1))
			}
			conn.Close()
		}
		if err == nil {
			t.Errorf("%s: the relay answered", name)
		}
	}
	// A relayed request is not passed on again.
	client := &http.Client{Transport: &http.Transport{TLSClientConfig: credentials(t, 7).RelayClient()}}
	request, _ := http.NewRequest("GET", pair.back.URL, nil)
	request.Host = "beta.example.com"
	response, err := client.Do(request)
	if err != nil {
		t.Fatal(err)
	}
	response.Body.Close()
	if response.StatusCode != http.StatusServiceUnavailable {
		t.Fatalf("a relayed request for another node's tenant: %s", response.Status)
	}
}

func TestRequestsWaitWhileTheirTenantMoves(t *testing.T) {
	backend := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_, _ = io.WriteString(w, "arrived")
	}))
	defer backend.Close()
	table := &LiveTable{}
	table.Set(NewTable(map[string]Route{"acme": {Moving: true}}))
	proxy := NewProxy("example.com", table, quietLog())
	server := httptest.NewServer(proxy)
	defer server.Close()
	done := make(chan string, 1)
	start := time.Now()
	go func() {
		request, _ := http.NewRequest("GET", server.URL, nil)
		request.Host = "acme.example.com"
		response, err := http.DefaultClient.Do(request)
		if err != nil {
			done <- err.Error()
			return
		}
		body, _ := io.ReadAll(response.Body)
		response.Body.Close()
		done <- response.Status + " " + string(body)
	}()
	time.Sleep(300 * time.Millisecond)
	table.Set(NewTable(map[string]Route{"acme": {Backend: strings.TrimPrefix(backend.URL, "http://")}}))
	if got := <-done; got != "200 OK arrived" || time.Since(start) < 300*time.Millisecond {
		t.Fatalf("after %v: %s", time.Since(start), got)
	}
	// One that does not arrive in time is answered, not left hanging.
	proxy.hold = 100 * time.Millisecond
	table.Set(NewTable(map[string]Route{"acme": {Moving: true}}))
	request, _ := http.NewRequest("GET", server.URL, nil)
	request.Host = "acme.example.com"
	response, err := http.DefaultClient.Do(request)
	if err != nil {
		t.Fatal(err)
	}
	response.Body.Close()
	if response.StatusCode != http.StatusServiceUnavailable || response.Header.Get("Retry-After") == "" {
		t.Fatalf("a move that did not end: %s", response.Status)
	}
}

func TestAnEdgeWithoutRoutesYetAsksToComeBack(t *testing.T) {
	table := &LiveTable{}
	server := httptest.NewServer(NewProxy("example.com", table, quietLog()))
	defer server.Close()
	get := func() *http.Response {
		request, _ := http.NewRequest("GET", server.URL, nil)
		request.Host = "acme.example.com"
		response, err := http.DefaultClient.Do(request)
		if err != nil {
			t.Fatal(err)
		}
		response.Body.Close()
		return response
	}
	if response := get(); response.StatusCode != http.StatusServiceUnavailable || response.Header.Get("Retry-After") == "" {
		t.Fatalf("before any routes: %s", response.Status)
	}
	table.Set(NewTable(nil))
	if response := get(); response.StatusCode != http.StatusNotFound {
		t.Fatalf("with routes: %s", response.Status)
	}
}
