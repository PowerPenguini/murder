package edge

import (
	"bytes"
	"context"
	"crypto/tls"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/PowerPenguini/murder/internal/cluster"
	"github.com/PowerPenguini/murder/internal/metrics"
)

func credentials(t *testing.T, seed byte) *cluster.Credentials { return credentialsFor(t, seed) }

func credentialsFor(t testing.TB, seed byte) *cluster.Credentials {
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
	// A request two edges passed on already is not passed on again.
	client := &http.Client{Transport: &http.Transport{TLSClientConfig: credentials(t, 7).RelayClient()}}
	request, _ := http.NewRequest("GET", pair.back.URL, nil)
	request.Host = "beta.example.com"
	request.Header.Set(hopsHeader, "2")
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

func TestWarmKeepsAConnectionToEveryRelay(t *testing.T) {
	creds := credentialsFor(t, 7)
	connections := 0
	var mu sync.Mutex
	backProxy := NewProxy("example.com", staticRoutes{}, quietLog())
	back := httptest.NewUnstartedServer(backProxy.Relay())
	back.TLS = creds.RelayServer()
	back.Config.ConnState = func(_ net.Conn, state http.ConnState) {
		if state == http.StateNew {
			mu.Lock()
			connections++
			mu.Unlock()
		}
	}
	back.StartTLS()
	defer back.Close()
	relay := strings.TrimPrefix(back.URL, "https://")
	front := NewProxy("example.com", staticRoutes{}, quietLog()).WithRelay(creds.RelayClient())
	table := NewTable(map[string]Route{"acme": {Relay: relay}, "beta": {Relay: relay}, "gamma": {Backend: "10.0.0.1:8080"}})
	if relays := Relays(table); len(relays) != 1 || relays[0] != relay {
		t.Fatalf("relays = %v", relays)
	}
	for range 3 {
		front.Warm(context.Background(), Relays(table))
	}
	mu.Lock()
	defer mu.Unlock()
	if connections != 1 {
		t.Fatalf("%d connections for three warmings", connections)
	}
	// Warming counts as no request on the other edge.
	var out bytes.Buffer
	writer := metrics.NewWriter(&out)
	backProxy.Stats().Write(writer)
	if strings.Contains(out.String(), "murder_edge_unrouted_requests_total 1") || strings.Contains(out.String(), "murder_edge_unrouted_requests_total 3") {
		t.Fatalf("warming was counted:\n%s", out.String())
	}
}

// The edge passes bodies on as they are: it asks a tenant for no
// compression a client did not ask for, and decompresses nothing.
func TestTheEdgeLeavesCompressionToClientAndApp(t *testing.T) {
	seen := make(chan string, 2)
	backend := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		seen <- r.Header.Get("Accept-Encoding")
		if r.Header.Get("Accept-Encoding") == "gzip" {
			w.Header().Set("Content-Encoding", "gzip")
			_, _ = w.Write([]byte("compressed bytes"))
			return
		}
		_, _ = io.WriteString(w, "plain")
	}))
	defer backend.Close()
	front := httptest.NewServer(NewProxy("example.com", staticRoutes{"acme": {Backend: strings.TrimPrefix(backend.URL, "http://")}}, quietLog()))
	defer front.Close()
	client := &http.Client{Transport: &http.Transport{DisableCompression: true}}
	for _, encoding := range []string{"", "gzip"} {
		request, _ := http.NewRequest("GET", front.URL, nil)
		request.Host = "acme.example.com"
		if encoding != "" {
			request.Header.Set("Accept-Encoding", encoding)
		}
		response, err := client.Do(request)
		if err != nil {
			t.Fatal(err)
		}
		body, _ := io.ReadAll(response.Body)
		response.Body.Close()
		if got := <-seen; got != encoding {
			t.Fatalf("the app was asked for %q, the client asked for %q", got, encoding)
		}
		if encoding == "gzip" && (string(body) != "compressed bytes" || response.Header.Get("Content-Encoding") != "gzip") {
			t.Fatalf("the edge changed a compressed body: %q", body)
		}
	}
}

// A response of unknown length reaches the client as it comes, as an
// event stream does, also when it is not one by its type.
func TestAStreamOfUnknownLengthIsNotHeldBack(t *testing.T) {
	release := make(chan struct{})
	backend := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "text/plain")
		_, _ = io.WriteString(w, "first\n")
		w.(http.Flusher).Flush()
		<-release
		_, _ = io.WriteString(w, "second\n")
	}))
	defer backend.Close()
	defer close(release)
	front := httptest.NewServer(NewProxy("example.com", staticRoutes{"acme": {Backend: strings.TrimPrefix(backend.URL, "http://")}}, quietLog()))
	defer front.Close()
	request, _ := http.NewRequest("GET", front.URL, nil)
	request.Host = "acme.example.com"
	response, err := http.DefaultClient.Do(request)
	if err != nil {
		t.Fatal(err)
	}
	defer response.Body.Close()
	first := make(chan string, 1)
	go func() {
		line := make([]byte, 6)
		_, _ = io.ReadFull(response.Body, line)
		first <- string(line)
	}()
	select {
	case got := <-first:
		if got != "first\n" {
			t.Fatalf("first chunk %q", got)
		}
	case <-time.After(3 * time.Second):
		t.Fatal("the first chunk was held back")
	}
}

// A request that waited on a node for its tenant, which then moved to
// another node, is passed on to that node instead of failing, once.
func TestARequestFollowsItsTenantToTheNodeItMovedTo(t *testing.T) {
	creds := credentials(t, 7)
	seen := make(chan http.Header, 1)
	backend := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		seen <- r.Header.Clone()
		_, _ = io.WriteString(w, "acme on node 3")
	}))
	defer backend.Close()
	third := httptest.NewUnstartedServer(NewProxy("example.com", staticRoutes{"acme": {Backend: strings.TrimPrefix(backend.URL, "http://")}}, quietLog()).WithRelay(creds.RelayClient()).Relay())
	third.TLS = creds.RelayServer()
	third.StartTLS()
	defer third.Close()
	// The second node's routes say acme moved to the third.
	second := httptest.NewUnstartedServer(NewProxy("example.com", staticRoutes{"acme": {Relay: strings.TrimPrefix(third.URL, "https://")}}, quietLog()).WithRelay(creds.RelayClient()).Relay())
	second.TLS = creds.RelayServer()
	second.StartTLS()
	defer second.Close()
	// The first node still has acme on the second.
	front := httptest.NewServer(NewProxy("example.com", staticRoutes{"acme": {Relay: strings.TrimPrefix(second.URL, "https://")}}, quietLog()).WithRelay(creds.RelayClient()))
	defer front.Close()

	request, _ := http.NewRequest("GET", front.URL, nil)
	request.Host = "acme.example.com"
	request.Header.Set("X-Forwarded-For", "6.6.6.6")
	request.Header.Set(hopsHeader, "9") // a client's claim counts for nothing
	response, err := http.DefaultClient.Do(request)
	if err != nil {
		t.Fatal(err)
	}
	body, _ := io.ReadAll(response.Body)
	response.Body.Close()
	if response.StatusCode != 200 || string(body) != "acme on node 3" {
		t.Fatalf("%s %q", response.Status, body)
	}
	headers := <-seen
	if headers.Get("X-Forwarded-For") != "127.0.0.1" || headers.Get(hopsHeader) != "" {
		t.Fatalf("headers at the app: %v", headers)
	}
}
