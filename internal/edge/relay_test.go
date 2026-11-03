package edge

import (
	"bufio"
	"bytes"
	"context"
	"crypto/tls"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"sync/atomic"
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
	front, back           *httptest.Server
	frontProxy, backProxy *Proxy
	seen                  chan http.Header
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
	frontProxy := NewProxy("example.com", frontRoutes, quietLog()).WithRelay(credentials(t, 7).RelayClient())
	front := httptest.NewServer(frontProxy)
	t.Cleanup(front.Close)
	return relayPair{front: front, back: back, frontProxy: frontProxy, backProxy: backProxy, seen: seen}
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
	// Its time to the response counts the wait: over 250 ms.
	counts := proxy.Stats().Counts()["acme"]
	var faster uint64
	for _, n := range counts.Waits[:6] {
		faster += n
	}
	if counts.Count != 1 || faster != 0 {
		t.Fatalf("a request that waited for its tenant: %+v", counts)
	}
	// One that does not arrive in time is answered, not left hanging.
	proxy.holdMoving = 100 * time.Millisecond
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

// relayThrough starts a node whose edge runs backend as acme, and a front
// edge that passes acme's requests on to it; protos gets the protocol of
// each relayed request.
func relayThrough(t *testing.T, backend http.Handler) (front *httptest.Server, protos chan string) {
	t.Helper()
	creds := credentials(t, 7)
	app := httptest.NewServer(backend)
	t.Cleanup(app.Close)
	protos = make(chan string, 10)
	relay := NewProxy("example.com", staticRoutes{"acme": {Backend: strings.TrimPrefix(app.URL, "http://")}}, quietLog()).Relay()
	back := httptest.NewUnstartedServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		protos <- r.Proto
		relay.ServeHTTP(w, r)
	}))
	back.TLS = creds.RelayServer()
	back.StartTLS()
	t.Cleanup(back.Close)
	front = httptest.NewServer(NewProxy("example.com", staticRoutes{"acme": {Relay: strings.TrimPrefix(back.URL, "https://")}}, quietLog()).WithRelay(creds.RelayClient()))
	t.Cleanup(front.Close)
	return front, protos
}

// An app may take its time before it reads an upload: the node is alive,
// and the request is passed on whole, over HTTP/2.
func TestARelayedUploadMayBeReadSlowly(t *testing.T) {
	front, protos := relayThrough(t, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		time.Sleep(7 * time.Second) // longer than any liveness timeout
		n, _ := io.Copy(io.Discard, r.Body)
		fmt.Fprint(w, n)
	}))
	body := bytes.Repeat([]byte("x"), 32<<20)
	request, _ := http.NewRequest(http.MethodPost, front.URL+"/upload", bytes.NewReader(body))
	request.Host = "acme.example.com"
	response, err := http.DefaultClient.Do(request)
	if err != nil {
		t.Fatal(err)
	}
	got, _ := io.ReadAll(response.Body)
	response.Body.Close()
	if response.StatusCode != 200 || string(got) != fmt.Sprint(len(body)) {
		t.Fatalf("%s %q", response.Status, got)
	}
	if proto := <-protos; proto != "HTTP/2.0" {
		t.Fatalf("relayed over %s", proto)
	}
}

// A protocol switch (a WebSocket) is passed on to another node over
// HTTP/1.1, and its connection carries both ways.
func TestAnUpgradeIsRelayed(t *testing.T) {
	front, protos := relayThrough(t, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
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
	}))
	conn, err := net.Dial("tcp", strings.TrimPrefix(front.URL, "http://"))
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()
	_, _ = io.WriteString(conn, "GET /socket HTTP/1.1\r\nHost: acme.example.com\r\nConnection: Upgrade\r\nUpgrade: echo\r\n\r\n")
	reader := bufio.NewReader(conn)
	response, err := http.ReadResponse(reader, nil)
	if err != nil || response.StatusCode != http.StatusSwitchingProtocols {
		t.Fatalf("upgrade = %v, %v", response, err)
	}
	_, _ = io.WriteString(conn, "ping\n")
	if line, err := reader.ReadString('\n'); err != nil || line != "ping\n" {
		t.Fatalf("echo through the relayed upgrade = %q, %v", line, err)
	}
	if proto := <-protos; proto != "HTTP/1.1" {
		t.Fatalf("an upgrade relayed over %s", proto)
	}
}

// A node that completes the handshake and then answers nothing, as one
// switched off answers nothing on a connection opened before, fails its
// requests within seconds instead of holding them for minutes.
func TestASilentNodeFailsItsRequestsWithinSeconds(t *testing.T) {
	pingAfter, pingTimeout := relayPingAfter, relayPingTimeout
	relayPingAfter, relayPingTimeout = 300*time.Millisecond, 300*time.Millisecond
	t.Cleanup(func() { relayPingAfter, relayPingTimeout = pingAfter, pingTimeout })
	creds := credentials(t, 7)
	listener, err := tls.Listen("tcp", "127.0.0.1:0", creds.RelayServer())
	if err != nil {
		t.Fatal(err)
	}
	defer listener.Close()
	go func() {
		for {
			conn, err := listener.Accept()
			if err != nil {
				return
			}
			_ = conn.(*tls.Conn).Handshake()
			defer conn.Close() // and never a byte more
		}
	}()
	front := httptest.NewServer(NewProxy("example.com", staticRoutes{"acme": {Relay: listener.Addr().String()}}, quietLog()).WithRelay(creds.RelayClient()))
	defer front.Close()
	request, _ := http.NewRequest(http.MethodGet, front.URL, nil)
	request.Host = "acme.example.com"
	start := time.Now()
	response, err := http.DefaultClient.Do(request)
	if err != nil {
		t.Fatal(err)
	}
	response.Body.Close()
	if response.StatusCode != http.StatusBadGateway || time.Since(start) > 5*time.Second {
		t.Fatalf("%s after %s", response.Status, time.Since(start))
	}
}

// handedListener shares one socket between servers, as edges do across a
// handover: closing one server's listener leaves the socket to the next.
type handedListener struct {
	conns <-chan net.Conn
	done  chan struct{}
	addr  net.Addr
	once  sync.Once
}

func (l *handedListener) Accept() (net.Conn, error) {
	select {
	case conn := <-l.conns:
		return conn, nil
	case <-l.done:
		return nil, net.ErrClosed
	}
}

func (l *handedListener) Close() error   { l.once.Do(func() { close(l.done) }); return nil }
func (l *handedListener) Addr() net.Addr { return l.addr }

// A node whose edge hands over answers its open connections GOAWAY; a
// relayed request it did not take, body and all, goes again on a new
// connection instead of failing.
func TestRelayedRequestsSurviveTheTargetsHandover(t *testing.T) {
	creds := credentials(t, 7)
	app := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		body, _ := io.ReadAll(r.Body)
		_, _ = w.Write(body)
	}))
	defer app.Close()
	relay := NewProxy("example.com", staticRoutes{"acme": {Backend: strings.TrimPrefix(app.URL, "http://")}}, quietLog()).Relay()
	inner, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer inner.Close()
	conns := make(chan net.Conn)
	go func() {
		for {
			conn, err := inner.Accept()
			if err != nil {
				return
			}
			conns <- conn
		}
	}()
	serve := func() *http.Server {
		server := &http.Server{Handler: relay}
		go server.Serve(tls.NewListener(&handedListener{conns: conns, done: make(chan struct{}), addr: inner.Addr()}, creds.RelayServer()))
		return server
	}
	old := serve()
	front := httptest.NewServer(NewProxy("example.com", staticRoutes{"acme": {Relay: inner.Addr().String()}}, quietLog()).WithRelay(creds.RelayClient()))
	defer front.Close()

	var failed atomic.Int32
	var group sync.WaitGroup
	stop := make(chan struct{})
	for worker := range 8 {
		group.Go(func() {
			for i := 0; ; i++ {
				select {
				case <-stop:
					return
				default:
				}
				want := fmt.Sprintf("worker %d request %d", worker, i)
				request, _ := http.NewRequest(http.MethodPut, front.URL, strings.NewReader(want))
				request.Host = "acme.example.com"
				response, err := http.DefaultClient.Do(request)
				if err != nil {
					failed.Add(1)
					continue
				}
				got, _ := io.ReadAll(response.Body)
				response.Body.Close()
				if response.StatusCode != 200 || string(got) != want {
					failed.Add(1)
				}
			}
		})
	}
	// Hand over a few times while requests flow.
	for range 5 {
		time.Sleep(100 * time.Millisecond)
		next := serve()
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		_ = old.Shutdown(ctx)
		cancel()
		old = next
	}
	time.Sleep(100 * time.Millisecond)
	close(stop)
	group.Wait()
	_ = old.Close()
	if n := failed.Load(); n != 0 {
		t.Fatalf("%d relayed requests failed across handovers", n)
	}
}

// Only a small body of known length is kept to send again, within a
// budget for all requests; others stream.
func TestOnlySmallBodiesAreKeptToSendAgain(t *testing.T) {
	for name, c := range map[string]struct {
		length int64
		kept   bool
	}{
		"small":          {length: 10, kept: true},
		"over the cap":   {length: replayBody + 1, kept: false},
		"length unknown": {length: -1, kept: false},
	} {
		body := strings.Repeat("x", int(max(c.length, 10)))
		original, _ := http.NewRequest(http.MethodPut, "https://edge/", io.NopCloser(strings.NewReader(body)))
		original.ContentLength = c.length
		request, release, err := replayable(original)
		if err != nil {
			t.Fatal(err)
		}
		if (request.GetBody != nil) != c.kept {
			t.Errorf("%s: kept %v", name, request.GetBody != nil)
		}
		if original.GetBody != nil {
			t.Errorf("%s: the caller's request was changed", name)
		}
		if c.kept {
			for range 2 {
				again, _ := request.GetBody()
				if got, _ := io.ReadAll(again); string(got) != body {
					t.Errorf("%s: sent again as %q", name, got)
				}
			}
		}
		release()
	}
	if replayKept.Load() != 0 {
		t.Fatalf("%d bytes still counted as kept", replayKept.Load())
	}
	// With the budget taken, bodies stream.
	replayKept.Add(replayBudget)
	defer replayKept.Add(-replayBudget)
	original, _ := http.NewRequest(http.MethodPut, "https://edge/", strings.NewReader("small"))
	original.GetBody = nil
	if request, release, _ := replayable(original); request.GetBody != nil {
		t.Fatal("a body was kept beyond the budget")
	} else {
		release()
	}
}

// A request is counted as served once, by the edge of the node that runs
// the tenant: the agent measures its tenants by these counts.
func TestARelayedRequestIsServedOnce(t *testing.T) {
	pair := newRelayPair(t)
	request, _ := http.NewRequest("GET", pair.front.URL+"/", nil)
	request.Host = "acme.example.com"
	response, err := http.DefaultClient.Do(request)
	if err != nil {
		t.Fatal(err)
	}
	_, _ = io.Copy(io.Discard, response.Body)
	response.Body.Close()
	<-pair.seen
	if served := pair.frontProxy.Served().Counts(); served["acme"].Count != 0 {
		t.Fatalf("the edge that passed it on served it: %+v", served)
	}
	// The back edge counts once its response is written.
	deadline := time.Now().Add(5 * time.Second)
	for pair.backProxy.Served().Counts()["acme"].Count == 0 && time.Now().Before(deadline) {
		time.Sleep(10 * time.Millisecond)
	}
	if served := pair.backProxy.Served().Counts()["acme"]; served.Count != 1 || served.ByClass[1] != 1 || len(served.Waits) != 13 {
		t.Fatalf("the edge that served it: %+v", served)
	}
	// Both still count it for Prometheus, each its own way.
	if pair.frontProxy.Stats().Counts()["acme"].Count != 1 {
		t.Fatal("the front edge's own counts lost the request")
	}
}

// Only so many requests are held for a moving tenant at once; the rest
// are answered at once.
func TestHeldRequestsAreBounded(t *testing.T) {
	backend := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_, _ = io.WriteString(w, "arrived")
	}))
	defer backend.Close()
	table := &LiveTable{}
	table.Set(NewTable(map[string]Route{"acme": {Moving: true}}))
	proxy := NewProxy("example.com", table, quietLog())
	proxy.held = make(chan struct{}, 1)
	server := httptest.NewServer(proxy)
	defer server.Close()
	get := func() (int, error) {
		request, _ := http.NewRequest("GET", server.URL, nil)
		request.Host = "acme.example.com"
		response, err := http.DefaultClient.Do(request)
		if err != nil {
			return 0, err
		}
		response.Body.Close()
		return response.StatusCode, nil
	}
	first := make(chan int, 1)
	go func() { status, _ := get(); first <- status }()
	for len(proxy.held) == 0 {
		time.Sleep(time.Millisecond)
	}
	start := time.Now()
	if status, err := get(); err != nil || status != http.StatusServiceUnavailable || time.Since(start) > time.Second {
		t.Fatalf("a request beyond the bound: %d, %v after %v", status, err, time.Since(start))
	}
	table.Set(NewTable(map[string]Route{"acme": {Backend: strings.TrimPrefix(backend.URL, "http://")}}))
	if status := <-first; status != http.StatusOK {
		t.Fatalf("the request held: %d", status)
	}
	if len(proxy.held) != 0 {
		t.Fatalf("%d places still taken", len(proxy.held))
	}
}

// A client cannot pass a request for its tenant off as another edge
// keeping its connection warm: the header goes, and only the request Warm
// sends is taken for one.
func TestAClientCannotPassForAWarmUp(t *testing.T) {
	pair := newRelayPair(t)
	request, _ := http.NewRequest(http.MethodHead, pair.front.URL+"/private", nil)
	request.Host = "acme.example.com"
	request.Header.Set(warmHeader, "1")
	response, err := http.DefaultClient.Do(request)
	if err != nil {
		t.Fatal(err)
	}
	response.Body.Close()
	if response.StatusCode != http.StatusOK {
		t.Fatalf("a client's HEAD with the warm-up header: %s", response.Status)
	}
	select {
	case headers := <-pair.seen:
		if headers.Get(warmHeader) != "" {
			t.Fatalf("the warm-up header reached the app: %v", headers)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("the app was not asked")
	}
	// Nor is a request for a tenant one on the relay, however it came.
	client := &http.Client{Transport: &http.Transport{TLSClientConfig: credentials(t, 7).RelayClient()}}
	request, _ = http.NewRequest(http.MethodHead, pair.back.URL+"/", nil)
	request.Host = "acme.example.com"
	request.Header.Set(warmHeader, "1")
	if response, err = client.Do(request); err != nil {
		t.Fatal(err)
	}
	response.Body.Close()
	if response.StatusCode != http.StatusOK || len(pair.seen) != 1 {
		t.Fatalf("a request for a tenant with the warm-up header: %s", response.Status)
	}
	<-pair.seen
	// Warm's own is answered by the relay, and goes no further.
	pair.frontProxy.Warm(context.Background(), []string{strings.TrimPrefix(pair.back.URL, "https://")})
	if len(pair.seen) != 0 {
		t.Fatal("a warm-up reached the app")
	}
}

// An edge gives up on a body for another node that stopped coming, and
// gives back what it held for it; a body that keeps coming, however
// slowly, is passed on.
func TestAStalledUploadIsGivenUp(t *testing.T) {
	pair := newRelayPair(t)
	server := httptest.NewServer(UploadDeadline(pair.frontProxy, 200*time.Millisecond))
	defer server.Close()
	send := func(parts ...string) (*http.Response, time.Duration) {
		t.Helper()
		conn, err := net.Dial("tcp", server.Listener.Addr().String())
		if err != nil {
			t.Fatal(err)
		}
		t.Cleanup(func() { conn.Close() })
		start := time.Now()
		fmt.Fprint(conn, "POST /upload HTTP/1.1\r\nHost: acme.example.com\r\nContent-Length: 10\r\n\r\n")
		for _, part := range parts {
			time.Sleep(100 * time.Millisecond)
			fmt.Fprint(conn, part)
		}
		_ = conn.SetReadDeadline(time.Now().Add(5 * time.Second))
		response, err := http.ReadResponse(bufio.NewReader(conn), nil)
		if err != nil {
			t.Fatal(err)
		}
		response.Body.Close()
		return response, time.Since(start)
	}
	// One byte of ten, then nothing.
	if response, took := send("x"); response.StatusCode != http.StatusRequestTimeout || took > 2*time.Second {
		t.Fatalf("a body that stopped: %s after %v", response.Status, took)
	}
	if len(pair.seen) != 0 || len(pair.frontProxy.held) != 0 || replayKept.Load() != 0 {
		t.Fatalf("after a body that stopped: app asked %d times, %d held, %d bytes kept", len(pair.seen), len(pair.frontProxy.held), replayKept.Load())
	}
	// Ten bytes over a second, a tenth of a second apart.
	if response, _ := send(strings.Split("xxxxxxxxxx", "")...); response.StatusCode != http.StatusOK {
		t.Fatalf("a slow body: %s", response.Status)
	}
	<-pair.seen
}
