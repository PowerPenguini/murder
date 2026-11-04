package edge

import (
	"bufio"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"
)

// dial opens a raw connection to server, closed when the test ends.
func dial(t *testing.T, server *httptest.Server) net.Conn {
	t.Helper()
	conn, err := net.Dial("tcp", server.Listener.Addr().String())
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { conn.Close() })
	_ = conn.SetDeadline(time.Now().Add(10 * time.Second))
	return conn
}

// An Upgrade header switches nothing without Connection: upgrade: a body
// sent with it that stops coming is given up on all the same.
func TestAnUpgradeHeaderDoesNotExemptABody(t *testing.T) {
	read := make(chan error, 1)
	server := httptest.NewServer(UploadDeadline(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_, err := io.Copy(io.Discard, r.Body)
		read <- err
	}), 200*time.Millisecond))
	defer server.Close()
	conn := dial(t, server)
	fmt.Fprint(conn, "POST / HTTP/1.1\r\nHost: acme.example.com\r\nUpgrade: ignored\r\nContent-Length: 10\r\n\r\nx")
	select {
	case err := <-read:
		if err == nil {
			t.Fatal("a body that stopped was read whole")
		}
	case <-time.After(3 * time.Second):
		t.Fatal("a body that stopped is still waited for")
	}
}

// A request that waits for its tenant to arrive, longer than a body may
// stop, has its whole time to send the body once the edge asks for it:
// a client that waits to be told to go on sends nothing before.
func TestAnUploadMayWaitForItsTenant(t *testing.T) {
	backend := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		body, _ := io.ReadAll(r.Body)
		_, _ = fmt.Fprintf(w, "got %s", body)
	}))
	defer backend.Close()
	table := &LiveTable{}
	table.Set(NewTable(map[string]Route{"acme": {Moving: true}}))
	server := httptest.NewServer(UploadDeadline(NewProxy("example.com", table, quietLog()), 200*time.Millisecond))
	defer server.Close()
	conn := dial(t, server)
	fmt.Fprint(conn, "POST /upload HTTP/1.1\r\nHost: acme.example.com\r\nExpect: 100-continue\r\nContent-Length: 5\r\n\r\n")
	time.Sleep(500 * time.Millisecond)
	table.Set(NewTable(map[string]Route{"acme": {Backend: strings.TrimPrefix(backend.URL, "http://")}}))
	reader := bufio.NewReader(conn)
	response, err := http.ReadResponse(reader, nil)
	if err != nil || response.StatusCode != http.StatusContinue {
		t.Fatalf("asked for the body: %v, %v", response, err)
	}
	time.Sleep(100 * time.Millisecond)
	fmt.Fprint(conn, "hello")
	// The backend may ask for the body too, its 100 Continue passed on
	// after the edge's own: a client takes any number of them.
	for response.StatusCode == http.StatusContinue {
		if response, err = http.ReadResponse(reader, nil); err != nil {
			t.Fatal(err)
		}
	}
	got, _ := io.ReadAll(response.Body)
	response.Body.Close()
	if response.StatusCode != http.StatusOK || string(got) != "got hello" {
		t.Fatalf("an upload that waited for its tenant: %s %q", response.Status, got)
	}
}

// A connection taken over for a protocol switch before its request's body
// was read whole is the new protocol's: the body's deadline goes with the
// takeover (the server lifts it), however quiet the new protocol is.
func TestATakenOverConnectionHasNoDeadline(t *testing.T) {
	read := make(chan string, 1)
	server := httptest.NewServer(UploadDeadline(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		first := make([]byte, 1)
		if _, err := io.ReadFull(r.Body, first); err != nil {
			read <- err.Error()
			return
		}
		conn, buffered, err := http.NewResponseController(w).Hijack()
		if err != nil {
			read <- err.Error()
			return
		}
		defer conn.Close()
		fmt.Fprint(buffered, "HTTP/1.1 101 Switching Protocols\r\nConnection: Upgrade\r\nUpgrade: test\r\n\r\n")
		_ = buffered.Flush()
		rest := make([]byte, 4)
		if _, err := io.ReadFull(buffered, rest); err != nil {
			read <- err.Error()
			return
		}
		read <- string(first) + string(rest)
	}), 200*time.Millisecond))
	defer server.Close()
	conn := dial(t, server)
	fmt.Fprint(conn, "POST / HTTP/1.1\r\nHost: acme.example.com\r\nConnection: Upgrade\r\nUpgrade: test\r\nContent-Length: 2\r\n\r\nab")
	time.Sleep(500 * time.Millisecond)
	fmt.Fprint(conn, "cde")
	select {
	case got := <-read:
		if got != "abcde" {
			t.Fatalf("read after the switch: %q", got)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("nothing read after the switch")
	}
}

// A handler that answers without reading the body makes the server read
// the rest of it itself as the response goes out (HTTP/1.1); a body that
// stopped must not hold the response, and the handler, there. (The new
// tests close their servers after their connections: a handler held
// would make a deferred Close wait for ever.)
func TestAnAnswerDoesNotWaitForABodyNeverRead(t *testing.T) {
	returned := make(chan struct{})
	redirect := NewProxy("example.com", nil, quietLog()).RedirectToHTTPS(func() bool { return true })
	server := httptest.NewServer(UploadDeadline(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		defer close(returned)
		redirect.ServeHTTP(w, r)
	}), 100*time.Millisecond))
	t.Cleanup(server.Close)
	conn := dial(t, server)
	// The redirect repeats the path: long enough, it is written out
	// before the handler returns.
	path := "/" + strings.Repeat("a", 4096)
	fmt.Fprintf(conn, "GET %s HTTP/1.1\r\nHost: acme.example.com\r\nContent-Length: 10\r\n\r\nx", path)
	select {
	case <-returned:
	case <-time.After(3 * time.Second):
		t.Fatal("the handler still waits for a body it never read")
	}
	response, err := http.ReadResponse(bufio.NewReader(conn), nil)
	if err != nil {
		t.Fatal(err)
	}
	response.Body.Close()
	if response.StatusCode != http.StatusPermanentRedirect {
		t.Fatalf("answer: %s", response.Status)
	}
}

// A CONNECT is not exempt from the deadline: its body, if it has one, must
// keep coming like any other.
func TestAConnectBodyIsWatched(t *testing.T) {
	read := make(chan error, 1)
	server := httptest.NewServer(UploadDeadline(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_, err := io.Copy(io.Discard, r.Body)
		read <- err
	}), 100*time.Millisecond))
	t.Cleanup(server.Close)
	conn := dial(t, server)
	fmt.Fprint(conn, "CONNECT acme.example.com:443 HTTP/1.1\r\nHost: acme.example.com:443\r\nContent-Length: 10\r\n\r\nx")
	select {
	case err := <-read:
		if err == nil {
			t.Fatal("a body that stopped was read whole")
		}
	case <-time.After(3 * time.Second):
		t.Fatal("a CONNECT's body that stopped is still waited for")
	}
}

// The edge tunnels nothing: a CONNECT for a tenant is refused at once, its
// stalled body given up on, and the tenant never sees it.
func TestTheEdgeRefusesConnect(t *testing.T) {
	reached := make(chan struct{}, 1)
	backend := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		reached <- struct{}{}
		_, _ = io.Copy(io.Discard, r.Body)
	}))
	defer backend.Close()
	table := &LiveTable{}
	table.Set(NewTable(map[string]Route{"acme": {Backend: strings.TrimPrefix(backend.URL, "http://")}}))
	proxy := NewProxy("example.com", table, quietLog())
	for name, handler := range map[string]http.Handler{
		"proxy":    proxy,
		"relay":    proxy.Relay(),
		"redirect": proxy.RedirectToHTTPS(func() bool { return true }),
	} {
		t.Run(name, func(t *testing.T) {
			server := httptest.NewServer(UploadDeadline(handler, 100*time.Millisecond))
			t.Cleanup(server.Close)
			conn := dial(t, server)
			fmt.Fprint(conn, "CONNECT acme.example.com:443 HTTP/1.1\r\nHost: acme.example.com:443\r\nContent-Length: 10\r\n\r\nx")
			_ = conn.SetReadDeadline(time.Now().Add(3 * time.Second))
			response, err := http.ReadResponse(bufio.NewReader(conn), &http.Request{Method: http.MethodConnect})
			if err != nil {
				t.Fatal(err)
			}
			response.Body.Close()
			if response.StatusCode != http.StatusNotImplemented {
				t.Fatalf("CONNECT answered %s", response.Status)
			}
		})
	}
	select {
	case <-reached:
		t.Fatal("a CONNECT reached the tenant")
	default:
	}
}
