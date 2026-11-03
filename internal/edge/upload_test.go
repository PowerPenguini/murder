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
	if response, err = http.ReadResponse(reader, nil); err != nil {
		t.Fatal(err)
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
