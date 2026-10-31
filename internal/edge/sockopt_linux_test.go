package edge

import (
	"context"
	"crypto/tls"
	"io"
	"log/slog"
	"net"
	"testing"

	"golang.org/x/sys/unix"
)

// Connections to other edges have the kernel drop them once what was sent
// stays unacknowledged, so a request to a node switched off fails within
// seconds instead of waiting for a response that never comes.
func TestRelayConnectionsGiveUpOnSilentNodes(t *testing.T) {
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer listener.Close()
	go func() {
		if conn, err := listener.Accept(); err == nil {
			defer conn.Close()
			_, _ = io.Copy(io.Discard, conn)
		}
	}()
	proxy := NewProxy("example.test", nil, slog.New(slog.DiscardHandler)).WithRelay(&tls.Config{})
	conn, err := proxy.relayTransport.DialContext(context.Background(), "tcp", listener.Addr().String())
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()
	raw, err := conn.(*net.TCPConn).SyscallConn()
	if err != nil {
		t.Fatal(err)
	}
	var timeout int
	if err := raw.Control(func(fd uintptr) {
		timeout, err = unix.GetsockoptInt(int(fd), unix.IPPROTO_TCP, unix.TCP_USER_TIMEOUT)
	}); err != nil {
		t.Fatal(err)
	}
	if err != nil || timeout != int(relayUserTimeout.Milliseconds()) {
		t.Fatalf("TCP_USER_TIMEOUT = %d ms, %v; want %d ms", timeout, err, relayUserTimeout.Milliseconds())
	}
}
