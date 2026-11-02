package cluster

import (
	"context"
	"net"
	"testing"

	"golang.org/x/sys/unix"
)

// Both ends of a connection between nodes give up on a vanished peer
// after userTimeout.
func TestConnectionsBetweenNodesNoticeAVanishedPeer(t *testing.T) {
	l, err := listenConfig().Listen(context.Background(), "tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer l.Close()
	accepted := make(chan net.Conn, 1)
	go func() {
		conn, err := l.Accept()
		if err != nil {
			close(accepted)
			return
		}
		accepted <- conn
	}()
	dialed, err := netDialer().Dial("tcp", l.Addr().String())
	if err != nil {
		t.Fatal(err)
	}
	defer dialed.Close()
	other, ok := <-accepted
	if !ok {
		t.Fatal("nothing accepted")
	}
	defer other.Close()
	for side, conn := range map[string]net.Conn{"dialed": dialed, "accepted": other} {
		raw, err := conn.(*net.TCPConn).SyscallConn()
		if err != nil {
			t.Fatal(err)
		}
		var got int
		var getErr error
		if err := raw.Control(func(fd uintptr) {
			got, getErr = unix.GetsockoptInt(int(fd), unix.IPPROTO_TCP, unix.TCP_USER_TIMEOUT)
		}); err != nil || getErr != nil {
			t.Fatal(err, getErr)
		}
		if got != int(userTimeout.Milliseconds()) {
			t.Errorf("%s: user timeout %d ms, want %d", side, got, userTimeout.Milliseconds())
		}
		keepAlive, _ := 0, 0
		_ = raw.Control(func(fd uintptr) { keepAlive, _ = unix.GetsockoptInt(int(fd), unix.SOL_SOCKET, unix.SO_KEEPALIVE) })
		if keepAlive != 1 {
			t.Errorf("%s: no keepalive", side)
		}
	}
}
