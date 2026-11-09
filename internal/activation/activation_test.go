package activation

import (
	"net"
	"os"
	"strconv"
	"syscall"
	"testing"
)

// pass puts copies of files at fds first, first+1, ... and says, as
// systemd does, that they are this process's.
func pass(t *testing.T, first int, files ...*os.File) {
	t.Helper()
	for i, file := range files {
		if err := syscall.Dup3(int(file.Fd()), first+i, 0); err != nil {
			t.Fatal(err)
		}
		t.Cleanup(func() { _ = syscall.Close(first + i) })
	}
	t.Setenv("LISTEN_PID", strconv.Itoa(os.Getpid()))
	t.Setenv("LISTEN_FDS", strconv.Itoa(len(files)))
	t.Setenv("LISTEN_FDNAMES", "edge:edge")
}

func listenerFile(t *testing.T, address string) (*net.TCPListener, *os.File) {
	t.Helper()
	listener, err := net.Listen("tcp", address)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = listener.Close() })
	file, err := listener.(*net.TCPListener).File()
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = file.Close() })
	return listener.(*net.TCPListener), file
}

func closed(fd int) bool {
	_, err := fcntl(fd)
	return err == syscall.EBADF
}

func fcntl(fd int) (uintptr, error) {
	value, _, errno := syscall.Syscall(syscall.SYS_FCNTL, uintptr(fd), syscall.F_GETFD, 0)
	if errno != 0 {
		return 0, errno
	}
	return value, nil
}

// The sockets systemd passed are taken as listeners on the same ports,
// which accept the connections made to them; the fds systemd put them at
// are closed, and the variables unset, so a process started to hand over
// to takes neither.
func TestListenersTakeTheSocketsSystemdPassed(t *testing.T) {
	first := 900
	a, aFile := listenerFile(t, "127.0.0.1:0")
	b, bFile := listenerFile(t, "127.0.0.1:0")
	pass(t, first, aFile, bFile)
	got, err := listeners(first)
	if err != nil {
		t.Fatal(err)
	}
	if len(got) != 2 {
		t.Fatalf("took %d listeners", len(got))
	}
	for i, want := range []*net.TCPListener{a, b} {
		if got[i].Addr().String() != want.Addr().String() {
			t.Fatalf("listener %d on %s, want %s", i, got[i].Addr(), want.Addr())
		}
	}
	for _, name := range []string{"LISTEN_PID", "LISTEN_FDS", "LISTEN_FDNAMES"} {
		if _, set := os.LookupEnv(name); set {
			t.Errorf("%s still set", name)
		}
	}
	if !closed(first) || !closed(first+1) {
		t.Error("the fds systemd passed are still open")
	}
	// The original listener is closed, as the edge that stopped closes
	// its own: the socket still takes connections, for the taken one.
	_ = a.Close()
	conn, err := net.Dial("tcp", got[0].Addr().String())
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()
	accepted, err := got[0].Accept()
	if err != nil {
		t.Fatal(err)
	}
	_ = accepted.Close()
	for _, listener := range got {
		// A taken listener is closed on exec: only a copy tableflip hands
		// over reaches the next edge.
		file, err := listener.(*net.TCPListener).SyscallConn()
		if err != nil {
			t.Fatal(err)
		}
		_ = file.Control(func(fd uintptr) {
			if flags, err := fcntl(int(fd)); err != nil || flags&syscall.FD_CLOEXEC == 0 {
				t.Errorf("listener fd %d flags %d, %v: not closed on exec", fd, flags, err)
			}
		})
		_ = listener.Close()
	}
}

// Sockets meant for another process (LISTEN_PID is not this one's, as in
// a child that inherited the variables) are not taken, and none are
// without the variables.
func TestListenersAreNoneWithoutSocketActivation(t *testing.T) {
	_, file := listenerFile(t, "127.0.0.1:0")
	pass(t, 900, file)
	t.Setenv("LISTEN_PID", strconv.Itoa(os.Getpid()+1))
	if got, err := listeners(900); err != nil || len(got) != 0 {
		t.Fatalf("took %v, %v", got, err)
	}
	if _, set := os.LookupEnv("LISTEN_FDS"); set {
		t.Error("LISTEN_FDS still set")
	}
	if closed(900) {
		t.Error("closed a socket meant for another process")
	}
	if got, err := listeners(900); err != nil || len(got) != 0 {
		t.Fatalf("without the variables: took %v, %v", got, err)
	}
}

// A socket that does not listen (a mistaken ListenDatagram=, say) is left
// out and named; the others are taken.
func TestListenersLeaveOutASocketThatDoesNotListen(t *testing.T) {
	udp, err := net.ListenPacket("udp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer udp.Close()
	udpFile, err := udp.(*net.UDPConn).File()
	if err != nil {
		t.Fatal(err)
	}
	defer udpFile.Close()
	tcp, tcpFile := listenerFile(t, "127.0.0.1:0")
	pass(t, 900, udpFile, tcpFile)
	got, err := listeners(900)
	if err == nil {
		t.Fatal("no error for the datagram socket")
	}
	if len(got) != 1 || got[0].Addr().String() != tcp.Addr().String() {
		t.Fatalf("took %v", got)
	}
	_ = got[0].Close()
	if !closed(900) {
		t.Error("the socket left out is still open")
	}
}

type fakeListener struct {
	net.Listener
	addr *net.TCPAddr
}

func (f fakeListener) Addr() net.Addr { return f.addr }

func on(address string) net.Listener {
	addr, err := net.ResolveTCPAddr("tcp", address)
	if err != nil {
		panic(err)
	}
	return fakeListener{addr: addr}
}

// Each address in the config takes the socket bound to it; one on all
// addresses at its port does when none is bound to it exactly.
func TestTakeMatchesTheConfigsAddresses(t *testing.T) {
	all80, all443, one443, local, one8443 := on("[::]:80"), on("[::]:443"), on("192.0.2.1:443"), on("127.0.0.1:7402"), on("127.0.0.1:8443")
	set := NewSet([]net.Listener{all80, one443, all443, local, one8443})
	for _, c := range []struct {
		address string
		want    net.Listener
		exact   bool
	}{
		{"example.com:80", nil, false}, // systemd binds addresses, not names
		{":8080", nil, false},
		{"192.0.2.1:https", one443, true},
		{"0.0.0.0:443", all443, true},
		{"10.0.0.1:80", all80, false},
		{"127.0.0.1:7402", local, true},
		{"127.0.0.1:7402", nil, false}, // taken already
		{"10.0.0.1:8443", nil, false},  // binds beside it
		{":8443", one8443, false},      // would not
	} {
		got, exact := set.Take(c.address)
		if got != c.want || exact != c.exact {
			t.Errorf("Take(%q) = %v, %v; want %v, %v", c.address, got, exact, c.want, c.exact)
		}
	}
	if set.Active() || len(set.Rest()) != 0 {
		t.Error("the set does not end empty")
	}
	var none *Set
	if got, _ := none.Take(":80"); got != nil || none.Active() {
		t.Error("no set took a socket")
	}
}
