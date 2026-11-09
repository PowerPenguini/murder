// Package activation takes the listening sockets systemd passes a service
// it starts (socket activation, see sd_listen_fds(3)). murder-edge.socket
// holds the edge's ports this way: systemd keeps them open while the edge
// stops and starts again, so a connection that arrives meanwhile waits in
// the kernel's queue for the next edge instead of being refused.
package activation

import (
	"errors"
	"fmt"
	"net"
	"os"
	"strconv"
	"syscall"
)

// firstFD is where systemd puts the first socket it passes.
const firstFD = 3

// Listeners returns the listening TCP sockets systemd passed this process,
// none if it passed none. It unsets the variables that say so, whether
// they were meant for this process or not: a process the edge starts to
// hand over to must not take the sockets for its own (it gets them from
// the edge instead). A socket it cannot take is left out and named in the
// error, with the others still returned.
func Listeners() ([]net.Listener, error) { return listeners(firstFD) }

func listeners(first int) ([]net.Listener, error) {
	pid, count := os.Getenv("LISTEN_PID"), os.Getenv("LISTEN_FDS")
	for _, name := range []string{"LISTEN_PID", "LISTEN_FDS", "LISTEN_FDNAMES"} {
		_ = os.Unsetenv(name)
	}
	if count == "" || pid != strconv.Itoa(os.Getpid()) {
		return nil, nil
	}
	n, err := strconv.Atoi(count)
	if err != nil || n < 0 {
		return nil, fmt.Errorf("LISTEN_FDS=%q is not a number of sockets", count)
	}
	var taken []net.Listener
	var errs []error
	for fd := first; fd < first+n; fd++ {
		listener, err := take(fd)
		if err != nil {
			errs = append(errs, fmt.Errorf("socket %d from systemd: %w", fd, err))
			continue
		}
		taken = append(taken, listener)
	}
	return taken, errors.Join(errs...)
}

// take makes fd a listener. The listener holds a copy of fd, which closes
// with this process's other files when it starts another; fd itself is
// closed either way, so nothing systemd passed leaks into such a process.
func take(fd int) (net.Listener, error) {
	file := os.NewFile(uintptr(fd), "systemd socket "+strconv.Itoa(fd))
	defer file.Close()
	// A socket that is not listening would fail only at its first accept.
	listening, err := syscall.GetsockoptInt(fd, syscall.SOL_SOCKET, syscall.SO_ACCEPTCONN)
	if err != nil {
		return nil, err
	}
	if listening != 1 {
		return nil, errors.New("not a listening socket (murder-edge.socket needs ListenStream=)")
	}
	listener, err := net.FileListener(file)
	if err != nil {
		return nil, err
	}
	if _, ok := listener.Addr().(*net.TCPAddr); !ok {
		_ = listener.Close()
		return nil, fmt.Errorf("a %s socket, not TCP", listener.Addr().Network())
	}
	return listener, nil
}

// Set is the sockets systemd passed, taken one by one for the addresses
// the edge's config names.
type Set struct{ listeners []net.Listener }

// NewSet holds listeners until they are taken.
func NewSet(listeners []net.Listener) *Set { return &Set{listeners: listeners} }

// Active reports whether systemd passed any socket at all: then an address
// it holds none for is a socket unit out of date with the config.
func (s *Set) Active() bool { return s != nil && len(s.listeners) > 0 }

// Take returns and removes the socket for address, as the config names it
// (host:port, an empty host for all addresses): one bound to exactly that.
// Else (exact false) it is one that binding address anew would collide
// with, as a socket unit not yet restarted after the config changed holds:
// one on all addresses at that port, which accepts on address too, or, for
// all addresses, one on a single address at that port, which is better
// served than fail the edge's start. nil if none fits; a host name never
// does, since systemd binds addresses only.
func (s *Set) Take(address string) (listener net.Listener, exact bool) {
	if s == nil {
		return nil, false
	}
	host, service, err := net.SplitHostPort(address)
	if err != nil {
		return nil, false
	}
	port, err := net.LookupPort("tcp", service)
	if err != nil {
		return nil, false
	}
	var ip net.IP
	if host != "" {
		if ip = net.ParseIP(host); ip == nil {
			return nil, false
		}
	}
	all := ip == nil || ip.IsUnspecified()
	found, fit := -1, 0
	for i, candidate := range s.listeners {
		bound := candidate.Addr().(*net.TCPAddr)
		if bound.Port != port {
			continue
		}
		switch {
		case all && bound.IP.IsUnspecified() || !all && ip.Equal(bound.IP):
			found, fit = i, 3
		case bound.IP.IsUnspecified() && fit < 2:
			found, fit = i, 2
		case all && fit < 1:
			found, fit = i, 1
		}
		if fit == 3 {
			break
		}
	}
	exact = fit == 3
	if found < 0 {
		return nil, false
	}
	listener = s.listeners[found]
	s.listeners = append(s.listeners[:found], s.listeners[found+1:]...)
	return listener, exact
}

// Rest returns the sockets not taken, and forgets them.
func (s *Set) Rest() []net.Listener {
	if s == nil {
		return nil
	}
	rest := s.listeners
	s.listeners = nil
	return rest
}
