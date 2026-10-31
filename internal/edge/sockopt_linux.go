package edge

import (
	"syscall"
	"time"

	"golang.org/x/sys/unix"
)

// userTimeout has the kernel drop a connection once what was sent on it
// stays unacknowledged for timeout.
func userTimeout(timeout time.Duration) func(network, address string, conn syscall.RawConn) error {
	return func(_, _ string, conn syscall.RawConn) error {
		var err error
		if control := conn.Control(func(fd uintptr) {
			err = unix.SetsockoptInt(int(fd), unix.IPPROTO_TCP, unix.TCP_USER_TIMEOUT, int(timeout.Milliseconds()))
		}); control != nil {
			return control
		}
		return err
	}
}
