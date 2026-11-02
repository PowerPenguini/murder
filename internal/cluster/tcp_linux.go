package cluster

import (
	"errors"
	"syscall"

	"golang.org/x/sys/unix"
)

func setUserTimeout(_, _ string, raw syscall.RawConn) error {
	var set error
	err := raw.Control(func(fd uintptr) {
		set = unix.SetsockoptInt(int(fd), unix.IPPROTO_TCP, unix.TCP_USER_TIMEOUT, int(userTimeout.Milliseconds()))
	})
	return errors.Join(err, set)
}
