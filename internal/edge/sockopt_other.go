//go:build !linux

package edge

import (
	"syscall"
	"time"
)

// userTimeout is Linux's; elsewhere a dead peer is noticed by TCP
// keep-alive and the transport's timeouts only.
func userTimeout(time.Duration) func(network, address string, conn syscall.RawConn) error {
	return nil
}
