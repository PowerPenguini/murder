//go:build !linux

package cluster

import "syscall"

// Elsewhere only the keepalive notices a vanished peer.
func setUserTimeout(_, _ string, _ syscall.RawConn) error { return nil }
