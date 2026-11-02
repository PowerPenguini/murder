package cluster

import (
	"net"
	"time"
)

// Connections between nodes end soon after the peer vanishes, powered off
// or cut off, instead of waiting on the kernel's defaults for many minutes:
// a send to a dead replica would hold up every other tenant's, and a
// receive from a dead owner would keep its copy locked against promotion.
const (
	// dialTimeout bounds connecting to a node.
	dialTimeout = 10 * time.Second
	// userTimeout ends a connection whose data stays unacknowledged this
	// long. A live peer that is merely slow to read still acknowledges
	// the window probes, so it is not cut off.
	userTimeout = 30 * time.Second
)

// keepAlive notices an idle connection's peer gone in about 30 s.
var keepAlive = net.KeepAliveConfig{Enable: true, Idle: 15 * time.Second, Interval: 5 * time.Second, Count: 3}

func netDialer() *net.Dialer {
	return &net.Dialer{Timeout: dialTimeout, KeepAliveConfig: keepAlive, Control: setUserTimeout}
}

func listenConfig() *net.ListenConfig {
	// Accepted connections inherit the listening socket's user timeout.
	return &net.ListenConfig{KeepAliveConfig: keepAlive, Control: setUserTimeout}
}
