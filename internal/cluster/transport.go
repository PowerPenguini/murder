package cluster

import (
	"sync"

	"github.com/hashicorp/raft"
)

// gatedTransport is Raft's network transport, its heartbeats held off once
// the node closes. Raft handles a heartbeat on the transport's own
// goroutine, outside what its Shutdown waits for: one that passed Raft's
// check for a shutdown just before it may still save the term after
// Shutdown returns, and Raft panics when the log it saves to is closed
// meanwhile.
type gatedTransport struct {
	*raft.NetworkTransport
	mu     sync.RWMutex
	closed bool
}

func (t *gatedTransport) SetHeartbeatHandler(handle func(raft.RPC)) {
	if handle == nil {
		t.NetworkTransport.SetHeartbeatHandler(nil)
		return
	}
	t.NetworkTransport.SetHeartbeatHandler(t.gate(handle))
}

// gate is handle, until heartbeats stop.
func (t *gatedTransport) gate(handle func(raft.RPC)) func(raft.RPC) {
	return func(rpc raft.RPC) {
		t.mu.RLock()
		defer t.mu.RUnlock()
		if t.closed {
			rpc.Respond(nil, raft.ErrRaftShutdown)
			return
		}
		handle(rpc)
	}
}

// stopHeartbeats returns once no heartbeat is being handled, and refuses
// every later one.
func (t *gatedTransport) stopHeartbeats() {
	t.mu.Lock()
	defer t.mu.Unlock()
	t.closed = true
}
