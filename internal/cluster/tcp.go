package cluster

import (
	"context"
	"errors"
	"net"
	"time"
)

// Connections between nodes notice a vanished peer, powered off or cut
// off, in about 30 s instead of the kernel's many minutes: a send to a dead
// replica would otherwise hold up every other tenant's, and a receive from
// a dead owner would keep its copy locked against promotion.
//
// Dialing is bounded, and the keepalive ends a connection that waits on a
// silent peer: a receiver waiting for data, or a sender waiting for the
// answer. A sender whose data the peer does not acknowledge is not ended
// by TCP: a TCP user timeout would also end one to a live peer that merely
// stops reading for a while, as a receive waiting for a dataset's lock
// does. Such a sender instead pings the peer meanwhile (watchPeer).
const dialTimeout = 10 * time.Second

var keepAlive = net.KeepAliveConfig{Enable: true, Idle: 15 * time.Second, Interval: 5 * time.Second, Count: 3}

func netDialer() *net.Dialer {
	return &net.Dialer{Timeout: dialTimeout, KeepAliveConfig: keepAlive}
}

func listenConfig() *net.ListenConfig {
	return &net.ListenConfig{KeepAliveConfig: keepAlive}
}

// A stream to a node is cancelled once the node has answered no ping, sent
// every peerPingEvery, for peerSilentFor.
const (
	peerPingEvery = 5 * time.Second
	peerSilentFor = 30 * time.Second
)

var errPeerSilent = errors.New("the node stopped answering")

// watchPeer returns ctx, cancelled with errPeerSilent once ping has failed
// for silentFor; stop ends the watch.
func watchPeer(ctx context.Context, ping func(context.Context) error, every, silentFor time.Duration) (context.Context, func()) {
	ctx, cancel := context.WithCancelCause(ctx)
	done := make(chan struct{})
	go func() {
		answered := time.Now()
		ticker := time.NewTicker(every)
		defer ticker.Stop()
		for {
			select {
			case <-done:
				return
			case <-ctx.Done():
				return
			case <-ticker.C:
			}
			probe, cancelProbe := context.WithTimeout(ctx, every)
			err := ping(probe)
			cancelProbe()
			if err == nil {
				answered = time.Now()
			} else if time.Since(answered) >= silentFor {
				cancel(errPeerSilent)
				return
			}
		}
	}()
	return ctx, func() {
		close(done)
		cancel(nil)
	}
}
