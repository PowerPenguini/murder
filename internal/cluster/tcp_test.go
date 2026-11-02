package cluster

import (
	"context"
	"errors"
	"sync/atomic"
	"testing"
	"time"
)

// A stream to a node that answers its pings goes on however long it
// takes; one to a node that stopped answering is cancelled.
func TestAStreamEndsOnlyOnceThePeerStopsAnswering(t *testing.T) {
	var up atomic.Bool
	up.Store(true)
	ping := func(context.Context) error {
		if up.Load() {
			return nil
		}
		return errors.New("no answer")
	}
	ctx, stop := watchPeer(context.Background(), ping, 5*time.Millisecond, 50*time.Millisecond)
	defer stop()
	select {
	case <-ctx.Done():
		t.Fatal("cancelled while the peer answers")
	case <-time.After(150 * time.Millisecond):
	}
	up.Store(false)
	select {
	case <-ctx.Done():
		if !errors.Is(context.Cause(ctx), errPeerSilent) {
			t.Fatalf("cancelled because %v", context.Cause(ctx))
		}
	case <-time.After(5 * time.Second):
		t.Fatal("not cancelled once the peer stopped answering")
	}
	// Stopping the watch cancels nothing the caller sees as the peer's.
	ctx, stop = watchPeer(context.Background(), ping, time.Hour, time.Hour)
	stop()
	if errors.Is(context.Cause(ctx), errPeerSilent) {
		t.Fatal("a stopped watch blamed the peer")
	}
}
