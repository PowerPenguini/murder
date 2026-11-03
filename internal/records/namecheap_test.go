package records

import (
	"context"
	"sync/atomic"
	"testing"
	"time"
)

// A call the provider cannot stop holds back the next ones: they are
// refused, not started beside it.
func TestNamecheapCallsDoNotPileUp(t *testing.T) {
	n := &Namecheap{}
	var active atomic.Int32
	release := make(chan struct{})
	for range 5 {
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Millisecond)
		err := n.withContext(ctx, func() error { active.Add(1); defer active.Add(-1); <-release; return nil })
		cancel()
		if err == nil {
			t.Fatal("a call that did not end succeeded")
		}
	}
	if got := active.Load(); got != 1 {
		t.Fatalf("%d calls running", got)
	}
	close(release)
	for deadline := time.Now().Add(2 * time.Second); n.busy.Load(); time.Sleep(time.Millisecond) {
		if time.Now().After(deadline) {
			t.Fatal("the call never ended")
		}
	}
	if err := n.withContext(context.Background(), func() error { return nil }); err != nil {
		t.Fatalf("after the earlier call ended: %v", err)
	}
}
