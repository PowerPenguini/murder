package records

import (
	"context"
	"net/netip"
	"sync/atomic"
	"testing"
	"time"

	"github.com/libdns/libdns"
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

// fakeNamecheap keeps one zone's records.
type fakeNamecheap struct{ records []libdns.Record }

func (f *fakeNamecheap) GetRecords(context.Context, string) ([]libdns.Record, error) {
	return f.records, nil
}

func (f *fakeNamecheap) SetRecords(_ context.Context, _ string, records []libdns.Record) ([]libdns.Record, error) {
	f.records = records
	return records, nil
}

// Publish reads the records and then writes them: the read that ended
// does not refuse the write, nor one publish the next.
func TestNamecheapPublishesOneAfterAnother(t *testing.T) {
	fake := &fakeNamecheap{}
	n := &Namecheap{Name: "*", Zone: "example.com", api: fake}
	for i := range 200 {
		address := netip.AddrFrom4([4]byte{203, 0, 113, byte(i%250 + 1)})
		if err := n.Publish(context.Background(), []netip.Addr{address}); err != nil {
			t.Fatalf("publish %d: %v", i, err)
		}
		if len(fake.records) != 1 || fake.records[0].RR().Data != address.String() {
			t.Fatalf("publish %d wrote %v", i, fake.records)
		}
	}
}
