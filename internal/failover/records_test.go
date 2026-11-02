package failover

import (
	"context"
	"errors"
	"net/netip"
	"slices"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/PowerPenguini/murder/internal/metrics"
)

// fakeRecords records what is published.
type fakeRecords struct {
	mu     sync.Mutex
	writes [][]string
	err    error
	// block, if set, holds a write until its context ends.
	block bool
}

func (r *fakeRecords) Publish(ctx context.Context, addresses []netip.Addr) error {
	r.mu.Lock()
	block := r.block
	r.mu.Unlock()
	if block {
		<-ctx.Done()
		return ctx.Err()
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	var out []string
	for _, address := range addresses {
		out = append(out, address.String())
	}
	r.writes = append(r.writes, out)
	return r.err
}

func (r *fakeRecords) last() []string {
	r.mu.Lock()
	defer r.mu.Unlock()
	if len(r.writes) == 0 {
		return nil
	}
	return r.writes[len(r.writes)-1]
}

func (r *fakeRecords) count() int {
	r.mu.Lock()
	defer r.mu.Unlock()
	return len(r.writes)
}

// withRecords gives the fixture's nodes public addresses: n1 has
// 192.0.2.1 and 2001:db8::1, n2 192.0.2.2, n3 192.0.2.3.
func withRecords(t *testing.T, f *fixture) *fakeRecords {
	t.Helper()
	for name, values := range map[string][]string{"n1": {"192.0.2.1", "2001:db8::1"}, "n2": {"192.0.2.2"}, "n3": {"192.0.2.3"}} {
		var addresses []netip.Addr
		for _, value := range values {
			addresses = append(addresses, netip.MustParseAddr(value))
		}
		if err := f.store.SetPublic(name, addresses); err != nil {
			t.Fatal(err)
		}
	}
	fake := &fakeRecords{}
	f.monitor.Records = fake
	return fake
}

// round runs a check and waits for the write it started, if any.
func (f *fixture) round(t *testing.T) {
	t.Helper()
	f.check(t)
	deadline := time.Now().Add(5 * time.Second)
	for {
		f.monitor.mu.Lock()
		writing := f.monitor.writing
		f.monitor.mu.Unlock()
		if !writing {
			return
		}
		if time.Now().After(deadline) {
			t.Fatal("the write of the records does not end")
		}
		time.Sleep(time.Millisecond)
	}
}

var all3 = []string{"192.0.2.1", "192.0.2.2", "192.0.2.3", "2001:db8::1"}

// A leader names every node once it has led for 30 s; a node silent for
// 30 s leaves the records, and comes back once it has answered for 30 s.
func TestTheRecordsNameTheNodesThatServe(t *testing.T) {
	f := newFixture(t)
	fake := withRecords(t, f)
	f.round(t)
	if fake.count() != 0 {
		t.Fatal("written before leading for 30 s")
	}
	f.advance(30 * time.Second)
	f.round(t)
	if !slices.Equal(fake.last(), all3) {
		t.Fatalf("records %v", fake.last())
	}
	// Unchanged: not written again, until the refresh.
	f.advance(2 * time.Second)
	f.round(t)
	if fake.count() != 1 {
		t.Fatalf("written %d times", fake.count())
	}
	// n3, which runs nothing, stops answering.
	f.down["n3"] = true
	for range 14 {
		f.advance(2 * time.Second)
		f.round(t)
	}
	if fake.count() != 1 {
		t.Fatal("a node left the records before 30 s")
	}
	f.advance(2 * time.Second)
	f.round(t)
	if want := []string{"192.0.2.1", "192.0.2.2", "2001:db8::1"}; !slices.Equal(fake.last(), want) {
		t.Fatalf("records %v, want %v", fake.last(), want)
	}
	// Back: named again after 30 s of answers, not at the first.
	f.down["n3"] = false
	f.advance(2 * time.Second)
	f.round(t)
	if fake.count() != 2 {
		t.Fatal("a node back at once named again")
	}
	f.advance(30 * time.Second)
	f.round(t)
	if !slices.Equal(fake.last(), all3) {
		t.Fatalf("records %v", fake.last())
	}
	// The refresh writes the same again.
	f.advance(recordsRefresh)
	writes := fake.count()
	f.round(t)
	if fake.count() != writes+1 {
		t.Fatal("no refresh")
	}
}

// Nothing leaves the records while changes are frozen, nor while the
// leader hears too few nodes; a failed write is reported and retried.
func TestTheRecordsWaitWhenUnsure(t *testing.T) {
	f := newFixture(t)
	fake := withRecords(t, f)
	f.round(t)
	f.advance(30 * time.Second)
	f.round(t)
	if err := f.store.Freeze("upgrade", "test"); err != nil {
		t.Fatal(err)
	}
	f.down["n3"] = true
	f.advance(time.Minute)
	f.round(t)
	if fake.count() != 1 {
		t.Fatalf("written during a freeze: %v", fake.last())
	}
	if err := f.store.Thaw("test"); err != nil {
		t.Fatal(err)
	}
	// n2 and n3 both silent: this leader may be the one cut off.
	f.down["n2"] = true
	f.advance(time.Minute)
	f.round(t)
	if fake.count() != 1 {
		t.Fatalf("written without a majority: %v", fake.last())
	}
	f.down["n2"] = false
	fake.err = errors.New("dns down")
	f.advance(2 * time.Second)
	f.round(t)
	var page strings.Builder
	f.monitor.WriteMetrics(metrics.NewWriter(&page))
	if !strings.Contains(page.String(), "murder_dns_records_failing 1") {
		t.Fatalf("metrics:\n%s", page.String())
	}
	writes := fake.count()
	f.advance(2 * time.Second)
	f.round(t)
	if fake.count() != writes {
		t.Fatal("tried again at once")
	}
	fake.err = nil
	f.advance(recordsRetry)
	f.round(t)
	page.Reset()
	f.monitor.WriteMetrics(metrics.NewWriter(&page))
	if want := []string{"192.0.2.1", "192.0.2.2", "2001:db8::1"}; !slices.Equal(fake.last(), want) || !strings.Contains(page.String(), "murder_dns_records_failing 0") || !strings.Contains(page.String(), "murder_dns_records_addresses 3") {
		t.Fatalf("records %v, metrics:\n%s", fake.last(), page.String())
	}
}

// Nothing is written while a node that serves has no public addresses:
// the records would leave it out.
func TestTheRecordsWaitForEveryNodesAddresses(t *testing.T) {
	f := newFixture(t)
	fake := withRecords(t, f)
	if err := f.store.SetPublic("n2", nil); err != nil {
		t.Fatal(err)
	}
	f.round(t)
	f.advance(30 * time.Second)
	f.round(t)
	if fake.count() != 0 {
		t.Fatalf("written without n2: %v", fake.last())
	}
	if err := f.store.SetPublic("n2", []netip.Addr{netip.MustParseAddr("192.0.2.2")}); err != nil {
		t.Fatal(err)
	}
	f.advance(2 * time.Second)
	f.round(t)
	if !slices.Equal(fake.last(), all3) {
		t.Fatalf("records %v", fake.last())
	}
}

// A new leader leaves out failed nodes from the start.
func TestANewLeaderLeavesOutFailedNodes(t *testing.T) {
	f := newFixture(t)
	fake := withRecords(t, f)
	if _, err := f.store.MarkFailed("n3", "test"); err != nil {
		t.Fatal(err)
	}
	f.down["n3"] = true
	f.round(t)
	f.advance(30 * time.Second)
	f.round(t)
	if want := []string{"192.0.2.1", "192.0.2.2", "2001:db8::1"}; !slices.Equal(fake.last(), want) {
		t.Fatalf("records %v, want %v", fake.last(), want)
	}
}

// A write still under way when this node stops leading is cancelled, and
// counts for nothing.
func TestAnOldLeadersWriteIsCancelled(t *testing.T) {
	f := newFixture(t)
	fake := withRecords(t, f)
	f.round(t)
	f.advance(30 * time.Second)
	fake.mu.Lock()
	fake.block = true
	fake.mu.Unlock()
	f.check(t)
	f.monitor.mu.Lock()
	writing := f.monitor.writing
	f.monitor.mu.Unlock()
	if !writing {
		t.Fatal("no write began")
	}
	f.mu.Lock()
	f.leader = false
	f.mu.Unlock()
	deadline := time.Now().Add(5 * time.Second)
	for {
		f.monitor.mu.Lock()
		writing, published := f.monitor.writing, f.monitor.records.published
		f.monitor.mu.Unlock()
		if !writing {
			if published != "" {
				t.Fatalf("a cancelled write counted: %q", published)
			}
			return
		}
		if time.Now().After(deadline) {
			t.Fatal("the old leader's write was not cancelled")
		}
		time.Sleep(10 * time.Millisecond)
	}
}
