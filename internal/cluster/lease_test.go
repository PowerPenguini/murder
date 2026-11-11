package cluster

import (
	"errors"
	"strings"
	"testing"

	"github.com/PowerPenguini/murder/internal/store"
)

// The operation lease goes through Raft: taken on one node, it refuses
// another operation on every node, and a freeze too, and is released or
// broken from any; it outlives its leader.
func TestTheOperationLeaseHoldsAcrossTheCluster(t *testing.T) {
	nodes := threeNodes(t)
	if err := nodes[0].cluster.EveryNodeRuns(ctx(t), store.LeaseSchema); err != nil {
		t.Fatalf("every node runs this release: %v", err)
	}
	lease, err := nodes[2].store.TakeLease("drain", "node fsn-1 to nbg-1", "alice")
	if err != nil {
		t.Fatal(err)
	}
	sameState(t, nodes)
	for _, n := range nodes {
		if n.store.Lease() != lease {
			t.Fatalf("%s holds %+v, not %+v", n.name, n.store.Lease(), lease)
		}
		if _, err := n.store.TakeLease("balance", "", "bob"); !errors.Is(err, store.ErrLeased) || !strings.Contains(err.Error(), "drain node fsn-1 to nbg-1, begun by alice on node hel-1") {
			t.Fatalf("a second lease on %s: %v", n.name, err)
		}
	}
	if err := nodes[1].store.Freeze(store.UpgradeFreeze+"abc from node nbg-1, operation 01", "bob"); !errors.Is(err, store.ErrLeased) {
		t.Fatalf("a freeze beside the lease: %v", err)
	}
	// The leader goes: the lease stays, and its holder renews it.
	nodes[0].stop()
	rest := nodes[1:]
	eventually(t, "a new leader", func() bool { return rest[0].cluster.Leader() || rest[1].cluster.Leader() })
	eventually(t, "the holder renews its lease under the new leader", func() bool {
		_, err := nodes[2].store.RenewLease(lease.ID)
		return err == nil
	})
	nodes[0].start()
	sameState(t, nodes)
	if _, err := nodes[0].store.BreakLease(lease.ID, "root"); err != nil {
		t.Fatal(err)
	}
	sameState(t, nodes)
	if _, err := nodes[2].store.RenewLease(lease.ID); !errors.Is(err, store.ErrLeaseLost) {
		t.Fatalf("a renewal of a broken lease: %v", err)
	}
	if _, err := nodes[1].store.TakeLease("balance", "", "bob"); err != nil {
		t.Fatalf("a lease once the other was broken: %v", err)
	}
}
