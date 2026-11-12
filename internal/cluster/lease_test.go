package cluster

import (
	"context"
	"errors"
	"io"
	"log/slog"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/PowerPenguini/murder/internal/move"
	"github.com/PowerPenguini/murder/internal/reconcile"
	"github.com/PowerPenguini/murder/internal/store"
	"github.com/PowerPenguini/murder/internal/volume"
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

// breakingPeer is the target of a move whose operation's lease a person
// breaks, from another node, while the data goes across.
type breakingPeer struct {
	breakIt   func()
	received  []bool
	discarded bool
}

func (p *breakingPeer) Receive(_ context.Context, _, _ string, incremental bool, data io.Reader) error {
	p.received = append(p.received, incremental)
	_, err := io.Copy(io.Discard, data)
	if !incremental {
		p.breakIt()
	}
	return err
}

func (p *breakingPeer) Discard(context.Context, string, string) error { p.discarded = true; return nil }

func (p *breakingPeer) Status(context.Context, string, string) (reconcile.Status, error) {
	return reconcile.Status{Phase: reconcile.PhaseRunning, Healthy: true}, nil
}

func (p *breakingPeer) Snapshots(context.Context, string, string) ([]string, error) { return nil, nil }

type noRelease struct{}

func (noRelease) Release(context.Context, string) error { return nil }

// A move made for an operation whose lease is broken mid-move, on another
// node, is not handed over: its handover is fenced by the lease through
// Raft, and the move is undone as one that fails, the tenant on its node
// on every node of the cluster, moving nowhere.
func TestAMoveWhoseLeaseIsBrokenMidMoveDoesNotCommit(t *testing.T) {
	nodes := threeNodes(t)
	source := nodes[0]
	if _, err := source.store.PutApp(store.App{Name: "web", Spec: appSpec()}, "alice"); err != nil {
		t.Fatal(err)
	}
	created, err := source.store.Create(store.NewTenant{Name: "acme", App: "web"}, "alice")
	if err != nil || created.Node != source.name {
		t.Fatalf("created %+v, %v", created.Node, err)
	}
	volumes := volume.Dir{Root: filepath.Join(t.TempDir(), "tenants")}
	data, err := volumes.Ensure(context.Background(), "acme", os.Getuid(), os.Getgid(), 0)
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(data, "app.db"), []byte("the tenant's data"), 0o600); err != nil {
		t.Fatal(err)
	}
	lease, err := nodes[2].store.TakeLease("drain", "node fsn-1 to nbg-1", "alice")
	if err != nil {
		t.Fatal(err)
	}
	eventually(t, "nbg-1 sees the lease", func() bool { return nodes[1].store.Lease().ID == lease.ID })
	peer := &breakingPeer{breakIt: func() {
		// A write retried after a leader changed may find its own change
		// made already: what counts is that the lease is gone.
		if _, err := nodes[1].store.BreakLease(lease.ID, "root"); err != nil && nodes[1].store.Lease().ID == lease.ID {
			t.Errorf("break the lease: %v", err)
		}
	}}
	mover := &move.Mover{Store: source.store, Reconciler: noRelease{}, Volumes: volumes, Peer: peer,
		Log: slog.New(slog.NewTextHandler(io.Discard, nil)), CatchUp: source.cluster.CatchUp, Healthy: time.Second}
	ctx := store.WithFence(context.Background(), store.Fence{Lease: lease.ID, From: source.name})
	err = mover.Move(ctx, "acme", "nbg-1", "alice")
	if !errors.Is(err, store.ErrLeaseLost) {
		t.Fatalf("the move: %v", err)
	}
	if len(peer.received) != 2 || !peer.discarded {
		t.Fatalf("received %v, discarded %v", peer.received, peer.discarded)
	}
	sameState(t, nodes)
	for _, n := range nodes {
		record, err := n.store.Get("acme")
		if err != nil || record.Node != source.name || record.MoveTo != "" || record.MoveHeld {
			t.Fatalf("on %s, acme is %+v, %v", n.name, record, err)
		}
	}
	// Its data stays where it runs.
	if kept, err := volumes.Exists(context.Background(), "acme"); err != nil || !kept {
		t.Fatalf("the tenant's data on its node: %v, %v", kept, err)
	}
}
