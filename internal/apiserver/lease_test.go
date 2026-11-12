package apiserver

import (
	"context"
	"errors"
	"net/http"
	"strings"
	"testing"

	"github.com/PowerPenguini/murder/internal/api"
	"github.com/PowerPenguini/murder/internal/cluster"
	"github.com/PowerPenguini/murder/internal/reboot"
	"github.com/PowerPenguini/murder/internal/store"
)

// recordingMoves takes every move it is asked for.
type recordingMoves struct{ moved []string }

func (m *recordingMoves) Move(_ context.Context, tenant, to, _ string) error {
	m.moved = append(m.moved, tenant+" "+to)
	return nil
}

// leaseCluster is a cluster whose nodes run releases of schema, and that
// removes nodes.
type leaseCluster struct {
	Cluster
	schema  int
	removed *[]string
	asked   map[string]reboot.Request
}

func (c leaseCluster) EveryNodeRuns(_ context.Context, schema int) error {
	if c.schema < schema {
		return errors.New("node n2 runs a release of schema version 19, before 20")
	}
	return nil
}

func (c leaseCluster) Status() (cluster.Status, error) {
	return cluster.Status{Leader: "test-node"}, nil
}

func (c leaseCluster) Ping(context.Context, string) error { return nil }

func (c leaseCluster) Remove(_ context.Context, node, _ string) error {
	*c.removed = append(*c.removed, node)
	return nil
}

func (c leaseCluster) RebootOn(_ context.Context, node string, request reboot.Request) error {
	c.asked[node] = request
	return nil
}

func conflict(err error) bool {
	var apiErr *api.Error
	return errors.As(err, &apiErr) && apiErr.Status == http.StatusConflict
}

// While an operation holds the cluster, the agent refuses a move, a
// node's removal and a reboot made beside it, naming the holder, and takes
// them from the operation itself.
func TestTheAgentTakesChangesOfTheClusterOnlyFromTheLeaseHolder(t *testing.T) {
	f := newAPIFixture(t)
	ctx := context.Background()
	for _, node := range []string{f.store.Node(), "n2"} {
		if err := f.store.PutNode(store.Node{Name: node, Address: node + ":7400"}, "test"); err != nil {
			t.Fatal(err)
		}
	}
	var removed []string
	cluster := leaseCluster{schema: store.LeaseSchema, removed: &removed, asked: map[string]reboot.Request{}}
	moves := &recordingMoves{}
	f.server.WithCluster(cluster).WithMoves(moves)
	if lease, err := f.client.Lease(ctx); err != nil || lease.ID != "" || lease.Held {
		t.Fatalf("lease before: %+v, %v", lease, err)
	}
	lease, err := f.client.TakeLease(ctx, "drain", "node n2 to test-node")
	if err != nil {
		t.Fatal(err)
	}
	if _, err := f.client.TakeLease(ctx, "balance", ""); !conflict(err) || !strings.Contains(err.Error(), "drain node n2 to test-node") {
		t.Fatalf("a second lease: %v", err)
	}
	// Beside the lease: refused, the holder named.
	if _, err := f.client.Move(ctx, "acme", "n2"); !conflict(err) || !strings.Contains(err.Error(), "another operation changes the cluster: drain node n2") || !strings.Contains(err.Error(), "murder cluster unlock") {
		t.Fatalf("a move beside the lease: %v", err)
	}
	if err := f.client.RemoveNode(ctx, "n2"); !conflict(err) {
		t.Fatalf("a removal beside the lease: %v", err)
	}
	if err := f.client.Reboot(ctx, "n2", reboot.Request{Away: 1}); !conflict(err) {
		t.Fatalf("a reboot beside the lease: %v", err)
	}
	// A check changes nothing, and is answered.
	if err := f.client.Reboot(ctx, "n2", reboot.Request{Check: true}); err != nil {
		t.Fatalf("a check beside the lease: %v", err)
	}
	if len(moves.moved) != 0 || len(removed) != 0 || cluster.asked["n2"].Away != 0 {
		t.Fatalf("changed beside the lease: moved %v, removed %v, asked %v", moves.moved, removed, cluster.asked)
	}
	// Under it: taken. The move answers once the tenant runs there,
	// which this fake never does, so only the request is checked.
	held := api.WithLease(ctx, lease.ID)
	if err := f.client.RemoveNode(held, "n2"); err != nil {
		t.Fatalf("a removal under the lease: %v", err)
	}
	if err := f.client.Reboot(held, "n2", reboot.Request{Away: 1}); err != nil {
		t.Fatalf("a reboot under the lease: %v", err)
	}
	if _, err := f.client.Move(held, "acme", "n2"); err == nil || conflict(err) {
		t.Fatalf("a move under the lease: %v", err)
	}
	if len(moves.moved) != 1 || len(removed) != 1 {
		t.Fatalf("under the lease: moved %v, removed %v", moves.moved, removed)
	}
	if view, err := f.client.Nodes(ctx); err != nil || view.Lease == nil || view.Lease.ID != lease.ID {
		t.Fatalf("nodes view: %+v, %v", view.Lease, err)
	}
	// Renewed by its holder; released, others go on.
	if _, err := f.client.RenewLease(ctx, lease.ID); err != nil {
		t.Fatal(err)
	}
	if err := f.client.ReleaseLease(ctx, lease.ID); err != nil {
		t.Fatal(err)
	}
	var apiErr *api.Error
	if _, err := f.client.RenewLease(ctx, lease.ID); !errors.As(err, &apiErr) || apiErr.Status != http.StatusGone {
		t.Fatalf("a renewal once released: %v", err)
	}
	if err := f.client.RemoveNode(ctx, "n2"); err != nil {
		t.Fatalf("a removal with no lease held: %v", err)
	}
}

// A person breaks a lease by its ID; a lease is taken only once every
// node runs a release that keeps it.
func TestALeaseIsBrokenByIDAndTakenOnlyInAClusterThatKeepsIt(t *testing.T) {
	f := newAPIFixture(t)
	ctx := context.Background()
	for _, node := range []string{f.store.Node(), "n2"} {
		if err := f.store.PutNode(store.Node{Name: node, Address: node + ":7400"}, "test"); err != nil {
			t.Fatal(err)
		}
	}
	f.server.WithCluster(leaseCluster{schema: store.LeaseSchema - 1})
	if _, err := f.client.TakeLease(ctx, "drain", ""); !conflict(err) || !strings.Contains(err.Error(), "finish upgrading the cluster first") {
		t.Fatalf("a lease in a cluster of older releases: %v", err)
	}
	f.server.WithCluster(leaseCluster{schema: store.LeaseSchema})
	lease, err := f.client.TakeLease(ctx, "reboot", "node n2")
	if err != nil {
		t.Fatal(err)
	}
	if _, err := f.client.BreakLease(ctx, "another"); !conflict(err) {
		t.Fatalf("break of another lease: %v", err)
	}
	broken, err := f.client.BreakLease(ctx, lease.ID)
	if err != nil || broken.ID != lease.ID {
		t.Fatalf("break: %+v, %v", broken, err)
	}
	if now, err := f.client.Lease(ctx); err != nil || now.ID != "" {
		t.Fatalf("after the break: %+v, %v", now, err)
	}
	if _, err := f.client.BreakLease(ctx, lease.ID); !api.IsNotFound(err) {
		t.Fatalf("break of no lease: %v", err)
	}
}
