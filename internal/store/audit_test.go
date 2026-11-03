package store

import (
	"testing"
	"time"
)

// clustered is a store of a named cluster of three nodes, this node
// (node-1) among them, with tenant acme of app web.
func clustered(t *testing.T) *SQLite {
	t.Helper()
	st, _ := withApp(t, options())
	if err := st.NameCluster("test-cluster", "test"); err != nil {
		t.Fatal(err)
	}
	for _, n := range []Node{{Name: "node-1", Address: "10.0.0.1:7400"}, {Name: "node-2", Address: "10.0.0.2:7400"}, {Name: "node-3", Address: "10.0.0.3:7400"}} {
		if err := st.PutNode(n, "test"); err != nil {
			t.Fatal(err)
		}
	}
	if _, err := st.Create(NewTenant{Name: "acme", App: "web"}, "test"); err != nil {
		t.Fatal(err)
	}
	return st
}

// A send's report, late, proves nothing of a copy a move now changes, or of
// a tenant another node runs now; a copy marked stale is not whole.
func TestALateReportMakesNoCopyWhole(t *testing.T) {
	st := clustered(t)
	if _, err := st.PlaceReplicas("test"); err != nil {
		t.Fatal(err)
	}
	r, _ := st.Get("acme")
	if _, err := st.StartMove("acme", r.Replica, "test"); err != nil {
		t.Fatal(err)
	}
	if err := st.ReplicaReady("acme", r.Replica, r.Node); err != nil {
		t.Fatal(err)
	}
	if moving, _ := st.Get("acme"); moving.ReplicaReady {
		t.Fatal("a report made the copy a move changes whole")
	}
	if _, err := st.CancelMove("acme", "test"); err != nil {
		t.Fatal(err)
	}
	// From a node that does not run it, or none.
	for _, owner := range []string{"node-3", ""} {
		if err := st.ReplicaReady("acme", r.Replica, owner); err != nil {
			t.Fatal(err)
		}
		if got, _ := st.Get("acme"); got.ReplicaReady {
			t.Fatalf("a report from node %q made the copy whole", owner)
		}
	}
	// From its node, it does.
	if err := st.ReplicaReady("acme", r.Replica, r.Node); err != nil {
		t.Fatal(err)
	}
	if got, _ := st.Get("acme"); !got.ReplicaReady {
		t.Fatal("a report from the tenant's node did not make the copy whole")
	}
	// Stale, it is not whole.
	if err := st.ReplicaStale("acme", "its data was replaced", "test"); err != nil {
		t.Fatal(err)
	}
	if got, _ := st.Get("acme"); got.ReplicaReady {
		t.Fatal("a stale copy stayed whole")
	}
}

// A copy that moves to another node is not switched to by a send's report
// from a node that does not run the tenant, nor while the tenant moves.
func TestALateReportSwitchesNoCopy(t *testing.T) {
	st := clustered(t)
	if _, err := st.PlaceReplicas("test"); err != nil {
		t.Fatal(err)
	}
	r, _ := st.Get("acme")
	next := "node-3"
	if r.Replica == next {
		next = "node-2"
	}
	if _, err := st.write(false, func(tx *recorder, _ *snapshot, _ time.Time) error {
		return tx.Exec(`UPDATE tenants SET replica_ready = 1, replica_next = ? WHERE name = 'acme'`, next)
	}); err != nil {
		t.Fatal(err)
	}
	if switched, err := st.SwitchReplica("acme", next, next); err != nil || switched {
		t.Fatalf("switched by another node: %v, %v", switched, err)
	}
	if _, err := st.StartMove("acme", r.Replica, "test"); err != nil {
		t.Fatal(err)
	}
	if switched, err := st.SwitchReplica("acme", next, r.Node); err != nil || switched {
		t.Fatalf("switched while moving: %v, %v", switched, err)
	}
	if got, _ := st.Get("acme"); got.Replica != r.Replica {
		t.Fatalf("the copy is kept on %s", got.Replica)
	}
}

func TestAMoveToANodeThatFailedIsNotFinished(t *testing.T) {
	st := clustered(t)
	if _, err := st.StartMove("acme", "node-2", "test"); err != nil {
		t.Fatal(err)
	}
	if err := st.HoldMove("acme"); err != nil {
		t.Fatal(err)
	}
	if _, err := st.MarkFailed("node-2", "test"); err != nil {
		t.Fatal(err)
	}
	if r, err := st.FinishMove("acme", "test"); err == nil {
		t.Fatalf("handed to a failed node: node %s, held %v", r.Node, r.MoveHeld)
	}
	if r, _ := st.Get("acme"); r.Node != "node-1" {
		t.Fatalf("acme runs on %s", r.Node)
	}
}

func TestNoTenantIsCreatedOnANodeOutOfTheCluster(t *testing.T) {
	st := clustered(t)
	if err := st.Archive("acme", "test"); err != nil {
		t.Fatal(err)
	}
	if err := st.Forget("acme", "test"); err != nil {
		t.Fatal(err)
	}
	if err := st.RemoveNode("node-1", "test"); err != nil {
		t.Fatal(err)
	}
	if r, err := st.Create(NewTenant{Name: "new", App: "web"}, "test"); err == nil {
		t.Fatalf("created on node %s, out of the cluster", r.Node)
	}
	// With its last member removed, the cluster is still one.
	for _, node := range []string{"node-2", "node-3"} {
		if err := st.RemoveNode(node, "test"); err != nil {
			t.Fatal(err)
		}
	}
	if r, err := st.Create(NewTenant{Name: "new", App: "web"}, "test"); err == nil {
		t.Fatalf("created on node %s, of a cluster with no members", r.Node)
	}
}

// An observation taken again, after another's of the same image won,
// keeps the image before as the previous one.
func TestAnObservationRetriedKeepsThePreviousImage(t *testing.T) {
	st := clustered(t)
	if err := st.Observe("acme", image); err != nil {
		t.Fatal(err)
	}
	original := st.commit
	first := true
	st.commit = func(b Batch) (*snapshot, error) {
		if first {
			first = false
			if _, err := original(b); err != nil {
				t.Fatal(err)
			}
		}
		return original(b)
	}
	if err := st.Observe("acme", otherImage); err != nil {
		t.Fatal(err)
	}
	if r, _ := st.Get("acme"); r.PreviousImage != image || r.DataImage != otherImage {
		t.Fatalf("previous %s, data %s", r.PreviousImage, r.DataImage)
	}
}
