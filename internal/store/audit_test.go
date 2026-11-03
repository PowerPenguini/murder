package store

import (
	"testing"
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
// a tenant another node runs now, or one changed since the send began.
func TestALateReportMakesNoCopyWhole(t *testing.T) {
	st := clustered(t)
	if _, err := st.PlaceReplicas("test"); err != nil {
		t.Fatal(err)
	}
	r, _ := st.Get("acme")
	sentAt := r.Generation
	if _, err := st.StartMove("acme", r.Replica, "test"); err != nil {
		t.Fatal(err)
	}
	if err := st.ReplicaReady("acme", r.Replica, r.Node, sentAt); err != nil {
		t.Fatal(err)
	}
	if moving, _ := st.Get("acme"); moving.ReplicaReady {
		t.Fatal("a report made the copy a move changes whole")
	}
	if _, err := st.CancelMove("acme", "test"); err != nil {
		t.Fatal(err)
	}
	// From a node that does not run it.
	if err := st.ReplicaReady("acme", r.Replica, "node-3", sentAt); err != nil {
		t.Fatal(err)
	}
	if got, _ := st.Get("acme"); got.ReplicaReady {
		t.Fatal("a report from another node made the copy whole")
	}
	// Data replaced since the send began.
	if err := st.ReplicaStale("acme", "its data was replaced", "test"); err != nil {
		t.Fatal(err)
	}
	if err := st.ReplicaReady("acme", r.Replica, r.Node, sentAt); err != nil {
		t.Fatal(err)
	}
	if got, _ := st.Get("acme"); got.ReplicaReady {
		t.Fatal("a send from before the data was replaced made the copy whole")
	}
	// A send of now does.
	now, _ := st.Get("acme")
	if err := st.ReplicaReady("acme", now.Replica, now.Node, now.Generation); err != nil {
		t.Fatal(err)
	}
	if got, _ := st.Get("acme"); !got.ReplicaReady {
		t.Fatal("a send of now did not make the copy whole")
	}
	// Stale again, it is not whole.
	if err := st.ReplicaStale("acme", "its data was replaced", "test"); err != nil {
		t.Fatal(err)
	}
	if got, _ := st.Get("acme"); got.ReplicaReady {
		t.Fatal("a stale copy stayed whole")
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
