package store

import (
	"testing"
)

// Every tenant gets a replica on another node of the cluster, spread over
// the nodes; one that moves onto its replica's node, or whose replica's
// node leaves, gets another.
func TestReplicasAreSpreadOverTheOtherNodes(t *testing.T) {
	st, _ := withApp(t, options())
	for _, name := range []string{"a", "b", "c", "d"} {
		if _, err := st.Create(NewTenant{Name: name, App: "web"}, "alice"); err != nil {
			t.Fatal(err)
		}
	}
	// Alone, a node has no one to keep copies.
	if placed, err := st.PlaceReplicas("murder"); err != nil || placed != 0 {
		t.Fatalf("placed %d alone, %v", placed, err)
	}
	for _, node := range []Node{{Name: "node-1", Address: "10.0.0.1:7400"}, {Name: "nbg-1", Address: "10.0.0.2:7400"}, {Name: "hel-1", Address: "10.0.0.3:7400"}} {
		if err := st.PutNode(node, "alice"); err != nil {
			t.Fatal(err)
		}
	}
	if placed, err := st.PlaceReplicas("murder"); err != nil || placed != 4 {
		t.Fatalf("placed %d, %v", placed, err)
	}
	counts := map[string]int{}
	for _, record := range st.List() {
		if record.Replica == "" || record.Replica == record.Node {
			t.Fatalf("tenant %s: replica %q, node %s", record.Spec.Name, record.Replica, record.Node)
		}
		counts[record.Replica]++
	}
	if counts["nbg-1"] != 2 || counts["hel-1"] != 2 {
		t.Fatalf("replicas per node: %v", counts)
	}
	version := st.state.Load().version
	if placed, err := st.PlaceReplicas("murder"); err != nil || placed != 0 || st.state.Load().version != version {
		t.Fatalf("placed again: %d, %v", placed, err)
	}
	if events, _ := st.Events(EventQuery{Tenant: "a"}); len(events) == 0 || events[0].Action != "tenant.replica" {
		t.Fatalf("events of a: %+v", events)
	}

	// A move onto the replica's node leaves it without one, until placed.
	record, _ := st.Get("a")
	if _, err := st.StartMove("a", record.Replica, "alice"); err != nil {
		t.Fatal(err)
	}
	// A moving tenant keeps its replica as it is.
	if placed, _ := st.PlaceReplicas("murder"); placed != 0 {
		t.Fatalf("placed %d during a move", placed)
	}
	moved, err := st.FinishMove("a", "alice")
	if err != nil || moved.Node != record.Replica || moved.Replica != "" {
		t.Fatalf("moved onto its replica: %+v, %v", moved, err)
	}
	if placed, _ := st.PlaceReplicas("murder"); placed != 1 {
		t.Fatalf("placed %d after the move", placed)
	}
	if again, _ := st.Get("a"); again.Replica == "" || again.Replica == again.Node {
		t.Fatalf("after the move: %+v", again)
	}

	// A node that leaves takes its copies with it.
	leaving := "hel-1"
	if moved.Node == leaving {
		leaving = "nbg-1"
	}
	if err := st.RemoveNode(leaving, "alice"); err != nil {
		t.Fatal(err)
	}
	for _, record := range st.List() {
		if record.Replica == leaving {
			t.Fatalf("tenant %s keeps its copy on a node that left", record.Spec.Name)
		}
	}
	if _, err := st.PlaceReplicas("murder"); err != nil {
		t.Fatal(err)
	}
	for _, record := range st.List() {
		if _, member := st.state.Load().nodes[record.Replica]; !member || record.Replica == record.Node {
			t.Fatalf("tenant %s on %s: replica %q", record.Spec.Name, record.Node, record.Replica)
		}
	}
}

// A copy is not whole again once a move writes over it, or once the
// tenant moved and its new node has not yet sent it anything.
func TestMovesClearACopysReadiness(t *testing.T) {
	st, _ := withApp(t, options())
	for _, node := range []Node{{Name: "node-1", Address: "10.0.0.1:7400"}, {Name: "nbg-1", Address: "10.0.0.2:7400"}, {Name: "hel-1", Address: "10.0.0.3:7400"}} {
		if err := st.PutNode(node, "alice"); err != nil {
			t.Fatal(err)
		}
	}
	if _, err := st.Create(NewTenant{Name: "a", App: "web"}, "alice"); err != nil {
		t.Fatal(err)
	}
	if _, err := st.PlaceReplicas("murder"); err != nil {
		t.Fatal(err)
	}
	ready := func() Record {
		t.Helper()
		record, _ := st.Get("a")
		if err := st.ReplicaReady("a", record.Replica); err != nil {
			t.Fatal(err)
		}
		record, _ = st.Get("a")
		if !record.ReplicaReady {
			t.Fatal("not ready")
		}
		return record
	}
	record := ready()
	// Onto the replica's node: not whole while the move writes over it.
	if moving, err := st.StartMove("a", record.Replica, "alice"); err != nil || moving.ReplicaReady {
		t.Fatalf("moving onto the replica: %+v, %v", moving, err)
	}
	if _, err := st.CancelMove("a", "alice"); err != nil {
		t.Fatal(err)
	}
	record = ready()
	// Elsewhere: whole until the move ends, then not until the new node
	// has sent it.
	other := "nbg-1"
	if record.Replica == other {
		other = "hel-1"
	}
	if moving, err := st.StartMove("a", other, "alice"); err != nil || !moving.ReplicaReady {
		t.Fatalf("moving elsewhere: %+v, %v", moving, err)
	}
	if moved, err := st.FinishMove("a", "alice"); err != nil || moved.ReplicaReady || moved.Replica != record.Replica {
		t.Fatalf("moved elsewhere: %+v, %v", moved, err)
	}
}
