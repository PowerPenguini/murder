package store

import (
	"errors"
	"slices"
	"testing"
	"time"
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

// A failover gives a dead node's tenants to their replicas' nodes in one
// batch and marks the node failed: it keeps no copies, nothing moves to it,
// until it answers again.
func TestAFailoverGivesTenantsToTheirReplicas(t *testing.T) {
	st, _ := withApp(t, options())
	for _, node := range []Node{{Name: "node-1", Address: "10.0.0.1:7400"}, {Name: "nbg-1", Address: "10.0.0.2:7400"}, {Name: "hel-1", Address: "10.0.0.3:7400"}} {
		if err := st.PutNode(node, "alice"); err != nil {
			t.Fatal(err)
		}
	}
	for name, node := range map[string]string{"a": "nbg-1", "b": "nbg-1", "c": "node-1", "lone": "nbg-1"} {
		if _, err := st.Create(NewTenant{Name: name, App: "web", Node: node}, "alice"); err != nil {
			t.Fatal(err)
		}
	}
	if _, err := st.PlaceReplicas("murder"); err != nil {
		t.Fatal(err)
	}
	// The copies of a and b hold the whole data; lone's never got there.
	for _, name := range []string{"a", "b", "c"} {
		record, _ := st.Get(name)
		if err := st.ReplicaReady(name, record.Replica); err != nil {
			t.Fatal(err)
		}
	}
	before := map[string]Record{}
	for _, record := range st.List() {
		before[record.Spec.Name] = record
	}
	if _, err := st.FailOver("node-1", "murder"); err == nil {
		t.Fatal("a node failed itself over")
	}
	outcome, err := st.FailOver("nbg-1", "murder")
	if err != nil {
		t.Fatal(err)
	}
	if !slices.Equal(outcome.Promoted, []string{"a", "b"}) || !slices.Equal(outcome.Stranded, []string{"lone"}) {
		t.Fatalf("outcome %+v", outcome)
	}
	for _, name := range []string{"a", "b"} {
		record, _ := st.Get(name)
		if record.Node != before[name].Replica || record.Replica != "" || record.Generation != before[name].Generation+1 {
			t.Fatalf("%s after the failover: %+v (replica was %s)", name, record, before[name].Replica)
		}
	}
	if c, _ := st.Get("c"); c.Replica == "nbg-1" {
		t.Fatal("a copy stays on the failed node")
	}
	if !st.state.Load().nodes["nbg-1"].Failed() {
		t.Fatal("the node is not marked failed")
	}
	// Again: nothing more to do.
	version := st.state.Load().version
	if again, err := st.FailOver("nbg-1", "murder"); err != nil || len(again.Promoted) != 0 || st.state.Load().version != version {
		t.Fatalf("a second failover: %+v, %v", again, err)
	}
	// No copies and no moves go to the failed node, and its stranded
	// tenant gets no copy that could never be whole: a second failover
	// still finds nothing to run it from.
	if _, err := st.PlaceReplicas("murder"); err != nil {
		t.Fatal(err)
	}
	for _, record := range st.List() {
		if record.Replica == "nbg-1" {
			t.Fatalf("tenant %s keeps its copy on the failed node", record.Spec.Name)
		}
	}
	if lone, _ := st.Get("lone"); lone.Replica != "" && lone.ReplicaReady {
		t.Fatalf("the stranded tenant: %+v", lone)
	}
	if again, err := st.FailOver("nbg-1", "murder"); err != nil || len(again.Promoted) != 0 {
		t.Fatalf("a failover after placement: %+v, %v", again, err)
	}
	if _, err := st.StartMove("c", "nbg-1", "alice"); !errors.Is(err, ErrConflict) {
		t.Fatalf("a move to the failed node: %v", err)
	}
	if err := st.NodeBack("nbg-1", "murder"); err != nil || st.state.Load().nodes["nbg-1"].Failed() {
		t.Fatalf("back: %v", err)
	}
	events, _ := st.Events(EventQuery{Tenant: "a"})
	if !slices.ContainsFunc(events, func(event Event) bool {
		return event.Action == "tenant.failed-over" && event.Detail == "nbg-1 → "+before["a"].Replica
	}) {
		t.Fatalf("events of a: %+v", events)
	}
}

// A node marked failed keeps no copies but keeps its tenants; one away is
// marked until a time, and the mark is cleared.
func TestNodesAreMarkedFailedAndAway(t *testing.T) {
	st, _ := withApp(t, options())
	for _, node := range []Node{{Name: "node-1", Address: "10.0.0.1:7400"}, {Name: "nbg-1", Address: "10.0.0.2:7400"}} {
		if err := st.PutNode(node, "alice"); err != nil {
			t.Fatal(err)
		}
	}
	if _, err := st.Create(NewTenant{Name: "a", App: "web"}, "alice"); err != nil {
		t.Fatal(err)
	}
	if _, err := st.Create(NewTenant{Name: "b", App: "web", Node: "nbg-1"}, "alice"); err != nil {
		t.Fatal(err)
	}
	if _, err := st.PlaceReplicas("murder"); err != nil {
		t.Fatal(err)
	}
	if marked, err := st.MarkFailed("nbg-1", "murder"); err != nil || !marked {
		t.Fatalf("marked %v, %v", marked, err)
	}
	if a, _ := st.Get("a"); a.Replica != "" {
		t.Fatalf("a keeps its copy on a failed node: %+v", a)
	}
	if b, _ := st.Get("b"); b.Node != "nbg-1" {
		t.Fatal("a tenant left the node marked failed")
	}
	if marked, err := st.MarkFailed("nbg-1", "murder"); err != nil || marked {
		t.Fatalf("marked again: %v, %v", marked, err)
	}
	until := time.Date(2026, 11, 2, 12, 0, 0, 0, time.UTC)
	if err := st.Away("nbg-1", until); err != nil {
		t.Fatal(err)
	}
	for _, node := range st.Nodes() {
		if node.Name == "nbg-1" && !node.AwayUntil.Equal(until) {
			t.Fatalf("away until %v", node.AwayUntil)
		}
	}
	if err := st.Away("nbg-1", time.Time{}); err != nil {
		t.Fatal(err)
	}
	for _, node := range st.Nodes() {
		if !node.AwayUntil.IsZero() {
			t.Fatalf("still away: %+v", node)
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

// ready marks every tenant's copy whole, as their nodes report once sent.
func ready(t *testing.T, st *SQLite) {
	t.Helper()
	for _, record := range st.List() {
		if record.Replica != "" && !record.ReplicaReady {
			if err := st.ReplicaReady(record.Spec.Name, record.Replica); err != nil {
				t.Fatal(err)
			}
		}
	}
}

func copiesPerNode(st *SQLite) map[string]int {
	counts := map[string]int{}
	for _, record := range st.List() {
		counts[record.Replica]++
	}
	return counts
}

// A node back from a failure gets its share of copies again: one copy at
// a time moves to it, the old copy staying the replica until the new one
// holds the whole data.
func TestCopiesMoveToANodeThatCameBack(t *testing.T) {
	st, _ := withApp(t, options())
	for _, node := range []Node{{Name: "node-1", Address: "10.0.0.1:7400"}, {Name: "n2", Address: "10.0.0.2:7400"}, {Name: "n3", Address: "10.0.0.3:7400"}, {Name: "n4", Address: "10.0.0.4:7400"}} {
		if err := st.PutNode(node, "alice"); err != nil {
			t.Fatal(err)
		}
	}
	// n4 is failed while the tenants get their copies.
	if _, err := st.MarkFailed("n4", "murder"); err != nil {
		t.Fatal(err)
	}
	for _, name := range []string{"a", "b", "c", "d", "e", "f"} {
		if _, err := st.Create(NewTenant{Name: name, App: "web"}, "alice"); err != nil {
			t.Fatal(err)
		}
	}
	if _, err := st.PlaceReplicas("murder"); err != nil {
		t.Fatal(err)
	}
	if counts := copiesPerNode(st); counts["n2"] != 3 || counts["n3"] != 3 {
		t.Fatalf("copies %v", counts)
	}
	if err := st.NodeBack("n4", "murder"); err != nil {
		t.Fatal(err)
	}
	// Nothing moves while a copy is not whole.
	if placed, _ := st.PlaceReplicas("murder"); placed != 0 {
		t.Fatalf("moved %d copies before every copy was whole", placed)
	}
	ready(t, st)
	moves := 0
	for range 10 {
		placed, err := st.PlaceReplicas("murder")
		if err != nil {
			t.Fatal(err)
		}
		if placed == 0 {
			break
		}
		// One at a time; the old copy stays the replica, whole.
		var next []Record
		for _, record := range st.List() {
			if record.ReplicaNext != "" {
				next = append(next, record)
			}
		}
		if len(next) != 1 || next[0].ReplicaNext != "n4" || !next[0].ReplicaReady || next[0].Replica == "n4" {
			t.Fatalf("copies moving: %+v", next)
		}
		if again, _ := st.PlaceReplicas("murder"); again != 0 {
			t.Fatal("a second copy moves at once")
		}
		// A stale switch changes nothing; the real one does.
		if err := st.SwitchReplica(next[0].Spec.Name, "n3x"); err != nil {
			t.Fatal(err)
		}
		if err := st.SwitchReplica(next[0].Spec.Name, "n4"); err != nil {
			t.Fatal(err)
		}
		switched, _ := st.Get(next[0].Spec.Name)
		if switched.Replica != "n4" || !switched.ReplicaReady || switched.ReplicaNext != "" {
			t.Fatalf("after the switch: %+v", switched)
		}
		moves++
	}
	if counts := copiesPerNode(st); moves != 2 || counts["n2"] != 2 || counts["n3"] != 2 || counts["n4"] != 2 {
		t.Fatalf("after %d moves: %v", moves, counts)
	}
}

// A copy stops moving when its target fails or leaves, when the tenant
// moves or fails over, and a failover promotes the old, whole copy.
func TestAMovingCopyStops(t *testing.T) {
	st, _ := withApp(t, options())
	for _, node := range []Node{{Name: "node-1", Address: "10.0.0.1:7400"}, {Name: "n2", Address: "10.0.0.2:7400"}, {Name: "n3", Address: "10.0.0.3:7400"}} {
		if err := st.PutNode(node, "alice"); err != nil {
			t.Fatal(err)
		}
	}
	if _, err := st.MarkFailed("n3", "murder"); err != nil {
		t.Fatal(err)
	}
	for _, name := range []string{"a", "b"} {
		if _, err := st.Create(NewTenant{Name: name, App: "web"}, "alice"); err != nil {
			t.Fatal(err)
		}
	}
	// Tenant c runs on n2, its copy on node-1.
	if _, err := st.Create(NewTenant{Name: "c", App: "web", Node: "n2"}, "alice"); err != nil {
		t.Fatal(err)
	}
	if _, err := st.PlaceReplicas("murder"); err != nil {
		t.Fatal(err)
	}
	ready(t, st)
	if err := st.NodeBack("n3", "murder"); err != nil {
		t.Fatal(err)
	}
	moving := func() string {
		t.Helper()
		if _, err := st.PlaceReplicas("murder"); err != nil {
			t.Fatal(err)
		}
		for _, record := range st.List() {
			if record.ReplicaNext != "" {
				return record.Spec.Name
			}
		}
		t.Fatalf("no copy moves: %v", copiesPerNode(st))
		return ""
	}
	// Its target fails: the move of the copy stops.
	name := moving()
	if _, err := st.MarkFailed("n3", "murder"); err != nil {
		t.Fatal(err)
	}
	if record, _ := st.Get(name); record.ReplicaNext != "" || record.Replica != "n2" || !record.ReplicaReady {
		t.Fatalf("after its target failed: %+v", record)
	}
	if err := st.NodeBack("n3", "murder"); err != nil {
		t.Fatal(err)
	}
	// The tenant moves: so does nothing else.
	name = moving()
	if _, err := st.StartMove(name, "n3", "alice"); err != nil {
		t.Fatal(err)
	}
	if record, _ := st.Get(name); record.ReplicaNext != "" {
		t.Fatalf("a moving tenant's copy still moves: %+v", record)
	}
	if _, err := st.CancelMove(name, "alice"); err != nil {
		t.Fatal(err)
	}
	// The target leaves the cluster.
	name = moving()
	if err := st.RemoveNode("n3", "alice"); err != nil {
		t.Fatal(err)
	}
	if record, _ := st.Get(name); record.ReplicaNext != "" {
		t.Fatalf("a copy moves to a node that left: %+v", record)
	}
}

// A tenant whose node fails while its copy moves runs from its replica,
// whole, not from the copy half sent.
func TestAFailoverPromotesTheWholeCopyNotTheMovingOne(t *testing.T) {
	st, _ := withApp(t, options())
	for _, node := range []Node{{Name: "node-1", Address: "10.0.0.1:7400"}, {Name: "n2", Address: "10.0.0.2:7400"}} {
		if err := st.PutNode(node, "alice"); err != nil {
			t.Fatal(err)
		}
	}
	for _, name := range []string{"x", "y", "z"} {
		if _, err := st.Create(NewTenant{Name: name, App: "web", Node: "n2"}, "alice"); err != nil {
			t.Fatal(err)
		}
	}
	if _, err := st.PlaceReplicas("murder"); err != nil {
		t.Fatal(err)
	}
	ready(t, st)
	if err := st.PutNode(Node{Name: "n3", Address: "10.0.0.3:7400"}, "alice"); err != nil {
		t.Fatal(err)
	}
	if placed, err := st.PlaceReplicas("murder"); err != nil || placed != 1 {
		t.Fatalf("moved %d copies, %v", placed, err)
	}
	if record, _ := st.Get("x"); record.ReplicaNext != "n3" || record.Replica != "node-1" {
		t.Fatalf("x: %+v", record)
	}
	outcome, err := st.FailOver("n2", "murder")
	if err != nil || len(outcome.Promoted) != 3 {
		t.Fatalf("failover: %+v, %v", outcome, err)
	}
	if record, _ := st.Get("x"); record.Node != "node-1" || record.ReplicaNext != "" || record.Replica != "" {
		t.Fatalf("x after the failover: %+v", record)
	}
}
