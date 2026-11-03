package store

import (
	"errors"
	"slices"
	"testing"
	"time"

	"github.com/PowerPenguini/murder/internal/spec"
)

func placed(name, node string) Record {
	return Record{Spec: spec.Tenant{Name: name}, Node: node}
}

func TestANewTenantGoesToTheLeastLoadedNode(t *testing.T) {
	nodes := []Node{{Name: "a"}, {Name: "b"}, {Name: "c"}, {Name: "dead", FailedAt: time.Now()}}
	records := []Record{placed("t1", "a"), placed("t2", "a"), placed("t3", "b"), placed("t4", "c")}
	if got := LeastLoaded(nodes, records, "a"); got != "b" {
		t.Fatalf("least loaded %s", got)
	}
	// Among equals, this node; a failed node never.
	if got := LeastLoaded(nodes, records, "c"); got != "c" {
		t.Fatalf("among equals %s", got)
	}
	// Archived and suspended tenants weigh nothing; a moving one counts
	// where it goes.
	suspended := placed("t5", "b")
	suspended.Spec.Suspended = true
	archived := placed("t6", "c")
	archived.ArchivedAt = time.Now()
	moving := placed("t7", "a")
	moving.MoveTo = "b"
	records = append(records, suspended, archived, moving)
	if got := LeastLoaded(nodes, records, "a"); got != "c" {
		t.Fatalf("with suspended, archived and moving tenants %s", got)
	}
	if got := LeastLoaded(nil, records, "solo"); got != "solo" {
		t.Fatalf("outside a cluster %s", got)
	}
}

func TestBalancingMovesTenantsOntoTheirCopiesFirst(t *testing.T) {
	nodes := []Node{{Name: "a"}, {Name: "b"}, {Name: "c"}}
	var records []Record
	for _, name := range []string{"t1", "t2", "t3", "t4", "t5"} {
		records = append(records, placed(name, "a"))
	}
	records[3].Replica, records[3].ReplicaReady = "c", true // t4's copy is on c
	records = append(records, placed("t6", "b"))
	moves := BalanceMoves(nodes, records)
	// 5/1/0 to 2/2/2.
	want := []Move{{Tenant: "t4", From: "a", To: "c"}, {Tenant: "t1", From: "a", To: "b"}, {Tenant: "t2", From: "a", To: "c"}}
	if !slices.Equal(moves, want) {
		t.Fatalf("moves %+v", moves)
	}
	// Even already, or while a tenant moves: nothing.
	if moves := BalanceMoves(nodes, []Record{placed("t1", "a"), placed("t2", "b")}); moves != nil {
		t.Fatalf("balanced: %+v", moves)
	}
	records[5].MoveTo = "c"
	if moves := BalanceMoves(nodes, records); moves != nil {
		t.Fatalf("while one moves: %+v", moves)
	}
}

func TestNoTenantIsCreatedOnAFailedNode(t *testing.T) {
	st, _ := openNew(t, options())
	if _, err := st.PutApp(App{Name: "web", Spec: appSpec()}, "test"); err != nil {
		t.Fatal(err)
	}
	for _, node := range []string{st.Node(), "n2"} {
		if err := st.PutNode(Node{Name: node, Address: node + ":7400"}, "test"); err != nil {
			t.Fatal(err)
		}
	}
	if _, err := st.MarkFailed("n2", "test"); err != nil {
		t.Fatal(err)
	}
	if _, err := st.Create(NewTenant{Name: "a", App: "web", Node: "n2"}, "test"); !errors.Is(err, ErrConflict) {
		t.Fatalf("created on a failed node: %v", err)
	}
}
