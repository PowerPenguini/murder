package store

import (
	"context"
	"database/sql"
	"errors"
	"os"
	"path/filepath"
	"reflect"
	"slices"
	"testing"
	"time"
)

// logged makes st commit through a log of its own: every batch becomes the
// next entry, applied with ApplyEntry.
func logged(t *testing.T, st *SQLite) *[]Batch {
	t.Helper()
	var entries []Batch
	st.Replicate(func(batch Batch) error {
		entries = append(entries, batch)
		return st.ApplyEntry(uint64(len(entries)), batch)
	}, func() error { return nil })
	return &entries
}

func openNamed(t *testing.T, node string) *SQLite {
	t.Helper()
	path := filepath.Join(t.TempDir(), "murder.db")
	if err := Init(path, node); err != nil {
		t.Fatal(err)
	}
	st, err := Open(path, options())
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = st.Close() })
	return st
}

func TestLogEntriesApplyOnceAndSurviveARestart(t *testing.T) {
	st, path := openNew(t, options())
	entries := logged(t, st)
	if _, err := st.PutApp(App{Name: "web", Spec: appSpec()}, "alice"); err != nil {
		t.Fatal(err)
	}
	if _, err := st.Create(NewTenant{Name: "acme", App: "web"}, "alice"); err != nil {
		t.Fatal(err)
	}
	if st.Applied() != 2 {
		t.Fatalf("applied = %d after two entries", st.Applied())
	}
	// A refused entry counts as applied, and changes nothing.
	stale := (*entries)[0]
	if err := st.ApplyEntry(3, stale); !errors.Is(err, ErrStale) || st.Applied() != 3 || st.state.Load().version != 2 {
		t.Fatalf("stale entry: %v, applied %d, version %d", err, st.Applied(), st.state.Load().version)
	}
	if err := st.Close(); err != nil {
		t.Fatal(err)
	}

	reopened, err := Open(path, options())
	if err != nil {
		t.Fatal(err)
	}
	defer reopened.Close()
	// Only what committed is remembered; the log replays everything after.
	if reopened.Applied() != 2 {
		t.Fatalf("applied after a restart = %d", reopened.Applied())
	}
	for i, batch := range *entries {
		if err := reopened.ApplyEntry(uint64(i+1), batch); err != nil {
			t.Fatalf("replay entry %d: %v", i+1, err)
		}
	}
	if err := reopened.ApplyEntry(3, stale); !errors.Is(err, ErrStale) {
		t.Fatalf("replay the refused entry: %v", err)
	}
	if reopened.state.Load().version != 2 || len(reopened.List()) != 1 {
		t.Fatalf("a replay applied entries twice: version %d, tenants %v", reopened.state.Load().version, reopened.List())
	}
}

func TestWaitAppliedReturnsOnceTheEntryIsApplied(t *testing.T) {
	st, _ := openNew(t, options())
	logged(t, st)
	done := make(chan error, 1)
	go func() { done <- st.WaitApplied(context.Background(), 1) }()
	select {
	case err := <-done:
		t.Fatalf("returned before the entry was applied: %v", err)
	case <-time.After(20 * time.Millisecond):
	}
	if _, err := st.PutApp(App{Name: "web", Spec: appSpec()}, "alice"); err != nil {
		t.Fatal(err)
	}
	if err := <-done; err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if err := st.WaitApplied(ctx, 9); !errors.Is(err, context.Canceled) {
		t.Fatalf("wait for an entry never applied: %v", err)
	}
}

func TestSnapshotsCarryTheReplicatedStateOnly(t *testing.T) {
	leader := openNamed(t, "node-1")
	logged(t, leader)
	if err := leader.PutNode(Node{Name: "node-1", Address: "10.0.0.1:7400", Role: RoleWorker}, "alice"); err != nil {
		t.Fatal(err)
	}
	if _, err := leader.PutApp(App{Name: "web", Spec: appSpec()}, "alice"); err != nil {
		t.Fatal(err)
	}
	if _, err := leader.Create(NewTenant{Name: "acme", App: "web"}, "alice"); err != nil {
		t.Fatal(err)
	}
	snapshot, err := leader.SnapshotFile()
	if err != nil {
		t.Fatal(err)
	}
	defer removeDatabase(snapshot)

	follower := openNamed(t, "node-2")
	if !follower.Fresh() || leader.Fresh() {
		t.Fatal("fresh is wrong")
	}
	if err := follower.local(func(tx *sql.Tx) error {
		_, err := tx.Exec(`INSERT INTO images (ref) VALUES ('mine')`)
		return err
	}); err != nil {
		t.Fatal(err)
	}
	drain(follower)
	install := func(st *SQLite) bool {
		file, err := os.Open(snapshot)
		if err != nil {
			t.Fatal(err)
		}
		defer file.Close()
		installed, err := st.InstallSnapshot(file)
		if err != nil {
			t.Fatal(err)
		}
		return installed
	}
	if !install(follower) {
		t.Fatal("a fresh node did not take the snapshot")
	}
	if !drain(follower) {
		t.Error("installing a snapshot did not wake the reconciler")
	}
	if follower.Node() != "node-2" || follower.Applied() != 3 || follower.state.Load().version != 3 {
		t.Fatalf("follower: node %s, applied %d, version %d", follower.Node(), follower.Applied(), follower.state.Load().version)
	}
	if !reflect.DeepEqual(follower.List(), leader.List()) || !reflect.DeepEqual(follower.Nodes(), leader.Nodes()) || !reflect.DeepEqual(follower.Apps(), leader.Apps()) {
		t.Fatalf("follower state differs:\n%+v\n%+v", follower.List(), leader.List())
	}
	if !slices.Contains(follower.Images(), "mine") {
		t.Error("the follower lost its own images")
	}
	leaderEvents, _ := leader.Events(EventQuery{})
	followerEvents, _ := follower.Events(EventQuery{})
	if !reflect.DeepEqual(leaderEvents, followerEvents) {
		t.Errorf("events differ: %+v %+v", leaderEvents, followerEvents)
	}

	// A node already holding the snapshot's entries keeps its state.
	if install(follower) || install(leader) {
		t.Fatal("a snapshot replaced a state at least as new")
	}
	// Writes go on from the snapshot's version.
	logged(t, follower)
	follower.applied.set(3)
	if _, err := follower.SetSuspended("acme", true, "bob"); err != nil {
		t.Fatal(err)
	}
}

func TestStartLogForgetsTheOldLog(t *testing.T) {
	st, _ := openNew(t, options())
	logged(t, st)
	if _, err := st.PutApp(App{Name: "web", Spec: appSpec()}, "alice"); err != nil {
		t.Fatal(err)
	}
	if err := st.StartLog(); err != nil {
		t.Fatal(err)
	}
	if st.Applied() != 0 || len(st.Apps()) != 1 {
		t.Fatalf("applied %d, apps %v", st.Applied(), st.Apps())
	}
}

func TestNodesAreReplicatedState(t *testing.T) {
	st, _ := openNew(t, options())
	if err := st.PutNode(Node{Name: "fsn-1", Address: "10.0.0.1:7400", Role: RoleWorker}, "alice"); err != nil {
		t.Fatal(err)
	}
	if err := st.PutNode(Node{Name: "hel-1", Address: "10.0.0.3:7400", Role: RoleWitness}, "alice"); err != nil {
		t.Fatal(err)
	}
	version := st.state.Load().version
	if err := st.PutNode(Node{Name: "hel-1", Address: "10.0.0.3:7400", Role: RoleWitness}, "alice"); err != nil || st.state.Load().version != version {
		t.Fatalf("an unchanged node was written again: %v", err)
	}
	for _, bad := range []Node{
		{Name: "x y", Address: "10.0.0.9:7400", Role: RoleWorker},
		{Name: "nbg-1", Address: "10.0.0.9:7400", Role: "boss"},
		{Name: "nbg-1", Address: "10.0.0.1:7400", Role: RoleWorker},
	} {
		if err := st.PutNode(bad, "alice"); err == nil {
			t.Errorf("node %+v was accepted", bad)
		}
	}
	if err := st.RemoveNode("fsn-1", "alice"); err != nil {
		t.Fatal(err)
	}
	if err := st.RemoveNode("fsn-1", "alice"); !errors.Is(err, ErrNotFound) {
		t.Fatalf("remove a node twice: %v", err)
	}
	nodes := st.Nodes()
	if len(nodes) != 1 || nodes[0].Name != "hel-1" || nodes[0].Role != RoleWitness || nodes[0].CreatedAt.IsZero() {
		t.Fatalf("nodes = %+v", nodes)
	}
	events, _ := st.Events(EventQuery{})
	if len(events) != 3 || events[0].Action != "node.removed" || events[0].Detail != "fsn-1: worker at 10.0.0.1:7400" {
		t.Fatalf("events = %+v", events)
	}
}

func TestRefusedTellsTheStateFromTheNode(t *testing.T) {
	st, _ := withApp(t, options())
	recorded := &recorder{}
	if err := recorded.Exec(`INSERT INTO apps (name, spec, created_at, updated_at) VALUES ('web', '{}', '', '')`); err != nil {
		t.Fatal(err)
	}
	err := st.ApplyEntry(1, Batch{Schema: schemaVersion, Version: st.state.Load().version, Statements: recorded.statements})
	if err == nil || !Refused(err) || st.Applied() != 1 {
		t.Fatalf("a duplicate key: %v (refused %v), applied %d", err, Refused(err), st.Applied())
	}
	err = st.ApplyEntry(2, Batch{Schema: schemaVersion + 1})
	if !errors.Is(err, ErrSchema) || Refused(err) || st.Applied() != 1 {
		t.Fatalf("a batch of another schema: %v, applied %d", err, st.Applied())
	}
}
