package store

import (
	"context"
	"database/sql"
	"errors"
	"os"
	"path/filepath"
	"reflect"
	"slices"
	"strings"
	"testing"
	"time"
)

// logged makes st commit through a log of its own: every batch becomes the
// next entry, applied with ApplyEntry.
func logged(t *testing.T, st *SQLite) *[]Batch {
	t.Helper()
	var entries []Batch
	st.Replicate(func(batch Batch) (uint64, error) {
		entries = append(entries, batch)
		index := uint64(len(entries))
		return index, st.ApplyEntry(index, batch)
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
	if err := leader.PutNode(Node{Name: "node-1", Address: "10.0.0.1:7400"}, "alice"); err != nil {
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
	if err := st.PutNode(Node{Name: "fsn-1", Address: "10.0.0.1:7400"}, "alice"); err != nil {
		t.Fatal(err)
	}
	if err := st.PutNode(Node{Name: "hel-1", Address: "10.0.0.3:7400"}, "alice"); err != nil {
		t.Fatal(err)
	}
	version := st.state.Load().version
	if err := st.PutNode(Node{Name: "hel-1", Address: "10.0.0.3:7400"}, "alice"); err != nil || st.state.Load().version != version {
		t.Fatalf("an unchanged node was written again: %v", err)
	}
	for _, bad := range []Node{
		{Name: "x y", Address: "10.0.0.9:7400"},
		{Name: "nbg-1", Address: "10.0.0.1:7400"},
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
	if len(nodes) != 1 || nodes[0].Name != "hel-1" || nodes[0].CreatedAt.IsZero() {
		t.Fatalf("nodes = %+v", nodes)
	}
	events, _ := st.Events(EventQuery{})
	if len(events) != 3 || events[0].Action != "node.removed" || events[0].Detail != "fsn-1 at 10.0.0.1:7400" {
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

func TestBatchesTheStateRefusesCountAsRefused(t *testing.T) {
	st, _ := withApp(t, options())
	version := st.state.Load().version
	broken := &recorder{}
	if err := broken.Exec(`UPDATE no_such_table SET x = 1`); err != nil {
		t.Fatal(err)
	}
	err := st.ApplyEntry(1, Batch{Schema: schemaVersion, Version: version, Statements: broken.statements})
	if err == nil || !Refused(err) || st.Applied() != 1 {
		t.Fatalf("a statement the schema cannot run: %v (refused %v)", err, Refused(err))
	}
	malformed := Batch{Schema: schemaVersion, Version: version, Statements: []Statement{{SQL: `SELECT ?`, Args: []Arg{{Text: new("a"), Bool: new(true)}}}}}
	if err := st.ApplyEntry(2, malformed); !Refused(err) {
		t.Fatalf("a malformed argument: %v", err)
	}
	if err := malformed.Check(); err == nil {
		t.Fatal("the leader would append a malformed batch")
	}
	if err := (Batch{Schema: schemaVersion + 1}).Check(); !errors.Is(err, ErrSchema) {
		t.Fatalf("the leader would append a newer release's batch: %v", err)
	}
	if err := (Batch{Schema: schemaVersion}).Check(); err != nil {
		t.Fatal(err)
	}
	if nodeFailure(errors.New("connection reset")) == false {
		t.Fatal("a failure that is not SQLite's verdict is the node's own")
	}
}

func TestAStaleRetryReportsOnlyItsOwnRollout(t *testing.T) {
	st, _ := withApp(t, options())
	for _, name := range []string{"acme", "beta"} {
		if _, err := st.Create(NewTenant{Name: name, App: "web"}, "alice"); err != nil {
			t.Fatal(err)
		}
	}
	other := &recorder{}
	if err := other.Exec(`INSERT INTO events (at, actor, action, tenant, app, detail) VALUES ('2026-10-30T00:00:00Z', 'x', 'x', '', '', '')`); err != nil {
		t.Fatal(err)
	}
	commits := 0
	st.commit = func(batch Batch) (*snapshot, error) {
		commits++
		if commits == 1 {
			if _, err := st.apply(Batch{Schema: schemaVersion, Version: batch.Version, Statements: other.statements}); err != nil {
				t.Fatal(err)
			}
		}
		return st.apply(batch)
	}
	next := appSpec()
	next.Image = otherImage
	change, err := st.PutApp(App{Name: "web", Spec: next}, "alice")
	if err != nil {
		t.Fatal(err)
	}
	if commits != 2 || !reflect.DeepEqual(change.Tenants, []string{"acme", "beta"}) || change.Created {
		t.Fatalf("after %d commits: %+v", commits, change)
	}
}

func TestAWriteReturnsTheStateItMade(t *testing.T) {
	st, _ := withApp(t, options())
	var entries []Batch
	st.Replicate(func(batch Batch) (uint64, error) {
		entries = append(entries, batch)
		index := uint64(len(entries))
		if err := st.ApplyEntry(index, batch); err != nil {
			return 0, err
		}
		// Another node's write lands before this one returns.
		later := &recorder{}
		_ = later.Exec(`DELETE FROM secrets`)
		_ = later.Exec(`DELETE FROM tenants`)
		entries = append(entries, Batch{Schema: schemaVersion, Version: st.state.Load().version, Statements: later.statements})
		if err := st.ApplyEntry(uint64(len(entries)), entries[len(entries)-1]); err != nil {
			t.Fatal(err)
		}
		return index, nil
	}, func() error { return nil })
	created, err := st.Create(NewTenant{Name: "acme", App: "web"}, "alice")
	if err != nil {
		t.Fatal(err)
	}
	if created.Spec.Name != "acme" || created.Subnet == "" {
		t.Fatalf("the write returned a later state: %+v", created)
	}
}

func TestTenantsRunOnANodeOfTheCluster(t *testing.T) {
	st, _ := withApp(t, options())
	local, err := st.Create(NewTenant{Name: "acme", App: "web"}, "alice")
	if err != nil {
		t.Fatal(err)
	}
	if local.Node != "node-1" {
		t.Fatalf("a tenant created here runs on %q", local.Node)
	}
	if _, err := st.Create(NewTenant{Name: "beta", App: "web", Node: "nbg-1"}, "alice"); !errors.Is(err, ErrNotFound) {
		t.Fatalf("a tenant on an unknown node: %v", err)
	}
	if err := st.PutNode(Node{Name: "nbg-1", Address: "10.0.0.2:7400"}, "alice"); err != nil {
		t.Fatal(err)
	}
	remote, err := st.Create(NewTenant{Name: "beta", App: "web", Node: "nbg-1"}, "alice")
	if err != nil || remote.Node != "nbg-1" {
		t.Fatalf("beta: %+v, %v", remote, err)
	}
	if err := st.RemoveNode("nbg-1", "alice"); !errors.Is(err, ErrInUse) || !strings.Contains(err.Error(), "beta") {
		t.Fatalf("remove a node that runs a tenant: %v", err)
	}
	if err := st.Archive("beta", "alice"); err != nil {
		t.Fatal(err)
	}
	if err := st.RemoveNode("nbg-1", "alice"); !errors.Is(err, ErrInUse) {
		t.Fatalf("remove a node that keeps an archived tenant: %v", err)
	}
	if err := st.Forget("beta", "alice"); err != nil {
		t.Fatal(err)
	}
	if err := st.RemoveNode("nbg-1", "alice"); err != nil {
		t.Fatal(err)
	}
	events, _ := st.Events(EventQuery{Tenant: "beta", Limit: 3})
	if events[2].Detail != "on node nbg-1" {
		t.Fatalf("created: %+v", events[0])
	}
}

func TestSnapshotsCarryTheClusterName(t *testing.T) {
	leader := openNamed(t, "node-1")
	logged(t, leader)
	if err := leader.NameCluster("blue", "alice"); err != nil {
		t.Fatal(err)
	}
	if leader.Cluster() != "blue" || leader.Fresh() {
		t.Fatalf("cluster %q, fresh %v", leader.Cluster(), leader.Fresh())
	}
	snapshot, err := leader.SnapshotFile()
	if err != nil {
		t.Fatal(err)
	}
	defer removeDatabase(snapshot)
	follower := openNamed(t, "node-2")
	file, err := os.Open(snapshot)
	if err != nil {
		t.Fatal(err)
	}
	defer file.Close()
	if installed, err := follower.InstallSnapshot(file); err != nil || !installed {
		t.Fatalf("install: %v %v", installed, err)
	}
	if follower.Cluster() != "blue" {
		t.Fatalf("the follower's cluster is %q", follower.Cluster())
	}
}

func TestAFrozenStateTakesNoChanges(t *testing.T) {
	st, _ := withApp(t, options())
	if err := st.Freeze("upgrade to abc", "alice"); err != nil {
		t.Fatal(err)
	}
	if st.Frozen() != "upgrade to abc" {
		t.Fatalf("frozen = %q", st.Frozen())
	}
	if _, err := st.Create(NewTenant{Name: "acme", App: "web"}, "bob"); !errors.Is(err, ErrFrozen) || !strings.Contains(err.Error(), "upgrade to abc") {
		t.Fatalf("a change while frozen: %v", err)
	}
	if err := st.Freeze("again", "alice"); !errors.Is(err, ErrFrozen) {
		t.Fatalf("a second freeze: %v", err)
	}
	if err := st.Thaw("alice"); err != nil {
		t.Fatal(err)
	}
	if err := st.Thaw("alice"); err != nil {
		t.Fatalf("thaw twice: %v", err)
	}
	if _, err := st.Create(NewTenant{Name: "acme", App: "web"}, "bob"); err != nil {
		t.Fatal(err)
	}
	events, _ := st.Events(EventQuery{Limit: 3})
	if events[1].Action != "changes.thawed" || events[2].Action != "changes.frozen" || events[2].Detail != "upgrade to abc" {
		t.Fatalf("events = %+v", events)
	}
	// A snapshot carries the freeze.
	logged(t, st)
	if err := st.Freeze("upgrade to def", "alice"); err != nil {
		t.Fatal(err)
	}
	snapshot, err := st.SnapshotFile()
	if err != nil {
		t.Fatal(err)
	}
	defer removeDatabase(snapshot)
	follower := openNamed(t, "node-2")
	file, _ := os.Open(snapshot)
	defer file.Close()
	if _, err := follower.InstallSnapshot(file); err != nil {
		t.Fatal(err)
	}
	if follower.Frozen() != "upgrade to def" {
		t.Fatalf("the follower's freeze is %q", follower.Frozen())
	}
}
