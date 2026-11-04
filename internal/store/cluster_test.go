package store

import (
	"bytes"
	"context"
	"crypto/ed25519"
	"database/sql"
	"encoding/base64"
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
	if err := st.PutNode(Node{Name: "hel-1", Address: "10.0.0.3:7400", Key: testKey(3)}, "alice"); err != nil {
		t.Fatal(err)
	}
	version := st.state.Load().version
	if err := st.PutNode(Node{Name: "hel-1", Address: "10.0.0.3:7400"}, "alice"); err != nil || st.state.Load().version != version {
		t.Fatalf("an unchanged node was written again: %v", err)
	}
	// A join adds a node; it does not move one already there.
	if err := st.AddNode(Node{Name: "hel-1", Address: "10.0.0.3:7400", Key: testKey(3)}, noVoters, "alice"); err != nil || st.state.Load().version != version {
		t.Fatalf("a node added again at its address: %v", err)
	}
	if err := st.AddNode(Node{Name: "hel-1", Address: "10.0.0.4:7400", Key: testKey(3)}, noVoters, "mallory"); !errors.Is(err, ErrConflict) {
		t.Fatalf("a node added at another address: %v", err)
	}
	if address := st.state.Load().nodes["hel-1"].Address; address != "10.0.0.3:7400" {
		t.Fatalf("hel-1 moved to %s", address)
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

// testKey is an identity key in base64, made of fill.
func testKey(fill byte) string {
	return base64.StdEncoding.EncodeToString(bytes.Repeat([]byte{fill}, ed25519.PublicKeySize))
}

func TestANodeJoinsOnlyWithTheKeyItWasAdmittedWith(t *testing.T) {
	now := time.Date(2026, 11, 3, 12, 0, 0, 0, time.UTC)
	st, _ := openNew(t, options())
	st.now = func() time.Time { return now }
	if err := st.PutNode(Node{Name: "fsn-1", Address: "10.0.0.1:7400", Key: testKey(1)}, "alice"); err != nil {
		t.Fatal(err)
	}
	// No admission: no join, with a key or without.
	for _, key := range []string{"", testKey(2)} {
		if err := st.AddNode(Node{Name: "nbg-1", Address: "10.0.0.2:7400", Key: key}, noVoters, "mallory"); !errors.Is(err, ErrNotAdmitted) {
			t.Fatalf("a join without an admission (key %q): %v", key, err)
		}
	}
	if err := st.Admit("nbg-1", testKey(2), now.Add(AdmissionValid), noVoters, "root"); err != nil {
		t.Fatal(err)
	}
	if !st.Admitted("nbg-1", testKey(2), now) || st.Admitted("nbg-1", testKey(9), now) || st.Admitted("nbg-1", testKey(2), now.Add(AdmissionValid)) {
		t.Fatal("Admitted is wrong")
	}
	// Another key than the one admitted is refused.
	if err := st.AddNode(Node{Name: "nbg-1", Address: "10.0.0.2:7400", Key: testKey(9)}, noVoters, "mallory"); !errors.Is(err, ErrNotAdmitted) {
		t.Fatalf("a join with another key: %v", err)
	}
	if err := st.AddNode(Node{Name: "nbg-1", Address: "10.0.0.2:7400", Key: testKey(2)}, noVoters, "alice"); err != nil {
		t.Fatal(err)
	}
	if key, ok := st.NodeKey("nbg-1"); !ok || key != testKey(2) {
		t.Fatalf("key %q, %v", key, ok)
	}
	// The admission is used up; the same join asked again changes nothing.
	if st.Admitted("nbg-1", testKey(2), now) {
		t.Fatal("the admission was not used up")
	}
	version := st.state.Load().version
	if err := st.AddNode(Node{Name: "nbg-1", Address: "10.0.0.2:7400", Key: testKey(2)}, noVoters, "alice"); err != nil || st.state.Load().version != version {
		t.Fatalf("the join again: %v", err)
	}
	// An admission expires.
	if err := st.Admit("hel-1", testKey(3), now.Add(AdmissionValid), noVoters, "root"); err != nil {
		t.Fatal(err)
	}
	now = now.Add(AdmissionValid + time.Second)
	if err := st.AddNode(Node{Name: "hel-1", Address: "10.0.0.3:7400", Key: testKey(3)}, noVoters, "alice"); !errors.Is(err, ErrNotAdmitted) {
		t.Fatalf("a join on an expired admission: %v", err)
	}
	// The next admission takes the expired one away.
	if err := st.Admit("hel-2", testKey(4), now.Add(AdmissionValid), noVoters, "root"); err != nil {
		t.Fatal(err)
	}
	if _, ok := st.state.Load().admissions["hel-1"]; ok {
		t.Fatal("an expired admission stayed")
	}
	if err := st.Admit("hel-3", "short", now.Add(AdmissionValid), noVoters, "root"); !errors.Is(err, ErrInvalid) {
		t.Fatalf("a malformed key: %v", err)
	}
}

// noVoters says no node is a voter, as for a store outside a cluster.
func noVoters(string) bool { return false }

// A key the state holds stays, whoever writes what: the state itself
// refuses any statement that would change it, rename its node or put
// another key, or none, in its place. Taking the row out is the only way.
func TestAPinnedKeyNeverChanges(t *testing.T) {
	st, _ := openNew(t, options())
	logged(t, st)
	if err := st.PutNode(Node{Name: "fsn-1", Address: "10.0.0.1:7400", Key: testKey(1)}, "alice"); err != nil {
		t.Fatal(err)
	}
	for _, statement := range []string{
		`UPDATE nodes SET key = '` + testKey(2) + `' WHERE name = 'fsn-1'`,
		`UPDATE nodes SET key = '' WHERE name = 'fsn-1'`,
		`UPDATE nodes SET name = 'other' WHERE name = 'fsn-1'`,
		`INSERT OR REPLACE INTO nodes (name, address, created_at, key) VALUES ('fsn-1', '10.0.0.1:7400', '2026-11-04T00:00:00Z', '` + testKey(2) + `')`,
		`INSERT OR REPLACE INTO nodes (name, address, created_at) VALUES ('fsn-1', '10.0.0.1:7400', '2026-11-04T00:00:00Z')`,
		`INSERT INTO nodes (name, address, created_at, key) VALUES ('fsn-1', '10.0.0.1:7400', '2026-11-04T00:00:00Z', '` + testKey(2) + `') ON CONFLICT (name) DO UPDATE SET key = excluded.key`,
	} {
		recorded := &recorder{}
		if err := recorded.Exec(statement); err != nil {
			t.Fatal(err)
		}
		err := st.Apply(Batch{Schema: SchemaVersion, Version: st.state.Load().version, Statements: recorded.statements})
		if !Refused(err) || !strings.Contains(err.Error(), "keeps its identity key") {
			t.Fatalf("%s: %v", statement, err)
		}
		if key, _ := st.NodeKey("fsn-1"); key != testKey(1) {
			t.Fatalf("%s changed the key to %q", statement, key)
		}
	}
	// A node that installs the state as a snapshot holds the key, and keeps
	// it the same way.
	snapshot, err := st.SnapshotFile()
	if err != nil {
		t.Fatal(err)
	}
	defer removeDatabase(snapshot)
	other, _ := openNew(t, options())
	file, err := os.Open(snapshot)
	if err != nil {
		t.Fatal(err)
	}
	defer file.Close()
	if _, err := other.InstallSnapshot(file); err != nil {
		t.Fatal(err)
	}
	if key, _ := other.NodeKey("fsn-1"); key != testKey(1) {
		t.Fatalf("installed key %q", key)
	}
	cleared := &recorder{}
	_ = cleared.Exec(`UPDATE nodes SET key = '' WHERE name = 'fsn-1'`)
	if err := other.Apply(Batch{Schema: SchemaVersion, Version: other.state.Load().version, Statements: cleared.statements}); !Refused(err) {
		t.Fatalf("a pinned key cleared on a node that installed the state: %v", err)
	}
	// Other columns change as before, and the row may go, key and all.
	recorded := &recorder{}
	_ = recorded.Exec(`UPDATE nodes SET public = '203.0.113.1' WHERE name = 'fsn-1'`)
	if err := st.Apply(Batch{Schema: SchemaVersion, Version: st.state.Load().version, Statements: recorded.statements}); err != nil {
		t.Fatal(err)
	}
	if err := st.RemoveNode("fsn-1", "alice"); err != nil {
		t.Fatal(err)
	}
}

// A voter keeps its key: no admission for its name, no join with another
// key. A node the state holds but the voters do not, as after a rebuild
// from a backup, is let in again with a new key.
func TestAVotersKeyIsNotReplaced(t *testing.T) {
	now := time.Date(2026, 11, 3, 12, 0, 0, 0, time.UTC)
	st, _ := openNew(t, options())
	st.now = func() time.Time { return now }
	if err := st.PutNode(Node{Name: "fsn-1", Address: "10.0.0.1:7400", Key: testKey(1)}, "alice"); err != nil {
		t.Fatal(err)
	}
	voter := func(name string) bool { return name == "fsn-1" || name == "ghost" }
	for _, name := range []string{"fsn-1", "ghost"} {
		if err := st.Admit(name, testKey(2), now.Add(AdmissionValid), voter, "root"); !errors.Is(err, ErrConflict) {
			t.Fatalf("an admission for voter %s: %v", name, err)
		}
	}
	// An admission that got in before the node became a voter replaces
	// nothing once it is one.
	if err := st.Admit("fsn-1", testKey(2), now.Add(AdmissionValid), noVoters, "root"); err != nil {
		t.Fatal(err)
	}
	if err := st.AddNode(Node{Name: "fsn-1", Address: "10.0.0.1:7400", Key: testKey(2)}, voter, "mallory"); !errors.Is(err, ErrConflict) {
		t.Fatalf("another key for a voter: %v", err)
	}
	if key, _ := st.NodeKey("fsn-1"); key != testKey(1) {
		t.Fatalf("key %q", key)
	}
	// No voter: the node comes back with its new key.
	if err := st.AddNode(Node{Name: "fsn-1", Address: "10.0.0.1:7400", Key: testKey(2)}, noVoters, "alice"); err != nil {
		t.Fatal(err)
	}
	if key, _ := st.NodeKey("fsn-1"); key != testKey(2) || st.Admitted("fsn-1", testKey(2), now) {
		t.Fatalf("after the rebuild: key %q", key)
	}
}

func TestANodeOfAnOlderReleaseRegistersItsKeyOnce(t *testing.T) {
	st, _ := openNew(t, options())
	// A node stored by a release before keys.
	if err := st.PutNode(Node{Name: "fsn-1", Address: "10.0.0.1:7400"}, "alice"); err != nil {
		t.Fatal(err)
	}
	if key, ok := st.NodeKey("fsn-1"); !ok || key != "" {
		t.Fatalf("key %q, %v", key, ok)
	}
	if err := st.SetNodeKey("fsn-1", testKey(1), "murder"); err != nil {
		t.Fatal(err)
	}
	version := st.state.Load().version
	if err := st.SetNodeKey("fsn-1", testKey(1), "murder"); err != nil || st.state.Load().version != version {
		t.Fatalf("the same key again: %v", err)
	}
	// Once pinned, no other key replaces it.
	if err := st.SetNodeKey("fsn-1", testKey(2), "mallory"); !errors.Is(err, ErrConflict) {
		t.Fatalf("another key: %v", err)
	}
	if err := st.SetNodeKey("nbg-1", testKey(2), "mallory"); !errors.Is(err, ErrNotFound) {
		t.Fatalf("a key for no node: %v", err)
	}
	if key, _ := st.NodeKey("fsn-1"); key != testKey(1) {
		t.Fatalf("key %q", key)
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
	// The same upgrade resumes under its own freeze.
	if err := st.Freeze("upgrade to abc", "alice"); err != nil {
		t.Fatalf("a freeze for the same reason: %v", err)
	}
	if err := st.Note("node.upgrading", "n2 to abc", "alice"); err != nil {
		t.Fatalf("a note while frozen: %v", err)
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
	events, _ := st.Events(EventQuery{Limit: 4})
	if events[1].Action != "changes.thawed" || events[2].Action != "node.upgrading" || events[3].Action != "changes.frozen" || events[3].Detail != "upgrade to abc" {
		t.Fatalf("events = %+v", events)
	}
	// A snapshot carries the freeze.
	entries := logged(t, st)
	if err := st.Freeze("upgrade to def", "alice"); err != nil {
		t.Fatal(err)
	}
	// Nodes of the release before apply a freeze.
	if (*entries)[0].Schema != firstBatchSchema {
		t.Fatalf("a freeze is written as of schema version %d", (*entries)[0].Schema)
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

func TestAMoveGivesTheTenantToItsTarget(t *testing.T) {
	st, _ := withApp(t, options())
	if _, err := st.Create(NewTenant{Name: "acme", App: "web"}, "alice"); err != nil {
		t.Fatal(err)
	}
	if _, err := st.StartMove("acme", "nbg-1", "alice"); !errors.Is(err, ErrNotFound) {
		t.Fatalf("a move to an unknown node: %v", err)
	}
	// A cluster has this node among its members, as bootstrapping or
	// joining makes it.
	if err := st.PutNode(Node{Name: "node-1", Address: "10.0.0.1:7400"}, "alice"); err != nil {
		t.Fatal(err)
	}
	if err := st.PutNode(Node{Name: "nbg-1", Address: "10.0.0.2:7400"}, "alice"); err != nil {
		t.Fatal(err)
	}
	if _, err := st.StartMove("acme", "node-1", "alice"); !errors.Is(err, ErrConflict) {
		t.Fatalf("a move to the node it runs on: %v", err)
	}
	moving, err := st.StartMove("acme", "nbg-1", "alice")
	if err != nil || moving.MoveTo != "nbg-1" || moving.MoveHeld || moving.Node != "node-1" {
		t.Fatalf("moving: %+v, %v", moving, err)
	}
	if _, err := st.StartMove("acme", "nbg-1", "alice"); !errors.Is(err, ErrConflict) {
		t.Fatalf("a second move: %v", err)
	}
	if err := st.HoldMove("acme"); err != nil {
		t.Fatal(err)
	}
	if record, _ := st.Get("acme"); !record.MoveHeld {
		t.Fatal("not held")
	}
	moved, err := st.FinishMove("acme", "alice")
	if err != nil || moved.Node != "nbg-1" || moved.MoveTo != "" || !moved.MoveHeld || moved.Generation != moving.Generation+1 {
		t.Fatalf("moved: %+v, %v", moved, err)
	}
	// Requests wait until the new node serves it.
	if err := st.Freeze("upgrade", "alice"); err != nil {
		t.Fatal(err)
	}
	if err := st.ReleaseHold("acme"); err != nil {
		t.Fatalf("release while frozen: %v", err)
	}
	if record, _ := st.Get("acme"); record.MoveHeld {
		t.Fatal("still held")
	}
	if err := st.Thaw("alice"); err != nil {
		t.Fatal(err)
	}
	// From here on, nbg-1 moves it.
	if _, err := st.StartMove("acme", "node-1", "alice"); !errors.Is(err, ErrConflict) {
		t.Fatalf("a move started away from the node that runs it: %v", err)
	}
	if _, err := st.Create(NewTenant{Name: "beta", App: "web"}, "alice"); err != nil {
		t.Fatal(err)
	}
	if _, err := st.StartMove("beta", "nbg-1", "alice"); err != nil {
		t.Fatal(err)
	}
	if err := st.Freeze("upgrade", "alice"); err != nil {
		t.Fatal(err)
	}
	if cancelled, err := st.CancelMove("beta", "alice"); err != nil || !cancelled {
		t.Fatalf("cancel while frozen: %v %v", cancelled, err)
	}
	if cancelled, err := st.CancelMove("beta", "alice"); err != nil || cancelled {
		t.Fatalf("cancel nothing: %v %v", cancelled, err)
	}
	if err := st.Thaw("alice"); err != nil {
		t.Fatal(err)
	}
	if record, _ := st.Get("beta"); record.MoveTo != "" || record.Node != "node-1" {
		t.Fatalf("after cancelling: %+v", record)
	}
	events, _ := st.Events(EventQuery{Tenant: "acme", Limit: 2})
	if events[0].Action != "tenant.moved" || events[0].Detail != "node-1 → nbg-1" || events[1].Action != "tenant.moving" {
		t.Fatalf("events = %+v", events)
	}
}

func TestEveryNodeKnowsHowALatelyAppliedBatchWent(t *testing.T) {
	st, _ := withApp(t, options())
	var last Batch
	st.Replicate(func(batch Batch) (uint64, error) {
		last = batch
		return 1, st.ApplyEntry(1, batch)
	}, func() error { return nil })
	if _, err := st.Create(NewTenant{Name: "acme", App: "web"}, "alice"); err != nil {
		t.Fatal(err)
	}
	if last.ID == "" {
		t.Fatal("a batch without an ID")
	}
	if outcome, ok := st.Outcome(last.ID); !ok || outcome.Index != 1 || outcome.Err != nil {
		t.Fatalf("outcome = %+v, %v", outcome, ok)
	}
	// A refused one is known as refused.
	stale := last
	stale.ID = "again"
	if err := st.ApplyEntry(2, stale); !errors.Is(err, ErrStale) {
		t.Fatalf("apply stale: %v", err)
	}
	if outcome, ok := st.Outcome("again"); !ok || !errors.Is(outcome.Err, ErrStale) {
		t.Fatalf("refused outcome = %+v, %v", outcome, ok)
	}
	if _, ok := st.Outcome("never"); ok {
		t.Fatal("an unknown batch has an outcome")
	}
}

// Nothing pulls a tenant from under its move: the node it moves to stays
// in the cluster, and it is neither archived, purged nor restored, until it
// has moved or the move is undone. Undoing a move and serving the moved
// tenant are batches nodes of the release that added moves apply.
func TestNothingPullsATenantFromUnderItsMove(t *testing.T) {
	st, _ := withApp(t, options())
	entries := logged(t, st)
	if _, err := st.Create(NewTenant{Name: "acme", App: "web"}, "alice"); err != nil {
		t.Fatal(err)
	}
	if err := st.PutNode(Node{Name: "nbg-1", Address: "10.0.0.2:7400"}, "alice"); err != nil {
		t.Fatal(err)
	}
	if _, err := st.StartMove("acme", "nbg-1", "alice"); err != nil {
		t.Fatal(err)
	}
	if err := st.RemoveNode("nbg-1", "alice"); !errors.Is(err, ErrInUse) {
		t.Fatalf("removing the node a tenant moves to: %v", err)
	}
	if err := st.Archive("acme", "alice"); !errors.Is(err, ErrConflict) {
		t.Fatalf("archiving a moving tenant: %v", err)
	}
	if moved, err := st.CancelMove("acme", "alice"); err != nil || !moved {
		t.Fatalf("cancel: %v, %v", moved, err)
	}
	if last := (*entries)[len(*entries)-1]; last.Schema != moveSchema {
		t.Fatalf("a cancelled move is a batch of schema %d", last.Schema)
	}
	// An archived tenant moves too, and is not purged or restored meanwhile.
	if err := st.Archive("acme", "alice"); err != nil {
		t.Fatal(err)
	}
	if _, err := st.StartMove("acme", "nbg-1", "alice"); err != nil {
		t.Fatal(err)
	}
	if err := st.Forget("acme", "alice"); !errors.Is(err, ErrConflict) {
		t.Fatalf("purging a moving tenant: %v", err)
	}
	if _, err := st.Restore("acme", "alice"); !errors.Is(err, ErrConflict) {
		t.Fatalf("restoring a moving tenant: %v", err)
	}
	if err := st.HoldMove("acme"); err != nil {
		t.Fatal(err)
	}
	if _, err := st.FinishMove("acme", "alice"); err != nil {
		t.Fatal(err)
	}
	if err := st.ReleaseHold("acme"); err != nil {
		t.Fatal(err)
	}
	if last := (*entries)[len(*entries)-1]; last.Schema != moveSchema {
		t.Fatalf("serving a moved tenant is a batch of schema %d", last.Schema)
	}
}

// An upgrade takes over another upgrade's freeze only by naming it: two
// upgrades never both think they hold it. A freeze for anything else is
// never taken over.
func TestAnUpgradeTakesOverAFreezeOnlyByNamingIt(t *testing.T) {
	st, _ := openNew(t, options())
	if err := st.Freeze(UpgradeFreeze+"aaa", "alice"); err != nil {
		t.Fatal(err)
	}
	if err := st.Freeze(UpgradeFreeze+"bbb", "alice"); !errors.Is(err, ErrFrozen) || st.Frozen() != UpgradeFreeze+"aaa" {
		t.Fatalf("an upgrade over another's freeze: %v, frozen %q", err, st.Frozen())
	}
	if err := st.TakeOverFreeze(UpgradeFreeze+"zzz", UpgradeFreeze+"bbb", "alice"); !errors.Is(err, ErrFrozen) || st.Frozen() != UpgradeFreeze+"aaa" {
		t.Fatalf("a takeover of a freeze that is not there: %v, frozen %q", err, st.Frozen())
	}
	if err := st.TakeOverFreeze(UpgradeFreeze+"aaa", UpgradeFreeze+"bbb", "alice"); err != nil || st.Frozen() != UpgradeFreeze+"bbb" {
		t.Fatalf("a takeover: %v, frozen %q", err, st.Frozen())
	}
	if err := st.ThawOwn(UpgradeFreeze+"aaa", "alice"); err != nil || st.Frozen() != UpgradeFreeze+"bbb" {
		t.Fatalf("the upgrade taken over thawed: %v, frozen %q", err, st.Frozen())
	}
	if err := st.Freeze("maintenance", "alice"); !errors.Is(err, ErrFrozen) {
		t.Fatalf("another freeze over an upgrade's: %v", err)
	}
	if err := st.ThawOwn(UpgradeFreeze+"bbb", "alice"); err != nil || st.Frozen() != "" {
		t.Fatalf("thaw its own: %v, frozen %q", err, st.Frozen())
	}
	if err := st.TakeOverFreeze(UpgradeFreeze+"bbb", UpgradeFreeze+"ccc", "alice"); !errors.Is(err, ErrConflict) || st.Frozen() != "" {
		t.Fatalf("a takeover once thawed: %v, frozen %q", err, st.Frozen())
	}
	if err := st.Freeze("maintenance", "alice"); err != nil {
		t.Fatal(err)
	}
	if err := st.Freeze(UpgradeFreeze+"ccc", "alice"); !errors.Is(err, ErrFrozen) {
		t.Fatalf("an upgrade over another freeze: %v", err)
	}
	if err := st.TakeOverFreeze("maintenance", UpgradeFreeze+"ccc", "alice"); err == nil || st.Frozen() != "maintenance" {
		t.Fatalf("an upgrade took over another freeze: %v, frozen %q", err, st.Frozen())
	}
	frozen, err := DatabaseFrozen(st.path)
	if err != nil || frozen != "maintenance" {
		t.Fatalf("DatabaseFrozen = %q, %v", frozen, err)
	}
}
