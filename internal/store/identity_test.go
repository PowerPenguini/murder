package store

import (
	"database/sql"
	"errors"
	"fmt"
	"net/netip"
	"os"
	"reflect"
	"slices"
	"strings"
	"testing"
	"time"
)

// A batch is a change of the state, not of how the database runs it: no
// transaction control, whose COMMIT would make part of it stick on one
// node and not as the log says.
func TestABatchControlsNoTransaction(t *testing.T) {
	st := openNamed(t, "leader")
	logged(t, st)
	if err := st.PutNode(Node{Name: "victim", Address: "127.0.0.1:1", Key: testKey(1)}, "root"); err != nil {
		t.Fatal(err)
	}
	for _, statements := range [][]string{
		{`DELETE FROM nodes WHERE name = 'victim'`, `COMMIT`},
		{`DELETE FROM nodes WHERE name = 'victim'; COMMIT; BEGIN`},
		{`SAVEPOINT s`, `DELETE FROM nodes WHERE name = 'victim'`, `RELEASE s`},
		{`ROLLBACK`},
	} {
		batch := Batch{Schema: SchemaVersion, Version: st.state.Load().version}
		for _, statement := range statements {
			batch.Statements = append(batch.Statements, Statement{SQL: statement})
		}
		if err := st.Apply(batch); !Refused(err) {
			t.Fatalf("%q: %v", statements, err)
		}
		var rows int
		if err := st.db.QueryRow(`SELECT count(*) FROM nodes WHERE name = 'victim'`).Scan(&rows); err != nil || rows != 1 || len(st.Nodes()) != 1 {
			t.Fatalf("%q left %d rows on disk, %d in memory: %v", statements, rows, len(st.Nodes()), err)
		}
	}
	// The leader refuses them before they reach the log.
	if err := (Batch{Schema: SchemaVersion, Statements: []Statement{{SQL: `COMMIT`}}}).Check(); err == nil {
		t.Fatal("Check took a COMMIT")
	}
}

// What a batch does may not depend on the node that applies it: it reads
// nothing only one node keeps, such as the node's own name, so every node
// does the same with it, refusing it alike.
func TestABatchReadsNothingOfTheNodesOwn(t *testing.T) {
	leader, follower := openNamed(t, "leader"), openNamed(t, "follower")
	for _, st := range []*SQLite{leader, follower} {
		for _, name := range []string{"leader", "follower"} {
			if err := st.PutNode(Node{Name: name, Address: name + ":7400", Key: testKey(1)}, "root"); err != nil {
				t.Fatal(err)
			}
		}
	}
	for i, statement := range []string{
		`DELETE FROM nodes WHERE name = 'follower' AND (SELECT value FROM meta WHERE key = 'node') = 'follower'`,
		`DELETE FROM nodes WHERE name = 'follower' AND (SELECT value FROM meta WHERE key = 'applied') = '0'`,
		`DELETE FROM nodes WHERE EXISTS (SELECT 1 FROM images)`,
		`INSERT INTO images (ref) VALUES ('mine')`,
	} {
		batch := Batch{Schema: SchemaVersion, Version: leader.state.Load().version, Statements: []Statement{{SQL: statement}}}
		for _, st := range []*SQLite{leader, follower} {
			if err := st.ApplyEntry(uint64(10+i), batch); !Refused(err) {
				t.Fatalf("%s on %s: %v", statement, st.Node(), err)
			}
		}
		for _, st := range []*SQLite{leader, follower} {
			if len(st.Nodes()) != 2 {
				t.Fatalf("%s changed %s's nodes", statement, st.Node())
			}
		}
	}
	// What the store's own changes read of meta still works.
	if err := leader.Freeze("upgrade to x", "root"); err != nil {
		t.Fatal(err)
	}
	if err := leader.ThawOwn("upgrade to x", "root"); err != nil {
		t.Fatal(err)
	}
}

// A batch changes rows, not the schema: no table, trigger or index made or
// dropped, so nothing it leaves behind acts on later changes, and the
// identity keys' triggers stay.
func TestABatchMakesNoSchema(t *testing.T) {
	st := openNamed(t, "leader")
	logged(t, st)
	if err := st.PutNode(Node{Name: "victim", Address: "victim:7400", Key: testKey(1)}, "root"); err != nil {
		t.Fatal(err)
	}
	for _, statements := range [][]string{
		{`CREATE TRIGGER wipe_nodes AFTER UPDATE ON meta WHEN NEW.key = 'version' BEGIN DELETE FROM nodes; END`},
		{`DROP TRIGGER events_id_bounded`, `DROP TRIGGER events_id_kept_bounded`, `DROP TRIGGER nodes_key_kept`, `DROP TRIGGER nodes_key_not_replaced`, `UPDATE nodes SET key = '` + testKey(2) + `'`},
		{`CREATE TABLE x (a TEXT)`},
		{`ALTER TABLE nodes ADD COLUMN x TEXT`},
		{`DROP INDEX tenants_node`},
		{`PRAGMA foreign_keys = OFF`},
		{`ATTACH DATABASE ':memory:' AS other`},
	} {
		batch := Batch{Schema: SchemaVersion, Version: st.state.Load().version}
		for _, statement := range statements {
			batch.Statements = append(batch.Statements, Statement{SQL: statement})
		}
		if err := batch.Check(); err == nil {
			t.Errorf("Check took %q", statements)
		}
		if err := st.Apply(batch); !Refused(err) {
			t.Fatalf("%q: %v", statements, err)
		}
	}
	// A later change of the version fires nothing, and the key stays.
	if err := st.Note("test", "", "root"); err != nil {
		t.Fatal(err)
	}
	if key, _ := st.NodeKey("victim"); len(st.Nodes()) != 1 || key != testKey(1) {
		t.Fatalf("nodes %+v", st.Nodes())
	}
	var triggers int
	if err := st.db.QueryRow(`SELECT count(*) FROM sqlite_master WHERE type = 'trigger'`).Scan(&triggers); err != nil || triggers != 4 {
		t.Fatalf("triggers %d, %v", triggers, err)
	}
}

// An admission never keys a voter, with a row or without, a key or none:
// a node without a key registers its own, presenting it.
func TestAnAdmissionDoesNotKeyAVoter(t *testing.T) {
	st := openNamed(t, "leader")
	if err := st.PutNode(Node{Name: "victim", Address: "victim:7400"}, "root"); err != nil {
		t.Fatal(err)
	}
	voter := func(name string) bool { return name == "victim" || name == "ghost" }
	for _, name := range []string{"victim", "ghost"} {
		if err := st.Admit(name, testKey(2), time.Now().Add(time.Hour), noVoters, "root"); err != nil {
			t.Fatal(err)
		}
		if err := st.AddNode(Node{Name: name, Address: name + ":7400", Key: testKey(2)}, voter, "attacker"); err == nil || !strings.Contains(err.Error(), "voter") {
			t.Fatalf("an admission keyed voter %s: %v", name, err)
		}
	}
	if key, _ := st.NodeKey("victim"); key != "" {
		t.Fatalf("key %q", key)
	}
	if _, named := st.NodeKey("ghost"); named {
		t.Fatal("a voter without a row got one")
	}
}

// A state of schema 15, migrated or installed as a snapshot, gets the
// identity keys' triggers.
func TestAStateOfSchema15GetsTheKeysTriggers(t *testing.T) {
	st := openNamed(t, "old")
	if err := st.PutNode(Node{Name: "victim", Address: "victim:7400"}, "root"); err != nil {
		t.Fatal(err)
	}
	source, err := st.SnapshotFile()
	if err != nil {
		t.Fatal(err)
	}
	defer removeDatabase(source)
	db, err := sql.Open("sqlite", source)
	if err != nil {
		t.Fatal(err)
	}
	for _, statement := range []string{`DROP TRIGGER events_id_bounded`, `DROP TRIGGER events_id_kept_bounded`, `DROP TRIGGER nodes_key_kept`, `DROP TRIGGER nodes_key_not_replaced`, `DROP TABLE admissions`, `DROP TABLE departed_keys`, `ALTER TABLE nodes DROP COLUMN key`, `DROP TABLE acme_challenges`, `DROP TABLE acme_accounts`, `DROP TABLE domains`, `DROP TABLE operation_lease`, `ALTER TABLE tenants DROP COLUMN canonical`, `DROP TABLE acme_revocations`, `ALTER TABLE rollouts DROP COLUMN first`, `PRAGMA user_version = 15`, `UPDATE meta SET value = '10' WHERE key = 'applied'`} {
		if _, err := db.Exec(statement); err != nil {
			t.Fatal(err)
		}
	}
	db.Close()
	target := openNamed(t, "new")
	file, err := os.Open(source)
	if err != nil {
		t.Fatal(err)
	}
	defer file.Close()
	if installed, err := target.InstallSnapshot(file); err != nil || !installed {
		t.Fatalf("install %v %v", installed, err)
	}
	migrated, err := Open(source, options())
	if err != nil {
		t.Fatal(err)
	}
	defer migrated.Close()
	for _, st := range []*SQLite{target, migrated} {
		if key, ok := st.NodeKey("victim"); !ok || key != "" {
			t.Fatalf("schema 15 row %v %q", ok, key)
		}
		if err := st.SetNodeKey("victim", testKey(1), "root"); err != nil {
			t.Fatal(err)
		}
		recorded := &recorder{}
		_ = recorded.Exec(`UPDATE nodes SET key = ''`)
		if err := st.Apply(Batch{Schema: SchemaVersion, Version: st.state.Load().version, Statements: recorded.statements}); !Refused(err) {
			t.Fatalf("a pinned key cleared: %v", err)
		}
	}
}

// applyAlike applies batch as log entry index on every store, and returns
// what each then holds as panel secret probe, or why it refused the batch.
func applyAlike(t *testing.T, index uint64, batch Batch, stores ...*SQLite) []string {
	t.Helper()
	var got []string
	for _, st := range stores {
		if err := st.ApplyEntry(index, batch); err != nil {
			if !Refused(err) {
				t.Fatalf("%s: not refused alike: %v", st.Node(), err)
			}
			got = append(got, "refused")
			continue
		}
		var value string
		if err := st.db.QueryRow(`SELECT value FROM panel_secrets WHERE name = 'probe'`).Scan(&value); err != nil {
			value = err.Error()
		}
		got = append(got, value)
	}
	return got
}

// A batch calls no function, however it names it: none of the store's
// changes calls one, and some give each node another result.
func TestABatchCallsNoFunction(t *testing.T) {
	a, b := openNamed(t, "a"), openNamed(t, "b")
	for i, statement := range []string{
		`INSERT INTO panel_secrets (name, value) VALUES ('probe', hex("randomblob"(32)))`,
		`INSERT INTO panel_secrets (name, value) VALUES ('probe', "random"())`,
		`INSERT INTO panel_secrets (name, value) VALUES ('probe', "datetime"('now'))`,
		`INSERT INTO panel_secrets (name, value) VALUES ('probe', "sqlite_version"())`,
		`INSERT INTO panel_secrets (name, value) VALUES ('probe', "load_extension"('x'))`,
		`INSERT INTO panel_secrets (name, value) VALUES ('probe', upper('x'))`,
	} {
		batch := Batch{Schema: SchemaVersion, Version: a.state.Load().version, Statements: []Statement{{SQL: statement}}}
		if got := applyAlike(t, uint64(10+i), batch, a, b); got[0] != "refused" || got[1] != "refused" {
			t.Fatalf("%s: %q", statement, got)
		}
	}
}

// A batch writes no meta row the store keeps for itself: the node's name,
// the version, the applied index.
func TestABatchKeepsOffTheStoresBookkeeping(t *testing.T) {
	st := openNamed(t, "a")
	for i, statement := range []string{
		`DELETE FROM meta WHERE key = 'applied'`,
		`UPDATE meta SET value = '7' WHERE key = 'version'`,
		`DELETE FROM meta WHERE key != 'frozen'`,
		`INSERT OR REPLACE INTO meta (key, value) VALUES ('node', 'b')`,
	} {
		batch := Batch{Schema: SchemaVersion, Version: st.state.Load().version, Statements: []Statement{{SQL: statement}}}
		if err := st.ApplyEntry(uint64(10+i), batch); !Refused(err) {
			t.Fatalf("%s: %v", statement, err)
		}
		var rows int
		if err := st.db.QueryRow(`SELECT count(*) FROM meta WHERE key IN ('node', 'version', 'applied')`).Scan(&rows); err != nil || rows != 3 {
			t.Fatalf("%s left %d bookkeeping rows: %v", statement, rows, err)
		}
	}
	// The replicated rows of meta, as a freeze writes them, are a batch's.
	if err := st.Freeze("upgrade to x", "root"); err != nil {
		t.Fatal(err)
	}
	if err := st.ThawOwn("upgrade to x", "root"); err != nil {
		t.Fatal(err)
	}
	st.Close()
	reopened, err := Open(st.path, options())
	if err != nil {
		t.Fatalf("reopen: %v", err)
	}
	reopened.Close()
}

// A trigger that is there, from wherever, runs under the batch's checks as
// the store counts the version and applied index, so it does the same on
// every node, or is refused on every node alike.
func TestATriggerFiredByTheStoresCountIsHeldToTheBatchsChecks(t *testing.T) {
	a, b := openNamed(t, "a"), openNamed(t, "b")
	for _, st := range []*SQLite{a, b} {
		if _, err := st.db.Exec(`CREATE TRIGGER legacy AFTER UPDATE ON meta WHEN NEW.key = 'version' BEGIN INSERT INTO panel_secrets (name, value) SELECT 'probe', value FROM meta WHERE key = 'node'; END`); err != nil {
			t.Fatal(err)
		}
	}
	batch := Batch{Schema: SchemaVersion, Version: a.state.Load().version, Statements: []Statement{{SQL: `DELETE FROM admissions`}}}
	if got := applyAlike(t, 10, batch, a, b); got[0] != got[1] {
		t.Fatalf("the nodes differ: %q", got)
	}
}

// A batch reads no table but the replicated ones, SQLite's own schema
// included, whose text differs between a new database and a migrated one.
func TestABatchReadsNoSchema(t *testing.T) {
	a, b := openNamed(t, "a"), openNamed(t, "b")
	for _, statement := range []string{`DROP TRIGGER events_id_bounded`, `DROP TRIGGER events_id_kept_bounded`, `DROP TRIGGER nodes_key_kept`, `DROP TRIGGER nodes_key_not_replaced`, `DROP TABLE admissions`, `DROP TABLE departed_keys`, `ALTER TABLE nodes DROP COLUMN key`, `DROP TABLE acme_challenges`, `DROP TABLE acme_accounts`, `DROP TABLE domains`, `DROP TABLE operation_lease`, `ALTER TABLE tenants DROP COLUMN canonical`, `DROP TABLE acme_revocations`, `ALTER TABLE rollouts DROP COLUMN first`, `PRAGMA user_version = 15`} {
		if _, err := b.db.Exec(statement); err != nil {
			t.Fatal(err)
		}
	}
	if err := upgrade(b.db); err != nil {
		t.Fatal(err)
	}
	for i, statement := range []string{
		`INSERT INTO panel_secrets (name, value) SELECT 'probe', sql FROM sqlite_schema WHERE name = 'nodes'`,
		`INSERT INTO panel_secrets (name, value) SELECT 'probe', sql FROM sqlite_master WHERE name = 'nodes'`,
		`INSERT INTO panel_secrets (name, value) SELECT 'probe', name FROM sqlite_sequence`,
	} {
		batch := Batch{Schema: SchemaVersion, Version: a.state.Load().version, Statements: []Statement{{SQL: statement}}}
		if got := applyAlike(t, uint64(10+i), batch, a, b); got[0] != "refused" || got[1] != "refused" {
			t.Fatalf("%s: %q", statement, got)
		}
	}
}

// The schema's own indexes and triggers are what ownObjects names, and an
// upgrade to schema 16 drops any other, such as one an earlier release's
// batch made.
func TestTheUpgradeDropsWhatIsNotTheSchemas(t *testing.T) {
	st := openNamed(t, "a")
	var own []string
	rows, err := st.db.Query(`SELECT name FROM sqlite_master WHERE type IN ('trigger', 'view', 'index') AND name NOT LIKE 'sqlite\_%' ESCAPE '\' ORDER BY name`)
	if err != nil {
		t.Fatal(err)
	}
	for rows.Next() {
		var name string
		_ = rows.Scan(&name)
		own = append(own, name)
	}
	rows.Close()
	want := slices.Clone(ownObjects)
	slices.Sort(want)
	if !slices.Equal(own, want) {
		t.Fatalf("the schema makes %q, ownObjects names %q", own, want)
	}
	for _, statement := range []string{
		`CREATE TRIGGER legacy AFTER UPDATE ON meta BEGIN DELETE FROM nodes; END`,
		`CREATE VIEW leftover AS SELECT * FROM nodes`,
		`CREATE INDEX leftover_index ON nodes (address)`,
		`DROP TRIGGER events_id_bounded`, `DROP TRIGGER events_id_kept_bounded`, `DROP TRIGGER nodes_key_kept`, `DROP TRIGGER nodes_key_not_replaced`, `DROP TABLE admissions`, `DROP TABLE departed_keys`, `ALTER TABLE nodes DROP COLUMN key`, `DROP TABLE acme_challenges`, `DROP TABLE acme_accounts`, `DROP TABLE domains`, `DROP TABLE operation_lease`, `ALTER TABLE tenants DROP COLUMN canonical`, `DROP TABLE acme_revocations`, `ALTER TABLE rollouts DROP COLUMN first`, `PRAGMA user_version = 15`,
	} {
		if _, err := st.db.Exec(statement); err != nil {
			t.Fatal(err)
		}
	}
	if err := upgrade(st.db); err != nil {
		t.Fatal(err)
	}
	var left int
	if err := st.db.QueryRow(`SELECT count(*) FROM sqlite_master WHERE name IN ('legacy', 'leftover', 'leftover_index')`).Scan(&left); err != nil || left != 0 {
		t.Fatalf("%d left, %v", left, err)
	}
}

// An event's id, a SQLite rowid, stays far from the largest rowid, past
// which SQLite gives the next one at random, another on each node: a batch
// that would go past the bound is refused on every node alike.
func TestAnEventIDStaysFarFromTheLargestRowid(t *testing.T) {
	a, b := openNamed(t, "a"), openNamed(t, "b")
	for i, statements := range [][]string{
		{`INSERT INTO events (id, at, actor, action, tenant, app, detail) VALUES (9223372036854775807, '', '', '', '', '', '')`,
			`INSERT INTO events (at, actor, action, tenant, app, detail) VALUES ('', '', '', '', '', 'second')`},
		{`INSERT INTO events (id, at, actor, action, tenant, app, detail) VALUES (4611686018427387905, '', '', '', '', '', '')`},
		{`INSERT INTO events (at, actor, action, tenant, app, detail) VALUES ('', '', '', '', '', 'x')`,
			`UPDATE events SET id = 9223372036854775807 WHERE detail = 'x'`},
	} {
		batch := Batch{Schema: SchemaVersion, Version: a.state.Load().version}
		for _, statement := range statements {
			batch.Statements = append(batch.Statements, Statement{SQL: statement})
		}
		for _, st := range []*SQLite{a, b} {
			if err := st.ApplyEntry(uint64(10+i), batch); !Refused(err) || !strings.Contains(err.Error(), "event id is at most") {
				t.Fatalf("%q on %s: %v", statements, st.Node(), err)
			}
		}
	}
	// Up to the bound, the next id is the largest plus one on every node.
	batch := Batch{Schema: SchemaVersion, Version: a.state.Load().version, Statements: []Statement{
		{SQL: `INSERT INTO events (id, at, actor, action, tenant, app, detail) VALUES (4611686018427387904, '', '', '', '', '', '')`},
		{SQL: `INSERT INTO events (at, actor, action, tenant, app, detail) VALUES ('', '', '', '', '', 'second')`},
	}}
	if got := applyAlike(t, 20, batch, a, b); got[0] != got[1] {
		t.Fatalf("the nodes differ: %q", got)
	}
	var x, y int64
	if err := a.db.QueryRow(`SELECT id FROM events WHERE detail = 'second'`).Scan(&x); err != nil {
		t.Fatal(err)
	}
	if err := b.db.QueryRow(`SELECT id FROM events WHERE detail = 'second'`).Scan(&y); err != nil {
		t.Fatal(err)
	}
	if x != y || x != 4611686018427387905 {
		t.Fatalf("event ids %d and %d", x, y)
	}
}

// Dropping a departure drops that one: a later departure of the name, after
// the node came back and was taken out again, stays, with the same key or
// another.
func TestDroppingADepartureKeepsALaterOne(t *testing.T) {
	st := openNamed(t, "leader")
	const name = "returning"
	for _, key := range []string{testKey(2), testKey(1)} {
		if err := st.PutNode(Node{Name: name, Address: "127.0.0.1:7400", Key: testKey(1)}, "root"); err != nil {
			t.Fatal(err)
		}
		if err := st.RemoveNode(name, "root"); err != nil {
			t.Fatal(err)
		}
		seen, ok := st.Departure(name)
		if !ok {
			t.Fatal("no departure")
		}
		// The node comes back, with key, and is taken out again, before
		// the departure seen first is dropped.
		if err := st.Admit(name, key, time.Now().Add(time.Hour), noVoters, "root"); err != nil {
			t.Fatal(err)
		}
		if err := st.AddNode(Node{Name: name, Address: "127.0.0.1:7400", Key: key}, noVoters, "root"); err != nil {
			t.Fatal(err)
		}
		if err := st.RemoveNode(name, "root"); err != nil {
			t.Fatal(err)
		}
		if err := st.ForgetDeparted(seen, "murder"); err != nil {
			t.Fatal(err)
		}
		if departure, ok := st.Departure(name); !ok || departure.Key != key || departure == seen {
			t.Fatalf("the later departure went: %+v, %v", departure, ok)
		}
		latest, _ := st.Departure(name)
		if err := st.ForgetDeparted(latest, "murder"); err != nil {
			t.Fatal(err)
		}
		if _, ok := st.Departure(name); ok {
			t.Fatal("the departure dropped stayed")
		}
	}
}

// A node given a new key, in place of one it lost, keeps everything else:
// its row as it was, the tenants that name it. Not while changes are
// frozen; and no statement but the store's own changes a key in place.
func TestANewKeyKeepsTheNodesPlace(t *testing.T) {
	st := openNamed(t, "a")
	if err := st.PutNode(Node{Name: "n2", Address: "10.0.0.2:7400", Key: testKey(1)}, "root"); err != nil {
		t.Fatal(err)
	}
	if err := st.SetPublic("n2", []netip.Addr{netip.MustParseAddr("203.0.113.2")}); err != nil {
		t.Fatal(err)
	}
	if err := st.Away("n2", time.Date(2030, 1, 1, 0, 0, 0, 0, time.UTC)); err != nil {
		t.Fatal(err)
	}
	if _, err := st.PutApp(App{Name: "web", Spec: appSpec()}, "root"); err != nil {
		t.Fatal(err)
	}
	if _, err := st.Create(NewTenant{Name: "acme", App: "web", Node: "n2"}, "root"); err != nil {
		t.Fatal(err)
	}
	before, _ := st.state.Load().nodes["n2"]
	if err := st.Freeze("upgrade to x", "root"); err != nil {
		t.Fatal(err)
	}
	if err := st.ReplaceNodeKey("n2", testKey(2), "root"); !errors.Is(err, ErrFrozen) {
		t.Fatalf("under a freeze: %v", err)
	}
	if err := st.ThawOwn("upgrade to x", "root"); err != nil {
		t.Fatal(err)
	}
	if err := st.ReplaceNodeKey("n2", testKey(2), "root"); err != nil {
		t.Fatal(err)
	}
	after := st.state.Load().nodes["n2"]
	if after.Key != testKey(2) {
		t.Fatalf("key %q", after.Key)
	}
	after.Key = before.Key
	if !reflect.DeepEqual(after, before) {
		t.Fatalf("the row changed:\n%+v\n%+v", before, after)
	}
	if tenant, err := st.Get("acme"); err != nil || tenant.Node != "n2" {
		t.Fatalf("tenant %+v, %v", tenant, err)
	}
	if err := st.ReplaceNodeKey("n9", testKey(2), "root"); !errors.Is(err, ErrNotFound) {
		t.Fatalf("an unknown node: %v", err)
	}
	recorded := &recorder{}
	_ = recorded.Exec(`UPDATE nodes SET key = ? WHERE name = 'n2'`, testKey(3))
	if err := st.Apply(Batch{Schema: SchemaVersion, Version: st.state.Load().version, Statements: recorded.statements}); !Refused(err) {
		t.Fatalf("a key changed in place: %v", err)
	}
}

// A state of schema 15 that holds an event of an id past the bound gets it
// below the bound as it upgrades, the same on every node, so the next
// event's id is the same everywhere.
func TestAnUpgradeBringsEventIDsBelowTheBound(t *testing.T) {
	a, b := openNamed(t, "a"), openNamed(t, "b")
	for _, st := range []*SQLite{a, b} {
		for _, statement := range []string{
			`DROP TRIGGER events_id_bounded`, `DROP TRIGGER events_id_kept_bounded`,
			`DROP TRIGGER nodes_key_kept`, `DROP TRIGGER nodes_key_not_replaced`,
			`DROP TABLE admissions`, `DROP TABLE departed_keys`, `ALTER TABLE nodes DROP COLUMN key`,
			`DROP TABLE acme_challenges`, `DROP TABLE acme_accounts`, `DROP TABLE domains`, `DROP TABLE operation_lease`, `ALTER TABLE tenants DROP COLUMN canonical`, `DROP TABLE acme_revocations`, `ALTER TABLE rollouts DROP COLUMN first`, `PRAGMA user_version = 15`,
			`INSERT INTO events (id, at, actor, action, tenant, app, detail) VALUES (9223372036854775807, '', '', '', '', '', 'legacy')`,
			`INSERT INTO events (id, at, actor, action, tenant, app, detail) VALUES (5000000000000000000, '', '', '', '', '', 'older')`,
		} {
			if _, err := st.db.Exec(statement); err != nil {
				t.Fatal(err)
			}
		}
		if err := upgrade(st.db); err != nil {
			t.Fatal(err)
		}
	}
	batch := Batch{Schema: SchemaVersion, Version: a.state.Load().version, Statements: []Statement{{SQL: `INSERT INTO events (at, actor, action, tenant, app, detail) VALUES ('', '', '', '', '', 'probe')`}}}
	if got := applyAlike(t, 10, batch, a, b); got[0] == "refused" || got[0] != got[1] {
		t.Fatalf("the probe: %q", got)
	}
	ids := func(st *SQLite) []int64 {
		rows, err := st.db.Query(`SELECT id FROM events WHERE detail IN ('older', 'legacy', 'probe') ORDER BY id`)
		if err != nil {
			t.Fatal(err)
		}
		defer rows.Close()
		var all []int64
		for rows.Next() {
			var id int64
			_ = rows.Scan(&id)
			all = append(all, id)
		}
		return all
	}
	x, y := ids(a), ids(b)
	if !slices.Equal(x, y) || len(x) != 3 || x[2] > maxEventID {
		t.Fatalf("event ids %v and %v", x, y)
	}
	// The old order stays: older, legacy, then the probe.
	var details []string
	rows, _ := a.db.Query(`SELECT detail FROM events WHERE detail IN ('older', 'legacy', 'probe') ORDER BY id`)
	for rows.Next() {
		var detail string
		_ = rows.Scan(&detail)
		details = append(details, detail)
	}
	rows.Close()
	if !slices.Equal(details, []string{"older", "legacy", "probe"}) {
		t.Fatalf("order %v", details)
	}
}

// legacy makes st's database one of schema 15, as a node of the release
// before keys left it.
func legacy(t *testing.T, st *SQLite) {
	t.Helper()
	for _, statement := range []string{
		`DROP TRIGGER events_id_bounded`, `DROP TRIGGER events_id_kept_bounded`,
		`DROP TRIGGER nodes_key_kept`, `DROP TRIGGER nodes_key_not_replaced`,
		`DROP TABLE admissions`, `DROP TABLE departed_keys`, `ALTER TABLE nodes DROP COLUMN key`,
		`DROP TABLE acme_challenges`, `DROP TABLE acme_accounts`, `DROP TABLE domains`, `DROP TABLE operation_lease`, `ALTER TABLE tenants DROP COLUMN canonical`, `DROP TABLE acme_revocations`, `ALTER TABLE rollouts DROP COLUMN first`, `PRAGMA user_version = 15`,
	} {
		if _, err := st.db.Exec(statement); err != nil {
			t.Fatal(err)
		}
	}
}

// eventIDsOf lists st's events' ids and details, in id order.
func eventIDsOf(t *testing.T, st *SQLite) []string {
	t.Helper()
	rows, err := st.db.Query(`SELECT id, detail FROM events ORDER BY id`)
	if err != nil {
		t.Fatal(err)
	}
	defer rows.Close()
	var all []string
	for rows.Next() {
		var id int64
		var detail string
		_ = rows.Scan(&id, &detail)
		all = append(all, fmt.Sprintf("%d %s", id, detail))
	}
	return all
}

// The ids events past the bound get come after the largest one below it,
// or, once those run out, are the smallest free ones.
func TestEventIDsPastTheBoundFindRoomBelowIt(t *testing.T) {
	for _, ids := range [][]int64{
		{maxEventID, maxEventID + 1, maxEventID + 2},
		{1, 2, 4, maxEventID, maxEventID + 1, maxEventID + 2},
		{7, maxEventID + 5, 9223372036854775807},
	} {
		st := openNamed(t, "a")
		legacy(t, st)
		for _, id := range ids {
			if _, err := st.db.Exec(`INSERT INTO events (id, at, actor, action, tenant, app, detail) VALUES (?, '', '', '', '', '', ?)`, id, fmt.Sprint(id)); err != nil {
				t.Fatal(err)
			}
		}
		if err := upgrade(st.db); err != nil {
			t.Fatalf("%v: %v", ids, err)
		}
		var high int
		if err := st.db.QueryRow(`SELECT count(*) FROM events WHERE id > ?`, maxEventID).Scan(&high); err != nil || high != 0 {
			t.Fatalf("%v: %d past the bound, %v", ids, high, err)
		}
		t.Logf("%v: %v", ids, eventIDsOf(t, st))
	}
	st := openNamed(t, "b")
	legacy(t, st)
	for _, id := range []int64{1, 2, 4, maxEventID, maxEventID + 1, maxEventID + 2} {
		if _, err := st.db.Exec(`INSERT INTO events (id, at, actor, action, tenant, app, detail) VALUES (?, '', '', '', '', '', ?)`, id, fmt.Sprint(id)); err != nil {
			t.Fatal(err)
		}
	}
	if err := upgrade(st.db); err != nil {
		t.Fatal(err)
	}
	want := []string{"1 1", "2 2", "3 4611686018427387905", "4 4", "5 4611686018427387906", fmt.Sprintf("%d %d", maxEventID, maxEventID)}
	if got := eventIDsOf(t, st); !slices.Equal(got, want) {
		t.Fatalf("ids %v, want %v", got, want)
	}
}

// A snapshot of an older release with events past the bound installs, and
// the node that installed it holds the same events, ids and all, as a node
// that upgraded the same state in place.
func TestASnapshotOfAnOlderReleaseGetsTheSameEventIDsAsAnUpgrade(t *testing.T) {
	source, migrated, installed := openNamed(t, "source"), openNamed(t, "migrated"), openNamed(t, "installed")
	for _, st := range []*SQLite{source, migrated} {
		legacy(t, st)
		for _, id := range []int64{3, maxEventID, maxEventID + 9, 9223372036854775807} {
			if _, err := st.db.Exec(`INSERT INTO events (id, at, actor, action, tenant, app, detail) VALUES (?, '', '', '', '', '', ?)`, id, fmt.Sprint(id)); err != nil {
				t.Fatal(err)
			}
		}
		if _, err := st.db.Exec(`UPDATE meta SET value = '10' WHERE key = 'applied'`); err != nil {
			t.Fatal(err)
		}
	}
	path, err := source.SnapshotFile()
	if err != nil {
		t.Fatal(err)
	}
	defer removeDatabase(path)
	file, err := os.Open(path)
	if err != nil {
		t.Fatal(err)
	}
	defer file.Close()
	if ok, err := installed.InstallSnapshot(file); err != nil || !ok {
		t.Fatalf("install a schema 15 snapshot with events past the bound: %v, %v", ok, err)
	}
	if err := upgrade(migrated.db); err != nil {
		t.Fatal(err)
	}
	if got, want := eventIDsOf(t, installed), eventIDsOf(t, migrated); !slices.Equal(got, want) {
		t.Fatalf("installed %v, migrated %v", got, want)
	}
}

// Every node holds the same meta keys, which a batch may read: a new
// database, one upgraded, one that installed a snapshot. The keys only
// one node has are none; a rebuild's note is a file of its own.
func TestEveryNodeHoldsTheSameMetaKeys(t *testing.T) {
	keys := func(st *SQLite) []string {
		rows, err := st.db.Query(`SELECT key FROM meta WHERE key NOT IN ('cluster', 'frozen') ORDER BY key`)
		if err != nil {
			t.Fatal(err)
		}
		defer rows.Close()
		var all []string
		for rows.Next() {
			var key string
			_ = rows.Scan(&key)
			all = append(all, key)
		}
		return all
	}
	fresh := openNamed(t, "fresh")
	upgraded := openNamed(t, "upgraded")
	legacy(t, upgraded)
	if err := upgrade(upgraded.db); err != nil {
		t.Fatal(err)
	}
	installed := openNamed(t, "installed")
	logged(t, fresh)
	if err := fresh.PutNode(Node{Name: "fresh", Address: "10.0.0.1:7400"}, "root"); err != nil {
		t.Fatal(err)
	}
	path, err := fresh.SnapshotFile()
	if err != nil {
		t.Fatal(err)
	}
	defer removeDatabase(path)
	file, err := os.Open(path)
	if err != nil {
		t.Fatal(err)
	}
	defer file.Close()
	if ok, err := installed.InstallSnapshot(file); err != nil || !ok {
		t.Fatalf("install: %v, %v", ok, err)
	}
	want := []string{"applied", "node", "version"}
	for _, st := range []*SQLite{fresh, upgraded, installed} {
		if got := keys(st); !slices.Equal(got, want) {
			t.Fatalf("%s holds meta keys %v", st.Node(), got)
		}
	}
	// So a batch reading meta's keys does the same on every node.
	a, b := openNamed(t, "a"), openNamed(t, "b")
	for i, statement := range []string{
		`INSERT INTO panel_secrets (name, value) SELECT 'probe', 'x' FROM meta WHERE key = 'rebuilding'`,
		`DELETE FROM meta WHERE key = 'rebuilding'`,
	} {
		batch := Batch{Schema: SchemaVersion, Version: a.state.Load().version, Statements: []Statement{{SQL: statement}}}
		if got := applyAlike(t, uint64(100+i), batch, a, b); got[0] != got[1] {
			t.Fatalf("%s: %q", statement, got)
		}
	}
}

// Giving ids below the bound costs the ids there and the events past it,
// not their product: a hundred thousand events below, a thousand past it.
func TestEventIDsBelowTheBoundAreFoundInOnePass(t *testing.T) {
	st := openNamed(t, "a")
	legacy(t, st)
	tx, err := st.db.Begin()
	if err != nil {
		t.Fatal(err)
	}
	defer tx.Rollback()
	if _, err := tx.Exec(`WITH RECURSIVE n(i) AS (VALUES (1) UNION ALL SELECT i + 1 FROM n WHERE i < 100000) INSERT INTO events (id, at, actor, action, tenant, app, detail) SELECT i, '', '', '', '', '', '' FROM n`); err != nil {
		t.Fatal(err)
	}
	if _, err := tx.Exec(`DELETE FROM events WHERE id IN (500, 50000)`); err != nil {
		t.Fatal(err)
	}
	if _, err := tx.Exec(`INSERT INTO events (id, at, actor, action, tenant, app, detail) VALUES (?, '', '', '', '', '', '')`, maxEventID); err != nil {
		t.Fatal(err)
	}
	ids, err := newEventIDs(tx, "events")
	if err != nil {
		t.Fatal(err)
	}
	start := time.Now()
	var taken []int64
	for range 1000 {
		id, err := ids.take()
		if err != nil {
			t.Fatal(err)
		}
		if _, err := tx.Exec(`INSERT INTO events (id, at, actor, action, tenant, app, detail) VALUES (?, '', '', '', '', '', '')`, id); err != nil {
			t.Fatal(err)
		}
		taken = append(taken, id)
	}
	if elapsed := time.Since(start); elapsed > 5*time.Second {
		t.Fatalf("1000 ids over 100000 events took %v", elapsed)
	}
	// The free ids from 1 up: the two gaps, then past the hundred thousand.
	if taken[0] != 500 || taken[1] != 50000 || taken[2] != 100001 || taken[999] != 100998 {
		t.Fatalf("ids %v … %v", taken[:3], taken[997:])
	}
}
