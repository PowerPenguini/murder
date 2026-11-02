package store

import (
	"context"
	"database/sql"
	"errors"
	"path/filepath"
	"testing"
)

func digestOf(t *testing.T, st *SQLite) Digest {
	t.Helper()
	digest, err := st.Digest(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	return digest
}

// Nodes that applied the same batches hash their states alike; what
// belongs to one node does not count, and a change made around the
// batches shows.
func TestDigestsOfTheSameStateAreTheSame(t *testing.T) {
	origin, originPath := openNew(t, options())
	replicaPath := filepath.Join(t.TempDir(), "murder.db")
	if err := Init(replicaPath, "node-2"); err != nil {
		t.Fatal(err)
	}
	replica, err := Open(replicaPath, options())
	if err != nil {
		t.Fatal(err)
	}
	defer replica.Close()
	replicate(t, origin, replica)
	if _, err := origin.PutApp(App{Name: "web", Spec: appSpec()}, "alice"); err != nil {
		t.Fatal(err)
	}
	if _, err := origin.Create(NewTenant{Name: "acme", App: "web"}, "alice"); err != nil {
		t.Fatal(err)
	}
	if err := origin.Observe("acme", image); err != nil {
		t.Fatal(err)
	}
	ours, theirs := digestOf(t, origin), digestOf(t, replica)
	if !ours.Comparable(theirs) || ours.Hash != theirs.Hash || ours.Version != 3 || ours.Schema != schemaVersion {
		t.Fatalf("digests of one state: %+v and %+v", ours, theirs)
	}
	// Images are each node's own.
	if err := replica.ForgetImage(image); err != nil {
		t.Fatal(err)
	}
	if again := digestOf(t, replica); again.Hash != ours.Hash {
		t.Fatal("a node's own images changed its digest")
	}
	// A write changes the version and the digest.
	if _, err := origin.SetSuspended("acme", true, "alice"); err != nil {
		t.Fatal(err)
	}
	after := digestOf(t, origin)
	if after.Version != 4 || after.Hash == ours.Hash || after.Comparable(ours) {
		t.Fatalf("after a write: %+v, before %+v", after, ours)
	}
	if digestOf(t, replica).Hash != after.Hash {
		t.Fatal("the replica's digest after the same write differs")
	}
	// The freeze is part of the state.
	frozen := digestOf(t, origin)
	if err := origin.Freeze("upgrade to x", "alice"); err != nil {
		t.Fatal(err)
	}
	if err := origin.Thaw("alice"); err != nil {
		t.Fatal(err)
	}
	after = digestOf(t, origin)
	if digestOf(t, replica).Hash != after.Hash || after.Hash == frozen.Hash {
		t.Fatal("a freeze and a thaw hash alike on both, and change the state")
	}
	db0, err := sql.Open("sqlite", dsn(originPath))
	if err != nil {
		t.Fatal(err)
	}
	defer db0.Close()
	if _, err := db0.Exec(`INSERT OR REPLACE INTO meta (key, value) VALUES ('cluster', 'drift')`); err != nil {
		t.Fatal(err)
	}
	if digestOf(t, origin).Hash == after.Hash {
		t.Fatal("the cluster's name is not hashed")
	}
	if _, err := db0.Exec(`DELETE FROM meta WHERE key = 'cluster'`); err != nil {
		t.Fatal(err)
	}
	// A change no batch made shows, though the version is the same.
	db, err := sql.Open("sqlite", dsn(originPath))
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	if _, err := db.Exec(`UPDATE tenants SET node = 'elsewhere' WHERE name = 'acme'`); err != nil {
		t.Fatal(err)
	}
	drifted := digestOf(t, origin)
	if !drifted.Comparable(after) || drifted.Hash == after.Hash {
		t.Fatalf("a drifted state hashes as %+v, the replica's as %+v", drifted, after)
	}
}

func TestDigestsOfOtherSchemasAreNotCompared(t *testing.T) {
	a := Digest{Version: 3, Schema: 13, Format: 1, Hash: "x"}
	for _, b := range []Digest{{Version: 3, Schema: 14, Format: 1, Hash: "x"}, {Version: 4, Schema: 13, Format: 1, Hash: "x"}, {Version: 3, Schema: 13, Format: 2, Hash: "x"}, {Version: 3, Schema: 13, Format: 1}} {
		if a.Comparable(b) {
			t.Fatalf("%+v compared with %+v", a, b)
		}
	}
	if !a.Comparable(Digest{Version: 3, Schema: 13, Format: 1, Hash: "y"}) {
		t.Fatal("digests of one version and schema not compared")
	}
}

// A statement whose result may differ between nodes is refused, when a
// write records it and when the leader checks a batch from another node.
func TestNondeterministicStatementsAreRefused(t *testing.T) {
	for _, query := range []string{
		`UPDATE tenants SET updated_at = datetime('now')`,
		`UPDATE tenants SET updated_at = CURRENT_TIMESTAMP`,
		`INSERT INTO events (at, actor, action, tenant, app, detail) VALUES (?, ?, ?, ?, ?, hex(randomblob(4)))`,
		`UPDATE tenants SET generation = RANDOM ()`,
		`INSERT INTO meta (key, value) VALUES ('x', last_insert_rowid())`,
		`UPDATE meta SET value = sqlite_version() WHERE key = 'x'`,
		`UPDATE tenants SET updated_at = date()`,
		`UPDATE tenants SET updated_at = strftime('%s')`,
		`UPDATE tenants SET updated_at = datetime(?, 'localtime')`,
		`UPDATE tenants SET updated_at = datetime("now")`,
		`UPDATE tenants SET generation = random/**/()`,
		`DELETE FROM events WHERE id IN (SELECT id FROM events LIMIT 10)`,
		`DELETE FROM tenants WHERE rowid = 1`,
	} {
		var tx recorder
		if err := tx.Exec(query); !errors.Is(err, ErrNondeterministic) {
			t.Errorf("recorded %s: %v", query, err)
		}
		batch := Batch{Schema: schemaVersion, Statements: []Statement{{SQL: query}}}
		if err := batch.Check(); !errors.Is(err, ErrNondeterministic) {
			t.Errorf("checked %s: %v", query, err)
		}
	}
	// A LIMIT in a given order picks the same rows everywhere.
	if err := (&recorder{}).Exec(`DELETE FROM events WHERE id IN (SELECT id FROM events ORDER BY id LIMIT 10)`); err != nil {
		t.Fatal(err)
	}
	// A value that only names such a function is an argument, not SQL.
	var tx recorder
	if err := tx.Exec(`UPDATE tenants SET overrides = ? WHERE name = ?`, `{"env":{"WHEN":"now","F":"random()"}}`, "acme"); err != nil {
		t.Fatal(err)
	}
}
