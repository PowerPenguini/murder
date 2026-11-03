package store

import (
	"database/sql"
	"errors"
	"path/filepath"
	"reflect"
	"testing"

	"github.com/PowerPenguini/murder/internal/spec"
)

// replicate makes every batch st commits pass through JSON to replica too,
// as a cluster's log would.
func replicate(t *testing.T, st, replica *SQLite) {
	t.Helper()
	st.commit = func(batch Batch) (*snapshot, error) {
		data, err := EncodeBatch(batch)
		if err != nil {
			return nil, err
		}
		decoded, err := DecodeBatch(data)
		if err != nil {
			return nil, err
		}
		next, err := st.apply(decoded)
		if err != nil {
			return nil, err
		}
		if err := replica.Apply(decoded); err != nil {
			t.Errorf("the replica refused a batch the origin applied: %v", err)
		}
		return next, nil
	}
}

// replicated is everything that must be equal on every node.
func replicated(t *testing.T, st *SQLite) (int64, []App, []Record, []Event, map[string]map[string]string) {
	t.Helper()
	events, err := st.Events(EventQuery{Limit: 1000})
	if err != nil {
		t.Fatal(err)
	}
	sealed := map[string]map[string]string{}
	records := st.List()
	for i := range records {
		sealed[records[i].Spec.Name] = records[i].Sealed
	}
	return st.state.Load().version, st.Apps(), records, events, sealed
}

func TestBatchesGiveEveryNodeTheSameState(t *testing.T) {
	origin, _ := openNew(t, options())
	replicaPath := filepath.Join(t.TempDir(), "murder.db")
	if err := Init(replicaPath, "node-2"); err != nil {
		t.Fatal(err)
	}
	replica, err := Open(replicaPath, Options{NetworkPool: options().NetworkPool, Sealer: func(string, string, spec.Secret) (string, error) {
		return "", errors.New("a replica never seals: values come in the batch")
	}})
	if err != nil {
		t.Fatal(err)
	}
	defer replica.Close()
	replicate(t, origin, replica)

	if _, err := origin.PutApp(App{Name: "web", Spec: appSpec()}, "alice"); err != nil {
		t.Fatal(err)
	}
	if _, err := origin.Create(NewTenant{Name: "acme", App: "web", Overrides: spec.Overrides{Env: map[string]string{"MODE": "x"}}}, "alice"); err != nil {
		t.Fatal(err)
	}
	if _, err := origin.Create(NewTenant{Name: "beta", App: "web"}, "bob"); err != nil {
		t.Fatal(err)
	}
	if _, err := origin.SetSuspended("beta", true, "bob"); err != nil {
		t.Fatal(err)
	}
	if _, err := origin.Rotate("acme", "SESSION", "alice"); err != nil {
		t.Fatal(err)
	}
	if err := origin.Observe("acme", image); err != nil {
		t.Fatal(err)
	}
	next := appSpec()
	next.Image = otherImage
	if _, err := origin.PutApp(App{Name: "web", Spec: next}, "alice"); err != nil {
		t.Fatal(err)
	}
	if err := origin.Archive("beta", "bob"); err != nil {
		t.Fatal(err)
	}
	if err := origin.Forget("beta", "bob"); err != nil {
		t.Fatal(err)
	}

	version, apps, records, events, sealed := replicated(t, origin)
	if version != 9 {
		t.Errorf("version = %d after 9 writes", version)
	}
	rVersion, rApps, rRecords, rEvents, rSealed := replicated(t, replica)
	if version != rVersion || !reflect.DeepEqual(apps, rApps) || !reflect.DeepEqual(records, rRecords) ||
		!reflect.DeepEqual(events, rEvents) || !reflect.DeepEqual(sealed, rSealed) {
		t.Fatalf("the replica differs:\norigin  %d %+v %+v\nreplica %d %+v %+v", version, records, events, rVersion, rRecords, rEvents)
	}
	if origin.Node() == replica.Node() {
		t.Error("the node name was replicated")
	}

	// A failed write leaves both alone.
	if _, err := origin.Create(NewTenant{Name: "acme", App: "web"}, "alice"); !errors.Is(err, ErrExists) {
		t.Fatalf("create twice: %v", err)
	}
	if replica.state.Load().version != 9 {
		t.Error("a refused write reached the replica")
	}
}

func TestStaleBatchesChangeNothing(t *testing.T) {
	st, _ := withApp(t, options())
	before := st.state.Load()
	recorded := &recorder{}
	if err := recorded.Exec(`DELETE FROM apps`); err != nil {
		t.Fatal(err)
	}
	for _, version := range []int64{before.version - 1, before.version + 1} {
		err := st.Apply(Batch{Schema: schemaVersion, Version: version, Statements: recorded.statements})
		if !errors.Is(err, ErrStale) {
			t.Fatalf("apply at version %d of %d: %v", version, before.version, err)
		}
	}
	if err := st.Apply(Batch{Schema: schemaVersion + 1, Version: before.version, Statements: recorded.statements}); !errors.Is(err, ErrSchema) {
		t.Fatalf("apply a newer schema's batch: %v", err)
	}
	if after := st.state.Load(); after.version != before.version || len(st.Apps()) != 1 {
		t.Fatalf("a refused batch changed the state: version %d → %d, apps %v", before.version, after.version, st.Apps())
	}
}

func TestAWriteBeatenByAnotherNodeIsComputedAgain(t *testing.T) {
	st, _ := withApp(t, options())
	// Another node creates acme first; a write computed before it saw that
	// must not be applied as computed.
	other := &recorder{}
	if err := other.Exec(`INSERT INTO tenants (name, app, overrides, suspended, subnet, data_image, previous_image, generation, created_at, updated_at, node)
		VALUES ('acme', 'web', '{}', 0, '10.240.0.0/28', '', '', 1, '2026-10-30T00:00:00Z', '2026-10-30T00:00:00Z', 'node-1')`); err != nil {
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
	created, err := st.Create(NewTenant{Name: "beta", App: "web"}, "alice")
	if err != nil {
		t.Fatal(err)
	}
	if commits != 2 || created.Subnet != "10.240.0.16/28" {
		t.Fatalf("after %d commits beta got %s, the subnet of a tenant created meanwhile", commits, created.Subnet)
	}
	commits = 0
	if _, err := st.Create(NewTenant{Name: "acme", App: "web"}, "alice"); !errors.Is(err, ErrExists) {
		t.Fatalf("a tenant created meanwhile: %v", err)
	}

	// Forever stale: given up on, not retried without end.
	st.commit = func(Batch) (*snapshot, error) { commits++; return nil, ErrStale }
	commits = 0
	if _, err := st.Create(NewTenant{Name: "gamma", App: "web"}, "alice"); !errors.Is(err, ErrStale) || commits != staleAttempts {
		t.Fatalf("after %d commits: %v", commits, err)
	}
}

func TestArgumentsKeepTheirTypes(t *testing.T) {
	recorded := &recorder{}
	if err := recorded.Exec(`SELECT ?, ?, ?, ?, ?`, "text", 7, int64(-3), true, nil); err != nil {
		t.Fatal(err)
	}
	if err := recorded.Exec(`SELECT ?`, 1.5); err == nil {
		t.Fatal("a float argument was recorded")
	}
	data, err := EncodeBatch(Batch{Statements: recorded.statements})
	if err != nil {
		t.Fatal(err)
	}
	batch, err := DecodeBatch(data)
	if err != nil {
		t.Fatal(err)
	}
	values, err := batch.Statements[0].values()
	if err != nil {
		t.Fatal(err)
	}
	if want := []any{"text", int64(7), int64(-3), true, nil}; !reflect.DeepEqual(values, want) {
		t.Fatalf("values = %#v, want %#v", values, want)
	}
	if _, err := (Statement{Args: []Arg{{Text: new("a"), Int: new(int64(1))}}}).values(); err == nil {
		t.Fatal("an argument with two values was accepted")
	}
}

func TestOpenUpgradesSchemaVersion4(t *testing.T) {
	path := filepath.Join(t.TempDir(), "murder.db")
	db, err := sql.Open("sqlite", initDSN(path))
	if err != nil {
		t.Fatal(err)
	}
	// Version 4 is today's schema without the state version and nodes.
	if _, err := db.Exec(schema); err != nil {
		t.Fatal(err)
	}
	for _, statement := range []string{`ALTER TABLE tenants DROP COLUMN app_revision`, `ALTER TABLE tenants DROP COLUMN before_revision`, `ALTER TABLE tenants DROP COLUMN canary`, `ALTER TABLE tenants DROP COLUMN hold`, `ALTER TABLE tenants DROP COLUMN restore`, `ALTER TABLE tenants DROP COLUMN served`, `ALTER TABLE tenants DROP COLUMN moved_on`, `DROP TABLE rollouts`, `DROP TABLE app_revisions`, `ALTER TABLE apps DROP COLUMN tag`, `ALTER TABLE apps DROP COLUMN revision`, `ALTER TABLE apps DROP COLUMN latest`, `ALTER TABLE apps DROP COLUMN latest_at`, `ALTER TABLE nodes DROP COLUMN failed_at`, `ALTER TABLE nodes DROP COLUMN away_until`, `ALTER TABLE nodes DROP COLUMN copies_refused_until`, `DROP TABLE nodes`, `DROP TABLE devices`, `DROP TABLE panel_sessions`, `DROP TABLE panel_enrollments`, `DROP TABLE panel_passkeys`, `DROP TABLE panel_users`, `DROP TABLE panel_secrets`, `DROP INDEX tenants_node`, `ALTER TABLE tenants DROP COLUMN node`, `ALTER TABLE tenants DROP COLUMN rollout`, `ALTER TABLE tenants DROP COLUMN move_to`, `ALTER TABLE tenants DROP COLUMN move_held`, `ALTER TABLE tenants DROP COLUMN replica`, `ALTER TABLE tenants DROP COLUMN replica_ready`, `ALTER TABLE tenants DROP COLUMN replica_next`} {
		if _, err := db.Exec(statement); err != nil {
			t.Fatal(err)
		}
	}
	if _, err := db.Exec(`INSERT INTO meta (key, value) VALUES ('node', 'old')`); err != nil {
		t.Fatal(err)
	}
	if _, err := db.Exec(`INSERT INTO apps (name, spec, created_at, updated_at) VALUES ('web', ?, '2026-10-01T00:00:00Z', '2026-10-01T00:00:00Z')`, marshal(normalizeApp(appSpec()))); err != nil {
		t.Fatal(err)
	}
	if _, err := db.Exec(`INSERT INTO tenants (name, app, overrides, suspended, subnet, data_image, previous_image, generation, created_at, updated_at)
		VALUES ('old', 'web', '{}', 0, '10.240.0.0/28', '', '', 1, '2026-10-01T00:00:00Z', '2026-10-01T00:00:00Z')`); err != nil {
		t.Fatal(err)
	}
	if _, err := db.Exec(`PRAGMA user_version = 4`); err != nil {
		t.Fatal(err)
	}
	if err := db.Close(); err != nil {
		t.Fatal(err)
	}
	if subnets, err := Subnets(path); err != nil || len(subnets) != 1 {
		t.Fatalf("the firewall cannot read a version 4 database: %v %v", subnets, err)
	}
	st, err := Open(path, options())
	if err != nil {
		t.Fatal(err)
	}
	defer st.Close()
	if st.Node() != "old" || st.state.Load().version != 0 || len(st.Apps()) != 1 || st.List()[0].Node != "old" {
		t.Fatalf("upgraded state: node %s, version %d, apps %v", st.Node(), st.state.Load().version, st.Apps())
	}
	if _, err := st.Create(NewTenant{Name: "acme", App: "web"}, "alice"); err != nil {
		t.Fatal(err)
	}
	var version int
	if err := st.db.QueryRow(`PRAGMA user_version`).Scan(&version); err != nil || version != schemaVersion {
		t.Fatalf("user_version = %d, %v", version, err)
	}
	if st.state.Load().version != 1 {
		t.Fatalf("version after one write = %d", st.state.Load().version)
	}
}

func TestOpenUpgradesSchemaVersion5(t *testing.T) {
	path := filepath.Join(t.TempDir(), "murder.db")
	db, err := sql.Open("sqlite", initDSN(path))
	if err != nil {
		t.Fatal(err)
	}
	// Version 5 is today's schema without rollouts.
	for _, statement := range []string{
		schema,
		`ALTER TABLE tenants DROP COLUMN app_revision`,
		`ALTER TABLE tenants DROP COLUMN before_revision`,
		`ALTER TABLE tenants DROP COLUMN canary`,
		`ALTER TABLE tenants DROP COLUMN hold`,
		`ALTER TABLE tenants DROP COLUMN restore`, `ALTER TABLE tenants DROP COLUMN served`, `ALTER TABLE tenants DROP COLUMN moved_on`,
		`DROP TABLE rollouts`,
		`DROP TABLE app_revisions`,
		`ALTER TABLE apps DROP COLUMN tag`,
		`ALTER TABLE apps DROP COLUMN revision`,
		`ALTER TABLE apps DROP COLUMN latest`,
		`ALTER TABLE apps DROP COLUMN latest_at`,
		`ALTER TABLE tenants DROP COLUMN rollout`,
		`ALTER TABLE tenants DROP COLUMN move_to`,
		`ALTER TABLE tenants DROP COLUMN move_held`,
		`ALTER TABLE tenants DROP COLUMN replica`,
		`ALTER TABLE tenants DROP COLUMN replica_ready`,
		`ALTER TABLE tenants DROP COLUMN replica_next`,
		`ALTER TABLE nodes DROP COLUMN failed_at`,
		`ALTER TABLE nodes DROP COLUMN away_until`,
		`ALTER TABLE nodes DROP COLUMN copies_refused_until`,
		`ALTER TABLE nodes DROP COLUMN public`,
		`ALTER TABLE nodes DROP COLUMN tunnel`,
		`DROP TABLE devices`,
		`DROP TABLE panel_sessions`,
		`DROP TABLE panel_enrollments`,
		`DROP TABLE panel_passkeys`,
		`DROP TABLE panel_users`,
		`DROP TABLE panel_secrets`,
		`INSERT INTO meta (key, value) VALUES ('node', 'n1'), ('version', '2'), ('applied', '7')`,
		`INSERT INTO apps (name, spec, created_at, updated_at) VALUES ('web', '` + marshal(normalizeApp(appSpec())) + `', '2026-10-01T00:00:00Z', '2026-10-01T00:00:00Z')`,
		`INSERT INTO tenants (name, app, overrides, suspended, subnet, data_image, previous_image, generation, created_at, updated_at, node)
			VALUES ('acme', 'web', '{}', 0, '10.240.0.0/28', '', '', 1, '2026-10-01T00:00:00Z', '2026-10-01T00:00:00Z', 'n1')`,
		`PRAGMA user_version = 5`,
	} {
		if _, err := db.Exec(statement); err != nil {
			t.Fatal(err)
		}
	}
	if err := db.Close(); err != nil {
		t.Fatal(err)
	}
	st, err := Open(path, options())
	if err != nil {
		t.Fatal(err)
	}
	defer st.Close()
	if st.Applied() != 7 || st.state.Load().version != 2 {
		t.Fatalf("applied %d, version %d", st.Applied(), st.state.Load().version)
	}
	rolled, err := st.Roll("acme", "alice")
	if err != nil || rolled.Rollout != 1 {
		t.Fatalf("roll after the upgrade: %+v, %v", rolled, err)
	}
	// A batch of a version 5 node still applies here.
	recorded := &recorder{}
	_ = recorded.Exec(`UPDATE tenants SET suspended = 1 WHERE name = 'acme'`)
	if err := st.Apply(Batch{Schema: 5, Version: st.state.Load().version, Statements: recorded.statements}); err != nil {
		t.Fatal(err)
	}
}
