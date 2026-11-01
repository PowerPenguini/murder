package store

import (
	"database/sql"
	"errors"
	"fmt"
)

// schemaVersion is the version of the schema below, kept in PRAGMA
// user_version. Versions 1 to 3 were development releases that never ran in
// production; their databases are refused, not upgraded. Open upgrades
// versions 4 to 8 in place.
const schemaVersion = 9

// oldestSchema is the oldest schema version Open upgrades.
const oldestSchema = 4

// firstBatchSchema is the first schema version of releases that write
// batches. A migration must keep the statements of batches of every
// version since valid: a node upgraded first applies the batches the
// others still write. Freezes, thaws and notes are written as of this
// version, so the meta and events tables must keep its shape.
const firstBatchSchema = 5

const schema = `
-- The node's name, which only this node keeps; the version of the
-- replicated state, the number of batches applied to it; and, in a
-- cluster, the cluster's name and the index of the last log entry applied.
CREATE TABLE meta (
	key   TEXT PRIMARY KEY,
	value TEXT NOT NULL
) STRICT;

CREATE TABLE apps (
	name       TEXT PRIMARY KEY,
	spec       TEXT NOT NULL,
	created_at TEXT NOT NULL,
	updated_at TEXT NOT NULL
) STRICT;

-- Live and archived tenants. A tenant keeps its subnet until it is purged,
-- archived or not, so no other tenant can be routed to a container still
-- running at its address.
CREATE TABLE tenants (
	name        TEXT PRIMARY KEY,
	app         TEXT NOT NULL REFERENCES apps (name),
	overrides   TEXT NOT NULL,
	suspended   INTEGER NOT NULL CHECK (suspended IN (0, 1)),
	subnet      TEXT NOT NULL UNIQUE,
	data_image  TEXT NOT NULL,
	-- The image the data had before data_image, kept for a quick rollback.
	previous_image TEXT NOT NULL,
	generation  INTEGER NOT NULL,
	created_at  TEXT NOT NULL,
	updated_at  TEXT NOT NULL,
	archived_at TEXT,
	-- The node that runs the tenant and keeps its data.
	node        TEXT NOT NULL,
	-- Rollouts asked for: each replaces the container as it is.
	rollout     INTEGER NOT NULL DEFAULT 0,
	-- A move to another node under way: its target, and whether requests
	-- are held because the tenant runs nowhere for a moment.
	move_to     TEXT NOT NULL DEFAULT '',
	move_held   INTEGER NOT NULL DEFAULT 0 CHECK (move_held IN (0, 1)),
	-- The node that keeps a copy of the tenant's data, to run it from if
	-- its node fails; never the tenant's own node. replica_ready is set
	-- once the copy holds the whole data: only then may it take over.
	replica     TEXT NOT NULL DEFAULT '',
	replica_ready INTEGER NOT NULL DEFAULT 0 CHECK (replica_ready IN (0, 1))
) STRICT;

CREATE INDEX tenants_app ON tenants (app);
CREATE INDEX tenants_node ON tenants (node);

-- Every image a tenant was given or ran: the candidates for removal once
-- no tenant needs them. Each node keeps its own, outside the replicated
-- state.
CREATE TABLE images (
	ref TEXT PRIMARY KEY
) STRICT;

-- The nodes of a cluster, which all run tenants.
CREATE TABLE nodes (
	name       TEXT PRIMARY KEY,
	address    TEXT NOT NULL UNIQUE,
	created_at TEXT NOT NULL,
	-- Set when the node's tenants failed over to their replicas, until it
	-- answers again: it is given no copies meanwhile.
	failed_at  TEXT
) STRICT;

-- Sealed secret values, kept until a purge deletes the tenant.
CREATE TABLE secrets (
	tenant     TEXT NOT NULL REFERENCES tenants (name) ON DELETE CASCADE,
	name       TEXT NOT NULL,
	sealed     TEXT NOT NULL,
	created_at TEXT NOT NULL,
	PRIMARY KEY (tenant, name)
) STRICT;

-- The audit log. It outlives the tenants and apps it names.
CREATE TABLE events (
	id     INTEGER PRIMARY KEY,
	at     TEXT NOT NULL,
	actor  TEXT NOT NULL,
	action TEXT NOT NULL,
	tenant TEXT NOT NULL,
	app    TEXT NOT NULL,
	detail TEXT NOT NULL
) STRICT;

CREATE INDEX events_tenant ON events (tenant, id);
CREATE INDEX events_app ON events (app, id);
`

// checkSchema refuses a database this release cannot run on.
func checkSchema(q querier) error {
	var version int
	if err := q.QueryRow(`PRAGMA user_version`).Scan(&version); err != nil {
		return err
	}
	switch {
	case version >= oldestSchema && version <= schemaVersion:
		return nil
	case version == 0:
		var objects int
		if err := q.QueryRow(`SELECT count(*) FROM sqlite_schema`).Scan(&objects); err != nil {
			return err
		}
		if objects > 0 {
			return errors.New("it is not a Murder database")
		}
		return errors.New(`it is not initialized; run "murder init"`)
	case version > schemaVersion:
		return fmt.Errorf("it has schema version %d; this release knows only %d — upgrade Murder", version, schemaVersion)
	default:
		return fmt.Errorf("it has schema version %d of a development release, which cannot be upgraded; create a new one with \"murder init\"", version)
	}
}

// upgrade brings a database of an older schema version to this one, in one
// transaction, one version after the other. Version 5 counts the batches
// and log entries applied to the state and knows the nodes of a cluster.
func upgrade(db *sql.DB) error {
	tx, err := db.Begin()
	if err != nil {
		return err
	}
	defer tx.Rollback()
	var version int
	if err := tx.QueryRow(`PRAGMA user_version`).Scan(&version); err != nil {
		return err
	}
	if version == schemaVersion {
		return nil
	}
	if version == 4 {
		if _, err := tx.Exec(`INSERT INTO meta (key, value) VALUES ('version', '0'), ('applied', '0')`); err != nil {
			return err
		}
		// Every tenant so far runs on the node upgraded.
		if _, err := tx.Exec(`ALTER TABLE tenants ADD COLUMN node TEXT NOT NULL DEFAULT ''`); err != nil {
			return err
		}
		if _, err := tx.Exec(`UPDATE tenants SET node = (SELECT value FROM meta WHERE key = 'node')`); err != nil {
			return err
		}
		if _, err := tx.Exec(`CREATE INDEX tenants_node ON tenants (node)`); err != nil {
			return err
		}
		if _, err := tx.Exec(`CREATE TABLE nodes (
			name       TEXT PRIMARY KEY,
			address    TEXT NOT NULL UNIQUE,
			created_at TEXT NOT NULL
		) STRICT`); err != nil {
			return err
		}
		version = 5
	}
	// Version 6 counts the rollouts asked for each tenant.
	if version == 5 {
		if _, err := tx.Exec(`ALTER TABLE tenants ADD COLUMN rollout INTEGER NOT NULL DEFAULT 0`); err != nil {
			return err
		}
		version = 6
	}
	// Version 7 moves tenants between nodes.
	if version == 6 {
		for _, statement := range []string{
			`ALTER TABLE tenants ADD COLUMN move_to TEXT NOT NULL DEFAULT ''`,
			`ALTER TABLE tenants ADD COLUMN move_held INTEGER NOT NULL DEFAULT 0 CHECK (move_held IN (0, 1))`,
		} {
			if _, err := tx.Exec(statement); err != nil {
				return err
			}
		}
		version = 7
	}
	// Version 8 keeps a copy of every tenant's data on a second node.
	if version == 7 {
		for _, statement := range []string{
			`ALTER TABLE tenants ADD COLUMN replica TEXT NOT NULL DEFAULT ''`,
			`ALTER TABLE tenants ADD COLUMN replica_ready INTEGER NOT NULL DEFAULT 0 CHECK (replica_ready IN (0, 1))`,
		} {
			if _, err := tx.Exec(statement); err != nil {
				return err
			}
		}
		version = 8
	}
	// Version 9 marks the nodes whose tenants failed over.
	if version == 8 {
		if _, err := tx.Exec(`ALTER TABLE nodes ADD COLUMN failed_at TEXT`); err != nil {
			return err
		}
	}
	if _, err := tx.Exec(fmt.Sprintf(`PRAGMA user_version = %d`, schemaVersion)); err != nil {
		return err
	}
	return tx.Commit()
}

// SchemaVersion is the schema version of this release's databases.
const SchemaVersion = schemaVersion
