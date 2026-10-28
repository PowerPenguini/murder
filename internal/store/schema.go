package store

import (
	"errors"
	"fmt"
)

// schemaVersion is the version of the schema below, kept in PRAGMA
// user_version. Versions 1 and 2 were development releases that never ran in
// production; their databases are refused, not upgraded. A later change of
// the schema adds a migration from this version.
const schemaVersion = 3

const schema = `
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
	generation  INTEGER NOT NULL,
	created_at  TEXT NOT NULL,
	updated_at  TEXT NOT NULL,
	archived_at TEXT
) STRICT;

CREATE INDEX tenants_app ON tenants (app);

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
	case version == schemaVersion:
		return nil
	case version == 0:
		return errors.New(`it is not initialized; run "corral init"`)
	case version > schemaVersion:
		return fmt.Errorf("it has schema version %d; this release knows only %d — upgrade Corral", version, schemaVersion)
	default:
		return fmt.Errorf("it has schema version %d of a development release, which cannot be upgraded; create a new one with \"corral init\"", version)
	}
}
