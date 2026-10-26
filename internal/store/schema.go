package store

import (
	"database/sql"
	"errors"
	"fmt"
)

// migrations are applied in order; PRAGMA user_version counts those applied.
// Append new ones; never edit one that has shipped.
var migrations = []string{
	`
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

-- Live and archived tenants. An archived tenant holds no host port or
-- subnet, so they are free for others. (Rebuilt by migration 2.)
CREATE TABLE tenants (
	name                     TEXT PRIMARY KEY,
	app                      TEXT NOT NULL REFERENCES apps (name),
	overrides                TEXT NOT NULL,
	suspended                INTEGER NOT NULL CHECK (suspended IN (0, 1)),
	suspended_before_archive INTEGER CHECK (suspended_before_archive IN (0, 1)),
	host_port                INTEGER UNIQUE,
	subnet                   TEXT UNIQUE,
	data_image               TEXT NOT NULL,
	generation               INTEGER NOT NULL,
	created_at               TEXT NOT NULL,
	updated_at               TEXT NOT NULL,
	archived_at              TEXT,
	CHECK ((archived_at IS NULL) = (host_port IS NOT NULL AND subnet IS NOT NULL))
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
`,
	// 2: tenants are reached at a fixed address in their subnet instead of a
	// published host port, and an archive is one write, so it needs no
	// marker. A tenant caught half way through an archive is left live with
	// the suspension it had before.
	`
CREATE TABLE tenants_v2 (
	name        TEXT PRIMARY KEY,
	app         TEXT NOT NULL REFERENCES apps (name),
	overrides   TEXT NOT NULL,
	suspended   INTEGER NOT NULL CHECK (suspended IN (0, 1)),
	subnet      TEXT UNIQUE,
	data_image  TEXT NOT NULL,
	generation  INTEGER NOT NULL,
	created_at  TEXT NOT NULL,
	updated_at  TEXT NOT NULL,
	archived_at TEXT,
	CHECK ((archived_at IS NULL) = (subnet IS NOT NULL))
) STRICT;

INSERT INTO tenants_v2 (name, app, overrides, suspended, subnet, data_image, generation, created_at, updated_at, archived_at)
SELECT name, app, overrides, COALESCE(suspended_before_archive, suspended), subnet, data_image, generation, created_at, updated_at, archived_at
FROM tenants;

DROP TABLE tenants;
ALTER TABLE tenants_v2 RENAME TO tenants;
CREATE INDEX tenants_app ON tenants (app);
`,
}

// migrate brings the schema up to date in one transaction, refusing a
// database written by a newer release. Foreign keys are off meanwhile, so a
// migration can rebuild a table others refer to (dropping it would cascade
// to them otherwise); they are checked before the commit.
func migrate(db *sql.DB) error {
	// Outside the transaction: SQLite ignores the pragma inside one.
	if _, err := db.Exec(`PRAGMA foreign_keys = OFF`); err != nil {
		return err
	}
	defer db.Exec(`PRAGMA foreign_keys = ON`)
	tx, err := db.Begin()
	if err != nil {
		return err
	}
	defer tx.Rollback()
	var version int
	if err := tx.QueryRow(`PRAGMA user_version`).Scan(&version); err != nil {
		return err
	}
	if version > len(migrations) {
		return fmt.Errorf("the database has schema version %d; this release knows only %d — upgrade Corral", version, len(migrations))
	}
	if version == len(migrations) {
		return nil
	}
	for i := version; i < len(migrations); i++ {
		if _, err := tx.Exec(migrations[i]); err != nil {
			return fmt.Errorf("migration %d: %w", i+1, err)
		}
	}
	rows, err := tx.Query(`PRAGMA foreign_key_check`)
	if err != nil {
		return err
	}
	broken := rows.Next()
	if err := closeRows(rows); err != nil {
		return err
	}
	if broken {
		return errors.New("a migration left rows that break foreign keys")
	}
	if _, err := tx.Exec(fmt.Sprintf(`PRAGMA user_version = %d`, len(migrations))); err != nil {
		return err
	}
	return tx.Commit()
}
