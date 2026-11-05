package store

import (
	"database/sql"
	"errors"
	"fmt"
	"slices"
	"strings"
)

// schemaVersion is the version of the schema below, kept in PRAGMA
// user_version. Versions 1 to 3 were development releases that never ran in
// production; their databases are refused, not upgraded. Open upgrades
// versions 4 to 17 in place.
const schemaVersion = 18

// IdentitySchema is the schema version that pins each node's identity
// key in the state: a node registers its key only once every node runs a
// release of this version or later, which applies that write.
const IdentitySchema = 16

// DomainsSchema is the schema version that keeps tenants' own domains,
// their certificates and the cluster's ACME account: the leader orders
// and writes certificates only once every node runs a release of this
// version or later, which applies those writes.
const DomainsSchema = 18

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
	updated_at TEXT NOT NULL,
	-- The tag the image was given as, and pinned from; empty when it was
	-- given pinned.
	tag        TEXT NOT NULL DEFAULT '',
	-- The app's newest revision; 0 for an app stored before revisions.
	revision   INTEGER NOT NULL DEFAULT 0,
	-- The tag's newest image in its registry, pinned, while the app runs
	-- another; and when it was found there.
	latest     TEXT NOT NULL DEFAULT '',
	latest_at  TEXT
) STRICT;

-- An app's newest rollout: its tenants move to revision to_revision in
-- waves, each healthy for a while before the next.
CREATE TABLE rollouts (
	app           TEXT PRIMARY KEY REFERENCES apps (name),
	to_revision   INTEGER NOT NULL,
	-- running, paused (by someone, or by a tenant that failed), done or
	-- aborted (its tenants went back).
	state         TEXT NOT NULL CHECK (state IN ('running', 'paused', 'done', 'aborted')),
	wave          INTEGER NOT NULL,
	wave_started  TEXT NOT NULL,
	reason        TEXT NOT NULL,
	started_at    TEXT NOT NULL,
	updated_at    TEXT NOT NULL,
	actor         TEXT NOT NULL,
	-- epoch counts the rollout's changes of state (waves, pauses, resumes):
	-- a decision made on one epoch is refused on another.
	epoch         INTEGER NOT NULL DEFAULT 0,
	-- first counts its leading waves of tenants named to move first (a
	-- fix's broken tenants): 0 or 1. Its next wave is then the canaries.
	first         INTEGER NOT NULL DEFAULT 0
) STRICT;

-- The specs an app was stored with, the recent ones, to go back to.
CREATE TABLE app_revisions (
	app        TEXT NOT NULL REFERENCES apps (name),
	revision   INTEGER NOT NULL,
	spec       TEXT NOT NULL,
	tag        TEXT NOT NULL,
	created_at TEXT NOT NULL,
	actor      TEXT NOT NULL,
	PRIMARY KEY (app, revision)
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
	replica_ready INTEGER NOT NULL DEFAULT 0 CHECK (replica_ready IN (0, 1)),
	-- The node the copy moves to, to even out the copies the nodes keep:
	-- it gets a whole copy while replica keeps its own, and replaces
	-- replica once that copy is whole.
	replica_next TEXT NOT NULL DEFAULT '',
	-- The app revision the tenant runs: 0 for the app's newest; another
	-- while a rollout has not reached it, or took it back. before_revision
	-- is the one it ran before the rollout that moved it, to go back to.
	app_revision    INTEGER NOT NULL DEFAULT 0,
	before_revision INTEGER NOT NULL DEFAULT 0,
	-- canary tenants take a rollout first; held ones are left out of it.
	canary INTEGER NOT NULL DEFAULT 0 CHECK (canary IN (0, 1)),
	hold   INTEGER NOT NULL DEFAULT 0 CHECK (hold IN (0, 1)),
	-- The image the tenant's data goes back to, from the snapshot taken as
	-- the image changed, on the tenant's node; empty for none.
	restore TEXT NOT NULL DEFAULT '',
	-- moved_on is the node it ran on as its rollout moved it, which keeps
	-- the snapshot its data can go back to.
	moved_on TEXT NOT NULL DEFAULT ''
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
	failed_at  TEXT,
	-- Until then the node restarts on purpose, its agent stopped cleanly:
	-- it is not failed over for not answering.
	away_until TEXT,
	-- Until then no copy moves to the node: one failed to get there.
	copies_refused_until TEXT,
	-- The node's public addresses, separated by spaces, which the
	-- base domain's records name while it serves.
	public     TEXT NOT NULL DEFAULT '',
	-- The node's end of the panel's WireGuard tunnel: its address in the
	-- tunnel, its public key and its UDP port, separated by spaces.
	tunnel     TEXT NOT NULL DEFAULT '',
	-- The public key of the node's identity, which its cluster certificate
	-- must carry: ed25519, in base64. Empty for a node of a release before
	-- keys, until it registers its own.
	key        TEXT NOT NULL DEFAULT ''
) STRICT;

-- Hosts an invitation let in, which may join as the node of that name
-- with that identity key until the admission expires.
CREATE TABLE admissions (
	name       TEXT PRIMARY KEY,
	key        TEXT NOT NULL,
	expires_at TEXT NOT NULL,
	actor      TEXT NOT NULL
) STRICT;
-- The identity keys of nodes taken out of the state, by name: a node is
-- taken out of the state before the voters, and while it still votes, Raft
-- takes it by this key, never by its name alone (an empty key: by its
-- name, for a node of a cluster from before keys that registered none).
-- seq tells one departure of a name from the next: the state's version as
-- the node was taken out.
CREATE TABLE departed_keys (
	name TEXT PRIMARY KEY,
	key  TEXT NOT NULL,
	seq  INTEGER NOT NULL
) STRICT;

-- A node keeps its identity key: see identityTriggers.
CREATE TRIGGER nodes_key_kept BEFORE UPDATE ON nodes
WHEN OLD.key != '' AND (NEW.key != OLD.key OR NEW.name != OLD.name)
BEGIN
	SELECT RAISE(ABORT, 'a node keeps its identity key: take the node out to drop it');
END;
CREATE TRIGGER nodes_key_not_replaced BEFORE INSERT ON nodes
WHEN EXISTS (SELECT 1 FROM nodes WHERE name = NEW.name AND key != '' AND key != NEW.key)
BEGIN
	SELECT RAISE(ABORT, 'a node keeps its identity key: take the node out to drop it');
END;

-- The devices that may reach the panel through its tunnel: a WireGuard
-- public key each, and its address in the tunnel.
CREATE TABLE devices (
	name       TEXT PRIMARY KEY,
	key        TEXT NOT NULL UNIQUE,
	address    TEXT NOT NULL UNIQUE,
	created_at TEXT NOT NULL,
	created_by TEXT NOT NULL
) STRICT;

-- The panel's people, each signing in with passkeys; handle is the
-- WebAuthn user handle, random.
CREATE TABLE panel_users (
	name       TEXT PRIMARY KEY,
	handle     TEXT NOT NULL UNIQUE,
	created_at TEXT NOT NULL,
	created_by TEXT NOT NULL
) STRICT;

-- A person's passkeys: the WebAuthn credential as JSON, and a label.
CREATE TABLE panel_passkeys (
	id         TEXT PRIMARY KEY,
	user       TEXT NOT NULL REFERENCES panel_users (name) ON DELETE CASCADE,
	data       TEXT NOT NULL,
	label      TEXT NOT NULL,
	created_at TEXT NOT NULL,
	used_at    TEXT
) STRICT;

-- One-time ways to add a passkey, by the SHA-256 of their token:
-- invitations, which expire, and recovery codes, which do not.
CREATE TABLE panel_enrollments (
	hash       TEXT PRIMARY KEY,
	user       TEXT NOT NULL REFERENCES panel_users (name) ON DELETE CASCADE,
	kind       TEXT NOT NULL CHECK (kind IN ('invite', 'recovery')),
	expires_at TEXT,
	created_at TEXT NOT NULL,
	created_by TEXT NOT NULL
) STRICT;

-- Signed-in sessions, by the SHA-256 of their cookie: the passkey that
-- began each, which takes them along when removed, and the challenge it
-- answered, which signs no one in twice.
CREATE TABLE panel_sessions (
	hash       TEXT PRIMARY KEY,
	user       TEXT NOT NULL REFERENCES panel_users (name) ON DELETE CASCADE,
	passkey    TEXT NOT NULL,
	challenge  TEXT NOT NULL UNIQUE,
	created_at TEXT NOT NULL,
	expires_at TEXT NOT NULL
) STRICT;

-- Secrets every node's panel shares, such as the key its sign-in
-- ceremonies are signed with.
CREATE TABLE panel_secrets (
	name  TEXT PRIMARY KEY,
	value TEXT NOT NULL
) STRICT;

-- Sealed secret values, kept until a purge deletes the tenant.
CREATE TABLE secrets (
	tenant     TEXT NOT NULL REFERENCES tenants (name) ON DELETE CASCADE,
	name       TEXT NOT NULL,
	sealed     TEXT NOT NULL,
	created_at TEXT NOT NULL,
	PRIMARY KEY (tenant, name)
) STRICT;

` + domainTables + `

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

-- An event's id stays well below SQLite's largest rowid: see eventTriggers.
CREATE TRIGGER events_id_bounded BEFORE INSERT ON events
WHEN NEW.id > 4611686018427387904
BEGIN
	SELECT RAISE(ABORT, 'an event id is at most 2^62');
END;
CREATE TRIGGER events_id_kept_bounded BEFORE UPDATE ON events
WHEN NEW.id > 4611686018427387904
BEGIN
	SELECT RAISE(ABORT, 'an event id is at most 2^62');
END;
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
		for _, statement := range []string{
			`ALTER TABLE nodes ADD COLUMN failed_at TEXT`,
			`ALTER TABLE nodes ADD COLUMN away_until TEXT`,
		} {
			if _, err := tx.Exec(statement); err != nil {
				return err
			}
		}
		version = 9
	}
	// Version 10 moves copies between nodes to even them out.
	if version == 9 {
		for _, statement := range []string{
			`ALTER TABLE tenants ADD COLUMN replica_next TEXT NOT NULL DEFAULT ''`,
			`ALTER TABLE nodes ADD COLUMN copies_refused_until TEXT`,
		} {
			if _, err := tx.Exec(statement); err != nil {
				return err
			}
		}
		version = 10
	}
	// Version 11 keeps the nodes' public addresses, for DNS.
	if version == 10 {
		if _, err := tx.Exec(`ALTER TABLE nodes ADD COLUMN public TEXT NOT NULL DEFAULT ''`); err != nil {
			return err
		}
		version = 11
	}
	// Version 12 keeps the panel's tunnel: each node's end and the devices.
	if version == 11 {
		for _, statement := range []string{
			`ALTER TABLE nodes ADD COLUMN tunnel TEXT NOT NULL DEFAULT ''`,
			`CREATE TABLE devices (
				name       TEXT PRIMARY KEY,
				key        TEXT NOT NULL UNIQUE,
				address    TEXT NOT NULL UNIQUE,
				created_at TEXT NOT NULL,
				created_by TEXT NOT NULL
			) STRICT`,
		} {
			if _, err := tx.Exec(statement); err != nil {
				return err
			}
		}
		version = 12
	}
	// Version 13 keeps the panel's people, passkeys and sessions.
	if version == 12 {
		if _, err := tx.Exec(panelTables); err != nil {
			return err
		}
		version = 13
	}
	// Version 14 keeps apps' tags and revisions, and what their registry
	// holds. Apps stored before keep no revision until they change: a
	// node that installs a snapshot of an older release has none either,
	// so every node holds the same.
	if version == 13 {
		for _, statement := range []string{
			`ALTER TABLE apps ADD COLUMN tag TEXT NOT NULL DEFAULT ''`,
			`ALTER TABLE apps ADD COLUMN revision INTEGER NOT NULL DEFAULT 0`,
			`ALTER TABLE apps ADD COLUMN latest TEXT NOT NULL DEFAULT ''`,
			`ALTER TABLE apps ADD COLUMN latest_at TEXT`,
			revisionsTable,
		} {
			if _, err := tx.Exec(statement); err != nil {
				return err
			}
		}
		version = 14
	}
	// Version 15 rolls app changes out in waves.
	if version == 14 {
		for _, statement := range []string{
			`ALTER TABLE tenants ADD COLUMN app_revision INTEGER NOT NULL DEFAULT 0`,
			`ALTER TABLE tenants ADD COLUMN before_revision INTEGER NOT NULL DEFAULT 0`,
			`ALTER TABLE tenants ADD COLUMN canary INTEGER NOT NULL DEFAULT 0 CHECK (canary IN (0, 1))`,
			`ALTER TABLE tenants ADD COLUMN hold INTEGER NOT NULL DEFAULT 0 CHECK (hold IN (0, 1))`,
			`ALTER TABLE tenants ADD COLUMN restore TEXT NOT NULL DEFAULT ''`,
			`ALTER TABLE tenants ADD COLUMN moved_on TEXT NOT NULL DEFAULT ''`,
			rolloutsTable,
		} {
			if _, err := tx.Exec(statement); err != nil {
				return err
			}
		}
		version = 15
	}
	// Version 16 pins each node's identity key and admits hosts that bring
	// an invitation. Nodes stored before have no key until they register
	// theirs.
	if version == 15 {
		// Events of ids past the bound the triggers below keep go below it
		// first, or the next event would get a random id.
		if err := boundEventIDs(tx); err != nil {
			return err
		}
		for _, statement := range []string{
			`ALTER TABLE nodes ADD COLUMN key TEXT NOT NULL DEFAULT ''`,
			admissionsTable,
			departedTable,
			identityTriggers,
			eventTriggers,
		} {
			if _, err := tx.Exec(statement); err != nil {
				return err
			}
		}
		if err := dropForeignObjects(tx); err != nil {
			return err
		}
		version = 16
	}
	// Version 17 lets a rollout move named tenants first, as its first
	// wave, and says so.
	if version == 16 {
		if _, err := tx.Exec(`ALTER TABLE rollouts ADD COLUMN first INTEGER NOT NULL DEFAULT 0`); err != nil {
			return err
		}
		version = 17
	}
	// Version 18 keeps tenants' own domains, their certificates, and the
	// cluster's ACME account and challenges.
	if version == 17 {
		if _, err := tx.Exec(domainTables); err != nil {
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

// maxEventID is the largest id an event may have (see eventTriggers).
const maxEventID = 1 << 62

// boundEventIDs gives each event of an id past maxEventID an id below it,
// in the order of their ids (see eventIDs). Every node holds the same
// events, so every node gives them the same ids, and so does a node that
// installs a snapshot of an older release (see InstallSnapshot). Events are
// the audit log, kept, not dropped; nothing refers to an event by its id
// but readers paging through the log, which an upgrade restarts anyway.
func boundEventIDs(tx *sql.Tx) error {
	high, err := highEventIDs(tx, "events")
	if err != nil {
		return err
	}
	ids, err := newEventIDs(tx, "events")
	if err != nil {
		return err
	}
	for _, old := range high {
		id, err := ids.take()
		if err != nil {
			return err
		}
		if _, err := tx.Exec(`UPDATE events SET id = ? WHERE id = ?`, id, old); err != nil {
			return err
		}
	}
	return nil
}

// highEventIDs lists the ids past maxEventID of the events in table, in
// order.
func highEventIDs(tx *sql.Tx, table string) ([]int64, error) {
	rows, err := tx.Query(`SELECT id FROM `+table+` WHERE id > ? ORDER BY id`, maxEventID)
	if err != nil {
		return nil, err
	}
	var high []int64
	for rows.Next() {
		var id int64
		if err := rows.Scan(&id); err != nil {
			rows.Close()
			return nil, err
		}
		high = append(high, id)
	}
	return high, closeRows(rows)
}

// eventIDs gives events ids at most maxEventID, for events past it: the
// ones after the largest id at most maxEventID first, then, once those run
// out, the free ones from 1 up, in order. Each id taken must be in table
// before the next is taken. Which ids it gives depends only on the ids in
// table. Giving n ids reads the ids in table at most once, in order, a
// page at a time: the cost is that of the ids and the events, not their
// product.
type eventIDs struct {
	tx    *sql.Tx
	table string
	next  int64
	// gap is the smallest id not yet known to be taken, once next ran out.
	gap int64
	// held are ids of table at least gap, read and not yet passed, in
	// order; read is the largest id read, and done says none is left.
	held []int64
	read int64
	done bool
}

// eventIDPage is how many ids eventIDs reads at a time.
const eventIDPage = 4096

func newEventIDs(tx *sql.Tx, table string) (*eventIDs, error) {
	ids := &eventIDs{tx: tx, table: table, gap: 1}
	if err := tx.QueryRow(`SELECT COALESCE(MAX(id), 0) + 1 FROM `+table+` WHERE id <= ?`, maxEventID).Scan(&ids.next); err != nil {
		return nil, err
	}
	return ids, nil
}

func (e *eventIDs) take() (int64, error) {
	if e.next <= maxEventID {
		id := e.next
		e.next++
		return id, nil
	}
	for {
		if e.gap > maxEventID {
			return 0, errors.New("no event id is left below 2^62")
		}
		if len(e.held) == 0 && !e.done {
			if err := e.readPage(); err != nil {
				return 0, err
			}
		}
		// Ids passed already, such as the ones taken, read again.
		for len(e.held) > 0 && e.held[0] < e.gap {
			e.held = e.held[1:]
		}
		if len(e.held) > 0 && e.held[0] == e.gap {
			e.gap++
			continue
		}
		if len(e.held) > 0 || e.done {
			id := e.gap
			e.gap++
			return id, nil
		}
	}
}

// readPage reads the next ids of table, at most maxEventID, in order.
func (e *eventIDs) readPage() error {
	rows, err := e.tx.Query(`SELECT id FROM `+e.table+` WHERE id > ? AND id <= ? ORDER BY id LIMIT ?`, max(e.read, 0), maxEventID, eventIDPage)
	if err != nil {
		return err
	}
	count := 0
	for rows.Next() {
		var id int64
		if err := rows.Scan(&id); err != nil {
			rows.Close()
			return err
		}
		e.held = append(e.held, id)
		e.read = id
		count++
	}
	if err := closeRows(rows); err != nil {
		return err
	}
	e.done = count < eventIDPage
	return nil
}

// ownObjects are the indexes and triggers of the schema; it has no views.
var ownObjects = []string{"tenants_app", "tenants_node", "events_tenant", "events_app", "nodes_key_kept", "nodes_key_not_replaced", "events_id_bounded", "events_id_kept_bounded", "domains_tenant"}

// domainTables are the tables schema 18 added, as one script.
const domainTables = `-- Host names of the tenants' own, outside the base domain, which every
-- edge routes to their tenant once they were seen to point at the
-- cluster. Each belongs to one tenant, and goes with it when it is
-- purged. state says where getting its certificate is: verifying (not
-- checked yet), pending (its DNS does not point at the cluster), issuing,
-- active, renewing or failed; detail says why, attempts counts failures
-- in a row, next_at is when the leader tries again. The certificate is
-- PEM, its private key PEM sealed with the master key.
CREATE TABLE domains (
	name        TEXT PRIMARY KEY,
	tenant      TEXT NOT NULL REFERENCES tenants (name) ON DELETE CASCADE,
	created_at  TEXT NOT NULL,
	created_by  TEXT NOT NULL,
	state       TEXT NOT NULL CHECK (state IN ('verifying', 'pending', 'issuing', 'active', 'renewing', 'failed')),
	detail      TEXT NOT NULL DEFAULT '',
	attempts    INTEGER NOT NULL DEFAULT 0,
	checked_at  TEXT,
	next_at     TEXT,
	verified_at TEXT,
	certificate TEXT NOT NULL DEFAULT '',
	key         TEXT NOT NULL DEFAULT '',
	issued_at   TEXT,
	not_after   TEXT
) STRICT;

CREATE INDEX domains_tenant ON domains (tenant);

-- The cluster's account with each ACME CA it orders certificates from:
-- its URL and its private key, PEM sealed with the master key.
CREATE TABLE acme_accounts (
	ca         TEXT PRIMARY KEY,
	email      TEXT NOT NULL,
	location   TEXT NOT NULL,
	key        TEXT NOT NULL,
	created_at TEXT NOT NULL
) STRICT;

-- HTTP-01 challenges an order waits on: every edge answers the CA's
-- request for the token, for the domain, with the key authorization.
CREATE TABLE acme_challenges (
	token      TEXT PRIMARY KEY,
	domain     TEXT NOT NULL REFERENCES domains (name) ON DELETE CASCADE,
	key_auth   TEXT NOT NULL,
	expires_at TEXT NOT NULL
) STRICT;`

// eventTriggers, of schema 16, keep events' ids, SQLite rowids, at most
// 2^62. An event inserted without an id gets the largest one plus one,
// unless that is SQLite's largest rowid: then it gets one at random, which
// differs between nodes. Bounded so, the largest plus one never comes near
// it, and a batch that would go past the bound is refused on every node
// alike. events is the only replicated table whose key is a rowid; the
// others' keys are text the store gives, and their hidden rowids no batch
// may name (Check refuses rowid, _rowid_ and oid) or read.
const eventTriggers = `CREATE TRIGGER events_id_bounded BEFORE INSERT ON events
WHEN NEW.id > 4611686018427387904
BEGIN
	SELECT RAISE(ABORT, 'an event id is at most 2^62');
END;
CREATE TRIGGER events_id_kept_bounded BEFORE UPDATE ON events
WHEN NEW.id > 4611686018427387904
BEGIN
	SELECT RAISE(ABORT, 'an event id is at most 2^62');
END`

// dropForeignObjects drops every trigger, view and index that is not part of
// the schema, such as one a batch of an earlier release made: it would act
// on changes, or read what differs between nodes, as no batch may.
func dropForeignObjects(tx *sql.Tx) error {
	rows, err := tx.Query(`SELECT type, name FROM sqlite_master WHERE type IN ('trigger', 'view', 'index') AND name NOT LIKE 'sqlite\_%' ESCAPE '\'`)
	if err != nil {
		return err
	}
	var drops []string
	for rows.Next() {
		var kind, name string
		if err := rows.Scan(&kind, &name); err != nil {
			rows.Close()
			return err
		}
		if !slices.Contains(ownObjects, name) {
			drops = append(drops, fmt.Sprintf(`DROP %s "%s"`, strings.ToUpper(kind), strings.ReplaceAll(name, `"`, `""`)))
		}
	}
	if err := closeRows(rows); err != nil {
		return err
	}
	for _, drop := range drops {
		if _, err := tx.Exec(drop); err != nil {
			return err
		}
	}
	return nil
}

// identityTriggers, of schema 16, keep a node's identity key once the
// state holds one: no statement changes it, renames its node or puts a
// node of the same name with another key, or none, in its place. Taking
// the node's row out is the only way to drop it. They hold on every node
// for every statement, whoever wrote it: a check of the state itself, not
// of the code that writes it.
const identityTriggers = `CREATE TRIGGER nodes_key_kept BEFORE UPDATE ON nodes
WHEN OLD.key != '' AND (NEW.key != OLD.key OR NEW.name != OLD.name)
BEGIN
	SELECT RAISE(ABORT, 'a node keeps its identity key: take the node out to drop it');
END;
CREATE TRIGGER nodes_key_not_replaced BEFORE INSERT ON nodes
WHEN EXISTS (SELECT 1 FROM nodes WHERE name = NEW.name AND key != '' AND key != NEW.key)
BEGIN
	SELECT RAISE(ABORT, 'a node keeps its identity key: take the node out to drop it');
END`

// departedTable is a table schema 16 added.
const departedTable = `CREATE TABLE departed_keys (
	name TEXT PRIMARY KEY,
	key  TEXT NOT NULL,
	seq  INTEGER NOT NULL
) STRICT`

// admissionsTable is the table schema 16 added.
const admissionsTable = `CREATE TABLE admissions (
	name       TEXT PRIMARY KEY,
	key        TEXT NOT NULL,
	expires_at TEXT NOT NULL,
	actor      TEXT NOT NULL
) STRICT`

// rolloutsTable is the table schema 15 added.
const rolloutsTable = `CREATE TABLE rollouts (
	app           TEXT PRIMARY KEY REFERENCES apps (name),
	to_revision   INTEGER NOT NULL,
	state         TEXT NOT NULL CHECK (state IN ('running', 'paused', 'done', 'aborted')),
	wave          INTEGER NOT NULL,
	wave_started  TEXT NOT NULL,
	reason        TEXT NOT NULL,
	started_at    TEXT NOT NULL,
	updated_at    TEXT NOT NULL,
	actor         TEXT NOT NULL,
	-- epoch counts the rollout's changes of state (waves, pauses, resumes):
	-- a decision made on one epoch is refused on another.
	epoch         INTEGER NOT NULL DEFAULT 0
) STRICT`

// revisionsTable is the table schema 14 added.
const revisionsTable = `CREATE TABLE app_revisions (
	app        TEXT NOT NULL REFERENCES apps (name),
	revision   INTEGER NOT NULL,
	spec       TEXT NOT NULL,
	tag        TEXT NOT NULL,
	created_at TEXT NOT NULL,
	actor      TEXT NOT NULL,
	PRIMARY KEY (app, revision)
) STRICT`

// panelTables are the tables schema 13 added, as one script.
const panelTables = `-- The panel's people, each signing in with passkeys; handle is the
-- WebAuthn user handle, random.
CREATE TABLE panel_users (
	name       TEXT PRIMARY KEY,
	handle     TEXT NOT NULL UNIQUE,
	created_at TEXT NOT NULL,
	created_by TEXT NOT NULL
) STRICT;

-- A person's passkeys: the WebAuthn credential as JSON, and a label.
CREATE TABLE panel_passkeys (
	id         TEXT PRIMARY KEY,
	user       TEXT NOT NULL REFERENCES panel_users (name) ON DELETE CASCADE,
	data       TEXT NOT NULL,
	label      TEXT NOT NULL,
	created_at TEXT NOT NULL,
	used_at    TEXT
) STRICT;

-- One-time ways to add a passkey, by the SHA-256 of their token:
-- invitations, which expire, and recovery codes, which do not.
CREATE TABLE panel_enrollments (
	hash       TEXT PRIMARY KEY,
	user       TEXT NOT NULL REFERENCES panel_users (name) ON DELETE CASCADE,
	kind       TEXT NOT NULL CHECK (kind IN ('invite', 'recovery')),
	expires_at TEXT,
	created_at TEXT NOT NULL,
	created_by TEXT NOT NULL
) STRICT;

-- Signed-in sessions, by the SHA-256 of their cookie: the passkey that
-- began each, which takes them along when removed, and the challenge it
-- answered, which signs no one in twice.
CREATE TABLE panel_sessions (
	hash       TEXT PRIMARY KEY,
	user       TEXT NOT NULL REFERENCES panel_users (name) ON DELETE CASCADE,
	passkey    TEXT NOT NULL,
	challenge  TEXT NOT NULL UNIQUE,
	created_at TEXT NOT NULL,
	expires_at TEXT NOT NULL
) STRICT;

-- Secrets every node's panel shares, such as the key its sign-in
-- ceremonies are signed with.
CREATE TABLE panel_secrets (
	name  TEXT PRIMARY KEY,
	value TEXT NOT NULL
) STRICT;`
