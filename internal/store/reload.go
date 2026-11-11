package store

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"maps"
	"os"
	"reflect"
	"slices"
	"strings"
	"sync/atomic"

	"modernc.org/sqlite"
)

// A transaction's new state is the previous one with what the transaction
// changed read again, not the whole state: a write costs what it changes,
// not what is stored. SQLite's pre-update hook reports every row a
// transaction changes, by its statements, their foreign key cascades and
// ON CONFLICT REPLACE alike, so what is read again does not depend on how
// a statement is written, whichever release wrote it.

// verifyReloads makes every transaction also read the whole state and
// refuse to commit if the two differ. The tests set it; so may an operator
// who suspects the state, with MURDER_VERIFY_RELOADS=1.
var verifyReloads = os.Getenv("MURDER_VERIFY_RELOADS") != ""

// ErrReload reports a state read again in part that differs from the same
// state read whole. Only a transaction under verifyReloads reports it.
var ErrReload = errors.New("the state read in part differs from the state read whole")

// reloadsWhole and reloadsInPart count how states were read, for tests.
var reloadsWhole, reloadsInPart atomic.Int64

// wholeTenants is how many tenants a transaction changes before the state
// is read whole instead: reading many tenants by name costs more than
// reading them all.
const wholeTenants = 64

// changes are what one transaction changed: the tables, and of tenants and
// their secrets, which tenants.
type changes struct {
	tables  map[string]bool
	tenants map[string]bool
	// whole is set by a change the hook cannot place: the state is then
	// read whole.
	whole bool
	// meta are the keys of the meta table's rows changed, while noted.
	meta map[string]bool
}

// noteMeta notes the keys of a meta row changed: the old one and the new.
func (c *changes) noteMeta(data sqlite.SQLitePreUpdateData) {
	for _, row := range []struct {
		has  bool
		read func(...any) error
	}{
		{data.Op != sqlite3Insert, data.Old},
		{data.Op != sqlite3Delete, data.New},
	} {
		if !row.has {
			continue
		}
		values := make([]any, 1)
		if err := row.read(values...); err != nil {
			c.meta["?"] = true
			continue
		}
		c.meta[fmt.Sprint(values[0])] = true
	}
}

// keyed tells, for each table whose rows are read again by key, which of
// its columns holds the key: a tenant's name. Columns are only ever added
// at the end, so the key stays where it is.
var keyed = map[string]struct {
	index  int
	column string
}{
	"tenants": {0, "name"},
	"secrets": {0, "tenant"},
}

// partOf names the part of the state each table other than tenants,
// secrets and apps is read into; loaders read each part.
var partOf = map[string]string{
	"meta":              "meta",
	"events":            "meta",
	"app_revisions":     "revisions",
	"rollouts":          "rollouts",
	"nodes":             "nodes",
	"admissions":        "nodes",
	"departed_keys":     "nodes",
	"devices":           "devices",
	"domains":           "domains",
	"acme_challenges":   "acme",
	"acme_accounts":     "acme",
	"acme_revocations":  "acme",
	"images":            "images",
	"panel_users":       "panel",
	"panel_passkeys":    "panel",
	"panel_enrollments": "panel",
	"panel_sessions":    "panel",
	"panel_secrets":     "panel",
}

var loaders = map[string]func(querier, *snapshot) error{
	"meta":      loadMeta,
	"revisions": loadRevisions,
	"rollouts":  loadRollouts,
	"nodes":     loadNodes,
	"devices":   loadDevices,
	"domains":   loadDomains,
	"acme":      loadACME,
	"images":    loadImages,
	"panel":     loadPanel,
}

func (c *changes) note(data sqlite.SQLitePreUpdateData) {
	if data.DatabaseName != "main" {
		c.whole = true
		return
	}
	c.tables[data.TableName] = true
	if data.TableName == "meta" && c.meta != nil {
		c.noteMeta(data)
	}
	key, ok := keyed[data.TableName]
	if !ok {
		if _, known := partOf[data.TableName]; !known && data.TableName != "apps" {
			c.whole = true
		}
		return
	}
	// An insert has only a new row, a delete only an old one, an update
	// both: a key that changes is read again under both.
	for _, row := range []struct {
		has  bool
		read func(...any) error
	}{
		{data.Op != sqlite3Insert, data.Old},
		{data.Op != sqlite3Delete, data.New},
	} {
		if !row.has {
			continue
		}
		values := make([]any, key.index+1)
		if err := row.read(values...); err != nil {
			c.whole = true
			return
		}
		name, ok := values[key.index].(string)
		if !ok {
			c.whole = true
			return
		}
		c.tenants[name] = true
	}
}

// checkKeys refuses a schema in which a table read again by key does not
// hold the key where the pre-update hook reads it: a migration that moved
// it would leave the state in memory stale.
func checkKeys(q querier) error {
	for table, key := range keyed {
		var column string
		if err := q.QueryRow(`SELECT name FROM pragma_table_info(?) WHERE cid = ?`, table, key.index).Scan(&column); err != nil {
			return fmt.Errorf("read the columns of %s: %w", table, err)
		}
		if column != key.column {
			return fmt.Errorf("column %d of %s is %s, not %s: the store reads its rows again by %s", key.index, table, column, key.column, key.column)
		}
	}
	return nil
}

// The operation codes the pre-update hook reports, as in sqlite3.h.
const (
	sqlite3Delete = 9
	sqlite3Insert = 18
)

// watched is a connection whose changes are noted.
type watched struct {
	conn    *sql.Conn
	changes *changes
}

// hooks is what the driver's connection offers.
type hooks interface {
	RegisterPreUpdateHook(sqlite.PreUpdateHookFn)
}

// watch takes the store's connection and notes the changes made on it
// until done; the caller holds s.applying.
func (s *SQLite) watch(ctx context.Context) (*watched, error) {
	conn, err := s.db.Conn(ctx)
	if err != nil {
		return nil, err
	}
	w := &watched{conn: conn, changes: &changes{tables: map[string]bool{}, tenants: map[string]bool{}}}
	err = conn.Raw(func(driverConn any) error {
		h, ok := driverConn.(hooks)
		if !ok {
			return fmt.Errorf("the sqlite driver's connection %T has no pre-update hook", driverConn)
		}
		h.RegisterPreUpdateHook(w.changes.note)
		return nil
	})
	if err != nil {
		_ = conn.Close()
		return nil, err
	}
	return w, nil
}

// done stops noting changes and gives the connection back.
func (w *watched) done() {
	_ = w.conn.Raw(func(driverConn any) error {
		driverConn.(hooks).RegisterPreUpdateHook(nil)
		return nil
	})
	_ = w.conn.Close()
}

// reload reads the state as the transaction left it: what it changed
// again, the rest as it was. Maps of the previous state are shared, never
// changed: what is read again goes into new maps.
func reload(q querier, previous *snapshot, c *changes) (*snapshot, error) {
	if c.whole || c.tables["apps"] || len(c.tenants) > wholeTenants {
		reloadsWhole.Add(1)
		return load(q)
	}
	reloadsInPart.Add(1)
	next := *previous
	// The version changes with every batch, and an event comes with most:
	// meta is always read again. It is a few rows.
	read := map[string]bool{"meta": true}
	for table := range c.tables {
		if part, ok := partOf[table]; ok {
			read[part] = true
		}
	}
	for _, part := range slices.Sorted(maps.Keys(read)) {
		if err := loaders[part](q, &next); err != nil {
			return nil, err
		}
	}
	if len(c.tenants) > 0 {
		next.tenants = maps.Clone(previous.tenants)
		names := slices.Sorted(maps.Keys(c.tenants))
		for _, name := range names {
			delete(next.tenants, name)
		}
		if err := loadTenants(q, &next, names); err != nil {
			return nil, err
		}
	}
	if !read["images"] {
		// recordImages adds to the new state's images: they must not be the
		// previous state's.
		next.images = maps.Clone(previous.images)
	}
	return &next, nil
}

// verifyReload reads the whole state and compares it with next.
func verifyReload(q querier, next *snapshot) error {
	whole, err := load(q)
	if err != nil {
		return err
	}
	if !reflect.DeepEqual(whole, next) {
		return fmt.Errorf("%w: %s", ErrReload, snapshotDiff(whole, next))
	}
	return nil
}

// snapshotDiff names the parts of two states that differ, and the first
// keys of each.
func snapshotDiff(whole, part *snapshot) string {
	var out []string
	diff := func(name string, a, b any) {
		if !reflect.DeepEqual(a, b) {
			out = append(out, name)
		}
	}
	diff("node", whole.node, part.node)
	diff("version", whole.version, part.version)
	diff("cluster", whole.cluster, part.cluster)
	diff("frozen", whole.frozen, part.frozen)
	diff("lastEvent", whole.lastEvent, part.lastEvent)
	diff("apps", whole.apps, part.apps)
	diff("revisions", whole.revisions, part.revisions)
	diff("rollouts", whole.rollouts, part.rollouts)
	for name := range whole.tenants {
		if !reflect.DeepEqual(whole.tenants[name], part.tenants[name]) {
			out = append(out, "tenant "+name)
		}
	}
	for name := range part.tenants {
		if _, ok := whole.tenants[name]; !ok {
			out = append(out, "tenant "+name+" (only in part)")
		}
	}
	diff("nodes", whole.nodes, part.nodes)
	diff("admissions", whole.admissions, part.admissions)
	diff("departed", whole.departed, part.departed)
	diff("devices", whole.devices, part.devices)
	diff("domains", whole.domains, part.domains)
	diff("challenges", whole.challenges, part.challenges)
	diff("accounts", whole.accounts, part.accounts)
	diff("revocations", whole.revocations, part.revocations)
	diff("panel", whole.panel, part.panel)
	if !reflect.DeepEqual(whole.images, part.images) {
		out = append(out, fmt.Sprintf("images %v != %v", whole.images, part.images))
	}
	return strings.Join(out, ", ")
}
