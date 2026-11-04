package store

import (
	"errors"
	"fmt"
	"slices"

	"modernc.org/sqlite"
)

// A batch's statements, and the store's own count of the version and the
// applied index that follow them, run under batchAuthorizer, which SQLite
// asks as it compiles each statement, and each trigger the statement fires,
// whatever its text, on every node alike. It allows what the store's own
// changes do, and nothing else:
//
//   - reads of the replicated tables, and of the meta table's keys, but not
//     of its values (some, such as the node's name and how far it applied
//     the log, differ between nodes), nor of any other table, SQLite's
//     own (sqlite_schema, whose text differs between a new database and a
//     migrated one) included;
//   - inserts, updates and deletes of the replicated tables and of meta,
//     whose rows the store keeps for itself are checked as the batch is
//     applied (see bookkeeping);
//   - no function: none of the store's changes calls one, and some (random,
//     the time, changes) differ between nodes;
//   - no transaction control (a COMMIT would make part of a batch stick,
//     whatever the rest did), schema change (a trigger left behind would act
//     on later changes, or the identity keys' triggers be dropped), pragma
//     or attached database.
//
// So the same batch does the same on every node, and every node refuses it
// alike when it cannot. It is a check of correctness, not of trust: a
// member that wants to change the replicated state may; the cluster keeps
// out those that are not members.
func batchAuthorizer(action sqlite.AuthorizerActionCode, table, column, database, _ string) sqlite.AuthorizerReturnCode {
	switch action {
	case sqlite.AuthSelect:
		return sqlite.AuthorizerOK
	case sqlite.AuthRead:
		if database == "main" && (slices.Contains(replicatedTables, table) || table == "meta" && column == "key") {
			return sqlite.AuthorizerOK
		}
	case sqlite.AuthInsert, sqlite.AuthUpdate, sqlite.AuthDelete:
		if database == "main" && (slices.Contains(replicatedTables, table) || table == "meta") {
			return sqlite.AuthorizerOK
		}
	}
	return sqlite.AuthorizerDeny
}

// batchMeta are the meta table's rows a batch may write: the replicated
// state's own. The others (the node's name, the state's version and the
// applied index) are the store's to keep.
var batchMeta = []string{"cluster", "frozen"}

// errBookkeeping refuses a batch that writes a meta row the store keeps.
var errBookkeeping = errors.New("a batch may not change the store's own bookkeeping in the meta table")

// bookkeeping refuses, after a batch's statements ran, one that wrote a meta
// row other than batchMeta's. Which rows a statement writes depends only on
// the keys it matches, the same on every node.
func (c *changes) bookkeeping() error {
	for key := range c.meta {
		if !slices.Contains(batchMeta, key) {
			return refusal{fmt.Errorf("%w: %q", errBookkeeping, key)}
		}
	}
	return nil
}

// authorize puts fn in charge of what the watched connection compiles;
// nil takes it off.
func (w *watched) authorize(fn sqlite.AuthorizerFn) error {
	return w.conn.Raw(func(driverConn any) error {
		registerer, ok := driverConn.(sqlite.AuthorizerRegisterer)
		if !ok {
			return errors.New("the sqlite driver's connection has no authorizer")
		}
		return registerer.RegisterAuthorizer(fn)
	})
}
