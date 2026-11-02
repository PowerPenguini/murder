package store

import (
	"context"
	"crypto/sha256"
	"database/sql"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"hash"
	"slices"
	"strconv"
	"strings"
)

// digestFormat versions what a digest hashes and how: a release that
// changes either changes it, so nodes of two releases are not compared.
const digestFormat = 1

// Digest is a hash of the replicated state as of a version of it. Nodes
// that applied the same batches hold the same state, so their digests of
// one version are the same, unless their schemas or digest formats differ,
// as in an upgrade between two releases. Digests that differ otherwise
// mean the nodes' states drifted apart.
type Digest struct {
	Version int64  `json:"version"`
	Schema  int    `json:"schema"`
	Format  int    `json:"format"`
	Hash    string `json:"hash"`
}

// Comparable reports whether two digests should be the same.
func (d Digest) Comparable(other Digest) bool {
	return d.Version == other.Version && d.Schema == other.Schema && d.Format == other.Format && d.Hash != "" && other.Hash != ""
}

// Digest hashes the replicated state: every row of the replicated tables,
// columns by name, rows in a fixed order, and the cluster and freeze the
// state names. It reads the database, not the state in memory, so it also
// sees what the state in memory does not hold. It reads through a
// connection of its own, in one read transaction, which sees one version
// of the state whole: it holds up no write meanwhile.
func (s *SQLite) Digest(ctx context.Context) (Digest, error) {
	db, err := sql.Open("sqlite", uri(s.path, "mode=ro&_pragma=busy_timeout(5000)"))
	if err != nil {
		return Digest{}, err
	}
	defer db.Close()
	tx, err := db.BeginTx(ctx, &sql.TxOptions{ReadOnly: true})
	if err != nil {
		return Digest{}, err
	}
	defer tx.Rollback()
	digest := Digest{Schema: schemaVersion, Format: digestFormat}
	var version string
	if err := tx.QueryRowContext(ctx, `SELECT value FROM meta WHERE key = 'version'`).Scan(&version); err != nil {
		return Digest{}, fmt.Errorf("read the state version: %w", err)
	}
	if digest.Version, err = strconv.ParseInt(version, 10, 64); err != nil {
		return Digest{}, fmt.Errorf("state version %q: %w", version, err)
	}
	sum := sha256.New()
	rows, err := tx.QueryContext(ctx, `SELECT key, value FROM meta WHERE key IN ('cluster', 'frozen') ORDER BY key`)
	if err != nil {
		return Digest{}, err
	}
	if err := hashRows(sum, "meta", rows); err != nil {
		return Digest{}, err
	}
	for _, table := range replicatedTables {
		columns, err := columnNames(ctx, tx, table)
		if err != nil {
			return Digest{}, err
		}
		list := strings.Join(columns, ", ")
		rows, err := tx.QueryContext(ctx, `SELECT `+list+` FROM `+table+` ORDER BY `+list)
		if err != nil {
			return Digest{}, err
		}
		if err := hashRows(sum, table, rows); err != nil {
			return Digest{}, err
		}
	}
	digest.Hash = hex.EncodeToString(sum.Sum(nil))
	return digest, nil
}

// columnNames lists a table's columns by name, quoted: by name, not by
// position, so a migrated database and a new one hash alike.
func columnNames(ctx context.Context, tx *sql.Tx, table string) ([]string, error) {
	rows, err := tx.QueryContext(ctx, `SELECT name FROM pragma_table_info(?)`, table)
	if err != nil {
		return nil, err
	}
	var names []string
	for rows.Next() {
		var name string
		if err := rows.Scan(&name); err != nil {
			rows.Close()
			return nil, err
		}
		names = append(names, `"`+name+`"`)
	}
	if err := closeRows(rows); err != nil {
		return nil, err
	}
	if len(names) == 0 {
		return nil, fmt.Errorf("table %s has no columns", table)
	}
	slices.Sort(names)
	return names, nil
}

// hashRows writes the table's name, its columns and each of its rows, as
// JSON, to sum, and closes rows.
func hashRows(sum hash.Hash, table string, rows *sql.Rows) error {
	columns, err := rows.Columns()
	if err != nil {
		rows.Close()
		return err
	}
	_, _ = sum.Write([]byte("table " + table + " " + strings.Join(columns, ",") + "\n"))
	values := make([]any, len(columns))
	pointers := make([]any, len(columns))
	for i := range values {
		pointers[i] = &values[i]
	}
	for rows.Next() {
		if err := rows.Scan(pointers...); err != nil {
			rows.Close()
			return err
		}
		line, err := json.Marshal(values)
		if err != nil {
			rows.Close()
			return err
		}
		_, _ = sum.Write(append(line, '\n'))
	}
	return closeRows(rows)
}
