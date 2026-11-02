package store

import (
	"crypto/sha256"
	"database/sql"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"strings"
)

// Digest is a hash of the replicated state as of a version of it. Nodes
// that applied the same batches hold the same state, so their digests of
// one version are the same, unless their schemas differ, as in an upgrade
// between two releases. Digests that differ otherwise mean the nodes'
// states drifted apart.
type Digest struct {
	Version int64  `json:"version"`
	Schema  int    `json:"schema"`
	Hash    string `json:"hash"`
}

// Comparable reports whether two digests should be the same.
func (d Digest) Comparable(other Digest) bool {
	return d.Version == other.Version && d.Schema == other.Schema && d.Hash != "" && other.Hash != ""
}

// Digest hashes the replicated state: every row of the replicated tables,
// in a fixed order, and the cluster and freeze the state names. It reads
// the database, not the state in memory, so it also sees what the state
// in memory does not hold.
func (s *SQLite) Digest() (Digest, error) {
	s.applying.Lock()
	defer s.applying.Unlock()
	tx, err := s.db.Begin()
	if err != nil {
		return Digest{}, err
	}
	defer tx.Rollback()
	digest := Digest{Schema: schemaVersion}
	hash := sha256.New()
	var version string
	if err := tx.QueryRow(`SELECT value FROM meta WHERE key = 'version'`).Scan(&version); err != nil {
		return Digest{}, fmt.Errorf("read the state version: %w", err)
	}
	if _, err := fmt.Sscan(version, &digest.Version); err != nil {
		return Digest{}, fmt.Errorf("state version %q: %w", version, err)
	}
	rows, err := tx.Query(`SELECT key, value FROM meta WHERE key IN ('cluster', 'frozen') ORDER BY key`)
	if err != nil {
		return Digest{}, err
	}
	if err := hashRows(hash, "meta", rows); err != nil {
		return Digest{}, err
	}
	for _, table := range replicatedTables {
		columns, err := columnNames(tx, table)
		if err != nil {
			return Digest{}, err
		}
		order := make([]string, len(columns))
		for i := range columns {
			order[i] = fmt.Sprint(i + 1)
		}
		rows, err := tx.Query(`SELECT ` + strings.Join(columns, ", ") + ` FROM ` + table + ` ORDER BY ` + strings.Join(order, ", "))
		if err != nil {
			return Digest{}, err
		}
		if err := hashRows(hash, table, rows); err != nil {
			return Digest{}, err
		}
	}
	digest.Hash = hex.EncodeToString(hash.Sum(nil))
	return digest, nil
}

// columnNames lists a table's columns in their order.
func columnNames(tx *sql.Tx, table string) ([]string, error) {
	rows, err := tx.Query(`SELECT name FROM pragma_table_info(?) ORDER BY cid`, table)
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
	return names, nil
}

// hashRows writes the table's name and each of its rows, as JSON, to
// hash, and closes rows.
func hashRows(hash interface{ Write([]byte) (int, error) }, table string, rows *sql.Rows) error {
	columns, err := rows.Columns()
	if err != nil {
		rows.Close()
		return err
	}
	_, _ = hash.Write([]byte("table " + table + "\n"))
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
		_, _ = hash.Write(append(line, '\n'))
	}
	return closeRows(rows)
}
