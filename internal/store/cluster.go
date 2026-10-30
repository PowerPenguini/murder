package store

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"io"
	"maps"
	"os"
	"path/filepath"
	"slices"
	"strconv"
	"sync"
	"time"

	"modernc.org/sqlite"
)

// replicatedTables are the tables every node of a cluster holds the same,
// in an order that inserts referenced rows first. The others (meta's node
// name, images) belong to one node.
var replicatedTables = []string{"apps", "tenants", "secrets", "nodes", "events"}

// sqliteConstraint is SQLite's result code for a violated constraint; the
// extended codes keep it in their low byte.
const sqliteConstraint = 19

// Refused reports whether a batch failed the same way on every node that
// applies it: stale, or against a constraint of the state. Any other
// failure, such as a full disk, is the node's own, and a node that cannot
// apply what every other node applied must stop rather than diverge.
func Refused(err error) bool {
	var sqliteErr *sqlite.Error
	return errors.Is(err, ErrStale) || errors.As(err, &sqliteErr) && sqliteErr.Code()&0xff == sqliteConstraint
}

// appliedIndex is the index of the last log entry applied, with a way to
// wait for a later one.
type appliedIndex struct {
	mu      sync.Mutex
	index   uint64
	advance chan struct{}
}

func (a *appliedIndex) get() uint64 {
	a.mu.Lock()
	defer a.mu.Unlock()
	return a.index
}

func (a *appliedIndex) set(index uint64) {
	a.mu.Lock()
	defer a.mu.Unlock()
	a.index = index
	if a.advance != nil {
		close(a.advance)
		a.advance = nil
	}
}

// wait returns once index is applied.
func (a *appliedIndex) wait(ctx context.Context, index uint64) error {
	for {
		a.mu.Lock()
		if a.index >= index {
			a.mu.Unlock()
			return nil
		}
		if a.advance == nil {
			a.advance = make(chan struct{})
		}
		advance := a.advance
		a.mu.Unlock()
		select {
		case <-advance:
		case <-ctx.Done():
			return fmt.Errorf("wait for log entry %d: %w", index, ctx.Err())
		}
	}
}

// Replicate hands every batch to commit, which must have the cluster's log
// apply it here (through ApplyEntry) before it returns, instead of applying
// it at once. Before a write is computed, catchUp must apply here every
// change the cluster has made, so the write is checked against them.
func (s *SQLite) Replicate(commit func(Batch) error, catchUp func() error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.catchUp = catchUp
	s.commit = func(batch Batch) (*snapshot, error) {
		if err := commit(batch); err != nil {
			return nil, err
		}
		return s.state.Load(), nil
	}
}

// Applied is the index of the last log entry applied here.
func (s *SQLite) Applied() uint64 { return s.applied.get() }

// WaitApplied returns once the log entry at index is applied here.
func (s *SQLite) WaitApplied(ctx context.Context, index uint64) error {
	return s.applied.wait(ctx, index)
}

// ApplyEntry applies the batch of the cluster log's entry at index. An entry
// at or below the applied index is skipped: the log replays entries the
// database already holds after a restart. A refused batch changes nothing,
// on every node, and still counts as applied.
func (s *SQLite) ApplyEntry(index uint64, batch Batch) error {
	if index <= s.applied.get() {
		return nil
	}
	_, err := s.transact(batch.Signal, func(tx *sql.Tx, current *snapshot) error {
		if err := applyBatch(tx, current, batch); err != nil {
			return err
		}
		_, err := tx.Exec(`UPDATE meta SET value = ? WHERE key = 'applied'`, strconv.FormatUint(index, 10))
		return err
	})
	if err == nil || Refused(err) {
		s.applied.set(index)
	}
	return err
}

// StartLog prepares the state for a new cluster log, whose first entries
// the state does not hold: the log starts at index 1 again.
func (s *SQLite) StartLog() error {
	err := s.local(func(tx *sql.Tx) error {
		_, err := tx.Exec(`UPDATE meta SET value = '0' WHERE key = 'applied'`)
		return err
	})
	if err == nil {
		s.applied.set(0)
	}
	return err
}

// Fresh reports whether the state holds nothing yet: a node may only join a
// cluster, and take the cluster's state, with nothing of its own to lose.
func (s *SQLite) Fresh() bool {
	state := s.state.Load()
	return state.version == 0 && len(state.apps) == 0 && len(state.tenants) == 0 && len(state.nodes) == 0
}

// SnapshotFile writes a copy of the whole database, as it is at one
// moment, to a new private file beside it and returns its name; the
// caller removes it. It includes the index of the last log entry applied.
func (s *SQLite) SnapshotFile() (string, error) {
	temp, err := os.CreateTemp(filepath.Dir(s.path), filepath.Base(s.path)+".backup-*")
	if err != nil {
		return "", err
	}
	name := temp.Name()
	_ = temp.Close()
	if _, err := s.db.Exec(`VACUUM INTO ?`, name); err != nil {
		removeDatabase(name)
		return "", fmt.Errorf("copy the database: %w", err)
	}
	if err := selfContained(name); err != nil {
		removeDatabase(name)
		return "", fmt.Errorf("check the copy: %w", err)
	}
	return name, nil
}

// InstallSnapshot replaces the replicated state with the one in from,
// a copy SnapshotFile made on another node of the same release, unless the
// state already holds the log entries the snapshot does. What belongs to
// this node alone stays. It reports whether it replaced the state.
func (s *SQLite) InstallSnapshot(from io.Reader) (bool, error) {
	temp, err := os.CreateTemp(filepath.Dir(s.path), filepath.Base(s.path)+".restore-*")
	if err != nil {
		return false, err
	}
	name := temp.Name()
	defer removeDatabase(name)
	_, err = io.Copy(temp, from)
	if err = errors.Join(err, temp.Close()); err != nil {
		return false, err
	}
	if err := checkSnapshot(name); err != nil {
		return false, fmt.Errorf("snapshot: %w", err)
	}

	s.applying.Lock()
	defer s.applying.Unlock()
	// ATTACH cannot run inside a transaction; the one connection keeps it.
	if _, err := s.db.Exec(`ATTACH DATABASE ? AS snapshot`, uri(name, "mode=ro")); err != nil {
		return false, err
	}
	defer s.db.Exec(`DETACH DATABASE snapshot`)
	var index uint64
	installed := false
	_, err = s.transactLocked(true, func(tx *sql.Tx, _ *snapshot) error {
		var applied string
		if err := tx.QueryRow(`SELECT value FROM snapshot.meta WHERE key = 'applied'`).Scan(&applied); err != nil {
			return err
		}
		if index, err = strconv.ParseUint(applied, 10, 64); err != nil {
			return fmt.Errorf("applied log index %q: %w", applied, err)
		}
		if index <= s.applied.get() {
			return nil
		}
		for _, table := range slices.Backward(replicatedTables) {
			if _, err := tx.Exec(`DELETE FROM main.` + table); err != nil {
				return err
			}
		}
		for _, table := range replicatedTables {
			if _, err := tx.Exec(`INSERT INTO main.` + table + ` SELECT * FROM snapshot.` + table); err != nil {
				return err
			}
		}
		installed = true
		_, err := tx.Exec(`UPDATE main.meta SET value = (SELECT copy.value FROM snapshot.meta AS copy WHERE copy.key = meta.key)
			WHERE key IN ('version', 'applied')`)
		return err
	})
	if err != nil {
		return false, err
	}
	if installed {
		s.applied.set(index)
	}
	return installed, nil
}

// checkSnapshot checks that a snapshot is whole and of this schema version
// exactly, so its tables match this database's.
func checkSnapshot(path string) error {
	db, err := sql.Open("sqlite", uri(path, "mode=ro"))
	if err != nil {
		return err
	}
	defer db.Close()
	if err := check(db); err != nil {
		return err
	}
	var version int
	if err := db.QueryRow(`PRAGMA user_version`).Scan(&version); err != nil {
		return err
	}
	if version != schemaVersion {
		return fmt.Errorf("%w: schema version %d, this node runs %d", ErrSchema, version, schemaVersion)
	}
	return nil
}

func (s *SQLite) Nodes() []Node {
	state := s.state.Load()
	nodes := make([]Node, 0, len(state.nodes))
	for _, name := range slices.Sorted(maps.Keys(state.nodes)) {
		nodes = append(nodes, state.nodes[name])
	}
	return nodes
}

func (s *SQLite) PutNode(node Node, actor string) error {
	if !nodePattern.MatchString(node.Name) {
		return fmt.Errorf("node name %q must be 1-63 letters, digits, dots or hyphens", node.Name)
	}
	if node.Role != RoleWorker && node.Role != RoleWitness {
		return fmt.Errorf("node role %q must be %s or %s", node.Role, RoleWorker, RoleWitness)
	}
	_, err := s.write(false, func(tx *recorder, current *snapshot, now time.Time) error {
		existing, found := current.nodes[node.Name]
		if found && existing.Address == node.Address && existing.Role == node.Role {
			return errUnchanged
		}
		for _, other := range current.nodes {
			if other.Name != node.Name && other.Address == node.Address {
				return fmt.Errorf("node %s already has address %s", other.Name, node.Address)
			}
		}
		if err := tx.Exec(`INSERT INTO nodes (name, address, role, created_at) VALUES (?, ?, ?, ?)
			ON CONFLICT (name) DO UPDATE SET address = excluded.address, role = excluded.role`,
			node.Name, node.Address, node.Role, formatTime(now)); err != nil {
			return err
		}
		action := "node.joined"
		if found {
			action = "node.changed"
		}
		return event(tx, now, actor, action, "", "", node.Name+": "+node.Role+" at "+node.Address)
	})
	if errors.Is(err, errUnchanged) {
		return nil
	}
	return err
}

func (s *SQLite) RemoveNode(name, actor string) error {
	_, err := s.write(false, func(tx *recorder, current *snapshot, now time.Time) error {
		node, found := current.nodes[name]
		if !found {
			return fmt.Errorf("node %s: %w", name, ErrNotFound)
		}
		if err := tx.Exec(`DELETE FROM nodes WHERE name = ?`, name); err != nil {
			return err
		}
		return event(tx, now, actor, "node.removed", "", "", node.Name+": "+node.Role+" at "+node.Address)
	})
	return err
}
