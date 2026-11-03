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
	"strings"
	"sync"
	"time"

	"modernc.org/sqlite"
)

// replicatedTables are the tables every node of a cluster holds the same,
// in an order that inserts referenced rows first. The others (meta's node
// name, images) belong to one node.
var replicatedTables = []string{"apps", "app_revisions", "tenants", "secrets", "nodes", "events", "devices", "panel_users", "panel_passkeys", "panel_enrollments", "panel_sessions", "panel_secrets"}

// tableSince is the schema version that added a replicated table: a
// snapshot of an older release has none.
var tableSince = map[string]int{"app_revisions": 14, "devices": 12, "panel_users": 13, "panel_passkeys": 13, "panel_enrollments": 13, "panel_sessions": 13, "panel_secrets": 13}

// Refused reports whether a batch failed the same way on every node that
// applies it: stale, malformed, or against the state (a constraint, a
// statement the schema cannot run). Any other failure, such as a full disk
// or a batch of a newer release, is the node's own, and a node that cannot
// apply what every other node applied must stop rather than diverge.
func Refused(err error) bool {
	return errors.Is(err, ErrStale) || errors.As(err, new(refusal))
}

// refusal is a batch every node refuses alike.
type refusal struct{ err error }

func (r refusal) Error() string { return "refused: " + r.err.Error() }
func (r refusal) Unwrap() error { return r.err }

// nodeFailure reports a SQLite failure of this node's own database rather
// than of the statement: it may not happen on another node.
func nodeFailure(err error) bool {
	var sqliteErr *sqlite.Error
	if !errors.As(err, &sqliteErr) {
		return true // not SQLite's verdict on the statement
	}
	switch sqliteErr.Code() & 0xff {
	case 3, 4, 5, 6, 7, 8, 9, 10, 11, 13, 14, 15, 26: // PERM ABORT BUSY LOCKED NOMEM READONLY INTERRUPT IOERR CORRUPT FULL CANTOPEN PROTOCOL NOTADB
		return true
	}
	return false
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
// apply it here (through ApplyEntry) before it returns its index, instead
// of applying it at once. Before a write is computed, catchUp must apply
// here every change the cluster has made, so the write is checked against
// them.
func (s *SQLite) Replicate(commit func(Batch) (uint64, error), catchUp func() error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.catchUp = catchUp
	s.commit = func(batch Batch) (*snapshot, error) {
		index, err := commit(batch)
		if err != nil {
			return nil, err
		}
		// What the write made, not what later entries made of it.
		if state, ok := s.recent.get(index); ok {
			return state, nil
		}
		return s.state.Load(), nil
	}
}

// recentStates keeps the states the last log entries made, for the writes
// waiting for them.
type recentStates struct {
	mu     sync.Mutex
	states map[uint64]*snapshot
	order  []uint64
	// outcomes are how the last batches went, by ID.
	outcomes map[string]Outcome
	ids      []string
}

// Outcome is how a batch went: the entry it was applied as, or why every
// node refused it.
type Outcome struct {
	Index uint64
	Err   error
}

func (r *recentStates) outcome(id string, index uint64, err error) {
	if id == "" {
		return
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.outcomes == nil {
		r.outcomes = map[string]Outcome{}
	}
	// A batch sent again after its first copy reached the log is refused
	// as stale: how the first went is its outcome.
	if _, known := r.outcomes[id]; known {
		return
	}
	r.outcomes[id] = Outcome{Index: index, Err: err}
	r.ids = append(r.ids, id)
	if len(r.ids) > outcomesKept {
		delete(r.outcomes, r.ids[0])
		r.ids = r.ids[1:]
	}
}

// Applied says how the batch with id went, if this node applied it
// lately.
func (s *SQLite) Outcome(id string) (Outcome, bool) {
	s.recent.mu.Lock()
	defer s.recent.mu.Unlock()
	outcome, ok := s.recent.outcomes[id]
	return outcome, ok
}

const recentKept = 64

// outcomesKept is how many batches' outcomes a node remembers, from every
// node's writes: enough that a write whose leader went away while it was
// committed finds how it went, however busy the cluster was meanwhile,
// instead of being made again. One costs about a hundred bytes.
const outcomesKept = 16384

func (r *recentStates) put(index uint64, state *snapshot) {
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.states == nil {
		r.states = map[uint64]*snapshot{}
	}
	r.states[index] = state
	r.order = append(r.order, index)
	if len(r.order) > recentKept {
		delete(r.states, r.order[0])
		r.order = r.order[1:]
	}
}

func (r *recentStates) get(index uint64) (*snapshot, bool) {
	r.mu.Lock()
	defer r.mu.Unlock()
	state, ok := r.states[index]
	return state, ok
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
	next, err := s.transact(batch.Signal, func(tx *sql.Tx, current *snapshot) error {
		if err := applyBatch(tx, current, batch); err != nil {
			return err
		}
		_, err := tx.Exec(`UPDATE meta SET value = ? WHERE key = 'applied'`, strconv.FormatUint(index, 10))
		return err
	})
	if err == nil {
		s.recent.put(index, next)
	}
	if err == nil || Refused(err) {
		s.recent.outcome(batch.ID, index, err)
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

// Cluster names the cluster the state belongs to: empty for a node of no
// cluster, which a bootstrap names.
func (s *SQLite) Cluster() string { return s.state.Load().cluster }

// NameCluster names the cluster a bootstrap started, in its first entry.
func (s *SQLite) NameCluster(name, actor string) error {
	_, err := s.write(false, func(tx *recorder, current *snapshot, now time.Time) error {
		if current.cluster == name {
			return errUnchanged
		}
		if err := tx.Exec(`INSERT INTO meta (key, value) VALUES ('cluster', ?) ON CONFLICT (key) DO UPDATE SET value = excluded.value`, name); err != nil {
			return err
		}
		return event(tx, now, actor, "cluster.started", "", "", name)
	})
	if errors.Is(err, errUnchanged) {
		return nil
	}
	return err
}

// Fresh reports whether the state holds nothing yet: a node may only join a
// cluster, and take the cluster's state, with nothing of its own to lose.
func (s *SQLite) Fresh() bool {
	state := s.state.Load()
	return state.version == 0 && len(state.apps) == 0 && len(state.tenants) == 0 && len(state.nodes) == 0 && state.cluster == ""
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
	version, err := checkSnapshot(name)
	if err != nil {
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
		// A snapshot of an older release, such as this node's own from
		// before an upgrade, lacks what migrations added since.
		if version > schemaVersion {
			return fmt.Errorf("%w: a snapshot of schema version %d, this node runs %d", ErrSchema, version, schemaVersion)
		}
		for _, table := range slices.Backward(replicatedTables) {
			if _, err := tx.Exec(`DELETE FROM main.` + table); err != nil {
				return err
			}
		}
		for _, table := range replicatedTables {
			if version < tableSince[table] {
				continue
			}
			columns, err := columnsOf(tx, "snapshot", table)
			if err != nil {
				return err
			}
			if _, err := tx.Exec(`INSERT INTO main.` + table + ` (` + columns + `) SELECT ` + columns + ` FROM snapshot.` + table); err != nil {
				return err
			}
		}
		installed = true
		if _, err := tx.Exec(`DELETE FROM main.meta WHERE key = 'cluster'`); err != nil {
			return err
		}
		if _, err := tx.Exec(`DELETE FROM main.meta WHERE key = 'frozen'`); err != nil {
			return err
		}
		_, err := tx.Exec(`INSERT INTO main.meta (key, value) SELECT key, value FROM snapshot.meta WHERE key IN ('version', 'applied', 'cluster', 'frozen')
			ON CONFLICT (key) DO UPDATE SET value = excluded.value`)
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

// checkSnapshot checks that a snapshot is whole and made by a release that
// replicates, and returns its schema version.
func checkSnapshot(path string) (int, error) {
	db, err := sql.Open("sqlite", uri(path, "mode=ro"))
	if err != nil {
		return 0, err
	}
	defer db.Close()
	var version int
	if err := db.QueryRow(`PRAGMA user_version`).Scan(&version); err != nil {
		return 0, err
	}
	if version < firstBatchSchema {
		return 0, fmt.Errorf("%w: schema version %d comes from no cluster", ErrSchema, version)
	}
	if version > schemaVersion {
		return version, nil // refused unless the node holds it already
	}
	return version, check(db)
}

// columnsOf lists a table's columns, quoted, for copying between schema
// versions.
func columnsOf(tx *sql.Tx, schema, table string) (string, error) {
	rows, err := tx.Query(`SELECT name FROM pragma_table_info(?, ?)`, table, schema)
	if err != nil {
		return "", err
	}
	var columns []string
	for rows.Next() {
		var column string
		if err := rows.Scan(&column); err != nil {
			rows.Close()
			return "", err
		}
		columns = append(columns, `"`+column+`"`)
	}
	if err := closeRows(rows); err != nil {
		return "", err
	}
	if len(columns) == 0 {
		return "", fmt.Errorf("the snapshot has no table %s", table)
	}
	return strings.Join(columns, ", "), nil
}

func (s *SQLite) Nodes() []Node {
	state := s.state.Load()
	nodes := make([]Node, 0, len(state.nodes))
	for _, name := range slices.Sorted(maps.Keys(state.nodes)) {
		// States share what a write did not change: a caller must not
		// change the state through what it gets.
		node := state.nodes[name]
		node.Public = slices.Clone(node.Public)
		if node.Tunnel != nil {
			tunnel := *node.Tunnel
			node.Tunnel = &tunnel
		}
		nodes = append(nodes, node)
	}
	return nodes
}

func (s *SQLite) PutNode(node Node, actor string) error {
	if !nodePattern.MatchString(node.Name) {
		return fmt.Errorf("node name %q must be 1-63 letters, digits, dots or hyphens", node.Name)
	}
	_, err := s.write(false, func(tx *recorder, current *snapshot, now time.Time) error {
		existing, found := current.nodes[node.Name]
		if found && existing.Address == node.Address {
			return errUnchanged
		}
		for _, name := range slices.Sorted(maps.Keys(current.nodes)) {
			other := current.nodes[name]
			if other.Name == node.Name {
				continue
			}
			if other.Address == node.Address {
				return fmt.Errorf("%w: node %s already has address %s", ErrConflict, other.Name, node.Address)
			}
		}
		if err := tx.Exec(`INSERT INTO nodes (name, address, created_at) VALUES (?, ?, ?)
			ON CONFLICT (name) DO UPDATE SET address = excluded.address, copies_refused_until = NULL`,
			node.Name, node.Address, formatTime(now)); err != nil {
			return err
		}
		action := "node.joined"
		if found {
			action = "node.changed"
		}
		return event(tx, now, actor, action, "", "", node.Name+" at "+node.Address)
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
		var tenants []string
		for _, tenant := range slices.Sorted(maps.Keys(current.tenants)) {
			if current.tenants[tenant].Node == name {
				tenants = append(tenants, tenant)
			}
		}
		if len(tenants) > 0 {
			return fmt.Errorf("%w: node %s runs tenants %s; purge them first (archived ones count)", ErrInUse, name, strings.Join(tenants, ", "))
		}
		for _, tenant := range slices.Sorted(maps.Keys(current.tenants)) {
			if current.tenants[tenant].MoveTo == name {
				return fmt.Errorf("%w: tenant %s is moving to node %s; wait until it has moved, or the move is undone", ErrInUse, tenant, name)
			}
		}
		// Copies kept there go with it; placement gives those tenants others.
		if err := tx.Exec(`UPDATE tenants SET replica = '', replica_ready = 0 WHERE replica = ?`, name); err != nil {
			return err
		}
		if err := tx.Exec(`UPDATE tenants SET replica_next = '' WHERE replica_next = ?`, name); err != nil {
			return err
		}
		if err := tx.Exec(`DELETE FROM nodes WHERE name = ?`, name); err != nil {
			return err
		}
		return event(tx, now, actor, "node.removed", "", "", node.Name+" at "+node.Address)
	})
	return err
}

func (s *SQLite) Frozen() string { return s.state.Load().frozen }

// UpgradeFreeze begins the reason of an upgrade's freeze.
const UpgradeFreeze = "upgrade to "

// RotationFreeze begins the reason of a master key rotation's freeze.
const RotationFreeze = "master key rotation from node "

// Freeze refuses changes from now on, on every node, and says why. A freeze
// for the same reason again is no change: an upgrade that stopped resumes
// under its own freeze. Freezes, thaws and notes are written as of the
// first schema version that writes batches, which every release since
// applies: nodes of two releases take them during an upgrade.
func (s *SQLite) Freeze(reason, actor string) error {
	if reason == "" {
		return errors.New("a freeze needs a reason")
	}
	_, err := s.writeAs(firstBatchSchema, false, false, func(tx *recorder, current *snapshot, now time.Time) error {
		switch {
		case current.frozen == reason:
			return errUnchanged
		case current.frozen == "":
		// An upgrade that stopped is taken over by one with a fixed
		// binary, under its own freeze: nodes stay apart until then.
		case strings.HasPrefix(current.frozen, UpgradeFreeze) && strings.HasPrefix(reason, UpgradeFreeze):
		default:
			return fmt.Errorf("%w: %s", ErrFrozen, current.frozen)
		}
		if err := tx.Exec(`INSERT INTO meta (key, value) VALUES ('frozen', ?) ON CONFLICT (key) DO UPDATE SET value = excluded.value`, reason); err != nil {
			return err
		}
		return event(tx, now, actor, "changes.frozen", "", "", reason)
	})
	if errors.Is(err, errUnchanged) {
		return nil
	}
	return err
}

// Thaw takes changes again.
func (s *SQLite) Thaw(actor string) error {
	return s.thaw("", actor)
}

// ThawOwn takes changes again only if the freeze is still the one for
// reason: one someone else took meanwhile stays.
func (s *SQLite) ThawOwn(reason, actor string) error {
	return s.thaw(reason, actor)
}

func (s *SQLite) thaw(reason, actor string) error {
	_, err := s.writeAs(firstBatchSchema, false, false, func(tx *recorder, current *snapshot, now time.Time) error {
		if current.frozen == "" || reason != "" && current.frozen != reason {
			return errUnchanged
		}
		if err := tx.Exec(`DELETE FROM meta WHERE key = 'frozen'`); err != nil {
			return err
		}
		return event(tx, now, actor, "changes.thawed", "", "", "")
	})
	if errors.Is(err, errUnchanged) {
		return nil
	}
	return err
}

// Note records an event that changes nothing else, frozen or not, such as
// a binary installed during an upgrade.
func (s *SQLite) Note(action, detail, actor string) error {
	_, err := s.writeAs(firstBatchSchema, false, false, func(tx *recorder, _ *snapshot, now time.Time) error {
		return event(tx, now, actor, action, "", "", detail)
	})
	return err
}

func (s *SQLite) StartMove(name, target, actor string) (Record, error) {
	next, err := s.write(true, func(tx *recorder, current *snapshot, now time.Time) error {
		record, ok := current.tenants[name]
		switch {
		case !ok:
			return fmt.Errorf("tenant %s: %w", name, ErrNotFound)
		case record.Node != current.node:
			return fmt.Errorf("%w: tenant %s runs on node %s: move it from there", ErrConflict, name, record.Node)
		case record.MoveTo != "":
			return fmt.Errorf("%w: tenant %s is already moving to node %s", ErrConflict, name, record.MoveTo)
		case target == record.Node:
			return fmt.Errorf("%w: tenant %s runs on node %s already", ErrConflict, name, target)
		}
		if node, member := current.nodes[target]; !member {
			return fmt.Errorf("node %s: %w", target, ErrNotFound)
		} else if node.Failed() {
			return fmt.Errorf("%w: node %s failed and has not answered since", ErrConflict, target)
		}
		// A move onto the replica's node writes over its copy: it is not
		// whole again until replication says so. A copy moving elsewhere
		// stops: the move settles where the tenant and its copy are.
		if err := tx.Exec(`UPDATE tenants SET move_to = ?, replica_ready = CASE WHEN replica = ? THEN 0 ELSE replica_ready END, replica_next = '', updated_at = ? WHERE name = ?`, target, target, formatTime(now), name); err != nil {
			return err
		}
		return event(tx, now, actor, "tenant.moving", name, record.App, record.Node+" → "+target)
	})
	if err != nil {
		return Record{}, err
	}
	return next.tenants[name].clone(), nil
}

func (s *SQLite) HoldMove(name string) error {
	_, err := s.write(true, func(tx *recorder, current *snapshot, _ time.Time) error {
		record, ok := current.tenants[name]
		if !ok || record.MoveTo == "" {
			return fmt.Errorf("tenant %s is not moving: %w", name, ErrConflict)
		}
		if err := targetMember(current, record); err != nil {
			return err
		}
		return tx.Exec(`UPDATE tenants SET move_held = 1 WHERE name = ?`, name)
	})
	return err
}

func (s *SQLite) FinishMove(name, actor string) (Record, error) {
	next, err := s.write(true, func(tx *recorder, current *snapshot, now time.Time) error {
		record, ok := current.tenants[name]
		if !ok || record.MoveTo == "" {
			return fmt.Errorf("tenant %s is not moving: %w", name, ErrConflict)
		}
		if err := targetMember(current, record); err != nil {
			return err
		}
		// A tenant moved onto its replica's node needs another replica.
		if err := tx.Exec(`UPDATE tenants SET node = move_to, move_to = '', replica = CASE WHEN replica = move_to THEN '' ELSE replica END, replica_ready = 0, replica_next = '', generation = generation + 1, updated_at = ? WHERE name = ?`, formatTime(now), name); err != nil {
			return err
		}
		return event(tx, now, actor, "tenant.moved", name, record.App, record.Node+" → "+record.MoveTo)
	})
	if err != nil {
		return Record{}, err
	}
	return next.tenants[name].clone(), nil
}

// targetMember refuses to hand a tenant to a node that left the cluster:
// nothing could run it or move it back.
func targetMember(current *snapshot, record Record) error {
	if _, member := current.nodes[record.MoveTo]; !member {
		return fmt.Errorf("%w: node %s, which tenant %s was moving to, left the cluster", ErrConflict, record.MoveTo, record.Spec.Name)
	}
	return nil
}

// moveSchema is the schema version that added moves. Cancelling a move and
// serving a moved tenant go on while changes are frozen, so that a move
// under way ends; their batches are marked as of this version, which every
// node that can be part of a move applies, during an upgrade too.
const moveSchema = 7

func (s *SQLite) CancelMove(name, actor string) (bool, error) {
	_, err := s.writeAs(moveSchema, false, true, func(tx *recorder, current *snapshot, now time.Time) error {
		record, ok := current.tenants[name]
		if !ok || record.MoveTo == "" {
			return errUnchanged
		}
		if err := tx.Exec(`UPDATE tenants SET move_to = '', move_held = 0, updated_at = ? WHERE name = ?`, formatTime(now), name); err != nil {
			return err
		}
		return event(tx, now, actor, "tenant.move-cancelled", name, record.App, "stays on "+record.Node)
	})
	if errors.Is(err, errUnchanged) {
		return false, nil
	}
	return err == nil, err
}

func (s *SQLite) ReleaseHold(name string) error {
	_, err := s.writeAs(moveSchema, false, true, func(tx *recorder, current *snapshot, _ time.Time) error {
		if record, ok := current.tenants[name]; !ok || !record.MoveHeld {
			return errUnchanged
		}
		return tx.Exec(`UPDATE tenants SET move_held = 0 WHERE name = ?`, name)
	})
	if errors.Is(err, errUnchanged) {
		return nil
	}
	return err
}
