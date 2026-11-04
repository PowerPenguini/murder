package store

import (
	"context"
	"crypto/rand"
	"database/sql"
	"encoding/binary"
	"encoding/json"
	"errors"
	"fmt"
	"maps"
	mathrand "math/rand/v2"
	"net/netip"
	"net/url"
	"os"
	"path/filepath"
	"regexp"
	"slices"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"syscall"
	"time"

	_ "modernc.org/sqlite"

	"github.com/PowerPenguini/murder/internal/spec"
)

const subnetBits = 28

var nodePattern = regexp.MustCompile(`^[A-Za-z0-9][A-Za-z0-9.-]{0,62}$`)

// SQLite is the store in one SQLite database. Reads come from a snapshot of
// the whole state in memory, rebuilt inside every write transaction before
// it commits, so memory and disk always agree.
type SQLite struct {
	path    string
	db      *sql.DB
	options Options
	now     func() time.Time
	lock    *os.File
	mu      sync.Mutex // serializes writers
	// applying serializes transactions and the publication of their state.
	applying sync.Mutex
	// commit makes a batch part of the state: applies it here, or, in a
	// cluster, has the cluster's log apply it on every node.
	commit func(Batch) (*snapshot, error)
	// catchUp, in a cluster, applies here what the cluster has applied.
	catchUp func() error
	state   atomic.Pointer[snapshot]
	changed chan struct{}
	// applied is the index of the last cluster log entry applied, or
	// refused: an entry refused on one node is refused on every node.
	applied appliedIndex
	recent  recentStates
	// published is closed, and replaced, whenever a new state is published.
	publishMu sync.Mutex
	published chan struct{}
}

type snapshot struct {
	node string
	// version counts the batches applied to the replicated state.
	version int64
	// cluster names the cluster the state belongs to; empty outside one.
	cluster string
	// frozen, while an upgrade runs, says why changes are refused.
	frozen string
	apps   map[string]App
	// revisions are each app's recent revisions, oldest first.
	revisions map[string][]Revision
	// rollouts are each app's newest rollout.
	rollouts map[string]Rollout
	tenants  map[string]Record
	nodes    map[string]Node
	// admissions are the hosts an invitation let in, by name.
	admissions map[string]Admission
	// departed are the nodes taken out of the state, with the identity
	// keys they had, by name.
	departed map[string]Departure
	devices  map[string]Device
	panel    panelState
	images   map[string]bool
	// lastEvent is the newest event the snapshot includes; readers of the
	// audit log see nothing newer, so no event shows before its change.
	lastEvent int64
}

const pragmas = "_pragma=foreign_keys(1)&_pragma=synchronous(FULL)&_pragma=busy_timeout(5000)&_txlock=immediate"

// dsn escapes path, so "#", "?" or "%" in it cannot be read as URI syntax
// and open another file than the one Init created and Open locks.
func dsn(path string) string { return uri(path, pragmas+"&_pragma=journal_mode(WAL)") }

// initDSN leaves the journal mode alone, so init does not change a database
// it then refuses; Open switches it to WAL.
func initDSN(path string) string { return uri(path, pragmas) }

func uri(path, query string) string {
	// A relative path would read as a URI authority.
	if absolute, err := filepath.Abs(path); err == nil {
		path = absolute
	}
	uri := url.URL{Scheme: "file", Path: path, RawQuery: query}
	return uri.String()
}

func openDB(dsn string) (*sql.DB, error) {
	db, err := sql.Open("sqlite", dsn)
	if err != nil {
		return nil, err
	}
	// One connection: writes are serialized anyway, and the pragmas above
	// then hold for every statement.
	db.SetMaxOpenConns(1)
	if err := db.Ping(); err != nil {
		_ = db.Close()
		return nil, err
	}
	return db, nil
}

// Init creates the database for node at path, refusing to overwrite one.
// The schema and the node name are written in one transaction, so an
// interrupted init leaves an empty database, which the next init completes.
func Init(path, node string) error {
	if !nodePattern.MatchString(node) {
		return fmt.Errorf("node name %q must be 1-63 letters, digits, dots or hyphens", node)
	}
	dir := filepath.Dir(path)
	if err := os.MkdirAll(dir, 0o700); err != nil {
		return err
	}
	// Owner-only from the start; SQLite gives its -wal and -shm files the
	// database's mode.
	file, err := os.OpenFile(path, os.O_RDWR|os.O_CREATE, 0o600)
	if err != nil {
		return err
	}
	_ = file.Close()
	db, err := openDB(initDSN(path))
	if err != nil {
		return err
	}
	if err := errors.Join(initDB(db, path, node), db.Close()); err != nil {
		return err
	}
	return syncDir(dir)
}

func initDB(db *sql.DB, path, node string) error {
	tx, err := db.Begin()
	if err != nil {
		return err
	}
	defer tx.Rollback()
	var version, objects int
	if err := tx.QueryRow(`PRAGMA user_version`).Scan(&version); err != nil {
		return fmt.Errorf("database %s: %w", path, err)
	}
	if err := tx.QueryRow(`SELECT count(*) FROM sqlite_schema`).Scan(&objects); err != nil {
		return err
	}
	if version != 0 || objects != 0 {
		return fmt.Errorf("database %s already exists", path)
	}
	if _, err := tx.Exec(schema); err != nil {
		return err
	}
	if _, err := tx.Exec(`INSERT INTO meta (key, value) VALUES ('node', ?), ('version', '0'), ('applied', '0')`, node); err != nil {
		return err
	}
	if _, err := tx.Exec(fmt.Sprintf(`PRAGMA user_version = %d`, schemaVersion)); err != nil {
		return err
	}
	return tx.Commit()
}

func syncDir(dir string) error {
	directory, err := os.Open(dir)
	if err != nil {
		return err
	}
	defer directory.Close()
	return directory.Sync()
}

// Open opens the database at path and takes an exclusive lock on it, so a
// second agent cannot run on the same state. A missing database is an
// error, not an empty state: an empty state would remove every tenant's
// containers.
func Open(path string, options Options) (*SQLite, error) {
	if !options.NetworkPool.IsValid() || !options.NetworkPool.Addr().Is4() || options.NetworkPool.Bits() > subnetBits-4 {
		return nil, fmt.Errorf("network pool %v must be an IPv4 prefix of /%d or larger", options.NetworkPool, subnetBits-4)
	}
	if options.Sealer == nil {
		return nil, errors.New("a store needs a sealer for secrets")
	}
	if _, err := os.Stat(path); errors.Is(err, os.ErrNotExist) {
		return nil, fmt.Errorf("database %s does not exist; run \"murder init\" on a new node, or restore it from a backup", path)
	} else if err != nil {
		return nil, err
	}
	lock, err := os.OpenFile(path+".lock", os.O_CREATE|os.O_RDWR, 0o600)
	if err != nil {
		return nil, err
	}
	if err := syscall.Flock(int(lock.Fd()), syscall.LOCK_EX|syscall.LOCK_NB); err != nil {
		_ = lock.Close()
		return nil, fmt.Errorf("database %s is in use by another agent", path)
	}
	if err := ownerOnly(path); err != nil {
		_ = lock.Close()
		return nil, err
	}
	removeStale(path)
	db, err := openDB(dsn(path))
	if err != nil {
		_ = lock.Close()
		return nil, err
	}
	s := &SQLite{path: path, db: db, options: options, now: time.Now, lock: lock, changed: make(chan struct{}, 1)}
	s.commit = s.apply
	if err := checkSchema(db); err != nil {
		_ = s.Close()
		return nil, fmt.Errorf("database %s: %w", path, err)
	}
	if err := upgrade(db); err != nil {
		_ = s.Close()
		return nil, fmt.Errorf("database %s: upgrade the schema: %w", path, err)
	}
	if err := checkKeys(db); err != nil {
		_ = s.Close()
		return nil, fmt.Errorf("database %s: %w", path, err)
	}
	state, err := load(db)
	if err != nil {
		_ = s.Close()
		return nil, fmt.Errorf("database %s: %w", path, err)
	}
	s.state.Store(state)
	var applied string
	if err := db.QueryRow(`SELECT value FROM meta WHERE key = 'applied'`).Scan(&applied); err != nil {
		_ = s.Close()
		return nil, fmt.Errorf("database %s: read the applied log index: %w", path, err)
	}
	index, err := strconv.ParseUint(applied, 10, 64)
	if err != nil {
		_ = s.Close()
		return nil, fmt.Errorf("database %s: applied log index %q: %w", path, applied, err)
	}
	s.applied.set(index)
	return s, nil
}

// Subnets returns every tenant's subnet, archived or not, from the database
// at path, which it only reads, without the agent's lock: the tenant
// firewall loads before the agent runs, and while it runs. A missing
// database has none.
func Subnets(path string) ([]netip.Prefix, error) {
	if _, err := os.Stat(path); errors.Is(err, os.ErrNotExist) {
		return nil, nil
	}
	db, err := sql.Open("sqlite", uri(path, "mode=ro&_pragma=busy_timeout(5000)"))
	if err != nil {
		return nil, err
	}
	defer db.Close()
	if err := checkSchema(db); err != nil {
		return nil, fmt.Errorf("database %s: %w", path, err)
	}
	rows, err := db.Query(`SELECT subnet FROM tenants`)
	if err != nil {
		return nil, err
	}
	var subnets []netip.Prefix
	for rows.Next() {
		var text string
		if err := rows.Scan(&text); err != nil {
			rows.Close()
			return nil, err
		}
		subnet, err := netip.ParsePrefix(text)
		if err != nil {
			rows.Close()
			return nil, fmt.Errorf("database %s: subnet %q: %w", path, text, err)
		}
		subnets = append(subnets, subnet)
	}
	return subnets, closeRows(rows)
}

// ownerOnly makes a database, and its write-ahead log files, readable by
// their owner only, as Init creates them: one copied into place from a
// backup may have come with a wider mode. It changes nothing already
// private, so a database owned by someone else that is private still opens.
func ownerOnly(path string) error {
	for _, file := range []string{path, path + "-wal", path + "-shm"} {
		info, err := os.Stat(file)
		if errors.Is(err, os.ErrNotExist) {
			continue
		}
		if err != nil {
			return err
		}
		if info.Mode().Perm()&0o077 == 0 {
			continue
		}
		if err := os.Chmod(file, info.Mode().Perm()&0o700); err != nil {
			return fmt.Errorf("%s is readable by others (mode %v) and cannot be made private: %w", file, info.Mode().Perm(), err)
		}
	}
	return nil
}

// Close closes the database and releases its lock.
func (s *SQLite) Close() error {
	err := s.db.Close()
	return errors.Join(err, s.lock.Close())
}

// Node is the node this state belongs to; it labels the node's Docker
// objects, so it must survive host renames.
func (s *SQLite) Node() string { return s.state.Load().node }

func (s *SQLite) Changed() <-chan struct{} { return s.changed }

// Watch returns a channel closed when the next state is published, for any
// number of watchers; each takes a new channel after every change.
func (s *SQLite) Watch() <-chan struct{} {
	s.publishMu.Lock()
	defer s.publishMu.Unlock()
	if s.published == nil {
		s.published = make(chan struct{})
	}
	return s.published
}

func (s *SQLite) publish() {
	s.publishMu.Lock()
	defer s.publishMu.Unlock()
	if s.published != nil {
		close(s.published)
		s.published = nil
	}
}

// querier is what load needs: a database or a transaction.
type querier interface {
	Query(query string, args ...any) (*sql.Rows, error)
	QueryRow(query string, args ...any) *sql.Row
}

// load reads the whole state. It is small: one row per app and tenant.
func load(q querier) (*snapshot, error) {
	state := &snapshot{}
	for _, part := range []func(querier, *snapshot) error{loadMeta, loadApps, loadRevisions, loadRollouts, loadAllTenants, loadNodes, loadDevices, loadPanel, loadImages} {
		if err := part(q, state); err != nil {
			return nil, err
		}
	}
	return state, nil
}

// loadMeta reads the node's name, the state's version, its cluster, its
// freeze and its newest event.
func loadMeta(q querier, state *snapshot) error {
	var err error
	if err := q.QueryRow(`SELECT value FROM meta WHERE key = 'node'`).Scan(&state.node); err != nil {
		return fmt.Errorf("read the node name: %w", err)
	}
	var version string
	if err := q.QueryRow(`SELECT value FROM meta WHERE key = 'version'`).Scan(&version); err != nil {
		return fmt.Errorf("read the state version: %w", err)
	}
	if state.version, err = strconv.ParseInt(version, 10, 64); err != nil {
		return fmt.Errorf("state version %q: %w", version, err)
	}
	if err := q.QueryRow(`SELECT COALESCE((SELECT value FROM meta WHERE key = 'cluster'), ''), COALESCE((SELECT value FROM meta WHERE key = 'frozen'), '')`).Scan(&state.cluster, &state.frozen); err != nil {
		return fmt.Errorf("read the cluster: %w", err)
	}
	if err := q.QueryRow(`SELECT COALESCE(MAX(id), 0) FROM events`).Scan(&state.lastEvent); err != nil {
		return err
	}
	return nil
}

func loadApps(q querier, state *snapshot) error {
	state.apps = map[string]App{}
	rows, err := q.Query(`SELECT name, spec, created_at, updated_at, tag, revision, latest, latest_at FROM apps`)
	if err != nil {
		return err
	}
	for rows.Next() {
		var app App
		var specJSON, created, updated string
		var latestAt sql.NullString
		if err := rows.Scan(&app.Name, &specJSON, &created, &updated, &app.Tag, &app.Revision, &app.Latest, &latestAt); err != nil {
			rows.Close()
			return err
		}
		if err := json.Unmarshal([]byte(specJSON), &app.Spec); err != nil {
			rows.Close()
			return fmt.Errorf("app %s: %w", app.Name, err)
		}
		app.CreatedAt, app.UpdatedAt = parseTime(created), parseTime(updated)
		if latestAt.Valid {
			app.LatestAt = parseTime(latestAt.String)
		}
		state.apps[app.Name] = app
	}
	if err := closeRows(rows); err != nil {
		return err
	}
	return nil
}

func loadAllTenants(q querier, state *snapshot) error {
	state.tenants = map[string]Record{}
	return loadTenants(q, state, nil)
}

// loadTenants reads the tenants named, or all of them if names is nil, into
// state.tenants, composing each spec from state.apps. A tenant named that
// is not stored is left out.
func loadTenants(q querier, state *snapshot, names []string) error {
	var args []any
	where := func(string) string { return "" }
	if names != nil {
		for _, name := range names {
			args = append(args, name)
		}
		marks := strings.TrimSuffix(strings.Repeat("?, ", len(names)), ", ")
		where = func(column string) string { return " WHERE " + column + " IN (" + marks + ")" }
	}
	sealed := map[string]map[string]string{}
	rows, err := q.Query(`SELECT tenant, name, sealed FROM secrets`+where("tenant"), args...)
	if err != nil {
		return err
	}
	for rows.Next() {
		var tenant, name, value string
		if err := rows.Scan(&tenant, &name, &value); err != nil {
			rows.Close()
			return err
		}
		if sealed[tenant] == nil {
			sealed[tenant] = map[string]string{}
		}
		sealed[tenant][name] = value
	}
	if err := closeRows(rows); err != nil {
		return err
	}
	rows, err = q.Query(`SELECT name, app, overrides, suspended, subnet, data_image, previous_image, generation, created_at, updated_at, archived_at, node, rollout, move_to, move_held, replica, replica_ready, replica_next, app_revision, before_revision, canary, hold, restore, moved_on FROM tenants`+where("name"), args...)
	if err != nil {
		return err
	}
	for rows.Next() {
		var record Record
		var name, overrides, created, updated string
		var suspended bool
		var archivedAt sql.NullString
		if err := rows.Scan(&name, &record.App, &overrides, &suspended, &record.Subnet, &record.DataImage, &record.PreviousImage, &record.Generation, &created, &updated, &archivedAt, &record.Node, &record.Rollout, &record.MoveTo, &record.MoveHeld, &record.Replica, &record.ReplicaReady, &record.ReplicaNext, &record.AppRevision, &record.BeforeRevision, &record.Canary, &record.Hold, &record.Restore, &record.MovedOn); err != nil {
			rows.Close()
			return err
		}
		if err := json.Unmarshal([]byte(overrides), &record.Overrides); err != nil {
			rows.Close()
			return fmt.Errorf("tenant %s: %w", name, err)
		}
		record.Spec = spec.Compose(revisionSpec(state, record.App, record.AppRevision), name, record.Overrides)
		if record.BeforeRevision != 0 {
			record.BeforeImage = spec.Compose(revisionSpec(state, record.App, record.BeforeRevision), name, record.Overrides).Image
		}
		record.Spec.Suspended = suspended
		record.Sealed = sealed[name]
		record.CreatedAt, record.UpdatedAt = parseTime(created), parseTime(updated)
		if archivedAt.Valid {
			record.ArchivedAt = parseTime(archivedAt.String)
		}
		state.tenants[name] = record
	}
	if err := closeRows(rows); err != nil {
		return err
	}
	return nil
}

func loadNodes(q querier, state *snapshot) error {
	state.nodes = map[string]Node{}
	rows, err := q.Query(`SELECT name, address, created_at, failed_at, away_until, copies_refused_until, public, tunnel, key FROM nodes`)
	if err != nil {
		return err
	}
	for rows.Next() {
		var node Node
		var created string
		var failed, away, refused sql.NullString
		var public, tunnel string
		if err := rows.Scan(&node.Name, &node.Address, &created, &failed, &away, &refused, &public, &tunnel, &node.Key); err != nil {
			rows.Close()
			return err
		}
		node.Tunnel = parseTunnel(tunnel)
		if refused.Valid {
			node.CopiesRefusedUntil = parseTime(refused.String)
		}
		for _, field := range strings.Fields(public) {
			if address, err := netip.ParseAddr(field); err == nil {
				node.Public = append(node.Public, address)
			}
		}
		node.CreatedAt = parseTime(created)
		if failed.Valid {
			node.FailedAt = parseTime(failed.String)
		}
		if away.Valid {
			node.AwayUntil = parseTime(away.String)
		}
		state.nodes[node.Name] = node
	}
	if err := closeRows(rows); err != nil {
		return err
	}
	return loadAdmissions(q, state)
}

// loadAdmissions reads the hosts an invitation let in; the nodes' part
// of the state holds them.
func loadAdmissions(q querier, state *snapshot) error {
	state.admissions = map[string]Admission{}
	rows, err := q.Query(`SELECT name, key, expires_at, actor FROM admissions`)
	if err != nil {
		return err
	}
	for rows.Next() {
		var admission Admission
		var expires string
		if err := rows.Scan(&admission.Name, &admission.Key, &expires, &admission.Actor); err != nil {
			rows.Close()
			return err
		}
		admission.Expires = parseTime(expires)
		state.admissions[admission.Name] = admission
	}
	if err := closeRows(rows); err != nil {
		return err
	}
	state.departed = map[string]Departure{}
	rows, err = q.Query(`SELECT name, key, seq FROM departed_keys`)
	if err != nil {
		return err
	}
	for rows.Next() {
		var departure Departure
		if err := rows.Scan(&departure.Name, &departure.Key, &departure.Seq); err != nil {
			rows.Close()
			return err
		}
		state.departed[departure.Name] = departure
	}
	return closeRows(rows)
}

func loadDevices(q querier, state *snapshot) error {
	state.devices = map[string]Device{}
	rows, err := q.Query(`SELECT name, key, address, created_at, created_by FROM devices`)
	if err != nil {
		return err
	}
	for rows.Next() {
		var device Device
		var address, created string
		if err := rows.Scan(&device.Name, &device.Key, &address, &created, &device.CreatedBy); err != nil {
			rows.Close()
			return err
		}
		device.Address, _ = netip.ParseAddr(address)
		device.CreatedAt = parseTime(created)
		state.devices[device.Name] = device
	}
	if err := closeRows(rows); err != nil {
		return err
	}
	return nil
}

func loadImages(q querier, state *snapshot) error {
	state.images = map[string]bool{}
	rows, err := q.Query(`SELECT ref FROM images`)
	if err != nil {
		return err
	}
	for rows.Next() {
		var ref string
		if err := rows.Scan(&ref); err != nil {
			rows.Close()
			return err
		}
		state.images[ref] = true
	}
	if err := closeRows(rows); err != nil {
		return err
	}
	return nil
}

// recordImages adds every image a tenant now has to the images table.
func recordImages(tx *sql.Tx, state *snapshot) error {
	for _, record := range state.tenants {
		for _, image := range []string{record.Spec.Image, record.DataImage, record.PreviousImage} {
			if image == "" || state.images[image] {
				continue
			}
			if _, err := tx.Exec(`INSERT INTO images (ref) VALUES (?) ON CONFLICT DO NOTHING`, image); err != nil {
				return err
			}
			state.images[image] = true
		}
	}
	return nil
}

func closeRows(rows *sql.Rows) error {
	err := rows.Err()
	return errors.Join(err, rows.Close())
}

func formatTime(t time.Time) string { return t.UTC().Format(time.RFC3339Nano) }

func parseTime(text string) time.Time {
	t, _ := time.Parse(time.RFC3339Nano, text)
	return t
}

// write computes a change against the current state as a batch of
// statements and commits it. A batch computed against a state that has
// changed since is stale and computed again; writers on one node are
// serialized, so only writes from other nodes can make it stale.
func (s *SQLite) write(signal bool, change func(tx *recorder, current *snapshot, now time.Time) error) (*snapshot, error) {
	return s.writeIf(true, signal, change)
}

// writeIf is write, refusing changes while the state is frozen unless
// unlessFrozen is false.
func (s *SQLite) writeIf(unlessFrozen, signal bool, change func(tx *recorder, current *snapshot, now time.Time) error) (*snapshot, error) {
	return s.writeAs(schemaVersion, unlessFrozen, signal, change)
}

// writeAs is writeIf for a batch marked as of schema version schema: one
// whose statements every release since runs alike, so nodes of older
// releases apply it during an upgrade.
func (s *SQLite) writeAs(schema int, unlessFrozen, signal bool, change func(tx *recorder, current *snapshot, now time.Time) error) (*snapshot, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	for attempt := 1; ; attempt++ {
		if s.catchUp != nil {
			if err := s.catchUp(); err != nil {
				return nil, err
			}
		}
		current := s.state.Load()
		if unlessFrozen && current.frozen != "" {
			return nil, fmt.Errorf("%w: %s", ErrFrozen, current.frozen)
		}
		tx := &recorder{}
		if err := change(tx, current, s.now().UTC()); err != nil {
			return nil, err
		}
		next, err := s.commit(Batch{ID: rand.Text(), Schema: schema, Version: current.version, Signal: signal, Statements: tx.statements, LeaderOnly: tx.leaderOnly})
		if !errors.Is(err, ErrStale) || attempt == staleAttempts {
			return next, err
		}
		// Writers racing on other nodes back off by different times.
		time.Sleep(time.Duration(mathrand.Int64N(int64(attempt) * int64(20*time.Millisecond))))
	}
}

// staleAttempts is how often a write is computed before a stale batch is
// reported: often enough for writes racing on other nodes to pass.
const staleAttempts = 10

// Apply applies a batch committed elsewhere, such as by the cluster's log,
// in one transaction. It refuses, with ErrStale and without a change, a
// batch computed against another version of the state. The new state is
// published only once the transaction has committed.
func (s *SQLite) Apply(batch Batch) error {
	_, err := s.apply(batch)
	return err
}

func (s *SQLite) apply(batch Batch) (*snapshot, error) {
	return s.transactBatch(batch.Signal, func(tx *sql.Tx, current *snapshot, w *watched) error {
		return applyBatch(tx, current, batch, w, nil)
	})
}

// applyBatch applies a batch of this schema version or an older one: a
// migration keeps the statements of earlier releases valid, so a node
// upgraded first still applies what the others write.
func applyBatch(tx *sql.Tx, current *snapshot, batch Batch, w *watched, then func() error) error {
	if batch.Schema < firstBatchSchema || batch.Schema > schemaVersion {
		return fmt.Errorf("%w: a batch for schema version %d, this node runs %d", ErrSchema, batch.Schema, schemaVersion)
	}
	if batch.Version != current.version {
		return fmt.Errorf("%w: computed against version %d, the state is at %d", ErrStale, batch.Version, current.version)
	}
	// The batch's statements run under batchAuthorizer, and so does the
	// store's count of the version and applied index that follows: a
	// trigger they fire is held to it too.
	if err := w.authorize(batchAuthorizer); err != nil {
		return err
	}
	defer w.authorize(nil)
	w.changes.meta = map[string]bool{}
	for _, statement := range batch.Statements {
		args, err := statement.values()
		if err != nil {
			return refusal{err}
		}
		if _, err := tx.Exec(statement.SQL, args...); err != nil {
			if nodeFailure(err) {
				return err
			}
			return refusal{err}
		}
	}
	err := w.changes.bookkeeping()
	w.changes.meta = nil
	if err != nil {
		return err
	}
	if err := setMeta(tx, "version", strconv.FormatInt(current.version+1, 10)); err != nil {
		return err
	}
	if then != nil {
		return then()
	}
	return nil
}

// setMeta sets one of the meta rows the store keeps, which must be there:
// a database without it is broken, and the change is not made.
func setMeta(tx *sql.Tx, key, value string) error {
	result, err := tx.Exec(`UPDATE meta SET value = ? WHERE key = ?`, value, key)
	if err != nil {
		if nodeFailure(err) {
			return err
		}
		return refusal{err}
	}
	if rows, err := result.RowsAffected(); err != nil || rows != 1 {
		return fmt.Errorf("the database's meta table has %d rows %q, not one: it is broken (%v)", rows, key, err)
	}
	return nil
}

// local changes what only this node keeps, such as the images it pulled,
// outside the replicated state.
func (s *SQLite) local(change func(tx *sql.Tx) error) error {
	_, err := s.transact(false, func(tx *sql.Tx, _ *snapshot) error { return change(tx) })
	return err
}

// transact runs change in one transaction, rebuilds the snapshot inside it
// and makes the snapshot current only once the transaction has committed.
func (s *SQLite) transact(signal bool, change func(tx *sql.Tx, current *snapshot) error) (*snapshot, error) {
	s.applying.Lock()
	defer s.applying.Unlock()
	return s.transactLocked(signal, change)
}

func (s *SQLite) transactLocked(signal bool, change func(tx *sql.Tx, current *snapshot) error) (*snapshot, error) {
	return s.transactWatched(signal, func(tx *sql.Tx, current *snapshot, _ *watched) error { return change(tx, current) })
}

// transactBatch is transact for applying a batch, whose statements run
// under batchAuthorizer.
func (s *SQLite) transactBatch(signal bool, change func(tx *sql.Tx, current *snapshot, w *watched) error) (*snapshot, error) {
	s.applying.Lock()
	defer s.applying.Unlock()
	return s.transactWatched(signal, change)
}

// transactWatched is transactLocked, handing change the connection it
// runs on; the caller holds s.applying.
func (s *SQLite) transactWatched(signal bool, change func(tx *sql.Tx, current *snapshot, w *watched) error) (*snapshot, error) {
	ctx := context.Background()
	watched, err := s.watch(ctx)
	if err != nil {
		return nil, err
	}
	defer watched.done()
	tx, err := watched.conn.BeginTx(ctx, nil)
	if err != nil {
		return nil, err
	}
	defer tx.Rollback()
	current := s.state.Load()
	if err := change(tx, current, watched); err != nil {
		return nil, err
	}
	next, err := reload(tx, current, watched.changes)
	if err != nil {
		return nil, err
	}
	if verifyReloads {
		if err := verifyReload(tx, next); err != nil {
			return nil, err
		}
	}
	if err := recordImages(tx, next); err != nil {
		return nil, err
	}
	if err := tx.Commit(); err != nil {
		return nil, err
	}
	s.state.Store(next)
	s.publish()
	if signal {
		select {
		case s.changed <- struct{}{}:
		default:
		}
	}
	return next, nil
}

func event(tx *recorder, now time.Time, actor, action, tenant, app, detail string) error {
	return tx.Exec(`INSERT INTO events (at, actor, action, tenant, app, detail) VALUES (?, ?, ?, ?, ?, ?)`,
		formatTime(now), actorOf(actor), action, tenant, app, detail)
}

// sealMissing generates a value for every secret of tenant that has none.
func (s *SQLite) sealMissing(tx *recorder, now time.Time, tenant spec.Tenant, sealed map[string]string) error {
	for _, name := range slices.Sorted(maps.Keys(tenant.Secrets)) {
		if _, ok := sealed[name]; ok {
			continue
		}
		value, err := s.options.Sealer(tenant.Name, name, tenant.Secrets[name])
		if err != nil {
			return fmt.Errorf("secret %s: %w", name, err)
		}
		if err := tx.Exec(`INSERT INTO secrets (tenant, name, sealed, created_at) VALUES (?, ?, ?, ?)`, tenant.Name, name, value, formatTime(now)); err != nil {
			return err
		}
	}
	return nil
}

func marshal(value any) string {
	data, err := json.Marshal(value)
	if err != nil {
		panic(err) // only plain structs and maps are marshalled
	}
	return string(data)
}

func (s *SQLite) Apps() []App {
	state := s.state.Load()
	apps := make([]App, 0, len(state.apps))
	for _, name := range slices.Sorted(maps.Keys(state.apps)) {
		apps = append(apps, cloneApp(state.apps[name]))
	}
	return apps
}

func (s *SQLite) App(name string) (App, error) {
	app, ok := s.state.Load().apps[name]
	if !ok {
		return App{}, fmt.Errorf("app %s: %w", name, ErrNotFound)
	}
	return cloneApp(app), nil
}

func cloneApp(app App) App {
	app.Spec.Env = maps.Clone(app.Spec.Env)
	app.Spec.Secrets = maps.Clone(app.Spec.Secrets)
	app.Spec.Tmpfs = maps.Clone(app.Spec.Tmpfs)
	return app
}

// normalizeApp is the form an app spec is stored in.
func normalizeApp(app spec.Tenant) spec.Tenant {
	app = app.WithDefaults()
	app.Name, app.Suspended = "", false
	return app
}

// checkTenants returns the live tenants of app that the new spec would make
// invalid. A tenant already invalid (under older rules) does not block the
// app: the reconciler does not run it either way. Archived tenants are
// checked when they are restored, and can be reconfigured meanwhile.
func checkTenants(state *snapshot, app string, next spec.Tenant, validate func(spec.Tenant) error) error {
	var problems []string
	for _, name := range slices.Sorted(maps.Keys(state.tenants)) {
		record := state.tenants[name]
		if record.App != app || record.Archived() || record.Spec.Validate() != nil {
			continue
		}
		if err := validate(spec.Compose(next, name, record.Overrides)); err != nil {
			problems = append(problems, fmt.Sprintf("tenant %s: %v", name, err))
		}
	}
	if len(problems) > 0 {
		return &spec.ValidationError{Problems: problems}
	}
	return nil
}

// CheckApp checks an app whose image may still be a tag, against the
// tenants it would change: everything worth knowing before pulling.
func (s *SQLite) CheckApp(app App) error {
	if err := spec.CheckApp(app.Name, app.Spec, spec.Tenant.ValidateUnpinned); err != nil {
		return err
	}
	return checkTenants(s.state.Load(), app.Name, normalizeApp(app.Spec), spec.Tenant.ValidateUnpinned)
}

func (s *SQLite) PutApp(app App, actor string) (AppChange, error) {
	return s.PutAppIf(app, AppIf{}, actor)
}

func (s *SQLite) PutAppIf(app App, cond AppIf, actor string) (AppChange, error) {
	return s.putApp(app, cond, 0, actor)
}

// putApp stores app as its newest revision; from, if set, is the earlier
// revision it brings back.
func (s *SQLite) putApp(app App, cond AppIf, from int64, actor string) (AppChange, error) {
	if err := spec.CheckApp(app.Name, app.Spec, spec.Tenant.Validate); err != nil {
		return AppChange{}, err
	}
	app.Spec = normalizeApp(app.Spec)
	var result AppChange
	var unchanged App
	next, err := s.write(true, func(tx *recorder, current *snapshot, now time.Time) error {
		existing, found := current.apps[app.Name]
		switch {
		case cond.Absent && found:
			return fmt.Errorf("%w: app %s exists already", ErrConflict, app.Name)
		case !cond.UpdatedAt.IsZero() && !found:
			return fmt.Errorf("%w: app %s was deleted meanwhile", ErrConflict, app.Name)
		case !cond.UpdatedAt.IsZero() && !existing.UpdatedAt.Equal(cond.UpdatedAt):
			return fmt.Errorf("%w: app %s was changed at %s, after the version read; read it again", ErrConflict, app.Name, formatTime(existing.UpdatedAt))
		}
		if found && app.AllAtOnce {
			if err := noPendingRestore(current, app.Name); err != nil {
				return err
			}
		}
		// A stale attempt's result is discarded with its batch.
		result, unchanged = AppChange{Created: !found}, App{}
		if found && existing.Spec.Equal(app.Spec) && existing.Tag == app.Tag {
			unchanged = cloneApp(existing)
			if !app.AllAtOnce {
				return errUnchanged
			}
			// The same spec at once: what its rollout has yet to move
			// moves now, and the rollout ends.
			return s.allAtOnce(tx, current, now, app.Name, &result, actor)
		}
		if err := checkTenants(current, app.Name, app.Spec, spec.Tenant.Validate); err != nil {
			return err
		}
		stamp := formatTime(now)
		revision := existing.Revision + 1
		// An app stored before revisions keeps the spec it had as its
		// first, to go back to.
		keepFirst := found && existing.Revision == 0
		if keepFirst {
			revision = 2
		}
		// What the registry was found to hold stays news only for the same
		// tag, and only while the app runs another image. What the caller
		// found in it just now replaces it.
		latest, latestAt := "", any(nil)
		switch {
		case app.Latest != "":
			if !SameImage(app.Latest, app.Spec.Image) {
				latest, latestAt = app.Latest, stamp
				if existing.Latest == app.Latest && existing.Tag == app.Tag {
					latestAt = formatTime(existing.LatestAt)
				}
			}
		case found && existing.Tag == app.Tag && existing.Latest != "" && !SameImage(existing.Latest, app.Spec.Image):
			latest, latestAt = existing.Latest, formatTime(existing.LatestAt)
		}
		if err := tx.Exec(`INSERT INTO apps (name, spec, created_at, updated_at, tag, revision, latest, latest_at) VALUES (?, ?, ?, ?, ?, ?, ?, ?)
			ON CONFLICT (name) DO UPDATE SET spec = excluded.spec, updated_at = excluded.updated_at, tag = excluded.tag,
				revision = excluded.revision, latest = excluded.latest, latest_at = excluded.latest_at`,
			app.Name, marshal(app.Spec), stamp, stamp, app.Tag, revision, latest, latestAt); err != nil {
			return err
		}
		if latest != "" && (latest != existing.Latest || existing.Tag != app.Tag) {
			if err := event(tx, now, actor, "app.image-found", "", app.Name, "image "+latest+" of "+app.Tag); err != nil {
				return err
			}
		}
		if keepFirst {
			if err := tx.Exec(`INSERT INTO app_revisions (app, revision, spec, tag, created_at, actor) VALUES (?, 1, ?, ?, ?, ?)`,
				app.Name, marshal(existing.Spec), existing.Tag, formatTime(existing.UpdatedAt), "murder"); err != nil {
				return err
			}
		}
		if err := tx.Exec(`INSERT INTO app_revisions (app, revision, spec, tag, created_at, actor) VALUES (?, ?, ?, ?, ?, ?)`,
			app.Name, revision, marshal(app.Spec), app.Tag, stamp, actorOf(actor)); err != nil {
			return err
		}
		detail := "image " + app.Spec.Image
		if app.Tag != "" {
			detail += " of " + app.Tag
		}
		changed := strings.Join(changedFields(existing.Spec, app.Spec), ", ")
		if found {
			detail = "changed " + changed + "; " + detail
		}
		if from > 0 {
			detail = fmt.Sprintf("back to revision %d; %s", from, detail)
		}
		// In waves, every live tenant keeps the revision it runs until the
		// rollout moves it; at once, each moves now. Held tenants stay.
		waves := found && !app.AllAtOnce
		oldRevision := existing.Revision
		if keepFirst {
			oldRevision = 1
		}
		for _, name := range slices.Sorted(maps.Keys(current.tenants)) {
			record := current.tenants[name]
			if record.App != app.Name || record.Archived() {
				continue
			}
			composed := spec.Compose(app.Spec, name, record.Overrides)
			composed.Suspended = record.Spec.Suspended
			if composed.Equal(record.Spec) && record.AppRevision == 0 {
				// It runs the change already: no earlier rollout's move is
				// said of it in this one.
				if record.BeforeRevision != 0 {
					if err := tx.Exec(`UPDATE tenants SET before_revision = 0 WHERE name = ?`, name); err != nil {
						return err
					}
				}
				continue
			}
			if err := s.sealMissing(tx, now, composed, record.Sealed); err != nil {
				return err
			}
			if record.Hold {
				if record.AppRevision == 0 {
					if err := tx.Exec(`UPDATE tenants SET app_revision = ?, before_revision = 0 WHERE name = ?`, oldRevision, name); err != nil {
						return err
					}
				}
				continue
			}
			result.Tenants = append(result.Tenants, name)
			if waves {
				if record.AppRevision == 0 {
					if err := tx.Exec(`UPDATE tenants SET app_revision = ?, before_revision = 0 WHERE name = ?`, oldRevision, name); err != nil {
						return err
					}
				}
				continue
			}
			if err := tx.Exec(`UPDATE tenants SET app_revision = 0, before_revision = 0, generation = generation + 1, updated_at = ? WHERE name = ?`, stamp, name); err != nil {
				return err
			}
			// The tenant's own trail shows the app changes that replaced
			// its container.
			if err := event(tx, now, actor, "app.rolled-out", name, app.Name, "app changed "+changed); err != nil {
				return err
			}
		}
		_, rolling := current.rollouts[app.Name]
		switch {
		case waves && len(result.Tenants) > 0:
			result.Rollout = true
			if err := tx.Exec(`INSERT INTO rollouts (app, to_revision, state, wave, wave_started, reason, started_at, updated_at, actor) VALUES (?, ?, 'running', 0, ?, '', ?, ?, ?)
				ON CONFLICT (app) DO UPDATE SET to_revision = excluded.to_revision, state = 'running', wave = 0, wave_started = excluded.wave_started,
					reason = '', started_at = excluded.started_at, updated_at = excluded.updated_at, actor = excluded.actor, epoch = rollouts.epoch + 1, first = 0`,
				app.Name, revision, stamp, stamp, stamp, actorOf(actor)); err != nil {
				return err
			}
			detail += fmt.Sprintf("; rolls out in waves to %d tenants", len(result.Tenants))
			// The tenants asked to go first move now, as the first wave.
			var first []string
			for _, name := range slices.Sorted(slices.Values(app.First)) {
				if slices.Contains(result.Tenants, name) && !slices.Contains(first, name) {
					first = append(first, name)
				}
			}
			if len(first) > 0 {
				for _, name := range first {
					from := current.tenants[name].AppRevision
					if from == 0 {
						from = oldRevision
					}
					if err := tx.Exec(`UPDATE tenants SET before_revision = app_revision, app_revision = 0, moved_on = node, generation = generation + 1, updated_at = ? WHERE name = ?`, stamp, name); err != nil {
						return err
					}
					if err := event(tx, now, actor, "app.rolled-out", name, app.Name, fmt.Sprintf("wave 1, first: revision %d → %d", from, revision)); err != nil {
						return err
					}
				}
				if err := tx.Exec(`UPDATE rollouts SET wave = 1, first = 1, epoch = epoch + 1 WHERE app = ?`, app.Name); err != nil {
					return err
				}
				detail += "; first, now: " + strings.Join(first, ", ")
			}
		case rolling && !waves:
			if err := tx.Exec(`UPDATE rollouts SET state = 'done', reason = 'replaced by a change rolled out at once', updated_at = ?, epoch = epoch + 1 WHERE app = ? AND state IN ('running', 'paused')`, stamp, app.Name); err != nil {
				return err
			}
		}
		// Revisions tenants run, or go back to, stay.
		if revision > revisionsKept {
			if err := tx.Exec(`DELETE FROM app_revisions WHERE app = ? AND revision <= ?
				AND revision NOT IN (SELECT app_revision FROM tenants WHERE app = ?)
				AND revision NOT IN (SELECT before_revision FROM tenants WHERE app = ?)`, app.Name, revision-revisionsKept, app.Name, app.Name); err != nil {
				return err
			}
		}
		action := "app.updated"
		if result.Created {
			action = "app.created"
		}
		return event(tx, now, actor, action, "", app.Name, detail)
	})
	if errors.Is(err, errUnchanged) {
		return AppChange{App: unchanged}, nil
	}
	if err != nil {
		return AppChange{}, err
	}
	// The same spec at once changed which tenants run it, not the app.
	result.App, result.Changed = cloneApp(next.apps[app.Name]), unchanged.Name == ""
	return result, nil
}

// changedFields names the top-level spec fields that differ.
func changedFields(before, after spec.Tenant) []string {
	var a, b map[string]json.RawMessage
	_ = json.Unmarshal([]byte(marshal(before)), &a)
	_ = json.Unmarshal([]byte(marshal(after)), &b)
	keys := map[string]bool{}
	for key := range a {
		keys[key] = true
	}
	for key := range b {
		keys[key] = true
	}
	var changed []string
	for _, key := range slices.Sorted(maps.Keys(keys)) {
		if string(a[key]) != string(b[key]) {
			changed = append(changed, key)
		}
	}
	return changed
}

// allAtOnce moves every live tenant of app its rollout has yet to move to
// the app's newest revision, held ones aside, and ends the rollout;
// errUnchanged if there is none.
func (s *SQLite) allAtOnce(tx *recorder, current *snapshot, now time.Time, app string, result *AppChange, actor string) error {
	stamp := formatTime(now)
	for _, name := range slices.Sorted(maps.Keys(current.tenants)) {
		record := current.tenants[name]
		if record.App != app || record.Archived() || record.Hold || record.AppRevision == 0 {
			continue
		}
		if err := tx.Exec(`UPDATE tenants SET app_revision = 0, before_revision = 0, moved_on = '', generation = generation + 1, updated_at = ? WHERE name = ?`, stamp, name); err != nil {
			return err
		}
		if err := event(tx, now, actor, "app.rolled-out", name, app, "at once"); err != nil {
			return err
		}
		result.Tenants = append(result.Tenants, name)
	}
	rollout, rolling := current.rollouts[app]
	if len(result.Tenants) == 0 && !(rolling && rollout.Active()) {
		return errUnchanged
	}
	if err := tx.Exec(`UPDATE rollouts SET state = 'done', reason = 'the rest rolled out at once', updated_at = ?, epoch = epoch + 1 WHERE app = ? AND state IN ('running', 'paused')`, stamp, app); err != nil {
		return err
	}
	return event(tx, now, actor, "rollout.done", "", app, fmt.Sprintf("the rest at once: %d tenants", len(result.Tenants)))
}

// errUnchanged ends a write that would change nothing.
var errUnchanged = errors.New("unchanged")

func (s *SQLite) DeleteApp(name, actor string) error {
	_, err := s.write(false, func(tx *recorder, current *snapshot, now time.Time) error {
		if _, ok := current.apps[name]; !ok {
			return fmt.Errorf("app %s: %w", name, ErrNotFound)
		}
		var names []string
		for _, tenant := range slices.Sorted(maps.Keys(current.tenants)) {
			if current.tenants[tenant].App == name {
				names = append(names, tenant)
			}
		}
		if len(names) > 0 {
			return fmt.Errorf("%w: tenants %v run app %s (archived ones count until purged)", ErrInUse, names, name)
		}
		if err := tx.Exec(`DELETE FROM rollouts WHERE app = ?`, name); err != nil {
			return err
		}
		if err := tx.Exec(`DELETE FROM app_revisions WHERE app = ?`, name); err != nil {
			return err
		}
		if err := tx.Exec(`DELETE FROM apps WHERE name = ?`, name); err != nil {
			return err
		}
		return event(tx, now, actor, "app.deleted", "", name, "")
	})
	return err
}

func (s *SQLite) List() []Record {
	state := s.state.Load()
	records := make([]Record, 0, len(state.tenants))
	for _, name := range slices.Sorted(maps.Keys(state.tenants)) {
		records = append(records, state.tenants[name].clone())
	}
	return records
}

func (s *SQLite) Get(name string) (Record, error) {
	record, ok := s.state.Load().tenants[name]
	if !ok {
		return Record{}, fmt.Errorf("tenant %s: %w", name, ErrNotFound)
	}
	return record.clone(), nil
}

func (s *SQLite) Live(name string) (Record, error) {
	record, err := liveTenant(s.state.Load(), name)
	if err != nil {
		return Record{}, err
	}
	return record.clone(), nil
}

// liveTenant returns a live tenant, explaining an archived one.
func liveTenant(state *snapshot, name string) (Record, error) {
	record, ok := state.tenants[name]
	switch {
	case !ok:
		return Record{}, fmt.Errorf("tenant %s: %w", name, ErrNotFound)
	case record.Archived():
		return Record{}, fmt.Errorf("tenant %s: %w", name, ErrArchived)
	}
	return record, nil
}

// prepare returns the spec a tenant of app with overrides runs, validated.
func prepare(state *snapshot, name, appName string, revision int64, overrides spec.Overrides) (spec.Tenant, error) {
	if _, ok := state.apps[appName]; !ok {
		return spec.Tenant{}, fmt.Errorf("app %s: %w", appName, ErrNotFound)
	}
	tenant := spec.Compose(revisionSpec(state, appName, revision), name, overrides)
	return tenant, tenant.Validate()
}

func (s *SQLite) Create(tenant NewTenant, actor string) (Record, error) {
	if err := spec.ValidName(tenant.Name); err != nil {
		return Record{}, &spec.ValidationError{Problems: []string{err.Error()}}
	}
	overrides := tenant.Overrides.Normalized()
	next, err := s.write(true, func(tx *recorder, current *snapshot, now time.Time) error {
		if existing, ok := current.tenants[tenant.Name]; ok && existing.Archived() {
			return ErrArchived
		} else if ok {
			return ErrExists
		}
		composed, err := prepare(current, tenant.Name, tenant.App, 0, overrides)
		if err != nil {
			return err
		}
		node := tenant.Node
		if node == "" {
			node = current.node
		}
		// Outside a cluster, this node alone; in one, its members alone:
		// this node too, which may be leaving it. A cluster is known by its
		// name, which stays when its last member is removed.
		member, isMember := current.nodes[node]
		inCluster := current.cluster != "" || len(current.nodes) > 0
		if inCluster && !isMember || !inCluster && node != current.node {
			return fmt.Errorf("node %s: %w", node, ErrNotFound)
		}
		if member.Failed() {
			return fmt.Errorf("%w: node %s failed and has not answered since", ErrConflict, node)
		}
		subnet, err := s.assign(current)
		if err != nil {
			return err
		}
		stamp := formatTime(now)
		if err := tx.Exec(`INSERT INTO tenants (name, app, overrides, suspended, subnet, data_image, previous_image, generation, created_at, updated_at, node)
			VALUES (?, ?, ?, 0, ?, '', '', 1, ?, ?, ?)`, tenant.Name, tenant.App, marshal(overrides), subnet, stamp, stamp, node); err != nil {
			return err
		}
		if err := s.sealMissing(tx, now, composed, nil); err != nil {
			return err
		}
		// In a cluster, where it runs is worth recording.
		detail := overridesDetail(overrides)
		if len(current.nodes) > 0 {
			detail = strings.TrimSuffix("on node "+node+"; "+detail, "; ")
		}
		return event(tx, now, actor, "tenant.created", tenant.Name, tenant.App, detail)
	})
	if err != nil {
		return Record{}, err
	}
	return next.tenants[tenant.Name].clone(), nil
}

// overridesDetail names what a tenant overrides, without env values: the
// audit log outlives tenants and may not keep credentials passed as env.
func overridesDetail(overrides spec.Overrides) string {
	var parts []string
	if overrides.Image != "" {
		parts = append(parts, "image "+overrides.Image)
	}
	if len(overrides.Env) > 0 {
		parts = append(parts, "env "+strings.Join(slices.Sorted(maps.Keys(overrides.Env)), ","))
	}
	if len(overrides.Secrets) > 0 {
		parts = append(parts, "secrets "+strings.Join(slices.Sorted(maps.Keys(overrides.Secrets)), ","))
	}
	if limits := overrides.Limits; limits != (spec.Limits{}) {
		parts = append(parts, fmt.Sprintf("limits memoryMB=%d cpus=%g pids=%d diskGB=%d", limits.MemoryMB, limits.CPUs, limits.PIDs, limits.DiskGB))
	}
	if len(parts) == 0 {
		return ""
	}
	return "overrides: " + strings.Join(parts, "; ")
}

func (s *SQLite) Configure(name, app string, overrides spec.Overrides, generation int64, actor string) (Record, bool, error) {
	overrides = overrides.Normalized()
	var unchanged Record
	next, err := s.write(true, func(tx *recorder, current *snapshot, now time.Time) error {
		record, ok := current.tenants[name]
		if !ok {
			return fmt.Errorf("tenant %s: %w", name, ErrNotFound)
		}
		if generation != 0 && record.Generation != generation {
			return fmt.Errorf("%w: tenant %s is at generation %d, not %d; read it again", ErrConflict, name, record.Generation, generation)
		}
		if record.App == app && record.Overrides.Equal(overrides) {
			unchanged = record.clone()
			return errUnchanged
		}
		// Another app runs its newest revision; the same app the one the
		// tenant runs.
		revision, before := record.AppRevision, record.BeforeRevision
		if record.App != app {
			revision, before = 0, 0
		}
		composed, err := prepare(current, name, app, revision, overrides)
		if err != nil {
			return err
		}
		if err := tx.Exec(`UPDATE tenants SET app = ?, overrides = ?, app_revision = ?, before_revision = ?, generation = generation + 1, updated_at = ? WHERE name = ?`,
			app, marshal(overrides), revision, before, formatTime(now), name); err != nil {
			return err
		}
		// An archived tenant gets missing secrets when it is restored.
		if !record.Archived() {
			if err := s.sealMissing(tx, now, composed, record.Sealed); err != nil {
				return err
			}
		}
		detail := overridesDetail(overrides)
		if detail == "" {
			detail = "no overrides"
		}
		if record.App != app {
			detail = "app " + record.App + " → " + app + "; " + detail
		}
		return event(tx, now, actor, "tenant.updated", name, app, detail)
	})
	if errors.Is(err, errUnchanged) {
		return unchanged, false, nil
	}
	if err != nil {
		return Record{}, false, err
	}
	return next.tenants[name].clone(), true, nil
}

func (s *SQLite) SetSuspended(name string, suspended bool, actor string) (Record, error) {
	next, err := s.write(true, func(tx *recorder, current *snapshot, now time.Time) error {
		record, err := liveTenant(current, name)
		if err != nil {
			return err
		}
		if err := tx.Exec(`UPDATE tenants SET suspended = ?, generation = generation + 1, updated_at = ? WHERE name = ?`,
			suspended, formatTime(now), name); err != nil {
			return err
		}
		action := "tenant.resumed"
		if suspended {
			action = "tenant.suspended"
		}
		return event(tx, now, actor, action, name, record.App, "")
	})
	if err != nil {
		return Record{}, err
	}
	return next.tenants[name].clone(), nil
}

func (s *SQLite) Rotate(name, secret, actor string) (Record, error) {
	next, err := s.write(true, func(tx *recorder, current *snapshot, now time.Time) error {
		record, err := liveTenant(current, name)
		if err != nil {
			return err
		}
		definition, ok := record.Spec.Secrets[secret]
		if !ok {
			return fmt.Errorf("tenant %s has no secret %s in its spec: %w", name, secret, ErrNotFound)
		}
		value, err := s.options.Sealer(name, secret, definition)
		if err != nil {
			return err
		}
		stamp := formatTime(now)
		if err := tx.Exec(`INSERT INTO secrets (tenant, name, sealed, created_at) VALUES (?, ?, ?, ?)
			ON CONFLICT (tenant, name) DO UPDATE SET sealed = excluded.sealed, created_at = excluded.created_at`, name, secret, value, stamp); err != nil {
			return err
		}
		if err := tx.Exec(`UPDATE tenants SET generation = generation + 1, updated_at = ? WHERE name = ?`, stamp, name); err != nil {
			return err
		}
		return event(tx, now, actor, "secret.rotated", name, record.App, secret)
	})
	if err != nil {
		return Record{}, err
	}
	return next.tenants[name].clone(), nil
}

func (s *SQLite) ResealSecrets(reseal Resealer, actor string) (int, error) {
	count := 0
	// Under a rotation's freeze. Always a change, even of no secret: a
	// batch made before it, with a value sealed by the old key, is stale
	// after it and made again.
	_, err := s.writeIf(false, false, func(tx *recorder, current *snapshot, now time.Time) error {
		count = 0
		for _, name := range slices.Sorted(maps.Keys(current.tenants)) {
			sealed := current.tenants[name].Sealed
			for _, secret := range slices.Sorted(maps.Keys(sealed)) {
				resealed, changed, err := reseal(name, secret, sealed[secret])
				if err != nil {
					return fmt.Errorf("tenant %s, secret %s: %w", name, secret, err)
				}
				if !changed {
					continue
				}
				if err := tx.Exec(`UPDATE secrets SET sealed = ? WHERE tenant = ? AND name = ? AND sealed = ?`, resealed, name, secret, sealed[secret]); err != nil {
					return err
				}
				count++
			}
		}
		return event(tx, now, actor, "secrets.resealed", "", "", fmt.Sprintf("%d secrets sealed with the master key in use", count))
	})
	return count, err
}

func (s *SQLite) Roll(name, actor string) (Record, error) {
	next, err := s.write(true, func(tx *recorder, current *snapshot, now time.Time) error {
		record, err := liveTenant(current, name)
		if err != nil {
			return err
		}
		if err := tx.Exec(`UPDATE tenants SET rollout = rollout + 1, generation = generation + 1, updated_at = ? WHERE name = ?`, formatTime(now), name); err != nil {
			return err
		}
		return event(tx, now, actor, "tenant.rolled", name, record.App, "")
	})
	if err != nil {
		return Record{}, err
	}
	return next.tenants[name].clone(), nil
}

func (s *SQLite) Observe(name, dataImage string) error {
	if record, ok := s.state.Load().tenants[name]; ok && record.DataImage == dataImage {
		return nil
	}
	_, err := s.write(false, func(tx *recorder, current *snapshot, _ time.Time) error {
		record, err := liveTenant(current, name)
		if err != nil {
			return err
		}
		// Again on every attempt: a retry after another observed the same
		// image would put it in previous_image too.
		if record.DataImage == dataImage {
			return errUnchanged
		}
		return tx.Exec(`UPDATE tenants SET previous_image = CASE WHEN data_image = '' THEN previous_image ELSE data_image END, data_image = ? WHERE name = ?`, dataImage, name)
	})
	if errors.Is(err, errUnchanged) {
		return nil
	}
	return err
}

// AppExited records, for the node that runs a tenant, that its app exited
// and its container was started again, with how: frozen or not, since an
// exit during an upgrade happened all the same. Every release applies it.
func (s *SQLite) AppExited(name, detail string) error {
	_, err := s.writeAs(firstBatchSchema, false, false, func(tx *recorder, current *snapshot, now time.Time) error {
		record, ok := current.tenants[name]
		if !ok {
			return fmt.Errorf("tenant %s: %w", name, ErrNotFound)
		}
		return event(tx, now, "murder", "tenant.restarted", name, record.App, detail)
	})
	return err
}

// notMoving refuses a change that would pull a tenant from under its move:
// the move finishes or is undone first.
func notMoving(record Record) error {
	if record.MoveTo != "" {
		return fmt.Errorf("%w: tenant %s is moving to node %s; wait until it has moved, or the move is undone", ErrConflict, record.Spec.Name, record.MoveTo)
	}
	return nil
}

// Archive takes a live tenant out of service in one write; the edge stops
// routing to it at once. Its suspension is kept for the restore.
func (s *SQLite) Archive(name, actor string) error {
	_, err := s.write(true, func(tx *recorder, current *snapshot, now time.Time) error {
		record, err := liveTenant(current, name)
		if err != nil {
			return err
		}
		if err := notMoving(record); err != nil {
			return err
		}
		if err := tx.Exec(`UPDATE tenants SET archived_at = ?, generation = generation + 1, updated_at = ? WHERE name = ?`,
			formatTime(now), formatTime(now), name); err != nil {
			return err
		}
		return event(tx, now, actor, "tenant.archived", name, record.App, "")
	})
	return err
}

func (s *SQLite) Restore(name, actor string) (Record, error) {
	next, err := s.write(true, func(tx *recorder, current *snapshot, now time.Time) error {
		record, ok := current.tenants[name]
		switch {
		case !ok:
			return fmt.Errorf("archived tenant %s: %w", name, ErrNotFound)
		case !record.Archived():
			return fmt.Errorf("tenant %s: %w", name, ErrNotArchived)
		}
		if err := notMoving(record); err != nil {
			return err
		}
		// A restored tenant runs the app's newest revision.
		composed, err := prepare(current, name, record.App, 0, record.Overrides)
		if err != nil {
			return fmt.Errorf("tenant %s cannot run app %s as it is now; change it with \"murder tenant set\" first: %w", name, record.App, err)
		}
		if err := tx.Exec(`UPDATE tenants SET archived_at = NULL, app_revision = 0, before_revision = 0, restore = '', generation = generation + 1, updated_at = ? WHERE name = ?`,
			formatTime(now), name); err != nil {
			return err
		}
		if err := s.sealMissing(tx, now, composed, record.Sealed); err != nil {
			return err
		}
		return event(tx, now, actor, "tenant.restored", name, record.App, "")
	})
	if err != nil {
		return Record{}, err
	}
	return next.tenants[name].clone(), nil
}

// Forget deletes an archived tenant and its secrets. It also records the
// purge of data no archive entry accounted for.
func (s *SQLite) Forget(name, actor string) error {
	_, err := s.write(false, func(tx *recorder, current *snapshot, now time.Time) error {
		record, ok := current.tenants[name]
		if ok && !record.Archived() {
			return fmt.Errorf("tenant %s: %w", name, ErrNotArchived)
		}
		if ok {
			if err := notMoving(record); err != nil {
				return err
			}
		}
		app, detail := "", "leftover data"
		if ok {
			app, detail = record.App, ""
			if err := tx.Exec(`DELETE FROM tenants WHERE name = ?`, name); err != nil {
				return err
			}
		}
		return event(tx, now, actor, "tenant.purged", name, app, detail)
	})
	return err
}

func (s *SQLite) Events(query EventQuery) ([]Event, error) {
	limit := query.Limit
	if limit <= 0 || limit > 1000 {
		limit = 100
	}
	// Never past the published snapshot: a write commits before its
	// snapshot is published, and its event must not show before its change.
	before := s.state.Load().lastEvent + 1
	if query.Before > 0 && query.Before < before {
		before = query.Before
	}
	// Built per filter, so the tenant and app indexes can serve it.
	where, args := "id < ?", []any{before}
	if query.Tenant != "" {
		where, args = where+" AND tenant = ?", append(args, query.Tenant)
	}
	if query.App != "" {
		where, args = where+" AND app = ?", append(args, query.App)
	}
	rows, err := s.db.Query(`SELECT id, at, actor, action, tenant, app, detail FROM events WHERE `+where+` ORDER BY id DESC LIMIT ?`, append(args, limit)...)
	if err != nil {
		return nil, err
	}
	events := []Event{}
	for rows.Next() {
		var e Event
		var at string
		if err := rows.Scan(&e.ID, &at, &e.Actor, &e.Action, &e.Tenant, &e.App, &e.Detail); err != nil {
			rows.Close()
			return nil, err
		}
		e.At = parseTime(at)
		events = append(events, e)
	}
	return events, closeRows(rows)
}

func (s *SQLite) Images() []string {
	return slices.Sorted(maps.Keys(s.state.Load().images))
}

func (s *SQLite) ForgetImage(ref string) error {
	if !s.state.Load().images[ref] {
		return nil
	}
	return s.local(func(tx *sql.Tx) error {
		_, err := tx.Exec(`DELETE FROM images WHERE ref = ?`, ref)
		return err
	})
}

// assign returns a subnet no tenant holds, live or archived.
func (s *SQLite) assign(state *snapshot) (string, error) {
	usedSubnets := map[string]bool{}
	for _, record := range state.tenants {
		usedSubnets[record.Subnet] = true
	}
	pool := s.options.NetworkPool
	base := pool.Masked().Addr().As4()
	start := binary.BigEndian.Uint32(base[:])
	count := uint32(1) << (subnetBits - pool.Bits())
	for i := range count {
		var addr [4]byte
		binary.BigEndian.PutUint32(addr[:], start+i<<(32-subnetBits))
		subnet := netip.PrefixFrom(netip.AddrFrom4(addr), subnetBits).String()
		if !usedSubnets[subnet] {
			return subnet, nil
		}
	}
	return "", fmt.Errorf("network pool %v has no free /%d; archived tenants keep theirs until they are purged", pool, subnetBits)
}
