package store

import (
	"database/sql"
	"encoding/binary"
	"encoding/json"
	"errors"
	"fmt"
	"maps"
	"net/netip"
	"net/url"
	"os"
	"path/filepath"
	"regexp"
	"slices"
	"strings"
	"sync"
	"sync/atomic"
	"syscall"
	"time"

	_ "modernc.org/sqlite"

	"github.com/PowerPenguini/corral/internal/spec"
)

const subnetBits = 28

var nodePattern = regexp.MustCompile(`^[A-Za-z0-9][A-Za-z0-9.-]{0,62}$`)

// SQLite is the store in one SQLite database. Reads come from a snapshot of
// the whole state in memory, rebuilt inside every write transaction before
// it commits, so memory and disk always agree.
type SQLite struct {
	db      *sql.DB
	options Options
	now     func() time.Time
	lock    *os.File
	mu      sync.Mutex // serializes writers
	state   atomic.Pointer[snapshot]
	changed chan struct{}
}

type snapshot struct {
	node     string
	apps     map[string]App
	live     map[string]Record
	archived map[string]Archived
	// lastEvent is the newest event the snapshot includes; readers of the
	// audit log see nothing newer, so no event shows before its change.
	lastEvent int64
}

// dsn escapes path, so "#", "?" or "%" in it cannot be read as URI syntax
// and open another file than the one Init created and Open locks.
func dsn(path string) string {
	// A relative path would read as a URI authority.
	if absolute, err := filepath.Abs(path); err == nil {
		path = absolute
	}
	uri := url.URL{Scheme: "file", Path: path, RawQuery: "_pragma=foreign_keys(1)&_pragma=journal_mode(WAL)&_pragma=synchronous(FULL)&_pragma=busy_timeout(5000)&_txlock=immediate"}
	return uri.String()
}

func openDB(path string) (*sql.DB, error) {
	db, err := sql.Open("sqlite", dsn(path))
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

// Init creates an empty database for node at path, refusing to overwrite
// one. It is built under a temporary name and linked into place complete, so
// an interrupted init never leaves a half-made database at path; leftovers
// of one are removed by the next.
func Init(path, node string) error {
	if !nodePattern.MatchString(node) {
		return fmt.Errorf("node name %q must be 1-63 letters, digits, dots or hyphens", node)
	}
	dir := filepath.Dir(path)
	if err := os.MkdirAll(dir, 0o700); err != nil {
		return err
	}
	if _, err := os.Stat(path); err == nil {
		return fmt.Errorf("database %s already exists", path)
	}
	stale, _ := filepath.Glob(filepath.Join(dir, ".corral-init-*"))
	for _, file := range stale {
		_ = os.Remove(file)
	}
	temp, err := os.CreateTemp(dir, ".corral-init-*.db") // mode 0600
	if err != nil {
		return err
	}
	_ = temp.Close()
	defer func() {
		for _, suffix := range []string{"", "-wal", "-shm", "-journal"} {
			_ = os.Remove(temp.Name() + suffix)
		}
	}()
	if err := initDB(temp.Name(), node); err != nil {
		return err
	}
	if err := os.Link(temp.Name(), path); errors.Is(err, os.ErrExist) {
		return fmt.Errorf("database %s already exists", path)
	} else if err != nil {
		return err
	}
	return syncDir(dir)
}

// initDB writes the schema and the node name; closing the database folds
// the write-ahead log into the file.
func initDB(path, node string) error {
	db, err := openDB(path)
	if err != nil {
		return err
	}
	err = migrate(db)
	if err == nil {
		_, err = db.Exec(`INSERT INTO meta (key, value) VALUES ('node', ?)`, node)
	}
	return errors.Join(err, db.Close())
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
		return nil, fmt.Errorf("database %s does not exist; run \"corral init\" on a new node, or restore it from a backup", path)
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
	db, err := openDB(path)
	if err != nil {
		_ = lock.Close()
		return nil, err
	}
	s := &SQLite{db: db, options: options, now: time.Now, lock: lock, changed: make(chan struct{}, 1)}
	if err := migrate(db); err != nil {
		_ = s.Close()
		return nil, fmt.Errorf("database %s: %w", path, err)
	}
	state, err := load(db)
	if err != nil {
		_ = s.Close()
		return nil, fmt.Errorf("database %s: %w", path, err)
	}
	s.state.Store(state)
	return s, nil
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

// querier is what load needs: a database or a transaction.
type querier interface {
	Query(query string, args ...any) (*sql.Rows, error)
	QueryRow(query string, args ...any) *sql.Row
}

// load reads the whole state. It is small: one row per app and tenant.
func load(q querier) (*snapshot, error) {
	state := &snapshot{apps: map[string]App{}, live: map[string]Record{}, archived: map[string]Archived{}}
	if err := q.QueryRow(`SELECT value FROM meta WHERE key = 'node'`).Scan(&state.node); err != nil {
		return nil, fmt.Errorf("read the node name: %w", err)
	}
	if err := q.QueryRow(`SELECT COALESCE(MAX(id), 0) FROM events`).Scan(&state.lastEvent); err != nil {
		return nil, err
	}
	rows, err := q.Query(`SELECT name, spec, created_at, updated_at FROM apps`)
	if err != nil {
		return nil, err
	}
	for rows.Next() {
		var app App
		var specJSON, created, updated string
		if err := rows.Scan(&app.Name, &specJSON, &created, &updated); err != nil {
			rows.Close()
			return nil, err
		}
		if err := json.Unmarshal([]byte(specJSON), &app.Spec); err != nil {
			rows.Close()
			return nil, fmt.Errorf("app %s: %w", app.Name, err)
		}
		app.CreatedAt, app.UpdatedAt = parseTime(created), parseTime(updated)
		state.apps[app.Name] = app
	}
	if err := closeRows(rows); err != nil {
		return nil, err
	}
	sealed := map[string]map[string]string{}
	rows, err = q.Query(`SELECT tenant, name, sealed FROM secrets`)
	if err != nil {
		return nil, err
	}
	for rows.Next() {
		var tenant, name, value string
		if err := rows.Scan(&tenant, &name, &value); err != nil {
			rows.Close()
			return nil, err
		}
		if sealed[tenant] == nil {
			sealed[tenant] = map[string]string{}
		}
		sealed[tenant][name] = value
	}
	if err := closeRows(rows); err != nil {
		return nil, err
	}
	rows, err = q.Query(`SELECT name, app, overrides, suspended, subnet, data_image, generation, created_at, updated_at, archived_at FROM tenants`)
	if err != nil {
		return nil, err
	}
	for rows.Next() {
		var record Record
		var name, overrides, created, updated string
		var suspended bool
		var subnet, archivedAt sql.NullString
		if err := rows.Scan(&name, &record.App, &overrides, &suspended, &subnet, &record.DataImage, &record.Generation, &created, &updated, &archivedAt); err != nil {
			rows.Close()
			return nil, err
		}
		if err := json.Unmarshal([]byte(overrides), &record.Overrides); err != nil {
			rows.Close()
			return nil, fmt.Errorf("tenant %s: %w", name, err)
		}
		record.Spec = spec.Compose(state.apps[record.App].Spec, name, record.Overrides)
		record.Spec.Suspended = suspended
		record.Subnet = subnet.String
		record.Sealed = sealed[name]
		record.CreatedAt, record.UpdatedAt = parseTime(created), parseTime(updated)
		if archivedAt.Valid {
			state.archived[name] = Archived{Record: record, ArchivedAt: parseTime(archivedAt.String)}
		} else {
			state.live[name] = record
		}
	}
	if err := closeRows(rows); err != nil {
		return nil, err
	}
	return state, nil
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

// write runs change in one transaction, rebuilds the snapshot inside it and
// makes the snapshot current only once the transaction has committed.
func (s *SQLite) write(signal bool, change func(tx *sql.Tx, current *snapshot, now time.Time) error) (*snapshot, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	tx, err := s.db.Begin()
	if err != nil {
		return nil, err
	}
	defer tx.Rollback()
	if err := change(tx, s.state.Load(), s.now().UTC()); err != nil {
		return nil, err
	}
	next, err := load(tx)
	if err != nil {
		return nil, err
	}
	if err := tx.Commit(); err != nil {
		return nil, err
	}
	s.state.Store(next)
	if signal {
		select {
		case s.changed <- struct{}{}:
		default:
		}
	}
	return next, nil
}

func event(tx *sql.Tx, now time.Time, actor, action, tenant, app, detail string) error {
	if actor == "" {
		actor = "unknown"
	}
	_, err := tx.Exec(`INSERT INTO events (at, actor, action, tenant, app, detail) VALUES (?, ?, ?, ?, ?, ?)`,
		formatTime(now), actor, action, tenant, app, detail)
	return err
}

// sealMissing generates a value for every secret of tenant that has none.
func (s *SQLite) sealMissing(tx *sql.Tx, now time.Time, tenant spec.Tenant, sealed map[string]string) error {
	for _, name := range slices.Sorted(maps.Keys(tenant.Secrets)) {
		if _, ok := sealed[name]; ok {
			continue
		}
		value, err := s.options.Sealer(tenant.Name, name, tenant.Secrets[name])
		if err != nil {
			return fmt.Errorf("secret %s: %w", name, err)
		}
		if _, err := tx.Exec(`INSERT INTO secrets (tenant, name, sealed, created_at) VALUES (?, ?, ?, ?)`, tenant.Name, name, value, formatTime(now)); err != nil {
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
	for _, name := range slices.Sorted(maps.Keys(state.live)) {
		record := state.live[name]
		if record.App != app || record.Spec.Validate() != nil {
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
	if err := spec.CheckApp(app.Name, app.Spec, spec.Tenant.Validate); err != nil {
		return AppChange{}, err
	}
	app.Spec = normalizeApp(app.Spec)
	var result AppChange
	var unchanged App
	next, err := s.write(true, func(tx *sql.Tx, current *snapshot, now time.Time) error {
		existing, found := current.apps[app.Name]
		result.Created = !found
		if found && existing.Spec.Equal(app.Spec) {
			unchanged = cloneApp(existing)
			return errUnchanged
		}
		if err := checkTenants(current, app.Name, app.Spec, spec.Tenant.Validate); err != nil {
			return err
		}
		stamp := formatTime(now)
		if _, err := tx.Exec(`INSERT INTO apps (name, spec, created_at, updated_at) VALUES (?, ?, ?, ?)
			ON CONFLICT (name) DO UPDATE SET spec = excluded.spec, updated_at = excluded.updated_at`,
			app.Name, marshal(app.Spec), stamp, stamp); err != nil {
			return err
		}
		detail := "image " + app.Spec.Image
		changed := strings.Join(changedFields(existing.Spec, app.Spec), ", ")
		if found {
			detail = "changed " + changed + "; " + detail
		}
		for _, name := range slices.Sorted(maps.Keys(current.live)) {
			record := current.live[name]
			if record.App != app.Name {
				continue
			}
			composed := spec.Compose(app.Spec, name, record.Overrides)
			composed.Suspended = record.Spec.Suspended
			if composed.Equal(record.Spec) {
				continue
			}
			if _, err := tx.Exec(`UPDATE tenants SET generation = generation + 1, updated_at = ? WHERE name = ?`, stamp, name); err != nil {
				return err
			}
			if err := s.sealMissing(tx, now, composed, record.Sealed); err != nil {
				return err
			}
			// The tenant's own trail shows the app changes that replaced
			// its container.
			if err := event(tx, now, actor, "app.rolled-out", name, app.Name, "app changed "+changed); err != nil {
				return err
			}
			result.Tenants = append(result.Tenants, name)
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
	result.App, result.Changed = cloneApp(next.apps[app.Name]), true
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

// errUnchanged ends a write that would change nothing.
var errUnchanged = errors.New("unchanged")

// tenantsOf returns the live and archived tenants of app.
func tenantsOf(state *snapshot, app string) []Record {
	var records []Record
	for _, record := range state.live {
		if record.App == app {
			records = append(records, record)
		}
	}
	for _, archived := range state.archived {
		if archived.Record.App == app {
			records = append(records, archived.Record)
		}
	}
	return records
}

func (s *SQLite) DeleteApp(name, actor string) error {
	_, err := s.write(false, func(tx *sql.Tx, current *snapshot, now time.Time) error {
		if _, ok := current.apps[name]; !ok {
			return fmt.Errorf("app %s: %w", name, ErrNotFound)
		}
		if users := tenantsOf(current, name); len(users) > 0 {
			names := make([]string, 0, len(users))
			for _, record := range users {
				names = append(names, record.Spec.Name)
			}
			slices.Sort(names)
			return fmt.Errorf("%w: tenants %v run app %s (archived ones count until purged)", ErrInUse, names, name)
		}
		if _, err := tx.Exec(`DELETE FROM apps WHERE name = ?`, name); err != nil {
			return err
		}
		return event(tx, now, actor, "app.deleted", "", name, "")
	})
	return err
}

func (s *SQLite) List() []Record {
	state := s.state.Load()
	records := make([]Record, 0, len(state.live))
	for _, name := range slices.Sorted(maps.Keys(state.live)) {
		records = append(records, state.live[name].clone())
	}
	return records
}

func (s *SQLite) Get(name string) (Record, error) {
	record, ok := s.state.Load().live[name]
	if !ok {
		return Record{}, fmt.Errorf("tenant %s: %w", name, ErrNotFound)
	}
	return record.clone(), nil
}

// liveTenant returns a live tenant, explaining an archived one.
func liveTenant(state *snapshot, name string) (Record, error) {
	if record, ok := state.live[name]; ok {
		return record, nil
	}
	if _, ok := state.archived[name]; ok {
		return Record{}, fmt.Errorf("tenant %s: %w", name, ErrArchived)
	}
	return Record{}, fmt.Errorf("tenant %s: %w", name, ErrNotFound)
}

// prepare returns the spec a tenant of app with overrides runs, validated.
func prepare(state *snapshot, name, appName string, overrides spec.Overrides) (spec.Tenant, error) {
	app, ok := state.apps[appName]
	if !ok {
		return spec.Tenant{}, fmt.Errorf("app %s: %w", appName, ErrNotFound)
	}
	tenant := spec.Compose(app.Spec, name, overrides)
	return tenant, tenant.Validate()
}

func (s *SQLite) Create(tenant NewTenant, actor string) (Record, error) {
	if err := spec.ValidName(tenant.Name); err != nil {
		return Record{}, &spec.ValidationError{Problems: []string{err.Error()}}
	}
	overrides := tenant.Overrides.Normalized()
	next, err := s.write(true, func(tx *sql.Tx, current *snapshot, now time.Time) error {
		if _, ok := current.live[tenant.Name]; ok {
			return ErrExists
		}
		if _, ok := current.archived[tenant.Name]; ok {
			return ErrArchived
		}
		composed, err := prepare(current, tenant.Name, tenant.App, overrides)
		if err != nil {
			return err
		}
		subnet, err := s.assign(current)
		if err != nil {
			return err
		}
		stamp := formatTime(now)
		if _, err := tx.Exec(`INSERT INTO tenants (name, app, overrides, suspended, subnet, data_image, generation, created_at, updated_at)
			VALUES (?, ?, ?, 0, ?, '', 1, ?, ?)`, tenant.Name, tenant.App, marshal(overrides), subnet, stamp, stamp); err != nil {
			return err
		}
		if err := s.sealMissing(tx, now, composed, nil); err != nil {
			return err
		}
		return event(tx, now, actor, "tenant.created", tenant.Name, tenant.App, overridesDetail(overrides))
	})
	if err != nil {
		return Record{}, err
	}
	return next.live[tenant.Name].clone(), nil
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
		parts = append(parts, fmt.Sprintf("limits memoryMB=%d cpus=%g pids=%d", limits.MemoryMB, limits.CPUs, limits.PIDs))
	}
	if len(parts) == 0 {
		return ""
	}
	return "overrides: " + strings.Join(parts, "; ")
}

func (s *SQLite) Configure(name, app string, overrides spec.Overrides, generation int64, actor string) (Record, bool, error) {
	overrides = overrides.Normalized()
	var unchanged Record
	next, err := s.write(true, func(tx *sql.Tx, current *snapshot, now time.Time) error {
		record, live := current.live[name]
		if !live {
			archived, ok := current.archived[name]
			if !ok {
				return fmt.Errorf("tenant %s: %w", name, ErrNotFound)
			}
			record = archived.Record
		}
		if generation != 0 && record.Generation != generation {
			return fmt.Errorf("%w: tenant %s is at generation %d, not %d; read it again", ErrConflict, name, record.Generation, generation)
		}
		if record.App == app && record.Overrides.Equal(overrides) {
			unchanged = record.clone()
			return errUnchanged
		}
		composed, err := prepare(current, name, app, overrides)
		if err != nil {
			return err
		}
		if _, err := tx.Exec(`UPDATE tenants SET app = ?, overrides = ?, generation = generation + 1, updated_at = ? WHERE name = ?`,
			app, marshal(overrides), formatTime(now), name); err != nil {
			return err
		}
		// An archived tenant gets missing secrets when it is restored.
		if live {
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
	if record, ok := next.live[name]; ok {
		return record.clone(), true, nil
	}
	return next.archived[name].Record.clone(), true, nil
}

func (s *SQLite) SetSuspended(name string, suspended bool, actor string) (Record, error) {
	next, err := s.write(true, func(tx *sql.Tx, current *snapshot, now time.Time) error {
		record, err := liveTenant(current, name)
		if err != nil {
			return err
		}
		if _, err := tx.Exec(`UPDATE tenants SET suspended = ?, generation = generation + 1, updated_at = ? WHERE name = ?`,
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
	return next.live[name].clone(), nil
}

func (s *SQLite) Rotate(name, secret, actor string) (Record, error) {
	next, err := s.write(true, func(tx *sql.Tx, current *snapshot, now time.Time) error {
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
		if _, err := tx.Exec(`INSERT INTO secrets (tenant, name, sealed, created_at) VALUES (?, ?, ?, ?)
			ON CONFLICT (tenant, name) DO UPDATE SET sealed = excluded.sealed, created_at = excluded.created_at`, name, secret, value, stamp); err != nil {
			return err
		}
		if _, err := tx.Exec(`UPDATE tenants SET generation = generation + 1, updated_at = ? WHERE name = ?`, stamp, name); err != nil {
			return err
		}
		return event(tx, now, actor, "secret.rotated", name, record.App, secret)
	})
	if err != nil {
		return Record{}, err
	}
	return next.live[name].clone(), nil
}

func (s *SQLite) Observe(name, dataImage string) error {
	if record, ok := s.state.Load().live[name]; ok && record.DataImage == dataImage {
		return nil
	}
	_, err := s.write(false, func(tx *sql.Tx, current *snapshot, _ time.Time) error {
		if _, err := liveTenant(current, name); err != nil {
			return err
		}
		_, err := tx.Exec(`UPDATE tenants SET data_image = ? WHERE name = ?`, dataImage, name)
		return err
	})
	return err
}

// Archive moves a live tenant to the archive in one write; the edge stops
// routing to it at once. Its suspension is kept for the restore. It keeps
// its subnet until the reconciler has removed its container and network
// and calls Release: until then a container may still run at the address,
// and no other tenant may be routed there.
func (s *SQLite) Archive(name, actor string) error {
	_, err := s.write(true, func(tx *sql.Tx, current *snapshot, now time.Time) error {
		record, err := liveTenant(current, name)
		if err != nil {
			return err
		}
		if _, err := tx.Exec(`UPDATE tenants SET archived_at = ?, generation = generation + 1, updated_at = ? WHERE name = ?`,
			formatTime(now), formatTime(now), name); err != nil {
			return err
		}
		return event(tx, now, actor, "tenant.archived", name, record.App, "")
	})
	return err
}

func (s *SQLite) Restore(name, actor string) (Record, error) {
	next, err := s.write(true, func(tx *sql.Tx, current *snapshot, now time.Time) error {
		archived, ok := current.archived[name]
		if !ok {
			if _, live := current.live[name]; live {
				return fmt.Errorf("tenant %s: %w", name, ErrNotArchived)
			}
			return fmt.Errorf("archived tenant %s: %w", name, ErrNotFound)
		}
		record := archived.Record
		composed, err := prepare(current, name, record.App, record.Overrides)
		if err != nil {
			return fmt.Errorf("tenant %s cannot run app %s as it is now; change it with \"corral tenant set\" first: %w", name, record.App, err)
		}
		// Back on its own subnet if it still holds it (its network may still
		// stand); otherwise on a free one.
		subnet := record.Subnet
		if subnet == "" {
			if subnet, err = s.assign(current); err != nil {
				return err
			}
		}
		if _, err := tx.Exec(`UPDATE tenants SET subnet = ?, archived_at = NULL, generation = generation + 1, updated_at = ? WHERE name = ?`,
			subnet, formatTime(now), name); err != nil {
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
	return next.live[name].clone(), nil
}

// Release frees the subnet of an archived tenant once the reconciler has
// removed everything that used it. It does nothing for a tenant that is not
// archived or holds none.
func (s *SQLite) Release(name string) error {
	if archived, ok := s.state.Load().archived[name]; !ok || archived.Record.Subnet == "" {
		return nil
	}
	_, err := s.write(false, func(tx *sql.Tx, current *snapshot, _ time.Time) error {
		_, err := tx.Exec(`UPDATE tenants SET subnet = NULL WHERE name = ? AND archived_at IS NOT NULL`, name)
		return err
	})
	return err
}

// Forget deletes an archived tenant and its secrets. It also records the
// purge of data no archive entry accounted for.
func (s *SQLite) Forget(name, actor string) error {
	_, err := s.write(false, func(tx *sql.Tx, current *snapshot, now time.Time) error {
		if _, live := current.live[name]; live {
			return fmt.Errorf("tenant %s: %w", name, ErrNotArchived)
		}
		app, detail := "", "leftover data"
		if archived, ok := current.archived[name]; ok {
			app, detail = archived.Record.App, ""
			if _, err := tx.Exec(`DELETE FROM tenants WHERE name = ?`, name); err != nil {
				return err
			}
		}
		return event(tx, now, actor, "tenant.purged", name, app, detail)
	})
	return err
}

func (s *SQLite) ArchivedTenant(name string) (Archived, bool) {
	archived, ok := s.state.Load().archived[name]
	if !ok {
		return Archived{}, false
	}
	archived.Record = archived.Record.clone()
	return archived, true
}

func (s *SQLite) ListArchived() []Archived {
	state := s.state.Load()
	archived := make([]Archived, 0, len(state.archived))
	for _, name := range slices.Sorted(maps.Keys(state.archived)) {
		entry := state.archived[name]
		entry.Record = entry.Record.clone()
		archived = append(archived, entry)
	}
	return archived
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

// assign returns a subnet no tenant holds, live or archived and not yet
// released.
func (s *SQLite) assign(state *snapshot) (string, error) {
	usedSubnets := map[string]bool{}
	for _, record := range state.live {
		usedSubnets[record.Subnet] = true
	}
	for _, archived := range state.archived {
		if archived.Record.Subnet != "" {
			usedSubnets[archived.Record.Subnet] = true
		}
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
	return "", fmt.Errorf("network pool %v has no free /%d", pool, subnetBits)
}
