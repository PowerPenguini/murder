package backup

import (
	"context"
	"errors"
	"io"
	"io/fs"
	"log/slog"
	"net/netip"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"testing"
	"time"

	"filippo.io/age"

	"github.com/PowerPenguini/murder/internal/metrics"
	"github.com/PowerPenguini/murder/internal/spec"
	"github.com/PowerPenguini/murder/internal/store"
)

func discard() *slog.Logger { return slog.New(slog.NewTextHandler(io.Discard, nil)) }

// Exists and Dataset make dirVolumes what a scheduler needs.
func (v *dirVolumes) Exists(_ context.Context, tenant string) (bool, error) {
	_, err := os.Stat(v.data(tenant))
	if errors.Is(err, fs.ErrNotExist) {
		return false, nil
	}
	return err == nil, err
}

func (v *dirVolumes) Dataset(tenant string) string { return dataset(tenant) }

// Written counts the writes since snapshot, the short name of one of the
// tenant's, as bytes.
func (v *dirVolumes) Written(_ context.Context, tenant, snapshot string) (int64, error) {
	name := dataset(tenant) + "@" + snapshot
	if _, err := os.Stat(v.snapshotPath(name)); err != nil {
		return 0, err
	}
	return int64(v.writes[tenant] - v.taken[name]), nil
}

// tenantResults leaves the state's result out.
func tenantResults(results []Result) []Result {
	var tenants []Result
	for _, result := range results {
		if !result.State {
			tenants = append(tenants, result)
		}
	}
	return tenants
}

// fakeState is a desired state of a few tenants.
type fakeState struct {
	node      string
	records   []store.Record
	dir       string
	snapshots []string
}

func (s *fakeState) Node() string         { return s.node }
func (s *fakeState) List() []store.Record { return slices.Clone(s.records) }
func (s *fakeState) Get(name string) (store.Record, error) {
	for _, r := range s.records {
		if r.Spec.Name == name {
			return r, nil
		}
	}
	return store.Record{}, store.ErrNotFound
}

func (s *fakeState) SnapshotFile() (string, error) {
	path := filepath.Join(s.dir, "state-"+time.Now().Format("150405.000000000"))
	s.snapshots = append(s.snapshots, path)
	return path, os.WriteFile(path, []byte("the state"), 0o600)
}

func record(name, node string) store.Record {
	return store.Record{Spec: spec.Tenant{Name: name}, Node: node}
}

func newScheduler(state State, setUp func() (*Backups, error), leader func() bool) *Scheduler {
	return NewScheduler(SchedulerConfig{
		State: state, SetUp: setUp, Leader: leader, Log: discard(),
		Exclusive: func(fn func() error) error { return fn() },
		Every:     time.Hour, VerifyEvery: 7 * 24 * time.Hour,
	})
}

// A run backs up the tenants this node runs, with data and not moving
// away, archived ones until their data as archived is backed up; the
// leader also backs the state up, and removes its copy of it.
func TestARunBacksUpThisNodesTenants(t *testing.T) {
	f := newFixture(t, Test)
	archived := record("archived", "n1")
	archived.ArchivedAt = f.clock
	moving := record("moving", "n1")
	moving.MoveTo = "n2"
	state := &fakeState{node: "n1", dir: t.TempDir(), records: []store.Record{
		record("acme", "n1"), record("other", "n2"), archived, moving, record("nodata", "n1"),
	}}
	for _, name := range []string{"acme", "other", "archived", "moving"} {
		f.volumes.write(t, name, "db", name)
	}
	s := newScheduler(state, func() (*Backups, error) { return f.backups, nil }, nil)
	results, err := s.RunNow(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if len(results) != 3 || results[0].Tenant != "acme" || results[0].Error != "" || results[0].Key == "" ||
		results[1].Tenant != "archived" || results[1].Error != "" || !results[2].State || results[2].Error != "" || results[2].Key == "" {
		t.Fatalf("results %+v", results)
	}
	states, err := f.backups.States(context.Background())
	if err != nil || len(states) != 1 {
		t.Fatalf("states backed up: %v, %v", states, err)
	}
	if _, err := os.Stat(state.snapshots[0]); !errors.Is(err, fs.ErrNotExist) {
		t.Fatalf("the state's copy is left behind: %v", err)
	}
	var page strings.Builder
	s.WriteMetrics(metrics.NewWriter(&page))
	for _, want := range []string{"\nmurder_backups_ready 1\n", `murder_tenant_backup_failing{tenant="acme"} 0`, `murder_tenant_backup_last_success_timestamp_seconds{tenant="acme"}`,
		"\nmurder_state_backup_failing 0\n", "\nmurder_state_backup_last_success_timestamp_seconds "} {
		if !strings.Contains(page.String(), want) {
			t.Fatalf("metrics lack %s:\n%s", want, page.String())
		}
	}
	// A scheduled run skips a tenant backed up within its interval, as
	// after an agent restart; a run on request does not. Neither backs up
	// an archived tenant whose backup holds its data as it is.
	f.clock = f.clock.Add(10 * time.Minute)
	if got := tenantResults(s.run(context.Background(), false)); len(got) != 0 {
		t.Fatalf("a scheduled run right after one made %+v", got)
	}
	if got := tenantResults(s.run(context.Background(), true)); len(got) != 1 || got[0].Tenant != "acme" {
		t.Fatalf("a run on request made %+v", got)
	}
}

// Only the leader backs the state up: it is the cluster's.
func TestOnlyTheLeaderBacksTheStateUp(t *testing.T) {
	f := newFixture(t, Test)
	state := &fakeState{node: "n1", dir: t.TempDir()}
	s := newScheduler(state, func() (*Backups, error) { return f.backups, nil }, func() bool { return false })
	if _, err := s.RunNow(context.Background()); err != nil {
		t.Fatal(err)
	}
	if states, _ := f.backups.States(context.Background()); len(states) != 0 || len(state.snapshots) != 0 {
		t.Fatalf("a follower backed the state up: %v", states)
	}
}

// A tenant whose backup fails is reported failing, and its older backups
// stay, though the policy would thin them out once a new one is there.
func TestAFailedBackupIsReported(t *testing.T) {
	f := newFixture(t, Test)
	state := &fakeState{node: "n1", dir: t.TempDir(), records: []store.Record{record("acme", "n1")}}
	f.volumes.write(t, "acme", "db", "v1")
	s := newScheduler(state, func() (*Backups, error) { return f.backups, nil }, nil)
	for range 3 {
		if _, err := s.RunNow(context.Background()); err != nil {
			t.Fatal(err)
		}
		f.clock = f.clock.Add(time.Hour)
	}
	// A hundred days on, the policy keeps one of the three.
	f.clock = f.clock.Add(100 * 24 * time.Hour)
	f.store.failPut = errors.New("bucket gone")
	results, err := s.RunNow(context.Background())
	if err != nil || len(tenantResults(results)) != 1 || !strings.Contains(results[0].Error, "bucket gone") {
		t.Fatalf("results %+v, %v", results, err)
	}
	var page strings.Builder
	s.WriteMetrics(metrics.NewWriter(&page))
	if !strings.Contains(page.String(), `murder_tenant_backup_failing{tenant="acme"} 1`) {
		t.Fatalf("metrics:\n%s", page.String())
	}
	if list, _ := f.backups.List(context.Background(), "acme"); len(list) != 3 {
		t.Fatalf("backups after a failed one: %v", list)
	}
	// Once a backup works again, the old ones are thinned out.
	f.store.failPut = nil
	if _, err := s.RunNow(context.Background()); err != nil {
		t.Fatal(err)
	}
	if list, _ := f.backups.List(context.Background(), "acme"); len(list) >= 3 {
		t.Fatalf("backups after one worked again: %v", list)
	}
}

// The leader thins out the backups of tenants no longer in the state as
// the policy says, keeping the newest.
func TestBackupsOfPurgedTenantsAreThinnedOut(t *testing.T) {
	f := newFixture(t, Test)
	state := &fakeState{node: "n1", dir: t.TempDir(), records: []store.Record{record("gone", "n1")}}
	f.volumes.write(t, "gone", "db", "v1")
	s := newScheduler(state, func() (*Backups, error) { return f.backups, nil }, nil)
	for range 3 {
		if _, err := s.RunNow(context.Background()); err != nil {
			t.Fatal(err)
		}
		f.clock = f.clock.Add(time.Hour)
	}
	state.records = nil
	f.clock = f.clock.Add(100 * 24 * time.Hour)
	if _, err := s.RunNow(context.Background()); err != nil {
		t.Fatal(err)
	}
	list, _ := f.backups.List(context.Background(), "gone")
	if len(list) == 0 || len(list) >= 3 || !list[len(list)-1].At.Equal(f.clock.Add(-100*24*time.Hour-time.Hour)) {
		t.Fatalf("backups of a purged tenant: %v", list)
	}
}

// replaceVolumes records replacements; nothing else is used.
type replaceVolumes struct {
	Volumes
	replaced []string
}

func (v *replaceVolumes) ReplaceWithCopy(_ context.Context, tenant, name string) (string, error) {
	v.replaced = append(v.replaced, tenant+"<-"+name)
	return "tank/murder/murder-replaced-" + tenant, nil
}

// A restored backup takes a tenant's place only while the tenant is
// archived and of this node, checked under the reconciler's lock: a
// tenant restored, purged or moving in the meantime keeps its data.
func TestABackupReplacesOnlyAnArchivedTenantOfThisNode(t *testing.T) {
	path := filepath.Join(t.TempDir(), "murder.db")
	if err := store.Init(path, "node-1"); err != nil {
		t.Fatal(err)
	}
	st, err := store.Open(path, store.Options{NetworkPool: netip.MustParsePrefix("10.240.0.0/16"), Sealer: func(string, string, spec.Secret) (string, error) { return "sealed", nil }})
	if err != nil {
		t.Fatal(err)
	}
	defer st.Close()
	app := spec.Tenant{Image: "sha256:" + strings.Repeat("a", 64), HealthPath: "/health", User: "1000:1000"}
	if _, err := st.PutApp(store.App{Name: "web", Spec: app}, "test"); err != nil {
		t.Fatal(err)
	}
	if _, err := st.Create(store.NewTenant{Name: "acme", App: "web"}, "test"); err != nil {
		t.Fatal(err)
	}
	volumes := &replaceVolumes{}
	exclusive := 0
	a := NewScheduler(SchedulerConfig{State: st, Log: discard(), Exclusive: func(fn func() error) error { exclusive++; return fn() }})
	a.backups = &Backups{Volumes: volumes}
	ctx := context.Background()
	if _, err := a.ReplaceTenant(ctx, "acme", "murder-copy-acme-restore-1"); !errors.Is(err, store.ErrNotArchived) {
		t.Fatalf("a running tenant's data replaced: %v", err)
	}
	if _, err := a.ReplaceTenant(ctx, "gone", "murder-copy-gone-restore-1"); !errors.Is(err, store.ErrNotFound) {
		t.Fatalf("a purged tenant's data replaced: %v", err)
	}
	if err := st.Archive("acme", "test"); err != nil {
		t.Fatal(err)
	}
	result, err := a.ReplaceTenant(ctx, "acme", "murder-copy-acme-restore-1")
	if err != nil || result.Error != "" || result.Aside != "tank/murder/murder-replaced-acme" {
		t.Fatalf("an archived tenant: %+v, %v", result, err)
	}
	if len(volumes.replaced) != 1 || exclusive != 3 {
		t.Fatalf("replaced %v, under the reconciler's lock %d times", volumes.replaced, exclusive)
	}
	// A run under way holds the replacement off; a caller that gives up
	// stops waiting.
	a.running <- struct{}{}
	waiting, cancel := context.WithCancel(ctx)
	cancel()
	if _, err := a.ReplaceTenant(waiting, "acme", "murder-copy-acme-restore-2"); !errors.Is(err, context.Canceled) || len(volumes.replaced) != 1 {
		t.Fatalf("beside a run: %v", err)
	}
	<-a.running
	// A caller that gives up while the reconciler's lock is waited for
	// replaces nothing.
	waiting, cancel = context.WithCancel(ctx)
	a.exclusive = func(fn func() error) error { cancel(); return fn() }
	if _, err := a.ReplaceTenant(waiting, "acme", "murder-copy-acme-restore-3"); !errors.Is(err, context.Canceled) || len(volumes.replaced) != 1 {
		t.Fatalf("given up under the lock: %v, replaced %v", err, volumes.replaced)
	}
}

// A tenant another node runs, or that moves away, keeps its data.
func TestABackupDoesNotReplaceATenantElsewhere(t *testing.T) {
	far, moving := record("far", "n2"), record("moving", "n1")
	far.ArchivedAt, moving.ArchivedAt, moving.MoveTo = time.Now(), time.Now(), "n2"
	volumes := &replaceVolumes{}
	a := newScheduler(&fakeState{node: "n1", records: []store.Record{far, moving}}, nil, nil)
	a.backups = &Backups{Volumes: volumes}
	for _, name := range []string{"far", "moving"} {
		if _, err := a.ReplaceTenant(context.Background(), name, "murder-copy-"+name+"-restore-1"); !errors.Is(err, store.ErrElsewhere) {
			t.Fatalf("%s: %v", name, err)
		}
	}
	if len(volumes.replaced) != 0 {
		t.Fatalf("replaced %v", volumes.replaced)
	}
}

// Backups that cannot be set up, as with a key file missing, leave the
// agent running: a run on request says why, /metrics says they are not
// ready, and once set-up works they are set up. A missing identity skips
// verifying only.
func TestBackupsThatCannotBeSetUpWaitForAFix(t *testing.T) {
	f := newFixture(t, Test)
	dir := t.TempDir()
	missing := &fs.PathError{Op: "open", Path: filepath.Join(dir, "s3.access"), Err: fs.ErrNotExist}
	setUpErr := error(missing)
	a := NewScheduler(SchedulerConfig{
		State: &fakeState{node: "n1", dir: dir}, Log: discard(), Exclusive: func(fn func() error) error { return fn() },
		Every: time.Hour, VerifyEvery: 7 * 24 * time.Hour, IdentityFile: filepath.Join(dir, "identity.txt"),
		SetUp: func() (*Backups, error) {
			if setUpErr != nil {
				return nil, setUpErr
			}
			return f.backups, nil
		},
	})
	if _, err := a.RunNow(context.Background()); !errors.Is(err, fs.ErrNotExist) {
		t.Fatalf("a run with the S3 keys missing: %v", err)
	}
	var page strings.Builder
	a.WriteMetrics(metrics.NewWriter(&page))
	if !strings.Contains(page.String(), "\nmurder_backups_ready 0\n") {
		t.Fatalf("metrics:\n%s", page.String())
	}
	setUpErr = nil
	if err := a.ready(); err != nil || a.backups == nil {
		t.Fatalf("set up once the keys are there: %v", err)
	}
	page.Reset()
	a.WriteMetrics(metrics.NewWriter(&page))
	if !strings.Contains(page.String(), "\nmurder_backups_ready 1\n") {
		t.Fatalf("metrics:\n%s", page.String())
	}
	// Verifications wait a run after backups start, and for the identity.
	if first := time.Until(a.verified.Add(a.verify)); first < 50*time.Minute || first > time.Hour {
		t.Fatalf("the first verification in %v, not a run after backups start", first)
	}
	if a.loadIdentity() {
		t.Fatal("a missing identity read")
	}
	identity, err := age.GenerateX25519Identity()
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(dir, "identity.txt"), []byte(identity.String()+"\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	if !a.loadIdentity() {
		t.Fatal("the identity, once there, not read")
	}
}

// Volumes that cannot say where a tenant's data is cannot be backed up.
func TestBackupsNeedTenantData(t *testing.T) {
	a := newScheduler(&fakeState{node: "n1"}, func() (*Backups, error) { return &Backups{Volumes: &replaceVolumes{}}, nil }, nil)
	if err := a.ready(); err == nil || a.backups != nil {
		t.Fatalf("set up without tenant data: %v", err)
	}
}

// Writes made after a tenant's last backup and before it was archived
// reach the bucket: an archived tenant is backed up until its newest
// backup holds its data as archived, forced or not, and then no more.
func TestAnArchivedTenantGetsItsFinalBackup(t *testing.T) {
	f := newFixture(t, Test)
	state := &fakeState{node: "n1", dir: t.TempDir(), records: []store.Record{record("acme", "n1"), record("brief", "n1")}}
	f.volumes.write(t, "acme", "db", "old")
	s := newScheduler(state, func() (*Backups, error) { return f.backups, nil }, nil)
	if _, err := s.RunNow(context.Background()); err != nil {
		t.Fatal(err)
	}
	// acme takes writes, then both are archived before the next run;
	// brief, made since, never had a backup.
	f.clock = f.clock.Add(10 * time.Minute)
	f.volumes.write(t, "acme", "db", "final archived data")
	f.volumes.write(t, "brief", "db", "made and archived between runs")
	state.records = append(state.records[:1], record("brief", "n1"))
	for i := range state.records {
		state.records[i].ArchivedAt = f.clock
	}
	// A scheduled run backs them up at once, though acme's backup is
	// recent; a run on request then backs up nothing more.
	f.clock = f.clock.Add(time.Minute)
	if got := tenantResults(s.run(context.Background(), false)); len(got) != 2 || got[0].Error != "" || got[1].Error != "" {
		t.Fatalf("the first run after archiving made %+v", got)
	}
	for range 3 {
		f.clock = f.clock.Add(time.Hour)
		results, err := s.RunNow(context.Background())
		if err != nil || len(tenantResults(results)) != 0 {
			t.Fatalf("a run on an archived tenant backed up made %+v, %v", results, err)
		}
	}
	for tenant, want := range map[string]string{"acme": "final archived data", "brief": "made and archived between runs"} {
		list, err := f.backups.List(context.Background(), tenant)
		if err != nil || len(list) == 0 {
			t.Fatalf("%s's backups: %v, %v", tenant, list, err)
		}
		_, restored, err := f.backups.Fill(context.Background(), tenant, list[len(list)-1].At, f.identity)
		if err != nil {
			t.Fatal(err)
		}
		if data, _ := os.ReadFile(filepath.Join(f.volumes.root, "copies", restored, "db")); string(data) != want {
			t.Fatalf("%s's newest backup holds %q", tenant, data)
		}
	}
	// Data that changes after all, as a restore replacing it, is backed up
	// again.
	f.volumes.write(t, "acme", "db", "restored")
	if got := tenantResults(s.run(context.Background(), false)); len(got) != 1 || got[0].Tenant != "acme" {
		t.Fatalf("a changed archived tenant: %+v", got)
	}
}

// A failed backup of the cluster's state is a failed result, as a tenant's
// is, and the metrics say so; a node that no longer leads reports nothing
// of the state.
func TestAFailedStateBackupIsReported(t *testing.T) {
	f := newFixture(t, Test)
	leads := true
	state := &fakeState{node: "n1", dir: t.TempDir()}
	f.store.failPut = errors.New("S3 unavailable")
	s := newScheduler(state, func() (*Backups, error) { return f.backups, nil }, func() bool { return leads })
	results, err := s.RunNow(context.Background())
	if err != nil || len(results) != 1 || !results[0].State || !strings.Contains(results[0].Error, "S3 unavailable") {
		t.Fatalf("results %+v, %v", results, err)
	}
	var page strings.Builder
	s.WriteMetrics(metrics.NewWriter(&page))
	if !strings.Contains(page.String(), "\nmurder_state_backup_failing 1\n") || strings.Contains(page.String(), "\nmurder_state_backup_last_success_timestamp_seconds ") {
		t.Fatalf("metrics:\n%s", page.String())
	}
	f.store.failPut = nil
	if results, err := s.RunNow(context.Background()); err != nil || len(results) != 1 || results[0].Error != "" || results[0].Key == "" {
		t.Fatalf("results %+v, %v", results, err)
	}
	page.Reset()
	s.WriteMetrics(metrics.NewWriter(&page))
	if !strings.Contains(page.String(), "\nmurder_state_backup_failing 0\n") || !strings.Contains(page.String(), "\nmurder_state_backup_last_success_timestamp_seconds ") {
		t.Fatalf("metrics:\n%s", page.String())
	}
	leads = false
	if results, err := s.RunNow(context.Background()); err != nil || len(results) != 0 {
		t.Fatalf("results %+v, %v", results, err)
	}
	page.Reset()
	s.WriteMetrics(metrics.NewWriter(&page))
	if strings.Contains(page.String(), "\nmurder_state_backup_failing ") {
		t.Fatalf("a follower reports the state's backups:\n%s", page.String())
	}
}
