package backup

import (
	"archive/tar"
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"io/fs"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"sync"
	"testing"
	"time"

	"filippo.io/age"
)

// memoryStore is a bucket in memory, with object lock as S3 has it: a
// locked object is not deleted before its lock ends.
type memoryStore struct {
	mu      sync.Mutex
	objects map[string][]byte
	until   map[string]time.Time
	now     func() time.Time
	locked  bool
	exists  bool
}

func newMemoryStore(now func() time.Time) *memoryStore {
	return &memoryStore{objects: map[string][]byte{}, until: map[string]time.Time{}, now: now, exists: true}
}

func (m *memoryStore) Put(_ context.Context, key string, r io.Reader, until time.Time) error {
	data, err := io.ReadAll(r)
	if err != nil {
		return err
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	m.objects[key] = data
	m.until[key] = until
	return nil
}

func (m *memoryStore) Get(_ context.Context, key string) (io.ReadCloser, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	data, ok := m.objects[key]
	if !ok {
		return nil, fmt.Errorf("no object %s", key)
	}
	return io.NopCloser(bytes.NewReader(data)), nil
}

func (m *memoryStore) List(_ context.Context, prefix string) ([]Object, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	var objects []Object
	for key, data := range m.objects {
		if strings.HasPrefix(key, prefix) {
			objects = append(objects, Object{Key: key, Size: int64(len(data))})
		}
	}
	sort.Slice(objects, func(i, j int) bool { return objects[i].Key < objects[j].Key })
	return objects, nil
}

var errLocked = errors.New("object locked")

func (m *memoryStore) Delete(_ context.Context, key string) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	if until := m.until[key]; !until.IsZero() && m.now().Before(until) {
		return errLocked
	}
	delete(m.objects, key)
	return nil
}

func (m *memoryStore) Locked(context.Context) (bool, error) { return m.locked, nil }
func (m *memoryStore) Create(_ context.Context, locked bool) error {
	m.exists, m.locked = true, locked
	return nil
}
func (m *memoryStore) Exists(context.Context) (bool, error) { return m.exists, nil }
func (m *memoryStore) Destroy(context.Context) error {
	for key := range m.objects {
		if err := m.Delete(context.Background(), key); err != nil {
			return err
		}
	}
	m.exists = false
	return nil
}

// dirVolumes is ZFS on plain directories: a tenant's data is a directory,
// a snapshot a copy of it, a copy a directory beside it.
type dirVolumes struct {
	root     string
	now      func() time.Time
	replaced map[string]string
}

func newDirVolumes(t *testing.T, now func() time.Time) *dirVolumes {
	return &dirVolumes{root: t.TempDir(), now: now, replaced: map[string]string{}}
}

func dataset(tenant string) string { return "tank/tenants/" + tenant }

func (v *dirVolumes) data(tenant string) string { return filepath.Join(v.root, "data", tenant) }

func (v *dirVolumes) write(t *testing.T, tenant, name, content string) {
	t.Helper()
	path := filepath.Join(v.data(tenant), name)
	if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(path, []byte(content), 0o600); err != nil {
		t.Fatal(err)
	}
}

func (v *dirVolumes) snapshotPath(snapshot string) string {
	return filepath.Join(v.root, "snapshots", strings.NewReplacer("/", "_", "@", "_").Replace(snapshot))
}

func copyTree(from, to string) error {
	return filepath.WalkDir(from, func(path string, entry fs.DirEntry, err error) error {
		if err != nil {
			return err
		}
		relative, _ := filepath.Rel(from, path)
		target := filepath.Join(to, relative)
		if entry.IsDir() {
			return os.MkdirAll(target, 0o755)
		}
		data, err := os.ReadFile(path)
		if err != nil {
			return err
		}
		return os.WriteFile(target, data, 0o600)
	})
}

func (v *dirVolumes) Snapshot(_ context.Context, tenant, reason string) (string, error) {
	name := dataset(tenant) + "@murder-" + reason + "-" + v.now().UTC().Format(stamp)
	if err := os.MkdirAll(v.data(tenant), 0o755); err != nil {
		return "", err
	}
	return name, copyTree(v.data(tenant), v.snapshotPath(name))
}

func (v *dirVolumes) Prune(_ context.Context, tenant, reason string, keep int) error {
	entries, _ := os.ReadDir(filepath.Join(v.root, "snapshots"))
	var names []string
	prefix := strings.NewReplacer("/", "_", "@", "_").Replace(dataset(tenant) + "@murder-" + reason + "-")
	for _, entry := range entries {
		if strings.HasPrefix(entry.Name(), prefix) {
			names = append(names, entry.Name())
		}
	}
	sort.Strings(names)
	for _, name := range names[:max(0, len(names)-keep)] {
		_ = os.RemoveAll(filepath.Join(v.root, "snapshots", name))
	}
	return nil
}

func (v *dirVolumes) SnapshotDir(_ context.Context, tenant, snapshot string) (string, error) {
	return v.snapshotPath(snapshot), nil
}

func (v *dirVolumes) CreateCopy(_ context.Context, tenant, name string) (string, error) {
	path := filepath.Join(v.root, "copies", name)
	return path, os.MkdirAll(path, 0o755)
}

func (v *dirVolumes) DestroyCopy(_ context.Context, name string) error {
	return os.RemoveAll(filepath.Join(v.root, "copies", name))
}

func (v *dirVolumes) ReplaceWithCopy(_ context.Context, tenant, name string) (string, error) {
	aside := filepath.Join(v.root, "aside", name)
	_ = os.MkdirAll(filepath.Dir(aside), 0o755)
	if err := os.Rename(v.data(tenant), aside); err != nil {
		return "", err
	}
	v.replaced[tenant] = name
	return aside, os.Rename(filepath.Join(v.root, "copies", name), v.data(tenant))
}

type fixture struct {
	clock    time.Time
	store    *memoryStore
	volumes  *dirVolumes
	backups  *Backups
	identity *age.X25519Identity
}

func policy(mode Mode) Policy {
	p := Policy{Mode: mode, KeepHourly: 48 * time.Hour, KeepDaily: 30 * 24 * time.Hour, KeepMonthly: 365 * 24 * time.Hour, KeepLocal: 3}
	if mode == Production {
		p.LockFor = 30 * 24 * time.Hour
	}
	return p
}

func newFixture(t *testing.T, mode Mode) *fixture {
	t.Helper()
	identity, err := age.GenerateX25519Identity()
	if err != nil {
		t.Fatal(err)
	}
	f := &fixture{clock: time.Date(2026, 11, 2, 10, 0, 0, 0, time.UTC), identity: identity}
	now := func() time.Time { return f.clock }
	f.store, f.volumes = newMemoryStore(now), newDirVolumes(t, now)
	f.backups = &Backups{Store: f.store, Volumes: f.volumes, Recipient: identity.Recipient(), Policy: policy(mode), Now: now}
	return f
}

func (f *fixture) backUp(t *testing.T, tenant string) Backup {
	t.Helper()
	backup, err := f.backups.BackUp(context.Background(), tenant, dataset(tenant))
	if err != nil {
		t.Fatal(err)
	}
	return backup
}

func TestAPolicyFitsItsMode(t *testing.T) {
	if err := policy(Test).Validate(); err != nil {
		t.Fatal(err)
	}
	if err := policy(Production).Validate(); err != nil {
		t.Fatal(err)
	}
	locked := policy(Test)
	locked.LockFor = time.Hour
	if locked.Validate() == nil {
		t.Error("a test policy with a lock")
	}
	unlocked := policy(Production)
	unlocked.LockFor = 0
	if unlocked.Validate() == nil {
		t.Error("a production policy without a lock")
	}
	long := policy(Production)
	long.LockFor = long.KeepDaily + time.Hour
	if long.Validate() == nil {
		t.Error("a lock longer than keeping daily backups")
	}
}

// Every backup is the whole data, receipts too, compressed and encrypted:
// each one alone restores everything.
func TestEveryBackupIsTheWholeDataCompressedAndEncrypted(t *testing.T) {
	f := newFixture(t, Test)
	database := strings.Repeat("row of the database;", 5000)
	f.volumes.write(t, "acme", "data.db", database)
	f.volumes.write(t, "acme", "receipts/2026/a.pdf", "%PDF receipt a")
	first := f.backUp(t, "acme")
	f.clock = f.clock.Add(time.Hour)
	f.volumes.write(t, "acme", "receipts/2026/b.pdf", "%PDF receipt b")
	second := f.backUp(t, "acme")
	object, _ := f.store.Get(context.Background(), second.Key)
	sealed, _ := io.ReadAll(object)
	if bytes.Contains(sealed, []byte("receipt")) || bytes.Contains(sealed, []byte("row of the database")) {
		t.Fatal("a backup is stored in the clear")
	}
	if second.Size >= int64(len(database)) {
		t.Fatalf("a backup of %d bytes for %d bytes of very compressible data", second.Size, len(database))
	}
	// The second backup alone has every file; the first has what was then.
	for key, want := range map[string][]string{first.Key: {"data.db", "receipts/2026/a.pdf"}, second.Key: {"data.db", "receipts/2026/a.pdf", "receipts/2026/b.pdf"}} {
		content, err := f.backups.open(context.Background(), key, f.identity)
		if err != nil {
			t.Fatal(err)
		}
		var names []string
		archive := tar.NewReader(content)
		for {
			header, err := archive.Next()
			if err != nil {
				break
			}
			if header.Typeflag == tar.TypeReg {
				names = append(names, header.Name)
			}
		}
		content.Close()
		sort.Strings(names)
		if strings.Join(names, ",") != strings.Join(want, ",") {
			t.Fatalf("%s holds %v, want %v", key, names, want)
		}
	}
	// Only KeepLocal backup snapshots stay on the node.
	for range 5 {
		f.clock = f.clock.Add(time.Hour)
		f.backUp(t, "acme")
	}
	if entries, _ := os.ReadDir(filepath.Join(f.volumes.root, "snapshots")); len(entries) != 3 {
		t.Fatalf("%d backup snapshots kept", len(entries))
	}
}

// In production the first backup of each day is locked for LockFor, the
// others only as long as they are kept; in test, none is locked.
func TestProductionLocksBackups(t *testing.T) {
	for _, mode := range []Mode{Test, Production} {
		f := newFixture(t, mode)
		f.volumes.write(t, "acme", "data.db", "x")
		first := f.backUp(t, "acme")
		f.clock = f.clock.Add(time.Hour)
		second := f.backUp(t, "acme")
		firstUntil, secondUntil := f.store.until[first.Key], f.store.until[second.Key]
		switch mode {
		case Production:
			if !firstUntil.Equal(first.At.Add(30*24*time.Hour)) || !secondUntil.Equal(second.At.Add(48*time.Hour)) {
				t.Fatalf("locks until %s and %s", firstUntil, secondUntil)
			}
			if err := f.store.Delete(context.Background(), first.Key); !errors.Is(err, errLocked) {
				t.Fatalf("a locked backup deleted: %v", err)
			}
		case Test:
			if !firstUntil.IsZero() || !secondUntil.IsZero() {
				t.Fatal("a test backup is locked")
			}
			if err := f.store.Destroy(context.Background()); err != nil {
				t.Fatalf("a test bucket not destroyed at once: %v", err)
			}
		}
	}
}

// Retention keeps every backup of the last 48 hours, the first of each day
// for 30 days and of each month for a year; production locks never stop it.
func TestRetentionThinsBackupsOut(t *testing.T) {
	f := newFixture(t, Production)
	f.volumes.write(t, "acme", "data.db", "x")
	start := f.clock
	for f.clock.Before(start.Add(14 * 30 * 24 * time.Hour)) {
		f.backUp(t, "acme")
		f.clock = f.clock.Add(3 * time.Hour)
		if f.clock.Hour() < 3 { // prune once a day, as the agent does more often
			if _, err := f.backups.Prune(context.Background(), "acme"); err != nil {
				t.Fatal(err)
			}
		}
	}
	if _, err := f.backups.Prune(context.Background(), "acme"); err != nil {
		t.Fatal(err)
	}
	backups, _ := f.backups.List(context.Background(), "acme")
	days, months := map[string]int{}, map[string]int{}
	recent := 0
	for _, b := range backups {
		age := f.clock.Sub(b.At)
		switch {
		case age >= 365*24*time.Hour:
			t.Fatalf("a backup of %s outlived a year", b.At)
		case age >= 30*24*time.Hour:
			months[b.At.Format("2006-01")]++
		case age >= 48*time.Hour:
			days[b.At.Format("2006-01-02")]++
		default:
			recent++
		}
	}
	for day, n := range days {
		if n != 1 {
			t.Fatalf("%d backups kept of %s", n, day)
		}
	}
	for month, n := range months {
		if n != 1 {
			t.Fatalf("%d backups kept of %s", n, month)
		}
	}
	if recent < 15 || recent > 16 || len(days) < 27 || len(months) < 10 {
		t.Fatalf("%d recent, %d daily, %d monthly backups", recent, len(days), len(months))
	}
}

// Verify reads the newest backup through and finds it the snapshot it was
// made of; Restore puts a chosen moment back, keeping what was there.
func TestVerifyAndRestore(t *testing.T) {
	f := newFixture(t, Test)
	var times []time.Time
	for i := 1; i <= 3; i++ {
		f.volumes.write(t, "acme", "data.db", fmt.Sprintf("version %d", i))
		f.volumes.write(t, "acme", fmt.Sprintf("receipts/%d.pdf", i), "%PDF")
		times = append(times, f.backUp(t, "acme").At)
		f.clock = f.clock.Add(time.Hour)
	}
	verified, err := f.backups.Verify(context.Background(), "acme", dataset("acme"), f.identity)
	if err != nil {
		t.Fatal(err)
	}
	if verified.Files != 4 || !verified.Compared {
		t.Fatalf("verified %+v", verified)
	}
	other, _ := age.GenerateX25519Identity()
	if _, err := f.backups.Verify(context.Background(), "acme", dataset("acme"), other); err == nil {
		t.Fatal("verified with another key")
	}
	backup, aside, err := f.backups.Restore(context.Background(), "acme", times[1].Add(time.Minute), f.identity)
	if err != nil {
		t.Fatal(err)
	}
	restored, _ := os.ReadFile(filepath.Join(f.volumes.data("acme"), "data.db"))
	_, third := os.Stat(filepath.Join(f.volumes.data("acme"), "receipts/3.pdf"))
	kept, _ := os.ReadFile(filepath.Join(aside, "data.db"))
	if !backup.At.Equal(times[1]) || string(restored) != "version 2" || !os.IsNotExist(third) || string(kept) != "version 3" {
		t.Fatalf("restored %+v: %q, receipt 3 %v, kept %q", backup, restored, third, kept)
	}
	// A time given to the second takes in its whole second.
	f.clock = f.clock.Add(1500 * time.Millisecond)
	f.volumes.write(t, "acme", "data.db", "version 4")
	precise := f.backUp(t, "acme")
	if chosen, err := at([]Backup{precise}, precise.At.Truncate(time.Second)); err != nil || chosen.Key != precise.Key {
		t.Fatalf("the backup of %s at its second: %+v, %v", precise.At, chosen, err)
	}
	if _, _, err := f.backups.Restore(context.Background(), "acme", times[0].Add(-time.Hour), f.identity); err == nil {
		t.Fatal("restored a moment before any backup")
	}
}

// The cluster's state is kept beside the tenants', compressed and
// encrypted, thinned out alike.
func TestTheStateIsBackedUp(t *testing.T) {
	f := newFixture(t, Test)
	path := filepath.Join(t.TempDir(), "murder.db")
	if err := os.WriteFile(path, []byte("state"), 0o600); err != nil {
		t.Fatal(err)
	}
	copied, err := f.backups.BackUpState(context.Background(), path)
	if err != nil {
		t.Fatal(err)
	}
	var got bytes.Buffer
	if err := f.backups.GetState(context.Background(), copied.Key, f.identity, &got); err != nil || got.String() != "state" {
		t.Fatalf("state = %q, %v", got.String(), err)
	}
	f.clock = f.clock.Add(400 * 24 * time.Hour)
	if deleted, err := f.backups.PruneState(context.Background()); err != nil || deleted != 1 {
		t.Fatalf("pruned %d, %v", deleted, err)
	}
}
