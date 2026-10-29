package store

import (
	"bytes"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"testing"
)

// backupTo writes a backup of st to a file and returns its path.
func backupTo(t *testing.T, st *SQLite) string {
	t.Helper()
	var copy bytes.Buffer
	if err := st.Backup(&copy); err != nil {
		t.Fatal(err)
	}
	path := filepath.Join(t.TempDir(), "backup.db")
	if err := os.WriteFile(path, copy.Bytes(), 0o600); err != nil {
		t.Fatal(err)
	}
	return path
}

func TestBackupIsOneWholeFile(t *testing.T) {
	st, path := withApp(t, options())
	if _, err := st.Create(NewTenant{Name: "acme", App: "web"}, "alice"); err != nil {
		t.Fatal(err)
	}
	backup := backupTo(t, st)
	info, err := CheckBackup(backup)
	if err != nil {
		t.Fatal(err)
	}
	if info.Node != "node-1" || info.Tenants != 1 || info.Secret == nil || info.Secret.Tenant != "acme" || info.Secret.Name != "SESSION" {
		t.Fatalf("backup info = %+v", info)
	}
	// Checking it reads only the one file and leaves no journal behind.
	if entries, _ := os.ReadDir(filepath.Dir(backup)); len(entries) != 1 {
		t.Fatalf("beside the backup: %v", entries)
	}
	// The copy is made and removed beside the database.
	var left []string
	entries, _ := os.ReadDir(filepath.Dir(path))
	for _, entry := range entries {
		if !slices.Contains([]string{"murder.db", "murder.db-wal", "murder.db-shm", "murder.db.lock"}, entry.Name()) {
			left = append(left, entry.Name())
		}
	}
	if len(left) > 0 {
		t.Fatalf("left beside the database: %v", left)
	}
}

func TestCheckBackupRefusesWhatIsNotOne(t *testing.T) {
	dir := t.TempDir()
	garbage := filepath.Join(dir, "garbage.db")
	if err := os.WriteFile(garbage, []byte(strings.Repeat("not a database ", 100)), 0o600); err != nil {
		t.Fatal(err)
	}
	if _, err := CheckBackup(garbage); err == nil {
		t.Fatal("garbage passed as a backup")
	}
	if _, err := CheckBackup(filepath.Join(dir, "missing.db")); err == nil {
		t.Fatal("a missing file passed as a backup")
	}
	st, _ := withApp(t, options())
	backup := backupTo(t, st)
	data, _ := os.ReadFile(backup)
	truncated := filepath.Join(dir, "truncated.db")
	if err := os.WriteFile(truncated, data[:len(data)/2], 0o600); err != nil {
		t.Fatal(err)
	}
	if _, err := CheckBackup(truncated); err == nil {
		t.Fatal("half a backup passed as a whole one")
	}
}

func TestRestoreReplacesTheDatabaseAndKeepsTheOldOne(t *testing.T) {
	st, path := withApp(t, options())
	if _, err := st.Create(NewTenant{Name: "acme", App: "web"}, "alice"); err != nil {
		t.Fatal(err)
	}
	backup := backupTo(t, st)
	if _, err := st.Create(NewTenant{Name: "beta", App: "web"}, "alice"); err != nil {
		t.Fatal(err)
	}
	if _, err := Restore(backup, path); err == nil || !strings.Contains(err.Error(), "stop it first") {
		t.Fatalf("Restore while the agent runs = %v", err)
	}
	// The node as a crash leaves it: beta is only in the log.
	crashed := filepath.Join(t.TempDir(), "murder.db")
	for _, suffix := range []string{"", "-wal"} {
		data, err := os.ReadFile(path + suffix)
		if err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(crashed+suffix, data, 0o600); err != nil {
			t.Fatal(err)
		}
	}
	kept, err := Restore(backup, crashed)
	if err != nil {
		t.Fatal(err)
	}
	for _, suffix := range []string{"-wal", "-shm"} {
		if _, err := os.Stat(crashed + suffix); !os.IsNotExist(err) {
			t.Fatalf("%s is beside the restored database: %v", suffix, err)
		}
	}
	restored, err := Open(crashed, options())
	if err != nil {
		t.Fatal(err)
	}
	defer restored.Close()
	if names := tenantNames(restored); !slices.Equal(names, []string{"acme"}) {
		t.Fatalf("restored tenants = %v", names)
	}
	if info, err := os.Stat(crashed); err != nil || info.Mode().Perm() != 0o600 {
		t.Fatalf("restored database = %v, %v; want owner-only", info, err)
	}
	// The database replaced is kept whole, with what was only in its log.
	if info, err := CheckBackup(kept); err != nil || info.Tenants != 2 {
		t.Fatalf("kept database = %+v, %v; want both tenants", info, err)
	}
}

// A log whose database is gone is moved aside too, not applied to the
// restored copy.
func TestRestoreMovesALogWithoutItsDatabase(t *testing.T) {
	st, path := withApp(t, options())
	backup := backupTo(t, st)
	if _, err := st.Create(NewTenant{Name: "acme", App: "web"}, "alice"); err != nil {
		t.Fatal(err)
	}
	lost := filepath.Join(t.TempDir(), "murder.db")
	data, err := os.ReadFile(path + "-wal")
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(lost+"-wal", data, 0o600); err != nil {
		t.Fatal(err)
	}
	kept, err := Restore(backup, lost)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := os.Stat(kept + "-wal"); err != nil {
		t.Fatalf("the log was not kept: %v", err)
	}
	restored, err := Open(lost, options())
	if err != nil {
		t.Fatal(err)
	}
	defer restored.Close()
	if names := tenantNames(restored); len(names) != 0 {
		t.Fatalf("restored tenants = %v", names)
	}
}

// Copies a crash left beside the database go when it is next opened.
func TestOpenRemovesStaleCopies(t *testing.T) {
	st, path := openNew(t, options())
	if err := st.Close(); err != nil {
		t.Fatal(err)
	}
	for _, name := range []string{path + ".backup-123", path + ".restore-456", path + ".before-restore-20260101T000000Z"} {
		if err := os.WriteFile(name, []byte("x"), 0o600); err != nil {
			t.Fatal(err)
		}
	}
	reopened, err := Open(path, options())
	if err != nil {
		t.Fatal(err)
	}
	defer reopened.Close()
	for name, want := range map[string]bool{".backup-123": false, ".restore-456": false, ".before-restore-20260101T000000Z": true} {
		if _, err := os.Stat(path + name); (err == nil) != want {
			t.Errorf("%s exists = %v, want %v", name, err == nil, want)
		}
	}
}

func TestRestoreRefusesADamagedBackup(t *testing.T) {
	st, path := withApp(t, options())
	if err := st.Close(); err != nil {
		t.Fatal(err)
	}
	before, _ := os.ReadFile(path)
	garbage := filepath.Join(t.TempDir(), "garbage.db")
	if err := os.WriteFile(garbage, []byte(strings.Repeat("x", 8192)), 0o600); err != nil {
		t.Fatal(err)
	}
	if _, err := Restore(garbage, path); err == nil {
		t.Fatal("a damaged backup was restored")
	}
	if after, _ := os.ReadFile(path); !bytes.Equal(before, after) {
		t.Fatal("a refused restore changed the database")
	}
	entries, _ := os.ReadDir(filepath.Dir(path))
	for _, entry := range entries {
		if strings.Contains(entry.Name(), "restore") {
			t.Fatalf("a refused restore left %s", entry.Name())
		}
	}
}

// A new node, with no database yet, is restored into place.
func TestRestoreOntoANewNode(t *testing.T) {
	st, _ := withApp(t, options())
	backup := backupTo(t, st)
	path := filepath.Join(t.TempDir(), "new", "murder.db")
	kept, err := Restore(backup, path)
	if err != nil || kept != "" {
		t.Fatalf("Restore = %q, %v", kept, err)
	}
	if _, err := CheckBackup(path); err != nil {
		t.Fatal(err)
	}
}

func tenantNames(st *SQLite) []string {
	var names []string
	for _, record := range st.List() {
		names = append(names, record.Spec.Name)
	}
	return names
}
