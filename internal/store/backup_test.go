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
	if err := st.Close(); err != nil {
		t.Fatal(err)
	}
	// A log left by an agent that did not stop cleanly must not be applied
	// to the restored copy.
	if err := os.WriteFile(path+"-wal", []byte("the old database's log"), 0o600); err != nil {
		t.Fatal(err)
	}
	kept, err := Restore(backup, path)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := os.Stat(path + "-wal"); !os.IsNotExist(err) {
		t.Fatalf("the old log is still beside the restored database: %v", err)
	}
	if data, err := os.ReadFile(kept + "-wal"); err != nil || string(data) != "the old database's log" {
		t.Fatalf("the old log was not kept with the old database: %q, %v", data, err)
	}
	restored, err := Open(path, options())
	if err != nil {
		t.Fatal(err)
	}
	defer restored.Close()
	if names := tenantNames(restored); !slices.Equal(names, []string{"acme"}) {
		t.Fatalf("restored tenants = %v", names)
	}
	if info, err := os.Stat(path); err != nil || info.Mode().Perm() != 0o600 {
		t.Fatalf("restored database = %v, %v; want owner-only", info, err)
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
