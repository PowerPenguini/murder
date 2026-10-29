package main

import (
	"io"
	"log/slog"
	"net"
	"net/http"
	"net/netip"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"testing"

	"github.com/PowerPenguini/murder/internal/api"
	"github.com/PowerPenguini/murder/internal/secrets"
	"github.com/PowerPenguini/murder/internal/spec"
	"github.com/PowerPenguini/murder/internal/store"
)

// A backup taken through the agent restores on a node with the same master
// key, and only there.
func TestBackupAndRestore(t *testing.T) {
	dir := t.TempDir()
	keyPath := filepath.Join(dir, "master.key")
	if err := secrets.CreateKey(keyPath); err != nil {
		t.Fatal(err)
	}
	key, err := secrets.LoadKey(keyPath)
	if err != nil {
		t.Fatal(err)
	}
	database := filepath.Join(dir, "murder.db")
	if err := store.Init(database, "node-1"); err != nil {
		t.Fatal(err)
	}
	st, err := store.Open(database, store.Options{NetworkPool: netip.MustParsePrefix("10.240.0.0/16"), Sealer: key.NewSealed})
	if err != nil {
		t.Fatal(err)
	}
	defer st.Close()
	app := spec.Tenant{Image: "sha256:" + strings.Repeat("a", 64), HealthPath: "/health", User: "1000:1000", Secrets: spec.Secrets{"SESSION": {}}}
	if _, err := st.PutApp(store.App{Name: "web", Spec: app}, "test"); err != nil {
		t.Fatal(err)
	}
	if _, err := st.Create(store.NewTenant{Name: "acme", App: "web"}, "test"); err != nil {
		t.Fatal(err)
	}
	socket := filepath.Join(dir, "agent.sock")
	listener, err := net.Listen("unix", socket)
	if err != nil {
		t.Fatal(err)
	}
	server := &http.Server{Handler: api.NewServer(st, nil, nil, key, slog.New(slog.NewTextHandler(io.Discard, nil))).Handler()}
	go server.Serve(listener)
	defer server.Close()

	backups := filepath.Join(dir, "backups")
	if err := os.MkdirAll(backups, 0o700); err != nil {
		t.Fatal(err)
	}
	for _, old := range []string{"murder-20260101T000000Z.db", "murder-20260102T000000Z.db"} {
		if err := os.WriteFile(filepath.Join(backups, old), nil, 0o600); err != nil {
			t.Fatal(err)
		}
	}
	out := capture(t)
	if err := run([]string{"backup", "-socket", socket, "-keep", "2", backups}); err != nil {
		t.Fatal(err)
	}
	entries, _ := os.ReadDir(backups)
	var names []string
	for _, entry := range entries {
		names = append(names, entry.Name())
	}
	if len(names) != 2 || names[0] != "murder-20260102T000000Z.db" || !strings.HasPrefix(names[1], "murder-2") || !strings.Contains(out.String(), "backed up node node-1 (1 tenants)") {
		t.Fatalf("backups = %v, output %q; want the newest old one and the new one", names, out)
	}
	backup := filepath.Join(backups, names[1])

	// A node whose key is not the one the secrets were sealed with refuses
	// the backup.
	otherKey := filepath.Join(dir, "other.key")
	if err := secrets.CreateKey(otherKey); err != nil {
		t.Fatal(err)
	}
	restored := filepath.Join(dir, "new", "murder.db")
	config := func(keyFile string) string {
		path := filepath.Join(dir, "agent.yaml")
		yaml := "baseDomain: example.test\nfirewall: off\ndatabase: " + restored + "\nmasterKeyFile: " + keyFile + "\n"
		if err := os.WriteFile(path, []byte(yaml), 0o600); err != nil {
			t.Fatal(err)
		}
		return path
	}
	if err := run([]string{"restore", "-config", config(otherKey), backup}); err == nil || !strings.Contains(err.Error(), "does not open the backup's secrets") {
		t.Fatalf("restore with another key = %v", err)
	}
	if _, err := os.Stat(restored); !os.IsNotExist(err) {
		t.Fatalf("a refused restore wrote the database: %v", err)
	}
	if err := run([]string{"restore", "-config", config(keyPath), backup}); err != nil {
		t.Fatal(err)
	}
	info, err := store.CheckBackup(restored)
	if err != nil || info.Node != "node-1" || info.Tenants != 1 {
		t.Fatalf("restored database = %+v, %v", info, err)
	}
	if !slices.Contains(strings.Split(strings.TrimSpace(out.String()), "\n"), "restored node node-1 (1 tenants) from "+backup) {
		t.Fatalf("output:\n%s", out)
	}
}
