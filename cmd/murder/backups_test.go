package main

import (
	"context"
	"errors"
	"io"
	"log/slog"
	"net/netip"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/PowerPenguini/murder/internal/backup"
	"github.com/PowerPenguini/murder/internal/spec"
	"github.com/PowerPenguini/murder/internal/store"
)

// Backups commands take their flags before or after the tenant: with the
// config named after it, verify gets as far as asking for the identity.
func TestBackupsFlagsComeAnywhere(t *testing.T) {
	dir := t.TempDir()
	for _, name := range []string{"s3.access", "s3.secret"} {
		if err := os.WriteFile(filepath.Join(dir, name), []byte("key\n"), 0o600); err != nil {
			t.Fatal(err)
		}
	}
	configPath := filepath.Join(dir, "agent.yaml")
	config := "baseDomain: example.com\nvolumes: {driver: zfs, parent: tank/murder/tenants}\nbackups:\n  mode: test\n" +
		"  endpoint: https://hel1.your-objectstorage.com\n  region: hel1\n  bucket: b\n" +
		"  accessKeyFile: " + filepath.Join(dir, "s3.access") + "\n  secretKeyFile: " + filepath.Join(dir, "s3.secret") + "\n" +
		"  recipient: age1ql3z7hjy54pw3hyww5ayyfg7zqgvc7w3j2elw8zmrj2kg5sfn9aqmcac8p\n"
	if err := os.WriteFile(configPath, []byte(config), 0o600); err != nil {
		t.Fatal(err)
	}
	err := runBackups([]string{"verify", "acme", "-config", configPath})
	if err == nil || !strings.Contains(err.Error(), "needs -identity") {
		t.Fatalf("flags after the tenant: %v", err)
	}
}

// replaceVolumes records replacements; nothing else is used.
type replaceVolumes struct {
	backup.Volumes
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
	a := &agentBackups{
		backups: &backup.Backups{Volumes: volumes}, store: st, running: make(chan struct{}, 1),
		log:       slog.New(slog.NewTextHandler(io.Discard, nil)),
		exclusive: func(fn func() error) error { exclusive++; return fn() },
	}
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
}
