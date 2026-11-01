package main

import (
	"context"
	"errors"
	"io"
	"io/fs"
	"log/slog"
	"net/netip"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"filippo.io/age"

	"github.com/PowerPenguini/murder/internal/backup"
	"github.com/PowerPenguini/murder/internal/config"
	"github.com/PowerPenguini/murder/internal/metrics"
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

// Backups that cannot be set up, as with a key file missing, leave the
// agent running: a run on request says why, /metrics says they are not
// ready, and once the file is there they are set up. A missing identity
// skips verifying only; a recipient that is no age key refuses the agent.
func TestBackupsThatCannotBeSetUpWaitForAFix(t *testing.T) {
	dir := t.TempDir()
	agentConfig := func(recipient string) config.Agent {
		return config.Agent{
			Volumes: config.Volumes{Driver: "zfs", Parent: "tank/murder/tenants", MountRoot: filepath.Join(dir, "tenants")},
			Backups: &config.Backups{
				Mode: "test", Endpoint: "https://hel1.your-objectstorage.com", Region: "hel1", Bucket: "b",
				AccessKeyFile: filepath.Join(dir, "s3.access"), SecretKeyFile: filepath.Join(dir, "s3.secret"),
				IdentityFile: filepath.Join(dir, "identity.txt"), Recipient: recipient,
				Every: config.Duration{Duration: time.Hour}, KeepHourly: config.Duration{Duration: 48 * time.Hour},
				KeepDaily: config.Duration{Duration: 30 * 24 * time.Hour}, KeepMonthly: config.Duration{Duration: 365 * 24 * time.Hour},
				VerifyEvery: config.Duration{Duration: 7 * 24 * time.Hour}, KeepLocal: 24,
			},
		}
	}
	discard := slog.New(slog.NewTextHandler(io.Discard, nil))
	if _, err := newAgentBackups(agentConfig("age1notavalidkey"), nil, nil, discard); err == nil {
		t.Fatal("a recipient that is no age key accepted")
	}
	identity, err := age.GenerateX25519Identity()
	if err != nil {
		t.Fatal(err)
	}
	a, err := newAgentBackups(agentConfig(identity.Recipient().String()), nil, nil, discard)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := a.RunNow(context.Background()); !errors.Is(err, fs.ErrNotExist) {
		t.Fatalf("a run with the S3 keys missing: %v", err)
	}
	var page strings.Builder
	a.writeMetrics(metrics.NewWriter(&page))
	if !strings.Contains(page.String(), "murder_backups_ready 0") {
		t.Fatalf("metrics:\n%s", page.String())
	}
	for _, name := range []string{"s3.access", "s3.secret"} {
		if err := os.WriteFile(filepath.Join(dir, name), []byte("key\n"), 0o600); err != nil {
			t.Fatal(err)
		}
	}
	if err := a.ready(); err != nil || a.backups == nil {
		t.Fatalf("set up once the keys are there: %v", err)
	}
	page.Reset()
	a.writeMetrics(metrics.NewWriter(&page))
	if !strings.Contains(page.String(), "murder_backups_ready 1") {
		t.Fatalf("metrics:\n%s", page.String())
	}
	// Verifications wait a run after backups start, and for the identity.
	if time.Until(a.verified.Add(a.verify)) < 50*time.Minute {
		t.Fatalf("the first verification at %v, not a run after backups start", a.verified.Add(a.verify))
	}
	if a.loadIdentity() {
		t.Fatal("a missing identity read")
	}
	if err := os.WriteFile(filepath.Join(dir, "identity.txt"), []byte(identity.String()+"\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	if !a.loadIdentity() {
		t.Fatal("the identity, once there, not read")
	}
}
