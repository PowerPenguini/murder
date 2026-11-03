package main

import (
	"context"
	"errors"
	"io"
	"io/fs"
	"log/slog"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"filippo.io/age"

	"github.com/PowerPenguini/murder/internal/config"
	"github.com/PowerPenguini/murder/internal/metrics"
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

// The agent's backups come from its config: a recipient that is no age key
// refuses the agent, a key file missing leaves it running without backups
// (internal/backup tests how they wait for a fix).
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
	if _, err := newAgentBackups(agentConfig("age1notavalidkey"), nil, nil, nil, nil, discard); err == nil {
		t.Fatal("a recipient that is no age key accepted")
	}
	identity, err := age.GenerateX25519Identity()
	if err != nil {
		t.Fatal(err)
	}
	a, err := newAgentBackups(agentConfig(identity.Recipient().String()), nil, nil, nil, nil, discard)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := a.RunNow(context.Background()); !errors.Is(err, fs.ErrNotExist) {
		t.Fatalf("a run with the S3 keys missing: %v", err)
	}
	var page strings.Builder
	a.WriteMetrics(metrics.NewWriter(&page))
	if !strings.Contains(page.String(), "\nmurder_backups_ready 0\n") {
		t.Fatalf("metrics:\n%s", page.String())
	}
}
