package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"log/slog"
	"os"
	"strings"
	"sync"
	"time"

	"filippo.io/age"

	"github.com/PowerPenguini/murder/internal/api"
	"github.com/PowerPenguini/murder/internal/backup"
	"github.com/PowerPenguini/murder/internal/config"
	"github.com/PowerPenguini/murder/internal/store"
	"github.com/PowerPenguini/murder/internal/volume"
)

const backupsUsage = `Usage: murder backups <command> [arguments]

  setup                       make the bucket (production: with object lock),
                              or check that it fits the mode
  now                         back this node's tenants up now
  list TENANT                 the tenant's backups
  verify TENANT -identity F   read the newest backup through and compare
                              every file with the snapshot it was made of
  restore TENANT -at TIME -identity F
                              put an archived tenant's data back as it was at
                              TIME (RFC 3339); what it had is kept aside
  state [-at TIME] -identity F -out FILE
                              write a copy of the cluster's state
  destroy -yes                delete every backup and the bucket (test mode
                              only: production backups are locked)

Each backup is the whole data of a tenant (database, receipts, everything)
from a snapshot: tar, compressed with zstd, encrypted with age for the
recipient in the config. F holds the matching identity, kept offline. One
backup restores alone, also without Murder:
  age -d -i F < backup.tar.zst.age | zstd -d | tar x
`

func runBackups(args []string) error {
	if len(args) == 0 || args[0] == "-h" || args[0] == "help" {
		fmt.Fprint(os.Stderr, backupsUsage)
		return flag.ErrHelp
	}
	command, args := args[0], args[1:]
	flags := flag.NewFlagSet("backups "+command, flag.ContinueOnError)
	configPath := flags.String("config", defaultConfig, "agent configuration file")
	identityPath := flags.String("identity", "", "age identity file that decrypts backups")
	at := flags.String("at", "", "restore as of this time (RFC 3339)")
	out := flags.String("out", "", "with state: write the copy here")
	yes := flags.Bool("yes", false, "with destroy: really delete everything")
	positional, err := parseInterspersed(flags, args)
	if err != nil {
		return err
	}
	cfg, err := config.Load(*configPath)
	if err != nil {
		return err
	}
	if cfg.Backups == nil {
		return errors.New("this node's config has no backups: section")
	}
	ctx := context.Background()
	if command == "now" {
		results, err := api.NewClient(cfg.Socket).RunBackups(ctx)
		for _, result := range results {
			if result.Error != "" {
				fmt.Fprintf(stdout, "%s: %s\n", result.Tenant, result.Error)
			} else {
				fmt.Fprintf(stdout, "%s: %s (%.1f MB)\n", result.Tenant, result.Key, float64(result.Size)/1e6)
			}
		}
		return err
	}
	backups, s3, err := newBackups(cfg)
	if err != nil {
		return err
	}
	identity := func() (age.Identity, error) {
		if *identityPath == "" {
			return nil, errors.New("decrypting backups needs -identity, the age key backups are encrypted for")
		}
		return readIdentity(*identityPath)
	}
	tenant := func() (string, error) {
		if len(positional) != 1 {
			return "", fmt.Errorf("backups %s needs a tenant", command)
		}
		return positional[0], nil
	}
	zfs, _ := backups.Volumes.(volume.ZFS)
	switch command {
	case "setup":
		return setupBucket(ctx, s3, backup.Mode(cfg.Backups.Mode))
	case "list":
		name, err := tenant()
		if err != nil {
			return err
		}
		list, err := backups.List(ctx, name)
		if err != nil {
			return err
		}
		for _, b := range list {
			fmt.Fprintf(stdout, "%s  %8.1f MB  %s\n", b.At.Format("2006-01-02T15:04:05.000Z07:00"), float64(b.Size)/1e6, b.Key)
		}
		return nil
	case "verify":
		name, err := tenant()
		if err != nil {
			return err
		}
		key, err := identity()
		if err != nil {
			return err
		}
		verified, err := backups.Verify(ctx, name, zfs.Dataset(name), key)
		if err != nil {
			return err
		}
		fmt.Fprintf(stdout, "%s: the backup of %s reads through: %d files, %.1f MB", name, verified.Backup.At.Format(time.RFC3339), verified.Files, float64(verified.Bytes)/1e6)
		if verified.Compared {
			fmt.Fprintln(stdout, ", each the same as in the snapshot it was made of")
		} else {
			fmt.Fprintln(stdout, "; its snapshot is no longer here to compare with")
		}
		return nil
	case "restore":
		name, err := tenant()
		if err != nil {
			return err
		}
		when := time.Now()
		if *at != "" {
			if when, err = time.Parse(time.RFC3339, *at); err != nil {
				return fmt.Errorf("-at: %w", err)
			}
		}
		key, err := identity()
		if err != nil {
			return err
		}
		// Only an archived tenant of this node: nothing runs on its data,
		// and the agent's backups leave it alone.
		record, err := api.NewClient(cfg.Socket).Get(ctx, name)
		if err != nil {
			return err
		}
		if !record.Archived() || record.Elsewhere {
			return fmt.Errorf("tenant %s must be archived, on this node: murder tenant archive %s, on node %s", name, name, record.Node)
		}
		restored, aside, err := backups.Restore(ctx, name, when, key)
		if err != nil {
			return err
		}
		fmt.Fprintf(stdout, "%s: data as of %s put back", name, restored.At.Format(time.RFC3339))
		if aside != "" {
			fmt.Fprintf(stdout, "; what it had is %s", aside)
		}
		fmt.Fprintf(stdout, "\nnow: murder tenant restore %s\n", name)
		return nil
	case "state":
		if *out == "" {
			return errors.New("state needs -out FILE")
		}
		key, err := identity()
		if err != nil {
			return err
		}
		states, err := backups.States(ctx)
		if err != nil {
			return err
		}
		chosen := ""
		for _, state := range states {
			if *at == "" || state.At.Format(time.RFC3339) <= stampOf(*at) {
				chosen = state.Key
			}
		}
		if chosen == "" {
			return errors.New("no copy of the state there")
		}
		file, err := os.OpenFile(*out, os.O_WRONLY|os.O_CREATE|os.O_EXCL, 0o600)
		if err != nil {
			return err
		}
		if err := backups.GetState(ctx, chosen, key, file); err != nil {
			file.Close()
			os.Remove(*out)
			return err
		}
		fmt.Fprintf(stdout, "wrote %s (%s); restore it with murder restore\n", *out, chosen)
		return file.Close()
	case "destroy":
		if backup.Mode(cfg.Backups.Mode) != backup.Test {
			return errors.New("production backups are locked: only a test bucket is destroyed")
		}
		if locked, err := s3.Locked(ctx); err != nil {
			return err
		} else if locked {
			return errors.New("this bucket has object lock: its backups cannot be deleted before their locks end")
		}
		if !*yes {
			return errors.New("destroy deletes every backup and the bucket: add -yes")
		}
		if err := s3.Destroy(ctx); err != nil {
			return err
		}
		fmt.Fprintf(stdout, "deleted every backup and bucket %s\n", cfg.Backups.Bucket)
		return nil
	default:
		fmt.Fprint(os.Stderr, backupsUsage)
		return flag.ErrHelp
	}
}

// stampOf normalizes an RFC 3339 time to UTC, as backups' times are.
func stampOf(value string) string {
	at, err := time.Parse(time.RFC3339, value)
	if err != nil {
		return value
	}
	return at.UTC().Format(time.RFC3339)
}

// setupBucket makes the bucket for mode, or checks the one there: a
// production bucket needs object lock, which is given only when a bucket is
// made; a test bucket must not have it, or it could not be destroyed.
func setupBucket(ctx context.Context, s3 *backup.S3, mode backup.Mode) error {
	exists, err := s3.Exists(ctx)
	if err != nil {
		return err
	}
	if !exists {
		if err := s3.Create(ctx, mode == backup.Production); err != nil {
			return err
		}
	}
	locked, err := s3.Locked(ctx)
	if err != nil {
		return err
	}
	switch {
	case mode == backup.Production && !locked:
		return errors.New("the bucket has no object lock, which production backups need and only a new bucket gets: use another bucket")
	case mode == backup.Test && locked:
		return errors.New("the bucket has object lock: a test bucket must not, so it can be destroyed; use another bucket")
	}
	state := "made"
	if exists {
		state = "fits"
	}
	fmt.Fprintf(stdout, "bucket %s %s (%s mode)\n", map[bool]string{true: "with object lock", false: "without lock"}[locked], state, mode)
	return nil
}

func readIdentity(path string) (age.Identity, error) {
	file, err := os.Open(path)
	if err != nil {
		return nil, err
	}
	defer file.Close()
	identities, err := age.ParseIdentities(file)
	if err != nil {
		return nil, err
	}
	if len(identities) != 1 {
		return nil, fmt.Errorf("%s: one identity, not %d", path, len(identities))
	}
	return identities[0], nil
}

func readSecret(path string) (string, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return "", err
	}
	return strings.TrimSpace(string(data)), nil
}

// newBackups sets backups up as the config says.
func newBackups(cfg config.Agent) (*backup.Backups, *backup.S3, error) {
	b := cfg.Backups
	access, err := readSecret(b.AccessKeyFile)
	if err != nil {
		return nil, nil, err
	}
	secret, err := readSecret(b.SecretKeyFile)
	if err != nil {
		return nil, nil, err
	}
	s3, err := backup.NewS3(b.Endpoint, b.Region, b.Bucket, access, secret)
	if err != nil {
		return nil, nil, err
	}
	recipient, err := age.ParseX25519Recipient(b.Recipient)
	if err != nil {
		return nil, nil, fmt.Errorf("backups.recipient: %w", err)
	}
	volumes, err := newVolumes(cfg.Volumes)
	if err != nil {
		return nil, nil, err
	}
	zfs, ok := volumes.(volume.ZFS)
	if !ok {
		return nil, nil, volume.ErrBackupsUnsupported
	}
	policy := backup.Policy{
		Mode: backup.Mode(b.Mode), KeepHourly: b.KeepHourly.Duration, KeepDaily: b.KeepDaily.Duration,
		KeepMonthly: b.KeepMonthly.Duration, LockFor: b.LockFor.Duration, KeepLocal: b.KeepLocal,
	}
	if err := policy.Validate(); err != nil {
		return nil, nil, err
	}
	return &backup.Backups{Store: s3, Volumes: zfs, Recipient: recipient, Policy: policy}, s3, nil
}

// agentBackups backs this node's tenants up every Every, and on request,
// one run at a time: two runs side by side could both send changes since
// the same snapshot and fork a chain.
type agentBackups struct {
	backups  *backup.Backups
	store    *store.SQLite
	zfs      volume.ZFS
	leader   func() bool
	identity age.Identity
	every    time.Duration
	verify   time.Duration
	log      *slog.Logger
	running  sync.Mutex
	verified time.Time
}

// RunNow runs a backup of this node's tenants now.
func (a *agentBackups) RunNow(ctx context.Context) ([]api.BackupResult, error) {
	a.running.Lock()
	defer a.running.Unlock()
	return a.run(ctx), nil
}

func (a *agentBackups) loop(ctx context.Context) {
	for {
		a.running.Lock()
		a.run(ctx)
		if a.identity != nil && time.Since(a.verified) >= a.verify {
			a.verifyAll(ctx)
			a.verified = time.Now()
		}
		a.running.Unlock()
		select {
		case <-ctx.Done():
			return
		case <-time.After(a.every):
		}
	}
}

// own lists the tenants whose backups are this node's: those it runs,
// with data, not moving away and not archived (an archived tenant's data
// stays as its last backups have it).
func (a *agentBackups) own(ctx context.Context) []string {
	var names []string
	for _, record := range a.store.List() {
		if record.Node != a.store.Node() || record.MoveTo != "" || record.Archived() {
			continue
		}
		if exists, err := a.zfs.Exists(ctx, record.Spec.Name); err != nil || !exists {
			continue
		}
		names = append(names, record.Spec.Name)
	}
	return names
}

func (a *agentBackups) run(ctx context.Context) []api.BackupResult {
	var results []api.BackupResult
	for _, name := range a.own(ctx) {
		made, err := a.backups.BackUp(ctx, name, a.zfs.Dataset(name))
		result := api.BackupResult{Tenant: name, Key: made.Key, Size: made.Size}
		if err != nil {
			result.Error = err.Error()
			a.log.Error("back a tenant up", "tenant", name, "error", err)
		} else {
			a.log.Info("backed a tenant up", "tenant", name, "key", made.Key, "bytes", made.Size)
		}
		results = append(results, result)
		if deleted, err := a.backups.Prune(ctx, name); err != nil {
			a.log.Warn("delete old backups", "tenant", name, "error", err)
		} else if deleted > 0 {
			a.log.Info("deleted old backups", "tenant", name, "deleted", deleted)
		}
	}
	// The state is the cluster's: its leader keeps it.
	if a.leader() {
		if path, err := a.store.SnapshotFile(); err != nil {
			a.log.Error("copy the state for a backup", "error", err)
		} else {
			if made, err := a.backups.BackUpState(ctx, path); err != nil {
				a.log.Error("back the state up", "error", err)
			} else {
				a.log.Info("backed the state up", "key", made.Key)
			}
			os.Remove(path)
			if _, err := a.backups.PruneState(ctx); err != nil {
				a.log.Warn("delete old copies of the state", "error", err)
			}
		}
	}
	return results
}

func (a *agentBackups) verifyAll(ctx context.Context) {
	for _, name := range a.own(ctx) {
		verified, err := a.backups.Verify(ctx, name, a.zfs.Dataset(name), a.identity)
		if err != nil {
			a.log.Error("a backup does not restore", "tenant", name, "error", err)
			continue
		}
		a.log.Info("verified a backup", "tenant", name, "at", verified.Backup.At, "files", verified.Files, "compared", verified.Compared)
	}
}
