package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"log/slog"
	"maps"
	"os"
	"os/signal"
	"slices"
	"strings"
	"sync"
	"syscall"
	"time"

	"filippo.io/age"

	"github.com/PowerPenguini/murder/internal/api"
	"github.com/PowerPenguini/murder/internal/backup"
	"github.com/PowerPenguini/murder/internal/config"
	"github.com/PowerPenguini/murder/internal/metrics"
	"github.com/PowerPenguini/murder/internal/reconcile"
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
  forget TENANT -yes          delete every backup of a purged tenant (in
                              production, those locked stay until their
                              locks end)
  destroy -yes                delete every backup and the bucket (test mode
                              only: production backups are locked)

Each backup is the whole data of a tenant (database, receipts, everything)
from a snapshot: tar, compressed with zstd, encrypted with age for the
recipient in the config. F holds the matching identity, kept offline. One
backup restores alone, also without Murder:
  age -d -i F < backup.tar.zst.age | zstd -d | tar x

One bucket serves one cluster (or one node without a cluster): tenants'
backups are kept by name.
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
	yes := flags.Bool("yes", false, "with destroy or forget: really delete")
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
	// Ctrl-C, or a lost SSH session, stops a restore or verification, which
	// then removes its copy.
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM, syscall.SIGHUP)
	defer stop()
	if command == "now" {
		results, err := api.NewClient(cfg.Socket).RunBackups(ctx)
		if err != nil {
			return err
		}
		failed := 0
		for _, result := range results {
			if result.Error != "" {
				failed++
				fmt.Fprintf(stdout, "%s: %s\n", result.Tenant, result.Error)
			} else {
				fmt.Fprintf(stdout, "%s: %s (%.1f MB)\n", result.Tenant, result.Key, float64(result.Size)/1e6)
			}
		}
		if failed > 0 {
			return fmt.Errorf("%d of %d backups failed", failed, len(results))
		}
		return nil
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
		// and the agent's backups leave it alone. The agent checks again
		// when it puts the backup in place.
		client := api.NewClient(cfg.Socket)
		record, err := client.Get(ctx, name)
		if err != nil {
			return err
		}
		if !record.Archived() || record.Elsewhere {
			return fmt.Errorf("tenant %s must be archived, on this node: murder tenant archive %s, on node %s", name, name, record.Node)
		}
		// The download, into a copy apart from the tenant's data, holds
		// nothing up; the agent then swaps it in, between its own work.
		restored, copy, err := backups.Fill(ctx, name, when, key)
		if err != nil {
			return err
		}
		replaced, err := client.ReplaceTenant(ctx, name, copy)
		if err == nil && replaced.Error != "" {
			err = errors.New(replaced.Error)
		}
		if err != nil {
			if replaced.Aside != "" {
				return fmt.Errorf("%w\ntenant %s's data is in %s: rename it back to %s before the tenant runs again", err, name, replaced.Aside, zfs.Dataset(name))
			}
			if discard := backups.DiscardCopy(context.WithoutCancel(ctx), copy); discard != nil {
				return fmt.Errorf("%w; remove copy %s too: %w", err, copy, discard)
			}
			return err
		}
		fmt.Fprintf(stdout, "%s: data as of %s put back", name, restored.At.Format(time.RFC3339))
		if replaced.Aside != "" {
			fmt.Fprintf(stdout, "; what it had is %s", replaced.Aside)
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
		when := time.Now()
		if *at != "" {
			if when, err = time.Parse(time.RFC3339, *at); err != nil {
				return fmt.Errorf("-at: %w", err)
			}
		}
		state, err := backups.StateAt(ctx, when)
		if err != nil {
			return err
		}
		chosen := state.Key
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
	case "forget":
		name, err := tenant()
		if err != nil {
			return err
		}
		if _, err := api.NewClient(cfg.Socket).Get(ctx, name); err == nil {
			return fmt.Errorf("tenant %s is still in the cluster (archived, perhaps): purge it first", name)
		} else if !api.IsNotFound(err) {
			return err
		}
		if !*yes {
			return fmt.Errorf("forget deletes every backup of %s: add -yes", name)
		}
		deleted, err := backups.Forget(ctx, name)
		fmt.Fprintf(stdout, "deleted %d backups of %s\n", deleted, name)
		return err
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
// one run at a time: a run on request never doubles one under way, and a
// restore's replacement waits for both.
type agentBackups struct {
	backups  *backup.Backups
	store    *store.SQLite
	zfs      volume.ZFS
	leader   func() bool
	identity age.Identity
	every    time.Duration
	verify   time.Duration
	log      *slog.Logger
	// exclusive runs a function while no reconcile pass, removal or purge
	// runs: the reconciler's Exclusive.
	exclusive func(func() error) error
	// running is held by a run or a replacement.
	running  chan struct{}
	verified time.Time

	// setUp makes the backups as the config says; ready calls it until
	// it works.
	setUp func() (*backup.Backups, error)
	// identityFile, if set, holds the identity verifications decrypt with;
	// read when first needed, and again while it cannot be.
	identityFile string

	mu sync.Mutex
	// up is whether backups are set up.
	up bool
	// last is each tenant's newest backup that worked, and failing whether
	// its last try failed, for /metrics.
	last    map[string]time.Time
	failing map[string]bool
}

// newAgentBackups refuses a config that is wrong, such as a recipient that
// is no age key: only files that may be missing or unreadable for a while,
// the S3 keys and the identity, wait for a fix.
func newAgentBackups(cfg config.Agent, st *store.SQLite, exclusive func(func() error) error, log *slog.Logger) (*agentBackups, error) {
	if _, err := age.ParseX25519Recipient(cfg.Backups.Recipient); err != nil {
		return nil, fmt.Errorf("backups.recipient: %w", err)
	}
	return &agentBackups{
		store: st, leader: func() bool { return true },
		every: cfg.Backups.Every.Duration, verify: cfg.Backups.VerifyEvery.Duration, log: log, exclusive: exclusive,
		running: make(chan struct{}, 1), last: map[string]time.Time{}, failing: map[string]bool{},
		identityFile: cfg.Backups.IdentityFile,
		setUp: func() (*backup.Backups, error) {
			made, _, err := newBackups(cfg)
			return made, err
		},
	}, nil
}

// setupRetry is how often backups that cannot be set up, as with a key
// file missing, are tried again.
const setupRetry = time.Minute

// ready sets backups up, if they are not yet; the caller holds running.
// Until it works, the agent runs, without backups, and says why.
func (a *agentBackups) ready() error {
	if a.backups != nil {
		return nil
	}
	made, err := a.setUp()
	if err != nil {
		return fmt.Errorf("backups are not set up: %w", err)
	}
	made.Log = a.log
	a.backups, a.zfs = made, made.Volumes.(volume.ZFS)
	// The first verification comes a run after backups start, not with
	// them: an agent restarted often still verifies, without a burst at
	// each start.
	a.verified = time.Now().Add(a.every - a.verify)
	a.mu.Lock()
	a.up = true
	a.mu.Unlock()
	return nil
}

// lock waits for no run or replacement to be under way, or for ctx.
func (a *agentBackups) lock(ctx context.Context) error {
	select {
	case a.running <- struct{}{}:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

func (a *agentBackups) unlock() { <-a.running }

// RunNow runs a backup of this node's tenants now.
func (a *agentBackups) RunNow(ctx context.Context) ([]api.BackupResult, error) {
	if err := a.lock(ctx); err != nil {
		return nil, err
	}
	defer a.unlock()
	if err := a.ready(); err != nil {
		return nil, err
	}
	return a.run(ctx, true), nil
}

// ReplaceTenant makes copy the tenant's data, once it is checked again,
// with no backup run, reconcile pass or purge beside it, that the tenant
// is archived, of this node and not moving: nothing may run on the data,
// back it up or remove it meanwhile.
func (a *agentBackups) ReplaceTenant(ctx context.Context, tenant, copy string) (api.ReplaceResult, error) {
	if err := a.lock(ctx); err != nil {
		return api.ReplaceResult{}, err
	}
	defer a.unlock()
	if err := a.ready(); err != nil {
		return api.ReplaceResult{}, err
	}
	var result api.ReplaceResult
	err := a.exclusive(func() error {
		record, err := a.store.Get(tenant)
		switch {
		case ctx.Err() != nil:
			return fmt.Errorf("the client gave up while waiting; nothing was replaced: %w", ctx.Err())
		case err != nil:
			return err
		case !record.Archived():
			return fmt.Errorf("tenant %s: %w: archive it before its data is replaced", tenant, store.ErrNotArchived)
		case record.Node != a.store.Node() || record.MoveTo != "":
			return fmt.Errorf("tenant %s: %w", tenant, reconcile.ErrElsewhere)
		}
		// Begun, the replacement ends, whether the client waits or not.
		aside, err := a.backups.ReplaceCopy(context.WithoutCancel(ctx), tenant, copy)
		result.Aside = aside
		if err != nil {
			result.Error = err.Error()
			a.log.Error("replace a tenant's data with a backup", "tenant", tenant, "copy", copy, "aside", aside, "error", err)
		} else {
			a.log.Info("replaced a tenant's data with a backup", "tenant", tenant, "copy", copy, "aside", aside)
		}
		return nil
	})
	return result, err
}

func (a *agentBackups) loop(ctx context.Context) {
	for {
		if a.lock(ctx) != nil {
			return
		}
		err := a.ready()
		a.unlock()
		if err == nil {
			break
		}
		a.log.Error("no backups until this is fixed; trying again in a minute", "error", err)
		select {
		case <-ctx.Done():
			return
		case <-time.After(setupRetry):
		}
	}
	// Set up, which nothing undoes: the bucket is read without holding
	// runs and replacements up.
	a.checkBucket(ctx)
	for {
		if a.lock(ctx) != nil {
			return
		}
		a.run(ctx, false)
		// An identity that cannot be read is tried again next run.
		if a.identityFile != "" && time.Since(a.verified) >= a.verify && a.loadIdentity() {
			a.verifyAll(ctx)
			a.verified = time.Now()
		}
		a.unlock()
		select {
		case <-ctx.Done():
			return
		case <-time.After(a.every):
		}
	}
}

// checkBucket says at once, rather than at every backup's lock, that a
// production bucket has no object lock: murder backups setup was skipped.
func (a *agentBackups) checkBucket(ctx context.Context) {
	locked, err := a.backups.Store.Locked(ctx)
	switch {
	case err != nil:
		a.log.Error("check the backups bucket", "error", err)
	case a.backups.Policy.Mode == backup.Production && !locked:
		a.log.Error("the backups bucket has no object lock, which production backups need: every backup will fail; run murder backups setup with a new bucket")
	case a.backups.Policy.Mode == backup.Test && locked:
		a.log.Warn("the backups bucket has object lock, but backups are in test mode: they are not locked, and the bucket cannot be destroyed")
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

func (a *agentBackups) record(tenant string, at time.Time, failed bool) {
	a.mu.Lock()
	defer a.mu.Unlock()
	a.failing[tenant] = failed
	if at.After(a.last[tenant]) {
		a.last[tenant] = at
	}
}

// recent reports whether the tenant's newest backup is younger than a
// run's interval, as after an agent restart, which then makes no other.
func (a *agentBackups) recent(ctx context.Context, tenant string) bool {
	list, err := a.backups.List(ctx, tenant)
	if err != nil || len(list) == 0 {
		return false
	}
	newest := list[len(list)-1].At
	if time.Since(newest) >= a.every-a.every/10 {
		return false
	}
	a.record(tenant, newest, false)
	return true
}

// run backs this node's tenants up: all of them when forced, otherwise
// those without a backup since about a run's interval.
func (a *agentBackups) run(ctx context.Context, force bool) []api.BackupResult {
	var results []api.BackupResult
	for _, name := range a.own(ctx) {
		if !force && a.recent(ctx, name) {
			continue
		}
		made, err := a.backups.BackUp(ctx, name, a.zfs.Dataset(name))
		result := api.BackupResult{Tenant: name, Key: made.Key, Size: made.Size}
		a.record(name, made.At, err != nil)
		if err != nil {
			result.Error = err.Error()
			a.log.Error("back a tenant up", "tenant", name, "error", err)
		} else {
			a.log.Info("backed a tenant up", "tenant", name, "key", made.Key, "bytes", made.Size)
		}
		results = append(results, result)
		// Old backups go only once a new one is there.
		if err != nil {
			continue
		}
		if deleted, err := a.backups.Prune(ctx, name); err != nil {
			a.log.Warn("delete old backups", "tenant", name, "error", err)
		} else if deleted > 0 {
			a.log.Info("deleted old backups", "tenant", name, "deleted", deleted)
		}
	}
	// The state is the cluster's: its leader keeps it, and thins out the
	// backups of tenants no longer in it.
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
		a.pruneGone(ctx)
	}
	return results
}

// pruneGone thins out the backups of purged tenants as any others, down
// to the newest, which stays until murder backups forget.
func (a *agentBackups) pruneGone(ctx context.Context) {
	names, err := a.backups.Tenants(ctx)
	if err != nil {
		a.log.Warn("list the tenants with backups", "error", err)
		return
	}
	known := map[string]bool{}
	for _, record := range a.store.List() {
		known[record.Spec.Name] = true
	}
	for _, name := range names {
		if known[name] {
			continue
		}
		if deleted, err := a.backups.Prune(ctx, name); err != nil {
			a.log.Warn("delete old backups of a purged tenant", "tenant", name, "error", err)
		} else if deleted > 0 {
			a.log.Info("deleted old backups of a purged tenant", "tenant", name, "deleted", deleted)
		}
	}
}

// loadIdentity reads the identity verifications need, if not yet read;
// one that cannot be read skips verifying, not backing up.
func (a *agentBackups) loadIdentity() bool {
	if a.identity != nil {
		return true
	}
	identity, err := readIdentity(a.identityFile)
	if err != nil {
		a.log.Error("no verification of backups until backups.identityFile is fixed", "error", err)
		return false
	}
	a.identity = identity
	return true
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

// writeMetrics writes when each tenant was last backed up, and whether its
// last backup failed.
func (a *agentBackups) writeMetrics(out *metrics.Writer) {
	a.mu.Lock()
	defer a.mu.Unlock()
	out.Family("murder_backups_ready", "gauge", "1 once backups are set up; 0 while they cannot be, as with a key file missing.")
	out.Sample("murder_backups_ready", metrics.Bool(a.up))
	names := slices.Sorted(maps.Keys(a.failing))
	out.Family("murder_tenant_backup_last_success_timestamp_seconds", "gauge", "When the tenant's newest backup that worked was made.")
	for _, name := range names {
		if at := a.last[name]; !at.IsZero() {
			out.Sample("murder_tenant_backup_last_success_timestamp_seconds", metrics.Timestamp(at), "tenant", name)
		}
	}
	out.Family("murder_tenant_backup_failing", "gauge", "1 if the tenant's last backup failed.")
	for _, name := range names {
		out.Sample("murder_tenant_backup_failing", metrics.Bool(a.failing[name]), "tenant", name)
	}
}
