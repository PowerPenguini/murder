package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	measure "github.com/PowerPenguini/murder/internal/usage"
	"log/slog"
	"os"
	"os/signal"
	"strings"
	"syscall"
	"time"

	"filippo.io/age"

	"github.com/PowerPenguini/murder/internal/access"
	"github.com/PowerPenguini/murder/internal/api"
	"github.com/PowerPenguini/murder/internal/backup"
	"github.com/PowerPenguini/murder/internal/config"
	"github.com/PowerPenguini/murder/internal/volume"
)

const backupsUsage = `Usage: murder backups <command> [arguments]

  setup                       make the bucket (production: with object lock),
                              or check that it fits the mode
  now                         back this node's tenants up now
  list TENANT                 the tenant's backups
  verify TENANT -identity F [-offline]
                              read the newest backup through and compare
                              every file with the snapshot it was made of
  restore TENANT -at TIME -identity F
                              put an archived tenant's data back as it was at
                              TIME (RFC 3339); what it had is kept aside
  state [-at TIME] -identity F -out FILE [-offline]
                              write a copy of the cluster's state
  forget TENANT -yes          delete every backup of a purged tenant (in
                              production, those locked stay until their
                              locks end)

verify, restore, state and forget take -reason TEXT and -ticket REF: each is
recorded in the access register through the agent before a backup is read
("murder access" says more). With -offline, verify and state go ahead when
the agent cannot record them, such as in a disaster recovery: the access is
written to this host's offline file, for "murder access sync" later.
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
	reason := addReasonFlags(flags, "verify, restore, state and forget")
	offline := flags.Bool("offline", false, "with verify and state: if the agent cannot record the access, write it to the offline file")
	offlineFile := flags.String("offline-file", access.OfflineFile, "the offline file of accesses not recorded yet")
	positional, err := parseInterspersed(flags, args)
	if err != nil {
		return err
	}
	if *offline && command != "verify" && command != "state" {
		return fmt.Errorf("backups %s needs the agent: -offline is for verify and state", command)
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
	// Reading a backup decrypts a tenant's data: recorded in the access
	// register first, before anything is decrypted.
	record := func(kind access.Kind, tenant string) (access.Entry, error) {
		request, err := reason.request(kind, tenant, "")
		if err != nil {
			return access.Entry{}, err
		}
		return recordAccess(ctx, api.NewClient(cfg.Socket), request, *offline, *offlineFile)
	}
	if command == "now" {
		results, err := api.NewClient(cfg.Socket).RunBackups(ctx)
		if err != nil {
			return err
		}
		failed := 0
		for _, result := range results {
			if result.Error != "" {
				failed++
				fmt.Fprintf(stdout, "%s: %s\n", result.Label(), result.Error)
			} else {
				fmt.Fprintf(stdout, "%s: %s (%.1f MB)\n", result.Label(), result.Key, float64(result.Size)/1e6)
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
		return backup.ReadIdentity(*identityPath)
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
		if _, err := record(access.BackupVerify, name); err != nil {
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
		// Only an archived tenant of this node: nothing runs on its data,
		// and the agent's backups leave it alone. The agent checks again
		// when it puts the backup in place.
		client := asOperator(api.NewClient(cfg.Socket))
		tenantRecord, err := client.Get(ctx, name)
		if err != nil {
			return err
		}
		if !tenantRecord.Archived() || tenantRecord.Elsewhere {
			return fmt.Errorf("tenant %s must be archived, on this node: murder tenant archive %s, on node %s", name, name, tenantRecord.Node)
		}
		key, err := identity()
		if err != nil {
			return err
		}
		entry, err := record(access.BackupRestore, name)
		if err != nil {
			return err
		}
		// The download, into a copy apart from the tenant's data, holds
		// nothing up; the agent then swaps it in, between its own work.
		restored, copy, err := backups.Fill(ctx, name, when, key)
		if err != nil {
			return err
		}
		// A download of many hours may outlast its entry: one more is
		// recorded, rather than the download lost.
		if time.Since(entry.At) > access.RestoreGrantFor-time.Hour {
			if entry, err = record(access.BackupRestore, name); err != nil {
				if discard := backups.DiscardCopy(context.WithoutCancel(ctx), copy); discard != nil {
					return fmt.Errorf("%w; remove copy %s too: %w", err, copy, discard)
				}
				return err
			}
		}
		replaced, err := client.ReplaceTenant(ctx, name, copy, entry.ID)
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
		if _, err := record(access.State, ""); err != nil {
			file.Close()
			os.Remove(*out)
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
		if _, err := record(access.BackupForget, name); err != nil {
			return err
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

// agentBackups serves the agent's backup scheduler to its API.
type agentBackups struct{ *backup.Scheduler }

// newAgentBackups refuses a config that is wrong, such as a recipient that
// is no age key: only files that may be missing or unreadable for a while,
// the S3 keys and the identity, wait for a fix. leader is nil on a node
// alone, hold on a node that sends no copies.
func newAgentBackups(cfg config.Agent, st backup.State, exclusive func(func() error) error, leader func() bool, hold func(context.Context, string) (func(), error), log *slog.Logger) (*agentBackups, error) {
	if _, err := age.ParseX25519Recipient(cfg.Backups.Recipient); err != nil {
		return nil, fmt.Errorf("backups.recipient: %w", err)
	}
	return &agentBackups{backup.NewScheduler(backup.SchedulerConfig{
		State: st, Exclusive: exclusive, Leader: leader, Hold: hold, Log: log,
		Every: cfg.Backups.Every.Duration, VerifyEvery: cfg.Backups.VerifyEvery.Duration,
		IdentityFile: cfg.Backups.IdentityFile,
		SetUp: func() (*backup.Backups, error) {
			made, _, err := newBackups(cfg)
			return made, err
		},
	})}, nil
}

// usage is how the node's backups go, for its use reports, which the
// panel reads.
func (a *agentBackups) useReport() *measure.Backups {
	status := a.Scheduler.Status()
	out := &measure.Backups{Ready: status.Ready, Every: int(status.Every / time.Second), Tenants: map[string]measure.Backup{}}
	for name, failing := range status.Failing {
		out.Tenants[name] = measure.Backup{Last: status.Last[name], Failing: failing, Error: status.Why[name]}
	}
	for name, last := range status.Last {
		if _, ok := out.Tenants[name]; !ok {
			out.Tenants[name] = measure.Backup{Last: last}
		}
	}
	if status.StateTried {
		out.State = &measure.Backup{Last: status.StateLast, Failing: status.StateFailing}
	}
	return out
}

func (a *agentBackups) RunNow(ctx context.Context) ([]api.BackupResult, error) {
	results, err := a.Scheduler.RunNow(ctx)
	if err != nil {
		return nil, err
	}
	var answers []api.BackupResult
	for _, r := range results {
		answers = append(answers, api.BackupResult{Tenant: r.Tenant, State: r.State, Key: r.Key, Size: r.Size, Error: r.Error})
	}
	return answers, nil
}

func (a *agentBackups) ReplaceTenant(ctx context.Context, tenant, copy string) (api.ReplaceResult, error) {
	replaced, err := a.Scheduler.ReplaceTenant(ctx, tenant, copy)
	return api.ReplaceResult{Aside: replaced.Aside, Error: replaced.Error}, err
}
