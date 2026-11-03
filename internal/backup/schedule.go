package backup

import (
	"context"
	"fmt"
	"log/slog"
	"maps"
	"os"
	"slices"
	"sync"
	"time"

	"filippo.io/age"

	"github.com/PowerPenguini/murder/internal/metrics"
	"github.com/PowerPenguini/murder/internal/store"
	"github.com/PowerPenguini/murder/internal/volume"
)

// State is what a Scheduler reads of the desired state.
type State interface {
	Node() string
	List() []store.Record
	Get(name string) (store.Record, error)
	// SnapshotFile copies the state to a file of its own, for a backup.
	SnapshotFile() (string, error)
	// ReplicaStale records that a tenant's copy no longer matches its data.
	ReplicaStale(name, why, actor string) error
}

// Result is how backing up one tenant went.
type Result struct {
	Tenant string
	Key    string
	Size   int64
	Error  string
}

// Replaced is how replacing a tenant's data with a copy went: Aside names
// where its data before went, Error why the replacement failed.
type Replaced struct {
	Aside string
	Error string
}

// SchedulerConfig is what a Scheduler runs with.
type SchedulerConfig struct {
	State State
	// SetUp makes the backups as the config says; the scheduler calls it
	// until it works, and runs without backups meanwhile.
	SetUp func() (*Backups, error)
	// Exclusive runs a function while no reconcile pass, removal or purge
	// runs: the reconciler's Exclusive.
	Exclusive func(func() error) error
	// Leader reports whether this node leads the cluster, which keeps the
	// state's backups; nil means it does (a node alone).
	Leader func() bool
	// Every is how often tenants are backed up, VerifyEvery how often
	// their newest backups are restored aside and compared.
	Every       time.Duration
	VerifyEvery time.Duration
	// IdentityFile, if set, holds the identity verifications decrypt with;
	// read when first needed, and again while it cannot be.
	IdentityFile string
	Log          *slog.Logger
}

// Scheduler backs this node's tenants up every Every, and on request, one
// run at a time: a run on request never doubles one under way, and a
// restore's replacement waits for both.
type Scheduler struct {
	backups   *Backups
	state     State
	data      tenantData
	leader    func() bool
	identity  age.Identity
	every     time.Duration
	verify    time.Duration
	log       *slog.Logger
	exclusive func(func() error) error
	// running is held by a run or a replacement.
	running  chan struct{}
	verified time.Time

	setUp        func() (*Backups, error)
	identityFile string

	mu sync.Mutex
	// up is whether backups are set up.
	up bool
	// last is each tenant's newest backup that worked, and failing whether
	// its last try failed, for /metrics.
	last    map[string]time.Time
	failing map[string]bool
}

// tenantData is what a scheduler needs of the volume driver besides what
// Backups need: whether a tenant has data on this node, and its dataset.
type tenantData interface {
	Exists(ctx context.Context, tenant string) (bool, error)
	Dataset(tenant string) string
}

// The volume driver backups run with gives a scheduler what it needs.
var _ tenantData = volume.ZFS{}

// NewScheduler makes a scheduler; Run runs it.
func NewScheduler(c SchedulerConfig) *Scheduler {
	leader := c.Leader
	if leader == nil {
		leader = func() bool { return true }
	}
	return &Scheduler{
		state: c.State, leader: leader, every: c.Every, verify: c.VerifyEvery, log: c.Log, exclusive: c.Exclusive,
		running: make(chan struct{}, 1), last: map[string]time.Time{}, failing: map[string]bool{},
		identityFile: c.IdentityFile, setUp: c.SetUp,
	}
}

// setupRetry is how often backups that cannot be set up, as with a key
// file missing, are tried again.
const setupRetry = time.Minute

// ready sets backups up, if they are not yet; the caller holds running.
// Until it works, the agent runs, without backups, and says why.
func (a *Scheduler) ready() error {
	if a.backups != nil {
		return nil
	}
	made, err := a.setUp()
	if err != nil {
		return fmt.Errorf("backups are not set up: %w", err)
	}
	data, ok := made.Volumes.(tenantData)
	if !ok {
		return fmt.Errorf("backups are not set up: %w", volume.ErrBackupsUnsupported)
	}
	made.Log = a.log
	a.backups, a.data = made, data
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
func (a *Scheduler) lock(ctx context.Context) error {
	select {
	case a.running <- struct{}{}:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

func (a *Scheduler) unlock() { <-a.running }

// RunNow runs a backup of this node's tenants now.
func (a *Scheduler) RunNow(ctx context.Context) ([]Result, error) {
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
func (a *Scheduler) ReplaceTenant(ctx context.Context, tenant, copy string) (Replaced, error) {
	if err := a.lock(ctx); err != nil {
		return Replaced{}, err
	}
	defer a.unlock()
	if err := a.ready(); err != nil {
		return Replaced{}, err
	}
	var result Replaced
	err := a.exclusive(func() error {
		record, err := a.state.Get(tenant)
		switch {
		case ctx.Err() != nil:
			return fmt.Errorf("the client gave up while waiting; nothing was replaced: %w", ctx.Err())
		case err != nil:
			return err
		case !record.Archived():
			return fmt.Errorf("tenant %s: %w: archive it before its data is replaced", tenant, store.ErrNotArchived)
		case record.Node != a.state.Node() || record.MoveTo != "":
			return fmt.Errorf("tenant %s: %w", tenant, store.ErrElsewhere)
		}
		// The copy on another node holds the data being replaced: no
		// failover may take it, nor a send begun before report it whole.
		if err := a.state.ReplicaStale(tenant, "its data is replaced with backup "+copy, "murder"); err != nil {
			return err
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

// Run backs up until ctx is done: it sets backups up, trying again while
// it cannot, then runs every Every and verifies every VerifyEvery.
func (a *Scheduler) Run(ctx context.Context) {
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
func (a *Scheduler) checkBucket(ctx context.Context) {
	locked, err := a.backups.Store.Locked(ctx)
	switch {
	case err != nil:
		a.log.Error("check the backups bucket", "error", err)
	case a.backups.Policy.Mode == Production && !locked:
		a.log.Error("the backups bucket has no object lock, which production backups need: every backup will fail; run murder backups setup with a new bucket")
	case a.backups.Policy.Mode == Test && locked:
		a.log.Warn("the backups bucket has object lock, but backups are in test mode: they are not locked, and the bucket cannot be destroyed")
	}
}

// own lists the tenants whose backups are this node's: those it runs,
// with data, not moving away and not archived (an archived tenant's data
// stays as its last backups have it).
func (a *Scheduler) own(ctx context.Context) []string {
	var names []string
	for _, record := range a.state.List() {
		if record.Node != a.state.Node() || record.MoveTo != "" || record.Archived() {
			continue
		}
		if exists, err := a.data.Exists(ctx, record.Spec.Name); err != nil || !exists {
			continue
		}
		names = append(names, record.Spec.Name)
	}
	return names
}

func (a *Scheduler) record(tenant string, at time.Time, failed bool) {
	a.mu.Lock()
	defer a.mu.Unlock()
	a.failing[tenant] = failed
	if at.After(a.last[tenant]) {
		a.last[tenant] = at
	}
}

// recent reports whether the tenant's newest backup is younger than a
// run's interval, as after an agent restart, which then makes no other.
func (a *Scheduler) recent(ctx context.Context, tenant string) bool {
	list, err := a.backups.List(ctx, tenant)
	if err != nil || len(list) == 0 {
		return false
	}
	newest := list[len(list)-1].At
	if a.backups.now().Sub(newest) >= a.every-a.every/10 {
		return false
	}
	a.record(tenant, newest, false)
	return true
}

// run backs this node's tenants up: all of them when forced, otherwise
// those without a backup since about a run's interval.
func (a *Scheduler) run(ctx context.Context, force bool) []Result {
	var results []Result
	for _, name := range a.own(ctx) {
		if !force && a.recent(ctx, name) {
			continue
		}
		made, err := a.backups.BackUp(ctx, name, a.data.Dataset(name))
		result := Result{Tenant: name, Key: made.Key, Size: made.Size}
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
		if path, err := a.state.SnapshotFile(); err != nil {
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
func (a *Scheduler) pruneGone(ctx context.Context) {
	names, err := a.backups.Tenants(ctx)
	if err != nil {
		a.log.Warn("list the tenants with backups", "error", err)
		return
	}
	known := map[string]bool{}
	for _, record := range a.state.List() {
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
func (a *Scheduler) loadIdentity() bool {
	if a.identity != nil {
		return true
	}
	identity, err := ReadIdentity(a.identityFile)
	if err != nil {
		a.log.Error("no verification of backups until backups.identityFile is fixed", "error", err)
		return false
	}
	a.identity = identity
	return true
}

func (a *Scheduler) verifyAll(ctx context.Context) {
	for _, name := range a.own(ctx) {
		verified, err := a.backups.Verify(ctx, name, a.data.Dataset(name), a.identity)
		if err != nil {
			a.log.Error("a backup does not restore", "tenant", name, "error", err)
			continue
		}
		a.log.Info("verified a backup", "tenant", name, "at", verified.Backup.At, "files", verified.Files, "compared", verified.Compared)
	}
}

// WriteMetrics writes whether backups are set up, when each tenant was
// last backed up, and whether its last backup failed.
func (a *Scheduler) WriteMetrics(out *metrics.Writer) {
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

// ReadIdentity reads the one age identity in the file at path.
func ReadIdentity(path string) (age.Identity, error) {
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
