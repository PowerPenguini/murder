package backup

import (
	"context"
	"errors"
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
	// PurgedAt is when a tenant of the name was last purged; the zero time
	// if none was.
	PurgedAt(name string) (time.Time, error)
	// NoteTenant records an event of a tenant's in the audit log.
	NoteTenant(tenant, action, detail, actor string) error
}

// Result is how backing up one tenant, or the cluster's state, went.
type Result struct {
	Tenant string
	// State says the result is the cluster's state's, which the leader
	// backs up; Tenant is then empty.
	State bool
	Key   string
	Size  int64
	Error string
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
	// Hold keeps a tenant's data from being sent to its copies, or
	// reported whole, until release is called, once the sends under way
	// ended: the replicator's Hold. nil if nothing sends (a node alone).
	Hold func(ctx context.Context, tenant string) (release func(), err error)
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
	hold      func(ctx context.Context, tenant string) (func(), error)
	// running is held by a run or a replacement.
	running  chan struct{}
	verified time.Time
	// keptNew is, for each purged tenant whose newest backups are kept as
	// perhaps a new tenant's, the purge that was said, so it is said once.
	keptNew map[string]time.Time

	setUp        func() (*Backups, error)
	identityFile string

	mu sync.Mutex
	// up is whether backups are set up.
	up bool
	// last is each tenant's newest backup that worked, and failing whether
	// its last try failed, for /metrics.
	last    map[string]time.Time
	failing map[string]bool
	// why is why each failing tenant's last backup failed.
	why map[string]string
	// stateTried is whether this node, leading, backed the state up since
	// it last led; stateLast is when that last worked, stateFailing
	// whether its last try failed.
	stateTried   bool
	stateLast    time.Time
	stateFailing bool
}

// tenantData is what a scheduler needs of the volume driver besides what
// Backups need: whether a tenant has data on this node, its dataset, and
// how much it took on since one of its snapshots.
type tenantData interface {
	Exists(ctx context.Context, tenant string) (bool, error)
	Dataset(tenant string) string
	Written(ctx context.Context, tenant, snapshot string) (int64, error)
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
		state: c.State, leader: leader, every: c.Every, verify: c.VerifyEvery, log: c.Log, exclusive: c.Exclusive, hold: c.Hold,
		running: make(chan struct{}, 1), keptNew: map[string]time.Time{}, last: map[string]time.Time{}, failing: map[string]bool{}, why: map[string]string{},
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

// RunNow runs a backup of this node's tenants now, and of the cluster's
// state if this node leads; a backup that failed is a result with an
// Error.
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
	// No send of the tenant runs meanwhile, nor reports a copy whole: one
	// begun before would report the data replaced, which no copy matches
	// once the replacement began. Taken before the reconciler's lock, so
	// the passes do not wait for the sends under way too.
	if a.hold != nil {
		release, err := a.hold(ctx, tenant)
		if err != nil {
			return Replaced{}, fmt.Errorf("the client gave up while waiting for the tenant's sends to its copies; nothing was replaced: %w", err)
		}
		defer release()
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
		// failover may take it until it is sent again, after the hold.
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
// archived or not, with data and not moving away.
func (a *Scheduler) own(ctx context.Context) []store.Record {
	var records []store.Record
	for _, record := range a.state.List() {
		if record.Node != a.state.Node() || record.MoveTo != "" {
			continue
		}
		if exists, err := a.data.Exists(ctx, record.Spec.Name); err != nil || !exists {
			continue
		}
		records = append(records, record)
	}
	return records
}

// captured reports whether the tenant's newest backup holds its data as it
// is: the snapshot it was made of is here, and nothing was written since.
// An archived tenant is backed up until its data, as it was archived, is;
// then no more, as nothing changes it.
func (a *Scheduler) captured(ctx context.Context, tenant string) bool {
	list, err := a.backups.List(ctx, tenant)
	if err != nil || len(list) == 0 {
		return false
	}
	newest := list[len(list)-1]
	written, err := a.data.Written(ctx, tenant, "murder-"+snapshotReason+"-"+newest.At.Format(stamp))
	if err != nil || written != 0 {
		return false
	}
	a.record(tenant, newest.At, false)
	return true
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
// those without a backup since about a run's interval. An archived tenant
// is backed up, forced or not, until its newest backup holds its data as
// it is, and then no more.
func (a *Scheduler) run(ctx context.Context, force bool) []Result {
	var results []Result
	for _, record := range a.own(ctx) {
		name := record.Spec.Name
		if record.Archived() {
			if a.captured(ctx, name) {
				continue
			}
		} else if !force && a.recent(ctx, name) {
			continue
		}
		made, err := a.backups.BackUp(ctx, name, a.data.Dataset(name))
		result := Result{Tenant: name, Key: made.Key, Size: made.Size}
		a.record(name, made.At, err != nil)
		a.mu.Lock()
		if err != nil {
			a.why[name] = err.Error()
		} else {
			delete(a.why, name)
		}
		a.mu.Unlock()
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
	if !a.leader() {
		a.mu.Lock()
		a.stateTried = false
		a.mu.Unlock()
		return results
	}
	results = append(results, a.backUpState(ctx))
	a.pruneGone(ctx)
	return results
}

// backUpState backs the cluster's state up, and thins out its old copies
// once it worked.
func (a *Scheduler) backUpState(ctx context.Context) Result {
	result := Result{State: true}
	made, err := func() (Backup, error) {
		path, err := a.state.SnapshotFile()
		if err != nil {
			return Backup{}, fmt.Errorf("copy the state for a backup: %w", err)
		}
		defer os.Remove(path)
		return a.backups.BackUpState(ctx, path)
	}()
	a.mu.Lock()
	a.stateTried, a.stateFailing = true, err != nil
	if err == nil && made.At.After(a.stateLast) {
		a.stateLast = made.At
	}
	a.mu.Unlock()
	if err != nil {
		result.Error = err.Error()
		a.log.Error("back the state up", "error", err)
		return result
	}
	result.Key, result.Size = made.Key, made.Size
	a.log.Info("backed the state up", "key", made.Key)
	if _, err := a.backups.PruneState(ctx); err != nil {
		a.log.Warn("delete old copies of the state", "error", err)
	}
	return result
}

// pruneGone thins out the backups of tenants no longer in the state as any
// others, down to the newest; and deletes every backup of a purged one,
// the newest too, once the policy's time after its purge has passed.
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
		if a.forgetPurged(ctx, name) {
			continue
		}
		if deleted, err := a.backups.Prune(ctx, name); err != nil {
			a.log.Warn("delete old backups of a purged tenant", "tenant", name, "error", err)
		} else if deleted > 0 {
			a.log.Info("deleted old backups of a purged tenant", "tenant", name, "deleted", deleted)
		}
	}
}

// forgetPurged deletes the backups of a purged tenant, once it was purged
// long enough ago, and records in the audit log what went. It reports
// whether it left nothing for pruneGone to thin out.
//
// Purged means two things at once: no tenant of the name in the state,
// archived or not, and a purge of it recorded. A tenant missing from the
// state with no purge, as after the state was put back from a copy older
// than the tenant, may have no data but its backups: they stay. A name
// can be used again after its purge, and a backup's time is the clock of
// the node that made it, which may run behind: so the state is read again
// just before anything goes, and a backup stamped later than the purge
// less clockSkew may be a new tenant's. It stays, with every backup after
// it, thinned out as any, and said once; murder backups forget deletes
// them. A backup still locked stays, and is tried again next run: its lock
// ends before long, as the newest a purged tenant keeps is locked for
// LockFor at most.
func (a *Scheduler) forgetPurged(ctx context.Context, name string) bool {
	purged, err := a.state.PurgedAt(name)
	if err != nil {
		a.log.Warn("find when a tenant was purged", "tenant", name, "error", err)
		return false
	}
	if purged.IsZero() || a.backups.now().Sub(purged) < a.backups.Policy.forgetPurged() {
		return false
	}
	list, err := a.backups.List(ctx, name)
	if err != nil {
		a.log.Warn("list the backups of a purged tenant", "tenant", name, "error", err)
		return true
	}
	// The first backup that may be of a tenant of the name made since.
	kept := len(list)
	for i, backup := range list {
		if backup.At.After(purged.Add(-clockSkew)) {
			kept = i
			break
		}
	}
	if kept < len(list) && !a.keptNew[name].Equal(purged) {
		a.keptNew[name] = purged
		a.log.Warn("a purged tenant has backups made about or after its purge, which may be of a tenant of the name made since: they are kept, thinned out, and murder backups forget deletes them",
			"tenant", name, "purged", purged, "first", list[kept].Key)
	}
	doomed := list[:kept]
	if len(doomed) == 0 {
		return kept == len(list)
	}
	// Read again just before: a tenant of the name created since the list
	// pruneGone read keeps every backup.
	if _, err := a.state.Get(name); !errors.Is(err, store.ErrNotFound) {
		if err != nil {
			a.log.Warn("check that a purged tenant is still gone", "tenant", name, "error", err)
		}
		return true
	}
	deleted, err := a.backups.Delete(ctx, doomed)
	if deleted > 0 {
		a.log.Info("deleted the backups of a purged tenant", "tenant", name, "purged", purged, "deleted", deleted, "of", len(list))
		detail := fmt.Sprintf("%d of %d backups deleted, the tenant purged %s", deleted, len(list), purged.UTC().Format(time.RFC3339))
		if noted := a.state.NoteTenant(name, forgottenAction, detail, "murder"); noted != nil {
			a.log.Error("record the deletion of a purged tenant's backups in the audit log", "tenant", name, "detail", detail, "error", noted)
		}
	}
	if err != nil {
		a.log.Warn("some backups of a purged tenant are not deleted yet, locked perhaps; trying again next run", "tenant", name, "error", err)
	}
	return kept == len(list)
}

// clockSkew is how far a node's clock may run behind another's: a backup
// stamped within it before a purge may have been made after it.
const clockSkew = 10 * time.Minute

// forgottenAction is the audit log's event of a purged tenant's backups
// deleted.
const forgottenAction = "backup.forgotten"

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
	for _, record := range a.own(ctx) {
		name := record.Spec.Name
		verified, err := a.backups.Verify(ctx, name, a.data.Dataset(name), a.identity)
		if err != nil {
			a.log.Error("a backup does not restore", "tenant", name, "error", err)
			continue
		}
		a.log.Info("verified a backup", "tenant", name, "at", verified.Backup.At, "files", verified.Files, "compared", verified.Compared)
	}
}

// Status is how backups go, as /metrics says it: whether they are set
// up, how often they run, each tenant's newest that worked and whether
// its last failed, and, on the leader, the same of the cluster's state.
type Status struct {
	Ready   bool
	Every   time.Duration
	Last    map[string]time.Time
	Failing map[string]bool
	// Why says why each failing tenant's last backup failed.
	Why          map[string]string
	StateTried   bool
	StateLast    time.Time
	StateFailing bool
}

// Status is how the scheduler's backups go now.
func (a *Scheduler) Status() Status {
	a.mu.Lock()
	defer a.mu.Unlock()
	return Status{Ready: a.up, Every: a.every, Last: maps.Clone(a.last), Failing: maps.Clone(a.failing), Why: maps.Clone(a.why),
		StateTried: a.stateTried, StateLast: a.stateLast, StateFailing: a.stateFailing}
}

// WriteMetrics writes whether backups are set up, when each tenant was
// last backed up, and whether its last backup failed; and, on the leader,
// the same of the cluster's state.
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
	out.Family("murder_state_backup_last_success_timestamp_seconds", "gauge", "When the newest backup of the cluster's state that worked was made; on the leader only.")
	if a.stateTried && !a.stateLast.IsZero() {
		out.Sample("murder_state_backup_last_success_timestamp_seconds", metrics.Timestamp(a.stateLast))
	}
	out.Family("murder_state_backup_failing", "gauge", "1 if the last backup of the cluster's state failed; on the leader only.")
	if a.stateTried {
		out.Sample("murder_state_backup_failing", metrics.Bool(a.stateFailing))
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
