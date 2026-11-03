// Package reconcile makes the container runtime match the stored desired
// state: one container per running tenant, nothing for anyone else.
package reconcile

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"maps"
	"net/http"
	"slices"
	"sync"
	"time"

	"github.com/PowerPenguini/murder/internal/runtime"
	"github.com/PowerPenguini/murder/internal/secrets"
	"github.com/PowerPenguini/murder/internal/spec"
	"github.com/PowerPenguini/murder/internal/store"
	"github.com/PowerPenguini/murder/internal/volume"
)

// snapshotsKept is how many snapshots before image changes a tenant keeps:
// enough to go back a few releases without keeping old data forever.
const snapshotsKept = 5

// Phases a tenant can be in, as seen by the reconciler.
const (
	PhaseRunning   = "running"
	PhaseSuspended = "suspended"
	PhaseFailed    = "failed"
	// PhasePulling waits for the tenant's image; a container already
	// running keeps serving meanwhile.
	PhasePulling = "pulling"
	// PhaseRestarting is a container whose process exited and that is
	// being started again; LastExit says why it exited.
	PhaseRestarting = "restarting"
	// PhaseMoving is a tenant moving to another node: the move, not the
	// reconciler, handles it meanwhile.
	PhaseMoving = "moving"
)

// healthChecks is how many health checks run at once.
const healthChecks = 16

// A tenant that moved here, whose requests wait for it, is checked every
// heldHealthEvery until it is healthy, for heldHealthWait at most in a pass.
const (
	heldHealthEvery = 200 * time.Millisecond
	heldHealthWait  = 10 * time.Second
)

// Status is the observed state of one tenant.
type Status struct {
	Phase   string `json:"phase"`
	Healthy bool   `json:"healthy"`
	Message string `json:"message,omitempty"`
	// Warning reports a problem that does not stop the tenant, such as a
	// disk limit below what it already stores.
	Warning string `json:"warning,omitempty"`
	// Restarts counts how often the container was started again after its
	// app exited; LastExit says when it last exited and, if known, why.
	// Both start again with a new container.
	Restarts    int       `json:"restarts,omitempty"`
	LastExit    string    `json:"lastExit,omitempty"`
	ContainerID string    `json:"containerID,omitempty"`
	Generation  int64     `json:"generation"`
	CheckedAt   time.Time `json:"checkedAt"`
	// ServedGeneration is the generation of the tenant this node saw pass
	// a health check on the revision its rollout moved it to, on its new
	// container: it served, whatever its health since.
	ServedGeneration int64 `json:"servedGeneration,omitempty"`
}

// Config holds the node-wide settings the reconciler applies to every tenant.
type Config struct {
	// BaseDomain and Scheme build each tenant's public host and URL.
	BaseDomain string
	Scheme     string
	// DefaultLimits fill in limits a tenant spec leaves at zero.
	DefaultLimits spec.Limits
	StopTimeout   time.Duration
	HealthTimeout time.Duration
}

// Reconciler drives the runtime towards the store's desired state.
type Reconciler struct {
	store   State
	runtime runtime.Runtime
	volumes volume.Volumes
	key     *secrets.Key
	config  Config
	log     *slog.Logger
	health  *http.Client
	// backend finds where to check a tenant's health: Backend, the same
	// address the container is created with. Tests point it at their own
	// server.
	backend func(store.Record) (string, error)
	pulls   *pulls
	// life ends when Run's context does, and with it the background pulls
	// and every pass, also one started through the API; stop ends it.
	life context.Context
	stop context.CancelFunc

	// pass serializes reconcile passes and the removal of tenants.
	pass sync.Mutex
	// imageRetryAt holds back images the engine kept; only passes use it.
	imageRetryAt map[string]time.Time
	mu           sync.RWMutex
	statuses     map[string]Status
	// served is the generation of each tenant recorded as served on the
	// revision its rollout moved it to, seenServing the one seen passing a
	// health check there, recorded or not; under mu.
	served, seenServing map[string]int64
	// passAt is when the last whole pass ended, passErrors how many errors
	// it had.
	passAt     time.Time
	passErrors int
	// replacing are the tenants whose container is being replaced: until
	// the new one answers, or the pass ends, requests for them wait.
	replacing *replacing
	// replacedWaits are the waits, within a pass, for replaced containers
	// to answer.
	replacedWaits sync.WaitGroup
	// replaceNotice and replacedWait are replaceNotice and heldHealthWait;
	// tests shorten them.
	replaceNotice, replacedWait time.Duration
	// started is when the reconciler began: exits before it are the
	// server's or the engine's doing, or happened while the agent was
	// down, not the app's.
	started time.Time
	// exits counts, per tenant, the app's exits its container was started
	// again after; passes alone use it.
	exits map[string]appExits
}

// New returns a reconciler. All dependencies are required.
func New(st State, rt runtime.Runtime, volumes volume.Volumes, key *secrets.Key, config Config, log *slog.Logger) *Reconciler {
	if config.StopTimeout == 0 {
		config.StopTimeout = 30 * time.Second
	}
	if config.HealthTimeout == 0 {
		config.HealthTimeout = 3 * time.Second
	}
	if config.Scheme == "" {
		config.Scheme = "https"
	}
	life, stop := context.WithCancel(context.Background())
	return &Reconciler{
		store: st, runtime: rt, volumes: volumes, key: key, config: config, log: log,
		health:        &http.Client{Timeout: config.HealthTimeout, CheckRedirect: func(*http.Request, []*http.Request) error { return http.ErrUseLastResponse }},
		statuses:      map[string]Status{},
		imageRetryAt:  map[string]time.Time{},
		replacing:     &replacing{tenants: map[string]bool{}, changed: make(chan struct{})},
		replaceNotice: replaceNotice, replacedWait: heldHealthWait,
		started: time.Now(), exits: map[string]appExits{},
		backend: Backend,
		pulls:   newPulls(life, rt.PullImage),
		life:    life,
		stop:    stop,
	}
}

// Run reconciles every interval, whenever the store changes and whenever a
// pull ends, until ctx ends. Then it stops the pulls and any pass, and
// returns once the tenant step under way, if any, has finished.
func (r *Reconciler) Run(ctx context.Context, interval time.Duration) {
	context.AfterFunc(ctx, r.stop)
	defer func() {
		r.stop()
		// Wait for the tenant step under way in a pass begun through the
		// API; the pass stops after it.
		r.pass.Lock()
		defer r.pass.Unlock()
	}()
	ticker := time.NewTicker(interval)
	defer ticker.Stop()
	for {
		if err := r.Reconcile(ctx); err != nil && ctx.Err() == nil {
			r.log.Error("reconcile", "error", err)
		}
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
		case <-r.store.Changed():
		case <-r.pulls.done:
		}
	}
}

// ReconcileNow runs a pass for "murder reconcile": failed pulls are tried
// again at once, and the pulls the pass starts are waited for, so their
// failures are reported too.
func (r *Reconciler) ReconcileNow(ctx context.Context) error {
	r.pulls.retryNow()
	for {
		before := r.pulls.startedCount()
		err := r.Reconcile(ctx)
		if r.pulls.startedCount() == before || r.life.Err() != nil {
			return err
		}
		// The pass started pulls: wait for them and pass again, to use the
		// images or report their failures.
		if !r.pulls.waitContext(ctx) {
			return errors.Join(err, ctx.Err())
		}
	}
}

// Reconcile performs one full pass. A failing tenant does not stop the pass;
// every failure is returned joined.
func (r *Reconciler) Reconcile(ctx context.Context) error {
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()
	defer context.AfterFunc(r.life, cancel)()
	r.pass.Lock()
	defer r.pass.Unlock()
	// However the pass ends, no request waits for a replacement beyond it.
	defer r.replacing.clear()
	defer r.replacedWaits.Wait()
	if err := r.interrupted(ctx); err != nil {
		return err
	}
	return r.reconcile(ctx)
}

// SetRestartPolicies gives every container of this node's tenants the
// restart policy that has the engine start none by itself
// (runtime.RestartPolicy), whatever state its tenant is in: the agent runs
// it at start, before it may wait for the cluster, and once in a while. A
// container left with an older release's policy would be started at boot
// on data a failover may have made stale.
func (r *Reconciler) SetRestartPolicies(ctx context.Context) error {
	containers, err := r.runtime.List(ctx)
	if err != nil {
		return err
	}
	var errs []error
	for _, container := range containers {
		state, err := r.runtime.Inspect(ctx, container.ID)
		if errors.Is(err, runtime.ErrNotFound) {
			continue
		}
		if err != nil {
			errs = append(errs, err)
			continue
		}
		if state.RestartPolicy != "" && state.RestartPolicy != runtime.RestartPolicy {
			if err := r.runtime.SetRestartPolicy(ctx, container.ID, runtime.RestartPolicy); err != nil {
				errs = append(errs, err)
				continue
			}
			r.log.Info("changed a container's restart policy", "container", container.Name, "policy", runtime.RestartPolicy)
		}
	}
	return errors.Join(errs...)
}

// interrupted reports why a pass must not go on to its next tenant: ctx
// ended, or the reconciler is stopping (which ends ctx too, but not at
// once).
func (r *Reconciler) interrupted(ctx context.Context) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	return r.life.Err()
}

// reconcile is cancelled with ctx between tenants: a tenant whose step has
// begun, such as a container replacement, finishes it (within
// stepGrace), so stopping the agent never leaves a tenant with its old
// container removed and no new one. An interrupted pass keeps the
// statuses of the last whole one.
func (r *Reconciler) reconcile(ctx context.Context) error {
	step, cancel := outlive(ctx, r.config.StopTimeout+stepGrace)
	defer cancel()
	containers, err := r.runtime.List(step)
	if err != nil {
		return fmt.Errorf("list containers: %w", err)
	}
	byTenant := map[string][]runtime.Container{}
	for _, container := range containers {
		tenant := container.Labels[runtime.LabelTenant]
		byTenant[tenant] = append(byTenant[tenant], container)
	}
	// This node runs its own live tenants; another node's tenants run
	// there.
	var records, moving []store.Record
	wanted := map[string]bool{}
	for _, record := range r.own() {
		if record.Archived() {
			continue
		}
		wanted[record.Spec.Name] = true
		// A move handles the tenant's container and data meanwhile.
		if record.MoveTo == "" {
			records = append(records, record)
		} else {
			moving = append(moving, record)
		}
	}
	var errs []error
	for _, tenant := range slices.Sorted(maps.Keys(byTenant)) {
		if wanted[tenant] {
			continue
		}
		if err := r.interrupted(ctx); err != nil {
			return err
		}
		if err := r.removeContainers(step, byTenant[tenant]); err != nil {
			errs = append(errs, fmt.Errorf("remove containers of %q, which no tenant wants: %w", tenant, err))
			continue
		}
		r.log.Info("removed the containers of a tenant no longer wanted", "tenant", tenant)
	}
	if err := r.removeOrphanNetworks(step, wanted); err != nil {
		errs = append(errs, err)
	}
	statuses := make([]Status, len(records))
	for i, record := range records {
		if err := r.interrupted(ctx); err != nil {
			return errors.Join(append(errs, err)...)
		}
		status, err := r.reconcileTenant(step, record, byTenant[record.Spec.Name])
		if err != nil {
			status.Phase, status.Healthy, status.Message = PhaseFailed, false, err.Error()
			errs = append(errs, fmt.Errorf("tenant %s: %w", record.Spec.Name, err))
		}
		statuses[i] = status
	}
	// Side by side, so tenants that do not answer cost one timeout, not one
	// each.
	var checks sync.WaitGroup
	limit := make(chan struct{}, healthChecks)
	released := make([]bool, len(records))
	releaseErrs := make([]error, len(records))
	for i, record := range records {
		// While its image is pulled, the old container still serves: its
		// health is the tenant's.
		serving := statuses[i].Phase == PhasePulling && statuses[i].ContainerID != ""
		if statuses[i].Phase != PhaseRunning && !serving {
			continue
		}
		checks.Go(func() {
			limit <- struct{}{}
			defer func() { <-limit }()
			healthy, message := r.checkHealth(ctx, record)
			// Requests for a tenant that just moved here wait until it is
			// healthy; its app takes a moment to listen once started, so
			// it is checked again at once, not at the next pass.
			if record.MoveHeld {
				deadline := time.Now().Add(heldHealthWait)
				for !healthy && time.Now().Before(deadline) && ctx.Err() == nil {
					select {
					case <-ctx.Done():
					case <-time.After(heldHealthEvery):
					}
					healthy, message = r.checkHealth(ctx, record)
				}
			}
			statuses[i].Healthy = healthy
			if message != "" {
				statuses[i].Message = message
			}
			// Only a check of the container of the tenant's revision, not
			// the old one serving while the new image is pulled, says it
			// served on that revision.
			recorded := true
			if healthy && statuses[i].Phase == PhaseRunning {
				recorded = r.noteServed(record)
			}
			// Served at once, and said so, not when the slowest check of
			// the pass is done.
			if record.MoveHeld && healthy && statuses[i].Phase == PhaseRunning && recorded {
				released[i], releaseErrs[i] = true, r.store.ReleaseHold(record.Spec.Name)
				status := statuses[i]
				status.Generation, status.CheckedAt = record.Generation, time.Now().UTC()
				r.mu.Lock()
				if r.statuses != nil {
					r.statuses[record.Spec.Name] = status
				}
				r.mu.Unlock()
			}
		})
	}
	checks.Wait()
	for i, err := range releaseErrs {
		if err != nil {
			errs = append(errs, fmt.Errorf("tenant %s: serve it: %w", records[i].Spec.Name, err))
		}
	}
	if err := r.interrupted(ctx); err != nil {
		return errors.Join(append(errs, err)...)
	}
	byName := make(map[string]Status, len(records)+len(moving))
	for i, record := range records {
		statuses[i].Generation = record.Generation
		statuses[i].CheckedAt = time.Now().UTC()
		byName[record.Spec.Name] = statuses[i]
	}
	for _, record := range moving {
		byName[record.Spec.Name] = Status{Phase: PhaseMoving, Message: "moving to node " + record.MoveTo, Generation: record.Generation, CheckedAt: time.Now().UTC()}
	}
	// A tenant that moved here is served once it is healthy: until then,
	// requests for it wait.
	for i, record := range records {
		if record.MoveHeld && !released[i] && (statuses[i].Phase == PhaseRunning && statuses[i].Healthy || statuses[i].Phase == PhaseSuspended) {
			if err := r.store.ReleaseHold(record.Spec.Name); err != nil {
				errs = append(errs, fmt.Errorf("tenant %s: serve it: %w", record.Spec.Name, err))
			}
		}
	}
	r.mu.Lock()
	r.statuses = byName
	r.passAt, r.passErrors = time.Now(), len(errs)
	r.mu.Unlock()
	images := map[string]bool{}
	for _, record := range records {
		images[record.Spec.Image] = true
	}
	r.pulls.keep(images)
	r.sweepImages(ctx)
	return errors.Join(errs...)
}

// stepGrace is how long, beyond the stop timeout, a tenant's step may go on
// once its pass is cancelled. It keeps the agent's stop within systemd's
// default 90 s.
const stepGrace = 30 * time.Second

// outlive returns a context that ends grace after ctx ends, or when cancel
// is called: a step begun under ctx gets that long to finish.
func outlive(ctx context.Context, grace time.Duration) (context.Context, context.CancelFunc) {
	step, cancel := context.WithCancel(context.WithoutCancel(ctx))
	stop := context.AfterFunc(ctx, func() {
		timer := time.NewTimer(grace)
		defer timer.Stop()
		select {
		case <-timer.C:
			cancel()
		case <-step.Done():
		}
	})
	return step, func() {
		stop()
		cancel()
	}
}

// removeOrphanNetworks removes the networks of tenants not running here,
// whether or not they still had containers.
func (r *Reconciler) removeOrphanNetworks(ctx context.Context, wanted map[string]bool) error {
	networks, err := r.runtime.ListNetworks(ctx)
	if err != nil {
		return fmt.Errorf("list networks: %w", err)
	}
	remaining, err := r.runtime.List(ctx)
	if err != nil {
		return fmt.Errorf("list containers: %w", err)
	}
	var errs []error
	for _, network := range networks {
		tenant := network.Labels[runtime.LabelTenant]
		if wanted[tenant] || hasTenant(remaining, tenant) {
			continue // a live tenant, or containers this pass could not remove
		}
		if err := r.runtime.RemoveNetwork(ctx, network.Name); err != nil {
			errs = append(errs, fmt.Errorf("remove network %s: %w", network.Name, err))
		}
	}
	return errors.Join(errs...)
}

func hasTenant(containers []runtime.Container, tenant string) bool {
	for _, container := range containers {
		if container.Labels[runtime.LabelTenant] == tenant {
			return true
		}
	}
	return false
}

// Status returns what the last pass saw for tenant.
func (r *Reconciler) Status(tenant string) (Status, bool) {
	r.mu.RLock()
	defer r.mu.RUnlock()
	status, ok := r.statuses[tenant]
	status.ServedGeneration = r.seenServing[tenant]
	return status, ok
}

// LastPass returns when the last whole pass ended (zero before the first)
// and how many errors it had, not counting failures to remove images.
func (r *Reconciler) LastPass() (time.Time, int) {
	r.mu.RLock()
	defer r.mu.RUnlock()
	return r.passAt, r.passErrors
}

// own lists the tenants this node runs, archived ones too.
func (r *Reconciler) own() []store.Record {
	var records []store.Record
	for _, record := range r.store.List() {
		if record.Node == r.store.Node() {
			records = append(records, record)
		}
	}
	return records
}

// Exclusive runs fn while no reconcile pass, removal or purge runs, so a
// check and the change based on it cannot interleave with them.
func (r *Reconciler) Exclusive(fn func() error) error {
	r.pass.Lock()
	defer r.pass.Unlock()
	if r.life.Err() != nil {
		return ErrStopping
	}
	return fn()
}

// ErrStopping refuses work queued behind a pass while the agent stops, so
// the agent's stop does not wait for it.
var ErrStopping = errors.New("the agent is stopping; try again once it is back")
