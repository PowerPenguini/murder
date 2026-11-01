// Package reconcile makes the container runtime match the stored desired
// state: one container per running tenant, nothing for anyone else.
package reconcile

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"maps"
	"net"
	"net/http"
	"slices"
	"strconv"
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
	// PhaseRestarting is a container whose process exited and that the
	// engine is about to start again; LastExit says why it exited.
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
	// Restarts counts how often the engine restarted the container after
	// its process exited; LastExit says when it last exited and, if known,
	// why. Both start again with a new container.
	Restarts    int       `json:"restarts,omitempty"`
	LastExit    string    `json:"lastExit,omitempty"`
	ContainerID string    `json:"containerID,omitempty"`
	Generation  int64     `json:"generation"`
	CheckedAt   time.Time `json:"checkedAt"`
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
	store   store.Store
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
}

// replaceNotice is how long a replacement waits, once requests for the
// tenant are held, before it stops the old container: the edge learns of
// the hold through its long poll, in milliseconds.
const replaceNotice = 300 * time.Millisecond

// replacing is a set of tenants that says when it changes.
type replacing struct {
	mu      sync.Mutex
	tenants map[string]bool
	changed chan struct{}
}

func (s *replacing) set(tenant string, on bool) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.tenants[tenant] == on {
		return
	}
	if on {
		s.tenants[tenant] = true
	} else {
		delete(s.tenants, tenant)
	}
	close(s.changed)
	s.changed = make(chan struct{})
}

func (s *replacing) clear() {
	s.mu.Lock()
	defer s.mu.Unlock()
	if len(s.tenants) == 0 {
		return
	}
	clear(s.tenants)
	close(s.changed)
	s.changed = make(chan struct{})
}

// Replacing reports whether tenant's container is being replaced, so that
// requests for it wait instead of failing while it runs nowhere.
func (r *Reconciler) Replacing(tenant string) bool {
	if r == nil {
		return false
	}
	r.replacing.mu.Lock()
	defer r.replacing.mu.Unlock()
	return r.replacing.tenants[tenant]
}

// WatchReplacing is closed when the tenants being replaced change; without
// a reconciler, never.
func (r *Reconciler) WatchReplacing() <-chan struct{} {
	if r == nil {
		return nil
	}
	r.replacing.mu.Lock()
	defer r.replacing.mu.Unlock()
	return r.replacing.changed
}

// New returns a reconciler. All dependencies are required.
func New(st store.Store, rt runtime.Runtime, volumes volume.Volumes, key *secrets.Key, config Config, log *slog.Logger) *Reconciler {
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
		backend: Backend,
		pulls:   newPulls(life, rt.PullImage),
		life:    life,
		stop:    stop,
	}
}

// ContainerName and NetworkName name what Murder creates for a tenant.
func ContainerName(tenant string) string { return "murder-" + tenant }
func NetworkName(tenant string) string   { return "murder-" + tenant }

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
			// Served at once, and said so, not when the slowest check of
			// the pass is done.
			if record.MoveHeld && healthy && statuses[i].Phase == PhaseRunning {
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

func (r *Reconciler) reconcileTenant(ctx context.Context, record store.Record, existing []runtime.Container) (status Status, err error) {
	tenant := record.Spec
	if tenant.Suspended {
		if err := r.removeContainers(ctx, existing); err != nil {
			return Status{}, err
		}
		return Status{Phase: PhaseSuspended}, nil
	}
	// A spec stored under older, looser rules is not run until fixed; its
	// running container, if any, is left alone.
	if err := tenant.Validate(); err != nil {
		return Status{}, fmt.Errorf("stored spec no longer valid; correct the app or the tenant: %w", err)
	}
	uid, gid := tenant.Owner()
	dataPath, err := r.volumes.Ensure(ctx, tenant.Name, uid, gid, tenant.Limits.Or(r.config.DefaultLimits).DiskGB)
	if errors.Is(err, volume.ErrDiskLimit) {
		r.log.Warn("disk limit", "tenant", tenant.Name, "error", err)
		warning := err.Error()
		defer func() { status.Warning = warning }()
		err = nil
	}
	if err != nil {
		return Status{}, fmt.Errorf("data volume: %w", err)
	}
	labels := map[string]string{runtime.LabelTenant: tenant.Name}
	err = r.runtime.EnsureNetwork(ctx, NetworkName(tenant.Name), record.Subnet, labels)
	if errors.Is(err, runtime.ErrNetworkMismatch) {
		// A network left with another subnet, say by restoring an older
		// database: it and anything on it are rebuilt.
		r.log.Info("rebuilding a network with an outdated subnet", "tenant", tenant.Name, "subnet", record.Subnet)
		if err := r.clearRuntime(ctx, tenant.Name); err != nil {
			return Status{}, fmt.Errorf("network: %w", err)
		}
		existing = nil
		err = r.runtime.EnsureNetwork(ctx, NetworkName(tenant.Name), record.Subnet, labels)
	}
	if err != nil {
		return Status{}, fmt.Errorf("network: %w", err)
	}
	desired, err := r.containerSpec(record, dataPath)
	if err != nil {
		return Status{}, err
	}
	// A container made by a release before inputs were hashed carries the
	// hash of the container as made: it is current as well.
	legacy, err := legacyHash(desired)
	if err != nil {
		return Status{}, err
	}
	var current *runtime.Container
	var outdated []runtime.Container
	for i := range existing {
		hash := existing[i].Labels[runtime.LabelHash]
		if current == nil && (hash == desired.Labels[runtime.LabelHash] || hash == legacy) {
			current = &existing[i]
			continue
		}
		outdated = append(outdated, existing[i])
	}
	// What List said may be out of date: a stopped container may have been
	// started by its restart policy since (possibly before Ensure mounted
	// the dataset), a running one may have exited.
	var state runtime.ContainerState
	if current != nil {
		state, err = r.runtime.Inspect(ctx, current.ID)
		switch {
		case errors.Is(err, runtime.ErrNotFound):
			current = nil // removed since List; recreated below
		case err != nil:
			return Status{}, fmt.Errorf("inspect container: %w", err)
		case state.RestartPolicy != "" && state.RestartPolicy != runtime.RestartPolicy:
			// Containers made before the engine stopped starting them at
			// boot change in place.
			if err := r.runtime.SetRestartPolicy(ctx, current.ID, runtime.RestartPolicy); err != nil {
				r.log.Warn("change the container's restart policy", "tenant", tenant.Name, "container", current.ID, "error", err)
			}
		}
	}
	// A container started before its dataset was mounted runs on the empty
	// directory underneath; it is replaced, not adopted. One not running
	// sees the dataset once started.
	if current != nil && state.Pid > 0 && r.volumes.LateMounts() {
		serving, err := r.volumes.Serving(ctx, tenant.Name, state.Pid)
		if err != nil {
			return Status{}, fmt.Errorf("check the container's data mount: %w", err)
		}
		if !serving {
			r.log.Warn("container does not see the tenant's data; replacing it", "tenant", tenant.Name, "container", current.ID)
			outdated = append(outdated, *current)
			current = nil
		}
	}
	if current == nil {
		id, removed, err := r.replace(ctx, record, desired, outdated)
		if err != nil {
			// The status names the container whose logs tell most: the old
			// one while it still serves, else the new one, if created.
			status := Status{ContainerID: id}
			for _, container := range outdated {
				if container.Running && !removed {
					status.ContainerID = container.ID
					break
				}
			}
			if errors.Is(err, errPulling) {
				status.Phase, status.Message = PhasePulling, "pulling "+tenant.Image
				return status, nil
			}
			return status, err
		}
		current = &runtime.Container{ID: id, Running: true}
	} else {
		if err := r.removeContainers(ctx, outdated); err != nil {
			return Status{}, err
		}
		// One the engine is restarting is left to it.
		if state.Pid == 0 && !state.Restarting {
			if err := r.runtime.Start(ctx, current.ID); err != nil {
				return Status{}, fmt.Errorf("start: %w", err)
			}
			r.log.Info("restarted stopped tenant container", "tenant", tenant.Name, "container", current.ID)
			// The engine counts restarts again from here; the exit was not
			// the app's to report.
			state = runtime.ContainerState{}
		}
	}
	status = Status{Phase: PhaseRunning, ContainerID: current.ID, Restarts: state.Restarts}
	// An exit the container was not restarted after was Murder's or the
	// host's doing, not the app's.
	if state.Restarts > 0 {
		status.LastExit = lastExit(state, tenant.Limits.Or(r.config.DefaultLimits).MemoryMB)
	}
	if state.Restarting {
		status.Phase, status.Message = PhaseRestarting, "the app keeps exiting; the engine restarts it"
	}
	return status, nil
}

// lastExit describes the last exit of a container's process.
func lastExit(state runtime.ContainerState, memoryMB int) string {
	if state.ExitedAt.IsZero() {
		return ""
	}
	at := state.ExitedAt.UTC().Format(time.RFC3339)
	switch {
	case !state.ExitKnown:
		return "exited at " + at
	case state.OOMKilled:
		return fmt.Sprintf("killed for using more than its %d MB of memory, at %s", memoryMB, at)
	// SIGKILL, and the engine restarting the container: nothing but the
	// kernel kills a tenant's process so, most often for its memory. The
	// engine misses some out-of-memory kills (a quarter on GitHub's
	// runners, Docker 28 on cgroup v2), so a kill it did not report as one
	// is most likely one still.
	case state.ExitCode == 137 && memoryMB > 0:
		return fmt.Sprintf("killed (SIGKILL) at %s, most likely for using more than its %d MB of memory", at, memoryMB)
	case state.ExitCode == 137:
		return "killed (SIGKILL) at " + at
	default:
		return fmt.Sprintf("exited with code %d at %s", state.ExitCode, at)
	}
}

// replace starts the desired container in place of outdated ones. Nothing is
// stopped until the new image is present (errPulling until then) and, after
// an image change, the
// data has been snapshotted. The snapshot is taken while the old container
// runs: apps are only onboarded once they recover from point-in-time
// snapshots, and a failed snapshot then blocks the change without an outage.
//
// It returns the new container's ID, once created, and whether the outdated
// containers are gone: until then the old one still serves.
func (r *Reconciler) replace(ctx context.Context, record store.Record, desired runtime.ContainerSpec, outdated []runtime.Container) (id string, removed bool, err error) {
	tenant := record.Spec
	if err := r.image(ctx, tenant.Image); err != nil {
		return "", false, err
	}
	if record.DataImage != "" && record.DataImage != tenant.Image {
		name, err := r.volumes.Snapshot(ctx, tenant.Name, "image-change")
		switch {
		case errors.Is(err, volume.ErrSnapshotsUnsupported):
		case err != nil:
			return "", false, fmt.Errorf("snapshot before the image change (the old container keeps running): %w", err)
		default:
			r.log.Info("snapshot before image change", "tenant", tenant.Name, "snapshot", name, "from", record.DataImage, "to", tenant.Image)
			if err := r.volumes.Prune(ctx, tenant.Name, "image-change", snapshotsKept); err != nil {
				r.log.Warn("prune old snapshots", "tenant", tenant.Name, "error", err)
			}
		}
	}
	// From here on the data belongs to the new image: a later pass must not
	// snapshot again for the same change.
	if err := r.store.Observe(tenant.Name, tenant.Image); err != nil {
		return "", false, fmt.Errorf("record the data's image: %w", err)
	}
	// A running tenant runs nowhere from here until its new container
	// answers: requests for it wait meanwhile.
	held := len(outdated) > 0 && !tenant.Suspended
	if held {
		r.replacing.set(tenant.Name, true)
		select {
		case <-ctx.Done():
		case <-time.After(r.replaceNotice):
		}
	}
	if err := r.removeContainers(ctx, outdated); err != nil {
		return "", false, err
	}
	id, err = r.runtime.Create(ctx, desired)
	if err != nil {
		return "", true, err
	}
	if err := r.runtime.Start(ctx, id); err != nil {
		return id, true, fmt.Errorf("start: %w", err)
	}
	// Served as soon as it answers, not once every other tenant of the
	// pass is reconciled.
	if held {
		// The tenant's step ends now; the wait lasts as long as the
		// reconciler, within its own bound.
		r.replacedWaits.Go(func() { r.awaitReplaced(r.life, record) })
	}
	r.log.Info("started tenant container", "tenant", tenant.Name, "container", id, "generation", record.Generation)
	return id, true, nil
}

// image returns nil once image is present. Until then a pull runs in the
// background and image returns errPulling, or the failure of the last
// attempt until the next one is due.
// awaitReplaced serves a tenant whose container was replaced once it
// answers, checking every heldHealthEvery for heldHealthWait at most;
// after that its requests are served, or fail, as they would.
func (r *Reconciler) awaitReplaced(ctx context.Context, record store.Record) {
	defer r.replacing.set(record.Spec.Name, false)
	deadline := time.Now().Add(r.replacedWait)
	for time.Now().Before(deadline) && ctx.Err() == nil {
		if healthy, _ := r.checkHealth(ctx, record); healthy {
			return
		}
		select {
		case <-ctx.Done():
		case <-time.After(heldHealthEvery):
		}
	}
}

func (r *Reconciler) image(ctx context.Context, image string) error {
	present, err := r.runtime.HasImage(ctx, image)
	if err != nil {
		return fmt.Errorf("inspect image: %w", err)
	}
	if present {
		r.pulls.present(image)
		return nil
	}
	return r.pulls.fetch(image)
}

func (r *Reconciler) removeContainers(ctx context.Context, containers []runtime.Container) error {
	var errs []error
	for _, container := range containers {
		if container.Running {
			if err := r.runtime.Stop(ctx, container.ID, r.config.StopTimeout); err != nil {
				errs = append(errs, fmt.Errorf("stop %s: %w", container.Name, err))
				continue
			}
		}
		if err := r.runtime.Remove(ctx, container.ID); err != nil {
			errs = append(errs, fmt.Errorf("remove %s: %w", container.Name, err))
		}
	}
	return errors.Join(errs...)
}

// imageRetry is how long an image the engine kept, in use by a container,
// waits before the next attempt to remove it.
const imageRetry = time.Hour

// sweepImages removes images no tenant needs any more, so old releases do
// not fill the disk. A tenant needs the image it runs, the one its data has
// and the one before (a rollback then needs no registry). Only images
// pinned by registry digest go: those can be pulled again, a local image
// could not. Images are compared by ID, so an image a tenant needs under
// another name stays.
func (r *Reconciler) sweepImages(ctx context.Context) {
	needed := map[string]bool{}
	for _, record := range r.own() {
		for _, image := range []string{record.Spec.Image, record.DataImage, record.PreviousImage} {
			if image != "" {
				needed[image] = true
			}
		}
	}
	now := time.Now()
	var candidates []string
	for _, image := range r.store.Images() {
		switch {
		case needed[image] || now.Before(r.imageRetryAt[image]):
		case !spec.RegistryPinned(image):
			r.forgetImage(image) // a local image stays on the node, untracked
		default:
			candidates = append(candidates, image)
		}
	}
	if len(candidates) == 0 {
		return
	}
	neededIDs := map[string]bool{}
	for image := range needed {
		id, err := r.runtime.ImageID(ctx, image)
		if err != nil {
			r.log.Warn("inspect an image a tenant needs; no image is removed this pass", "image", image, "error", err)
			return
		}
		neededIDs[id] = true
	}
	for _, image := range candidates {
		id, err := r.runtime.ImageID(ctx, image)
		switch {
		case err != nil:
			r.log.Warn("inspect an image no tenant needs", "image", image, "error", err)
			continue
		case id == "" && r.pulls.pulling(image):
			continue // its pull may still bring it
		case id == "":
			r.forgetImage(image) // gone already
			continue
		case neededIDs[id]:
			r.imageRetryAt[image] = now.Add(imageRetry) // needed under another name
			continue
		case r.needed(image):
			continue // a tenant was given it since the sweep began
		}
		switch err := r.runtime.RemoveImage(ctx, id); {
		case errors.Is(err, runtime.ErrInUse):
			r.imageRetryAt[image] = now.Add(imageRetry)
		case err != nil:
			r.log.Warn("remove an image no tenant needs", "image", image, "error", err)
			r.imageRetryAt[image] = now.Add(imageRetry)
		default:
			r.log.Info("removed an image no tenant needs", "image", image)
			r.forgetImage(image)
		}
	}
}

// needed reports whether a tenant needs image now, read from the store
// right before it is removed: API writes do not wait for the pass.
func (r *Reconciler) needed(image string) bool {
	for _, record := range r.own() {
		if record.Spec.Image == image || record.DataImage == image || record.PreviousImage == image {
			return true
		}
	}
	return false
}

func (r *Reconciler) forgetImage(image string) {
	delete(r.imageRetryAt, image)
	if err := r.store.ForgetImage(image); err != nil {
		r.log.Warn("forget an image", "image", image, "error", err)
	}
}

// Backend is where a tenant listens: its fixed address in its network and
// the app's port. Only the host reaches it; other tenants cannot.
func Backend(record store.Record) (string, error) {
	_, address, err := runtime.Addresses(record.Subnet)
	if err != nil {
		return "", err
	}
	return net.JoinHostPort(address.String(), strconv.Itoa(record.Spec.Port)), nil
}

// Host and URL are a tenant's public address.
func (r *Reconciler) Host(tenant string) string { return tenant + "." + r.config.BaseDomain }
func (r *Reconciler) URL(tenant string) string  { return r.config.Scheme + "://" + r.Host(tenant) }

func (r *Reconciler) containerSpec(record store.Record, dataPath string) (runtime.ContainerSpec, error) {
	tenant := record.Spec
	env := tenant.Render(spec.Values{Host: r.Host(tenant.Name), URL: r.URL(tenant.Name), Network: record.Subnet})
	for name := range tenant.Secrets {
		sealed, ok := record.Sealed[name]
		if !ok {
			return runtime.ContainerSpec{}, fmt.Errorf("secret %s has not been generated", name)
		}
		value, err := r.key.Open(secrets.Context(tenant.Name, name), sealed)
		if err != nil {
			return runtime.ContainerSpec{}, fmt.Errorf("secret %s: %w", name, err)
		}
		env[name] = value
	}
	pairs := make([]string, 0, len(env))
	for _, name := range slices.Sorted(maps.Keys(env)) {
		pairs = append(pairs, name+"="+env[name])
	}
	limits := tenant.Limits.Or(r.config.DefaultLimits)
	var tmpfs map[string]string
	if len(tenant.Tmpfs) > 0 {
		tmpfs = maps.Clone(tenant.Tmpfs)
	}
	_, address, err := runtime.Addresses(record.Subnet)
	if err != nil {
		return runtime.ContainerSpec{}, err
	}
	desired := runtime.ContainerSpec{
		Name:         ContainerName(tenant.Name),
		Image:        tenant.Image,
		Env:          pairs,
		User:         tenant.User,
		Network:      NetworkName(tenant.Name),
		Address:      address.String(),
		Mounts:       []runtime.Mount{{Source: dataPath, Target: tenant.DataPath}},
		MemoryBytes:  int64(limits.MemoryMB) << 20,
		NanoCPUs:     int64(limits.CPUs * 1e9),
		PIDs:         int64(limits.PIDs),
		ReadOnlyRoot: !tenant.WritableRoot,
		Tmpfs:        tmpfs,
		Labels: map[string]string{
			runtime.LabelTenant: tenant.Name,
			runtime.LabelImage:  tenant.Image,
		},
	}
	hash, err := r.inputsHash(record, dataPath)
	if err != nil {
		return runtime.ContainerSpec{}, err
	}
	desired.Labels[runtime.LabelHash] = hash
	return desired, nil
}

// inputs are what a tenant's container is made from that the operator
// decides: the tenant's spec, its limits, address, names, secrets and data.
// How Murder turns them into a container is not among them, so a new
// release of Murder replaces no container; "murder tenant roll" applies
// what it changed. Fields only ever get added, omitted when empty, so a
// tenant whose inputs are as they were keeps its container across releases.
type inputs struct {
	Spec     spec.Tenant       `json:"spec"`
	Limits   spec.Limits       `json:"limits"`
	Subnet   string            `json:"subnet"`
	Host     string            `json:"host"`
	URL      string            `json:"url"`
	Sealed   map[string]string `json:"sealed,omitempty"`
	DataPath string            `json:"dataPath"`
	Rollout  int64             `json:"rollout,omitempty"`
}

// inputsHash fingerprints a tenant's inputs. A container whose label differs
// is replaced. JSON encodes maps in sorted key order, so equal inputs hash
// alike.
func (r *Reconciler) inputsHash(record store.Record, dataPath string) (string, error) {
	tenant := record.Spec
	tenant.Suspended = false // a resumed tenant gets a new container anyway
	// Health checks, routing and the disk limit (set on the volume as it
	// runs) use these; the container does not.
	tenant.HealthPath, tenant.Port, tenant.Limits.DiskGB = "", 0, 0
	limits := tenant.Limits.Or(r.config.DefaultLimits)
	limits.DiskGB = 0
	// No mounts, variables or secrets are none, however written.
	if len(tenant.Tmpfs) == 0 {
		tenant.Tmpfs = nil
	}
	if len(tenant.Env) == 0 {
		tenant.Env = nil
	}
	if len(tenant.Secrets) == 0 {
		tenant.Secrets = nil
	}
	sealed := map[string]string{}
	for name := range tenant.Secrets {
		sealed[name] = record.Sealed[name]
	}
	encoded, err := json.Marshal(inputs{
		Spec: tenant, Limits: limits, Subnet: record.Subnet,
		Host: r.Host(tenant.Name), URL: r.URL(tenant.Name), Sealed: sealed, DataPath: dataPath, Rollout: record.Rollout,
	})
	if err != nil {
		return "", err
	}
	sum := sha256.Sum256(encoded)
	return hex.EncodeToString(sum[:16]), nil
}

// legacyHash is the label releases before inputs hashing gave a container:
// the fingerprint of the container as made, before its hash label. Such a
// container keeps it until it is replaced.
func legacyHash(desired runtime.ContainerSpec) (string, error) {
	desired.Labels = maps.Clone(desired.Labels)
	delete(desired.Labels, runtime.LabelHash)
	encoded, err := json.Marshal(desired)
	if err != nil {
		return "", err
	}
	sum := sha256.Sum256(encoded)
	return hex.EncodeToString(sum[:16]), nil
}

func (r *Reconciler) checkHealth(ctx context.Context, record store.Record) (bool, string) {
	backend, err := r.backend(record)
	if err != nil {
		return false, err.Error()
	}
	target := "http://" + backend + record.Spec.HealthPath
	request, err := http.NewRequestWithContext(ctx, http.MethodGet, target, nil)
	if err != nil {
		return false, err.Error()
	}
	request.Host = r.Host(record.Spec.Name)
	response, err := r.health.Do(request)
	if err != nil {
		return false, "health check: " + err.Error()
	}
	response.Body.Close()
	if response.StatusCode < 200 || response.StatusCode > 299 {
		return false, "health check: HTTP " + strconv.Itoa(response.StatusCode)
	}
	return true, ""
}

// Status returns what the last pass saw for tenant.
func (r *Reconciler) Status(tenant string) (Status, bool) {
	r.mu.RLock()
	defer r.mu.RUnlock()
	status, ok := r.statuses[tenant]
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

// ErrElsewhere refuses work on a tenant another node runs.
var ErrElsewhere = errors.New("the tenant runs on another node")

// DataExists reports whether a tenant name still has data on this node.
func (r *Reconciler) DataExists(ctx context.Context, tenant string) (bool, error) {
	return r.volumes.Exists(ctx, tenant)
}

// Purge deletes an archived tenant for good: its data, snapshots and saved
// secrets. It also cleans up data no archive entry accounts for. A live
// tenant must be archived first.
func (r *Reconciler) Purge(ctx context.Context, name, actor string) error {
	r.pass.Lock()
	defer r.pass.Unlock()
	if r.life.Err() != nil {
		return ErrStopping
	}
	// The data goes before the record: a frozen state would keep the
	// record of a tenant whose data is gone.
	if frozen := r.store.Frozen(); frozen != "" {
		return fmt.Errorf("%w: %s", store.ErrFrozen, frozen)
	}
	record, err := r.store.Get(name)
	switch {
	case err == nil && record.Node != r.store.Node():
		return fmt.Errorf("%w: tenant %s runs on node %s: purge it there", ErrElsewhere, name, record.Node)
	case err == nil && !record.Archived():
		return fmt.Errorf("%w: archive %s before purging it", store.ErrNotArchived, name)
	case errors.Is(err, store.ErrNotFound):
		exists, err := r.volumes.Exists(ctx, name)
		if err != nil {
			return err
		}
		if !exists {
			return fmt.Errorf("%w: no archived tenant or data named %s", store.ErrNotFound, name)
		}
	case err != nil:
		return err
	}
	if err := r.clearRuntime(ctx, name); err != nil {
		return err
	}
	if err := r.volumes.Remove(ctx, name); err != nil {
		return err
	}
	return r.store.Forget(name, actor)
}

// Release stops a tenant's container and removes its network, between
// passes, for a move: the tenant then runs nowhere until its new node
// starts it.
func (r *Reconciler) Release(ctx context.Context, name string) error {
	return r.Exclusive(func() error { return r.clearRuntime(ctx, name) })
}

// clearRuntime makes sure no container or network of the tenant remains.
func (r *Reconciler) clearRuntime(ctx context.Context, name string) error {
	containers, err := r.runtime.List(ctx)
	if err != nil {
		return err
	}
	var own []runtime.Container
	for _, container := range containers {
		if container.Labels[runtime.LabelTenant] == name {
			own = append(own, container)
		}
	}
	if err := r.removeContainers(ctx, own); err != nil {
		return fmt.Errorf("remove containers: %w", err)
	}
	if err := r.runtime.RemoveNetwork(ctx, NetworkName(name)); errors.Is(err, runtime.ErrNotOwned) {
		// Not ours to remove, but it will block a new tenant of this name.
		r.log.Warn("a network with the tenant's name belongs to someone else", "tenant", name, "error", err)
	} else if err != nil {
		return fmt.Errorf("remove network: %w", err)
	}
	return nil
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
