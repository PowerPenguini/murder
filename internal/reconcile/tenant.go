package reconcile

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"slices"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/PowerPenguini/murder/internal/runtime"
	"github.com/PowerPenguini/murder/internal/store"
	"github.com/PowerPenguini/murder/internal/volume"
)

// appExits are the exits of one container's app.
type appExits struct {
	container string
	count     int
	last      string
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
	if err := r.checkData(ctx, record); err != nil {
		return Status{}, err
	}
	if record.Restore != "" {
		if err := r.restoreData(ctx, record, existing); err != nil {
			return Status{}, err
		}
		existing, record.DataImage = nil, record.Restore
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
	// One made by a release before master key rotations carries the hash
	// of its secrets' sealed values: current until they are sealed again.
	sealedHash, err := r.inputsHash(record, dataPath)
	if err != nil {
		return Status{}, err
	}
	var current *runtime.Container
	var outdated []runtime.Container
	for i := range existing {
		hash := existing[i].Labels[runtime.LabelHash]
		if current == nil && (hash == desired.Labels[runtime.LabelHash] || hash == sealedHash || hash == legacy) {
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
		// The engine starts no container by itself (runtime.RestartPolicy):
		// a stopped one is started here. One an older release's policy has
		// the engine restarting is left to it.
		exits := r.exits[tenant.Name]
		if exits.container != current.ID {
			exits = appExits{container: current.ID}
		}
		restarted := false
		if state.Pid == 0 && !state.Restarting {
			// The app's exit, while the agent ran, and not a stop (SIGTERM,
			// as the engine or the server sends): it is reported.
			if state.ExitKnown && state.ExitCode != 143 && state.ExitedAt.After(r.started) {
				exits.count++
				exits.last = lastExit(state, tenant.Limits.Or(r.config.DefaultLimits).MemoryMB)
				restarted = true
			}
			if err := r.runtime.Start(ctx, current.ID); err != nil {
				r.exits[tenant.Name] = exits
				return Status{}, fmt.Errorf("start: %w", err)
			}
			r.log.Info("started a stopped tenant container", "tenant", tenant.Name, "container", current.ID, "exit", exits.last, "app_exited", restarted)
			state = runtime.ContainerState{}
		}
		r.exits[tenant.Name] = exits
		status = Status{Phase: PhaseRunning, ContainerID: current.ID, Restarts: state.Restarts + exits.count}
		switch {
		case exits.count > 0:
			status.LastExit = exits.last
		// An older release's policy had the engine restart the container.
		case state.Restarts > 0:
			status.LastExit = lastExit(state, tenant.Limits.Or(r.config.DefaultLimits).MemoryMB)
		}
		if state.Restarting {
			status.Phase, status.Message = PhaseRestarting, "the app keeps exiting; the engine restarts it"
		} else if restarted {
			status.Phase, status.Message = PhaseRestarting, "the app exited; it was started again"
		}
		return status, nil
	}
	return Status{Phase: PhaseRunning, ContainerID: current.ID}, nil
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
// data has been snapshotted. A first snapshot is taken while the old
// container runs: a failed one then blocks the change without an outage. A
// second is taken once the old container has stopped, so the snapshot to
// go back to holds every write the old image made; it replaces the first.
// Should it fail, the first stays, and the change goes on: apps are only
// onboarded once they recover from point-in-time snapshots.
//
// It returns the new container's ID, once created, and whether the outdated
// containers are gone: until then the old one still serves.
func (r *Reconciler) replace(ctx context.Context, record store.Record, desired runtime.ContainerSpec, outdated []runtime.Container) (id string, removed bool, err error) {
	tenant := record.Spec
	if err := r.image(ctx, tenant.Image); err != nil {
		return "", false, err
	}
	gate := ""
	if record.DataImage != "" && record.DataImage != tenant.Image {
		name, err := r.volumes.Snapshot(ctx, tenant.Name, "image-change")
		switch {
		case errors.Is(err, volume.ErrSnapshotsUnsupported):
		case err != nil:
			return "", false, fmt.Errorf("snapshot before the image change (the old container keeps running): %w", err)
		default:
			gate = name
			r.log.Info("snapshot before image change", "tenant", tenant.Name, "snapshot", name, "from", record.DataImage, "to", tenant.Image)
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
	// A tenant a wave moved takes no request until it is known to serve:
	// the edges must hold its requests before its new container starts. If
	// they do not say so in time, it may take one, so its data never goes
	// back by itself: that is recorded first.
	if record.AwaitsServing() && r.edgesHold != nil {
		wait, cancel := context.WithTimeout(ctx, edgesHoldWait)
		held := r.edgesHold(wait, tenant.Name)
		cancel()
		if !held {
			r.log.Warn("the edge did not say it holds the tenant's requests: its data will not go back by itself", "tenant", tenant.Name)
			if err := r.store.Served(tenant.Name, record.Generation); err != nil {
				return "", false, fmt.Errorf("the edge does not hold the tenant's requests, and that could not be recorded: %w", err)
			}
		}
	}
	if err := r.removeContainers(ctx, outdated); err != nil {
		return "", false, err
	}
	// With no old container running, the first snapshot holds every write.
	if gate != "" && slices.ContainsFunc(outdated, func(c runtime.Container) bool { return c.Running }) {
		r.finalSnapshot(ctx, tenant.Name, gate)
	} else if gate != "" {
		if err := r.volumes.Prune(ctx, tenant.Name, "image-change", snapshotsKept); err != nil {
			r.log.Warn("prune old snapshots", "tenant", tenant.Name, "error", err)
		}
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

// snapshotRestorer puts a tenant's data back to its newest snapshot taken
// for reason, destroying the newer ones; ZFS can. It reports
// volume.ErrSnapshotsUnsupported when there is none to go back to.
type snapshotRestorer interface {
	RestoreSnapshot(ctx context.Context, tenant, reason string) (string, error)
}

// restoreData takes a tenant's data back to the snapshot taken as its image
// changed, as its Restore asks: a rollout took it back to an image that
// owned the data before, and the newer image never served. Its containers
// go first; the old image then starts on the data as it left it.
func (r *Reconciler) restoreData(ctx context.Context, record store.Record, existing []runtime.Container) error {
	tenant := record.Spec.Name
	if err := r.removeContainers(ctx, existing); err != nil {
		return err
	}
	if record.DataImage != record.Restore {
		restorer, ok := r.volumes.(snapshotRestorer)
		snapshot, err := "", volume.ErrSnapshotsUnsupported
		if ok {
			snapshot, err = restorer.RestoreSnapshot(ctx, tenant, "image-change")
		}
		switch {
		case errors.Is(err, volume.ErrSnapshotsUnsupported):
			r.log.Warn("no snapshot to take the data back to: the old image runs on the data as the newer one left it", "tenant", tenant, "image", record.Restore)
		case errors.Is(err, volume.ErrNoSnapshot):
			// The old image is not started on data the newer one may
			// have changed past what it reads.
			return fmt.Errorf("its data should go back to before image %s, but this node has no snapshot of then: it stays stopped; `murder app rollout abort %s` starts it on its data as it is (%w)", record.Restore, record.App, err)
		case err != nil:
			return fmt.Errorf("take the data back to the snapshot before the image change: %w", err)
		default:
			r.log.Info("took the data back to the snapshot before the image change", "tenant", tenant, "snapshot", snapshot, "image", record.Restore)
		}
		if err := r.store.Observe(tenant, record.Restore); err != nil {
			return fmt.Errorf("record the data's image: %w", err)
		}
	}
	return r.store.Restored(tenant, record.Restore)
}

// snapshotDestroyer destroys one snapshot of a tenant's data, by the name
// after its "@"; ZFS can.
type snapshotDestroyer interface {
	DestroySnapshot(ctx context.Context, tenant, snapshot string) error
}

// finalSnapshot snapshots a tenant's data once its old container has
// stopped, every write of the old image in, and destroys gate, the one
// taken while it ran; then it keeps the newest snapshotsKept. Requests
// wait meanwhile: a ZFS snapshot takes milliseconds.
func (r *Reconciler) finalSnapshot(ctx context.Context, tenant, gate string) {
	name, err := r.volumes.Snapshot(ctx, tenant, "image-change")
	if err != nil {
		r.log.Warn("snapshot after the old container stopped; going back would lose what it wrote since the one taken while it ran", "tenant", tenant, "snapshot", gate, "error", err)
	} else {
		r.log.Info("snapshot after the old container stopped", "tenant", tenant, "snapshot", name)
		if destroyer, ok := r.volumes.(snapshotDestroyer); ok {
			_, short, _ := strings.Cut(gate, "@")
			if err := destroyer.DestroySnapshot(ctx, tenant, short); err != nil {
				r.log.Warn("destroy the snapshot taken while the old container ran", "tenant", tenant, "snapshot", gate, "error", err)
			}
		}
	}
	if err := r.volumes.Prune(ctx, tenant, "image-change", snapshotsKept); err != nil {
		r.log.Warn("prune old snapshots", "tenant", tenant, "error", err)
	}
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
		// The new container answers: once its serving is recorded, the
		// requests held for it go to it.
		if healthy, _ := r.checkHealth(ctx, record); healthy && r.noteServed(record) {
			return
		}
		select {
		case <-ctx.Done():
		case <-time.After(heldHealthEvery):
		}
	}
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

// noteServed records, once per generation, that a tenant a rollout moved
// passed a health check on its new revision, its new container's: before
// it is served, so its rollout never takes back the data it then takes.
// Recorded by the node that serves it, it does not depend on the leader
// looking at that moment; and its status says so too, in case the record
// cannot be written. It reports whether the store has it: requests held
// for the tenant wait until it does.
func (r *Reconciler) noteServed(record store.Record) bool {
	if record.AppRevision != 0 || record.BeforeRevision == 0 || record.Served {
		return true
	}
	name := record.Spec.Name
	r.mu.Lock()
	if r.seenServing == nil {
		r.seenServing = map[string]int64{}
	}
	r.seenServing[name] = record.Generation
	done := r.served[name] == record.Generation
	r.mu.Unlock()
	if done {
		return true
	}
	if err := r.store.Served(name, record.Generation); err != nil {
		r.log.Warn("record that a tenant served on its new revision", "tenant", name, "error", err)
		return false
	}
	r.mu.Lock()
	if r.served == nil {
		r.served = map[string]int64{}
	}
	r.served[name] = record.Generation
	r.mu.Unlock()
	return true
}
