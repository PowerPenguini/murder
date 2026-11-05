package reconcile

import (
	"context"
	"errors"
	"fmt"
	"io/fs"
	"net/http"
	"os"
	"path"
	"path/filepath"
	"slices"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/PowerPenguini/murder/internal/runtime"
	"github.com/PowerPenguini/murder/internal/spec"
	"github.com/PowerPenguini/murder/internal/store"
	"github.com/PowerPenguini/murder/internal/volume"
)

// appExits are the exits of one container's app: how many, and the last,
// when and how.
type appExits struct {
	container string
	count     int
	at        time.Time
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
		if err := r.removeContainers(ctx, existing, "stopped"); err != nil {
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
	// The limits it runs with, and the node's defaults among them.
	limits := tenant.Limits.Or(r.config.DefaultLimits)
	defer func() { status.Limits, status.Defaults = limits, r.config.DefaultLimits }()
	// Who wrote its data, as read: what an app that runs as another user
	// would find there.
	if who, mixed, err := volume.DataOwner(dataPath); err == nil {
		defer func() { status.DataOwner, status.DataMixed = who, mixed }()
	} else {
		r.log.Debug("data owner not read", "tenant", tenant.Name, "error", err)
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
		id, removed, err := r.replace(ctx, record, dataPath, desired, outdated)
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
		if err := r.removeContainers(ctx, outdated, "replaced"); err != nil {
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
			// as the engine or the server sends): it is reported, and
			// counted once, however many passes fail to start it again.
			if state.ExitKnown && state.ExitCode != 143 && state.ExitedAt.After(r.started) {
				if !state.ExitedAt.Equal(exits.at) {
					exits.count++
					exits.at = state.ExitedAt
					exits.last = lastExit(state, tenant.Limits.Or(r.config.DefaultLimits).MemoryMB)
					// Its lines up to the exit, before the new run's.
					memoryMB := tenant.Limits.Or(r.config.DefaultLimits).MemoryMB
					r.keepPrevious(ctx, tenant.Name, *current, Previous{Why: "exited", Image: tenant.Image,
						ExitedAt: state.ExitedAt.UTC(), ExitKnown: state.ExitKnown, ExitCode: state.ExitCode, OOMKilled: state.OOMKilled,
						MemoryMB: memoryMB})
					// Said in the audit log, once per exit, with the limit
					// it ran under: whether a limit is too low is judged
					// from the kills at the limit in effect.
					if err := r.store.AppExited(tenant.Name, ExitDetail(state, memoryMB)); err != nil {
						r.log.Warn("record the app's exit", "tenant", tenant.Name, "error", err)
					}
				}
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

// ExitDetail is how an app exit is said in the audit log: its code first,
// then, for a kill, the memory limit it ran under, which the panel reads:
// "exit 137: killed for using more than its 256 MB of memory".
func ExitDetail(state runtime.ContainerState, memoryMB int) string {
	switch {
	case !state.ExitKnown:
		return "exited; its code is not known"
	case state.OOMKilled:
		return fmt.Sprintf("exit %d: killed for using more than its %d MB of memory", state.ExitCode, memoryMB)
	case state.ExitCode == 137 && memoryMB > 0:
		return fmt.Sprintf("exit 137: killed (SIGKILL), most likely for using more than its %d MB of memory", memoryMB)
	}
	return fmt.Sprintf("exit %d", state.ExitCode)
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
// The volumes the image declares (VOLUME) are bound from the tenant's
// data, dataPath on the host; an image with one outside the data is
// refused before anything stops.
//
// It returns the new container's ID, once created, and whether the outdated
// containers are gone: until then the old one still serves.
func (r *Reconciler) replace(ctx context.Context, record store.Record, dataPath string, desired runtime.ContainerSpec, outdated []runtime.Container) (id string, removed bool, err error) {
	tenant := record.Spec
	if err := r.image(ctx, tenant.Image); err != nil {
		return "", false, err
	}
	volumes, err := r.imageVolumes(ctx, tenant)
	if err != nil {
		return "", false, err
	}
	uid, gid := tenant.Owner()
	if err := volumeDirs(dataPath, volumes, uid, gid); err != nil {
		return "", false, err
	}
	for _, dir := range volumes {
		desired.Mounts = append(desired.Mounts, runtime.Mount{Source: filepath.Join(dataPath, dir), Target: path.Join(tenant.DataPath, dir)})
	}
	gate := ""
	if record.DataImage != "" && record.DataImage != tenant.Image {
		name, err := r.snapshotImage(ctx, tenant.Name, record.DataImage)
		switch {
		case errors.Is(err, volume.ErrSnapshotsUnsupported):
		case err != nil:
			return "", false, fmt.Errorf("snapshot before the image change (the old container keeps running): %w", err)
		default:
			gate = name
			r.log.Info("snapshot before image change", "tenant", tenant.Name, "snapshot", name, "from", record.DataImage, "to", tenant.Image)
		}
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
	if err := r.removeContainers(ctx, outdated, "replaced"); err != nil {
		return "", false, err
	}
	// With no old container running, the first snapshot holds every write.
	if gate != "" && slices.ContainsFunc(outdated, func(c runtime.Container) bool { return c.Running }) {
		r.finalSnapshot(ctx, record, gate)
	} else if gate != "" {
		r.pruneSnapshots(ctx, record)
	}
	// Checked again with no container of the tenant left to change them.
	if err := volumeDirs(dataPath, volumes, uid, gid); err != nil {
		return "", true, err
	}
	// From here on the data belongs to the new image: a later pass must not
	// snapshot again for the same change. Not before: a pass that failed to
	// remove the old container leaves the next one to snapshot once it has
	// stopped.
	if err := r.store.Observe(tenant.Name, tenant.Image); err != nil {
		return "", true, fmt.Errorf("record the data's image: %w", err)
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

// imageSnapshots records on each snapshot the image whose data it holds,
// puts a tenant's data back to the newest snapshot holding an image's,
// destroying the newer ones, and prunes keeping that one; ZFS can.
// RestoreSnapshot reports volume.ErrNoSnapshot when there is none.
type imageSnapshots interface {
	SnapshotImage(ctx context.Context, tenant, reason, image string) (string, error)
	RestoreSnapshot(ctx context.Context, tenant, reason, image string) (string, error)
	PruneKeeping(ctx context.Context, tenant, reason string, keep int, image string) error
}

// snapshotImage snapshots a tenant's data, that of image, as its image
// changes.
func (r *Reconciler) snapshotImage(ctx context.Context, tenant, image string) (string, error) {
	if snapshots, ok := r.volumes.(imageSnapshots); ok {
		return snapshots.SnapshotImage(ctx, tenant, "image-change", image)
	}
	return r.volumes.Snapshot(ctx, tenant, "image-change")
}

// pruneSnapshots keeps a tenant's newest snapshotsKept snapshots taken as
// its image changed, and the newest of the image its data would go back
// to, however old: its pending restore's, or the revision before its
// rollout's.
func (r *Reconciler) pruneSnapshots(ctx context.Context, record store.Record) {
	tenant := record.Spec.Name
	var err error
	if snapshots, ok := r.volumes.(imageSnapshots); ok {
		keep := record.Restore
		if keep == "" {
			keep = record.BeforeImage
		}
		err = snapshots.PruneKeeping(ctx, tenant, "image-change", snapshotsKept, keep)
	} else {
		err = r.volumes.Prune(ctx, tenant, "image-change", snapshotsKept)
	}
	if err != nil {
		r.log.Warn("prune old snapshots", "tenant", tenant, "error", err)
	}
}

// restoreData takes a tenant's data back to the snapshot taken as its image
// changed, as its Restore asks: someone took it back from its rollout with
// its data, to the image that owned the data before, losing what the newer
// one wrote. Its containers go first; the old image then starts on the data
// as it left it. Until then the edges hold its requests.
func (r *Reconciler) restoreData(ctx context.Context, record store.Record, existing []runtime.Container) error {
	tenant := record.Spec.Name
	if err := r.removeContainers(ctx, existing, "replaced"); err != nil {
		return err
	}
	if record.DataImage != record.Restore {
		restorer, ok := r.volumes.(imageSnapshots)
		snapshot, err := "", volume.ErrSnapshotsUnsupported
		if ok {
			snapshot, err = restorer.RestoreSnapshot(ctx, tenant, "image-change", record.Restore)
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

// imageVolumes returns the directories below the tenant's data that the
// volumes its image declares (VOLUME) are bound from: the engine would
// otherwise keep each in an anonymous volume, outside the data, with no
// snapshots, copies, backups or disk limit, and destroy it with the
// container. One at the data path itself or a tmpfs needs none; one outside
// the data is refused.
func (r *Reconciler) imageVolumes(ctx context.Context, tenant spec.Tenant) ([]string, error) {
	declared, err := r.runtime.ImageVolumes(ctx, tenant.Image)
	if err != nil {
		return nil, fmt.Errorf("read the image's volumes: %w", err)
	}
	var dirs []string
	for _, declaredAt := range declared {
		target := path.Clean(declaredAt)
		if _, tmpfs := tenant.Tmpfs[target]; tmpfs || target == tenant.DataPath {
			continue
		}
		dir, inside := strings.CutPrefix(target, tenant.DataPath+"/")
		if !inside || !path.IsAbs(target) {
			return nil, fmt.Errorf("%w: image %s declares a volume at %s, outside the tenant's data (dataPath %s), where its data would be lost with the container; set dataPath to %s, or to a directory it is in", runtime.ErrUncoveredVolume, tenant.Image, declaredAt, tenant.DataPath, target)
		}
		dirs = append(dirs, dir)
	}
	return dirs, nil
}

// volumeDirs makes the directories below data that image volumes are bound
// from, owned by the app's user, and refuses one that is not a directory
// of its own: the engine follows a symlink in a mount's source, and one the
// app left there could lead anywhere on the host.
func volumeDirs(data string, dirs []string, uid, gid int) error {
	if len(dirs) == 0 {
		return nil
	}
	root, err := os.OpenRoot(data)
	if err != nil {
		return fmt.Errorf("image volumes: %w", err)
	}
	defer root.Close()
	for _, dir := range dirs {
		parts := strings.Split(dir, "/")
		for i := range parts {
			name := filepath.Join(parts[:i+1]...)
			info, err := root.Lstat(name)
			if errors.Is(err, fs.ErrNotExist) {
				if err := root.Mkdir(name, 0o700); err != nil {
					return fmt.Errorf("image volume %s: %w", dir, err)
				}
				if err := root.Lchown(name, uid, gid); err != nil {
					return fmt.Errorf("image volume %s: %w", dir, err)
				}
				continue
			}
			if err != nil {
				return fmt.Errorf("image volume %s: %w", dir, err)
			}
			if !info.IsDir() {
				return fmt.Errorf("image volume %s: %s in the tenant's data is not a directory (a symlink is not followed); move it away, and the container is made at the next pass", dir, name)
			}
		}
	}
	return nil
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
func (r *Reconciler) finalSnapshot(ctx context.Context, record store.Record, gate string) {
	tenant := record.Spec.Name
	name, err := r.snapshotImage(ctx, tenant, record.DataImage)
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
	r.pruneSnapshots(ctx, record)
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

// removeContainers stops and removes containers. With a reason, "replaced"
// or "stopped", the last lines of the tenant's container are kept first
// (previous.go), with its exit if it had exited; with none, nothing is
// kept: the tenant leaves the node, or is purged.
func (r *Reconciler) removeContainers(ctx context.Context, containers []runtime.Container, why string) error {
	var errs []error
	for _, container := range containers {
		var exit runtime.ContainerState
		if why != "" && !container.Running {
			exit, _ = r.runtime.Inspect(ctx, container.ID)
		}
		if container.Running {
			if err := r.runtime.Stop(ctx, container.ID, r.config.StopTimeout); err != nil {
				errs = append(errs, fmt.Errorf("stop %s: %w", container.Name, err))
				continue
			}
		}
		if tenant := container.Labels[runtime.LabelTenant]; why != "" && tenant != "" {
			kept := Previous{Why: why}
			// A container that had exited before it was replaced: its
			// lines end at that exit.
			if exit.Pid == 0 && !exit.ExitedAt.IsZero() && exit.ExitKnown && exit.ExitCode != 143 {
				kept.ExitedAt, kept.ExitKnown, kept.ExitCode, kept.OOMKilled = exit.ExitedAt.UTC(), true, exit.ExitCode, exit.OOMKilled
			}
			r.keepPrevious(ctx, tenant, container, kept)
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
