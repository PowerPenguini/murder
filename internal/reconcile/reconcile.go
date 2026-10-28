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

	"github.com/PowerPenguini/corral/internal/runtime"
	"github.com/PowerPenguini/corral/internal/secrets"
	"github.com/PowerPenguini/corral/internal/spec"
	"github.com/PowerPenguini/corral/internal/store"
	"github.com/PowerPenguini/corral/internal/volume"
)

// Phases a tenant can be in, as seen by the reconciler.
const (
	PhaseRunning   = "running"
	PhaseSuspended = "suspended"
	PhaseFailed    = "failed"
)

// Status is the observed state of one tenant.
type Status struct {
	Phase       string    `json:"phase"`
	Healthy     bool      `json:"healthy"`
	Message     string    `json:"message,omitempty"`
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

	// pass serializes reconcile passes and the removal of tenants.
	pass     sync.Mutex
	mu       sync.RWMutex
	statuses map[string]Status
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
	return &Reconciler{
		store: st, runtime: rt, volumes: volumes, key: key, config: config, log: log,
		health:   &http.Client{Timeout: config.HealthTimeout, CheckRedirect: func(*http.Request, []*http.Request) error { return http.ErrUseLastResponse }},
		statuses: map[string]Status{},
		backend:  Backend,
	}
}

// ContainerName and NetworkName name what Corral creates for a tenant.
func ContainerName(tenant string) string { return "corral-" + tenant }
func NetworkName(tenant string) string   { return "corral-" + tenant }

// Run reconciles every interval and whenever the store changes, until ctx
// ends.
func (r *Reconciler) Run(ctx context.Context, interval time.Duration) {
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
		}
	}
}

// Reconcile performs one full pass. A failing tenant does not stop the pass;
// every failure is returned joined.
func (r *Reconciler) Reconcile(ctx context.Context) error {
	r.pass.Lock()
	defer r.pass.Unlock()
	return r.reconcile(ctx)
}

func (r *Reconciler) reconcile(ctx context.Context) error {
	containers, err := r.runtime.List(ctx)
	if err != nil {
		return fmt.Errorf("list containers: %w", err)
	}
	byTenant := map[string][]runtime.Container{}
	for _, container := range containers {
		tenant := container.Labels[runtime.LabelTenant]
		byTenant[tenant] = append(byTenant[tenant], container)
	}
	var records []store.Record
	wanted := map[string]bool{}
	for _, record := range r.store.List() {
		if !record.Archived() {
			records = append(records, record)
			wanted[record.Spec.Name] = true
		}
	}
	var errs []error
	for _, tenant := range slices.Sorted(maps.Keys(byTenant)) {
		if wanted[tenant] {
			continue
		}
		if err := r.removeContainers(ctx, byTenant[tenant]); err != nil {
			errs = append(errs, fmt.Errorf("remove containers of %q, which no tenant wants: %w", tenant, err))
			continue
		}
		r.log.Info("removed the containers of a tenant no longer wanted", "tenant", tenant)
	}
	if err := r.removeOrphanNetworks(ctx, wanted); err != nil {
		errs = append(errs, err)
	}
	statuses := map[string]Status{}
	for _, record := range records {
		name := record.Spec.Name
		status, err := r.reconcileTenant(ctx, record, byTenant[name])
		status.Generation = record.Generation
		status.CheckedAt = time.Now().UTC()
		if err != nil {
			status.Phase, status.Healthy, status.Message = PhaseFailed, false, err.Error()
			errs = append(errs, fmt.Errorf("tenant %s: %w", name, err))
		}
		statuses[name] = status
	}
	r.mu.Lock()
	r.statuses = statuses
	r.mu.Unlock()
	return errors.Join(errs...)
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

func (r *Reconciler) reconcileTenant(ctx context.Context, record store.Record, existing []runtime.Container) (Status, error) {
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
	dataPath, mountedNow, err := r.volumes.Ensure(ctx, tenant.Name, uid, gid)
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
	var current *runtime.Container
	var outdated []runtime.Container
	for i := range existing {
		// A container started before its dataset was mounted runs on the
		// empty directory underneath; it is replaced, not adopted.
		if current == nil && !mountedNow && existing[i].Labels[runtime.LabelHash] == desired.Labels[runtime.LabelHash] {
			current = &existing[i]
			continue
		}
		outdated = append(outdated, existing[i])
	}
	if current != nil && current.Running && r.volumes.LateMounts() {
		serving, err := r.servesData(ctx, record, current.ID)
		switch {
		case errors.Is(err, runtime.ErrNotRunning):
			current.Running = false // it exited since List; started below
		case errors.Is(err, runtime.ErrNotFound):
			current = nil // it was removed since List; recreated below
		case err != nil:
			return Status{}, err
		case !serving:
			r.log.Warn("container does not see the tenant's data; replacing it", "tenant", tenant.Name, "container", current.ID)
			outdated = append(outdated, *current)
			current = nil
		}
	}
	if current == nil {
		id, err := r.replace(ctx, record, desired, outdated)
		if err != nil {
			return Status{}, err
		}
		current = &runtime.Container{ID: id, Running: true}
	} else {
		if err := r.removeContainers(ctx, outdated); err != nil {
			return Status{}, err
		}
		if !current.Running {
			if err := r.runtime.Start(ctx, current.ID); err != nil {
				return Status{}, fmt.Errorf("start: %w", err)
			}
			r.log.Info("restarted stopped tenant container", "tenant", tenant.Name, "container", current.ID)
		}
	}
	status := Status{Phase: PhaseRunning, ContainerID: current.ID}
	status.Healthy, status.Message = r.checkHealth(ctx, record)
	return status, nil
}

// servesData reports whether a running container sees the tenant's data.
func (r *Reconciler) servesData(ctx context.Context, record store.Record, id string) (bool, error) {
	pid, err := r.runtime.Pid(ctx, id)
	if err != nil {
		return false, fmt.Errorf("inspect container: %w", err)
	}
	serving, err := r.volumes.Serving(ctx, record.Spec.Name, pid)
	if err != nil {
		return false, fmt.Errorf("check the container's data mount: %w", err)
	}
	return serving, nil
}

// replace starts the desired container in place of outdated ones. Nothing is
// stopped until the new image is present and, after an image change, the
// data has been snapshotted. The snapshot is taken while the old container
// runs: apps are only onboarded once they recover from point-in-time
// snapshots, and a failed snapshot then blocks the change without an outage.
func (r *Reconciler) replace(ctx context.Context, record store.Record, desired runtime.ContainerSpec, outdated []runtime.Container) (string, error) {
	tenant := record.Spec
	if err := r.ensureImage(ctx, tenant.Image); err != nil {
		return "", err
	}
	if record.DataImage != "" && record.DataImage != tenant.Image {
		name, err := r.volumes.Snapshot(ctx, tenant.Name, "image-change")
		switch {
		case errors.Is(err, volume.ErrSnapshotsUnsupported):
		case err != nil:
			return "", fmt.Errorf("snapshot before the image change (the old container keeps running): %w", err)
		default:
			r.log.Info("snapshot before image change", "tenant", tenant.Name, "snapshot", name, "from", record.DataImage, "to", tenant.Image)
		}
	}
	// From here on the data belongs to the new image: a later pass must not
	// snapshot again for the same change.
	if err := r.store.Observe(tenant.Name, tenant.Image); err != nil {
		return "", fmt.Errorf("record the data's image: %w", err)
	}
	if err := r.removeContainers(ctx, outdated); err != nil {
		return "", err
	}
	id, err := r.runtime.Create(ctx, desired)
	if err != nil {
		return "", err
	}
	if err := r.runtime.Start(ctx, id); err != nil {
		return "", fmt.Errorf("start: %w", err)
	}
	r.log.Info("started tenant container", "tenant", tenant.Name, "container", id, "generation", record.Generation)
	return id, nil
}

func (r *Reconciler) ensureImage(ctx context.Context, image string) error {
	present, err := r.runtime.HasImage(ctx, image)
	if err != nil {
		return fmt.Errorf("inspect image: %w", err)
	}
	if present {
		return nil
	}
	return r.runtime.PullImage(ctx, image)
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
	limits := tenant.Limits
	if limits.MemoryMB == 0 {
		limits.MemoryMB = r.config.DefaultLimits.MemoryMB
	}
	if limits.CPUs == 0 {
		limits.CPUs = r.config.DefaultLimits.CPUs
	}
	if limits.PIDs == 0 {
		limits.PIDs = r.config.DefaultLimits.PIDs
	}
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
	hash, err := specHash(desired)
	if err != nil {
		return runtime.ContainerSpec{}, err
	}
	desired.Labels[runtime.LabelHash] = hash
	return desired, nil
}

// specHash fingerprints everything that shapes the container. A container
// whose label differs from the desired hash is replaced. JSON encodes maps
// in sorted key order, so equal specs always hash alike.
func specHash(desired runtime.ContainerSpec) (string, error) {
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
	record, err := r.store.Get(name)
	switch {
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
	return fn()
}
