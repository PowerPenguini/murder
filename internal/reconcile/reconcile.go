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
	PhasePending   = "pending"
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
	containers, err := r.runtime.List(ctx)
	if err != nil {
		return fmt.Errorf("list containers: %w", err)
	}
	byTenant := map[string][]runtime.Container{}
	for _, container := range containers {
		tenant := container.Labels[runtime.LabelTenant]
		byTenant[tenant] = append(byTenant[tenant], container)
	}
	records := r.store.List()
	wanted := map[string]bool{}
	statuses := map[string]Status{}
	var errs []error
	for _, record := range records {
		name := record.Spec.Name
		wanted[name] = true
		status, err := r.reconcileTenant(ctx, record, byTenant[name])
		status.Generation = record.Generation
		status.CheckedAt = time.Now().UTC()
		if err != nil {
			status.Phase, status.Healthy, status.Message = PhaseFailed, false, err.Error()
			errs = append(errs, fmt.Errorf("tenant %s: %w", name, err))
		}
		statuses[name] = status
	}
	for _, tenant := range slices.Sorted(maps.Keys(byTenant)) {
		if wanted[tenant] {
			continue
		}
		if err := r.removeContainers(ctx, byTenant[tenant]); err != nil {
			errs = append(errs, fmt.Errorf("remove containers of deleted tenant %q: %w", tenant, err))
			continue
		}
		if tenant != "" {
			if err := r.runtime.RemoveNetwork(ctx, NetworkName(tenant)); err != nil {
				errs = append(errs, fmt.Errorf("remove network of deleted tenant %q: %w", tenant, err))
			}
		}
		r.log.Info("removed deleted tenant's containers", "tenant", tenant)
	}
	r.mu.Lock()
	r.statuses = statuses
	r.mu.Unlock()
	return errors.Join(errs...)
}

func (r *Reconciler) reconcileTenant(ctx context.Context, record store.Record, existing []runtime.Container) (Status, error) {
	tenant := record.Spec
	if tenant.Suspended {
		if err := r.removeContainers(ctx, existing); err != nil {
			return Status{}, err
		}
		return Status{Phase: PhaseSuspended}, nil
	}
	dataPath, err := r.volumes.Ensure(ctx, tenant.Name, tenant.DataUID, tenant.DataGID)
	if err != nil {
		return Status{}, fmt.Errorf("data volume: %w", err)
	}
	subnet, err := r.runtime.EnsureNetwork(ctx, NetworkName(tenant.Name), map[string]string{runtime.LabelManaged: "true", runtime.LabelTenant: tenant.Name})
	if err != nil {
		return Status{}, fmt.Errorf("network: %w", err)
	}
	desired, err := r.containerSpec(record, dataPath, subnet)
	if err != nil {
		return Status{}, err
	}
	var current *runtime.Container
	var extra []runtime.Container
	for i := range existing {
		if current == nil && existing[i].Labels[runtime.LabelHash] == desired.Labels[runtime.LabelHash] {
			current = &existing[i]
			continue
		}
		extra = append(extra, existing[i])
	}
	if len(extra) > 0 {
		if current == nil && imageChanged(extra, tenant.Image) {
			r.snapshotBeforeChange(ctx, tenant.Name)
		}
		if err := r.removeContainers(ctx, extra); err != nil {
			return Status{}, err
		}
	}
	if current == nil {
		if err := r.ensureImage(ctx, tenant.Image); err != nil {
			return Status{}, err
		}
		id, err := r.runtime.Create(ctx, desired)
		if err != nil {
			return Status{}, err
		}
		if err := r.runtime.Start(ctx, id); err != nil {
			return Status{}, fmt.Errorf("start: %w", err)
		}
		r.log.Info("started tenant container", "tenant", tenant.Name, "container", id, "generation", record.Generation)
		current = &runtime.Container{ID: id, Running: true}
	} else if !current.Running {
		if err := r.runtime.Start(ctx, current.ID); err != nil {
			return Status{}, fmt.Errorf("start: %w", err)
		}
		r.log.Info("restarted stopped tenant container", "tenant", tenant.Name, "container", current.ID)
	}
	status := Status{Phase: PhaseRunning, ContainerID: current.ID}
	status.Healthy, status.Message = r.checkHealth(ctx, record)
	return status, nil
}

func imageChanged(containers []runtime.Container, image string) bool {
	for _, container := range containers {
		if container.Labels[runtime.LabelImage] != image {
			return true
		}
	}
	return false
}

// snapshotBeforeChange keeps the data as the old image left it, so a failed
// upgrade can be rolled back. Drivers without snapshots skip it.
func (r *Reconciler) snapshotBeforeChange(ctx context.Context, tenant string) {
	name, err := r.volumes.Snapshot(ctx, tenant, "image-change")
	switch {
	case errors.Is(err, volume.ErrSnapshotsUnsupported):
	case err != nil:
		r.log.Error("snapshot before image change", "tenant", tenant, "error", err)
	default:
		r.log.Info("snapshot before image change", "tenant", tenant, "snapshot", name)
	}
}

func (r *Reconciler) ensureImage(ctx context.Context, image string) error {
	present, err := r.runtime.HasImage(ctx, image)
	if err != nil {
		return fmt.Errorf("inspect image: %w", err)
	}
	if present {
		return nil
	}
	if err := r.runtime.PullImage(ctx, image); err != nil {
		return err
	}
	return nil
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

// Host and URL are a tenant's public address.
func (r *Reconciler) Host(tenant string) string { return tenant + "." + r.config.BaseDomain }
func (r *Reconciler) URL(tenant string) string  { return r.config.Scheme + "://" + r.Host(tenant) }

func (r *Reconciler) containerSpec(record store.Record, dataPath, subnet string) (runtime.ContainerSpec, error) {
	tenant := record.Spec
	env := tenant.Render(spec.Values{Host: r.Host(tenant.Name), URL: r.URL(tenant.Name), Network: subnet})
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
	desired := runtime.ContainerSpec{
		Name:          ContainerName(tenant.Name),
		Image:         tenant.Image,
		Env:           pairs,
		User:          tenant.User,
		Network:       NetworkName(tenant.Name),
		ContainerPort: tenant.Port,
		HostPort:      record.HostPort,
		Mounts:        []runtime.Mount{{Source: dataPath, Target: tenant.DataPath}},
		MemoryBytes:   int64(limits.MemoryMB) << 20,
		NanoCPUs:      int64(limits.CPUs * 1e9),
		PIDs:          int64(limits.PIDs),
		ReadOnlyRoot:  tenant.ReadOnlyRoot,
		Tmpfs:         maps.Clone(tenant.Tmpfs),
		Labels: map[string]string{
			runtime.LabelManaged: "true",
			runtime.LabelTenant:  tenant.Name,
			runtime.LabelImage:   tenant.Image,
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
	target := "http://127.0.0.1:" + strconv.Itoa(record.HostPort) + record.Spec.HealthPath
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

// Purge deletes a removed tenant's data once no container of it remains.
func (r *Reconciler) Purge(ctx context.Context, tenant string) error {
	if _, err := r.store.Get(tenant); err == nil {
		return fmt.Errorf("tenant %s still exists; delete it first", tenant)
	} else if !errors.Is(err, store.ErrNotFound) {
		return err
	}
	// Another tenant's failure must not block the purge; what matters is
	// that this tenant's containers are gone, checked below.
	if err := r.Reconcile(ctx); err != nil {
		r.log.Warn("reconcile before purge", "tenant", tenant, "error", err)
	}
	containers, err := r.runtime.List(ctx)
	if err != nil {
		return err
	}
	for _, container := range containers {
		if container.Labels[runtime.LabelTenant] == tenant {
			return fmt.Errorf("container %s of tenant %s still exists", container.Name, tenant)
		}
	}
	return r.volumes.Remove(ctx, tenant)
}
