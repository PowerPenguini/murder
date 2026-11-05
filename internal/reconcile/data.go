package reconcile

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/PowerPenguini/murder/internal/runtime"
	"github.com/PowerPenguini/murder/internal/store"
)

// replicaVolumes are volumes that keep copies of other nodes' tenants.
type replicaVolumes interface {
	IsReplica(ctx context.Context, tenant string) (bool, error)
	DropReplica(ctx context.Context, tenant string) error
}

// ErrDataMissing refuses to run a tenant that ran before on new, empty
// data: its data should be here, and is not.
var ErrDataMissing = errors.New("the tenant ran before, but its data is not on this node")

// checkData refuses a tenant that ran before (its data has an image) whose
// data is not here, rather than run it on an empty data directory: a copy
// that should have taken over, but holds nothing. A copy here of a tenant
// that never ran is of an earlier tenant of the same name, purged: it goes.
func (r *Reconciler) checkData(ctx context.Context, record store.Record) error {
	exists, err := r.volumes.Exists(ctx, record.Spec.Name)
	if err != nil {
		return fmt.Errorf("data volume: %w", err)
	}
	if !exists && record.DataImage != "" {
		return fmt.Errorf("%w: a restore that failed half way may have put it aside (zfs list | grep murder-replaced-%s); otherwise restore it (murder backups restore %s), or purge the tenant", ErrDataMissing, record.Spec.Name, record.Spec.Name)
	}
	replicas, ok := r.volumes.(replicaVolumes)
	if !exists || record.DataImage != "" || !ok {
		return nil
	}
	if replica, err := replicas.IsReplica(ctx, record.Spec.Name); err != nil {
		return fmt.Errorf("data volume: %w", err)
	} else if replica {
		r.log.Warn("dropping a copy of an earlier tenant of the same name", "tenant", record.Spec.Name)
		return replicas.DropReplica(ctx, record.Spec.Name)
	}
	return nil
}

// ErrElsewhere refuses work on a tenant another node runs: it is
// store.ErrElsewhere, which other packages refuse with too.
var ErrElsewhere = store.ErrElsewhere

// DataExists reports whether a tenant name still has data on this node.
func (r *Reconciler) DataExists(ctx context.Context, tenant string) (bool, error) {
	return r.volumes.Exists(ctx, tenant)
}

// purgeTime bounds a purge once it has begun.
const purgeTime = 10 * time.Minute

// Purge deletes an archived tenant for good: its data, snapshots and saved
// secrets. It also cleans up data no archive entry accounts for. A live
// tenant must be archived first. A caller that gives up (ctx) before the
// deletion begins, while waiting for a pass too, purges nothing; once
// begun, the deletion runs to its end, so nothing is left half deleted.
func (r *Reconciler) Purge(ctx context.Context, name, actor string) error {
	if err := r.lockPass(ctx); err != nil {
		return err
	}
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
	if ctx.Err() != nil {
		return fmt.Errorf("%w: the purge was given up before it began; nothing was deleted", context.Cause(ctx))
	}
	ctx, cancel := context.WithTimeout(context.WithoutCancel(ctx), purgeTime)
	defer cancel()
	if err := r.clearRuntime(ctx, name); err != nil {
		return err
	}
	if err := r.volumes.Remove(ctx, name); err != nil {
		return err
	}
	// The store takes no context: a write waiting there for the writer's
	// lock is not given up either.
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
	if err := r.removeContainers(ctx, own, ""); err != nil {
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
