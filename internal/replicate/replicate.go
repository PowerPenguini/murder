// Package replicate keeps a copy of every tenant's data on a second node,
// its replica, to run the tenant from if its own node fails. The node that
// runs a tenant snapshots its data every few seconds and sends what changed
// since the copy's newest snapshot; the replica's node takes it into a
// dataset that is never mounted, and refuses data from any node but the
// one that runs the tenant, so a node that lost its tenants never writes
// over the copy that took them over.
package replicate

import (
	"context"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"maps"
	"slices"
	"strings"
	"sync"
	"time"

	"github.com/PowerPenguini/murder/internal/metrics"
	"github.com/PowerPenguini/murder/internal/store"
	"github.com/PowerPenguini/murder/internal/volume"
)

// Volumes is what replication does with this node's data.
type Volumes interface {
	volume.Replicas
	Snapshot(ctx context.Context, tenant, reason string) (string, error)
	Prune(ctx context.Context, tenant, reason string, keep int) error
}

// Peer reaches the node that keeps a tenant's copy.
type Peer interface {
	// Snapshots lists the snapshots of the copy node keeps, oldest first.
	Snapshots(ctx context.Context, node, tenant string) ([]string, error)
	// Send streams the tenant's data to the copy node keeps: whole, or
	// what changed since the copy's newest snapshot.
	Send(ctx context.Context, node, tenant string, whole bool, data io.Reader) error
}

// Replicator sends this node's tenants to their replicas, and keeps the
// copies this node holds for other nodes as the state says.
type Replicator struct {
	Store   store.Store
	Volumes Volumes
	Peer    Peer
	Log     *slog.Logger
	// CatchUp applies here what the cluster has applied; copies and data
	// are only dropped or put aside on a state no older than the cluster's.
	CatchUp func() error
	// Moving reports a tenant a move on this node is handling: its data is
	// the move's to remove.
	Moving func(tenant string) bool
	// Leader reports whether this node leads the cluster; the leader gives
	// tenants their replicas.
	Leader func() bool
	// Every is how often each tenant's changes are sent; 15 s if zero.
	Every time.Duration
	// Parallel bounds the tenants sent at once; 4 if zero.
	Parallel int
	// Keep is how many replica snapshots each side keeps; 3 if zero.
	Keep int
	Now  func() time.Time

	mu sync.Mutex
	// synced is when each tenant's copy last matched its data, failing
	// why its last send failed.
	synced  map[string]time.Time
	failing map[string]string
	// caughtUp is set, by Run only, once this node first caught up.
	caughtUp bool
}

// ReplicaReason names the snapshots replication takes.
const ReplicaReason = volume.ReplicaReason

func (r *Replicator) now() time.Time {
	if r.Now != nil {
		return r.Now()
	}
	return time.Now()
}

func (r *Replicator) keep() int {
	if r.Keep > 0 {
		return r.Keep
	}
	return 3
}

// Run replicates until ctx ends.
func (r *Replicator) Run(ctx context.Context) {
	every := r.Every
	if every == 0 {
		every = 15 * time.Second
	}
	for {
		r.step(ctx)
		select {
		case <-ctx.Done():
			return
		case <-time.After(every):
		}
	}
}

// step is one round of Run. Nothing is done before this node has once
// caught up with the cluster: a node back from the dead would otherwise
// send, from its old state, tenants that have since run elsewhere.
func (r *Replicator) step(ctx context.Context) {
	if !r.caughtUp {
		if r.CatchUp != nil {
			if err := r.CatchUp(); err != nil {
				r.Log.Debug("wait for the cluster before replicating", "error", err)
				return
			}
		}
		r.caughtUp = true
	}
	if r.Leader != nil && r.Leader() {
		if placed, err := r.Store.PlaceReplicas("murder"); err != nil {
			r.Log.Warn("give tenants their replicas", "error", err)
		} else if placed > 0 {
			r.Log.Info("gave tenants their replicas", "tenants", placed)
		}
	}
	r.Pass(ctx)
	if err := r.Sweep(ctx); err != nil {
		r.Log.Warn("tidy the copies on this node", "error", err)
	}
}

// Pass sends every tenant of this node with a replica what changed since
// its copy's newest snapshot. A tenant moving to its replica's node waits:
// the move brings that copy up to date.
func (r *Replicator) Pass(ctx context.Context) {
	var tenants []store.Record
	current := map[string]bool{}
	for _, record := range r.Store.List() {
		if record.Node == r.Store.Node() && record.Replica != "" && record.Replica != record.Node && record.MoveTo != record.Replica {
			tenants = append(tenants, record)
			current[record.Spec.Name] = true
		}
	}
	// Tenants that went, or whose copy is elsewhere's to report, leave
	// the metrics.
	r.mu.Lock()
	for name := range r.synced {
		if !current[name] {
			delete(r.synced, name)
		}
	}
	for name := range r.failing {
		if !current[name] {
			delete(r.failing, name)
		}
	}
	r.mu.Unlock()
	parallel := r.Parallel
	if parallel <= 0 {
		parallel = 4
	}
	slots := make(chan struct{}, parallel)
	var wg sync.WaitGroup
	for _, record := range tenants {
		slots <- struct{}{}
		wg.Add(1)
		go func() {
			defer func() { <-slots; wg.Done() }()
			err := r.Replicate(ctx, record.Spec.Name, record.Replica)
			// Once the copy holds the whole data, it may take over.
			if err == nil && !record.ReplicaReady {
				err = r.Store.ReplicaReady(record.Spec.Name, record.Replica)
			}
			r.mu.Lock()
			if r.failing == nil {
				r.failing = map[string]string{}
			}
			if err != nil {
				r.failing[record.Spec.Name] = err.Error()
			} else {
				delete(r.failing, record.Spec.Name)
			}
			r.mu.Unlock()
			if err != nil && ctx.Err() == nil {
				r.Log.Warn("send a tenant's changes to its replica", "tenant", record.Spec.Name, "replica", record.Replica, "error", err)
			}
		}()
	}
	wg.Wait()
}

// Replicate brings the copy of tenant on node up to date with its data:
// the changes since the newest snapshot both have, or the whole data if
// they have none in common. Nothing is sent when nothing changed.
func (r *Replicator) Replicate(ctx context.Context, tenant, node string) error {
	snapshots, err := r.Volumes.Snapshots(ctx, tenant, ReplicaReason)
	if err != nil {
		return err
	}
	theirs, err := r.Peer.Snapshots(ctx, node, tenant)
	if err != nil {
		return fmt.Errorf("ask node %s for its copy: %w", node, err)
	}
	base := NewestCommon(snapshots, theirs)
	if base != "" {
		written, err := r.Volumes.Written(ctx, tenant, base)
		if err != nil {
			return err
		}
		if written == 0 {
			r.markSynced(tenant, r.now())
			return nil
		}
	}
	taken := r.now()
	full, err := r.Volumes.Snapshot(ctx, tenant, ReplicaReason)
	if err != nil {
		return err
	}
	_, snapshot, _ := strings.Cut(full, "@")
	if err := r.send(ctx, tenant, node, base, snapshot); err != nil {
		// The snapshot goes, so failures never push the copy's base out of
		// the snapshots kept.
		if destroyErr := r.Volumes.DestroySnapshot(context.WithoutCancel(ctx), tenant, snapshot); destroyErr != nil {
			err = errors.Join(err, destroyErr)
		}
		return err
	}
	r.markSynced(tenant, taken)
	if err := r.Volumes.Prune(ctx, tenant, ReplicaReason, r.keep()); err != nil {
		r.Log.Warn("prune replica snapshots", "tenant", tenant, "error", err)
	}
	return nil
}

// NewestCommon is the newest of the snapshots ours and theirs both list,
// oldest first; "" if none.
func NewestCommon(ours, theirs []string) string {
	for i := len(theirs) - 1; i >= 0; i-- {
		if slices.Contains(ours, theirs[i]) {
			return theirs[i]
		}
	}
	return ""
}

func (r *Replicator) send(ctx context.Context, tenant, node, base, snapshot string) error {
	reader, writer := io.Pipe()
	sent := make(chan error, 1)
	go func() {
		err := r.Volumes.SendSnapshot(ctx, tenant, base, snapshot, writer)
		_ = writer.CloseWithError(err)
		sent <- err
	}()
	received := r.Peer.Send(ctx, node, tenant, base == "", reader)
	_ = reader.CloseWithError(received)
	return errors.Join(<-sent, received)
}

func (r *Replicator) markSynced(tenant string, at time.Time) {
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.synced == nil {
		r.synced = map[string]time.Time{}
	}
	if at.After(r.synced[tenant]) {
		r.synced[tenant] = at
	}
}

// Synced is when the copy of tenant last matched its data, as far as this
// node knows; zero if not since the agent started.
func (r *Replicator) Synced(tenant string) time.Time {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.synced[tenant]
}

// Sweep drops the copies this node keeps of tenants it is no longer the
// replica of, or that are gone, and puts aside the data of tenants it no
// longer runs, as after its tenants failed over to their replicas while
// it was down: that data holds the writes the failover lost, so it is kept.
// It acts only on a state caught up with the cluster's.
func (r *Replicator) Sweep(ctx context.Context) error {
	if r.CatchUp != nil {
		if err := r.CatchUp(); err != nil {
			return fmt.Errorf("the state may be behind the cluster's, so nothing is tidied: %w", err)
		}
	}
	datasets, err := r.Volumes.Datasets(ctx)
	if err != nil {
		return err
	}
	self := r.Store.Node()
	var errs []error
	for _, dataset := range datasets {
		name := dataset.Tenant
		record, err := r.Store.Get(name)
		known := err == nil
		if err != nil && !errors.Is(err, store.ErrNotFound) {
			errs = append(errs, err)
			continue
		}
		if known && (record.Node == self || record.MoveTo == self) {
			continue // this node's data, or a copy taking over
		}
		if r.Moving != nil && r.Moving(name) {
			continue
		}
		// Data a move left unmounted here, which it removes itself if the
		// move is undone.
		if !dataset.Replica && !dataset.Mounted {
			continue
		}
		if dataset.Replica {
			if known && record.Replica == self {
				continue
			}
			if err := r.Volumes.DropReplica(ctx, name); err != nil {
				errs = append(errs, err)
				continue
			}
			r.Log.Info("dropped a copy this node no longer keeps", "tenant", name)
			continue
		}
		why := "elsewhere"
		if !known {
			why = "purged"
		}
		aside, err := r.Volumes.PutAside(ctx, name, why)
		if err != nil {
			errs = append(errs, fmt.Errorf("put aside the data of tenant %s, which runs elsewhere or is gone: %w", name, err))
			continue
		}
		r.Log.Warn("put aside the data of a tenant this node no longer runs; destroy it once it is not needed", "tenant", name, "dataset", aside)
	}
	return errors.Join(errs...)
}

// WriteMetrics writes how far behind each tenant's copy is.
func (r *Replicator) WriteMetrics(out *metrics.Writer) {
	r.mu.Lock()
	defer r.mu.Unlock()
	now := r.now()
	out.Family("murder_tenant_replica_lag_seconds", "gauge", "How long ago the tenant's copy on its replica last matched its data.")
	for _, name := range slices.Sorted(maps.Keys(r.synced)) {
		out.Sample("murder_tenant_replica_lag_seconds", now.Sub(r.synced[name]).Seconds(), "tenant", name)
	}
	out.Family("murder_tenant_replica_failing", "gauge", "1 if the last send of the tenant's changes to its replica failed.")
	names := slices.Sorted(maps.Keys(r.failing))
	for _, name := range slices.Sorted(maps.Keys(r.synced)) {
		if !slices.Contains(names, name) {
			out.Sample("murder_tenant_replica_failing", 0, "tenant", name)
		}
	}
	for _, name := range names {
		out.Sample("murder_tenant_replica_failing", 1, "tenant", name)
	}
}
