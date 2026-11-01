// Package move moves a tenant, with its data, from the node that runs it to
// another node of the cluster. The data goes over while the tenant runs;
// then the tenant stops, what changed meanwhile follows, and the other node
// starts it. Requests wait meanwhile, on every edge, so none fails: the
// tenant is away for seconds, as long as its last changes take to send.
package move

import (
	"context"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"sync"
	"time"

	"github.com/PowerPenguini/murder/internal/reconcile"
	"github.com/PowerPenguini/murder/internal/store"
	"github.com/PowerPenguini/murder/internal/volume"
)

// Peer reaches the other node of a move.
type Peer interface {
	// Receive streams the tenant's data to node.
	Receive(ctx context.Context, node, tenant string, incremental bool, data io.Reader) error
	// Discard drops a copy of the tenant node received for a move that
	// failed.
	Discard(ctx context.Context, node, tenant string) error
	// Status is the tenant's status on node.
	Status(ctx context.Context, node, tenant string) (reconcile.Status, error)
}

// Releaser stops a tenant's container between reconcile passes.
type Releaser interface {
	Release(ctx context.Context, tenant string) error
}

// Mover moves tenants away from this node.
type Mover struct {
	Store      store.Store
	Reconciler Releaser
	Volumes    volume.Volumes
	Peer       Peer
	Log        *slog.Logger
	// CatchUp applies here what the cluster has applied, before a failed
	// move decides what to undo.
	CatchUp func() error
	// Healthy bounds the wait for the tenant on its new node.
	Healthy time.Duration
	// Away bounds the part of a move in which the tenant runs nowhere: a
	// target that hangs then does not keep it down; the move is undone.
	Away time.Duration

	mu     sync.Mutex
	active map[string]bool
}

// ErrUnhealthy reports a tenant that moved but is not healthy on its new
// node; its data stays on the old node too.
var ErrUnhealthy = errors.New("the tenant moved but is not healthy on its new node")

// Move moves tenant from this node to target. Until the tenant is the
// target's, a failure leaves it here, running as before; what the state
// says decides that, not what a failed call returned.
func (m *Mover) Move(ctx context.Context, tenant, target, actor string) (err error) {
	if !m.begin(tenant) {
		return fmt.Errorf("%w: tenant %s is moving already", store.ErrConflict, tenant)
	}
	defer m.end(tenant)
	record, err := m.Store.StartMove(tenant, target, actor)
	if err != nil {
		return err
	}
	handedOver := false
	defer func() {
		if err != nil && !handedOver {
			err = errors.Join(err, m.undo(context.WithoutCancel(ctx), tenant, record.Node, target, actor))
		}
	}()
	log := m.Log.With("tenant", tenant, "to", target)
	// A tenant that never ran here has no data to send.
	hasData, err := m.Volumes.Exists(ctx, tenant)
	if err != nil {
		return err
	}
	since := ""
	if hasData {
		start := time.Now()
		if since, err = m.send(ctx, tenant, target, ""); err != nil {
			return fmt.Errorf("copy the data: %w", err)
		}
		log.Info("copied the tenant's data to its new node", "took", time.Since(start).Round(time.Millisecond))
	}
	// From here on the tenant runs nowhere, and requests wait for it.
	limit := m.Away
	if limit == 0 {
		limit = 5 * time.Minute
	}
	awayCtx, cancel := context.WithTimeout(ctx, limit)
	defer cancel()
	if !record.Archived() {
		if err := m.Store.HoldMove(tenant); err != nil {
			return err
		}
		if err := m.Reconciler.Release(awayCtx, tenant); err != nil {
			return fmt.Errorf("stop the tenant: %w", err)
		}
	}
	away := time.Now()
	if hasData {
		if _, err := m.send(awayCtx, tenant, target, since); err != nil {
			return fmt.Errorf("copy the last changes: %w", err)
		}
	}
	if _, err := m.Store.FinishMove(tenant, actor); err != nil {
		// The handover may have been made all the same.
		if m.settled(tenant) != target {
			return err
		}
		log.Warn("the handover reported a failure but was made", "error", err)
	}
	handedOver = true
	log.Info("the tenant is its new node's", "away", time.Since(away).Round(time.Millisecond))
	// The rest is the move's own: a client that stops waiting for it does
	// not cut the wait for the tenant's health short.
	ctx = context.WithoutCancel(ctx)
	if !record.Archived() {
		if err := m.waitHealthy(ctx, tenant, target); err != nil {
			// Waiting longer helps no one: requests get answers again.
			return errors.Join(err, m.Store.ReleaseHold(tenant))
		}
	}
	// Its data here is a copy no one needs now, unless the tenant came
	// back meanwhile.
	if node := m.settled(tenant); node == "" || node == m.Store.Node() {
		log.Warn("the moved tenant's data stays on this node: the tenant is this node's again, or its state is unknown")
		return nil
	}
	if moving, err := m.Store.Get(tenant); err == nil && moving.MoveTo == m.Store.Node() {
		log.Warn("the moved tenant's data stays on this node: the tenant is moving back")
		return nil
	}
	if err := m.Volumes.Remove(ctx, tenant); err != nil {
		log.Warn("remove the moved tenant's data from this node", "error", err)
	}
	return nil
}

// Moving reports whether a move of tenant away from this node is under
// way here.
func (m *Mover) Moving(tenant string) bool {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.active[tenant]
}

func (m *Mover) begin(tenant string) bool {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.active == nil {
		m.active = map[string]bool{}
	}
	if m.active[tenant] {
		return false
	}
	m.active[tenant] = true
	return true
}

func (m *Mover) end(tenant string) {
	m.mu.Lock()
	defer m.mu.Unlock()
	delete(m.active, tenant)
}

// settled is the node the state gives the tenant once this node has
// applied what the cluster has; "" if that is not known.
func (m *Mover) settled(tenant string) string {
	if m.CatchUp != nil && m.CatchUp() != nil {
		return ""
	}
	record, err := m.Store.Get(tenant)
	if err != nil {
		return ""
	}
	return record.Node
}

// undo leaves a tenant whose move failed on this node, and only once the
// state says it stays here is the target's partial copy dropped: a copy
// the target may yet run is never dropped.
func (m *Mover) undo(ctx context.Context, tenant, source, target, actor string) error {
	if m.CatchUp != nil {
		if err := m.CatchUp(); err != nil {
			return fmt.Errorf("the move's outcome is unknown, so nothing was undone (it is undone later): %w", err)
		}
	}
	record, err := m.Store.Get(tenant)
	if err != nil {
		return err
	}
	switch {
	case record.Node == target:
		return nil // it moved after all; the target keeps its copy
	case record.Node != source:
		return fmt.Errorf("tenant %s runs on node %s: nothing undone", tenant, record.Node)
	case record.MoveTo != "":
		if _, err := m.Store.CancelMove(tenant, actor); err != nil {
			return fmt.Errorf("cancel the move (it is undone later): %w", err)
		}
	}
	if err := m.Peer.Discard(ctx, target, tenant); err != nil {
		return fmt.Errorf("drop the target's partial copy: %w", err)
	}
	// Snapshots taken for the move are of no use now.
	if err := m.Volumes.Prune(ctx, tenant, "move", 1); err != nil {
		m.Log.Warn("prune the move's snapshots", "tenant", tenant, "error", err)
	}
	return nil
}

// Recover undoes the moves away from this node that no Move here is
// making, such as one an agent restart cut short: the tenant stays here.
func (m *Mover) Recover(ctx context.Context) error {
	var errs []error
	for _, record := range m.Store.List() {
		if record.Node != m.Store.Node() || record.MoveTo == "" || !m.begin(record.Spec.Name) {
			continue
		}
		m.Log.Warn("undoing a move cut short", "tenant", record.Spec.Name, "to", record.MoveTo)
		errs = append(errs, m.undo(ctx, record.Spec.Name, record.Node, record.MoveTo, "murder (recovery)"))
		m.end(record.Spec.Name)
	}
	return errors.Join(errs...)
}

// send streams the tenant's data to target, all of it or what changed
// since the snapshot since, and returns the snapshot it sent.
func (m *Mover) send(ctx context.Context, tenant, target, since string) (string, error) {
	reader, writer := io.Pipe()
	sent := make(chan error, 1)
	var snapshot string
	go func() {
		var err error
		snapshot, err = m.Volumes.Send(ctx, tenant, since, writer)
		_ = writer.CloseWithError(err)
		sent <- err
	}()
	received := m.Peer.Receive(ctx, target, tenant, since != "", reader)
	_ = reader.CloseWithError(received)
	if err := errors.Join(<-sent, received); err != nil {
		return "", err
	}
	return snapshot, nil
}

func (m *Mover) waitHealthy(ctx context.Context, tenant, target string) error {
	wait := m.Healthy
	if wait == 0 {
		wait = 5 * time.Minute
	}
	ctx, cancel := context.WithTimeout(ctx, wait)
	defer cancel()
	last := "not seen yet"
	for {
		status, err := m.Peer.Status(ctx, target, tenant)
		switch {
		case err != nil:
			last = err.Error()
		case status.Phase == reconcile.PhaseRunning && status.Healthy, status.Phase == reconcile.PhaseSuspended:
			return nil
		default:
			last = status.Phase + ": " + status.Message
		}
		select {
		case <-ctx.Done():
			return fmt.Errorf("%w (node %s: %s); its data stays on this node too", ErrUnhealthy, target, last)
		case <-time.After(time.Second):
		}
	}
}
