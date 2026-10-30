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
	// Healthy bounds the wait for the tenant on its new node.
	Healthy time.Duration
}

// ErrUnhealthy reports a tenant that moved but is not healthy on its new
// node; its data stays on the old node too.
var ErrUnhealthy = errors.New("the tenant moved but is not healthy on its new node")

// Move moves tenant from this node to target. Until the tenant is the
// target's, a failure leaves it here, running as before.
func (m *Mover) Move(ctx context.Context, tenant, target, actor string) (err error) {
	record, err := m.Store.StartMove(tenant, target, actor)
	if err != nil {
		return err
	}
	moved := false
	defer func() {
		if moved || err == nil {
			return
		}
		// Here it stays: the reconciler starts it again.
		cancelled := m.Store.CancelMove(tenant, actor)
		discarded := m.Peer.Discard(context.WithoutCancel(ctx), target, tenant)
		err = errors.Join(err, cancelled, discarded)
	}()
	log := m.Log.With("tenant", tenant, "to", target)
	// Everything, while the tenant runs.
	start := time.Now()
	since, err := m.send(ctx, tenant, target, "")
	if err != nil {
		return fmt.Errorf("copy the data: %w", err)
	}
	log.Info("copied the tenant's data to its new node", "took", time.Since(start).Round(time.Millisecond))
	// From here on the tenant runs nowhere, and requests wait for it.
	if !record.Archived() {
		if err := m.Store.HoldMove(tenant); err != nil {
			return err
		}
		if err := m.Reconciler.Release(ctx, tenant); err != nil {
			return fmt.Errorf("stop the tenant: %w", err)
		}
	}
	away := time.Now()
	if _, err := m.send(ctx, tenant, target, since); err != nil {
		return fmt.Errorf("copy the last changes: %w", err)
	}
	if _, err := m.Store.FinishMove(tenant, actor); err != nil {
		return err
	}
	moved = true
	log.Info("the tenant is its new node's", "away", time.Since(away).Round(time.Millisecond))
	if !record.Archived() {
		if err := m.waitHealthy(ctx, tenant, target); err != nil {
			return err
		}
	}
	// Its data here is a copy no one needs now.
	if err := m.Volumes.Remove(ctx, tenant); err != nil {
		log.Warn("remove the moved tenant's data from this node", "error", err)
	}
	return nil
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
