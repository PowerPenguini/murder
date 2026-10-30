package main

import (
	"context"
	"errors"
	"fmt"
	"io"
	"sync"

	"github.com/PowerPenguini/murder/internal/cluster"
	"github.com/PowerPenguini/murder/internal/move"
	"github.com/PowerPenguini/murder/internal/reconcile"
	"github.com/PowerPenguini/murder/internal/store"
	"github.com/PowerPenguini/murder/internal/volume"
)

// agentMoves is the agent's part in moving tenants between nodes.
type agentMoves struct {
	store      *store.SQLite
	reconciler *reconcile.Reconciler
	volumes    volume.Volumes
	// mover and cluster are set once the cluster runs.
	mu      sync.Mutex
	mover   *move.Mover
	cluster *cluster.Cluster
}

func (a *agentMoves) setCluster(c *cluster.Cluster, mover *move.Mover) {
	a.mu.Lock()
	defer a.mu.Unlock()
	a.cluster, a.mover = c, mover
}

// Receive takes the data of a tenant moving here, and nothing else: the
// state must say the tenant is moving to this node.
func (a *agentMoves) Receive(ctx context.Context, tenant string, incremental bool, data io.Reader) error {
	record, err := a.store.Get(tenant)
	if err != nil || record.MoveTo != a.store.Node() {
		// The node moving it may have marked the move a moment ago.
		a.mu.Lock()
		member := a.cluster
		a.mu.Unlock()
		if member != nil {
			if err := member.CatchUp(); err != nil {
				return err
			}
		}
		record, err = a.store.Get(tenant)
	}
	if err != nil {
		return err
	}
	if record.MoveTo != a.store.Node() || record.Node == a.store.Node() {
		return fmt.Errorf("tenant %s is not moving to node %s", tenant, a.store.Node())
	}
	if !incremental {
		// What a failed move left here goes first.
		if exists, err := a.volumes.Exists(ctx, tenant); err != nil {
			return err
		} else if exists {
			if err := a.volumes.Remove(ctx, tenant); err != nil {
				return err
			}
		}
	}
	return a.volumes.Receive(ctx, tenant, incremental, data)
}

// Discard drops a copy of a tenant this node does not run.
func (a *agentMoves) Discard(ctx context.Context, tenant string) error {
	if record, err := a.store.Get(tenant); err == nil && record.Node == a.store.Node() {
		return fmt.Errorf("tenant %s runs on this node: its data stays", tenant)
	}
	return a.volumes.Remove(ctx, tenant)
}

func (a *agentMoves) Status(tenant string) (any, bool) { return a.reconciler.Status(tenant) }

// Move moves a tenant to another node, from the node that runs it.
func (a *agentMoves) Move(ctx context.Context, tenant, to, actor string) error {
	record, err := a.store.Get(tenant)
	if err != nil {
		return err
	}
	a.mu.Lock()
	mover, member := a.mover, a.cluster
	a.mu.Unlock()
	switch {
	case mover == nil:
		return errors.New("tenants move between nodes of a cluster")
	case record.Node == a.store.Node():
		return mover.Move(ctx, tenant, to, actor)
	default:
		return member.MoveOn(ctx, record.Node, tenant, to, actor)
	}
}

// clusterPeer reaches the other node of a move through the cluster.
type clusterPeer struct{ cluster *cluster.Cluster }

func (p clusterPeer) Receive(ctx context.Context, node, tenant string, incremental bool, data io.Reader) error {
	return p.cluster.ReceiveOn(ctx, node, tenant, incremental, data)
}

func (p clusterPeer) Discard(ctx context.Context, node, tenant string) error {
	return p.cluster.DiscardOn(ctx, node, tenant)
}

func (p clusterPeer) Status(ctx context.Context, node, tenant string) (reconcile.Status, error) {
	var status reconcile.Status
	err := p.cluster.StatusOn(ctx, node, tenant, &status)
	return status, err
}
