package replicate

import (
	"context"
	"fmt"
	"io"
)

// Receiver keeps the copies of other nodes' tenants on this node.
type Receiver struct {
	Store   Placements
	Volumes Volumes
	// CatchUp applies here what the cluster has applied: a whole copy is
	// taken only on a state no older than the cluster's.
	CatchUp func() error
	// Keep is how many snapshots a copy keeps; 3 if zero.
	Keep int
}

// check refuses a copy of tenant from sender unless, as this node's state
// has it, sender runs the tenant, this node keeps its copy, and it is not
// moving: a node that lost its tenants never writes over their copies. A
// refusal is decided on a state caught up with the cluster's: the owner
// may send as soon as the leader gave the tenant this replica, before this
// node applied it.
func (r *Receiver) check(sender, tenant string) error {
	err := r.checkState(sender, tenant)
	if err == nil || r.CatchUp == nil {
		return err
	}
	if catchUpErr := r.CatchUp(); catchUpErr != nil {
		return err
	}
	return r.checkState(sender, tenant)
}

func (r *Receiver) checkState(sender, tenant string) error {
	record, err := r.Store.Get(tenant)
	if err != nil {
		return err
	}
	self := r.Store.Node()
	switch {
	case record.Node == self:
		return fmt.Errorf("tenant %s runs on this node, %s: it keeps no copy of it", tenant, self)
	case record.Node != sender:
		return fmt.Errorf("tenant %s runs on node %s, not %s: its copy is not taken from %s", tenant, record.Node, sender, sender)
	case record.Replica != self && record.ReplicaNext != self:
		return fmt.Errorf("node %s does not keep the copy of tenant %s (node %q does)", self, tenant, record.Replica)
	case record.MoveTo == self:
		return fmt.Errorf("tenant %s is moving here: the move brings its copy up to date", tenant)
	}
	return nil
}

func (r *Receiver) Snapshots(ctx context.Context, sender, tenant string) ([]string, error) {
	if err := r.check(sender, tenant); err != nil {
		return nil, err
	}
	return r.Volumes.Snapshots(ctx, tenant, ReplicaReason)
}

func (r *Receiver) Receive(ctx context.Context, sender, tenant string, whole bool, data io.Reader) error {
	if whole && r.CatchUp != nil {
		if err := r.CatchUp(); err != nil {
			return err
		}
	}
	if err := r.check(sender, tenant); err != nil {
		return err
	}
	keep := r.Keep
	if keep <= 0 {
		keep = 3
	}
	return r.Volumes.ReceiveReplica(ctx, tenant, whole, data, keep)
}
