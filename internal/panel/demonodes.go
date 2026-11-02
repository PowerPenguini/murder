package panel

import (
	"cmp"
	"context"
	"fmt"
	"io"
	"slices"
	"time"

	"github.com/PowerPenguini/murder/internal/api"
	"github.com/PowerPenguini/murder/internal/cluster"
	"github.com/PowerPenguini/murder/internal/store"
	"github.com/PowerPenguini/murder/internal/upgrade"
)

// demoNodes is what the demo's nodes do: their binaries, which left, and
// whether changes are frozen.
type demoNodes struct {
	binaries map[string]string
	removed  map[string]bool
	failed   map[string]time.Time
	frozen   string
	started  time.Time
}

const demoBinary = "9f2c4be1a07d3355e0c1d7a8b6f4e2d1c0b9a8f7e6d5c4b3a2918070605040302"

// AsCluster is the cluster acted on by actor, for the cluster page.
func (d *DemoCluster) AsCluster(actor string) ClusterAgent { return &demoActor{d, actor} }

func (d *DemoCluster) Nodes(ctx context.Context) (api.Nodes, error) {
	nodes, err := d.Demo.Nodes(ctx)
	d.mu.Lock()
	defer d.mu.Unlock()
	nodes.Frozen = d.nodes.frozen
	nodes.Nodes = slices.DeleteFunc(nodes.Nodes, func(n api.NodeState) bool { return d.nodes.removed[n.Name] })
	for i, n := range nodes.Nodes {
		if at, ok := d.nodes.failed[n.Name]; ok {
			nodes.Nodes[i].FailedAt = at
		}
	}
	return nodes, err
}

func (a *demoActor) ClusterStatus(ctx context.Context) (cluster.Status, error) {
	nodes, _ := a.Nodes(ctx)
	status := cluster.Status{Node: nodes.Self, State: "Leader", Leader: nodes.Leader, Applied: 4812}
	for _, n := range nodes.Nodes {
		status.Members = append(status.Members, cluster.Member{Name: n.Name, Address: n.Address})
	}
	return status, nil
}

func (a *demoActor) NodeInfo(ctx context.Context, node string) (upgrade.Info, error) {
	nodes, _ := a.Nodes(ctx)
	if node == "-" {
		node = nodes.Self
	}
	a.mu.Lock()
	defer a.mu.Unlock()
	binary, ok := a.nodes.binaries[node]
	if !ok {
		binary = demoBinary
	}
	for _, n := range nodes.Nodes {
		if n.Name == node && n.Answers {
			return upgrade.Info{Node: node, Version: upgrade.Version{Binary: binary, Schema: 13}, Started: a.nodes.started, Applied: 4812, LastPass: time.Now()}, nil
		}
	}
	return upgrade.Info{}, &api.Error{Status: 502, Message: "node " + node + " does not answer"}
}

func (a *demoActor) Install(_ context.Context, node string, binary io.Reader, sha string) error {
	if _, err := io.Copy(io.Discard, binary); err != nil {
		return err
	}
	time.Sleep(2 * a.Pace)
	a.mu.Lock()
	defer a.mu.Unlock()
	if node == "-" {
		node = "fsn-1"
	}
	a.nodes.binaries[node] = sha
	a.record("node.upgraded", "", node+" runs "+shortImage("sha256:"+sha))
	return nil
}

func (a *demoActor) Freeze(_ context.Context, reason string) error {
	a.mu.Lock()
	defer a.mu.Unlock()
	a.nodes.frozen = reason
	return nil
}

func (a *demoActor) Thaw(context.Context) error {
	a.mu.Lock()
	defer a.mu.Unlock()
	a.nodes.frozen = ""
	return nil
}

func (a *demoActor) RemoveNode(ctx context.Context, node string) error {
	tenants, _ := a.List(ctx)
	for _, t := range tenants {
		if t.Node == node || t.Replica == node {
			return &api.Error{Status: 409, Message: fmt.Sprintf("node %s still runs tenant %s: drain it first", node, t.Spec.Name)}
		}
	}
	a.mu.Lock()
	defer a.mu.Unlock()
	a.nodes.removed[node] = true
	a.record("node.removed", "", node)
	return nil
}

func (a *demoActor) FailOver(_ context.Context, node string) (store.FailedOver, error) {
	a.mu.Lock()
	defer a.mu.Unlock()
	var outcome store.FailedOver
	for name, t := range a.tenants {
		if t.Node != node {
			continue
		}
		if t.Replica == "" {
			outcome.Stranded = append(outcome.Stranded, name)
			continue
		}
		t.Node, t.Replica = t.Replica, ""
		a.tenants[name] = t
		outcome.Promoted = append(outcome.Promoted, name)
	}
	a.nodes.failed[node] = time.Now()
	a.record("node.failed-over", "", node)
	return outcome, nil
}

func (a *demoActor) RunBackups(ctx context.Context) ([]api.BackupResult, error) {
	tenants, _ := a.List(ctx)
	var results []api.BackupResult
	for _, t := range tenants {
		if t.Node != "fsn-1" || !t.ArchivedAt.IsZero() {
			continue
		}
		time.Sleep(a.Pace / 3)
		results = append(results, api.BackupResult{Tenant: t.Spec.Name, Key: "backups/" + t.Spec.Name + "/" + time.Now().UTC().Format("20060102T150405Z") + ".db.zst", Size: 1 << 20})
	}
	slices.SortFunc(results, func(x, y api.BackupResult) int { return cmp.Compare(x.Tenant, y.Tenant) })
	return results, nil
}
