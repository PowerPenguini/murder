package clusterops

import (
	"context"
	"errors"
	"fmt"
	"io"
	"maps"
	"net/http"
	"slices"
	"strings"
	"time"

	"github.com/PowerPenguini/murder/internal/api"
	"github.com/PowerPenguini/murder/internal/cluster"
	"github.com/PowerPenguini/murder/internal/reboot"
)

// RebootClient is what a reboot asks of the agent: Client, and reboots.
type RebootClient interface {
	Client
	Reboot(ctx context.Context, node string, request reboot.Request) error
}

// RebootOptions are how a node of a cluster reboots.
type RebootOptions struct {
	// To is the node every tenant moves to; empty chooses for each: the
	// node keeping its whole copy, or the one running the fewest.
	To string
	// Stay leaves the tenants where they moved once the node is back; a
	// node that runs the command must leave them, as it cannot wait for
	// itself.
	Stay bool
	// Force reboots the node even though the cluster then takes no change
	// until it is back: fewer than a majority of its voters answer.
	Force bool
	// Wait bounds how long the node may take to come back and be ready.
	Wait time.Duration
	// Now is the time; time.Now if nil.
	Now func() time.Time
}

// ErrAlone is a node in no cluster, or a cluster of it alone: its tenants
// have nowhere to go, and stop until it is back.
var ErrAlone = errors.New("this node is in no cluster with another")

// moved is a tenant a reboot moved, and where to.
type moved struct{ tenant, to string }

// Reboot reboots node of the cluster with the least downtime: its tenants
// move to other nodes, one after the other, then node reboots; once it is
// back and ready, they move back to it, unless opts.Stay. It refuses,
// changing nothing, when changes are frozen, when no other node can take
// the tenants, when node would not come back as it runs (its units do not
// start at boot), or when the cluster would lose its majority meanwhile
// (unless opts.Force). Node "" or "-" is this node.
func Reboot(ctx context.Context, client RebootClient, node string, opts RebootOptions, out io.Writer) error {
	if opts.Wait <= 0 {
		opts.Wait = 15 * time.Minute
	}
	if opts.Now == nil {
		opts.Now = time.Now
	}
	members, err := client.ClusterStatus(ctx)
	if status(err) == http.StatusConflict {
		return ErrAlone // the agent's answer for a node in no cluster
	}
	if err != nil {
		return err
	}
	self := members.Node
	if node == "" || node == "-" {
		node = self
	}
	if !slices.ContainsFunc(members.Members, func(m cluster.Member) bool { return m.Name == node }) {
		return fmt.Errorf("node %s is no member of the cluster", node)
	}
	if len(members.Members) == 1 {
		return ErrAlone
	}
	if node == self && !opts.Stay {
		return fmt.Errorf("this command runs on node %s, which it would reboot, so it could not move the tenants back once it is up: run \"murder node reboot %s\" on another node of the cluster, which waits for it and moves them back, or add -stay to leave them where they move", node, node)
	}
	view, err := client.Nodes(ctx)
	if err != nil {
		return err
	}
	if view.Frozen != "" {
		return fmt.Errorf("changes are frozen (%s): a reboot moves tenants, so nothing was changed; reboot once that is over", view.Frozen)
	}
	states := map[string]api.NodeState{}
	for _, state := range view.Nodes {
		states[state.Name] = state
	}
	if !states[node].Answers {
		return fmt.Errorf("node %s does not answer, so its tenants cannot move and it cannot be asked to reboot: if it is down, see \"murder cluster failover\"", node)
	}
	// Quorum: the voters left answering must be a majority of them all.
	voters, answering := 0, 0
	for _, member := range members.Members {
		if member.Suffrage != "Voter" {
			continue
		}
		voters++
		if member.Name != node && states[member.Name].Answers {
			answering++
		}
	}
	if answering*2 <= voters {
		lost := fmt.Sprintf("while node %s reboots, %d of the cluster's %d voters answer, not a majority: the cluster takes no change until it is back (no tenant is created, moved or failed over, the panel only reads)", node, answering, voters)
		if !opts.Force {
			return fmt.Errorf("%s; nothing was changed: bring the other nodes back first, or add -force to reboot it all the same", lost)
		}
		fmt.Fprintf(out, "rebooting all the same (-force): %s\n", lost)
	}
	targets := rebootTargets(view, node, opts.Now())
	if opts.To != "" {
		if opts.To == node {
			return fmt.Errorf("the tenants of node %s cannot move to itself: name another node with -to", node)
		}
		if !slices.Contains(targets, opts.To) {
			return fmt.Errorf("node %s cannot take the tenants: %s", opts.To, whyNoTarget(states, opts.To, opts.Now()))
		}
		targets = []string{opts.To}
	}
	tenants, err := client.List(ctx)
	if err != nil {
		return err
	}
	var theirs []api.Tenant
	for _, tenant := range tenants {
		if tenant.Node == node {
			theirs = append(theirs, tenant)
		}
	}
	if len(theirs) > 0 && len(targets) == 0 {
		return fmt.Errorf("no other node can take the tenants of node %s (each must answer, and be neither failed over nor restarting), so nothing was changed", node)
	}
	// The node must come back as it runs: asked before anything moves.
	if err := client.Reboot(ctx, nodeOrSelf(node), reboot.Request{Check: true}); err != nil {
		return rebootRefused(node, err)
	}
	plan := planDrain(theirs, tenants, targets, opts.To)
	fmt.Fprintf(out, "rebooting node %s: %d tenants move away first\n", node, len(plan))
	var done []moved
	for _, move := range plan {
		start := time.Now()
		if _, err := client.Move(ctx, move.tenant, move.to); err != nil {
			return fmt.Errorf("move %s to %s: %w\nnode %s was not rebooted; %s", move.tenant, move.to, err, node, whereMoved(done, node))
		}
		done = append(done, move)
		fmt.Fprintf(out, "%s moved to %s in %v\n", move.tenant, move.to, time.Since(start).Round(time.Second))
	}
	// Caught up, once back, means as far as the cluster has applied now.
	before, err := client.NodeInfo(ctx, nodeOrSelf(node))
	if err != nil {
		return fmt.Errorf("node %s: %w\nit was not rebooted; %s", node, err, whereMoved(done, node))
	}
	target := before.Applied
	if now, err := client.ClusterStatus(ctx); err == nil && now.Applied > target {
		target = now.Applied
	}
	// The node is not failed over while it boots: it is marked away for
	// as long as the wait.
	if err := client.Reboot(ctx, nodeOrSelf(node), reboot.Request{Away: opts.Wait}); err != nil {
		return fmt.Errorf("%w\n%s", rebootRefused(node, err), whereMoved(done, node))
	}
	if node == self {
		fmt.Fprintf(out, "node %s reboots now; %s\n", node, whereMoved(done, node))
		return nil
	}
	fmt.Fprintf(out, "node %s reboots; waiting up to %v for it to come back\n", node, opts.Wait)
	err = retry(ctx, opts.Wait, func() error {
		info, err := client.NodeInfo(ctx, node)
		switch {
		case err != nil:
			return err
		case !info.Started.After(before.Started):
			return errors.New("has not restarted yet")
		case info.Applied < target:
			return fmt.Errorf("has applied log entry %d of the cluster's %d", info.Applied, target)
		case !info.LastPass.After(info.Started):
			return errors.New("has not reconciled since it started")
		}
		return nil
	})
	if err != nil {
		return fmt.Errorf("node %s did not come back ready within %v: %w\n%s; once it is back, move them with \"murder tenant move NAME -to %s\"", node, opts.Wait, err, whereMoved(done, node), node)
	}
	fmt.Fprintf(out, "node %s is back\n", node)
	if opts.Stay || len(done) == 0 {
		if len(done) > 0 {
			fmt.Fprintf(out, "%s\n", whereMoved(done, node))
		}
		return nil
	}
	return moveBack(ctx, client, node, done, out)
}

// rebootTargets are the nodes that can take node's tenants: they answer,
// and are neither failed over nor restarting.
func rebootTargets(view api.Nodes, node string, now time.Time) []string {
	var targets []string
	for _, state := range view.Nodes {
		if state.Name != node && state.Answers && !state.Failed() && !state.AwayUntil.After(now) {
			targets = append(targets, state.Name)
		}
	}
	slices.Sort(targets)
	return targets
}

func whyNoTarget(states map[string]api.NodeState, node string, now time.Time) string {
	state, ok := states[node]
	switch {
	case !ok:
		return "it is no member of the cluster"
	case state.Failed():
		return "it was failed over and has not answered since"
	case !state.Answers:
		return "it does not answer"
	case state.AwayUntil.After(now):
		return "it restarts (marked away until " + state.AwayUntil.Local().Format(time.DateTime) + ")"
	}
	return "it is the node that reboots"
}

// planDrain chooses where each of theirs moves: to, if set; else the node
// keeping its whole copy, which takes only what changed since; else the
// target running the fewest tenants as the moves go, the first by name
// among equals. A target is one of targets.
func planDrain(theirs, all []api.Tenant, targets []string, to string) []moved {
	load := map[string]int{}
	for _, target := range targets {
		load[target] = 0
	}
	for _, tenant := range all {
		if _, ok := load[tenant.Node]; ok && tenant.ArchivedAt.IsZero() && !tenant.Spec.Suspended {
			load[tenant.Node]++
		}
	}
	var plan []moved
	for _, tenant := range theirs {
		target := to
		if target == "" && tenant.ReplicaReady && slices.Contains(targets, tenant.Replica) {
			target = tenant.Replica
		}
		if target == "" {
			for _, name := range slices.Sorted(maps.Keys(load)) {
				if target == "" || load[name] < load[target] {
					target = name
				}
			}
		}
		if tenant.ArchivedAt.IsZero() && !tenant.Spec.Suspended {
			load[target]++
		}
		plan = append(plan, moved{tenant: tenant.Spec.Name, to: target})
	}
	return plan
}

// whereMoved says where the tenants a reboot moved run now.
func whereMoved(done []moved, node string) string {
	if len(done) == 0 {
		return "no tenant was moved"
	}
	by := map[string][]string{}
	for _, move := range done {
		by[move.to] = append(by[move.to], move.tenant)
	}
	var parts []string
	for _, to := range slices.Sorted(maps.Keys(by)) {
		parts = append(parts, strings.Join(by[to], ", ")+" on "+to)
	}
	return fmt.Sprintf("the %d tenants moved from %s run %s", len(done), node, strings.Join(parts, "; "))
}

// rebootRefused says why node does not reboot.
func rebootRefused(node string, err error) error {
	switch status(err) {
	case http.StatusNotFound, http.StatusNotImplemented:
		return fmt.Errorf("node %s takes no reboots: it runs a release before \"murder node reboot\"; upgrade it first (%w)", node, err)
	case http.StatusUnprocessableEntity:
		return fmt.Errorf("node %s refuses to reboot: %w", node, err)
	}
	return fmt.Errorf("node %s: reboot: %w", node, err)
}

// moveBack moves the tenants a reboot moved back to node, those still where
// they moved: one moved on since stays.
func moveBack(ctx context.Context, client Client, node string, done []moved, out io.Writer) error {
	tenants, err := client.List(ctx)
	if err != nil {
		return fmt.Errorf("list the tenants to move back: %w\n%s", err, whereMoved(done, node))
	}
	where := map[string]string{}
	for _, tenant := range tenants {
		where[tenant.Spec.Name] = tenant.Node
	}
	back := 0
	for i, move := range done {
		now, ok := where[move.tenant]
		if !ok || now != move.to {
			fmt.Fprintf(out, "%s stays: it is no longer on %s\n", move.tenant, move.to)
			continue
		}
		start := time.Now()
		if _, err := client.Move(ctx, move.tenant, node); err != nil {
			return fmt.Errorf("move %s back to %s: %w\nmoved %d back before it; %s", move.tenant, node, err, back, whereMoved(done[i:], node))
		}
		back++
		fmt.Fprintf(out, "%s moved back to %s in %v\n", move.tenant, node, time.Since(start).Round(time.Second))
	}
	fmt.Fprintf(out, "node %s rebooted and runs its tenants again (%d moved back)\n", node, back)
	return nil
}
