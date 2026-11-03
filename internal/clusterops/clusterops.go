// Package clusterops does what spans the cluster's nodes, for the CLI and
// the panel alike: an upgrade, node by node, and a drain.
package clusterops

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"net/http"
	"os"
	"slices"
	"strings"
	"time"

	"github.com/PowerPenguini/murder/internal/api"
	"github.com/PowerPenguini/murder/internal/cluster"
	"github.com/PowerPenguini/murder/internal/store"
	"github.com/PowerPenguini/murder/internal/upgrade"
)

// Client is what these ask of the agent: its API client.
type Client interface {
	ClusterStatus(ctx context.Context) (cluster.Status, error)
	Nodes(ctx context.Context) (api.Nodes, error)
	NodeInfo(ctx context.Context, node string) (upgrade.Info, error)
	Install(ctx context.Context, node string, binary io.Reader, sha, freeze string) error
	Freeze(ctx context.Context, reason string) error
	TakeOverFreeze(ctx context.Context, from, reason string) error
	ThawOwn(ctx context.Context, reason string) error
	List(ctx context.Context) ([]api.Tenant, error)
	Move(ctx context.Context, tenant, to string) (api.Tenant, error)
	BalanceMoves(ctx context.Context) ([]store.Move, error)
}

func shortSHA(sha string) string {
	if len(sha) > 12 {
		return sha[:12]
	}
	return sha
}

// An upgrade's freeze names its binary, the node that coordinates it and
// an operation of its own: "upgrade to <binary> from node <node>,
// operation <id>". Releases before named only the binary.
const (
	freezeNode      = " from node "
	freezeOperation = ", operation "
)

// upgradeFreeze is the freeze of a new upgrade to binary that node
// coordinates.
func upgradeFreeze(binary, node string) string {
	id := make([]byte, 6)
	_, _ = rand.Read(id)
	return store.UpgradeFreeze + shortSHA(binary) + freezeNode + node + freezeOperation + hex.EncodeToString(id)
}

// freezeOwner is what an upgrade's freeze says of the upgrade.
type freezeOwner struct{ binary, node string }

// ownerOf reads an upgrade's freeze; ok is false for any other freeze.
func ownerOf(reason string) (owner freezeOwner, ok bool) {
	rest, ok := strings.CutPrefix(reason, store.UpgradeFreeze)
	if !ok {
		return freezeOwner{}, false
	}
	binary, by, _ := strings.Cut(rest, freezeNode)
	node, _, _ := strings.Cut(by, freezeOperation)
	return freezeOwner{binary: binary, node: node}, true
}

// Upgrade installs binary on every node, one after the other, this one
// last, with changes frozen meanwhile, telling out how it goes. It begins
// only with changes taken, or resumes an upgrade to the same binary that
// this node coordinated and that stopped; it never takes over another
// upgrade's freeze, which may still run.
func Upgrade(ctx context.Context, client Client, binary string, wait time.Duration, out io.Writer) error {
	return run(ctx, client, binary, wait, false, out)
}

// TakeOver is Upgrade that also takes over the freeze of another upgrade,
// one that stopped: whoever runs it says that upgrade runs no more.
func TakeOver(ctx context.Context, client Client, binary string, wait time.Duration, out io.Writer) error {
	return run(ctx, client, binary, wait, true, out)
}

func run(ctx context.Context, client Client, binary string, wait time.Duration, takeOver bool, out io.Writer) error {
	version, err := upgrade.Probe(ctx, binary)
	if err != nil {
		return err
	}
	// The file's own hash, not what it says of itself: nodes check it.
	if version.Binary, err = upgrade.Hash(binary); err != nil {
		return err
	}
	nodes, self, err := upgradeOrder(ctx, client)
	if err != nil {
		return err
	}
	// A node that does not answer would stop the upgrade before it changed
	// anything, with changes frozen: find out first.
	coordinator := ""
	for _, node := range nodes {
		info, err := client.NodeInfo(ctx, nodeOrSelf(node))
		if err != nil {
			return fmt.Errorf("%s does not answer, so nothing was changed: %w", nameOf(node), err)
		}
		if node == self {
			coordinator = info.Node
		}
	}
	reason, fresh, err := freeze(ctx, client, version.Binary, coordinator, takeOver, out)
	if err != nil {
		return err
	}
	// What the nodes run is read only now, under this upgrade's own
	// freeze: another upgrade may have changed a node until then. Only if
	// they run one release does a refusal before any node changed leave
	// nothing apart.
	running := map[string]bool{}
	for _, node := range nodes {
		info, err := client.NodeInfo(ctx, nodeOrSelf(node))
		if err != nil {
			err = fmt.Errorf("%s does not answer: %w", nameOf(node), err)
			if !fresh {
				return fmt.Errorf("%w\nchanges stay frozen: run this upgrade again once it answers", err)
			}
			// Changes were taken before this freeze: no node took a
			// binary since.
			if thawErr := client.ThawOwn(ctx, reason); thawErr != nil {
				return errors.Join(err, fmt.Errorf("thaw changes: %w", thawErr))
			}
			return fmt.Errorf("%w\nnothing was changed; changes are taken again", err)
		}
		running[info.Version.Binary] = true
	}
	oneRelease := len(running) == 1
	fmt.Fprintf(out, "upgrading %v to %s (schema version %d)\n", nodes, shortSHA(version.Binary), version.Schema)
	upgraded := 0
	for _, node := range nodes {
		err := upgradeNode(ctx, client, node, node == self, binary, version, reason, wait, out)
		var refused *refusedError
		switch {
		case err == nil:
			upgraded++
			continue
		case errors.As(err, new(*lostError)):
			return fmt.Errorf("%w\nthis upgrade stops here and installs nothing more; the freeze stays with whoever holds it now", err)
		case errors.As(err, &refused) && upgraded == 0 && oneRelease:
			// No node runs the new binary: nothing to keep apart.
			if thawErr := client.ThawOwn(ctx, reason); thawErr != nil {
				return errors.Join(err, fmt.Errorf("thaw changes: %w", thawErr))
			}
			return fmt.Errorf("%w\nno node was upgraded; changes are taken again", err)
		case errors.As(err, &refused) && upgraded == 0:
			return fmt.Errorf("%w\nit runs its old binary, but the nodes ran different binaries before this upgrade (one stopped before it), so changes stay frozen: run an upgrade that every node takes, or roll the nodes that differ back", err)
		case errors.As(err, &refused):
			return fmt.Errorf("%w\nit runs its old binary; %d node(s) run the new one, so changes stay frozen until every node runs one release: run this upgrade again (it resumes), or one with a fixed binary (-take-over)", err, upgraded)
		case errors.As(err, new(*backError)):
			return fmt.Errorf("%w\nchanges stay frozen: on node %s, with its agent stopped, \"murder upgrade rollback\" (the new binary, whose rollback resumes if stopped; \"murder.prev upgrade rollback\" only if it does not run) puts it back; then run this upgrade again, or, once no node runs the new binary, \"murder upgrade -thaw\"", err, node)
		default:
			return fmt.Errorf("%w\nchanges stay frozen: check node %s (\"murder version\" there), then run this upgrade again (it resumes where it stopped)", err, node)
		}
	}
	// The local agent restarted last: wait until it takes requests. Only
	// this upgrade's own freeze is thawed.
	var lost error
	err = retry(ctx, wait, func() error {
		err := client.ThawOwn(ctx, reason)
		if status(err) != http.StatusConflict {
			return err
		}
		// A write that lost a race is tried again; a freeze that is not
		// this upgrade's any more stays.
		if view, viewErr := client.Nodes(ctx); viewErr == nil && view.Frozen != reason {
			lost = err
			return nil
		}
		return err
	})
	if err != nil {
		return fmt.Errorf("thaw changes: %w", err)
	}
	if lost != nil {
		return fmt.Errorf("every node runs the new binary, but another upgrade took the freeze over meanwhile, so changes stay frozen for it: %w", lost)
	}
	fmt.Fprintln(out, "every node runs the new binary; changes are taken again")
	return nil
}

// freeze freezes changes for an upgrade to binary that node coordinates,
// and returns the freeze; fresh says changes were taken until then. An
// upgrade that stopped is resumed, under a freeze of its own, if it was
// to the same binary from the same node; another upgrade's freeze is
// taken over only if takeOver says so.
func freeze(ctx context.Context, client Client, binary, node string, takeOver bool, out io.Writer) (reason string, fresh bool, err error) {
	view, err := client.Nodes(ctx)
	if err != nil {
		return "", false, fmt.Errorf("read the freeze: %w", err)
	}
	reason = upgradeFreeze(binary, node)
	held := view.Frozen
	owner, upgrading := ownerOf(held)
	switch {
	case held == "" || !upgrading:
		// One that froze changes meanwhile keeps them frozen: this is
		// refused then.
		if err := client.Freeze(ctx, reason); err != nil {
			return "", false, fmt.Errorf("freeze changes: %w", err)
		}
		return reason, true, nil
	case owner.binary == shortSHA(binary) && (owner.node == node || owner.node == ""):
		fmt.Fprintf(out, "resuming the upgrade that stopped (%s)\n", held)
	case takeOver:
		fmt.Fprintf(out, "taking over the freeze of another upgrade (%s)\n", held)
	default:
		return "", false, fmt.Errorf("changes are frozen for another upgrade (%s), which may still run, so nothing was changed: if it stopped, run it again on the node it began on (it resumes), or take it over with -take-over once sure it runs nowhere", held)
	}
	// Only the freeze read above is taken over: one that took it since
	// keeps it, and the upgrade it was runs no further once it is gone.
	if err := client.TakeOverFreeze(ctx, held, reason); err != nil {
		return "", false, fmt.Errorf("take the freeze over: %w", err)
	}
	return reason, false, nil
}

func nameOf(node string) string {
	if node == "" {
		return "this node"
	}
	return node
}

// status is the HTTP status of an API error, 0 for any other.
func status(err error) int {
	var apiErr *api.Error
	if errors.As(err, &apiErr) {
		return apiErr.Status
	}
	return 0
}

// upgradeOrder lists the nodes to upgrade, this one last: it takes this
// command's requests.
func upgradeOrder(ctx context.Context, client Client) ([]string, string, error) {
	status, err := client.ClusterStatus(ctx)
	var apiErr *api.Error
	if errors.As(err, &apiErr) && apiErr.Status == 409 {
		return []string{""}, "", nil // this node alone
	}
	if err != nil {
		return nil, "", err
	}
	var nodes []string
	for _, member := range status.Members {
		if member.Name != status.Node {
			nodes = append(nodes, member.Name)
		}
	}
	slices.Sort(nodes)
	return append(nodes, status.Node), status.Node, nil
}

// upgradeNode installs the binary on node, under the upgrade's freeze,
// then waits until the node runs it, has caught up with the cluster and
// has reconciled once. A node that runs it already, as one an upgrade that
// stopped had upgraded, is not sent it again, but waited for alike.
func upgradeNode(ctx context.Context, client Client, node string, self bool, binary string, version upgrade.Version, freeze string, wait time.Duration, out io.Writer) error {
	name := nameOf(node)
	before, err := client.NodeInfo(ctx, nodeOrSelf(node))
	if err != nil {
		return fmt.Errorf("%s: %w", name, err)
	}
	install := before.Version.Binary != version.Binary
	if !install {
		fmt.Fprintf(out, "%s already runs it\n", name)
	} else {
		file, err := os.Open(binary)
		if err != nil {
			return err
		}
		err = client.Install(ctx, nodeOrSelf(node), file, version.Binary, freeze)
		_ = file.Close()
		switch status(err) {
		case 0:
			if err != nil {
				return fmt.Errorf("%s: install: %w", name, err)
			}
		case http.StatusUnprocessableEntity:
			// A node that refused the binary, or whose edge did not take
			// it, runs its old one, as if nothing was sent.
			return &refusedError{fmt.Errorf("%s refused the binary: %w", name, err)}
		case http.StatusPreconditionFailed:
			return &lostError{fmt.Errorf("%s took no binary: %w", name, err)}
		default:
			return fmt.Errorf("%s: install: %w", name, err)
		}
		fmt.Fprintf(out, "%s: installed; its edge handed over, its agent restarts\n", name)
	}
	// Caught up means as far as the cluster applied once the node took
	// the binary; changes are frozen, so that is where it stays.
	target := before.Applied
	if status, err := client.ClusterStatus(ctx); err == nil && status.Applied > target {
		target = status.Applied
	}
	if self && install {
		// This command's agent restarts: wait for its socket again.
		time.Sleep(time.Second)
	}
	err = retry(ctx, wait, func() error {
		info, err := client.NodeInfo(ctx, nodeOrSelf(node))
		switch {
		case err != nil:
			return err
		case info.Version.Binary != version.Binary:
			return fmt.Errorf("still runs %s", shortSHA(info.Version.Binary))
		case info.Applied < target:
			return fmt.Errorf("has applied log entry %d of the cluster's %d", info.Applied, target)
		case !info.LastPass.After(info.Started):
			return errors.New("has not reconciled since it started")
		}
		return nil
	})
	switch {
	case err != nil && install:
		return &backError{fmt.Errorf("%s did not come back with the new binary within %v: %w", name, wait, err)}
	case err != nil:
		return fmt.Errorf("%s runs the new binary but is not ready within %v: %w", name, wait, err)
	case install:
		fmt.Fprintf(out, "%s runs the new binary\n", name)
	}
	return nil
}

// nodeOrSelf names a node for the API; "" is this node, whatever its name.
func nodeOrSelf(node string) string {
	if node == "" {
		return "-"
	}
	return node
}

// retry calls try every second until it succeeds or wait passes, and
// returns its last error.
func retry(ctx context.Context, wait time.Duration, try func() error) error {
	deadline := time.Now().Add(wait)
	for {
		err := try()
		if err == nil || time.Now().After(deadline) {
			return err
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(time.Second):
		}
	}
}

// backError is a node that took the binary but did not come back with it.
type backError struct{ err error }

func (e *backError) Error() string { return e.err.Error() }
func (e *backError) Unwrap() error { return e.err }

// lostError is a node that took no binary as the upgrade no longer holds
// the freeze: a person thawed, or another upgrade took it over.
type lostError struct{ err error }

func (e *lostError) Error() string { return e.err.Error() }
func (e *lostError) Unwrap() error { return e.err }

// refusedError is a node that kept its old binary.
type refusedError struct{ err error }

func (e *refusedError) Error() string { return e.err.Error() }
func (e *refusedError) Unwrap() error { return e.err }

// Balance moves tenants from the nodes running the most to those running
// the fewest, one after the other, as the agent plans it; dryRun only
// prints the plan.
func Balance(ctx context.Context, client Client, dryRun bool, out io.Writer) error {
	moves, err := client.BalanceMoves(ctx)
	if err != nil {
		return err
	}
	if len(moves) == 0 {
		fmt.Fprintln(out, "the nodes are balanced: none runs two tenants more than another")
		return nil
	}
	for i, move := range moves {
		if dryRun {
			fmt.Fprintf(out, "%s would move from %s to %s\n", move.Tenant, move.From, move.To)
			continue
		}
		start := time.Now()
		if _, err := client.Move(ctx, move.Tenant, move.To); err != nil {
			return fmt.Errorf("move %s to %s: %w (moved %d before it)", move.Tenant, move.To, err, i)
		}
		fmt.Fprintf(out, "%s moved from %s to %s in %v\n", move.Tenant, move.From, move.To, time.Since(start).Round(time.Second))
	}
	if !dryRun {
		fmt.Fprintf(out, "balanced: %d tenants moved\n", len(moves))
	}
	return nil
}

// Drain moves every tenant of node, live or archived, to target, one after
// the other, telling out each move, and stops at the first that does not
// move.
func Drain(ctx context.Context, client Client, node, target string, out io.Writer) error {
	tenants, err := client.List(ctx)
	if err != nil {
		return err
	}
	moved := 0
	for _, tenant := range tenants {
		if tenant.Node != node {
			continue
		}
		start := time.Now()
		if _, err := client.Move(ctx, tenant.Spec.Name, target); err != nil {
			return fmt.Errorf("move %s: %w (moved %d before it)", tenant.Spec.Name, err, moved)
		}
		moved++
		fmt.Fprintf(out, "%s moved to %s in %v\n", tenant.Spec.Name, target, time.Since(start).Round(time.Second))
	}
	fmt.Fprintf(out, "node %s runs no tenant now (%d moved)\n", node, moved)
	return nil
}
