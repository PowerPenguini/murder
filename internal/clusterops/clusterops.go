// Package clusterops does what spans the cluster's nodes, for the CLI and
// the panel alike: an upgrade, node by node, and a drain.
package clusterops

import (
	"context"
	"errors"
	"fmt"
	"io"
	"os"
	"slices"
	"time"

	"github.com/PowerPenguini/murder/internal/api"
	"github.com/PowerPenguini/murder/internal/cluster"
	"github.com/PowerPenguini/murder/internal/store"
	"github.com/PowerPenguini/murder/internal/upgrade"
)

// Client is what these ask of the agent: its API client.
type Client interface {
	ClusterStatus(ctx context.Context) (cluster.Status, error)
	NodeInfo(ctx context.Context, node string) (upgrade.Info, error)
	Install(ctx context.Context, node string, binary io.Reader, sha string) error
	Freeze(ctx context.Context, reason string) error
	Thaw(ctx context.Context) error
	List(ctx context.Context) ([]api.Tenant, error)
	Move(ctx context.Context, tenant, to string) (api.Tenant, error)
}

func shortSHA(sha string) string {
	if len(sha) > 12 {
		return sha[:12]
	}
	return sha
}

// Upgrade installs binary on every node, one after the other, this one
// last, with changes frozen meanwhile, telling out how it goes.
func Upgrade(ctx context.Context, client Client, binary string, wait time.Duration, out io.Writer) error {
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
	// It also tells whether the nodes run one release now: only then does
	// a refusal before any node changed leave nothing apart.
	running := map[string]bool{}
	for _, node := range nodes {
		info, err := client.NodeInfo(ctx, nodeOrSelf(node))
		if err != nil {
			name := node
			if name == "" {
				name = "this node"
			}
			return fmt.Errorf("%s does not answer, so nothing was changed: %w", name, err)
		}
		running[info.Version.Binary] = true
	}
	oneRelease := len(running) == 1
	fmt.Fprintf(out, "upgrading %v to %s (schema version %d)\n", nodes, shortSHA(version.Binary), version.Schema)
	reason := store.UpgradeFreeze + shortSHA(version.Binary)
	if err := client.Freeze(ctx, reason); err != nil {
		return fmt.Errorf("freeze changes: %w", err)
	}
	upgraded := 0
	for _, node := range nodes {
		err := upgradeNode(ctx, client, node, node == self, binary, version, wait, out)
		var refused *refusedError
		switch {
		case err == nil:
			upgraded++
			continue
		case errors.As(err, &refused) && upgraded == 0 && oneRelease:
			// No node runs the new binary: nothing to keep apart.
			if thawErr := client.Thaw(ctx); thawErr != nil {
				return errors.Join(err, fmt.Errorf("thaw changes: %w", thawErr))
			}
			return fmt.Errorf("%w\nno node was upgraded; changes are taken again", err)
		case errors.As(err, &refused) && upgraded == 0:
			return fmt.Errorf("%w\nit runs its old binary, but the nodes ran different binaries before this upgrade (one stopped before it), so changes stay frozen: run an upgrade that every node takes, or roll the nodes that differ back", err)
		case errors.As(err, &refused):
			return fmt.Errorf("%w\nit runs its old binary; %d node(s) run the new one, so changes stay frozen until every node runs one release: run this upgrade again (it resumes), or one with a fixed binary", err, upgraded)
		case errors.As(err, new(*backError)):
			return fmt.Errorf("%w\nchanges stay frozen: on node %s, \"murder.prev upgrade rollback\" (the old binary, beside murder; agent stopped) puts it back; then run this upgrade again, or, once no node runs the new binary, \"murder upgrade -thaw\"", err, node)
		default:
			return fmt.Errorf("%w\nchanges stay frozen: check node %s (\"murder version\" there), then run this upgrade again (it resumes where it stopped)", err, node)
		}
	}
	// The local agent restarted last: wait until it takes requests.
	if err := retry(ctx, wait, func() error { return client.Thaw(ctx) }); err != nil {
		return fmt.Errorf("thaw changes: %w", err)
	}
	fmt.Fprintln(out, "every node runs the new binary; changes are taken again")
	return nil
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

// upgradeNode installs the binary on node, then waits until the node runs
// it, has caught up with the cluster and has reconciled once.
func upgradeNode(ctx context.Context, client Client, node string, self bool, binary string, version upgrade.Version, wait time.Duration, out io.Writer) error {
	name := node
	if name == "" {
		name = "this node"
	}
	before, err := client.NodeInfo(ctx, nodeOrSelf(node))
	if err != nil {
		return fmt.Errorf("%s: %w", name, err)
	}
	if before.Version.Binary == version.Binary {
		fmt.Fprintf(out, "%s already runs it\n", name)
		return nil
	}
	file, err := os.Open(binary)
	if err != nil {
		return err
	}
	err = client.Install(ctx, nodeOrSelf(node), file, version.Binary)
	_ = file.Close()
	if err != nil {
		// A node that refused the binary, or whose edge did not take it,
		// runs its old one, as if nothing was sent.
		var apiErr *api.Error
		if errors.As(err, &apiErr) && apiErr.Status == 422 {
			return &refusedError{fmt.Errorf("%s refused the binary: %w", name, err)}
		}
		return fmt.Errorf("%s: install: %w", name, err)
	}
	fmt.Fprintf(out, "%s: installed; its edge handed over, its agent restarts\n", name)
	// Caught up means as far as the cluster applied once the node took
	// the binary; changes are frozen, so that is where it stays.
	target := before.Applied
	if status, err := client.ClusterStatus(ctx); err == nil && status.Applied > target {
		target = status.Applied
	}
	if self {
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
	if err != nil {
		return &backError{fmt.Errorf("%s did not come back with the new binary within %v: %w", name, wait, err)}
	}
	fmt.Fprintf(out, "%s runs the new binary\n", name)
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

// refusedError is a node that kept its old binary.
type refusedError struct{ err error }

func (e *refusedError) Error() string { return e.err.Error() }
func (e *refusedError) Unwrap() error { return e.err }

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
