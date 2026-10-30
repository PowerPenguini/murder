package main

import (
	"context"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"os"
	"slices"
	"time"

	"github.com/PowerPenguini/murder/internal/api"
	"github.com/PowerPenguini/murder/internal/config"
	"github.com/PowerPenguini/murder/internal/reconcile"
	"github.com/PowerPenguini/murder/internal/store"
	"github.com/PowerPenguini/murder/internal/upgrade"
)

// runVersion prints this binary's version: its hash and schema version.
func runVersion(args []string) error {
	flags := flag.NewFlagSet("version", flag.ContinueOnError)
	asJSON := flags.Bool("json", false, "print JSON")
	if err := flags.Parse(args); err != nil {
		return err
	}
	version, err := ownVersion()
	if err != nil {
		return err
	}
	if *asJSON {
		return json.NewEncoder(stdout).Encode(version)
	}
	fmt.Fprintf(stdout, "murder %s, schema version %d\n", short(version.Binary), version.Schema)
	return nil
}

func ownVersion() (upgrade.Version, error) {
	executable, err := os.Executable()
	if err != nil {
		return upgrade.Version{}, err
	}
	sum, err := upgrade.Hash(executable)
	if err != nil {
		return upgrade.Version{}, err
	}
	return upgrade.Version{Binary: sum, Schema: store.SchemaVersion}, nil
}

// agentUpgrader is the agent's part in an upgrade.
type agentUpgrader struct {
	*upgrade.Node
	store      *store.SQLite
	reconciler *reconcile.Reconciler
	started    time.Time
}

func (a *agentUpgrader) Info() upgrade.Info {
	at, _ := a.reconciler.LastPass()
	return upgrade.Info{Node: a.store.Node(), Version: a.Current, Started: a.started, Applied: a.store.Applied(), LastPass: at}
}

const upgradeUsage = `Usage: murder upgrade [-wait 5m] <binary>
       murder upgrade -thaw
       murder upgrade rollback [-config file] [-binary path]

Upgrade installs a murder binary on every node of the cluster (or this node
alone), one node after the other, this node last, without stopping a tenant
or dropping a request. Changes are frozen meanwhile, so nodes of two
releases exchange none: the panel and CLI only read. Each node checks the
binary, keeps the one it runs (murder.prev) and a copy of its database,
hands its edge over to the new binary and restarts its agent into it; the
next node follows once it is back and has caught up.

If a node does not come back, the upgrade stops there and changes stay
frozen: on that node, "murder upgrade rollback" puts the old binary (and,
if the new release changed it, the old database) back; then "murder upgrade
-thaw", or the upgrade again.
`

func runUpgrade(args []string) error {
	if len(args) > 0 && args[0] == "rollback" {
		return runRollback(args[1:])
	}
	flags := flag.NewFlagSet("upgrade", flag.ContinueOnError)
	flags.Usage = func() { fmt.Fprint(flags.Output(), upgradeUsage); flags.PrintDefaults() }
	socket := socketPath(flags)
	wait := flags.Duration("wait", 5*time.Minute, "how long each node may take to come back")
	thaw := flags.Bool("thaw", false, "only take changes again after an upgrade that stopped")
	positional, err := parseInterspersed(flags, args)
	if err != nil {
		return err
	}
	client := api.NewClient(*socket)
	ctx := context.Background()
	if *thaw {
		if len(positional) != 0 {
			flags.Usage()
			return flag.ErrHelp
		}
		if err := client.Thaw(ctx); err != nil {
			return err
		}
		fmt.Fprintln(stdout, "changes are taken again")
		return nil
	}
	if len(positional) != 1 {
		flags.Usage()
		return flag.ErrHelp
	}
	return upgradeCluster(ctx, client, positional[0], *wait)
}

func upgradeCluster(ctx context.Context, client *api.Client, binary string, wait time.Duration) error {
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
	fmt.Fprintf(stdout, "upgrading %v to %s (schema version %d)\n", nodes, short(version.Binary), version.Schema)
	reason := "upgrade to " + short(version.Binary)
	if err := client.Freeze(ctx, reason); err != nil {
		return fmt.Errorf("freeze changes: %w", err)
	}
	upgraded := 0
	for _, node := range nodes {
		err := upgradeNode(ctx, client, node, node == self, binary, version, wait)
		var refused *refusedError
		switch {
		case err == nil:
			upgraded++
			continue
		case errors.As(err, &refused) && upgraded == 0:
			// No node runs the new binary: nothing to keep apart.
			if thawErr := client.Thaw(ctx); thawErr != nil {
				return errors.Join(err, fmt.Errorf("thaw changes: %w", thawErr))
			}
			return fmt.Errorf("%w\nno node was upgraded; changes are taken again", err)
		case errors.As(err, &refused):
			return fmt.Errorf("%w\nit runs its old binary; %d node(s) run the new one, so changes stay frozen: fix the binary and upgrade again", err, upgraded)
		default:
			return fmt.Errorf("%w\nchanges stay frozen: on node %s, \"murder upgrade rollback\" puts its old binary back; then \"murder upgrade -thaw\", or upgrade again", err, node)
		}
	}
	// The local agent restarted last: wait until it takes requests.
	if err := retry(ctx, wait, func() error { return client.Thaw(ctx) }); err != nil {
		return fmt.Errorf("thaw changes: %w", err)
	}
	fmt.Fprintln(stdout, "every node runs the new binary; changes are taken again")
	return nil
}

// upgradeOrder lists the nodes to upgrade, this one last: it takes this
// command's requests.
func upgradeOrder(ctx context.Context, client *api.Client) ([]string, string, error) {
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
func upgradeNode(ctx context.Context, client *api.Client, node string, self bool, binary string, version upgrade.Version, wait time.Duration) error {
	name := node
	if name == "" {
		name = "this node"
	}
	before, err := client.NodeInfo(ctx, nodeOrSelf(node))
	if err != nil {
		return fmt.Errorf("%s: %w", name, err)
	}
	if before.Version.Binary == version.Binary {
		fmt.Fprintf(stdout, "%s already runs it\n", name)
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
	fmt.Fprintf(stdout, "%s: installed; its edge handed over, its agent restarts\n", name)
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
			return fmt.Errorf("still runs %s", short(info.Version.Binary))
		case info.Applied < before.Applied:
			return fmt.Errorf("has applied log entry %d of %d", info.Applied, before.Applied)
		case !info.LastPass.After(info.Started):
			return errors.New("has not reconciled since it started")
		}
		return nil
	})
	if err != nil {
		return fmt.Errorf("%s did not come back with the new binary within %v: %w", name, wait, err)
	}
	fmt.Fprintf(stdout, "%s runs the new binary\n", name)
	return nil
}

// nodeOrSelf names a node for the API; "" is this node, whatever its name.
func nodeOrSelf(node string) string {
	if node == "" {
		return "-"
	}
	return node
}

func short(sha string) string {
	if len(sha) > 12 {
		return sha[:12]
	}
	return sha
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

// runRollback puts the binary an upgrade replaced back on this node, and
// the database as it was before, if the new release changed its schema.
// The agent must be stopped.
func runRollback(args []string) error {
	flags := flag.NewFlagSet("upgrade rollback", flag.ContinueOnError)
	configPath := flags.String("config", defaultConfig, "agent configuration file")
	binary := flags.String("binary", "", "the agent's binary (this one if unset)")
	if err := flags.Parse(args); err != nil {
		return err
	}
	cfg, err := config.Load(*configPath)
	if err != nil {
		return err
	}
	executable := *binary
	if executable == "" {
		if executable, err = os.Executable(); err != nil {
			return err
		}
	}
	previous := executable + ".prev"
	old, err := upgrade.Probe(context.Background(), previous)
	if err != nil {
		return fmt.Errorf("no binary to go back to: %w", err)
	}
	// The database first: the old binary may not open it as the new
	// release left it.
	schema, err := store.DatabaseSchema(cfg.Database)
	if err != nil {
		return err
	}
	if schema > old.Schema {
		kept, err := store.Restore(cfg.Database+".before-upgrade", cfg.Database)
		if err != nil {
			return fmt.Errorf("put the database back: %w", err)
		}
		fmt.Fprintf(stdout, "put the database from before the upgrade back; the one the new release changed is %s\n", kept)
	}
	if err := os.Rename(executable, executable+".failed"); err != nil {
		return err
	}
	if err := os.Rename(previous, executable); err != nil {
		return err
	}
	fmt.Fprintf(stdout, "put murder %s back (the new binary is %s.failed); now: systemctl start murder-agent; systemctl reload murder-edge\n", short(old.Binary), executable)
	return nil
}

// refusedError is a node that kept its old binary.
type refusedError struct{ err error }

func (e *refusedError) Error() string { return e.err.Error() }
func (e *refusedError) Unwrap() error { return e.err }
