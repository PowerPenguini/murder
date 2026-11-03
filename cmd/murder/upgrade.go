package main

import (
	"context"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io"
	"os"
	"strings"
	"time"

	"github.com/PowerPenguini/murder/internal/api"
	"github.com/PowerPenguini/murder/internal/clusterops"
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
	if release != "" {
		fmt.Fprintf(stdout, "murder %s (%s), schema version %d\n", release, short(version.Binary), version.Schema)
		return nil
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
	// catchUp applies here what the cluster has applied; nil outside one.
	catchUp func() error
}

// Install takes a binary only under an upgrade's freeze, as this node has
// it once caught up: a rollback here then finds the freeze it undoes, and
// no binary is installed while changes are taken. Given the freeze of the
// upgrade that sends it, only under that one: an upgrade whose freeze
// another took over, or a person thawed, installs nothing more. Upgrades
// of releases before say no freeze.
func (a *agentUpgrader) Install(ctx context.Context, binary io.Reader, sha, freeze string) error {
	if a.catchUp != nil {
		if err := a.catchUp(); err != nil {
			return err
		}
	}
	frozen := a.store.Frozen()
	switch {
	case freeze != "" && frozen == "":
		return fmt.Errorf("%w: changes were taken again here", upgrade.ErrNotOwner)
	case freeze != "" && frozen != freeze:
		return fmt.Errorf("%w: changes are frozen here for %q", upgrade.ErrNotOwner, frozen)
	case !strings.HasPrefix(frozen, store.UpgradeFreeze):
		return fmt.Errorf("changes are not frozen for an upgrade here (%q): murder upgrade freezes them first", frozen)
	}
	return a.Node.Install(ctx, binary, sha)
}

func (a *agentUpgrader) Info() upgrade.Info {
	at, _ := a.reconciler.LastPass()
	return upgrade.Info{Node: a.store.Node(), Version: a.Current, Started: a.started, Applied: a.store.Applied(), LastPass: at}
}

const upgradeUsage = `Usage: murder upgrade [-wait 5m] [-take-over] <binary>
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
frozen: on that node, with its agent stopped, "murder upgrade rollback"
puts the old binary (and, if the new release changed it, the old database)
back. Run it as the new binary in place: it puts the database back first,
and if stopped, run again, it finishes. Only if the new binary does not run
at all, run it as /usr/local/bin/murder.prev, the old binary; one of
v0.1.0 does not resume. Running the same upgrade again on the same node resumes it.
An upgrade never takes over the freeze of another, which may still run
elsewhere, unless told -take-over. "murder upgrade -thaw" takes changes
again; it is for an upgrade that stopped, safe only once every node runs
one release, and never to be used while an upgrade runs on another node.
`

func runUpgrade(args []string) error {
	if len(args) > 0 && args[0] == "rollback" {
		return runRollback(args[1:])
	}
	flags := flag.NewFlagSet("upgrade", flag.ContinueOnError)
	flags.Usage = func() { fmt.Fprint(flags.Output(), upgradeUsage); flags.PrintDefaults() }
	socket := socketPath(flags)
	wait := flags.Duration("wait", 5*time.Minute, "how long each node may take to come back")
	thaw := flags.Bool("thaw", false, "only take changes again after an upgrade that stopped (never while one runs on another node)")
	takeOver := flags.Bool("take-over", false, "take over the freeze of another upgrade, which stopped and runs nowhere")
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
	return upgradeCluster(ctx, client, positional[0], *wait, *takeOver)
}

func short(sha string) string {
	if len(sha) > 12 {
		return sha[:12]
	}
	return sha
}

// restoreDatabase puts a database copy in place; a test makes it fail.
var restoreDatabase = store.Restore

// runRollback puts the binary an upgrade replaced back on this node, and
// the database as it was before, if the new release changed its schema. It
// undoes the last install only, and only while the agent is stopped. The
// database goes back first, while the new binary, whose code resumes a
// rollback, is still in place: the old one may be of a release that cannot
// (v0.1.0 drops the record as it puts its binary back).
func runRollback(args []string) error {
	flags := flag.NewFlagSet("upgrade rollback", flag.ContinueOnError)
	configPath := flags.String("config", defaultConfig, "agent configuration file")
	binary := flags.String("binary", "", "the agent's binary (this one if unset)")
	force := flags.Bool("force", false, "roll back even though no upgrade is under way")
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
		executable = upgrade.RollbackTarget(executable)
	}
	if running, err := store.InUse(cfg.Database); err != nil {
		return err
	} else if running {
		return errors.New("the agent runs: stop it first (systemctl stop murder-agent)")
	}
	// Nothing changes unless the binary can go back too.
	record, err := upgrade.CheckRollback(executable)
	if err != nil {
		return err
	}
	// Once an upgrade is over, its rollback would put back a database as
	// it was then, losing every change since.
	if frozen, err := store.DatabaseFrozen(cfg.Database); err != nil {
		return err
	} else if !strings.HasPrefix(frozen, store.UpgradeFreeze) && !*force {
		return errors.New("no upgrade is under way (changes were taken again since): rolling the last one back now would put back the binary, and the database if its schema changed, as they were before it, losing what changed since; -force does it anyway")
	}
	// The old binary may not open the database as the new release left
	// it: then the copy from before the upgrade goes back too, and is
	// checked before anything changes.
	schema, err := store.DatabaseSchema(cfg.Database)
	if err != nil {
		return err
	}
	before := cfg.Database + ".before-upgrade"
	if schema > record.From.Schema {
		if _, err := store.CheckBackup(before); err != nil {
			return fmt.Errorf("nothing was rolled back: the new release changed the database (schema %d, the old binary opens %d), and its copy from before the upgrade cannot be put back: %w", schema, record.From.Schema, err)
		}
		if copied, err := store.DatabaseSchema(before); err != nil {
			return err
		} else if copied > record.From.Schema {
			return fmt.Errorf("nothing was rolled back: %s is of schema %d, which the old binary (schema %d) does not open", before, copied, record.From.Schema)
		}
		kept, err := restoreDatabase(before, cfg.Database)
		if err != nil {
			return fmt.Errorf("put the database back: %w\nnothing else changed: the new binary is still in place; once this is fixed, run \"%s upgrade rollback\" again", err, executable)
		}
		fmt.Fprintf(stdout, "put the database from before the upgrade back; the one the new release changed is %s\n", kept)
	}
	// The record stays until the binary is back too: a rollback that
	// stops before is run again, and finishes.
	if record, err = upgrade.Rollback(executable); err != nil {
		return fmt.Errorf("put the old binary back: %w\nthe new binary is still in place; once this is fixed, run \"%s upgrade rollback\" again", err, executable)
	}
	fmt.Fprintf(stdout, "put murder %s back (the new binary is kept as %s.failed)\n", short(record.From.Binary), executable)
	if err := upgrade.FinishRollback(executable); err != nil {
		return fmt.Errorf("%w\nthe old binary and its database are back; run \"%s.failed upgrade rollback\" (the new binary's code) to finish", err, executable)
	}
	fmt.Fprintln(stdout, "now: systemctl start murder-agent; systemctl reload murder-edge")
	return nil
}

// upgradeCluster upgrades every node, telling stdout.
func upgradeCluster(ctx context.Context, client *api.Client, binary string, wait time.Duration, takeOver bool) error {
	if takeOver {
		return clusterops.TakeOver(ctx, client, binary, wait, stdout)
	}
	return clusterops.Upgrade(ctx, client, binary, wait, stdout)
}
