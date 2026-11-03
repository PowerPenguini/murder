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
// no binary is installed while changes are taken.
func (a *agentUpgrader) Install(ctx context.Context, binary io.Reader, sha string) error {
	if a.catchUp != nil {
		if err := a.catchUp(); err != nil {
			return err
		}
	}
	if frozen := a.store.Frozen(); !strings.HasPrefix(frozen, store.UpgradeFreeze) {
		return fmt.Errorf("changes are not frozen for an upgrade here (%q): murder upgrade freezes them first", frozen)
	}
	return a.Node.Install(ctx, binary, sha)
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
frozen: on that node, with its agent stopped, "murder upgrade rollback"
puts the old binary (and, if the new release changed it, the old database)
back; run it as /usr/local/bin/murder.prev, the old binary, if the new one
does not run. Running the same upgrade again resumes it; "murder upgrade -thaw"
takes changes again, which is safe only once every node runs one release.
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

func short(sha string) string {
	if len(sha) > 12 {
		return sha[:12]
	}
	return sha
}

// runRollback puts the binary an upgrade replaced back on this node, and
// the database as it was before, if the new release changed its schema. It
// undoes the last install only, and only while the agent is stopped.
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
	// Once an upgrade is over, its rollback would put back a database as
	// it was then, losing every change since.
	if _, err := upgrade.LastInstall(executable); err != nil {
		return err
	}
	if frozen, err := store.DatabaseFrozen(cfg.Database); err != nil {
		return err
	} else if !strings.HasPrefix(frozen, store.UpgradeFreeze) && !*force {
		return errors.New("no upgrade is under way (changes were taken again since): rolling the last one back now would put back the binary, and the database if its schema changed, as they were before it, losing what changed since; -force does it anyway")
	}
	record, err := upgrade.Rollback(executable)
	if err != nil {
		return err
	}
	fmt.Fprintf(stdout, "put murder %s back (the new binary is %s.failed)\n", short(record.From.Binary), executable)
	// The old binary may not open the database as the new release left it.
	schema, err := store.DatabaseSchema(cfg.Database)
	if err != nil {
		return err
	}
	if schema > record.From.Schema {
		kept, err := store.Restore(cfg.Database+".before-upgrade", cfg.Database)
		if err != nil {
			return fmt.Errorf("put the database back: %w", err)
		}
		fmt.Fprintf(stdout, "put the database from before the upgrade back; the one the new release changed is %s\n", kept)
	}
	fmt.Fprintln(stdout, "now: systemctl start murder-agent; systemctl reload murder-edge")
	return nil
}

// upgradeCluster upgrades every node, telling stdout.
func upgradeCluster(ctx context.Context, client *api.Client, binary string, wait time.Duration) error {
	return clusterops.Upgrade(ctx, client, binary, wait, stdout)
}
