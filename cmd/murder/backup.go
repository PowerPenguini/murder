package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"time"

	"github.com/PowerPenguini/murder/internal/access"
	"github.com/PowerPenguini/murder/internal/api"
	"github.com/PowerPenguini/murder/internal/config"
	"github.com/PowerPenguini/murder/internal/localbackup"
	"github.com/PowerPenguini/murder/internal/secrets"
	"github.com/PowerPenguini/murder/internal/store"
)

// scheduledDir is where the agent keeps scheduled copies; tests replace it
// along with the agent's.
var scheduledDir = localbackup.Dir

// runBackup keeps a copy of the agent's database in a directory as
// murder-<UTC time>.db, removing all but the newest keep. A copy taken by
// hand is an access to every tenant's data, recorded in the register;
// murder-backup.timer's (-scheduled) the agent keeps in its node's backup
// directory itself, handed to no one.
func runBackup(args []string) error {
	flags := flag.NewFlagSet("backup", flag.ContinueOnError)
	socket := socketPath(flags)
	keep := flags.Int("keep", 48, "backups to keep in the directory, newest first; 0 keeps all")
	reason := addReasonFlags(flags, "a copy made by hand")
	scheduled := flags.Bool("scheduled", false, "the copy murder-backup.timer has the agent keep in "+localbackup.Dir+", recorded in no access register")
	flags.Usage = func() {
		fmt.Fprintln(flags.Output(), "Usage: murder backup [-keep n] -reason TEXT -ticket REF <directory>\n       murder backup [-keep n] -scheduled "+localbackup.Dir)
		fmt.Fprintln(flags.Output(), "\nThe copy holds every tenant's sealed secrets: one made by hand is recorded in\nthe access register. murder-backup.timer's copies the agent keeps itself, in\n"+localbackup.Dir+", for root alone (-scheduled).")
		flags.PrintDefaults()
	}
	positional, err := parseInterspersed(flags, args)
	if err != nil {
		return err
	}
	if len(positional) != 1 || *keep < 0 {
		flags.Usage()
		return flag.ErrHelp
	}
	dir := positional[0]
	proof := reason.proof()
	// murder-backup.service of releases before the register runs this
	// without -scheduled, into the node's backup directory.
	if !*scheduled && !proof.Given() && filepath.Clean(dir) == scheduledDir {
		*scheduled = true
	}
	ctx := context.Background()
	if *scheduled {
		if proof.Given() {
			return errors.New("-scheduled is the timer's copy, recorded nowhere: give it no -reason or -ticket")
		}
		if filepath.Clean(dir) != scheduledDir {
			return fmt.Errorf("the agent keeps scheduled copies in %s, not %s: a copy elsewhere is made by hand, with -reason and -ticket", scheduledDir, dir)
		}
		kept, err := api.NewClient(*socket).ScheduledBackup(ctx, *keep)
		if err != nil {
			return err
		}
		fmt.Fprintf(stdout, "backed up node %s (%d tenants) to %s\n", kept.Node, kept.Tenants, kept.Path)
		return nil
	}
	if _, err := reason.request(access.State, "", ""); err != nil {
		return fmt.Errorf("%w\n(murder-backup.timer's copies are -scheduled)", err)
	}
	client := asOperator(api.NewClient(*socket))
	path, info, err := localbackup.Write(dir, *keep, time.Now(), func(w io.Writer) error { return client.Backup(ctx, w, proof) })
	if err != nil {
		return err
	}
	fmt.Fprintf(stdout, "backed up node %s (%d tenants) to %s\n", info.Node, info.Tenants, path)
	return nil
}

// runRestore puts a backup in place of the node's database while the agent
// is stopped.
func runRestore(args []string) error {
	flags := flag.NewFlagSet("restore", flag.ContinueOnError)
	configPath := flags.String("config", defaultConfig, "agent configuration file")
	flags.Usage = func() {
		fmt.Fprintln(flags.Output(), "Usage: murder restore [-config file] <backup>")
		flags.PrintDefaults()
	}
	positional, err := parseInterspersed(flags, args)
	if err != nil {
		return err
	}
	if len(positional) != 1 {
		flags.Usage()
		return flag.ErrHelp
	}
	from := positional[0]
	cfg, err := config.Load(*configPath)
	if err != nil {
		return err
	}
	// The cluster's log would apply its entries over the backup, or the
	// backup's state would replace the cluster's when the node rejoins.
	if cfg.Cluster != nil {
		if _, err := os.Stat(cfg.Cluster.Dir); err == nil {
			return fmt.Errorf("this node is part of a cluster (its log is in %s): its database comes from the other nodes. "+
				"To rebuild a cluster from a backup, move the cluster directory aside on every node, restore on one node, "+
				"\"murder cluster bootstrap\" there and join the others with new databases", cfg.Cluster.Dir)
		} else if !errors.Is(err, os.ErrNotExist) {
			return err
		}
	}
	info, err := store.CheckBackup(from)
	if err != nil {
		return err
	}
	// Tenants whose secrets the node's key cannot open would not start.
	if info.Secret != nil {
		key, err := secrets.LoadKey(cfg.MasterKeyFile)
		if err != nil {
			return fmt.Errorf("%w (the backup's secrets need the master key of node %s)", err, info.Node)
		}
		if _, err := key.Open(secrets.Context(info.Secret.Tenant, info.Secret.Name), info.Secret.Sealed); err != nil {
			return fmt.Errorf("the master key in %s does not open the backup's secrets: add the master key node %s had then to it, as a line of its own, first", cfg.MasterKeyFile, info.Node)
		}
	}
	kept, err := store.Restore(from, cfg.Database)
	if err != nil {
		if kept != "" {
			return fmt.Errorf("%w; part of the database it was replacing is now at %s*", err, kept)
		}
		return err
	}
	fmt.Fprintf(stdout, "restored node %s (%d tenants) from %s\n", info.Node, info.Tenants, from)
	if kept != "" {
		fmt.Fprintf(stdout, "the database it replaced is kept as %s\n", kept)
	}
	return nil
}

func syncDir(dir string) error {
	directory, err := os.Open(dir)
	if err != nil {
		return err
	}
	defer directory.Close()
	return directory.Sync()
}
