package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"os"
	"path/filepath"
	"regexp"
	"slices"
	"strings"
	"time"

	"github.com/PowerPenguini/murder/internal/api"
	"github.com/PowerPenguini/murder/internal/config"
	"github.com/PowerPenguini/murder/internal/secrets"
	"github.com/PowerPenguini/murder/internal/store"
)

// backupName matches the backups "murder backup" writes; their names sort
// by time.
var backupName = regexp.MustCompile(`^murder-\d{8}T\d{6}Z\.db$`)

// partialPrefix starts the name of a backup being received; staleAfter is
// when one is left from a run that died, not one still running.
const (
	partialPrefix = ".murder-backup-"
	staleAfter    = time.Hour
)

// runBackup asks the agent for a copy of its database and keeps it in a
// directory as murder-<UTC time>.db, removing all but the newest keep.
func runBackup(args []string) error {
	flags := flag.NewFlagSet("backup", flag.ContinueOnError)
	socket := socketPath(flags)
	keep := flags.Int("keep", 48, "backups to keep in the directory, newest first; 0 keeps all")
	flags.Usage = func() {
		fmt.Fprintln(flags.Output(), "Usage: murder backup [-keep n] <directory>")
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
	if err := os.MkdirAll(dir, 0o700); err != nil {
		return err
	}
	removeStalePartials(dir)
	temp, err := os.CreateTemp(dir, partialPrefix+"*")
	if err != nil {
		return err
	}
	defer os.Remove(temp.Name())
	err = api.NewClient(*socket).Backup(context.Background(), temp)
	if err = errors.Join(err, temp.Sync(), temp.Close()); err != nil {
		return fmt.Errorf("back up the database: %w", err)
	}
	info, err := store.CheckBackup(temp.Name())
	if err != nil {
		return fmt.Errorf("the agent's copy is not usable: %w", err)
	}
	path := filepath.Join(dir, "murder-"+time.Now().UTC().Format("20060102T150405Z")+".db")
	if err := os.Rename(temp.Name(), path); err != nil {
		return err
	}
	if err := syncDir(dir); err != nil {
		return err
	}
	fmt.Fprintf(stdout, "backed up node %s (%d tenants) to %s\n", info.Node, info.Tenants, path)
	if *keep == 0 {
		return nil
	}
	return pruneBackups(dir, filepath.Base(path), *keep)
}

// pruneBackups removes all but the newest keep backups in dir, never the one
// just written (a clock set back would make it sort first), nor files of
// other names. Another run may have removed some already.
func pruneBackups(dir, written string, keep int) error {
	entries, err := os.ReadDir(dir)
	if err != nil {
		return err
	}
	var backups []string
	for _, entry := range entries {
		if backupName.MatchString(entry.Name()) && entry.Name() != written {
			backups = append(backups, entry.Name())
		}
	}
	slices.Sort(backups)
	for _, old := range backups[:max(len(backups)-(keep-1), 0)] {
		if err := os.Remove(filepath.Join(dir, old)); err != nil && !errors.Is(err, os.ErrNotExist) {
			return fmt.Errorf("remove an old backup: %w", err)
		}
	}
	return nil
}

// removeStalePartials removes backups a run that died left half received.
func removeStalePartials(dir string) {
	entries, err := os.ReadDir(dir)
	if err != nil {
		return
	}
	for _, entry := range entries {
		info, err := entry.Info()
		if err == nil && strings.HasPrefix(entry.Name(), partialPrefix) && time.Since(info.ModTime()) > staleAfter {
			_ = os.Remove(filepath.Join(dir, entry.Name()))
		}
	}
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
