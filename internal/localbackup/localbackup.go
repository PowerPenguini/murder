// Package localbackup keeps copies of a node's database in a directory,
// as murder-<UTC time>.db, the newest so many of them.
package localbackup

import (
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"regexp"
	"slices"
	"strings"
	"time"

	"github.com/PowerPenguini/murder/internal/store"
)

// Dir is where murder-backup.timer has the agent keep copies of the
// database, readable by root alone. They are the system's own keeping,
// recorded in no access register: they stay on the node, as the database
// does.
const Dir = "/var/backups/murder"

// name matches the copies Write makes; their names sort by time.
var name = regexp.MustCompile(`^murder-\d{8}T\d{6}Z\.db$`)

// partialPrefix starts the name of a copy being received; staleAfter is
// when one is left from a run that died, not one still running.
const (
	partialPrefix = ".murder-backup-"
	staleAfter    = time.Hour
)

// Write has copy write a copy of the database into dir, checks it, keeps
// it as murder-<UTC time>.db and removes all but the newest keep (0 keeps
// all). It returns where the copy is and what it holds.
func Write(dir string, keep int, now time.Time, copy func(io.Writer) error) (string, store.BackupInfo, error) {
	if err := os.MkdirAll(dir, 0o700); err != nil {
		return "", store.BackupInfo{}, err
	}
	removeStalePartials(dir, now)
	temp, err := os.CreateTemp(dir, partialPrefix+"*")
	if err != nil {
		return "", store.BackupInfo{}, err
	}
	defer os.Remove(temp.Name())
	err = copy(temp)
	if err = errors.Join(err, temp.Sync(), temp.Close()); err != nil {
		return "", store.BackupInfo{}, fmt.Errorf("back up the database: %w", err)
	}
	info, err := store.CheckBackup(temp.Name())
	if err != nil {
		return "", store.BackupInfo{}, fmt.Errorf("the agent's copy is not usable: %w", err)
	}
	path := filepath.Join(dir, "murder-"+now.UTC().Format("20060102T150405Z")+".db")
	if err := os.Rename(temp.Name(), path); err != nil {
		return "", store.BackupInfo{}, err
	}
	if err := syncDir(dir); err != nil {
		return "", store.BackupInfo{}, err
	}
	if keep == 0 {
		return path, info, nil
	}
	return path, info, prune(dir, filepath.Base(path), keep)
}

// prune removes all but the newest keep copies in dir, never the one just
// written (a clock set back would make it sort first), nor files of other
// names. Another run may have removed some already.
func prune(dir, written string, keep int) error {
	entries, err := os.ReadDir(dir)
	if err != nil {
		return err
	}
	var copies []string
	for _, entry := range entries {
		if name.MatchString(entry.Name()) && entry.Name() != written {
			copies = append(copies, entry.Name())
		}
	}
	slices.Sort(copies)
	for _, old := range copies[:max(len(copies)-(keep-1), 0)] {
		if err := os.Remove(filepath.Join(dir, old)); err != nil && !errors.Is(err, os.ErrNotExist) {
			return fmt.Errorf("remove an old backup: %w", err)
		}
	}
	return nil
}

// removeStalePartials removes copies a run that died left half received.
func removeStalePartials(dir string, now time.Time) {
	entries, err := os.ReadDir(dir)
	if err != nil {
		return
	}
	for _, entry := range entries {
		info, err := entry.Info()
		if err == nil && strings.HasPrefix(entry.Name(), partialPrefix) && now.Sub(info.ModTime()) > staleAfter {
			_ = os.Remove(filepath.Join(dir, entry.Name()))
		}
	}
}

func syncDir(dir string) error {
	directory, err := os.Open(dir)
	if err != nil {
		return err
	}
	defer directory.Close()
	return directory.Sync()
}
