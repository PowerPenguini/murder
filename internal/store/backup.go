package store

import (
	"database/sql"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"
	"syscall"
	"time"
)

// Backup writes a copy of the database to w as it is at one moment: a plain
// copy of a live database can be torn. The copy is made in a private file
// next to the database and checked before any of it is written. It is one
// self-contained file, without a write-ahead log.
func (s *SQLite) Backup(w io.Writer) error {
	temp, err := os.CreateTemp(filepath.Dir(s.path), filepath.Base(s.path)+".backup-*")
	if err != nil {
		return err
	}
	name := temp.Name()
	_ = temp.Close()
	defer removeDatabase(name)
	if _, err := s.db.Exec(`VACUUM INTO ?`, name); err != nil {
		return fmt.Errorf("copy the database: %w", err)
	}
	if err := selfContained(name); err != nil {
		return fmt.Errorf("check the copy: %w", err)
	}
	file, err := os.Open(name)
	if err != nil {
		return err
	}
	defer file.Close()
	_, err = io.Copy(w, file)
	return err
}

// selfContained switches a database copy to a rollback journal, so it is
// one file, and checks it.
func selfContained(path string) error {
	db, err := sql.Open("sqlite", uri(path, "_pragma=journal_mode(DELETE)"))
	if err != nil {
		return err
	}
	return errors.Join(check(db), db.Close())
}

func check(db *sql.DB) error {
	var result string
	if err := db.QueryRow(`PRAGMA quick_check`).Scan(&result); err != nil {
		return err
	}
	if result != "ok" {
		return fmt.Errorf("the database is damaged: %s", result)
	}
	return checkSchema(db)
}

// checkpoint writes a database's log into it and empties the log.
func checkpoint(path string) error {
	db, err := sql.Open("sqlite", uri(path, "_pragma=busy_timeout(5000)"))
	if err != nil {
		return err
	}
	_, err = db.Exec(`PRAGMA wal_checkpoint(TRUNCATE)`)
	return errors.Join(err, db.Close())
}

// removeStale removes the temporary copies Backup and Restore make beside
// the database at path, left by a crash. Only the holder of the
// database's lock calls it: nothing else makes them.
func removeStale(path string) {
	entries, err := os.ReadDir(filepath.Dir(path))
	if err != nil {
		return
	}
	base := filepath.Base(path)
	for _, entry := range entries {
		if strings.HasPrefix(entry.Name(), base+".backup-") || strings.HasPrefix(entry.Name(), base+".restore-") {
			removeDatabase(filepath.Join(filepath.Dir(path), entry.Name()))
		}
	}
}

// removeDatabase removes a database file and any journal SQLite left.
func removeDatabase(path string) {
	for _, suffix := range []string{"", "-journal", "-wal", "-shm"} {
		_ = os.Remove(path + suffix)
	}
}

// BackupInfo describes a backup.
type BackupInfo struct {
	Node    string
	Tenants int
	// Secret is one sealed secret, if the backup holds any, to check that
	// the node's master key opens it.
	Secret *SealedSecret
}

// SealedSecret is a secret as the database keeps it.
type SealedSecret struct {
	Tenant, Name, Sealed string
}

// CheckBackup checks that the file at path is a whole database this release
// runs on, without changing it, and describes it.
func CheckBackup(path string) (BackupInfo, error) {
	if _, err := os.Stat(path); err != nil {
		return BackupInfo{}, err
	}
	db, err := sql.Open("sqlite", uri(path, "mode=ro"))
	if err != nil {
		return BackupInfo{}, err
	}
	defer db.Close()
	if err := check(db); err != nil {
		return BackupInfo{}, fmt.Errorf("%s: %w", path, err)
	}
	var info BackupInfo
	if err := db.QueryRow(`SELECT value FROM meta WHERE key = 'node'`).Scan(&info.Node); err != nil {
		return BackupInfo{}, fmt.Errorf("%s: read the node name: %w", path, err)
	}
	if err := db.QueryRow(`SELECT count(*) FROM tenants`).Scan(&info.Tenants); err != nil {
		return BackupInfo{}, fmt.Errorf("%s: %w", path, err)
	}
	var secret SealedSecret
	switch err := db.QueryRow(`SELECT tenant, name, sealed FROM secrets LIMIT 1`).Scan(&secret.Tenant, &secret.Name, &secret.Sealed); {
	case errors.Is(err, sql.ErrNoRows):
	case err != nil:
		return BackupInfo{}, fmt.Errorf("%s: %w", path, err)
	default:
		info.Secret = &secret
	}
	return info, nil
}

// Restore puts the backup at from in place of the database at path. The
// agent must be stopped: Restore takes its lock. The database it replaces,
// if any, is kept beside it, with its write-ahead log, under the name
// returned, also with an error if Restore stopped while moving it; the log
// is never left to be applied to the restored copy.
func Restore(from, path string) (kept string, err error) {
	dir := filepath.Dir(path)
	if err := os.MkdirAll(dir, 0o700); err != nil {
		return "", err
	}
	lock, err := os.OpenFile(path+".lock", os.O_CREATE|os.O_RDWR, 0o600)
	if err != nil {
		return "", err
	}
	defer lock.Close()
	if err := syscall.Flock(int(lock.Fd()), syscall.LOCK_EX|syscall.LOCK_NB); err != nil {
		return "", fmt.Errorf("database %s is in use by the agent; stop it first", path)
	}
	removeStale(path)
	// The copy is complete on disk before anything is moved.
	temp, err := os.CreateTemp(dir, filepath.Base(path)+".restore-*")
	if err != nil {
		return "", err
	}
	defer os.Remove(temp.Name())
	source, err := os.Open(from)
	if err != nil {
		_ = temp.Close()
		return "", err
	}
	_, err = io.Copy(temp, source)
	_ = source.Close()
	if err = errors.Join(err, temp.Sync(), temp.Close()); err != nil {
		return "", err
	}
	// What is checked is what is put in place.
	if _, err := CheckBackup(temp.Name()); err != nil {
		return "", fmt.Errorf("backup %s cannot be restored: %w", from, err)
	}
	if _, err := os.Stat(path); err == nil {
		// Fold the log into the database first, so the one kept is whole
		// even if the moves below are interrupted. A database too damaged
		// for that keeps its log beside it.
		_ = checkpoint(path)
	}
	// Whatever is there goes aside, the log even without its database: a
	// log left beside the restored copy would be applied to it.
	name := path + ".before-restore-" + time.Now().UTC().Format("20060102T150405.000000000Z")
	for _, suffix := range []string{"-wal", "-shm", "-journal", ""} {
		err := os.Rename(path+suffix, name+suffix)
		switch {
		case err == nil:
			kept = name
		case !errors.Is(err, os.ErrNotExist):
			// What was moved stays where it went; the caller says where.
			return kept, fmt.Errorf("keep the current database: %w", err)
		}
	}
	if err := os.Rename(temp.Name(), path); err != nil {
		return kept, err
	}
	return kept, syncDir(dir)
}

// DatabaseSchema returns the schema version of the database at path,
// reading it only.
func DatabaseSchema(path string) (int, error) {
	db, err := sql.Open("sqlite", uri(path, "mode=ro"))
	if err != nil {
		return 0, err
	}
	defer db.Close()
	var version int
	if err := db.QueryRow(`PRAGMA user_version`).Scan(&version); err != nil {
		return 0, fmt.Errorf("database %s: %w", path, err)
	}
	return version, nil
}
