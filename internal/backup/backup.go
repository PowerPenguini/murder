package backup

import (
	"archive/tar"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"io/fs"
	"log/slog"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"time"

	"filippo.io/age"
	"github.com/klauspost/compress/zstd"

	"github.com/PowerPenguini/murder/internal/volume"
)

// Mode says whether a bucket keeps backups under lock.
type Mode string

const (
	// Test keeps no lock: everything, the bucket too, can be deleted at
	// once.
	Test Mode = "test"
	// Production locks every object (compliance mode): no one deletes it,
	// or shortens its lock, before the lock ends.
	Production Mode = "production"
)

// Policy says how long backups stay. Every backup is a whole copy of the
// tenant's data, so they are thinned out as they age: every one for
// KeepHourly, the first of each day for KeepDaily, the first of each month
// for KeepMonthly.
type Policy struct {
	Mode                               Mode
	KeepHourly, KeepDaily, KeepMonthly time.Duration
	// LockFor locks, in production, the first backup of each day (and so of
	// each month); the others are locked for as long as they are kept,
	// KeepHourly, if that is shorter. At most KeepDaily, so retention can
	// delete what it drops.
	LockFor time.Duration
	// KeepLocal is how many backup snapshots each tenant keeps on its node,
	// to go back to in a moment.
	KeepLocal int
}

// Validate checks the policy against its mode.
func (p Policy) Validate() error {
	switch p.Mode {
	case Test:
		if p.LockFor != 0 {
			return errors.New("test backups are not locked: set no lock in test mode")
		}
	case Production:
		if p.LockFor <= 0 {
			return errors.New("production backups need a lock")
		}
		if p.LockFor > p.KeepDaily {
			return fmt.Errorf("the lock (%s) outlasts keeping daily backups (%s): retention could not delete what it drops", p.LockFor, p.KeepDaily)
		}
	default:
		return fmt.Errorf("backup mode %q: test or production", p.Mode)
	}
	if p.KeepHourly <= 0 || p.KeepDaily < p.KeepHourly || p.KeepMonthly < p.KeepDaily || p.KeepLocal < 1 {
		return errors.New("backups need keepHourly, keepDaily (at least keepHourly), keepMonthly (at least keepDaily) and keepLocal (at least 1)")
	}
	return nil
}

// lockFor is how long a backup taken at, the first of its day or not, is
// locked.
func (p Policy) lockFor(firstOfDay bool) time.Duration {
	if p.Mode != Production {
		return 0
	}
	if firstOfDay {
		return p.LockFor
	}
	return min(p.LockFor, p.KeepHourly)
}

// Volumes is what backups need of the volume driver.
type Volumes interface {
	Snapshot(ctx context.Context, tenant, reason string) (string, error)
	Prune(ctx context.Context, tenant, reason string, keep int) error
	volume.Backups
}

// Backups backs up the tenants of this node, and the cluster's state.
type Backups struct {
	Store   Store
	Volumes Volumes
	// Recipient encrypts every backup; its identity, which decrypts them,
	// is kept offline, or on a node that verifies backups.
	Recipient age.Recipient
	Policy    Policy
	Log       *slog.Logger
	Now       func() time.Time
}

func (b *Backups) now() time.Time {
	if b.Now != nil {
		return b.Now().UTC()
	}
	return time.Now().UTC()
}

func (b *Backups) log() *slog.Logger {
	if b.Log != nil {
		return b.Log
	}
	return slog.New(slog.DiscardHandler)
}

// stamp is the time format of Murder's snapshot names and of object keys.
const stamp = "20060102T150405.000Z"

const (
	snapshotReason = "backup"
	// A tenant's backup is tenants/<tenant>/<time>.tar.zst.age: the files
	// of its data at that time, as tar, compressed with zstd, encrypted
	// with age. Any one restores alone, with age, zstd and tar.
	tenantSuffix = ".tar.zst.age"
	// A copy of the cluster's state is cluster/<time>.db.zst.age.
	statePrefix = "cluster/"
	stateSuffix = ".db.zst.age"
)

func tenantPrefix(tenant string) string { return "tenants/" + tenant + "/" }

// Backup is one backup.
type Backup struct {
	At   time.Time
	Key  string
	Size int64
}

// list returns the backups below prefix ending in suffix, oldest first.
func (b *Backups) list(ctx context.Context, prefix, suffix string) ([]Backup, error) {
	objects, err := b.Store.List(ctx, prefix)
	if err != nil {
		return nil, err
	}
	var backups []Backup
	for _, object := range objects {
		name, ok := strings.CutSuffix(strings.TrimPrefix(object.Key, prefix), suffix)
		if !ok {
			continue
		}
		at, err := time.Parse(stamp, name)
		if err != nil {
			continue
		}
		backups = append(backups, Backup{At: at, Key: object.Key, Size: object.Size})
	}
	sort.Slice(backups, func(i, j int) bool { return backups[i].At.Before(backups[j].At) })
	return backups, nil
}

// List returns a tenant's backups, oldest first.
func (b *Backups) List(ctx context.Context, tenant string) ([]Backup, error) {
	return b.list(ctx, tenantPrefix(tenant), tenantSuffix)
}

// firstOfDay reports whether a backup at would be the first of its day.
func firstOfDay(existing []Backup, at time.Time) bool {
	day := at.Format("2006-01-02")
	for _, backup := range existing {
		if backup.At.Format("2006-01-02") == day && backup.At.Before(at) {
			return false
		}
	}
	return true
}

// BackUp backs a tenant up, whose data is dataset: a snapshot, so the copy
// is of one moment with nothing stopped, and every file of it, compressed
// and encrypted, as one object.
func (b *Backups) BackUp(ctx context.Context, tenant, dataset string) (Backup, error) {
	existing, err := b.List(ctx, tenant)
	if err != nil {
		return Backup{}, err
	}
	snapshot, err := b.Volumes.Snapshot(ctx, tenant, snapshotReason)
	if err != nil {
		return Backup{}, err
	}
	taken, ok := strings.CutPrefix(snapshot, dataset+"@murder-"+snapshotReason+"-")
	if !ok {
		return Backup{}, fmt.Errorf("snapshot %s is not a backup snapshot of %s", snapshot, dataset)
	}
	at, err := time.Parse(stamp, taken)
	if err != nil {
		return Backup{}, err
	}
	dir, err := b.Volumes.SnapshotDir(ctx, tenant, snapshot)
	if err != nil {
		return Backup{}, err
	}
	key := tenantPrefix(tenant) + taken + tenantSuffix
	written, err := b.upload(ctx, key, b.Policy.lockFor(firstOfDay(existing, at)), func(w io.Writer) error {
		return volume.WriteTar(ctx, dir, w)
	})
	if err != nil {
		return Backup{}, fmt.Errorf("back up %s: %w", tenant, err)
	}
	if err := b.Volumes.Prune(ctx, tenant, snapshotReason, b.Policy.KeepLocal); err != nil {
		b.log().Warn("prune backup snapshots", "tenant", tenant, "error", err)
	}
	return Backup{At: at, Key: key, Size: written}, nil
}

// counter counts bytes written through it.
type counter struct {
	w io.Writer
	n int64
}

func (c *counter) Write(p []byte) (int, error) {
	n, err := c.w.Write(p)
	c.n += int64(n)
	return n, err
}

// upload compresses and encrypts what write writes, stores it at key,
// locked for lock, and returns how many bytes it stored.
func (b *Backups) upload(ctx context.Context, key string, lock time.Duration, write func(io.Writer) error) (int64, error) {
	reader, writer := io.Pipe()
	stored := &counter{w: writer}
	written := make(chan error, 1)
	go func() {
		err := func() error {
			encrypted, err := age.Encrypt(stored, b.Recipient)
			if err != nil {
				return err
			}
			compressed, err := zstd.NewWriter(encrypted, zstd.WithEncoderLevel(zstd.SpeedDefault))
			if err != nil {
				return err
			}
			if err := write(compressed); err != nil {
				compressed.Close()
				return err
			}
			if err := compressed.Close(); err != nil {
				return err
			}
			return encrypted.Close()
		}()
		_ = writer.CloseWithError(err)
		written <- err
	}()
	var until time.Time
	if lock > 0 {
		until = b.now().Add(lock)
	}
	put := b.Store.Put(ctx, key, reader, until)
	_ = reader.CloseWithError(put)
	return stored.n, errors.Join(<-written, put)
}

// open returns the decrypted, decompressed content of the object at key.
func (b *Backups) open(ctx context.Context, key string, identity age.Identity) (io.ReadCloser, error) {
	object, err := b.Store.Get(ctx, key)
	if err != nil {
		return nil, fmt.Errorf("get %s: %w", key, err)
	}
	decrypted, err := age.Decrypt(object, identity)
	if err != nil {
		object.Close()
		return nil, fmt.Errorf("decrypt %s: %w", key, err)
	}
	decompressed, err := zstd.NewReader(decrypted)
	if err != nil {
		object.Close()
		return nil, err
	}
	return readCloser{Reader: decompressed.IOReadCloser(), close: object.Close}, nil
}

type readCloser struct {
	io.Reader
	close func() error
}

func (r readCloser) Close() error {
	if closer, ok := r.Reader.(io.Closer); ok {
		closer.Close()
	}
	return r.close()
}

// keep decides which backups stay: every one younger than KeepHourly, the
// first of each day younger than KeepDaily, the first of each month younger
// than KeepMonthly.
func (b *Backups) keep(backups []Backup) map[string]bool {
	now := b.now()
	kept := map[string]bool{}
	days, months := map[string]bool{}, map[string]bool{}
	for _, backup := range backups { // oldest first: the first of a day is met first
		age := now.Sub(backup.At)
		day, month := backup.At.Format("2006-01-02"), backup.At.Format("2006-01")
		firstOfDay, firstOfMonth := !days[day], !months[month]
		days[day], months[month] = true, true
		if age < b.Policy.KeepHourly || (firstOfDay && age < b.Policy.KeepDaily) || (firstOfMonth && age < b.Policy.KeepMonthly) {
			kept[backup.Key] = true
		}
	}
	return kept
}

func (b *Backups) prune(ctx context.Context, backups []Backup) (int, error) {
	kept := b.keep(backups)
	deleted := 0
	var errs []error
	for _, backup := range backups {
		if kept[backup.Key] {
			continue
		}
		if err := b.Store.Delete(ctx, backup.Key); err != nil {
			errs = append(errs, fmt.Errorf("%s: %w", backup.Key, err))
			continue
		}
		deleted++
	}
	return deleted, errors.Join(errs...)
}

// Prune deletes the tenant's backups the policy no longer keeps. In
// production, one still locked is left for a later prune.
func (b *Backups) Prune(ctx context.Context, tenant string) (int, error) {
	backups, err := b.List(ctx, tenant)
	if err != nil {
		return 0, err
	}
	return b.prune(ctx, backups)
}

// at returns the newest backup at or before when. A time given to the
// second, as people give it, takes in that whole second: backups are timed
// to the millisecond.
func at(backups []Backup, when time.Time) (Backup, error) {
	if when.Nanosecond() == 0 {
		when = when.Add(time.Second - time.Nanosecond)
	}
	for i := len(backups) - 1; i >= 0; i-- {
		if !backups[i].At.After(when) {
			return backups[i], nil
		}
	}
	return Backup{}, fmt.Errorf("no backup at or before %s", when.Format(time.RFC3339))
}

// Verified reports a verification.
type Verified struct {
	Backup Backup
	// Files and Bytes are what the backup holds.
	Files int
	Bytes int64
	// Compared says whether every file was compared with the snapshot the
	// backup was made of, still here, and found the same.
	Compared bool
}

// Verify reads a tenant's newest backup through: decrypted (age checks
// every chunk), decompressed (zstd checks its frames) and every file of
// it; and, when the snapshot it was made of is still here, checks that
// it holds the same files with the same content.
func (b *Backups) Verify(ctx context.Context, tenant, dataset string, identity age.Identity) (Verified, error) {
	backups, err := b.List(ctx, tenant)
	if err != nil {
		return Verified{}, err
	}
	newest, err := at(backups, b.now())
	if err != nil {
		return Verified{}, err
	}
	content, err := b.open(ctx, newest.Key, identity)
	if err != nil {
		return Verified{}, err
	}
	defer content.Close()
	sums := map[string]string{}
	result := Verified{Backup: newest}
	archive := tar.NewReader(content)
	for {
		header, err := archive.Next()
		if errors.Is(err, io.EOF) {
			break
		}
		if err != nil {
			return result, fmt.Errorf("read %s: %w", newest.Key, err)
		}
		if header.Typeflag != tar.TypeReg {
			continue
		}
		hash := sha256.New()
		n, err := io.Copy(hash, archive)
		if err != nil {
			return result, fmt.Errorf("read %s from %s: %w", header.Name, newest.Key, err)
		}
		sums[filepath.Clean(header.Name)] = hex.EncodeToString(hash.Sum(nil))
		result.Files++
		result.Bytes += n
	}
	snapshot := dataset + "@murder-" + snapshotReason + "-" + newest.At.Format(stamp)
	dir, err := b.Volumes.SnapshotDir(ctx, tenant, snapshot)
	if err != nil {
		return result, nil // no longer here to compare with
	}
	if _, err := os.Stat(dir); err != nil {
		return result, nil
	}
	here := map[string]string{}
	err = filepath.WalkDir(dir, func(path string, entry fs.DirEntry, err error) error {
		if err != nil || !entry.Type().IsRegular() {
			return err
		}
		file, err := os.Open(path)
		if err != nil {
			return err
		}
		defer file.Close()
		hash := sha256.New()
		if _, err := io.Copy(hash, file); err != nil {
			return err
		}
		relative, _ := filepath.Rel(dir, path)
		here[relative] = hex.EncodeToString(hash.Sum(nil))
		return nil
	})
	if err != nil {
		return result, fmt.Errorf("read snapshot %s: %w", snapshot, err)
	}
	if len(here) != len(sums) {
		return result, fmt.Errorf("the backup holds %d files, its snapshot %d", len(sums), len(here))
	}
	for name, sum := range here {
		if sums[name] != sum {
			return result, fmt.Errorf("%s differs from its snapshot in the backup", name)
		}
	}
	result.Compared = true
	return result, nil
}

// Restore puts a tenant's data back as its newest backup at or before
// when. What the tenant had stays, as the returned dataset. The tenant must
// not run meanwhile.
func (b *Backups) Restore(ctx context.Context, tenant string, when time.Time, identity age.Identity) (Backup, string, error) {
	backups, err := b.List(ctx, tenant)
	if err != nil {
		return Backup{}, "", err
	}
	chosen, err := at(backups, when)
	if err != nil {
		return Backup{}, "", err
	}
	name := volume.CopyName(tenant, "restore-"+strings.ToLower(b.now().Format("20060102T150405")))
	path, err := b.Volumes.CreateCopy(ctx, tenant, name)
	if err != nil {
		return Backup{}, "", err
	}
	failed := func(err error) (Backup, string, error) {
		_ = b.Volumes.DestroyCopy(context.WithoutCancel(ctx), name)
		return Backup{}, "", err
	}
	content, err := b.open(ctx, chosen.Key, identity)
	if err != nil {
		return failed(err)
	}
	err = volume.ReadTar(ctx, path, content)
	content.Close()
	if err != nil {
		return failed(fmt.Errorf("unpack %s: %w", chosen.Key, err))
	}
	aside, err := b.Volumes.ReplaceWithCopy(ctx, tenant, name)
	if err != nil {
		return Backup{}, aside, err
	}
	return chosen, aside, nil
}

// BackUpState stores a copy of the cluster's state, the database file at
// path.
func (b *Backups) BackUpState(ctx context.Context, path string) (Backup, error) {
	file, err := os.Open(path)
	if err != nil {
		return Backup{}, err
	}
	defer file.Close()
	existing, err := b.list(ctx, statePrefix, stateSuffix)
	if err != nil {
		return Backup{}, err
	}
	now := b.now()
	key := statePrefix + now.Format(stamp) + stateSuffix
	written, err := b.upload(ctx, key, b.Policy.lockFor(firstOfDay(existing, now)), func(w io.Writer) error {
		_, err := io.Copy(w, file)
		return err
	})
	return Backup{At: now, Key: key, Size: written}, err
}

// States lists the copies of the cluster's state, oldest first.
func (b *Backups) States(ctx context.Context) ([]Backup, error) {
	return b.list(ctx, statePrefix, stateSuffix)
}

// PruneState deletes the copies of the state the policy no longer keeps.
func (b *Backups) PruneState(ctx context.Context) (int, error) {
	states, err := b.States(ctx)
	if err != nil {
		return 0, err
	}
	return b.prune(ctx, states)
}

// GetState writes the copy of the state at key, decrypted, to w.
func (b *Backups) GetState(ctx context.Context, key string, identity age.Identity, w io.Writer) error {
	content, err := b.open(ctx, key, identity)
	if err != nil {
		return err
	}
	defer content.Close()
	_, err = io.Copy(w, content)
	return err
}
