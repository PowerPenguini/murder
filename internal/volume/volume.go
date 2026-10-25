// Package volume gives every tenant its own data directory: a plain directory
// for development, or a ZFS dataset that can be snapshotted and replicated.
package volume

import (
	"context"
	"errors"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"time"

	"github.com/PowerPenguini/corral/internal/spec"
)

// ErrSnapshotsUnsupported is returned by drivers that cannot snapshot.
var ErrSnapshotsUnsupported = errors.New("volume driver does not support snapshots")

// Volumes manages tenant data directories. The directories above them are
// private to root, so no host user can reach into a tenant's data; a setuid
// file a tenant writes there is useless to them.
type Volumes interface {
	// Ensure creates the tenant's data directory if needed, owned by uid:gid,
	// and returns its host path. mounted reports that the data had to be
	// mounted just now, so a container started earlier sees the empty
	// directory underneath and must be recreated.
	Ensure(ctx context.Context, tenant string, uid, gid int) (path string, mounted bool, err error)
	// Exists reports whether the tenant has data, for example left behind by
	// a deleted tenant of the same name.
	Exists(ctx context.Context, tenant string) (bool, error)
	// Snapshot records the data as it is now and returns the snapshot name.
	Snapshot(ctx context.Context, tenant, reason string) (string, error)
	// Remove deletes the tenant's data and snapshots.
	Remove(ctx context.Context, tenant string) error
}

func checkTenant(tenant string) error { return spec.ValidLabel(tenant) }

// Dir keeps each tenant's data in a directory below Root. It cannot snapshot.
type Dir struct {
	Root string
}

func (d Dir) path(tenant string) string { return filepath.Join(d.Root, tenant) }

func (d Dir) Ensure(_ context.Context, tenant string, uid, gid int) (string, bool, error) {
	if err := checkTenant(tenant); err != nil {
		return "", false, err
	}
	if err := privateDir(d.Root); err != nil {
		return "", false, err
	}
	path := d.path(tenant)
	if err := os.Mkdir(path, 0o700); err != nil && !errors.Is(err, os.ErrExist) {
		return "", false, err
	}
	return path, false, chownIfNeeded(path, uid, gid)
}

func (d Dir) Exists(_ context.Context, tenant string) (bool, error) {
	if err := checkTenant(tenant); err != nil {
		return false, err
	}
	_, err := os.Stat(d.path(tenant))
	if errors.Is(err, os.ErrNotExist) {
		return false, nil
	}
	return err == nil, err
}

func (d Dir) Snapshot(context.Context, string, string) (string, error) {
	return "", ErrSnapshotsUnsupported
}

func (d Dir) Remove(_ context.Context, tenant string) error {
	if err := checkTenant(tenant); err != nil {
		return err
	}
	return os.RemoveAll(d.path(tenant))
}

// privateDir creates dir if needed and makes it accessible to its owner only.
func privateDir(dir string) error {
	if err := os.MkdirAll(dir, 0o700); err != nil {
		return err
	}
	return os.Chmod(dir, 0o700)
}

// chownIfNeeded hands the directory root to the app's user. Only the
// directory itself is changed; the app owns what it writes inside.
func chownIfNeeded(path string, uid, gid int) error {
	info, err := os.Stat(path)
	if err != nil {
		return err
	}
	if owner, ok := fileOwner(info); ok && owner.uid == uid && owner.gid == gid {
		return nil
	}
	if err := os.Chown(path, uid, gid); err != nil {
		return fmt.Errorf("give %s to %d:%d: %w", path, uid, gid, err)
	}
	return nil
}

// Runner runs a command and returns its combined output.
type Runner func(ctx context.Context, name string, args ...string) ([]byte, error)

// Exec runs commands on the host, giving each at most five minutes.
func Exec(ctx context.Context, name string, args ...string) ([]byte, error) {
	ctx, cancel := context.WithTimeout(ctx, 5*time.Minute)
	defer cancel()
	output, err := exec.CommandContext(ctx, name, args...).CombinedOutput()
	if err != nil {
		return output, fmt.Errorf("%s %s: %w: %s", name, strings.Join(args, " "), err, strings.TrimSpace(string(output)))
	}
	return output, nil
}

// ownerProperty marks datasets Corral created; Corral never touches a
// dataset without it, even below its own parent.
const ownerProperty = "io.corral:tenant"

// ZFS keeps each tenant in a dataset below Parent, mounted below MountRoot.
type ZFS struct {
	Parent    string
	MountRoot string
	Run       Runner
	Now       func() time.Time
}

func (z ZFS) dataset(tenant string) string { return z.Parent + "/" + tenant }

func (z ZFS) now() time.Time {
	if z.Now != nil {
		return z.Now()
	}
	return time.Now()
}

// properties returns the dataset's owner, mounted state and mountpoint, or
// ok=false if the dataset does not exist.
func (z ZFS) properties(ctx context.Context, dataset string) (owner, mounted, mountpoint string, ok bool, err error) {
	output, err := z.Run(ctx, "zfs", "get", "-H", "-p", "-o", "value", ownerProperty+",mounted,mountpoint", dataset)
	if err != nil {
		if strings.Contains(string(output), "does not exist") {
			return "", "", "", false, nil
		}
		return "", "", "", false, err
	}
	values := strings.Split(strings.TrimRight(string(output), "\n"), "\n")
	if len(values) != 3 {
		return "", "", "", false, fmt.Errorf("zfs get %s: unexpected output %q", dataset, output)
	}
	return values[0], values[1], values[2], true, nil
}

// owned checks that dataset was made by Corral for tenant and is mounted
// where Corral expects; anything else is refused rather than used.
func (z ZFS) owned(tenant, owner, mountpoint string) error {
	want := filepath.Join(z.MountRoot, tenant)
	if owner != tenant {
		return fmt.Errorf("dataset %s was not created by corral for tenant %s (%s=%q)", z.dataset(tenant), tenant, ownerProperty, owner)
	}
	if mountpoint != want {
		return fmt.Errorf("dataset %s is mounted at %s, want %s", z.dataset(tenant), mountpoint, want)
	}
	return nil
}

func (z ZFS) Ensure(ctx context.Context, tenant string, uid, gid int) (string, bool, error) {
	if err := checkTenant(tenant); err != nil {
		return "", false, err
	}
	if err := privateDir(z.MountRoot); err != nil {
		return "", false, err
	}
	dataset := z.dataset(tenant)
	mountpoint := filepath.Join(z.MountRoot, tenant)
	owner, mounted, current, exists, err := z.properties(ctx, dataset)
	if err != nil {
		return "", false, err
	}
	if !exists {
		if _, err := z.Run(ctx, "zfs", "create", "-p",
			"-o", "mountpoint="+mountpoint, "-o", "setuid=off", "-o", "devices=off",
			"-o", ownerProperty+"="+tenant, dataset); err != nil {
			return "", false, err
		}
		return mountpoint, false, chownIfNeeded(mountpoint, uid, gid)
	}
	if err := z.owned(tenant, owner, current); err != nil {
		return "", false, err
	}
	mountedNow := false
	if mounted != "yes" {
		if _, err := z.Run(ctx, "zfs", "mount", dataset); err != nil {
			return "", false, err
		}
		mountedNow = true
	}
	return mountpoint, mountedNow, chownIfNeeded(mountpoint, uid, gid)
}

func (z ZFS) Exists(ctx context.Context, tenant string) (bool, error) {
	if err := checkTenant(tenant); err != nil {
		return false, err
	}
	_, _, _, exists, err := z.properties(ctx, z.dataset(tenant))
	return exists, err
}

// Snapshot names snapshots "corral-<reason>-<UTC time>" so they sort by time.
func (z ZFS) Snapshot(ctx context.Context, tenant, reason string) (string, error) {
	if err := checkTenant(tenant); err != nil {
		return "", err
	}
	if !snapshotReason(reason) {
		return "", fmt.Errorf("snapshot reason %q must be lowercase letters and hyphens", reason)
	}
	name := z.dataset(tenant) + "@corral-" + reason + "-" + z.now().UTC().Format("20060102T150405.000Z")
	if _, err := z.Run(ctx, "zfs", "snapshot", name); err != nil {
		return "", err
	}
	return name, nil
}

func snapshotReason(reason string) bool {
	if reason == "" {
		return false
	}
	for _, r := range reason {
		if (r < 'a' || r > 'z') && r != '-' {
			return false
		}
	}
	return true
}

// Remove destroys the tenant's dataset only if Corral created it for this
// tenant and it is mounted where Corral put it.
func (z ZFS) Remove(ctx context.Context, tenant string) error {
	if err := checkTenant(tenant); err != nil {
		return err
	}
	owner, _, mountpoint, exists, err := z.properties(ctx, z.dataset(tenant))
	if err != nil || !exists {
		return err
	}
	if err := z.owned(tenant, owner, mountpoint); err != nil {
		return err
	}
	_, err = z.Run(ctx, "zfs", "destroy", "-r", z.dataset(tenant))
	return err
}
