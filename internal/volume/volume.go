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

// Volumes manages tenant data directories.
type Volumes interface {
	// Ensure creates the tenant's data directory if needed, owned by uid:gid,
	// and returns its host path.
	Ensure(ctx context.Context, tenant string, uid, gid int) (string, error)
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

func (d Dir) Ensure(_ context.Context, tenant string, uid, gid int) (string, error) {
	if err := checkTenant(tenant); err != nil {
		return "", err
	}
	if err := os.MkdirAll(d.Root, 0o711); err != nil {
		return "", err
	}
	path := d.path(tenant)
	if err := os.Mkdir(path, 0o700); err != nil && !errors.Is(err, os.ErrExist) {
		return "", err
	}
	return path, chownIfNeeded(path, uid, gid)
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

// Exec runs commands on the host.
func Exec(ctx context.Context, name string, args ...string) ([]byte, error) {
	output, err := exec.CommandContext(ctx, name, args...).CombinedOutput()
	if err != nil {
		return output, fmt.Errorf("%s %s: %w: %s", name, strings.Join(args, " "), err, strings.TrimSpace(string(output)))
	}
	return output, nil
}

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

func (z ZFS) exists(ctx context.Context, dataset string) (bool, error) {
	output, err := z.Run(ctx, "zfs", "list", "-H", "-o", "name", dataset)
	if err == nil {
		return true, nil
	}
	if strings.Contains(string(output), "does not exist") {
		return false, nil
	}
	return false, err
}

func (z ZFS) Ensure(ctx context.Context, tenant string, uid, gid int) (string, error) {
	if err := checkTenant(tenant); err != nil {
		return "", err
	}
	dataset := z.dataset(tenant)
	mountpoint := filepath.Join(z.MountRoot, tenant)
	exists, err := z.exists(ctx, dataset)
	if err != nil {
		return "", err
	}
	if !exists {
		if _, err := z.Run(ctx, "zfs", "create", "-p", "-o", "mountpoint="+mountpoint, dataset); err != nil {
			return "", err
		}
	}
	mounted, err := z.Run(ctx, "zfs", "get", "-H", "-o", "value", "mounted,mountpoint", dataset)
	if err != nil {
		return "", err
	}
	values := strings.Fields(string(mounted))
	if len(values) != 2 || values[1] != mountpoint {
		return "", fmt.Errorf("dataset %s is mounted at %v, want %s", dataset, values, mountpoint)
	}
	if values[0] != "yes" {
		if _, err := z.Run(ctx, "zfs", "mount", dataset); err != nil {
			return "", err
		}
	}
	return mountpoint, chownIfNeeded(mountpoint, uid, gid)
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

func (z ZFS) Remove(ctx context.Context, tenant string) error {
	if err := checkTenant(tenant); err != nil {
		return err
	}
	dataset := z.dataset(tenant)
	exists, err := z.exists(ctx, dataset)
	if err != nil || !exists {
		return err
	}
	_, err = z.Run(ctx, "zfs", "destroy", "-r", dataset)
	return err
}
