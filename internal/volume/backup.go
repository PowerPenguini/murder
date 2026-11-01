package volume

import (
	"context"
	"errors"
	"fmt"
	"io"
	"path/filepath"
	"strings"
)

// ErrBackupsUnsupported reports a driver that cannot back tenants up: the
// dir driver, which takes no snapshots, so a backup of a running tenant
// would not be of one moment.
var ErrBackupsUnsupported = errors.New("backups need the zfs driver")

// Backups is a driver whose tenants' data is backed up whole, from a
// snapshot, and restored into a copy that then takes its place.
type Backups interface {
	// SnapshotDir is where the files of snapshot, one Snapshot took of the
	// tenant, are, read-only.
	SnapshotDir(ctx context.Context, tenant, snapshot string) (string, error)
	// CreateCopy makes copy name of the tenant's data, empty and mounted
	// apart from it, and returns where.
	CreateCopy(ctx context.Context, tenant, name string) (string, error)
	// DestroyCopy removes copy name.
	DestroyCopy(ctx context.Context, name string) error
	// ReplaceWithCopy makes copy name the tenant's data, and keeps what the
	// tenant had as the returned dataset, which nothing mounts.
	ReplaceWithCopy(ctx context.Context, tenant, name string) (string, error)
}

// WriteTar writes the files below path as a tar stream, never leaving it.
func WriteTar(ctx context.Context, path string, w io.Writer) error { return writeTar(ctx, path, w) }

// ReadTar unpacks a tar stream below path, never leaving it; ownership is
// kept where the process may set it, setuid and setgid bits are not.
func ReadTar(ctx context.Context, path string, r io.Reader) error { return readTar(ctx, path, r) }

// copyPrefix begins every copy's name, so a copy is never taken for a
// tenant, and nothing but a copy is ever destroyed as one.
const copyPrefix = "murder-copy-"

// CopyName is the name of a copy of tenant.
func CopyName(tenant, suffix string) string { return copyPrefix + tenant + "-" + suffix }

func checkCopy(name string) error {
	if !strings.HasPrefix(name, copyPrefix) || strings.ContainsAny(name, "/@ .") {
		return fmt.Errorf("%q is not the name of a copy", name)
	}
	return nil
}

// Dataset is the dataset of a tenant's data.
func (z ZFS) Dataset(tenant string) string { return z.dataset(tenant) }

// SnapshotDir is the snapshot below the dataset's mountpoint, which ZFS
// shows read-only in .zfs/snapshot while the dataset is mounted.
func (z ZFS) SnapshotDir(ctx context.Context, tenant, snapshot string) (string, error) {
	if err := checkTenant(tenant); err != nil {
		return "", err
	}
	short, ok := strings.CutPrefix(snapshot, z.dataset(tenant)+"@")
	if !ok || strings.ContainsAny(short, "/ ") {
		return "", fmt.Errorf("snapshot %s is not tenant %s's", snapshot, tenant)
	}
	current, exists, err := z.properties(ctx, z.dataset(tenant))
	if err != nil {
		return "", err
	}
	if !exists || current.mounted != "yes" {
		return "", fmt.Errorf("tenant %s's data is not mounted", tenant)
	}
	return filepath.Join(current.mountpoint, ".zfs", "snapshot", short), nil
}

// copiesDir holds copies while they are filled, inside the private mount
// root.
func (z ZFS) copiesDir() string { return filepath.Join(z.MountRoot, ".copies") }

func (z ZFS) CreateCopy(ctx context.Context, tenant, name string) (string, error) {
	if err := checkTenant(tenant); err != nil {
		return "", err
	}
	if err := checkCopy(name); err != nil {
		return "", err
	}
	if err := privateDir(z.MountRoot); err != nil {
		return "", err
	}
	mountpoint := filepath.Join(z.copiesDir(), name)
	if _, err := z.Run(ctx, "zfs", "create",
		"-o", "mountpoint="+mountpoint, "-o", "setuid=off", "-o", "devices=off",
		"-o", ownerProperty+"="+tenant, z.Parent+"/"+name); err != nil {
		return "", err
	}
	return mountpoint, nil
}

func (z ZFS) DestroyCopy(ctx context.Context, name string) error {
	if err := checkCopy(name); err != nil {
		return err
	}
	_, err := z.Run(ctx, "zfs", "destroy", "-r", z.Parent+"/"+name)
	if err != nil && strings.Contains(err.Error(), "does not exist") {
		return nil
	}
	return err
}

// ReplaceWithCopy renames the tenant's dataset aside, unmounted, and the
// copy into its place, mounted where the tenant's data belongs.
func (z ZFS) ReplaceWithCopy(ctx context.Context, tenant, name string) (string, error) {
	if err := checkTenant(tenant); err != nil {
		return "", err
	}
	if err := checkCopy(name); err != nil {
		return "", err
	}
	copied, exists, err := z.properties(ctx, z.Parent+"/"+name)
	if err != nil {
		return "", err
	}
	if !exists || copied.owner != tenant {
		return "", fmt.Errorf("copy %s is not of tenant %s", name, tenant)
	}
	aside := ""
	if _, exists, err := z.properties(ctx, z.dataset(tenant)); err != nil {
		return "", err
	} else if exists {
		short := strings.Replace(name, copyPrefix, "murder-replaced-", 1)
		aside = z.Parent + "/" + short
		if _, err := z.Run(ctx, "zfs", "unmount", z.dataset(tenant)); err != nil && !strings.Contains(err.Error(), "not currently mounted") {
			return "", err
		}
		if _, err := z.Run(ctx, "zfs", "rename", z.dataset(tenant), aside); err != nil {
			return "", err
		}
		for _, property := range []string{"canmount=noauto", "mountpoint=" + filepath.Join(z.MountRoot, ".replaced", short)} {
			if _, err := z.Run(ctx, "zfs", "set", property, aside); err != nil {
				return aside, err
			}
		}
	}
	if _, err := z.Run(ctx, "zfs", "rename", z.Parent+"/"+name, z.dataset(tenant)); err != nil {
		return aside, err
	}
	// Setting the mountpoint mounts it there.
	if _, err := z.Run(ctx, "zfs", "set", "mountpoint="+filepath.Join(z.MountRoot, tenant), z.dataset(tenant)); err != nil {
		return aside, err
	}
	return aside, nil
}
