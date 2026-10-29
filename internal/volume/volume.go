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
	"strconv"
	"strings"
	"time"

	"github.com/PowerPenguini/murder/internal/spec"
)

// ErrDiskLimit reports a disk limit that could not be applied, such as one
// below what the tenant already stores. The data is usable all the same.
var ErrDiskLimit = errors.New("disk limit not applied")

// ErrSnapshotsUnsupported is returned by drivers that cannot snapshot.
var ErrSnapshotsUnsupported = errors.New("volume driver does not support snapshots")

// Volumes manages tenant data directories. The directories above them are
// private to root, so no host user can reach into a tenant's data; a setuid
// file a tenant writes there is useless to them.
type Volumes interface {
	// Ensure creates the tenant's data directory if needed, owned by uid:gid,
	// mounts it if it is not, limits it to diskGB (0 is no limit) where the
	// driver can, and returns its host path. A limit it cannot apply is
	// reported with ErrDiskLimit next to a usable path.
	Ensure(ctx context.Context, tenant string, uid, gid, diskGB int) (string, error)
	// Exists reports whether the tenant has data, for example left behind by
	// a deleted tenant of the same name.
	Exists(ctx context.Context, tenant string) (bool, error)
	// LateMounts reports whether data can be mounted after a container
	// started (ZFS); only then is Serving worth asking.
	LateMounts() bool
	// Serving reports whether the container process pid has the tenant's
	// data mounted. A container started before its data was mounted holds
	// the empty directory underneath instead, and must be replaced.
	Serving(ctx context.Context, tenant string, pid int) (bool, error)
	// Snapshot records the data as it is now and returns the snapshot name.
	Snapshot(ctx context.Context, tenant, reason string) (string, error)
	// Remove deletes the tenant's data and snapshots.
	Remove(ctx context.Context, tenant string) error
}

func checkTenant(tenant string) error { return spec.ValidLabel(tenant) }

// Dir keeps each tenant's data in a directory below Root. It can neither
// snapshot nor limit disk use.
type Dir struct {
	Root string
}

func (d Dir) path(tenant string) string { return filepath.Join(d.Root, tenant) }

func (d Dir) Ensure(_ context.Context, tenant string, uid, gid, _ int) (string, error) {
	if err := checkTenant(tenant); err != nil {
		return "", err
	}
	if err := privateDir(d.Root); err != nil {
		return "", err
	}
	path := d.path(tenant)
	if err := os.Mkdir(path, 0o700); err != nil && !errors.Is(err, os.ErrExist) {
		return "", err
	}
	return path, chownIfNeeded(path, uid, gid)
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

// A plain directory cannot be mounted late.
func (d Dir) LateMounts() bool                                   { return false }
func (d Dir) Serving(context.Context, string, int) (bool, error) { return true, nil }

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

// ownerProperty marks datasets Murder created; Murder never touches a
// dataset without it, even below its own parent.
const ownerProperty = "io.murder:tenant"

// ZFS keeps each tenant in a dataset below Parent, mounted below MountRoot.
type ZFS struct {
	Parent    string
	MountRoot string
	Run       Runner
	Now       func() time.Time
	// ProcRoot is where process information is read; "/proc" if empty.
	ProcRoot string
}

func (z ZFS) dataset(tenant string) string { return z.Parent + "/" + tenant }

func (z ZFS) now() time.Time {
	if z.Now != nil {
		return z.Now()
	}
	return time.Now()
}

// properties are what Murder reads of a dataset.
type properties struct {
	owner, mounted, mountpoint string
	// refquota is the limit on the dataset's own data in bytes; 0 is none.
	refquota int64
}

// properties returns the dataset's properties, or ok=false if the dataset
// does not exist.
func (z ZFS) properties(ctx context.Context, dataset string) (properties, bool, error) {
	output, err := z.Run(ctx, "zfs", "get", "-H", "-p", "-o", "value", ownerProperty+",mounted,mountpoint,refquota", dataset)
	if err != nil {
		if strings.Contains(string(output), "does not exist") {
			return properties{}, false, nil
		}
		return properties{}, false, err
	}
	values := strings.Split(strings.TrimRight(string(output), "\n"), "\n")
	if len(values) != 4 {
		return properties{}, false, fmt.Errorf("zfs get %s: unexpected output %q", dataset, output)
	}
	refquota, err := strconv.ParseInt(values[3], 10, 64)
	if err != nil {
		return properties{}, false, fmt.Errorf("zfs get %s: refquota %q", dataset, values[3])
	}
	return properties{owner: values[0], mounted: values[1], mountpoint: values[2], refquota: refquota}, true, nil
}

// diskBytes is the refquota for diskGB; 0 is none.
func diskBytes(diskGB int) int64 { return int64(diskGB) << 30 }

// owned checks that dataset was made by Murder for tenant and is mounted
// where Murder expects; anything else is refused rather than used.
func (z ZFS) owned(tenant, owner, mountpoint string) error {
	want := filepath.Join(z.MountRoot, tenant)
	if owner != tenant {
		return fmt.Errorf("dataset %s was not created by murder for tenant %s (%s=%q)", z.dataset(tenant), tenant, ownerProperty, owner)
	}
	if mountpoint != want {
		return fmt.Errorf("dataset %s is mounted at %s, want %s", z.dataset(tenant), mountpoint, want)
	}
	return nil
}

// Ensure limits the dataset's own data with refquota, so snapshots Murder
// takes never count against the tenant.
func (z ZFS) Ensure(ctx context.Context, tenant string, uid, gid, diskGB int) (string, error) {
	if err := checkTenant(tenant); err != nil {
		return "", err
	}
	if err := privateDir(z.MountRoot); err != nil {
		return "", err
	}
	dataset := z.dataset(tenant)
	mountpoint := filepath.Join(z.MountRoot, tenant)
	current, exists, err := z.properties(ctx, dataset)
	if err != nil {
		return "", err
	}
	if !exists {
		if _, err := z.Run(ctx, "zfs", "create", "-p",
			"-o", "mountpoint="+mountpoint, "-o", "setuid=off", "-o", "devices=off",
			"-o", "refquota="+refquota(diskGB), "-o", ownerProperty+"="+tenant, dataset); err != nil {
			return "", err
		}
		return mountpoint, chownIfNeeded(mountpoint, uid, gid)
	}
	if err := z.owned(tenant, current.owner, current.mountpoint); err != nil {
		return "", err
	}
	var limitErr error
	if current.refquota != diskBytes(diskGB) {
		if _, err := z.Run(ctx, "zfs", "set", "refquota="+refquota(diskGB), dataset); err != nil {
			limitErr = fmt.Errorf("%w: %s: %w", ErrDiskLimit, diskLimit(diskGB), err)
		}
	}
	if current.mounted != "yes" {
		if _, err := z.Run(ctx, "zfs", "mount", dataset); err != nil {
			return "", err
		}
	}
	if err := chownIfNeeded(mountpoint, uid, gid); err != nil {
		return "", err
	}
	return mountpoint, limitErr
}

func refquota(diskGB int) string {
	if diskGB == 0 {
		return "none"
	}
	return strconv.FormatInt(diskBytes(diskGB), 10)
}

func diskLimit(diskGB int) string {
	if diskGB == 0 {
		return "none"
	}
	return strconv.Itoa(diskGB) + " GB"
}

func (z ZFS) Exists(ctx context.Context, tenant string) (bool, error) {
	if err := checkTenant(tenant); err != nil {
		return false, err
	}
	_, exists, err := z.properties(ctx, z.dataset(tenant))
	return exists, err
}

func (z ZFS) LateMounts() bool { return true }

// Serving reads the container's mount table, which must contain the tenant's
// dataset. It looks for the dataset rather than the data path, because the
// engine resolves symlinks in mount targets.
func (z ZFS) Serving(_ context.Context, tenant string, pid int) (bool, error) {
	if err := checkTenant(tenant); err != nil {
		return false, err
	}
	procRoot := z.ProcRoot
	if procRoot == "" {
		procRoot = "/proc"
	}
	data, err := os.ReadFile(filepath.Join(procRoot, strconv.Itoa(pid), "mountinfo"))
	if err != nil {
		return false, fmt.Errorf("read the container's mounts: %w", err)
	}
	return mounts(string(data), "zfs", z.dataset(tenant)), nil
}

// mounts reports whether a mountinfo table has a mount of source with the
// given filesystem type.
func mounts(mountinfo, fstype, source string) bool {
	for _, line := range strings.Split(mountinfo, "\n") {
		_, after, ok := strings.Cut(line, " - ")
		tail := strings.Fields(after)
		if ok && len(tail) >= 2 && tail[0] == fstype && unescapeMount(tail[1]) == source {
			return true
		}
	}
	return false
}

// unescapeMount decodes the octal escapes (\040 for a space) mountinfo uses.
func unescapeMount(value string) string {
	if !strings.Contains(value, "\\") {
		return value
	}
	var out strings.Builder
	for i := 0; i < len(value); i++ {
		if value[i] == '\\' && i+3 < len(value) {
			if code, err := strconv.ParseUint(value[i+1:i+4], 8, 8); err == nil {
				out.WriteByte(byte(code))
				i += 3
				continue
			}
		}
		out.WriteByte(value[i])
	}
	return out.String()
}

// Snapshot names snapshots "murder-<reason>-<UTC time>" so they sort by time.
func (z ZFS) Snapshot(ctx context.Context, tenant, reason string) (string, error) {
	if err := checkTenant(tenant); err != nil {
		return "", err
	}
	if !snapshotReason(reason) {
		return "", fmt.Errorf("snapshot reason %q must be lowercase letters and hyphens", reason)
	}
	name := z.dataset(tenant) + "@murder-" + reason + "-" + z.now().UTC().Format("20060102T150405.000Z")
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

// Remove destroys the tenant's dataset only if Murder created it for this
// tenant and it is mounted where Murder put it.
func (z ZFS) Remove(ctx context.Context, tenant string) error {
	if err := checkTenant(tenant); err != nil {
		return err
	}
	current, exists, err := z.properties(ctx, z.dataset(tenant))
	if err != nil || !exists {
		return err
	}
	if err := z.owned(tenant, current.owner, current.mountpoint); err != nil {
		return err
	}
	_, err = z.Run(ctx, "zfs", "destroy", "-r", z.dataset(tenant))
	return err
}
