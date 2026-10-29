package volume

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"testing"
	"time"
)

func TestDirEnsureIsPrivateIdempotentAndRemoves(t *testing.T) {
	root := filepath.Join(t.TempDir(), "tenants")
	dir := Dir{Root: root}
	ctx := context.Background()
	uid, gid := os.Getuid(), os.Getgid()
	if exists, err := dir.Exists(ctx, "acme"); err != nil || exists {
		t.Fatalf("Exists before Ensure = %v, %v", exists, err)
	}
	path, err := dir.Ensure(ctx, "acme", uid, gid, 5)
	if err != nil || path != filepath.Join(root, "acme") {
		t.Fatalf("Ensure = %q, %v", path, err)
	}
	if exists, err := dir.Exists(ctx, "acme"); err != nil || !exists {
		t.Fatalf("Exists after Ensure = %v, %v", exists, err)
	}
	if info, _ := os.Stat(root); info.Mode().Perm() != 0o700 {
		t.Fatalf("tenants root mode = %v; host users must not reach tenant data", info.Mode().Perm())
	}
	if err := os.WriteFile(filepath.Join(path, "data.db"), []byte("x"), 0o600); err != nil {
		t.Fatal(err)
	}
	if again, err := dir.Ensure(ctx, "acme", uid, gid, 5); err != nil || again != path {
		t.Fatalf("second Ensure = %q, %v", again, err)
	}
	if _, err := os.Stat(filepath.Join(path, "data.db")); err != nil {
		t.Fatal("Ensure lost existing data")
	}
	if info, _ := os.Stat(path); info.Mode().Perm() != 0o700 {
		t.Fatalf("data dir mode = %v", info.Mode().Perm())
	}
	if _, err := dir.Snapshot(ctx, "acme", "image-change"); !errors.Is(err, ErrSnapshotsUnsupported) {
		t.Fatalf("Snapshot = %v", err)
	}
	if _, err := dir.Ensure(ctx, "../etc", uid, gid, 0); err == nil {
		t.Fatal("Ensure accepted a path-like tenant name")
	}
	if err := dir.Remove(ctx, "acme"); err != nil {
		t.Fatal(err)
	}
	if _, err := os.Stat(path); !errors.Is(err, os.ErrNotExist) {
		t.Fatal("Remove left the data")
	}
}

type fakeDataset struct {
	owner, mountpoint string
	mounted           bool
	refquota          int64
}

// fakeZFS answers zfs commands from an in-memory set of datasets.
type fakeZFS struct {
	datasets map[string]*fakeDataset
	calls    []string
}

func (f *fakeZFS) run(_ context.Context, name string, args ...string) ([]byte, error) {
	call := name + " " + strings.Join(args, " ")
	f.calls = append(f.calls, call)
	dataset := args[len(args)-1]
	switch args[0] {
	case "get":
		d, ok := f.datasets[dataset]
		if !ok {
			return []byte("cannot open '" + dataset + "': dataset does not exist\n"), errors.New("exit status 1")
		}
		mounted := "no"
		if d.mounted {
			mounted = "yes"
		}
		owner := d.owner
		if owner == "" {
			owner = "-"
		}
		return []byte(owner + "\n" + mounted + "\n" + d.mountpoint + "\n" + strconv.FormatInt(d.refquota, 10) + "\n"), nil
	case "create":
		d := &fakeDataset{mounted: true}
		for i, arg := range args {
			if arg != "-o" {
				continue
			}
			key, value, _ := strings.Cut(args[i+1], "=")
			switch key {
			case "mountpoint":
				d.mountpoint = value
			case "refquota":
				d.refquota = parseQuota(value)
			case ownerProperty:
				d.owner = value
			}
		}
		f.datasets[dataset] = d
		return nil, os.MkdirAll(d.mountpoint, 0o700)
	case "mount":
		f.datasets[dataset].mounted = true
		return nil, nil
	case "set":
		_, value, _ := strings.Cut(args[1], "=")
		f.datasets[dataset].refquota = parseQuota(value)
		return nil, nil
	case "snapshot", "destroy":
		return nil, nil
	}
	return nil, errors.New("unexpected command " + call)
}

func parseQuota(value string) int64 {
	quota, _ := strconv.ParseInt(value, 10, 64) // "none" is 0
	return quota
}

func (f *fakeZFS) ran(fragment string) bool {
	return strings.Contains(strings.Join(f.calls, "\n"), fragment)
}

func TestZFSEnsureSnapshotRemove(t *testing.T) {
	fake := &fakeZFS{datasets: map[string]*fakeDataset{}}
	mountRoot := filepath.Join(t.TempDir(), "mnt")
	volumes := ZFS{Parent: "tank/murder", MountRoot: mountRoot, Run: fake.run, Now: func() time.Time { return time.Date(2026, 10, 25, 12, 0, 0, 0, time.UTC) }}
	ctx := context.Background()
	uid, gid := os.Getuid(), os.Getgid()
	path, err := volumes.Ensure(ctx, "acme", uid, gid, 2)
	if err != nil || path != filepath.Join(mountRoot, "acme") {
		t.Fatalf("Ensure = %q, %v", path, err)
	}
	if !fake.ran("zfs create -p -o mountpoint=" + path + " -o setuid=off -o devices=off -o refquota=2147483648 -o " + ownerProperty + "=acme tank/murder/acme") {
		t.Fatalf("calls = %v", fake.calls)
	}
	if info, _ := os.Stat(mountRoot); info.Mode().Perm() != 0o700 {
		t.Fatalf("mount root mode = %v", info.Mode().Perm())
	}
	// An existing but unmounted dataset is mounted, not recreated.
	fake.datasets["tank/murder/acme"].mounted = false
	fake.calls = nil
	if _, err := volumes.Ensure(ctx, "acme", uid, gid, 2); err != nil {
		t.Fatalf("Ensure of an unmounted dataset = %v", err)
	}
	if fake.ran("create") || fake.ran("set") || !fake.ran("zfs mount tank/murder/acme") {
		t.Fatalf("calls = %v", fake.calls)
	}
	// A changed limit is set on the dataset; no limit is "none".
	for _, step := range []struct {
		diskGB int
		set    string
	}{{5, "refquota=5368709120"}, {0, "refquota=none"}} {
		fake.calls = nil
		if _, err := volumes.Ensure(ctx, "acme", uid, gid, step.diskGB); err != nil || !fake.ran("zfs set "+step.set+" tank/murder/acme") {
			t.Fatalf("Ensure with %d GB = %v, calls %v", step.diskGB, err, fake.calls)
		}
	}
	if exists, err := volumes.Exists(ctx, "acme"); err != nil || !exists {
		t.Fatalf("Exists = %v, %v", exists, err)
	}
	name, err := volumes.Snapshot(ctx, "acme", "image-change")
	if err != nil || name != "tank/murder/acme@murder-image-change-20261025T120000.000Z" {
		t.Fatalf("Snapshot = %q, %v", name, err)
	}
	if _, err := volumes.Snapshot(ctx, "acme", "Bad Reason"); err == nil {
		t.Fatal("accepted an unsafe snapshot reason")
	}
	fake.calls = nil
	if err := volumes.Remove(ctx, "acme"); err != nil {
		t.Fatal(err)
	}
	if !fake.ran("zfs destroy -r tank/murder/acme") {
		t.Fatalf("calls = %v", fake.calls)
	}
	if err := volumes.Remove(ctx, "never-existed"); err != nil {
		t.Fatalf("removing a missing dataset = %v", err)
	}
}

// A dataset Murder did not create for the tenant is never used or destroyed,
// even when its name matches.
func TestZFSRefusesForeignDatasets(t *testing.T) {
	mountRoot := filepath.Join(t.TempDir(), "mnt")
	fake := &fakeZFS{datasets: map[string]*fakeDataset{
		"tank/home":  {owner: "", mountpoint: "/home", mounted: true},
		"tank/other": {owner: "someone", mountpoint: filepath.Join(mountRoot, "other"), mounted: true},
		"tank/moved": {owner: "moved", mountpoint: "/elsewhere", mounted: true},
	}}
	volumes := ZFS{Parent: "tank", MountRoot: mountRoot, Run: fake.run}
	ctx := context.Background()
	for _, tenant := range []string{"home", "other", "moved"} {
		if _, err := volumes.Ensure(ctx, tenant, 1000, 1000, 0); err == nil {
			t.Errorf("Ensure used foreign dataset tank/%s", tenant)
		}
		if err := volumes.Remove(ctx, tenant); err == nil {
			t.Errorf("Remove destroyed foreign dataset tank/%s", tenant)
		}
	}
	if fake.ran("destroy") || fake.ran("mount ") {
		t.Fatalf("a foreign dataset was touched: %v", fake.calls)
	}
}

func TestMountsFindsASourceAnywhere(t *testing.T) {
	mountinfo := "22 1 8:2 / / rw,relatime shared:1 - ext4 /dev/sda2 rw\n" +
		"101 22 0:50 / /var/lib/app rw,noatime - zfs tank/murder/acme rw,xattr\n" +
		"102 22 0:51 / /odd rw - zfs tank/murder/with\\040space rw\n"
	if !mounts(mountinfo, "zfs", "tank/murder/acme") {
		t.Fatal("missed a dataset mounted at a resolved symlink target")
	}
	if !mounts(mountinfo, "zfs", "tank/murder/with space") {
		t.Fatal("missed an escaped source")
	}
	if mounts(mountinfo, "zfs", "tank/murder/beta") || mounts(mountinfo, "ext4", "tank/murder/acme") {
		t.Fatal("found a mount that is not there")
	}
}

func TestZFSServingChecksTheContainersMount(t *testing.T) {
	proc := t.TempDir()
	volumes := ZFS{Parent: "tank/murder", ProcRoot: proc}
	write := func(content string) {
		if err := os.MkdirAll(filepath.Join(proc, "42"), 0o700); err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(filepath.Join(proc, "42", "mountinfo"), []byte(content), 0o600); err != nil {
			t.Fatal(err)
		}
	}
	write("100 22 0:50 / /data rw - zfs tank/murder/acme rw\n")
	if ok, err := volumes.Serving(context.Background(), "acme", 42); err != nil || !ok {
		t.Fatalf("container on its dataset = %v, %v", ok, err)
	}
	write("100 22 8:2 /srv/murder/tenants/acme /data rw - ext4 /dev/sda2 rw\n")
	if ok, _ := volumes.Serving(context.Background(), "acme", 42); ok {
		t.Fatal("a container on the directory under the dataset counted as serving")
	}
	write("100 22 0:50 / /data rw - zfs tank/murder/beta rw\n")
	if ok, _ := volumes.Serving(context.Background(), "acme", 42); ok {
		t.Fatal("another tenant's dataset counted as serving")
	}
	if _, err := volumes.Serving(context.Background(), "acme", 7); err == nil {
		t.Fatal("a missing process was not an error")
	}
	if !volumes.LateMounts() || (Dir{}).LateMounts() {
		t.Fatal("LateMounts: only ZFS can mount late")
	}
}
