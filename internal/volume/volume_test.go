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
	owner, mountpoint, canmount, role string
	mounted                           bool
	refquota, used                    int64
}

// fakeZFS answers zfs commands from an in-memory set of datasets.
type fakeZFS struct {
	datasets  map[string]*fakeDataset
	snapshots []string // what "zfs list -t snapshot" prints
	held      string   // a snapshot that cannot be destroyed
	calls     []string
	fail      string // a command that fails, as a fragment of it
}

func (f *fakeZFS) run(_ context.Context, name string, args ...string) ([]byte, error) {
	call := name + " " + strings.Join(args, " ")
	f.calls = append(f.calls, call)
	if f.fail != "" && strings.Contains(call, f.fail) {
		return []byte("cannot " + args[0] + "\n"), errors.New("exit status 1")
	}
	dataset := args[len(args)-1]
	switch args[0] {
	case "rename":
		from, to := args[1], args[2]
		f.datasets[to] = f.datasets[from]
		delete(f.datasets, from)
		return nil, nil
	case "unmount":
		f.datasets[dataset].mounted = false
		return nil, nil
	case "rollback":
		return nil, nil
	case "get":
		d, ok := f.datasets[dataset]
		if !ok {
			return []byte("cannot open '" + dataset + "': dataset does not exist\n"), errors.New("exit status 1")
		}
		mounted := "no"
		if d.mounted {
			mounted = "yes"
		}
		dash := func(value string) string {
			if value == "" {
				return "-"
			}
			return value
		}
		canmount := d.canmount
		if canmount == "" {
			canmount = "on"
		}
		return []byte(dash(d.owner) + "\n" + mounted + "\n" + d.mountpoint + "\n" + strconv.FormatInt(d.refquota, 10) + "\n" + dash(d.role) + "\n" + canmount + "\n"), nil
	case "inherit":
		if args[1] == roleProperty {
			f.datasets[dataset].role = ""
		}
		return nil, nil
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
		key, value, _ := strings.Cut(args[1], "=")
		if key == "mountpoint" {
			// ZFS remounts a mounted dataset there; one unmounted stays so.
			f.datasets[dataset].mountpoint = value
			return nil, nil
		}
		if key == "canmount" {
			f.datasets[dataset].canmount = value
			return nil, nil
		}
		if quota := parseQuota(value); quota != 0 && quota < f.datasets[dataset].used {
			return []byte("cannot set property for '" + dataset + "': size is less than current used or reserved space\n"), errors.New("exit status 1")
		}
		f.datasets[dataset].refquota = parseQuota(value)
		return nil, nil
	case "snapshot":
		return nil, nil
	case "destroy":
		if dataset == f.held {
			return []byte("cannot destroy snapshot " + dataset + ": dataset is busy\n"), errors.New("exit status 1")
		}
		delete(f.datasets, dataset)
		return nil, nil
	case "list":
		if !strings.Contains(call, "-t snapshot") {
			var names []string
			for name := range f.datasets {
				names = append(names, name)
			}
			return []byte(strings.Join(names, "\n") + "\n"), nil
		}
		return []byte(strings.Join(f.snapshots, "\n") + "\n"), nil
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
	// A limit below what the tenant stores is reported, and the data is
	// still mounted and usable.
	fake.datasets["tank/murder/acme"].used = 3 << 30
	fake.datasets["tank/murder/acme"].mounted = false
	if got, err := volumes.Ensure(ctx, "acme", uid, gid, 2); !errors.Is(err, ErrDiskLimit) || got != path || !fake.datasets["tank/murder/acme"].mounted {
		t.Fatalf("Ensure below usage = %q, %v", got, err)
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
	if fake.ran("zfs destroy") || fake.ran("zfs mount ") || fake.ran("zfs set") {
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

// Prune keeps the newest snapshots of a reason and never touches others.
func TestZFSPruneKeepsTheNewest(t *testing.T) {
	fake := &fakeZFS{datasets: map[string]*fakeDataset{}, held: "tank/murder/acme@murder-image-change-20261024T120000.000Z", snapshots: []string{
		"tank/murder/acme@murder-image-change-20261024T120000.000Z",
		"tank/murder/acme@murder-image-change-20261025T120000.000Z",
		"tank/murder/acme@before-upgrade",
		"tank/murder/acme@murder-image-change-20261027T120000.000Z",
		"tank/murder/acme@murder-image-change-20261026T120000.000Z",
		"tank/murder/acme@murder-replica-20261020T120000.000Z",
	}}
	volumes := ZFS{Parent: "tank/murder", MountRoot: t.TempDir(), Run: fake.run}
	// The oldest is held: it is reported, and the next one still goes.
	if err := volumes.Prune(context.Background(), "acme", "image-change", 2); err == nil {
		t.Fatalf("Prune with a held snapshot = %v", err)
	}
	var destroyed []string
	for _, call := range fake.calls {
		if name, ok := strings.CutPrefix(call, "zfs destroy "); ok {
			destroyed = append(destroyed, name)
		}
	}
	if len(destroyed) != 2 || destroyed[1] != "tank/murder/acme@murder-image-change-20261025T120000.000Z" {
		t.Fatalf("destroyed %v", destroyed)
	}
	if err := volumes.Prune(context.Background(), "acme", "image-change", 0); err == nil {
		t.Fatal("pruned with keep 0")
	}
}
