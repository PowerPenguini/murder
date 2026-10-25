package volume

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

func TestDirEnsureIsIdempotentAndRemoves(t *testing.T) {
	root := filepath.Join(t.TempDir(), "tenants")
	dir := Dir{Root: root}
	ctx := context.Background()
	uid, gid := os.Getuid(), os.Getgid()
	path, err := dir.Ensure(ctx, "acme", uid, gid)
	if err != nil || path != filepath.Join(root, "acme") {
		t.Fatalf("Ensure = %q, %v", path, err)
	}
	if err := os.WriteFile(filepath.Join(path, "data.db"), []byte("x"), 0o600); err != nil {
		t.Fatal(err)
	}
	if again, err := dir.Ensure(ctx, "acme", uid, gid); err != nil || again != path {
		t.Fatalf("second Ensure = %q, %v", again, err)
	}
	if _, err := os.Stat(filepath.Join(path, "data.db")); err != nil {
		t.Fatal("Ensure lost existing data")
	}
	info, _ := os.Stat(path)
	if info.Mode().Perm() != 0o700 {
		t.Fatalf("data dir mode = %v", info.Mode().Perm())
	}
	if _, err := dir.Snapshot(ctx, "acme", "image-change"); !errors.Is(err, ErrSnapshotsUnsupported) {
		t.Fatalf("Snapshot = %v", err)
	}
	if _, err := dir.Ensure(ctx, "../etc", uid, gid); err == nil {
		t.Fatal("Ensure accepted a path-like tenant name")
	}
	if err := dir.Remove(ctx, "acme"); err != nil {
		t.Fatal(err)
	}
	if _, err := os.Stat(path); !errors.Is(err, os.ErrNotExist) {
		t.Fatal("Remove left the data")
	}
}

// fakeZFS answers zfs commands from an in-memory set of datasets.
type fakeZFS struct {
	datasets map[string]string // dataset → mountpoint
	mounted  map[string]bool
	calls    []string
	mountDir string
}

func (f *fakeZFS) run(_ context.Context, name string, args ...string) ([]byte, error) {
	call := name + " " + strings.Join(args, " ")
	f.calls = append(f.calls, call)
	switch {
	case args[0] == "list":
		if _, ok := f.datasets[args[len(args)-1]]; ok {
			return []byte(args[len(args)-1] + "\n"), nil
		}
		return []byte("cannot open '" + args[len(args)-1] + "': dataset does not exist\n"), errors.New("exit status 1")
	case args[0] == "create":
		mountpoint := strings.TrimPrefix(args[3], "mountpoint=")
		f.datasets[args[4]] = mountpoint
		f.mounted[args[4]] = true
		return nil, os.MkdirAll(mountpoint, 0o700)
	case args[0] == "get":
		dataset := args[len(args)-1]
		mounted := "no"
		if f.mounted[dataset] {
			mounted = "yes"
		}
		return []byte(mounted + "\n" + f.datasets[dataset] + "\n"), nil
	case args[0] == "mount":
		f.mounted[args[1]] = true
		return nil, nil
	case args[0] == "snapshot", args[0] == "destroy":
		return nil, nil
	}
	return nil, errors.New("unexpected command " + call)
}

func TestZFSEnsureSnapshotRemove(t *testing.T) {
	fake := &fakeZFS{datasets: map[string]string{}, mounted: map[string]bool{}}
	mountRoot := filepath.Join(t.TempDir(), "mnt")
	volumes := ZFS{Parent: "tank/corral", MountRoot: mountRoot, Run: fake.run, Now: func() time.Time { return time.Date(2026, 10, 25, 12, 0, 0, 0, time.UTC) }}
	ctx := context.Background()
	path, err := volumes.Ensure(ctx, "acme", os.Getuid(), os.Getgid())
	if err != nil || path != filepath.Join(mountRoot, "acme") {
		t.Fatalf("Ensure = %q, %v", path, err)
	}
	if !strings.Contains(strings.Join(fake.calls, "\n"), "zfs create -p -o mountpoint="+filepath.Join(mountRoot, "acme")+" tank/corral/acme") {
		t.Fatalf("calls = %v", fake.calls)
	}
	// An existing but unmounted dataset is mounted, not recreated.
	fake.mounted["tank/corral/acme"] = false
	fake.calls = nil
	if _, err := volumes.Ensure(ctx, "acme", os.Getuid(), os.Getgid()); err != nil {
		t.Fatal(err)
	}
	if got := strings.Join(fake.calls, "\n"); strings.Contains(got, "create") || !strings.Contains(got, "zfs mount tank/corral/acme") {
		t.Fatalf("calls = %v", fake.calls)
	}
	name, err := volumes.Snapshot(ctx, "acme", "image-change")
	if err != nil || name != "tank/corral/acme@corral-image-change-20261025T120000.000Z" {
		t.Fatalf("Snapshot = %q, %v", name, err)
	}
	if _, err := volumes.Snapshot(ctx, "acme", "Bad Reason"); err == nil {
		t.Fatal("accepted an unsafe snapshot reason")
	}
	// A dataset mounted somewhere else is refused rather than used.
	fake.datasets["tank/corral/beta"] = "/elsewhere"
	fake.mounted["tank/corral/beta"] = true
	if _, err := volumes.Ensure(ctx, "beta", os.Getuid(), os.Getgid()); err == nil {
		t.Fatal("used a dataset with a foreign mountpoint")
	}
	fake.calls = nil
	if err := volumes.Remove(ctx, "acme"); err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(strings.Join(fake.calls, "\n"), "zfs destroy -r tank/corral/acme") {
		t.Fatalf("calls = %v", fake.calls)
	}
	if err := volumes.Remove(ctx, "never-existed"); err != nil {
		t.Fatalf("removing a missing dataset = %v", err)
	}
}
