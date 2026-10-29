//go:build zfs

package volume

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"syscall"
	"testing"
	"time"
)

// These tests need root and the ZFS tools; test/vm/go-test.sh runs them in a
// throwaway VM. Each test builds its own pool on a file vdev.
func newTestPool(t *testing.T) string {
	t.Helper()
	if os.Geteuid() != 0 {
		t.Skip("ZFS tests need root")
	}
	if _, err := exec.LookPath("zpool"); err != nil {
		t.Skip("zpool not installed")
	}
	suffix := make([]byte, 3)
	_, _ = rand.Read(suffix)
	pool := "murdertest" + hex.EncodeToString(suffix)
	vdev := filepath.Join(t.TempDir(), "vdev")
	if err := os.WriteFile(vdev, nil, 0o600); err != nil {
		t.Fatal(err)
	}
	if err := os.Truncate(vdev, 256<<20); err != nil {
		t.Fatal(err)
	}
	if output, err := exec.Command("zpool", "create", "-m", "none", pool, vdev).CombinedOutput(); err != nil {
		t.Fatalf("zpool create: %v\n%s", err, output)
	}
	t.Cleanup(func() { _ = exec.Command("zpool", "destroy", "-f", pool).Run() })
	return pool
}

func zfsOutput(t *testing.T, args ...string) string {
	t.Helper()
	output, err := exec.Command("zfs", args...).CombinedOutput()
	if err != nil {
		t.Fatalf("zfs %s: %v\n%s", strings.Join(args, " "), err, output)
	}
	return string(output)
}

func TestZFSDriverOnRealZFS(t *testing.T) {
	pool := newTestPool(t)
	mountRoot := filepath.Join(t.TempDir(), "tenants")
	volumes := ZFS{Parent: pool + "/murder/tenants", MountRoot: mountRoot, Run: Exec}
	ctx := context.Background()

	path, err := volumes.Ensure(ctx, "acme", 10001, 10002, 1)
	if err != nil {
		t.Fatal(err)
	}
	if path != filepath.Join(mountRoot, "acme") {
		t.Fatalf("path = %q", path)
	}
	info, err := os.Stat(path)
	if err != nil {
		t.Fatal(err)
	}
	if stat := info.Sys().(*syscall.Stat_t); stat.Uid != 10001 || stat.Gid != 10002 {
		t.Fatalf("data directory owned by %d:%d, want 10001:10002", stat.Uid, stat.Gid)
	}
	if got := strings.Fields(zfsOutput(t, "get", "-H", "-p", "-o", "value", "mountpoint,setuid,devices,refquota,"+ownerProperty, pool+"/murder/tenants/acme")); strings.Join(got, " ") != path+" off off 1073741824 acme" {
		t.Fatalf("dataset properties = %v", got)
	}
	if info, err := os.Stat(mountRoot); err != nil || info.Mode().Perm() != 0o700 {
		t.Fatalf("mount root = %v, %v", info.Mode().Perm(), err)
	}
	if err := os.WriteFile(filepath.Join(path, "data.db"), []byte("before"), 0o600); err != nil {
		t.Fatal(err)
	}

	snapshot, err := volumes.Snapshot(ctx, "acme", "image-change")
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(zfsOutput(t, "list", "-H", "-t", "snapshot", "-o", "name"), snapshot) {
		t.Fatalf("snapshot %s not listed", snapshot)
	}
	if err := os.WriteFile(filepath.Join(path, "data.db"), []byte("after"), 0o600); err != nil {
		t.Fatal(err)
	}
	// The snapshot holds the data as it was when taken.
	snapshotFile := filepath.Join(path, ".zfs", "snapshot", strings.SplitN(snapshot, "@", 2)[1], "data.db")
	if data, err := os.ReadFile(snapshotFile); err != nil || string(data) != "before" {
		t.Fatalf("snapshot content = %q, %v", data, err)
	}

	// Pruning keeps the newest snapshots of the reason.
	clock := time.Date(2026, 10, 29, 12, 0, 0, 0, time.UTC)
	timed := volumes
	timed.Now = func() time.Time { clock = clock.Add(time.Second); return clock }
	var newest string
	for range 3 {
		if newest, err = timed.Snapshot(ctx, "acme", "image-change"); err != nil {
			t.Fatal(err)
		}
	}
	if err := volumes.Prune(ctx, "acme", "image-change", 1); err != nil {
		t.Fatal(err)
	}
	if left := strings.Fields(zfsOutput(t, "list", "-H", "-o", "name", "-t", "snapshot", "-d", "1", pool+"/murder/tenants/acme")); len(left) != 1 || left[0] != newest {
		t.Fatalf("snapshots after pruning = %v, want only %s", left, newest)
	}

	// An unmounted dataset (as after a reboot that skipped zfs-mount) is
	// mounted again, with its data.
	zfsOutput(t, "unmount", pool+"/murder/tenants/acme")
	if _, err := volumes.Ensure(ctx, "acme", 10001, 10002, 2); err != nil {
		t.Fatalf("Ensure of an unmounted dataset = %v", err)
	}
	if got := strings.TrimSpace(zfsOutput(t, "get", "-H", "-p", "-o", "value", "refquota", pool+"/murder/tenants/acme")); got != "2147483648" {
		t.Fatalf("refquota after a change = %s", got)
	}
	if data, err := os.ReadFile(filepath.Join(path, "data.db")); err != nil || string(data) != "after" {
		t.Fatalf("data after remount = %q, %v", data, err)
	}

	if exists, err := volumes.Exists(ctx, "acme"); err != nil || !exists {
		t.Fatalf("Exists = %v, %v", exists, err)
	}
	// Seen from this process, the path is the dataset while it is mounted
	// and plain directory once it is not.
	if serving, err := volumes.Serving(ctx, "acme", os.Getpid()); err != nil || !serving {
		t.Fatalf("Serving while mounted = %v, %v", serving, err)
	}
	zfsOutput(t, "unmount", pool+"/murder/tenants/acme")
	if serving, err := volumes.Serving(ctx, "acme", os.Getpid()); err != nil || serving {
		t.Fatalf("Serving while unmounted = %v, %v", serving, err)
	}
	if _, err := volumes.Ensure(ctx, "acme", 10001, 10002, 0); err != nil {
		t.Fatal(err)
	}

	// A dataset made by hand below the parent is neither used nor destroyed.
	zfsOutput(t, "create", "-o", "mountpoint="+filepath.Join(mountRoot, "manual"), pool+"/murder/tenants/manual")
	if _, err := volumes.Ensure(ctx, "manual", 10001, 10002, 0); err == nil {
		t.Fatal("Ensure adopted a dataset murder did not create")
	}
	if err := volumes.Remove(ctx, "manual"); err == nil {
		t.Fatal("Remove destroyed a dataset murder did not create")
	}
	zfsOutput(t, "list", pool+"/murder/tenants/manual")

	if err := volumes.Remove(ctx, "acme"); err != nil {
		t.Fatal(err)
	}
	if output, err := exec.Command("zfs", "list", pool+"/murder/tenants/acme").CombinedOutput(); err == nil {
		t.Fatalf("dataset still exists after Remove:\n%s", output)
	}
	if err := volumes.Remove(ctx, "acme"); err != nil {
		t.Fatalf("second Remove = %v", err)
	}
}
