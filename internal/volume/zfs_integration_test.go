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
	pool := "corraltest" + hex.EncodeToString(suffix)
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
	volumes := ZFS{Parent: pool + "/corral/tenants", MountRoot: mountRoot, Run: Exec}
	ctx := context.Background()

	path, mounted, err := volumes.Ensure(ctx, "acme", 10001, 10002)
	if err != nil || mounted {
		t.Fatalf("Ensure = %v, %v", mounted, err)
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
	if got := strings.Fields(zfsOutput(t, "get", "-H", "-o", "value", "mountpoint,setuid,devices,"+ownerProperty, pool+"/corral/tenants/acme")); strings.Join(got, " ") != path+" off off acme" {
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

	// An unmounted dataset (as after a reboot that skipped zfs-mount) is
	// mounted again, with its data.
	zfsOutput(t, "unmount", pool+"/corral/tenants/acme")
	if _, mounted, err := volumes.Ensure(ctx, "acme", 10001, 10002); err != nil || !mounted {
		t.Fatalf("Ensure of an unmounted dataset = %v, %v", mounted, err)
	}
	if data, err := os.ReadFile(filepath.Join(path, "data.db")); err != nil || string(data) != "after" {
		t.Fatalf("data after remount = %q, %v", data, err)
	}

	if exists, err := volumes.Exists(ctx, "acme"); err != nil || !exists {
		t.Fatalf("Exists = %v, %v", exists, err)
	}

	// A dataset made by hand below the parent is neither used nor destroyed.
	zfsOutput(t, "create", "-o", "mountpoint="+filepath.Join(mountRoot, "manual"), pool+"/corral/tenants/manual")
	if _, _, err := volumes.Ensure(ctx, "manual", 10001, 10002); err == nil {
		t.Fatal("Ensure adopted a dataset corral did not create")
	}
	if err := volumes.Remove(ctx, "manual"); err == nil {
		t.Fatal("Remove destroyed a dataset corral did not create")
	}
	zfsOutput(t, "list", pool+"/corral/tenants/manual")

	if err := volumes.Remove(ctx, "acme"); err != nil {
		t.Fatal(err)
	}
	if output, err := exec.Command("zfs", "list", pool+"/corral/tenants/acme").CombinedOutput(); err == nil {
		t.Fatalf("dataset still exists after Remove:\n%s", output)
	}
	if err := volumes.Remove(ctx, "acme"); err != nil {
		t.Fatalf("second Remove = %v", err)
	}
}
