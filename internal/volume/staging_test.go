package volume

import (
	"context"
	"errors"
	"os"
	"strings"
	"sync"
	"testing"
	"time"
)

// A copy that takes over while it is being dropped is never destroyed:
// the drop holds the dataset's lock from its check to the destroy, so the
// promotion waits for it, or the drop finds the copy promoted and refuses.
func TestAPromotedCopyIsNeverDropped(t *testing.T) {
	fake, z := newReplicaZFS()
	z.MountRoot = t.TempDir()
	if err := os.MkdirAll(z.MountRoot+"/acme", 0o700); err != nil {
		t.Fatal(err)
	}
	fake.datasets["tank/murder/acme"] = &fakeDataset{owner: "acme", role: replicaRole, canmount: "noauto", mountpoint: z.MountRoot + "/acme"}
	var mu sync.Mutex
	checked, promoted := make(chan struct{}), make(chan struct{})
	first, destroyedLive := true, false
	run := z.Run
	z.Run = func(ctx context.Context, name string, args ...string) ([]byte, error) {
		mu.Lock()
		if args[0] == "destroy" && fake.ran("zfs mount tank/murder/acme") {
			destroyedLive = true
		}
		output, err := run(ctx, name, args...)
		dropChecked := first && args[0] == "get"
		first = false
		mu.Unlock()
		if args[0] == "mount" {
			close(promoted)
		}
		// The drop has checked the copy: a promotion gets every chance to
		// run before it destroys it.
		if dropChecked {
			close(checked)
			select {
			case <-promoted:
			case <-time.After(200 * time.Millisecond):
			}
		}
		return output, err
	}
	dropped := make(chan error, 1)
	go func() { dropped <- z.DropReplica(context.Background(), "acme") }()
	<-checked
	if _, err := z.Ensure(context.Background(), "acme", os.Getuid(), os.Getgid(), 0); err != nil {
		t.Fatal(err)
	}
	if err := <-dropped; err != nil {
		t.Fatal(err)
	}
	if destroyedLive {
		t.Fatalf("the copy was destroyed after it took over; calls %v", fake.calls)
	}
}

// Tenant "acme-old"'s staging datasets are not tenant "acme"'s, under
// today's names or the ones an older release gave, and a whole copy for
// acme leaves them alone. What an older release left of acme's own goes.
func TestStagingNamesOfTenantsNeverCollide(t *testing.T) {
	fake, z := newReplicaZFS()
	copyOf := func(tenant string) *fakeDataset {
		return &fakeDataset{owner: tenant, role: replicaRole, canmount: "noauto", mountpoint: "/srv/murder/tenants/" + tenant}
	}
	fake.datasets["tank/murder/acme"] = copyOf("acme")
	theirs := map[string]*fakeDataset{
		// acme-old's copy being received, under both names.
		"tank/murder/murder-incoming:acme-old": copyOf("acme-old"),
		"tank/murder/murder-incoming-acme-old": copyOf("acme-old"),
	}
	for name, d := range theirs {
		fake.datasets[name] = d
	}
	// What an older release left of acme's: the copy it replaced.
	fake.datasets["tank/murder/murder-incoming-acme-old-old"] = copyOf("acme-old")
	if err := z.ReceiveReplica(context.Background(), "acme", true, strings.NewReader("x"), 3); err != nil {
		t.Fatal(err)
	}
	for name, d := range theirs {
		if fake.datasets[name] != d {
			t.Errorf("acme's whole copy destroyed %s, acme-old's", name)
		}
	}
	if fake.datasets["tank/murder/murder-incoming-acme-old-old"] == nil {
		t.Error("acme's whole copy destroyed acme-old's old copy")
	}
	// An older release's leftover of acme itself: its old copy.
	fake.datasets["tank/murder/murder-incoming-acme-old"] = copyOf("acme")
	if err := z.ReceiveReplica(context.Background(), "acme", true, strings.NewReader("x"), 3); err != nil {
		t.Fatal(err)
	}
	if fake.datasets["tank/murder/murder-incoming-acme-old"] != nil {
		t.Error("acme's old copy, left by an older release, was kept")
	}
	if fake.datasets["tank/murder/acme"] == nil || fake.datasets["tank/murder/murder-incoming:acme-old"] != theirs["tank/murder/murder-incoming:acme-old"] {
		t.Fatalf("datasets %v", fake.datasets)
	}
}

// A whole copy's swap that stops half way, between putting the old copy
// aside and renaming the new one in place, is finished before the tenant's
// data is looked for: the old copy goes back, so a tenant that took the
// copy over finds its data. That the undo failed is said, not ignored.
func TestASwapThatStoppedHalfWayIsFinished(t *testing.T) {
	fake, z := newReplicaZFS()
	old := &fakeDataset{owner: "acme", role: replicaRole, canmount: "noauto", mountpoint: "/srv/murder/tenants/acme"}
	fake.datasets["tank/murder/acme"] = old
	run := z.Run
	broken := true
	z.Run = func(ctx context.Context, name string, args ...string) ([]byte, error) {
		if broken && args[0] == "rename" && args[2] == "tank/murder/acme" {
			return nil, errors.New("the pool went read-only")
		}
		return run(ctx, name, args...)
	}
	err := z.ReceiveReplica(context.Background(), "acme", true, strings.NewReader("x"), 3)
	if err == nil || !strings.Contains(err.Error(), "putting the old copy back from tank/murder/murder-previous:acme failed too") {
		t.Fatalf("the failed swap and undo: %v", err)
	}
	if fake.datasets["tank/murder/murder-previous:acme"] != old || fake.datasets["tank/murder/murder-incoming:acme"] == nil {
		t.Fatalf("datasets %v", fake.datasets)
	}
	broken = false
	if exists, err := z.Exists(context.Background(), "acme"); err != nil || !exists {
		t.Fatalf("exists %v, %v", exists, err)
	}
	if fake.datasets["tank/murder/acme"] != old || fake.datasets["tank/murder/murder-previous:acme"] != nil || fake.datasets["tank/murder/murder-incoming:acme"] != nil {
		t.Fatalf("after the swap was finished: %v", fake.datasets)
	}

	// The same, left by an older release under its names, and found by a
	// promotion.
	z.MountRoot = t.TempDir()
	if err := os.MkdirAll(z.MountRoot+"/beta", 0o700); err != nil {
		t.Fatal(err)
	}
	legacy := &fakeDataset{owner: "beta", role: replicaRole, canmount: "noauto", mountpoint: z.MountRoot + "/beta"}
	fake.datasets["tank/murder/murder-incoming-beta-old"] = legacy
	fake.datasets["tank/murder/murder-incoming-beta"] = &fakeDataset{owner: "beta", role: replicaRole, canmount: "noauto", mountpoint: z.MountRoot + "/beta"}
	if _, err := z.Ensure(context.Background(), "beta", os.Getuid(), os.Getgid(), 0); err != nil {
		t.Fatal(err)
	}
	if fake.datasets["tank/murder/beta"] != legacy || !legacy.mounted || legacy.role != "" || fake.datasets["tank/murder/murder-incoming-beta"] != nil {
		t.Fatalf("the promoted copy: %+v; datasets %v", legacy, fake.datasets)
	}
}

// Purging a tenant also destroys what a whole copy's receive left of it.
func TestRemoveTakesTheStagingDatasetsAlong(t *testing.T) {
	fake, z := newReplicaZFS()
	copyOf := func(tenant string) *fakeDataset {
		return &fakeDataset{owner: tenant, role: replicaRole, canmount: "noauto", mountpoint: "/srv/murder/tenants/" + tenant}
	}
	fake.datasets["tank/murder/acme"] = copyOf("acme")
	fake.datasets["tank/murder/murder-incoming:acme"] = copyOf("acme")
	fake.datasets["tank/murder/murder-incoming-acme-old"] = copyOf("acme-old")
	if err := z.Remove(context.Background(), "acme"); err != nil {
		t.Fatal(err)
	}
	if fake.datasets["tank/murder/acme"] != nil || fake.datasets["tank/murder/murder-incoming:acme"] != nil {
		t.Fatalf("left %v", fake.datasets)
	}
	if fake.datasets["tank/murder/murder-incoming-acme-old"] == nil {
		t.Fatal("acme-old's copy destroyed with acme")
	}
}
