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

	// An older release's staging datasets say nothing of which swap, or
	// which tenant of the name, they were part of: a promotion never puts
	// them back, and the tenant, if it ran before, is refused for missing
	// data rather than given another's.
	z.MountRoot = t.TempDir()
	legacy := &fakeDataset{owner: "beta", role: replicaRole, canmount: "noauto", mountpoint: z.MountRoot + "/beta"}
	fake.datasets["tank/murder/murder-incoming-beta-old"] = legacy
	fake.datasets["tank/murder/murder-incoming-beta"] = &fakeDataset{owner: "beta", role: replicaRole, canmount: "noauto", mountpoint: z.MountRoot + "/beta"}
	if exists, err := z.Exists(context.Background(), "beta"); err != nil || exists {
		t.Fatalf("exists %v, %v", exists, err)
	}
	if _, err := z.Ensure(context.Background(), "beta", os.Getuid(), os.Getgid(), 0); err != nil {
		t.Fatal(err)
	}
	if fake.datasets["tank/murder/beta"] == legacy || legacy.mounted || fake.datasets["tank/murder/murder-incoming-beta-old"] != legacy {
		t.Fatalf("an older release's staging copy was put back: %+v; datasets %v", legacy, fake.datasets)
	}
}

// A whole copy that replaced the old one, but failed to destroy it, leaves
// the old one behind. A new tenant of the same name, which drops the copy
// of the earlier one, never gets that old copy put back as its data.
func TestAFinishedSwapsLeftoverIsNeverPutBack(t *testing.T) {
	fake, z := newReplicaZFS()
	z.MountRoot = t.TempDir()
	old := &fakeDataset{owner: "acme", role: replicaRole, canmount: "noauto", mountpoint: z.MountRoot + "/acme"}
	fake.datasets["tank/murder/acme"] = old
	fake.fail = "destroy -r tank/murder/murder-previous:acme"
	if err := z.ReceiveReplica(context.Background(), "acme", true, strings.NewReader("x"), 3); err == nil {
		t.Fatal("the failed destroy of the old copy was not reported")
	}
	if fake.datasets["tank/murder/murder-previous:acme"] != old || fake.datasets["tank/murder/acme"] == old {
		t.Fatalf("datasets %v", fake.datasets)
	}
	if old.swap != "" {
		t.Errorf("the old copy of a finished swap still carries the swap's token %q", old.swap)
	}
	fake.fail = ""
	// checkData drops the earlier tenant's copy; the new one starts.
	if err := z.DropReplica(context.Background(), "acme"); err != nil {
		t.Fatal(err)
	}
	if fake.datasets["tank/murder/murder-previous:acme"] != nil {
		t.Errorf("dropping the copy left the old copy behind: %v", fake.datasets)
	}
	if exists, err := z.Exists(context.Background(), "acme"); err != nil || exists {
		t.Fatalf("exists %v, %v", exists, err)
	}
	if _, err := z.Ensure(context.Background(), "acme", os.Getuid(), os.Getgid(), 0); err != nil {
		t.Fatal(err)
	}
	if d := fake.datasets["tank/murder/acme"]; d == old || d.role != "" {
		t.Fatalf("the new tenant got the earlier tenant's copy: %+v", d)
	}
}

// Only an old copy and a received one of the same swap make a swap to
// finish: an old copy left alone, one without a token, or one whose token
// the received copy does not carry, is never put back.
func TestOnlyTheCopiesOfOneSwapAreSwappedBack(t *testing.T) {
	for name, pair := range map[string]struct{ previous, incoming string }{
		"old copy without a token":   {"", "t1"},
		"tokens of different swaps":  {"t1", "t2"},
		"old copy alone, with token": {"t1", "none"},
	} {
		t.Run(name, func(t *testing.T) {
			fake, z := newReplicaZFS()
			z.MountRoot = t.TempDir()
			old := &fakeDataset{owner: "acme", role: replicaRole, canmount: "noauto", mountpoint: z.MountRoot + "/acme", swap: pair.previous}
			fake.datasets["tank/murder/murder-previous:acme"] = old
			if pair.incoming != "none" {
				fake.datasets["tank/murder/murder-incoming:acme"] = &fakeDataset{owner: "acme", role: replicaRole, canmount: "noauto", mountpoint: z.MountRoot + "/acme", swap: pair.incoming}
			}
			if exists, err := z.Exists(context.Background(), "acme"); err != nil || exists {
				t.Fatalf("exists %v, %v", exists, err)
			}
			if _, err := z.Ensure(context.Background(), "acme", os.Getuid(), os.Getgid(), 0); err != nil {
				t.Fatal(err)
			}
			if fake.datasets["tank/murder/acme"] == old || old.mounted {
				t.Fatalf("an old copy of no swap in progress was put back; datasets %v", fake.datasets)
			}
		})
	}
}

// Dropping a copy whose swap stopped half way takes both its copies:
// neither is left to be put back for a later tenant of the name.
func TestDroppingAHalfSwappedCopyTakesBothCopies(t *testing.T) {
	fake, z := newReplicaZFS()
	copyOf := func() *fakeDataset {
		return &fakeDataset{owner: "acme", role: replicaRole, canmount: "noauto", mountpoint: "/srv/murder/tenants/acme", swap: "t1"}
	}
	fake.datasets["tank/murder/murder-previous:acme"] = copyOf()
	fake.datasets["tank/murder/murder-incoming:acme"] = copyOf()
	if err := z.DropReplica(context.Background(), "acme"); err != nil {
		t.Fatal(err)
	}
	if len(fake.datasets) != 0 {
		t.Fatalf("left %v", fake.datasets)
	}
}

// A dataset under the incoming name that is not the tenant's unmounted
// copy is neither received over nor destroyed.
func TestAnOccupiedIncomingNameIsLeftAlone(t *testing.T) {
	for name, there := range map[string]*fakeDataset{
		"another tenant's": {owner: "other", role: replicaRole, canmount: "noauto", mountpoint: "/srv/murder/tenants/other"},
		"mounted":          {owner: "acme", role: replicaRole, mounted: true, mountpoint: "/srv/murder/tenants/acme"},
		"not a copy":       {owner: "acme", mountpoint: "/srv/murder/tenants/acme"},
	} {
		t.Run(name, func(t *testing.T) {
			fake, z := newReplicaZFS()
			fake.datasets["tank/murder/murder-incoming:acme"] = there
			if err := z.ReceiveReplica(context.Background(), "acme", true, strings.NewReader("x"), 3); err == nil {
				t.Fatal("received a whole copy over an occupied name")
			}
			if fake.datasets["tank/murder/murder-incoming:acme"] != there || fake.ran("destroy") || len(fake.piped) != 0 {
				t.Fatalf("the dataset there was touched; calls %v, piped %v", fake.calls, fake.piped)
			}
		})
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

// Rolling back a copy whose swap stopped half way finishes the swap first:
// the copy is rolled back, not dropped as one with no snapshots.
func TestRollingBackAHalfSwappedCopyKeepsIt(t *testing.T) {
	fake, z := newReplicaZFS()
	old := &fakeDataset{owner: "acme", role: replicaRole, canmount: "noauto", mountpoint: "/srv/murder/tenants/acme", swap: "t1"}
	fake.datasets["tank/murder/murder-previous:acme"] = old
	fake.datasets["tank/murder/murder-incoming:acme"] = &fakeDataset{owner: "acme", role: replicaRole, canmount: "noauto", mountpoint: "/srv/murder/tenants/acme", swap: "t1"}
	fake.snapshots = []string{"tank/murder/acme@murder-replica-20261102T100000.000Z"}
	if err := z.RollbackReplica(context.Background(), "acme"); err != nil {
		t.Fatal(err)
	}
	if fake.datasets["tank/murder/acme"] != old || !fake.ran("zfs rollback -r tank/murder/acme@murder-replica-20261102T100000.000Z") {
		t.Fatalf("datasets %v; calls %v", fake.datasets, fake.calls)
	}
}

// Asked whether a copy whose swap stopped half way is a copy, IsReplica
// finishes the swap and says yes: a caller told no would remove the copy.
// An old copy of no swap in progress stays aside.
func TestIsReplicaFinishesAHalfSwappedCopy(t *testing.T) {
	fake, z := newReplicaZFS()
	copyOf := func(swap string) *fakeDataset {
		return &fakeDataset{owner: "acme", role: replicaRole, canmount: "noauto", mountpoint: "/srv/murder/tenants/acme", swap: swap}
	}
	old := copyOf("t1")
	fake.datasets["tank/murder/murder-previous:acme"] = old
	fake.datasets["tank/murder/murder-incoming:acme"] = copyOf("t1")
	if replica, err := z.IsReplica(context.Background(), "acme"); err != nil || !replica {
		t.Fatalf("IsReplica = %v, %v", replica, err)
	}
	if fake.datasets["tank/murder/acme"] != old || len(fake.datasets) != 1 || old.swap != "" {
		t.Fatalf("datasets %v", fake.datasets)
	}

	fake, z = newReplicaZFS()
	fake.datasets["tank/murder/murder-previous:acme"] = copyOf("t1")
	if replica, err := z.IsReplica(context.Background(), "acme"); err != nil || replica {
		t.Fatalf("IsReplica of an old copy alone = %v, %v", replica, err)
	}
	if fake.datasets["tank/murder/acme"] != nil || fake.datasets["tank/murder/murder-previous:acme"] == nil {
		t.Fatalf("datasets %v", fake.datasets)
	}
}
