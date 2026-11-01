package volume

import (
	"context"
	"path/filepath"
	"strings"
	"testing"
)

func replaceFixture(t *testing.T) (*fakeZFS, ZFS) {
	t.Helper()
	root := t.TempDir()
	fake := &fakeZFS{datasets: map[string]*fakeDataset{
		"tank/murder/acme":                       {owner: "acme", mountpoint: filepath.Join(root, "acme"), mounted: true, canmount: "on"},
		"tank/murder/murder-copy-acme-restore-1": {owner: "acme", mountpoint: filepath.Join(root, ".copies", "murder-copy-acme-restore-1"), mounted: true},
	}}
	return fake, ZFS{Parent: "tank/murder", MountRoot: root, Run: fake.run}
}

// A restored copy takes the tenant's place; what it had stays aside,
// never mounted.
func TestACopyTakesTheTenantsPlace(t *testing.T) {
	fake, z := replaceFixture(t)
	aside, err := z.ReplaceWithCopy(context.Background(), "acme", "murder-copy-acme-restore-1")
	if err != nil {
		t.Fatal(err)
	}
	tenant, kept := fake.datasets["tank/murder/acme"], fake.datasets[aside]
	if aside != "tank/murder/murder-replaced-acme-restore-1" || kept == nil || tenant == nil {
		t.Fatalf("aside %q, datasets %v", aside, fake.datasets)
	}
	if tenant.mountpoint != filepath.Join(z.MountRoot, "acme") || !tenant.mounted {
		t.Fatalf("the restored data: %+v", tenant)
	}
	if kept.canmount != "noauto" || kept.mounted || !strings.Contains(kept.mountpoint, ".replaced") {
		t.Fatalf("the data put aside: %+v", kept)
	}
}

// A step that fails undoes the ones before: the tenant keeps its data,
// mounted where it was, and the copy stays a copy.
func TestAFailedReplacementIsUndone(t *testing.T) {
	for _, failing := range []string{"rename tank/murder/murder-copy", "set canmount=noauto", "set mountpoint="} {
		t.Run(failing, func(t *testing.T) {
			fake, z := replaceFixture(t)
			before := *fake.datasets["tank/murder/acme"]
			fake.fail = failing
			aside, err := z.ReplaceWithCopy(context.Background(), "acme", "murder-copy-acme-restore-1")
			if err == nil || !strings.Contains(err.Error(), "nothing was changed") || aside != "" {
				t.Fatalf("aside %q, error %v", aside, err)
			}
			tenant := fake.datasets["tank/murder/acme"]
			if tenant == nil || tenant.mountpoint != before.mountpoint || !tenant.mounted || tenant.canmount == "noauto" {
				t.Fatalf("the tenant's data after an undone replacement: %+v", tenant)
			}
			if fake.datasets["tank/murder/murder-copy-acme-restore-1"] == nil {
				t.Fatal("the copy is gone")
			}
		})
	}
}

// A tenant without data, whose data a restore put aside, gets no new
// empty dataset that would hide it.
func TestDataPutAsideIsNotHiddenByANewDataset(t *testing.T) {
	fake, z := replaceFixture(t)
	fake.datasets["tank/murder/murder-replaced-acme-restore-1"] = fake.datasets["tank/murder/acme"]
	delete(fake.datasets, "tank/murder/acme")
	if _, err := z.Ensure(context.Background(), "acme", 1000, 1000, 0); err == nil || !strings.Contains(err.Error(), "murder-replaced-acme-restore-1") {
		t.Fatalf("a new dataset over data put aside: %v", err)
	}
}

func TestCopyNamesAreNeverTenants(t *testing.T) {
	for _, name := range []string{"acme", "murder-copy-a/b", "murder-copy-a@x", "murder-copy-a.b"} {
		if checkCopy(name) == nil {
			t.Errorf("%q passed for a copy", name)
		}
	}
	if checkCopy(CopyName("acme", "restore-1")) != nil {
		t.Error("a copy's own name refused")
	}
}
