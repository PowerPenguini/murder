package volume

import (
	"context"
	"io"
	"os"
	"slices"
	"strings"
	"testing"
	"time"
)

// replicaZFS answers the commands replication runs, and records them.
type replicaZFS struct {
	fakeZFS
	piped []string
	list  string // what "zfs list ... -d 1 tank/murder" prints
}

func newReplicaZFS() (*replicaZFS, ZFS) {
	fake := &replicaZFS{fakeZFS: fakeZFS{datasets: map[string]*fakeDataset{}}}
	z := ZFS{Parent: "tank/murder", MountRoot: "/srv/murder/tenants", Run: fake.run, Pipe: fake.pipe,
		Now: func() time.Time { return time.Date(2026, 11, 2, 10, 0, 0, 0, time.UTC) }}
	return fake, z
}

func (f *replicaZFS) run(ctx context.Context, name string, args ...string) ([]byte, error) {
	call := strings.Join(args, " ")
	switch {
	case strings.HasPrefix(call, "list -H -p -o name,"):
		f.calls = append(f.calls, name+" "+call)
		return []byte(f.list), nil
	case strings.HasPrefix(call, "get -H -p -o value written@"):
		f.calls = append(f.calls, name+" "+call)
		return []byte("4096\n"), nil
	}
	return f.fakeZFS.run(ctx, name, args...)
}

func (f *replicaZFS) pipe(_ context.Context, stdin io.Reader, stdout io.Writer, name string, args ...string) error {
	f.piped = append(f.piped, name+" "+strings.Join(args, " "))
	if stdin != nil {
		_, _ = io.Copy(io.Discard, stdin)
	}
	if args[0] == "receive" {
		dataset := args[len(args)-1]
		f.datasets[dataset] = &fakeDataset{owner: strings.TrimPrefix(dataset, "tank/murder/"), role: replicaRole, canmount: "noauto", mountpoint: "/srv/murder/tenants/" + strings.TrimPrefix(dataset, "tank/murder/")}
	}
	if args[0] == "send" {
		_, _ = io.WriteString(stdout, "stream")
	}
	return nil
}

// A copy is received unmounted and marked, never over live data; a whole
// copy replaces the one there, changes go on top of it.
func TestACopyIsReceivedUnmountedAndMarked(t *testing.T) {
	fake, z := newReplicaZFS()
	ctx := context.Background()
	if err := z.ReceiveReplica(ctx, "acme", false, strings.NewReader("x"), 3); err == nil {
		t.Fatal("changes received with no copy under them")
	}
	if err := z.ReceiveReplica(ctx, "acme", true, strings.NewReader("x"), 3); err != nil {
		t.Fatal(err)
	}
	want := "zfs receive -u -o canmount=noauto -o io.murder:role=replica -o mountpoint=/srv/murder/tenants/acme tank/murder/acme"
	if fake.piped[0] != want {
		t.Fatalf("received with %q, want %q", fake.piped[0], want)
	}
	if err := z.ReceiveReplica(ctx, "acme", false, strings.NewReader("x"), 3); err != nil {
		t.Fatal(err)
	}
	if !strings.HasSuffix(fake.piped[1], " -F tank/murder/acme") {
		t.Fatalf("changes received with %q", fake.piped[1])
	}
	if err := z.ReceiveReplica(ctx, "acme", true, strings.NewReader("x"), 3); err != nil || !fake.ran("zfs destroy -r tank/murder/acme") {
		t.Fatalf("a whole copy over a copy: %v, %v", err, fake.calls)
	}
	// Live data is never replaced, nor dropped as a copy.
	fake.datasets["tank/murder/live"] = &fakeDataset{owner: "live", mountpoint: "/srv/murder/tenants/live", mounted: true}
	if err := z.ReceiveReplica(ctx, "live", true, strings.NewReader("x"), 3); err == nil {
		t.Fatal("a copy received over live data")
	}
	if err := z.DropReplica(ctx, "live"); err == nil {
		t.Fatal("live data dropped as a copy")
	}
	if err := z.DropReplica(ctx, "acme"); err != nil {
		t.Fatal(err)
	}
}

// A promoted copy is mounted, at boot too, and no longer marked a copy.
func TestAPromotedCopyBecomesTheData(t *testing.T) {
	fake, z := newReplicaZFS()
	z.MountRoot = t.TempDir()
	fake.datasets["tank/murder/acme"] = &fakeDataset{owner: "acme", role: replicaRole, canmount: "noauto", mountpoint: z.MountRoot + "/acme"}
	if err := os.MkdirAll(z.MountRoot+"/acme", 0o700); err != nil {
		t.Fatal(err)
	}
	if _, err := z.Ensure(context.Background(), "acme", os.Getuid(), os.Getgid(), 0); err != nil {
		t.Fatal(err)
	}
	d := fake.datasets["tank/murder/acme"]
	if d.canmount != "on" || d.role != "" || !d.mounted {
		t.Fatalf("the promoted copy: %+v", d)
	}
}

// Sends name the tenant's own snapshots only.
func TestSendsAndSnapshotsOfACopy(t *testing.T) {
	fake, z := newReplicaZFS()
	ctx := context.Background()
	var out strings.Builder
	if err := z.SendSnapshot(ctx, "acme", "murder-replica-1", "murder-replica-2", &out); err != nil {
		t.Fatal(err)
	}
	if fake.piped[0] != "zfs send -p -i @murder-replica-1 tank/murder/acme@murder-replica-2" {
		t.Fatalf("sent with %q", fake.piped[0])
	}
	for _, bad := range []string{"other@x", "murder-x/y", "murder-a b", "base"} {
		if err := z.SendSnapshot(ctx, "acme", "", bad, &out); err == nil {
			t.Errorf("sent snapshot %q", bad)
		}
	}
	if written, err := z.Written(ctx, "acme", "murder-replica-2"); err != nil || written != 4096 {
		t.Fatalf("written %d, %v", written, err)
	}
	fake.snapshots = []string{"tank/murder/acme@murder-replica-2", "tank/murder/acme@murder-move-1", "tank/murder/acme@murder-replica-1", "tank/murder/acme@by-hand"}
	if names, err := z.Snapshots(ctx, "acme", ReplicaReason); err != nil || !slices.Equal(names, []string{"murder-replica-1", "murder-replica-2"}) {
		t.Fatalf("snapshots %v, %v", names, err)
	}
}

// Only datasets Murder made for the tenant they are named after count.
func TestDatasetsAreTenantsOnly(t *testing.T) {
	fake, z := newReplicaZFS()
	fake.list = strings.Join([]string{
		"tank/murder\t-\t-\tyes",
		"tank/murder/acme\tacme\t-\tyes",
		"tank/murder/beta\tbeta\treplica\tno",
		"tank/murder/other\tacme\t-\tyes",
		"tank/murder/murder-failed-acme-x\tacme\t-\tno",
		"tank/murder/home\t-\t-\tyes",
	}, "\n") + "\n"
	datasets, err := z.Datasets(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	want := []Dataset{{Tenant: "acme", Mounted: true}, {Tenant: "beta", Replica: true}}
	if !slices.Equal(datasets, want) {
		t.Fatalf("datasets %+v, want %+v", datasets, want)
	}
}

// Data put aside is unmounted, renamed out of every tenant's way and never
// mounted again.
func TestDataIsPutAside(t *testing.T) {
	fake, z := newReplicaZFS()
	fake.datasets["tank/murder/acme"] = &fakeDataset{owner: "acme", mountpoint: "/srv/murder/tenants/acme", mounted: true}
	aside, err := z.PutAside(context.Background(), "acme", "elsewhere")
	if err != nil {
		t.Fatal(err)
	}
	if aside != "tank/murder/murder-failed-acme-elsewhere-20261102t100000z" {
		t.Fatalf("put aside as %s", aside)
	}
	d := fake.datasets[aside]
	if d == nil || d.mounted || d.canmount != "off" || fake.datasets["tank/murder/acme"] != nil {
		t.Fatalf("put aside: %+v", d)
	}
	fake.datasets["tank/murder/foreign"] = &fakeDataset{owner: "someone", mountpoint: "/x", mounted: true}
	if _, err := z.PutAside(context.Background(), "foreign", "elsewhere"); err == nil {
		t.Fatal("a foreign dataset put aside")
	}
	if _, err := z.PutAside(context.Background(), "acme", "Bad Reason"); err == nil {
		t.Fatal("a reason that could change the name's shape")
	}
}
