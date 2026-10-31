package volume

import (
	"archive/tar"
	"bytes"
	"context"
	"io"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

func TestDirSendsAndReceivesTheWholeTree(t *testing.T) {
	ctx := context.Background()
	from := Dir{Root: filepath.Join(t.TempDir(), "from")}
	to := Dir{Root: filepath.Join(t.TempDir(), "to")}
	path, err := from.Ensure(ctx, "acme", os.Getuid(), os.Getgid(), 0)
	if err != nil {
		t.Fatal(err)
	}
	if err := os.MkdirAll(filepath.Join(path, "receipts", "2026"), 0o750); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(path, "receipts", "2026", "a.jpg"), []byte("jpeg"), 0o640); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(path, "app.db"), []byte("database"), 0o600); err != nil {
		t.Fatal(err)
	}
	if err := os.Symlink("app.db", filepath.Join(path, "current")); err != nil {
		t.Fatal(err)
	}
	old := time.Date(2026, 1, 2, 3, 4, 5, 0, time.UTC)
	if err := os.Chtimes(filepath.Join(path, "app.db"), old, old); err != nil {
		t.Fatal(err)
	}
	var stream bytes.Buffer
	if _, err := from.Send(ctx, "acme", "", &stream); err != nil {
		t.Fatal(err)
	}
	// What the receiving node had before is replaced, not merged.
	stale, _ := to.Ensure(ctx, "acme", os.Getuid(), os.Getgid(), 0)
	if err := os.WriteFile(filepath.Join(stale, "stale"), []byte("x"), 0o600); err != nil {
		t.Fatal(err)
	}
	if err := to.Receive(ctx, "acme", false, &stream); err != nil {
		t.Fatal(err)
	}
	got := filepath.Join(to.Root, "acme")
	if data, err := os.ReadFile(filepath.Join(got, "receipts", "2026", "a.jpg")); err != nil || string(data) != "jpeg" {
		t.Fatalf("receipt: %q, %v", data, err)
	}
	if info, err := os.Stat(filepath.Join(got, "app.db")); err != nil || info.Mode().Perm() != 0o600 || !info.ModTime().Equal(old) {
		t.Fatalf("app.db: %v, %v", info, err)
	}
	if info, _ := os.Stat(filepath.Join(got, "receipts")); info.Mode().Perm() != 0o750 {
		t.Fatalf("directory mode %v", info.Mode())
	}
	if link, err := os.Readlink(filepath.Join(got, "current")); err != nil || link != "app.db" {
		t.Fatalf("symlink %q, %v", link, err)
	}
	if _, err := os.Stat(filepath.Join(got, "stale")); !os.IsNotExist(err) {
		t.Fatal("what the node had before was kept")
	}
	entries, _ := os.ReadDir(to.Root)
	if len(entries) != 1 {
		t.Fatalf("left beside the data: %v", entries)
	}
}

func TestReceiveRefusesEntriesOutsideTheData(t *testing.T) {
	var stream bytes.Buffer
	archive := tar.NewWriter(&stream)
	_ = archive.WriteHeader(&tar.Header{Name: "../escape", Typeflag: tar.TypeReg, Mode: 0o600, Size: 1})
	_, _ = archive.Write([]byte("x"))
	_ = archive.Close()
	to := Dir{Root: filepath.Join(t.TempDir(), "to")}
	if err := to.Receive(context.Background(), "acme", false, &stream); err == nil || !strings.Contains(err.Error(), "leaves") {
		t.Fatalf("receive: %v", err)
	}
	if _, err := os.Stat(filepath.Join(to.Root, "escape")); !os.IsNotExist(err) {
		t.Fatal("an entry landed outside the data")
	}
}

func TestZFSSendsSnapshotsAndReceivesUnmounted(t *testing.T) {
	ctx := context.Background()
	fake := &fakeZFS{datasets: map[string]*fakeDataset{"tank/murder/acme": {owner: "acme", mountpoint: "/elsewhere/acme", mounted: true}}}
	var piped []string
	pipe := func(_ context.Context, stdin io.Reader, stdout io.Writer, name string, args ...string) error {
		piped = append(piped, name+" "+strings.Join(args, " "))
		if stdin != nil {
			data, _ := io.ReadAll(stdin)
			if args[0] == "receive" && !strings.Contains(strings.Join(args, " "), "-F") {
				fake.datasets[args[len(args)-1]] = &fakeDataset{owner: "acme", mountpoint: "/elsewhere/acme"}
			}
			_ = data
			return nil
		}
		_, err := stdout.Write([]byte("stream"))
		return err
	}
	clock := time.Date(2026, 10, 30, 12, 0, 0, 0, time.UTC)
	z := ZFS{Parent: "tank/murder", MountRoot: "/srv/murder/tenants", Run: fake.run, Pipe: pipe, Now: func() time.Time { return clock }}
	var stream bytes.Buffer
	first, err := z.Send(ctx, "acme", "", &stream)
	if err != nil || stream.String() != "stream" {
		t.Fatalf("send: %q, %v", stream.String(), err)
	}
	clock = clock.Add(time.Minute)
	second, err := z.Send(ctx, "acme", first, &stream)
	if err != nil {
		t.Fatal(err)
	}
	if piped[0] != "zfs send -p "+first || piped[1] != "zfs send -p -i "+first+" "+second || !strings.HasPrefix(first, "tank/murder/acme@murder-move-") {
		t.Fatalf("sends: %v", piped)
	}

	// On the other node: whole into nothing, then changes on top.
	other := &fakeZFS{datasets: map[string]*fakeDataset{}}
	fake = other
	z2 := ZFS{Parent: "tank/murder", MountRoot: "/srv/murder/tenants", Run: other.run, Pipe: pipe}
	if err := z2.Receive(ctx, "acme", true, strings.NewReader("x")); err == nil {
		t.Fatal("changes received with no whole copy under them")
	}
	piped = nil
	if err := z2.Receive(ctx, "acme", false, strings.NewReader("x")); err != nil {
		t.Fatal(err)
	}
	if err := z2.Receive(ctx, "acme", false, strings.NewReader("x")); err == nil {
		t.Fatal("a whole copy received over data the node has")
	}
	if err := z2.Receive(ctx, "acme", true, strings.NewReader("x")); err != nil {
		t.Fatal(err)
	}
	if piped[0] != "zfs receive -u tank/murder/acme" || piped[1] != "zfs receive -u -F tank/murder/acme" {
		t.Fatalf("receives: %v", piped)
	}
	if other.datasets["tank/murder/acme"].mountpoint != "/srv/murder/tenants/acme" || other.datasets["tank/murder/acme"].mounted {
		t.Fatalf("received dataset: %+v", other.datasets["tank/murder/acme"])
	}
}

func TestReceiveNeverWritesThroughASymlink(t *testing.T) {
	outside := t.TempDir()
	var stream bytes.Buffer
	archive := tar.NewWriter(&stream)
	_ = archive.WriteHeader(&tar.Header{Name: "a", Typeflag: tar.TypeSymlink, Linkname: outside})
	_ = archive.WriteHeader(&tar.Header{Name: "a/x", Typeflag: tar.TypeReg, Mode: 0o4755, Size: 1})
	_, _ = archive.Write([]byte("x"))
	_ = archive.Close()
	to := Dir{Root: filepath.Join(t.TempDir(), "to")}
	if err := to.Receive(context.Background(), "acme", false, &stream); err == nil {
		t.Fatal("an entry was written through a symlink")
	}
	if entries, _ := os.ReadDir(outside); len(entries) != 0 {
		t.Fatalf("written outside: %v", entries)
	}
}

func TestReceiveDropsSetuidBits(t *testing.T) {
	var stream bytes.Buffer
	archive := tar.NewWriter(&stream)
	_ = archive.WriteHeader(&tar.Header{Name: "tool", Typeflag: tar.TypeReg, Mode: 0o6755, Size: 1})
	_, _ = archive.Write([]byte("x"))
	_ = archive.Close()
	to := Dir{Root: filepath.Join(t.TempDir(), "to")}
	if err := to.Receive(context.Background(), "acme", false, &stream); err != nil {
		t.Fatal(err)
	}
	info, err := os.Stat(filepath.Join(to.Root, "acme", "tool"))
	if err != nil || info.Mode()&(os.ModeSetuid|os.ModeSetgid) != 0 || info.Mode().Perm() != 0o755 {
		t.Fatalf("mode %v, %v", info.Mode(), err)
	}
}

func TestATenantWithoutDataSendsAnEmptyTree(t *testing.T) {
	from := Dir{Root: filepath.Join(t.TempDir(), "from")}
	to := Dir{Root: filepath.Join(t.TempDir(), "to")}
	var stream bytes.Buffer
	if _, err := from.Send(context.Background(), "acme", "", &stream); err != nil {
		t.Fatal(err)
	}
	if err := to.Receive(context.Background(), "acme", false, &stream); err != nil {
		t.Fatal(err)
	}
	if info, err := os.Stat(filepath.Join(to.Root, "acme")); err != nil || !info.IsDir() {
		t.Fatalf("received %v, %v", info, err)
	}
}
