//go:build unix

package volume

import (
	"os"
	"path/filepath"
	"strconv"
	"testing"
)

// The owner said is the one most entries have, the directory itself aside;
// an empty directory has none.
func TestDataOwner(t *testing.T) {
	dir := t.TempDir()
	if who, mixed, err := DataOwner(dir); err != nil || who != "" || mixed {
		t.Fatalf("an empty directory: %q %v %v", who, mixed, err)
	}
	for _, name := range []string{"a", "b"} {
		if err := os.WriteFile(filepath.Join(dir, name), []byte("x"), 0o600); err != nil {
			t.Fatal(err)
		}
	}
	me := strconv.Itoa(os.Getuid()) + ":" + strconv.Itoa(os.Getgid())
	if who, mixed, err := DataOwner(dir); err != nil || who != me || mixed {
		t.Fatalf("files of one owner: %q %v %v, want %s", who, mixed, err, me)
	}
	// Another owner for one file needs root; as root, the most common wins.
	if os.Getuid() == 0 {
		if err := os.Chown(filepath.Join(dir, "a"), 10001, 10001); err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(filepath.Join(dir, "c"), []byte("x"), 0o600); err != nil {
			t.Fatal(err)
		}
		if who, mixed, _ := DataOwner(dir); who != me || !mixed {
			t.Fatalf("mixed owners: %q %v", who, mixed)
		}
	}
	if _, _, err := DataOwner(filepath.Join(dir, "missing")); err == nil {
		t.Fatal("a missing directory read without an error")
	}
}
