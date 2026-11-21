package pluginhost

import (
	"net"
	"os"
	"os/exec"
	"path/filepath"
	"syscall"
	"testing"
)

// The test binary, run again as the container's user, makes a socket
// where MURDER_TEST_SOCKET says, as a plugin does, and fills what room is
// left: what a container could do to its socket's directory.
func TestMain(m *testing.M) {
	if path := os.Getenv("MURDER_TEST_SOCKET"); path != "" {
		listener, err := net.Listen("unix", path)
		if err != nil {
			os.Stderr.WriteString(err.Error())
			os.Exit(3)
		}
		defer listener.Close()
		filled := 0
		for i := range 64 {
			if os.WriteFile(filepath.Join(filepath.Dir(path), "fill"+string(rune('a'+i%26))+string(rune('a'+i/26))), make([]byte, 16<<10), 0o600) != nil {
				break
			}
			filled++
		}
		if filled >= 8 {
			os.Stderr.WriteString("the tmpfs holds more than 64 KB")
			os.Exit(4)
		}
		os.Exit(0)
	}
	os.Exit(m.Run())
}

// On a node, each socket's directory is a tmpfs of its own, mounted once
// on a /run that is a tmpfs already: the container's user's, others only
// passing through, a few kilobytes; and it goes with the container.
func TestEachSocketsDirectoryIsATmpfsOfItsOwn(t *testing.T) {
	if os.Getuid() != 0 || os.Getenv("MURDER_TEST_MOUNTS") != "1" {
		t.Skip("mounts as root: MURDER_TEST_MOUNTS=1, in a privileged container (docs/develop.md)")
	}
	root := filepath.Join(t.TempDir(), "plugins")
	host := &Host{Root: root, Tmpfs: true}
	dir := filepath.Join(root, "0123456789ab")
	for range 2 {
		if err := host.ensureDir(dir); err != nil {
			t.Fatal(err)
		}
	}
	if is, err := mounted(dir); err != nil || !is {
		t.Fatalf("mounted %v, %v", is, err)
	}
	var stat syscall.Stat_t
	if err := syscall.Stat(dir, &stat); err != nil || stat.Uid != uid || stat.Mode&0o777 != 0o711 {
		t.Fatalf("owner %d, mode %o: %v", stat.Uid, stat.Mode&0o777, err)
	}
	if info, _ := os.Stat(root); info.Mode().Perm() != 0o700 {
		t.Fatalf("the root's mode %v", info.Mode().Perm())
	}
	// Mounted once: a second mount would hide the first, and a container's
	// socket with it.
	var mounts int
	if data, err := os.ReadFile("/proc/self/mountinfo"); err == nil {
		for _, line := range splitLines(string(data)) {
			if containsField(line, dir) {
				mounts++
			}
		}
	}
	if mounts != 1 {
		t.Fatalf("mounted %d times", mounts)
	}
	// The container's user cannot reach it by its path: only root passes
	// through the root.
	socket := filepath.Join(dir, "plugin.sock")
	asUser := func(path string) ([]byte, error) {
		child := exec.Command(os.Args[0], "-test.run", "^$")
		child.Env = append(os.Environ(), "MURDER_TEST_SOCKET="+path)
		child.SysProcAttr = &syscall.SysProcAttr{Credential: &syscall.Credential{Uid: uid, Gid: uid}}
		return child.CombinedOutput()
	}
	if _, err := asUser(socket); err == nil {
		t.Fatal("the container's user reached the directory through the root")
	}
	// Bound elsewhere, as the engine mounts it in the container, it makes
	// its socket there, and cannot fill more than the tmpfs holds.
	view, err := os.MkdirTemp("/", "view")
	if err != nil {
		t.Fatal(err)
	}
	defer os.Remove(view)
	if err := syscall.Mount(dir, view, "", syscall.MS_BIND, ""); err != nil {
		t.Fatal(err)
	}
	if output, err := asUser(filepath.Join(view, "plugin.sock")); err != nil {
		syscall.Unmount(view, 0)
		t.Fatalf("as the container's user, in its view: %v: %s", err, output)
	}
	if err := syscall.Unmount(view, 0); err != nil {
		t.Fatal(err)
	}
	if _, err := os.Stat(socket); err != nil {
		t.Fatalf("root does not see the socket: %v", err)
	}
	if err := host.removeDir(dir); err != nil {
		t.Fatal(err)
	}
	if _, err := os.Stat(dir); !os.IsNotExist(err) {
		t.Fatalf("the directory stays: %v", err)
	}
	// Nothing mounted, nothing to unmount.
	if err := host.removeDir(dir); err != nil {
		t.Fatal(err)
	}
}

func splitLines(s string) []string {
	var lines []string
	for len(s) > 0 {
		i := 0
		for i < len(s) && s[i] != '\n' {
			i++
		}
		lines = append(lines, s[:i])
		if i < len(s) {
			i++
		}
		s = s[i:]
	}
	return lines
}

// containsField reports whether a line of mountinfo has path as its
// mount point (its fifth field).
func containsField(line, path string) bool {
	field, start := 0, 0
	for i := 0; i <= len(line); i++ {
		if i == len(line) || line[i] == ' ' {
			if field == 4 {
				return line[start:i] == path
			}
			field++
			start = i + 1
		}
	}
	return false
}
