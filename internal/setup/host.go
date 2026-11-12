package setup

import (
	"bytes"
	"context"
	"fmt"
	"io/fs"
	"net/netip"
	"os"
	"path/filepath"
	"strconv"
	"strings"

	"github.com/PowerPenguini/murder/internal/hostprobe"
)

// System is the host setup runs on.
type System struct{}

func (System) Run(ctx context.Context, name string, args ...string) (string, error) {
	return hostprobe.Exec{}.Run(ctx, name, args...)
}

func (System) ReadFile(path string) ([]byte, error) { return os.ReadFile(path) }

// WriteFile writes through a temporary file renamed into place, so a
// binary in use is replaced, not changed under the process that runs it.
func (System) WriteFile(path string, data []byte, perm fs.FileMode) error {
	temp, err := os.CreateTemp(filepath.Dir(path), "."+filepath.Base(path)+".*")
	if err != nil {
		return err
	}
	defer os.Remove(temp.Name())
	if _, err := temp.Write(data); err != nil {
		temp.Close()
		return err
	}
	if err := temp.Chmod(perm); err != nil {
		temp.Close()
		return err
	}
	if err := temp.Sync(); err != nil {
		temp.Close()
		return err
	}
	if err := temp.Close(); err != nil {
		return err
	}
	return os.Rename(temp.Name(), path)
}

func (System) Remove(path string) error { return os.Remove(path) }

func (System) MkdirAll(path string, perm fs.FileMode) error { return os.MkdirAll(path, perm) }

func (System) Exists(path string) bool {
	_, err := os.Stat(path)
	return err == nil
}

func (System) Executable() (string, error) {
	path, err := os.Executable()
	if err != nil {
		return "", err
	}
	return filepath.EvalSymlinks(path)
}

func (System) Hostname() (string, error) { return os.Hostname() }

func (System) Addresses() ([]netip.Addr, error) { return hostprobe.Addresses() }

func (System) Root() bool { return os.Geteuid() == 0 }

// Connection is SSH_CONNECTION, from this process or the closest of its
// parents that has it: sudo leaves it out of the environment it makes.
func (System) Connection() string {
	if c := os.Getenv("SSH_CONNECTION"); c != "" {
		return c
	}
	pid := os.Getppid()
	for range 8 {
		if pid <= 1 {
			break
		}
		if environ, err := os.ReadFile(fmt.Sprintf("/proc/%d/environ", pid)); err == nil {
			for _, entry := range strings.Split(string(environ), "\x00") {
				if c, ok := strings.CutPrefix(entry, "SSH_CONNECTION="); ok {
					return c
				}
			}
		}
		stat, err := os.ReadFile(fmt.Sprintf("/proc/%d/stat", pid))
		if err != nil {
			break
		}
		// The parent is the 4th field, after the name in parentheses.
		fields := strings.Fields(string(stat[bytes.LastIndexByte(stat, ')')+1:]))
		if len(fields) < 2 {
			break
		}
		if pid, err = strconv.Atoi(fields[1]); err != nil {
			break
		}
	}
	return ""
}

func (System) Invoker() string {
	if account := os.Getenv("SUDO_USER"); account != "" {
		return account
	}
	return "root"
}
