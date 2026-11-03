package setup

import (
	"bytes"
	"context"
	"fmt"
	"io/fs"
	"net"
	"net/netip"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
)

// System is the host setup runs on.
type System struct{}

func (System) Run(ctx context.Context, name string, args ...string) (string, error) {
	var stdout, stderr bytes.Buffer
	command := exec.CommandContext(ctx, name, args...)
	command.Stdout, command.Stderr = &stdout, &stderr
	if err := command.Run(); err != nil {
		if message := strings.TrimSpace(stderr.String()); message != "" {
			return "", fmt.Errorf("%s %s: %w: %s", name, strings.Join(args, " "), err, message)
		}
		return "", fmt.Errorf("%s %s: %w", name, strings.Join(args, " "), err)
	}
	return stdout.String(), nil
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

// Addresses are those of the host's own interfaces that are up: not
// Docker's bridges and their containers' ends, nor tunnels.
func (System) Addresses() ([]netip.Addr, error) {
	interfaces, err := net.Interfaces()
	if err != nil {
		return nil, err
	}
	var addresses []netip.Addr
	for _, link := range interfaces {
		if link.Flags&net.FlagUp == 0 || link.Flags&net.FlagLoopback != 0 || virtual(link.Name) {
			continue
		}
		own, err := link.Addrs()
		if err != nil {
			continue
		}
		for _, address := range own {
			if prefix, err := netip.ParsePrefix(address.String()); err == nil {
				addresses = append(addresses, prefix.Addr().Unmap())
			}
		}
	}
	return addresses, nil
}

// virtual reports interfaces Docker, libvirt and VPNs make.
func virtual(name string) bool {
	for _, prefix := range []string{"docker", "br-", "veth", "virbr", "vnet", "wg", "tun", "tap", "tailscale", "zt", "cni", "flannel", "cali", "vxlan"} {
		if strings.HasPrefix(name, prefix) {
			return true
		}
	}
	return false
}

func (System) Root() bool { return os.Geteuid() == 0 }
