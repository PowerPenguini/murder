package setup

import (
	"bytes"
	"context"
	"fmt"
	"io/fs"
	"path"
	"strings"

	"github.com/PowerPenguini/murder/deploy"
)

// Target is where a node has the install file name (one of deploy.Files).
func Target(name string) string {
	switch name {
	case "murder-sysctl.conf":
		return SysctlFile
	case "murder-hardening.conf":
		return HardeningFile
	case "docker-after-zfs.conf":
		return DockerDropIn
	}
	return path.Join(UnitDir, name)
}

// Refresh brings the install files the node has to this binary's, so a
// node upgraded keeps the units and settings of the release it runs, as
// one set up anew gets them. A file the node does not have stays out: it
// was left out by hand, or (Docker's drop-in) needs no ZFS. A file that
// differs is replaced whole; changes of one's own belong in a drop-in
// (systemctl edit murder-edge), which stays. Units changed are read
// again (daemon-reload) and sysctl files applied; nothing is restarted,
// so the services run under a changed unit from their next start.
// Refresh returns the files it replaced. A binary rolled back to brings
// its own files back the same way.
func Refresh(ctx context.Context, host Host) ([]string, error) {
	files, err := fs.ReadDir(deploy.Files, ".")
	if err != nil {
		return nil, err
	}
	var replaced, sysctls []string
	units := false
	for _, file := range files {
		target := Target(file.Name())
		if !host.Exists(target) {
			continue
		}
		have, err := host.ReadFile(target)
		if err != nil {
			return replaced, err
		}
		want, err := deploy.Files.ReadFile(file.Name())
		if err != nil {
			return replaced, err
		}
		if bytes.Equal(have, want) {
			continue
		}
		if err := host.WriteFile(target, want, 0o644); err != nil {
			return replaced, err
		}
		replaced = append(replaced, target)
		if strings.HasPrefix(target, "/etc/sysctl.d/") {
			sysctls = append(sysctls, target)
		} else {
			units = true
		}
	}
	if units {
		if _, err := host.Run(ctx, "systemctl", "daemon-reload"); err != nil {
			return replaced, err
		}
	}
	for _, file := range sysctls {
		if _, err := host.Run(ctx, "sysctl", "-q", "-e", "-p", file); err != nil {
			return replaced, fmt.Errorf("apply %s: %w", file, err)
		}
	}
	return replaced, nil
}
