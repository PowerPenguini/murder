package setup

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io/fs"
	"path"
	"slices"
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

// EdgePathsDropIn lets the confined edge write the directories its config
// moved out of the two its unit keeps writable.
const EdgePathsDropIn = "/etc/systemd/system/murder-edge.service.d/murder-paths.conf"

// edgeOwnDirs are the directories murder-edge.service keeps writable.
var edgeOwnDirs = []string{"/var/lib/murder", "/run/murder-edge"}

// EdgeWritable names, of the directories the edge writes, those outside
// the ones its unit keeps writable: each needs a ReadWritePaths.
func EdgeWritable(dirs ...string) []string {
	var outside []string
	for _, dir := range dirs {
		dir = path.Clean(dir)
		if dir == "." || !path.IsAbs(dir) || slices.ContainsFunc(edgeOwnDirs, func(own string) bool { return dir == own || strings.HasPrefix(dir, own+"/") }) {
			continue
		}
		outside = append(outside, dir)
	}
	slices.Sort(outside)
	return slices.Compact(outside)
}

// edgePaths is the drop-in that makes dirs writable for the edge.
func edgePaths(dirs []string) []byte {
	var b bytes.Buffer
	b.WriteString("# Written by the murder agent: the directories the edge's config names\n# outside /var/lib/murder and /run/murder-edge. It is rewritten at every\n# start of the agent; edit the config instead.\n[Service]\n")
	for _, dir := range dirs {
		b.WriteString("ReadWritePaths=" + dir + "\n")
	}
	return b.Bytes()
}

// Refresh brings the install files the node has to this binary's, so a
// node upgraded keeps the units and settings of the release it runs, as
// one set up anew gets them. A file the node does not have stays out: it
// was left out by hand, or (Docker's drop-in) needs no ZFS; doctor names
// settings left out. A file that differs is replaced whole; changes of
// one's own belong in a drop-in (systemctl edit murder-edge), which stays.
// edgeDirs are the directories outside its own the edge's config has it
// write; a drop-in lets the confined edge write them.
//
// Units changed are read again (daemon-reload) and sysctl files applied;
// nothing is restarted, so the services run under a changed unit from
// their next start. A daemon-reload that failed is done at the next start
// (systemd says it is due). Refresh returns the files it replaced, with
// any error after it went on with the rest. Releases before this one do
// not refresh: one rolled back to keeps the newer files, as it ran with
// before.
func Refresh(ctx context.Context, host Host, edgeDirs []string) ([]string, error) {
	files, err := fs.ReadDir(deploy.Files, ".")
	if err != nil {
		return nil, err
	}
	want := map[string][]byte{}
	var targets []string
	for _, file := range files {
		data, err := deploy.Files.ReadFile(file.Name())
		if err != nil {
			return nil, err
		}
		target := Target(file.Name())
		if host.Exists(target) {
			want[target] = data
			targets = append(targets, target)
		}
	}
	if len(edgeDirs) > 0 || host.Exists(EdgePathsDropIn) {
		want[EdgePathsDropIn] = edgePaths(edgeDirs)
		targets = append(targets, EdgePathsDropIn)
	}
	var replaced []string
	var errs []error
	for _, target := range targets {
		if have, err := host.ReadFile(target); err == nil && bytes.Equal(have, want[target]) {
			continue
		}
		if err := host.MkdirAll(path.Dir(target), 0o755); err != nil {
			errs = append(errs, err)
			continue
		}
		if err := host.WriteFile(target, want[target], 0o644); err != nil {
			errs = append(errs, err)
			continue
		}
		replaced = append(replaced, target)
	}
	reload := slices.ContainsFunc(replaced, isUnit)
	if !reload {
		// An earlier start wrote a unit, but its daemon-reload failed.
		var units []string
		for _, target := range targets {
			if strings.HasSuffix(target, ".service") || strings.HasSuffix(target, ".timer") {
				units = append(units, path.Base(target))
			}
		}
		if len(units) > 0 {
			out, err := host.Run(ctx, "systemctl", append([]string{"show", "--property", "NeedDaemonReload", "--value"}, units...)...)
			reload = err == nil && slices.Contains(strings.Fields(out), "yes")
		}
	}
	errs = append(errs, activate(ctx, host, replaced, reload))
	return replaced, errors.Join(errs...)
}

// isUnit reports whether target is read by systemd, not the kernel.
func isUnit(target string) bool { return target != SysctlFile && target != HardeningFile }

// activate applies the sysctl files among written, then, if reload,
// has systemd read its units again.
func activate(ctx context.Context, host Host, written []string, reload bool) error {
	var errs []error
	for _, file := range []string{SysctlFile, HardeningFile} {
		if !slices.Contains(written, file) {
			continue
		}
		args := []string{"-q", "-p", file}
		if file == HardeningFile {
			// A kernel may lack a key the hardening names (IPv6 turned
			// off, no BPF): -e skips those, and doctor names what stays
			// unhardened.
			args = []string{"-q", "-e", "-p", file}
		}
		if _, err := host.Run(ctx, "sysctl", args...); err != nil {
			errs = append(errs, fmt.Errorf("apply %s: %w", file, err))
		}
	}
	if reload {
		if _, err := host.Run(ctx, "systemctl", "daemon-reload"); err != nil {
			errs = append(errs, err)
		}
	}
	return errors.Join(errs...)
}
