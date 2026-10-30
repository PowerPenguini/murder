package volume

import (
	"archive/tar"
	"context"
	"errors"
	"fmt"
	"io"
	"io/fs"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"syscall"
	"time"
)

// Transfers move a tenant's data to another node: Send writes it as it is
// now, all of it or what changed since the snapshot an earlier Send named;
// the other node's Receive takes it. A move sends everything while the
// tenant runs, then, stopped, what changed since.

// Send writes the dir tenant's data as a tar stream. A directory has no
// snapshots: every send is whole, and since is ignored.
func (d Dir) Send(ctx context.Context, tenant, _ string, w io.Writer) (string, error) {
	if err := checkTenant(tenant); err != nil {
		return "", err
	}
	return "", writeTar(ctx, d.path(tenant), w)
}

// Receive puts the data a Send wrote in place of the tenant's: it is
// unpacked beside, then takes its place whole.
func (d Dir) Receive(ctx context.Context, tenant string, _ bool, r io.Reader) error {
	if err := checkTenant(tenant); err != nil {
		return err
	}
	if err := privateDir(d.Root); err != nil {
		return err
	}
	staging, err := os.MkdirTemp(d.Root, "."+tenant+".receiving-")
	if err != nil {
		return err
	}
	defer os.RemoveAll(staging)
	if err := readTar(ctx, staging, r); err != nil {
		return err
	}
	path := d.path(tenant)
	old := path + ".replaced"
	_ = os.RemoveAll(old)
	if err := os.Rename(path, old); err != nil && !errors.Is(err, os.ErrNotExist) {
		return err
	}
	if err := os.Rename(staging, path); err != nil {
		return err
	}
	return os.RemoveAll(old)
}

func writeTar(ctx context.Context, root string, w io.Writer) error {
	archive := tar.NewWriter(w)
	err := filepath.WalkDir(root, func(path string, entry fs.DirEntry, err error) error {
		if err != nil {
			return err
		}
		if err := ctx.Err(); err != nil {
			return err
		}
		info, err := entry.Info()
		if err != nil {
			return err
		}
		link := ""
		if info.Mode()&fs.ModeSymlink != 0 {
			if link, err = os.Readlink(path); err != nil {
				return err
			}
		}
		header, err := tar.FileInfoHeader(info, link)
		if err != nil {
			return fmt.Errorf("%s: %w", path, err)
		}
		relative, _ := filepath.Rel(root, path)
		header.Name = filepath.ToSlash(relative)
		if stat, ok := info.Sys().(*syscall.Stat_t); ok {
			header.Uid, header.Gid = int(stat.Uid), int(stat.Gid)
		}
		header.Uname, header.Gname = "", ""
		if err := archive.WriteHeader(header); err != nil {
			return err
		}
		if !info.Mode().IsRegular() {
			return nil
		}
		file, err := os.Open(path)
		if err != nil {
			return err
		}
		defer file.Close()
		_, err = io.Copy(archive, file)
		return err
	})
	if err != nil {
		return err
	}
	return archive.Close()
}

// readTar unpacks a stream writeTar wrote below root, refusing any entry
// that would land outside it. Ownership is kept where the process may set
// it (as root on a node).
func readTar(ctx context.Context, root string, r io.Reader) error {
	archive := tar.NewReader(r)
	asRoot := os.Geteuid() == 0
	var directories []*tar.Header
	for {
		header, err := archive.Next()
		if errors.Is(err, io.EOF) {
			break
		}
		if err != nil {
			return err
		}
		if err := ctx.Err(); err != nil {
			return err
		}
		name := filepath.FromSlash(header.Name)
		if !filepath.IsLocal(name) && name != "." {
			return fmt.Errorf("entry %q leaves the tenant's data", header.Name)
		}
		path := filepath.Join(root, name)
		mode := fs.FileMode(header.Mode).Perm() | fs.FileMode(header.Mode)&(fs.ModeSetgid|fs.ModeSticky)
		switch header.Typeflag {
		case tar.TypeDir:
			if err := os.MkdirAll(path, 0o700); err != nil {
				return err
			}
			directories = append(directories, header)
		case tar.TypeReg:
			file, err := os.OpenFile(path, os.O_CREATE|os.O_EXCL|os.O_WRONLY, 0o600)
			if err != nil {
				return err
			}
			_, err = io.Copy(file, archive)
			if err = errors.Join(err, file.Close()); err != nil {
				return err
			}
		case tar.TypeSymlink:
			if err := os.Symlink(header.Linkname, path); err != nil {
				return err
			}
		default:
			continue // devices and the like have no place in tenant data
		}
		if asRoot {
			if err := os.Lchown(path, header.Uid, header.Gid); err != nil {
				return err
			}
		}
		if header.Typeflag != tar.TypeSymlink {
			if err := os.Chmod(path, mode); err != nil {
				return err
			}
			if err := os.Chtimes(path, header.ModTime, header.ModTime); err != nil {
				return err
			}
		}
	}
	// Directory times last: their files changed them.
	for _, header := range directories {
		path := filepath.Join(root, filepath.FromSlash(header.Name))
		_ = os.Chtimes(path, header.ModTime, header.ModTime)
	}
	return nil
}

// Pipe runs a command with its standard input and output connected.
type Pipe func(ctx context.Context, stdin io.Reader, stdout io.Writer, name string, args ...string) error

// ExecPipe runs commands on the host; a transfer may take hours.
func ExecPipe(ctx context.Context, stdin io.Reader, stdout io.Writer, name string, args ...string) error {
	command := exec.CommandContext(ctx, name, args...)
	command.Stdin, command.Stdout = stdin, stdout
	var stderr strings.Builder
	command.Stderr = &stderr
	command.WaitDelay = 10 * time.Second
	if err := command.Run(); err != nil {
		return fmt.Errorf("%s %s: %w: %s", name, strings.Join(args, " "), err, strings.TrimSpace(stderr.String()))
	}
	return nil
}

// Send writes a new snapshot of the tenant's dataset as a ZFS stream, with
// its properties: whole, or, with since naming the snapshot an earlier Send
// returned, what changed since. It returns the new snapshot.
func (z ZFS) Send(ctx context.Context, tenant, since string, w io.Writer) (string, error) {
	snapshot, err := z.Snapshot(ctx, tenant, "move")
	if err != nil {
		return "", err
	}
	args := []string{"send", "-p"}
	if since != "" {
		args = append(args, "-i", since)
	}
	if err := z.pipe()(ctx, nil, w, "zfs", append(args, snapshot)...); err != nil {
		return "", err
	}
	return snapshot, nil
}

// Receive takes a stream Send wrote. A whole one makes the dataset, which
// must not exist; an incremental one goes on top of what the whole one
// made, undoing anything since. The dataset is left unmounted, and mounted
// where this node keeps tenants once the tenant runs here.
func (z ZFS) Receive(ctx context.Context, tenant string, incremental bool, r io.Reader) error {
	if err := checkTenant(tenant); err != nil {
		return err
	}
	dataset := z.dataset(tenant)
	args := []string{"receive", "-u"}
	if incremental {
		current, exists, err := z.properties(ctx, dataset)
		if err != nil {
			return err
		}
		if !exists || current.owner != tenant {
			return fmt.Errorf("dataset %s is not a copy of tenant %s to add to", dataset, tenant)
		}
		args = append(args, "-F")
	} else if _, exists, err := z.properties(ctx, dataset); err != nil {
		return err
	} else if exists {
		return fmt.Errorf("dataset %s exists already", dataset)
	}
	if err := z.pipe()(ctx, r, io.Discard, "zfs", append(args, dataset)...); err != nil {
		return err
	}
	_, err := z.Run(ctx, "zfs", "set", "mountpoint="+filepath.Join(z.MountRoot, tenant), dataset)
	return err
}

func (z ZFS) pipe() Pipe {
	if z.Pipe != nil {
		return z.Pipe
	}
	return ExecPipe
}
