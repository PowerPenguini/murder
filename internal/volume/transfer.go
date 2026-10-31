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
// snapshots: every send is whole, and since is ignored. A tenant without
// data sends an empty tree.
func (d Dir) Send(ctx context.Context, tenant, _ string, w io.Writer) (string, error) {
	if err := checkTenant(tenant); err != nil {
		return "", err
	}
	if _, err := os.Stat(d.path(tenant)); errors.Is(err, os.ErrNotExist) {
		return "", tar.NewWriter(w).Close()
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

// writeTar writes the tree below root, never leaving it: the tenant runs
// meanwhile and may swap a directory for a symlink, which is then sent as
// a symlink, not followed. What it changes meanwhile is sent as far as it
// is there: a file removed is left out, one that shrank is padded, and
// sockets and pipes, which hold no data, are left out; the copy made while
// the tenant is stopped is the whole one.
func writeTar(ctx context.Context, path string, w io.Writer) error {
	root, err := os.OpenRoot(path)
	if err != nil {
		return err
	}
	defer root.Close()
	archive := tar.NewWriter(w)
	err = fs.WalkDir(root.FS(), ".", func(name string, _ fs.DirEntry, err error) error {
		if errors.Is(err, fs.ErrNotExist) {
			return nil
		}
		if err != nil {
			return err
		}
		if err := ctx.Err(); err != nil {
			return err
		}
		info, err := root.Lstat(name)
		if errors.Is(err, fs.ErrNotExist) {
			return nil
		}
		if err != nil {
			return err
		}
		if info.Mode()&(fs.ModeSocket|fs.ModeNamedPipe|fs.ModeDevice|fs.ModeCharDevice|fs.ModeIrregular) != 0 {
			return nil
		}
		var file *os.File
		if info.Mode().IsRegular() {
			// Opened within root: a path swapped for a symlink meanwhile
			// does not lead out of it.
			file, err = root.OpenFile(name, os.O_RDONLY|syscall.O_NOFOLLOW, 0)
			if errors.Is(err, fs.ErrNotExist) {
				return nil
			}
			if err != nil {
				return err
			}
			defer file.Close()
			if info, err = file.Stat(); err != nil {
				return err
			}
		}
		link := ""
		if info.Mode()&fs.ModeSymlink != 0 {
			if link, err = root.Readlink(name); errors.Is(err, fs.ErrNotExist) {
				return nil
			} else if err != nil {
				return err
			}
		}
		header, err := tar.FileInfoHeader(info, link)
		if err != nil {
			return fmt.Errorf("%s: %w", name, err)
		}
		header.Name = name
		if stat, ok := info.Sys().(*syscall.Stat_t); ok {
			header.Uid, header.Gid = int(stat.Uid), int(stat.Gid)
		}
		header.Uname, header.Gname = "", ""
		if err := archive.WriteHeader(header); err != nil {
			return err
		}
		if file == nil {
			return nil
		}
		copied, err := io.Copy(archive, io.LimitReader(file, header.Size))
		if err != nil {
			return err
		}
		// It shrank since: the rest is zeros, and the stopped copy has it
		// as it is.
		_, err = io.CopyN(archive, zeros{}, header.Size-copied)
		return err
	})
	if err != nil {
		return err
	}
	return archive.Close()
}

// zeros reads as zero bytes.
type zeros struct{}

func (zeros) Read(p []byte) (int, error) {
	clear(p)
	return len(p), nil
}

// readTar unpacks a stream writeTar wrote below path, never leaving it,
// also through a symlink an earlier entry made. Ownership is kept where the
// process may set it (as root on a node); setuid and setgid bits are not.
func readTar(ctx context.Context, path string, r io.Reader) error {
	root, err := os.OpenRoot(path)
	if err != nil {
		return err
	}
	defer root.Close()
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
		mode := fs.FileMode(header.Mode).Perm()
		if header.Mode&0o1000 != 0 {
			mode |= fs.ModeSticky
		}
		switch header.Typeflag {
		case tar.TypeDir:
			if err := root.MkdirAll(name, 0o700); err != nil {
				return fmt.Errorf("%s: %w", header.Name, err)
			}
			directories = append(directories, header)
		case tar.TypeReg:
			file, err := root.OpenFile(name, os.O_CREATE|os.O_EXCL|os.O_WRONLY, 0o600)
			if err != nil {
				return fmt.Errorf("%s: %w", header.Name, err)
			}
			_, err = io.Copy(file, archive)
			if err = errors.Join(err, file.Close()); err != nil {
				return err
			}
		case tar.TypeSymlink:
			if err := root.Symlink(header.Linkname, name); err != nil {
				return fmt.Errorf("%s: %w", header.Name, err)
			}
		default:
			continue // devices, hard links and the like have no place here
		}
		if asRoot {
			if err := root.Lchown(name, header.Uid, header.Gid); err != nil {
				return err
			}
		}
		if header.Typeflag != tar.TypeSymlink {
			if err := root.Chmod(name, mode); err != nil {
				return err
			}
			if err := root.Chtimes(name, header.ModTime, header.ModTime); err != nil {
				return err
			}
		}
	}
	// Directory times last: their files changed them.
	for _, header := range directories {
		_ = root.Chtimes(filepath.FromSlash(header.Name), header.ModTime, header.ModTime)
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
