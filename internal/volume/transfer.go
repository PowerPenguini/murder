package volume

import (
	"archive/tar"
	"context"
	"crypto/sha256"
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
	archive := tar.NewWriter(w)
	err := walkTree(ctx, path, func(header *tar.Header, content io.Reader) error {
		if err := archive.WriteHeader(header); err != nil {
			return err
		}
		if content == nil {
			return nil
		}
		_, err := io.Copy(archive, content)
		return err
	})
	if err != nil {
		return err
	}
	return archive.Close()
}

// walkTree calls visit with the header of every entry below path, as
// writeTar writes it, and the content of a regular file: header.Size
// bytes, a file that shrank since padded with zeros.
func walkTree(ctx context.Context, path string, visit func(header *tar.Header, content io.Reader) error) error {
	root, err := os.OpenRoot(path)
	if err != nil {
		return err
	}
	defer root.Close()
	return fs.WalkDir(root.FS(), ".", func(name string, _ fs.DirEntry, err error) error {
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
		if file == nil {
			return visit(header, nil)
		}
		// It shrank since: the rest is zeros, and the stopped copy has it
		// as it is.
		return visit(header, io.LimitReader(io.MultiReader(file, zeros{}), header.Size))
	})
}

// zeros reads as zero bytes.
type zeros struct{}

func (zeros) Read(p []byte) (int, error) {
	clear(p)
	return len(p), nil
}

// errLeftOut is an entry readTar leaves out: devices, hard links and the
// like have no place in a tenant's data, and writeTar writes none.
var errLeftOut = errors.New("an entry of a type that is not unpacked")

// tarTree follows the tree unpacking a tar stream makes, entry by entry,
// so that a stream is held to the same rules whether it is unpacked or
// only read through: within the tenant's data, each path once, and each
// file, directory or symlink in a directory an entry made.
type tarTree struct {
	// kinds is what each path is: its entry's type, or a directory made on
	// the way to one; entries the paths an entry gave.
	kinds   map[string]byte
	entries map[string]bool
}

// add checks the next entry of a stream and returns where it goes, below
// the data's root.
func (t *tarTree) add(header *tar.Header) (string, error) {
	if t.kinds == nil {
		t.kinds, t.entries = map[string]byte{".": tar.TypeDir}, map[string]bool{}
	}
	name := filepath.FromSlash(header.Name)
	if !filepath.IsLocal(name) && name != "." {
		return "", fmt.Errorf("entry %q leaves the tenant's data", header.Name)
	}
	name = filepath.Clean(name)
	switch header.Typeflag {
	case tar.TypeDir, tar.TypeReg, tar.TypeSymlink:
	default:
		return name, fmt.Errorf("entry %q: %w", header.Name, errLeftOut)
	}
	if t.entries[name] {
		return "", fmt.Errorf("entry %q comes twice", header.Name)
	}
	// Never below a file or a symlink: one would lead elsewhere.
	parent := filepath.Dir(name)
	for above := parent; above != "."; above = filepath.Dir(above) {
		if kind, ok := t.kinds[above]; ok && kind != tar.TypeDir {
			return "", fmt.Errorf("entry %q is below %s, which is no directory", header.Name, above)
		}
	}
	if header.Typeflag != tar.TypeDir {
		if t.kinds[parent] != tar.TypeDir {
			return "", fmt.Errorf("entry %q is in a directory no entry made", header.Name)
		}
		if _, ok := t.kinds[name]; ok {
			return "", fmt.Errorf("entry %q is a directory already", header.Name)
		}
	}
	t.entries[name] = true
	t.kinds[name] = header.Typeflag
	if header.Typeflag == tar.TypeDir {
		for above := parent; above != "."; above = filepath.Dir(above) {
			if _, ok := t.kinds[above]; !ok {
				t.kinds[above] = tar.TypeDir
			}
		}
	}
	return name, nil
}

// readTar unpacks a stream writeTar wrote below path, never leaving it.
// Ownership is kept where the process may set it (as root on a node);
// setuid and setgid bits are not.
func readTar(ctx context.Context, path string, r io.Reader) error {
	root, err := os.OpenRoot(path)
	if err != nil {
		return err
	}
	defer root.Close()
	archive := tar.NewReader(r)
	asRoot := os.Geteuid() == 0
	var tree tarTree
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
		name, err := tree.add(header)
		if errors.Is(err, errLeftOut) {
			continue
		}
		if err != nil {
			return err
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
		}
		if asRoot {
			if err := root.Lchown(name, header.Uid, header.Gid); err != nil {
				return err
			}
		}
		if header.Typeflag != tar.TypeSymlink {
			if err := root.Chmod(name, restoredMode(header)); err != nil {
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

// restoredMode is the mode readTar gives an entry: its permissions and
// sticky bit, never setuid or setgid.
func restoredMode(header *tar.Header) fs.FileMode {
	mode := fs.FileMode(header.Mode).Perm()
	if header.Mode&0o1000 != 0 {
		mode |= fs.ModeSticky
	}
	return mode
}

// TarEntry is an entry of a tar stream as ReadTar would unpack it: what it
// is, its owner, mode and time, a symlink's target, and a file's size and
// SHA-256.
type TarEntry struct {
	Type     byte
	Uid, Gid int
	Mode     int64
	ModTime  time.Time
	Link     string
	Size     int64
	Sum      [sha256.Size]byte
}

func tarEntry(header *tar.Header, content io.Reader) (TarEntry, error) {
	entry := TarEntry{Type: header.Typeflag, Uid: header.Uid, Gid: header.Gid, Mode: header.Mode, ModTime: header.ModTime, Link: header.Linkname, Size: header.Size}
	if header.Typeflag == tar.TypeReg {
		hash := sha256.New()
		n, err := io.Copy(hash, content)
		if err != nil {
			return entry, err
		}
		entry.Size = n
		hash.Sum(entry.Sum[:0])
	}
	return entry, nil
}

// ScanTar reads a tar stream through by the rules ReadTar unpacks it by,
// unpacking nothing, and returns its entries by path below the root. An
// entry ReadTar would refuse fails it, and so does one it would leave out:
// WriteTar writes none.
func ScanTar(ctx context.Context, r io.Reader) (map[string]TarEntry, error) {
	archive := tar.NewReader(r)
	var tree tarTree
	entries := map[string]TarEntry{}
	for {
		header, err := archive.Next()
		if errors.Is(err, io.EOF) {
			return entries, nil
		}
		if err != nil {
			return entries, err
		}
		if err := ctx.Err(); err != nil {
			return entries, err
		}
		name, err := tree.add(header)
		if err != nil {
			return entries, err
		}
		entry, err := tarEntry(header, archive)
		if err != nil {
			return entries, fmt.Errorf("%s: %w", header.Name, err)
		}
		entries[name] = entry
	}
}

// CompareTar reports how entries, as ScanTar read them, differ from the
// tree below path as WriteTar would write it, in what ReadTar puts back:
// the entries, their types, owners, modes and times, symlinks' targets and
// files' content.
func CompareTar(ctx context.Context, path string, entries map[string]TarEntry) error {
	found := 0
	err := walkTree(ctx, path, func(header *tar.Header, content io.Reader) error {
		name := filepath.Clean(filepath.FromSlash(header.Name))
		got, ok := entries[name]
		if !ok {
			return fmt.Errorf("%s is not in the backup", name)
		}
		found++
		want, err := tarEntry(header, content)
		if err != nil {
			return fmt.Errorf("%s: %w", name, err)
		}
		switch {
		case got.Type != want.Type:
			return fmt.Errorf("%s is of another type in the backup", name)
		case got.Uid != want.Uid || got.Gid != want.Gid:
			return fmt.Errorf("%s has another owner in the backup", name)
		case got.Link != want.Link:
			return fmt.Errorf("%s links elsewhere in the backup", name)
		case want.Type == tar.TypeSymlink:
			return nil // a symlink's mode and time are not put back
		case got.Mode != want.Mode:
			return fmt.Errorf("%s has another mode in the backup", name)
		// A tar header keeps whole seconds.
		case !got.ModTime.Equal(want.ModTime) && !got.ModTime.Equal(want.ModTime.Round(time.Second)):
			return fmt.Errorf("%s has another time in the backup", name)
		case got.Size != want.Size || got.Sum != want.Sum:
			return fmt.Errorf("%s differs from its snapshot in the backup", name)
		}
		return nil
	})
	if err != nil {
		return err
	}
	if found != len(entries) {
		return fmt.Errorf("the backup holds %d entries, its snapshot %d", len(entries), found)
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
	// Not beside a copy's receive or promotion.
	defer lockDataset(dataset)()
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
