package pluginhost

import (
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"syscall"
)

// mounted reports whether dir is a mount point: on another device than
// its parent. Its type says nothing: /run, where the sockets are, is a
// tmpfs already.
func mounted(dir string) (bool, error) {
	var own, parent syscall.Stat_t
	if err := syscall.Stat(dir, &own); err != nil {
		return false, err
	}
	if err := syscall.Stat(filepath.Dir(dir), &parent); err != nil {
		return false, err
	}
	return own.Dev != parent.Dev, nil
}

// mountTmpfs mounts a tmpfs on dir, unless one is mounted there: room for
// the socket and little more, owned by owner, which others may only pass
// through.
func mountTmpfs(dir string, owner int) error {
	if is, err := mounted(dir); err != nil || is {
		return err
	}
	options := fmt.Sprintf("size=64k,nr_inodes=16,mode=0711,uid=%d,gid=%d", owner, owner)
	return syscall.Mount("tmpfs", dir, "tmpfs", syscall.MS_NOSUID|syscall.MS_NODEV|syscall.MS_NOEXEC, options)
}

// unmountTmpfs unmounts what is mounted on dir, if anything is.
func unmountTmpfs(dir string) error {
	is, err := mounted(dir)
	if errors.Is(err, os.ErrNotExist) {
		return nil
	}
	if err != nil || !is {
		return err
	}
	return syscall.Unmount(dir, syscall.MNT_DETACH)
}
