package pluginhost

import (
	"errors"
	"fmt"
	"syscall"
)

// tmpfsMagic is the type statfs says of a tmpfs.
const tmpfsMagic = 0x01021994

// mountTmpfs mounts a tmpfs on dir, unless one is: room for the socket
// and little more, owned by owner, which others may only pass through.
func mountTmpfs(dir string, owner int) error {
	var stat syscall.Statfs_t
	if err := syscall.Statfs(dir, &stat); err != nil {
		return err
	}
	if stat.Type == tmpfsMagic {
		return nil
	}
	options := fmt.Sprintf("size=64k,nr_inodes=16,mode=0711,uid=%d,gid=%d", owner, owner)
	return syscall.Mount("tmpfs", dir, "tmpfs", syscall.MS_NOSUID|syscall.MS_NODEV|syscall.MS_NOEXEC, options)
}

// unmountTmpfs unmounts the tmpfs on dir, if one is.
func unmountTmpfs(dir string) error {
	var stat syscall.Statfs_t
	if err := syscall.Statfs(dir, &stat); err != nil {
		if errors.Is(err, syscall.ENOENT) {
			return nil
		}
		return err
	}
	if stat.Type != tmpfsMagic {
		return nil
	}
	return syscall.Unmount(dir, syscall.MNT_DETACH)
}
