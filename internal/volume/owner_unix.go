//go:build unix

package volume

import (
	"os"
	"syscall"
)

type owner struct{ uid, gid int }

func fileOwner(info os.FileInfo) (owner, bool) {
	stat, ok := info.Sys().(*syscall.Stat_t)
	if !ok {
		return owner{}, false
	}
	return owner{uid: int(stat.Uid), gid: int(stat.Gid)}, true
}
