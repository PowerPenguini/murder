//go:build unix

package volume

import (
	"errors"
	"io"
	"os"
	"path/filepath"
	"strconv"
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

// dataOwnerEntries is how many of a data directory's entries DataOwner
// reads at most: enough to say who wrote it, cheap enough for every pass.
const dataOwnerEntries = 256

// DataOwner says who wrote the data in dir, as "uid:gid": the owner most
// of its entries have, the directory itself aside (Ensure gives that to
// the user the tenant runs as, whoever wrote what is in it). Mixed says
// some entries have another owner. An empty directory has no owner: "".
func DataOwner(dir string) (who string, mixed bool, err error) {
	f, err := os.Open(dir)
	if err != nil {
		return "", false, err
	}
	defer f.Close()
	names, err := f.Readdirnames(dataOwnerEntries)
	if err != nil && len(names) == 0 {
		if errors.Is(err, io.EOF) {
			return "", false, nil
		}
		return "", false, err
	}
	counts := map[owner]int{}
	for _, name := range names {
		info, err := os.Lstat(filepath.Join(dir, name))
		if err != nil {
			continue
		}
		if o, ok := fileOwner(info); ok {
			counts[o]++
		}
	}
	var most owner
	best := 0
	for o, n := range counts {
		if n > best || (n == best && (o.uid < most.uid || (o.uid == most.uid && o.gid < most.gid))) {
			most, best = o, n
		}
	}
	if best == 0 {
		return "", false, nil
	}
	return strconv.Itoa(most.uid) + ":" + strconv.Itoa(most.gid), len(counts) > 1, nil
}
