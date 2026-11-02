package usage

import (
	"bufio"
	"bytes"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"time"
)

// Reader reads containers and the host from the kernel: cgroup v2 for a
// container's CPU, memory and disk I/O, its network namespace's interfaces
// for its traffic, and /proc for the host.
type Reader struct {
	// Proc and Cgroup are where /proc and the cgroup v2 hierarchy are
	// mounted; "/proc" and "/sys/fs/cgroup" if empty.
	Proc, Cgroup string
}

func (r Reader) proc() string {
	if r.Proc == "" {
		return "/proc"
	}
	return r.Proc
}

func (r Reader) cgroup() string {
	if r.Cgroup == "" {
		return "/sys/fs/cgroup"
	}
	return r.Cgroup
}

// ErrNoCgroup2 says a process is not in a cgroup v2 hierarchy: on a host
// with cgroup v1 only, containers are not measured.
var ErrNoCgroup2 = errors.New("the process is in no cgroup v2 hierarchy")

// CgroupOf is the cgroup v2 path of process pid, as the host sees it.
func (r Reader) CgroupOf(pid int) (string, error) {
	membership, err := os.ReadFile(filepath.Join(r.proc(), strconv.Itoa(pid), "cgroup"))
	if err != nil {
		return "", err
	}
	for _, line := range strings.Split(string(membership), "\n") {
		if rest, ok := strings.CutPrefix(line, "0::"); ok && rest != "" {
			return rest, nil
		}
	}
	return "", ErrNoCgroup2
}

// Container reads the container whose main process is pid, in cgroup path.
func (r Reader) Container(pid int, path string, at time.Time) (Raw, error) {
	raw := Raw{At: at}
	dir := filepath.Join(r.cgroup(), path)
	var err error
	stat, err := keyed(filepath.Join(dir, "cpu.stat"))
	if err != nil {
		return raw, err
	}
	raw.CPU = stat["usage_usec"]
	if raw.Memory, err = number(filepath.Join(dir, "memory.current")); err != nil {
		return raw, err
	}
	// "max" when unlimited.
	raw.MemoryLimit, _ = number(filepath.Join(dir, "memory.max"))
	if io, err := os.ReadFile(filepath.Join(dir, "io.stat")); err == nil {
		for _, line := range strings.Split(string(io), "\n") {
			for _, field := range strings.Fields(line) {
				key, value, _ := strings.Cut(field, "=")
				n, _ := strconv.ParseInt(value, 10, 64)
				switch key {
				case "rbytes":
					raw.DiskRead += n
				case "wbytes":
					raw.DiskWrite += n
				}
			}
		}
	}
	// The process's view of /proc/net is its network namespace's.
	if dev, err := os.ReadFile(filepath.Join(r.proc(), strconv.Itoa(pid), "net", "dev")); err == nil {
		raw.NetRx, raw.NetTx = traffic(dev)
	}
	return raw, nil
}

// traffic sums the bytes received and sent of every interface but
// loopback, from /proc/net/dev.
func traffic(dev []byte) (rx, tx int64) {
	for _, line := range strings.Split(string(dev), "\n") {
		name, rest, ok := strings.Cut(line, ":")
		if !ok || strings.TrimSpace(name) == "lo" {
			continue
		}
		fields := strings.Fields(rest)
		if len(fields) < 9 {
			continue
		}
		in, _ := strconv.ParseInt(fields[0], 10, 64)
		out, _ := strconv.ParseInt(fields[8], 10, 64)
		rx, tx = rx+in, tx+out
	}
	return rx, tx
}

// CPUTimes are the host's processor times as /proc/stat counts them.
type CPUTimes struct{ Busy, Total uint64 }

// Host reads the host; before is its CPU times as last read, and the times
// now come back for the next reading.
func (r Reader) Host(before CPUTimes) (Host, CPUTimes, error) {
	var host Host
	stat, err := os.ReadFile(filepath.Join(r.proc(), "stat"))
	if err != nil {
		return host, before, err
	}
	var now CPUTimes
	scanner := bufio.NewScanner(bytes.NewReader(stat))
	for scanner.Scan() {
		fields := strings.Fields(scanner.Text())
		if len(fields) == 0 || !strings.HasPrefix(fields[0], "cpu") {
			continue
		}
		if fields[0] != "cpu" {
			host.CPUs++
			continue
		}
		for i, field := range fields[1:] {
			n, _ := strconv.ParseUint(field, 10, 64)
			now.Total += n
			// idle and iowait are not busy; guest time is already in user.
			if i != 3 && i != 4 && i < 8 {
				now.Busy += n
			}
			if i >= 8 {
				now.Total -= n
			}
		}
	}
	if before.Total > 0 && now.Total > before.Total && now.Busy >= before.Busy {
		host.CPU = float64(host.CPUs) * float64(now.Busy-before.Busy) / float64(now.Total-before.Total)
	}
	if meminfo, err := keyed(filepath.Join(r.proc(), "meminfo")); err == nil {
		host.MemoryTotal, host.MemoryAvailable = meminfo["MemTotal:"]*1024, meminfo["MemAvailable:"]*1024
	}
	if load, err := os.ReadFile(filepath.Join(r.proc(), "loadavg")); err == nil {
		if fields := strings.Fields(string(load)); len(fields) > 0 {
			host.Load1, _ = strconv.ParseFloat(fields[0], 64)
		}
	}
	return host, now, nil
}

// keyed reads a file of "key value" lines.
func keyed(path string) (map[string]int64, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return nil, err
	}
	out := map[string]int64{}
	for _, line := range strings.Split(string(data), "\n") {
		fields := strings.Fields(line)
		if len(fields) >= 2 {
			n, err := strconv.ParseInt(fields[1], 10, 64)
			if err == nil {
				out[fields[0]] = n
			}
		}
	}
	return out, nil
}

// number reads a file holding one number.
func number(path string) (int64, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return 0, err
	}
	n, err := strconv.ParseInt(strings.TrimSpace(string(data)), 10, 64)
	if err != nil {
		return 0, fmt.Errorf("%s: %w", path, err)
	}
	return n, nil
}
