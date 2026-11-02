package usage

import (
	"context"
	"errors"
	"log/slog"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"time"
)

// Tenant is one of the node's tenants, as the collector measures it.
type Tenant struct {
	Name string
	// Container is its running container's ID; "" when none runs.
	Container string
	// CPULimit is the cores it may use, DiskLimit the bytes its data may
	// take; 0 if unlimited.
	CPULimit  float64
	DiskLimit int64
}

// Collector measures the node's tenants and the host into a Recorder.
type Collector struct {
	Recorder *Recorder
	Reader   Reader
	// Tenants lists the node's tenants; Pid the host PID of a container's
	// main process, 0 if none runs.
	Tenants func() []Tenant
	Pid     func(ctx context.Context, container string) (int, error)
	// Disk tells the size of a tenant's data, ok=false if the volume
	// driver cannot; nil when none can.
	Disk func(ctx context.Context, tenant string) (used int64, ok bool, err error)
	Log  *slog.Logger
	Now  func() time.Time

	pids    map[string]int
	hostCPU CPUTimes
	diskAt  time.Time
	// cgroup2 says the host has a cgroup v2 hierarchy; known after the
	// first collection.
	cgroup2, checked bool
	// failures counts each tenant's readings that failed in a row.
	failures map[string]int
}

// callTimeout bounds one call to the engine or the volume driver: one that
// hangs must not stop the node's tenants being measured.
const callTimeout = 5 * time.Second

// failuresToWarn is how many readings of a tenant in a row fail before it
// is said, not only logged at debug level.
const failuresToWarn = 4

// Every is how often the collector measures; DiskEvery how often it asks
// the volume driver, which costs more.
const (
	Every     = 15 * time.Second
	DiskEvery = 5 * time.Minute
)

// Run measures every Every until ctx ends.
func (c *Collector) Run(ctx context.Context) {
	ticker := time.NewTicker(Every)
	defer ticker.Stop()
	for {
		c.Collect(ctx)
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
		}
	}
}

func (c *Collector) now() time.Time {
	if c.Now != nil {
		return c.Now()
	}
	return time.Now()
}

// Collect measures once.
func (c *Collector) Collect(ctx context.Context) {
	now := c.now()
	if c.pids == nil {
		c.pids, c.failures = map[string]int{}, map[string]int{}
	}
	if !c.checked {
		c.checked = true
		_, err := os.Stat(filepath.Join(c.Reader.cgroup(), "cgroup.controllers"))
		if c.cgroup2 = err == nil; !c.cgroup2 {
			c.Log.Warn("the host has no cgroup v2 hierarchy: tenants' CPU, memory, network and disk use are not measured, their requests are", "cgroup", c.Reader.cgroup())
		}
	}
	if host, times, err := c.Reader.Host(c.hostCPU); err == nil {
		c.hostCPU = times
		c.Recorder.Host(host, now)
	}
	tenants := c.Tenants()
	names := make(map[string]bool, len(tenants))
	running := make(map[string]bool, len(tenants))
	measureDisk := c.Disk != nil && now.Sub(c.diskAt) >= DiskEvery
	for _, t := range tenants {
		names[t.Name] = true
		if measureDisk {
			call, cancel := context.WithTimeout(ctx, callTimeout)
			used, ok, err := c.Disk(call, t.Name)
			cancel()
			if err == nil {
				if !ok {
					used = -1
				}
				c.Recorder.Disk(t.Name, used, t.DiskLimit, now)
			} else {
				c.Log.Debug("measure a tenant's data", "tenant", t.Name, "error", err)
			}
		}
		if t.Container == "" {
			c.Recorder.Stopped(t.Name)
			continue
		}
		if !c.cgroup2 {
			continue
		}
		running[t.Container] = true
		raw, err := c.read(ctx, t.Container, now)
		if err != nil {
			c.failures[t.Name]++
			if c.failures[t.Name] == failuresToWarn {
				c.Log.Warn("a tenant's container cannot be measured", "tenant", t.Name, "error", err)
			} else {
				c.Log.Debug("measure a tenant's container", "tenant", t.Name, "error", err)
			}
			continue
		}
		delete(c.failures, t.Name)
		c.Recorder.Sample(t.Name, raw, t.CPULimit)
	}
	for name := range c.failures {
		if !names[name] {
			delete(c.failures, name)
		}
	}
	if measureDisk {
		c.diskAt = now
	}
	for container := range c.pids {
		if !running[container] {
			delete(c.pids, container)
		}
	}
	c.Recorder.Keep(func(tenant string) bool { return names[tenant] })
	c.Recorder.Tick(now)
}

// read reads container, finding its process again once the one known is
// gone, or is another's: a PID the kernel gave out again. A container's
// cgroup path names its ID, as Docker's and Podman's do. The reading is of
// the container's process: one started again, with the same ID, starts
// its counters again.
func (c *Collector) read(ctx context.Context, container string, now time.Time) (Raw, error) {
	read := func(pid int, path string) (Raw, error) {
		raw, err := c.Reader.Container(pid, path, now)
		raw.Container = container + "/" + strconv.Itoa(pid)
		return raw, err
	}
	if pid := c.pids[container]; pid > 0 {
		if path, err := c.Reader.CgroupOf(pid); err == nil && strings.Contains(path, container) {
			return read(pid, path)
		}
		delete(c.pids, container)
	}
	call, cancel := context.WithTimeout(ctx, callTimeout)
	pid, err := c.Pid(call, container)
	cancel()
	if err != nil {
		return Raw{}, err
	}
	if pid == 0 {
		return Raw{}, errors.New("its process does not run")
	}
	path, err := c.Reader.CgroupOf(pid)
	if err != nil {
		return Raw{}, err
	}
	c.pids[container] = pid
	return read(pid, path)
}
