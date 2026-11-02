package usage

import (
	"context"
	"errors"
	"log/slog"
	"strings"
	"time"
)

// Tenant is one of the node's tenants, as the collector measures it.
type Tenant struct {
	Name string
	// Container is its running container's ID; "" when none runs.
	Container string
	// CPULimit is the cores it may use; 0 if unlimited.
	CPULimit float64
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

	pids     map[string]int
	hostCPU  CPUTimes
	diskAt   time.Time
	warnedV1 bool
}

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
		c.pids = map[string]int{}
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
			if used, ok, err := c.Disk(ctx, t.Name); err == nil {
				if !ok {
					used = -1
				}
				c.Recorder.Disk(t.Name, used, now)
			} else {
				c.Log.Debug("measure a tenant's data", "tenant", t.Name, "error", err)
			}
		}
		if t.Container == "" {
			continue
		}
		running[t.Container] = true
		raw, err := c.read(ctx, t.Container, now)
		if errors.Is(err, ErrNoCgroup2) {
			if !c.warnedV1 {
				c.warnedV1 = true
				c.Log.Warn("containers are not in a cgroup v2 hierarchy: their CPU, memory and disk use are not measured")
			}
			continue
		}
		if err != nil {
			c.Log.Debug("measure a tenant's container", "tenant", t.Name, "error", err)
			continue
		}
		raw.Container = t.Container
		c.Recorder.Sample(t.Name, raw, t.CPULimit)
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
// cgroup path names its ID, as Docker's and Podman's do.
func (c *Collector) read(ctx context.Context, container string, now time.Time) (Raw, error) {
	if pid := c.pids[container]; pid > 0 {
		if path, err := c.Reader.CgroupOf(pid); err == nil && strings.Contains(path, container) {
			return c.Reader.Container(pid, path, now)
		}
		delete(c.pids, container)
	}
	pid, err := c.Pid(ctx, container)
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
	return c.Reader.Container(pid, path, now)
}
