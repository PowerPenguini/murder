package panel

import (
	"context"
	"hash/fnv"
	"math"
	"time"

	"github.com/PowerPenguini/murder/internal/api"
	"github.com/PowerPenguini/murder/internal/usage"
)

// The demo's use is made up: every tenant has a day of its own, busier by
// day, the same every time. baltic-freight runs out of memory.

// demoLoad is a tenant's load at t, from 0 to 1: a working day's curve,
// shifted by tenant, with a little noise.
func demoLoad(tenant string, t time.Time) float64 {
	h := fnv.New32a()
	_, _ = h.Write([]byte(tenant))
	seed := float64(h.Sum32()%1000) / 1000
	hour := float64(t.UTC().Hour()) + float64(t.UTC().Minute())/60
	day := 0.5 + 0.5*math.Sin((hour-9-seed*3)/24*2*math.Pi)
	hours := float64(t.Unix()) / 3600
	noise := 0.5 + 0.3*math.Sin(hours*2.1+seed*40) + 0.2*math.Sin(hours*5.3+seed*7)*math.Sin(hours*0.7)
	return math.Max(0, math.Min(1, 0.15+0.6*day*day+0.25*noise))
}

// demoPoint is a tenant's made-up use over the step starting at t.
// memMB, when above 0, is its memory in MB, from the incident timeline.
func demoPoint(tenant string, memoryLimitMB int, cpuLimit float64, t time.Time, step time.Duration, memMB float64) usage.Point {
	load := demoLoad(tenant, t)
	minutes := step.Minutes()
	memoryShare := 0.28 + 0.3*load
	errorShare := 0.002
	if memMB > 0 && memoryLimitMB > 0 {
		memoryShare = memMB / float64(memoryLimitMB)
		if memoryShare > 0.9 {
			errorShare = 0.04
		}
	}
	rpm := 4 + 180*load
	p := usage.Point{
		At:       t,
		CPU:      math.Max(0.004, cpuLimit*(0.03+0.45*load*load)),
		Memory:   int64(float64(memoryLimitMB) * memoryShare * (1 << 20)),
		Requests: int64(rpm * minutes),
		Errors:   int64(math.Round(rpm * minutes * errorShare * (0.5 + load))),
		P95:      0.04 + 0.22*load*load,
	}
	p.Minutes, p.Read, p.CPURead = int(minutes), int(minutes), int(minutes)
	p.NetRx = p.Requests * 2200
	p.NetTx = p.Requests * 18000
	p.DiskRead = int64(load * 40000 * step.Seconds())
	p.DiskWrite = int64((0.2 + load) * 90000 * step.Seconds())
	return p
}

func (d *DemoCluster) Usage(ctx context.Context) (api.UsageReport, error) {
	nodes, _ := d.Nodes(ctx)
	tenants, _ := d.List(ctx)
	now := time.Now()
	report := api.UsageReport{}
	for i, n := range nodes.Nodes {
		if !n.Answers {
			report.Missing = append(report.Missing, n.Name)
			continue
		}
		cpus := []int{4, 8, 4}[i%3]
		total := int64([]int{8, 16, 8}[i%3]) << 30
		r := usage.Report{Node: n.Name, At: now, Tenants: map[string]usage.Now{}}
		var memory int64
		var cpu float64
		for _, t := range tenants {
			if t.Node != n.Name || t.Archived() || t.Spec.Suspended {
				continue
			}
			limits := t.Spec.Limits
			at := now.Truncate(time.Minute).Add(-time.Minute)
			p := demoPoint(t.Spec.Name, limits.MemoryMB, limits.CPUs, at, time.Minute, d.demoMemory(t.Spec.Name, limits.MemoryMB, at.Add(time.Minute)))
			r.Tenants[t.Spec.Name] = usage.Now{At: p.At, CPU: p.CPU, Memory: p.Memory, MemoryLimit: int64(limits.MemoryMB) << 20, CPULimit: limits.CPUs,
				NetRx: p.NetRx, NetTx: p.NetTx, DiskUsed: int64(limits.DiskGB) << 30 / 7, Requests: p.Requests, Errors: p.Errors, P95: p.P95}
			memory += p.Memory
			cpu += p.CPU
		}
		// Backed up hourly; kowalski-trans's backups fail, its newest
		// three hours old.
		r.Backups = &usage.Backups{Ready: true, Every: 3600, Tenants: map[string]usage.Backup{}}
		for _, t := range tenants {
			if t.Node != n.Name {
				continue
			}
			h := fnv.New32a()
			_, _ = h.Write([]byte(t.Spec.Name))
			backup := usage.Backup{Last: now.Add(-time.Duration(4+h.Sum32()%50) * time.Minute)}
			if t.Spec.Name == "kowalski-trans" {
				backup = usage.Backup{Last: now.Add(-3*time.Hour - 12*time.Minute), Failing: true, Error: "put backups/kowalski-trans/20261104T1300Z.db.zst: 403 Forbidden: the bucket refused the upload (access key without s3:PutObject?)"}
			}
			if t.Archived() {
				backup.Last = t.ArchivedAt
			}
			r.Backups.Tenants[t.Spec.Name] = backup
		}
		if n.Name == nodes.Leader {
			r.Backups.State = &usage.Backup{Last: now.Add(-21 * time.Minute)}
		}
		r.Host = usage.Host{CPUs: cpus, CPU: cpu + 0.3, MemoryTotal: total, MemoryAvailable: total - memory - 900<<20, Load1: cpu + 0.2}
		report.Nodes = append(report.Nodes, r)
	}
	return report, nil
}

func (d *DemoCluster) TenantUsage(ctx context.Context, name string, step time.Duration) (usage.Series, error) {
	t, err := d.Get(ctx, name)
	if err != nil {
		return usage.Series{}, err
	}
	if t.Archived() {
		return usage.Series{}, &api.Error{Status: 404, Message: "tenant " + name + " has not been measured yet"}
	}
	now := time.Now()
	end := now.Truncate(step)
	limits := t.Spec.Limits
	series := usage.Series{Tenant: name, Node: t.Node, Step: int(step / time.Second)}
	for at := end.Add(-24 * time.Hour).Add(step); at.Before(end); at = at.Add(step) {
		limit := d.limitAt(name, limits.MemoryMB, at.Add(step))
		p := demoPoint(name, limit, limits.CPUs, at, step, d.demoMemory(name, limit, at.Add(step)))
		// The step's least, read each minute: a kill within it shows.
		if name == "baltic-freight" {
			low := float64(p.Memory)
			for m := at; m.Before(at.Add(step)); m = m.Add(time.Minute) {
				if v := d.demoMemory(name, d.limitAt(name, limits.MemoryMB, m), m) * (1 << 20); v > 0 {
					low = min(low, v)
				}
			}
			p.Memory = max(p.Memory, int64(d.peakIn(name, d.limitAt(name, limits.MemoryMB, at), at, step)*(1<<20)))
			p.MemoryLow = int64(low)
		}
		p.MemoryLimit = int64(limit) << 20
		// Its memory as a share of the limit it ran under then, against
		// the limit drawn: the one in effect now.
		series.Points = append(series.Points, p)
	}
	// The newest step reads as the cluster's use report does for the
	// last minute, so the tenants list, the overview and this page give
	// the same numbers.
	minute := now.Truncate(time.Minute).Add(-time.Minute)
	m := demoPoint(name, limits.MemoryMB, limits.CPUs, minute, time.Minute, d.demoMemory(name, limits.MemoryMB, minute.Add(time.Minute)))
	k := int64(step / time.Minute)
	newest := &series.Points[len(series.Points)-1]
	newest.CPU, newest.Memory, newest.P95 = m.CPU, m.Memory, m.P95
	newest.Requests, newest.Errors, newest.NetRx, newest.NetTx = m.Requests*k, m.Errors*k, m.NetRx*k, m.NetTx*k
	last := *newest
	series.Now = usage.Now{At: last.At, CPU: last.CPU, Memory: last.Memory, MemoryLimit: int64(limits.MemoryMB) << 20, CPULimit: limits.CPUs,
		DiskUsed: int64(limits.DiskGB) << 30 / 7, DiskLimit: int64(limits.DiskGB) << 30, Requests: last.Requests / int64(step.Minutes()), Errors: last.Errors / int64(step.Minutes()), P95: last.P95}
	return series, nil
}

// The incident the demo tells: baltic-freight's export needs more memory
// than web's 256 MB. After each kill its new container starts near
// 120 MB and climbs to the limit within minutes, where the kernel kills it
// again; given 512 MB, it settles near 260 MB, well under it.
const (
	demoNeedFrom = 120.0
	demoNeedTo   = 262.0
	demoClimb    = 3*time.Minute + 30*time.Second
)

// demoMemory is a tenant's memory in MB at t on a limit of limitMB, from
// the incident's timeline: 0 for a tenant without one.
func (d *DemoCluster) demoMemory(name string, limitMB int, t time.Time) float64 {
	if name != "baltic-freight" || limitMB <= 0 {
		return 0
	}
	since := 24 * time.Hour
	for _, kill := range d.killsOf(name) {
		if !kill.After(t) && t.Sub(kill) < since {
			since = t.Sub(kill)
		}
	}
	need := demoNeedTo
	if since < demoClimb {
		need = demoNeedFrom + (demoNeedTo-demoNeedFrom)*since.Seconds()/demoClimb.Seconds()
	}
	// A little of the day's load on top, at most to just under the limit.
	need += 6 * demoLoad(name, t)
	return math.Min(need, 0.975*float64(limitMB))
}

// killsOf are the times the tenant ran out of memory, from the audit log.
func (d *DemoCluster) killsOf(name string) []time.Time {
	d.mu.Lock()
	defer d.mu.Unlock()
	var out []time.Time
	for _, e := range d.events {
		if e.Tenant == name && outOfMemory(e) {
			out = append(out, e.At)
		}
	}
	return out
}

// limitAt is the memory limit the tenant ran under at t: now's, unless a
// change since raised it.
func (d *DemoCluster) limitAt(name string, now int, t time.Time) int {
	d.mu.Lock()
	defer d.mu.Unlock()
	limit := now
	for _, c := range d.limitChanges[name] {
		if t.Before(c.at) {
			return c.from
		}
	}
	return limit
}

// limitChange is a tenant's memory limit changing, from what.
type limitChange struct {
	at   time.Time
	from int
}

// peakIn is a tenant's most memory within the step from at, read each
// minute: just under its limit in a step it was killed in.
func (d *DemoCluster) peakIn(name string, limitMB int, at time.Time, step time.Duration) float64 {
	peak := 0.0
	for m := at; m.Before(at.Add(step)); m = m.Add(time.Minute) {
		peak = max(peak, d.demoMemory(name, limitMB, m))
		for _, kill := range d.killsOf(name) {
			if !kill.Before(m) && kill.Before(m.Add(time.Minute)) {
				peak = max(peak, 0.995*float64(limitMB))
			}
		}
	}
	return peak
}
