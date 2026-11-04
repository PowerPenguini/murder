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
func demoPoint(tenant string, memoryLimitMB int, cpuLimit float64, t time.Time, step time.Duration) usage.Point {
	load := demoLoad(tenant, t)
	minutes := step.Minutes()
	memoryShare := 0.28 + 0.3*load
	errorShare := 0.002
	if tenant == "baltic-freight" {
		memoryShare, errorShare = 0.91+0.07*load, 0.04
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
			p := demoPoint(t.Spec.Name, limits.MemoryMB, limits.CPUs, now.Truncate(time.Minute).Add(-time.Minute), time.Minute)
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
		series.Points = append(series.Points, demoPoint(name, limits.MemoryMB, limits.CPUs, at, step))
	}
	// The newest step reads as the cluster's use report does for the
	// last minute, so the tenants list, the overview and this page give
	// the same numbers.
	m := demoPoint(name, limits.MemoryMB, limits.CPUs, now.Truncate(time.Minute).Add(-time.Minute), time.Minute)
	k := int64(step / time.Minute)
	newest := &series.Points[len(series.Points)-1]
	newest.CPU, newest.Memory, newest.P95 = m.CPU, m.Memory, m.P95
	newest.Requests, newest.Errors, newest.NetRx, newest.NetTx = m.Requests*k, m.Errors*k, m.NetRx*k, m.NetTx*k
	last := *newest
	series.Now = usage.Now{At: last.At, CPU: last.CPU, Memory: last.Memory, MemoryLimit: int64(limits.MemoryMB) << 20, CPULimit: limits.CPUs,
		DiskUsed: int64(limits.DiskGB) << 30 / 7, DiskLimit: int64(limits.DiskGB) << 30, Requests: last.Requests / int64(step.Minutes()), Errors: last.Errors / int64(step.Minutes()), P95: last.P95}
	return series, nil
}
