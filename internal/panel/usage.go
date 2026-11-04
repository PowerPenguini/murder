package panel

import (
	"context"
	"fmt"
	"math"
	"strconv"
	"time"

	"github.com/PowerPenguini/murder/internal/api"
	"github.com/PowerPenguini/murder/internal/usage"
)

// Usage is what a backend that measures tenants answers: the agent's
// client does, and the demo.
type Usage interface {
	Usage(ctx context.Context) (api.UsageReport, error)
	TenantUsage(ctx context.Context, name string, step time.Duration) (usage.Series, error)
}

// errorShare is the share of requests that fail before it is wrong.
const errorShare = 0.01

// usageStep is the step of a tenant's charts: a day in 288 points.
const usageStep = 5 * time.Minute

// tenantUsage is a tenant's day, drawn.
type tenantUsage struct {
	Charts []lineChart
	// Range is the span shown: 1h, 6h or 24h; Span says it.
	Range, Span string
	// Memory is what it uses now against its limit, with its CPU; the
	// rest its requests a minute, errors among them, and the 95th
	// percentile of their response time, in its last minute.
	Memory                 tenantUse
	CPULimit               string
	Requests, Errors, Wait string
	// ErrorsBad says errors are more than errorShare of the requests.
	ErrorsBad bool
	// Data is its data's size against its disk limit, if both are known.
	Data                *meter
	DataText, DataLimit string
}

// chartSpans are the spans a tenant's charts show, and the step of each.
var chartSpans = map[string]struct {
	Span, Step time.Duration
}{"1h": {time.Hour, time.Minute}, "6h": {6 * time.Hour, usageStep}, "24h": {24 * time.Hour, usageStep}}

func (s *Server) tenantUsage(ctx context.Context, name, rangeName string) *tenantUsage {
	shape, ok := chartSpans[rangeName]
	if !ok {
		shape = chartSpans["24h"]
	}
	span, step := shape.Span, shape.Step
	measured, ok := s.backend.(Usage)
	if !ok {
		return nil
	}
	series, err := measured.TenantUsage(ctx, name, step)
	if err != nil {
		return nil
	}
	now := s.now()
	// Each chart draws the steps its measure was taken in: a rate is per
	// minute, or second, measured.
	pick := func(taken func(usage.Point) bool, values func(usage.Point) []float64) []sample {
		var out []sample
		for _, p := range series.Points {
			if taken(p) {
				out = append(out, sample{At: p.At, Values: values(p)})
			}
		}
		return out
	}
	read := func(p usage.Point) bool { return p.Read > 0 }
	cpuRead := func(p usage.Point) bool { return p.CPURead > 0 }
	counted := func(p usage.Point) bool { return p.Minutes > 0 }
	answered := func(p usage.Point) bool { return p.Requests > 0 }
	perSecond := func(n int64, p usage.Point) float64 { return float64(n) / float64(p.CPURead*60) }
	u := &tenantUsage{Charts: []lineChart{
		newLineChart("CPU, cores", []string{"cores"}, pick(cpuRead, func(p usage.Point) []float64 { return []float64{p.CPU} }), series.Now.CPULimit, now, step, span, cores, false),
		newLineChart("Memory", []string{"used"}, pick(read, func(p usage.Point) []float64 { return []float64{float64(p.Memory)} }), float64(series.Now.MemoryLimit), now, step, span, size, true),
		newLineChart("Requests, a minute", []string{"requests", "errors"}, pick(counted, func(p usage.Point) []float64 {
			minutes := float64(p.Minutes)
			return []float64{float64(p.Requests) / minutes, float64(p.Errors) / minutes}
		}), 0, now, step, span, rate, false),
		newLineChart("Response time, 95th percentile", []string{"p95"}, pick(answered, func(p usage.Point) []float64 { return []float64{p.P95} }), 0, now, step, span, wait, false),
		newLineChart("Network", []string{"in", "out"}, pick(cpuRead, func(p usage.Point) []float64 {
			return []float64{perSecond(p.NetRx, p), perSecond(p.NetTx, p)}
		}), 0, now, step, span, throughput, true),
		newLineChart("Disk", []string{"read", "written"}, pick(cpuRead, func(p usage.Point) []float64 {
			return []float64{perSecond(p.DiskRead, p), perSecond(p.DiskWrite, p)}
		}), 0, now, step, span, throughput, true),
	}}
	// Errors are drawn in the colour of what is wrong only while they are:
	// more than one request in a hundred.
	u.ErrorsBad = series.Now.Requests > 0 && float64(series.Now.Errors)/float64(series.Now.Requests) > errorShare
	if u.ErrorsBad && len(u.Charts[2].Series) > 1 {
		u.Charts[2].Series[1].Stroke = inkBad
	}
	u.Range, u.Span = rangeName, spanText(span)
	if _, ok := chartSpans[rangeName]; !ok {
		u.Range = "24h"
	}
	u.Memory = tenantUse{MemoryText: size(float64(series.Now.Memory)), CPUText: cores(series.Now.CPU)}
	if limit := series.Now.CPULimit; limit > 0 {
		u.Memory.CPU, u.CPULimit = meterOf(int(series.Now.CPU*1000), int(limit*1000)), cores(limit)
	}
	u.Requests, u.Errors, u.Wait = rate(float64(series.Now.Requests)), rate(float64(series.Now.Errors)), wait(series.Now.P95)
	// The tiles say what the charts' last step says: one sample, one value.
	if counts := pick(counted, func(p usage.Point) []float64 {
		return []float64{float64(p.Requests) / float64(p.Minutes), float64(p.Errors) / float64(p.Minutes)}
	}); len(counts) > 0 {
		last := counts[len(counts)-1].Values
		u.Requests, u.Errors = rate(last[0]), rate(last[1])
		u.ErrorsBad = last[0] > 0 && last[1]/last[0] > errorShare
		if len(u.Charts[2].Series) > 1 {
			u.Charts[2].Series[1].Stroke = map[bool]string{true: inkBad, false: ink1}[u.ErrorsBad]
		}
	}
	if answers := pick(answered, func(p usage.Point) []float64 { return []float64{p.P95} }); len(answers) > 0 {
		u.Wait = wait(answers[len(answers)-1].Values[0])
	}
	if limit := series.Now.MemoryLimit; limit > 0 {
		u.Memory.Memory = meterOf(int(series.Now.Memory>>20), int(limit>>20))
		u.Memory.MemoryLimit = size(float64(limit))
	}
	if used := series.Now.DiskUsed; used >= 0 {
		u.DataText = size(float64(used))
		if limit := series.Now.DiskLimit; limit > 0 {
			m := meterOf(int(used>>20), int(limit>>20))
			u.Data = &m
			u.DataLimit = size(float64(limit))
		}
	}
	return u
}

// nodeUse is a node's host as measured.
type nodeUse struct {
	CPU, Memory         meter
	CPUText, MemoryText string
}

// tenantUse is a tenant's use now, for the overview.
type tenantUse struct {
	Memory, CPU         meter
	MemoryText, CPUText string
	Requests            string
	// MemoryLimit says the limit, "" if none.
	MemoryLimit string
}

// clusterUse is the cluster's use now, by node and by tenant, and how its
// backups go; empty maps when the backend measures none, or did not
// answer.
type clusterUse struct {
	Nodes   map[string]nodeUse
	Tenants map[string]tenantUse
	// Backups are each tenant's newest backup, by tenant; NodeBackups each
	// node's backups, by node, for those that report them.
	Backups     map[string]backupView
	NodeBackups map[string]nodeBackups
	// Missing are the nodes that did not answer.
	Missing []string
	// Measured says the backend answered.
	Measured bool
}

// backupView is a tenant's backups, as the panel says them.
type backupView struct {
	Node string
	Last time.Time
	// Every is how often its node backs it up.
	Every time.Duration
	// Failing says its last try failed; Stale that its newest is older
	// than two runs; None that it has none yet.
	Failing, Stale, None bool
	// Error says why its last try failed, when its node said.
	Error string
}

// Bad says something is wrong with its backups.
func (b backupView) Bad() bool { return b.Failing || b.Stale }

// nodeBackups is a node's backups of its tenants.
type nodeBackups struct {
	Node    string
	Ready   bool
	Every   time.Duration
	Tenants int
	// Failing and Stale count its tenants whose backups are so; Oldest is
	// the oldest of their newest backups.
	Failing, Stale, None int
	Oldest               time.Time
	// State is the cluster's state's backups, on the leader.
	State *backupView
}

func backupOf(node string, b usage.Backup, every time.Duration, now time.Time) backupView {
	v := backupView{Node: node, Last: b.Last, Every: every, Failing: b.Failing, None: b.Last.IsZero(), Error: b.Error}
	v.Stale = every > 0 && !b.Last.IsZero() && now.Sub(b.Last) > 2*every
	return v
}

// usageNow is the cluster's use now, and how its backups go; the
// archived tenants named, backed up once as archived, are never late and
// count in no node's backups.
func (s *Server) usageNow(ctx context.Context, archived ...map[string]bool) clusterUse {
	isArchived := func(name string) bool {
		for _, a := range archived {
			if a[name] {
				return true
			}
		}
		return false
	}
	use := clusterUse{Nodes: map[string]nodeUse{}, Tenants: map[string]tenantUse{}, Backups: map[string]backupView{}, NodeBackups: map[string]nodeBackups{}}
	measured, ok := s.backend.(Usage)
	if !ok {
		return use
	}
	report, err := measured.Usage(ctx)
	if err != nil {
		return use
	}
	use.Measured, use.Missing = true, report.Missing
	now := s.now()
	for _, n := range report.Nodes {
		h := n.Host
		used := h.MemoryTotal - h.MemoryAvailable
		use.Nodes[n.Node] = nodeUse{
			CPU: meterOf(int(h.CPU*100), h.CPUs*100), CPUText: cores(h.CPU) + " of " + strconv.Itoa(h.CPUs),
			Memory: meterOf(int(used>>20), int(h.MemoryTotal>>20)), MemoryText: size(float64(used)) + " of " + size(float64(h.MemoryTotal)),
		}
		for name, t := range n.Tenants {
			tu := tenantUse{MemoryText: size(float64(t.Memory)), CPUText: cores(t.CPU), Requests: rate(float64(t.Requests))}
			if t.MemoryLimit > 0 {
				tu.Memory = meterOf(int(t.Memory>>20), int(t.MemoryLimit>>20))
				tu.MemoryLimit = size(float64(t.MemoryLimit))
			}
			if t.CPULimit > 0 {
				tu.CPU = meterOf(int(t.CPU*1000), int(t.CPULimit*1000))
			}
			use.Tenants[name] = tu
		}
		if b := n.Backups; b != nil {
			every := time.Duration(b.Every) * time.Second
			nb := nodeBackups{Node: n.Node, Ready: b.Ready, Every: every}
			for name, backup := range b.Tenants {
				v := backupOf(n.Node, backup, every, now)
				v.Stale = v.Stale && !isArchived(name)
				use.Backups[name] = v
				if isArchived(name) {
					continue
				}
				nb.Tenants++
				switch {
				case v.Failing:
					nb.Failing++
				case v.Stale:
					nb.Stale++
				case v.None:
					nb.None++
				}
				if !v.Last.IsZero() && (nb.Oldest.IsZero() || v.Last.Before(nb.Oldest)) {
					nb.Oldest = v.Last
				}
			}
			if b.State != nil {
				v := backupOf(n.Node, *b.State, every, now)
				nb.State = &v
			}
			use.NodeBackups[n.Node] = nb
		}
	}
	return use
}

// size says an amount of bytes: 812 KB, 312 MB, 1.2 GB.
func size(v float64) string {
	for _, unit := range []string{"B", "KB", "MB", "GB", "TB"} {
		if math.Abs(v) < 1024 || unit == "TB" {
			if unit == "B" || v >= 100 {
				return strconv.FormatFloat(math.Round(v), 'f', -1, 64) + " " + unit
			}
			return strconv.FormatFloat(math.Round(v*10)/10, 'f', -1, 64) + " " + unit
		}
		v /= 1024
	}
	return ""
}

func throughput(v float64) string { return size(v) + "/s" }

func cores(v float64) string {
	switch {
	case v == 0:
		return "0"
	case v < 0.01:
		return "<0.01"
	case v < 10:
		return strconv.FormatFloat(math.Round(v*100)/100, 'f', -1, 64)
	}
	return strconv.FormatFloat(math.Round(v*10)/10, 'f', -1, 64)
}

func rate(v float64) string {
	switch {
	case v == 0:
		return "0"
	case v < 1:
		return strconv.FormatFloat(math.Round(v*10)/10, 'f', -1, 64)
	case v < 1000:
		return strconv.FormatFloat(math.Round(v), 'f', -1, 64)
	}
	return fmt.Sprintf("%.1fk", v/1000)
}

// wait says a time to the response, from seconds.
func wait(v float64) string {
	switch {
	case v == 0:
		return "none"
	case v < 1:
		return strconv.FormatFloat(math.Round(v*1000), 'f', -1, 64) + " ms"
	case v >= 30:
		// The edge's histogram ends there.
		return "> 30 s"
	}
	return strconv.FormatFloat(math.Round(v*100)/100, 'f', -1, 64) + " s"
}
