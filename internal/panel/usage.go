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

// usageStep is the step of a tenant's charts: a day in 288 points.
const usageStep = 5 * time.Minute

// tenantUsage is a tenant's day, drawn.
type tenantUsage struct {
	Charts []lineChart
	// Memory is what it uses now against its limit, with its CPU; the
	// rest its requests a minute, errors among them, and the 95th
	// percentile of their response time, in its last minute.
	Memory                 tenantUse
	CPULimit               string
	Requests, Errors, Wait string
	// Data is its data's size against its disk limit, if both are known.
	Data                *meter
	DataText, DataLimit string
}

func (s *Server) tenantUsage(ctx context.Context, name string) *tenantUsage {
	measured, ok := s.backend.(Usage)
	if !ok {
		return nil
	}
	series, err := measured.TenantUsage(ctx, name, usageStep)
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
		newLineChart("CPU", []string{"cores"}, pick(cpuRead, func(p usage.Point) []float64 { return []float64{p.CPU} }), series.Now.CPULimit, now, usageStep, cores),
		newLineChart("Memory", []string{"used"}, pick(read, func(p usage.Point) []float64 { return []float64{float64(p.Memory)} }), float64(series.Now.MemoryLimit), now, usageStep, size),
		newLineChart("Requests", []string{"a minute", "errors"}, pick(counted, func(p usage.Point) []float64 {
			minutes := float64(p.Minutes)
			return []float64{float64(p.Requests) / minutes, float64(p.Errors) / minutes}
		}), 0, now, usageStep, rate),
		newLineChart("Response time, 95th percentile", []string{"p95"}, pick(answered, func(p usage.Point) []float64 { return []float64{p.P95} }), 0, now, usageStep, wait),
		newLineChart("Network", []string{"in", "out"}, pick(cpuRead, func(p usage.Point) []float64 {
			return []float64{perSecond(p.NetRx, p), perSecond(p.NetTx, p)}
		}), 0, now, usageStep, throughput),
		newLineChart("Disk", []string{"read", "written"}, pick(cpuRead, func(p usage.Point) []float64 {
			return []float64{perSecond(p.DiskRead, p), perSecond(p.DiskWrite, p)}
		}), 0, now, usageStep, throughput),
	}}
	// Errors are drawn in the colour of what is wrong.
	u.Charts[2].Series[1].Stroke = inkBad
	u.Memory = tenantUse{MemoryText: size(float64(series.Now.Memory)), CPUText: cores(series.Now.CPU)}
	if limit := series.Now.CPULimit; limit > 0 {
		u.Memory.CPU, u.CPULimit = meterOf(int(series.Now.CPU*1000), int(limit*1000)), cores(limit)
	}
	u.Requests, u.Errors, u.Wait = rate(float64(series.Now.Requests)), rate(float64(series.Now.Errors)), wait(series.Now.P95)
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

// usageNow is the cluster's use now, by node and by tenant; nil maps when
// the backend measures none, or did not answer.
func (s *Server) usageNow(ctx context.Context) (map[string]nodeUse, map[string]tenantUse) {
	measured, ok := s.backend.(Usage)
	if !ok {
		return nil, nil
	}
	report, err := measured.Usage(ctx)
	if err != nil {
		return nil, nil
	}
	nodes, tenants := map[string]nodeUse{}, map[string]tenantUse{}
	for _, n := range report.Nodes {
		h := n.Host
		used := h.MemoryTotal - h.MemoryAvailable
		nodes[n.Node] = nodeUse{
			CPU: meterOf(int(h.CPU*100), h.CPUs*100), CPUText: cores(h.CPU) + " of " + strconv.Itoa(h.CPUs),
			Memory: meterOf(int(used>>20), int(h.MemoryTotal>>20)), MemoryText: size(float64(used)) + " of " + size(float64(h.MemoryTotal)),
		}
		for name, t := range n.Tenants {
			use := tenantUse{MemoryText: size(float64(t.Memory)), CPUText: cores(t.CPU), Requests: rate(float64(t.Requests))}
			if t.MemoryLimit > 0 {
				use.Memory = meterOf(int(t.Memory>>20), int(t.MemoryLimit>>20))
				use.MemoryLimit = size(float64(t.MemoryLimit))
			}
			if t.CPULimit > 0 {
				use.CPU = meterOf(int(t.CPU*1000), int(t.CPULimit*1000))
			}
			tenants[name] = use
		}
	}
	return nodes, tenants
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
		return "—"
	case v < 1:
		return strconv.FormatFloat(math.Round(v*1000), 'f', -1, 64) + " ms"
	case v >= 30:
		// The edge's histogram ends there.
		return "> 30 s"
	}
	return strconv.FormatFloat(math.Round(v*100)/100, 'f', -1, 64) + " s"
}
