package plugins

import (
	"github.com/PowerPenguini/murder/internal/metrics"
	"maps"
	"slices"
	"strings"
	"sync"
	"time"
)

// MaxCount bounds one count, far above any real one: the agent keeps no
// count past it.
const MaxCount = 1 << 40

// Count is an amount of a metric a tenant used of a plugin on a day (UTC,
// as 2006-01-02), for one detail (a model, a service; empty for none).
type Count struct {
	Tenant string `json:"tenant"`
	Plugin string `json:"plugin"`
	Day    string `json:"day"`
	Metric string `json:"metric"`
	Detail string `json:"detail,omitempty"`
	Amount int64  `json:"amount"`
}

// key is what a Count counts.
type key struct{ tenant, plugin, day, metric, detail string }

func (c Count) key() key { return key{c.Tenant, c.Plugin, c.Day, c.Metric, c.Detail} }

// Report is what an edge counted since its last report: each edge process
// numbers its reports from 1, and sends one again, the same, until the
// agent took it, so the agent counts each once (see store.AddPluginUsage).
type Report struct {
	// Edge names the edge process (see usage.EdgeInstance).
	Edge   string  `json:"edge"`
	Seq    int64   `json:"seq"`
	Counts []Count `json:"counts"`
}

// Totals are what tenants used, cluster wide, by period: a day
// (2006-01-02) or a month (2006-01), summed over details. The agent gives
// them to its edge with each report it took.
type Totals struct {
	// Edge and Seq name the last report of this edge the totals include.
	Edge   string  `json:"edge"`
	Seq    int64   `json:"seq"`
	Totals []Total `json:"totals"`
}

// Total is what a tenant used of a plugin's metric in a period.
type Total struct {
	Tenant string `json:"tenant"`
	Plugin string `json:"plugin"`
	Period string `json:"period"`
	Metric string `json:"metric"`
	Amount int64  `json:"amount"`
}

// totalKey is what a Total counts.
type totalKey struct{ tenant, plugin, period, metric string }

// Day and Month name the periods a time is in, in UTC.
func Day(t time.Time) string   { return t.UTC().Format(time.DateOnly) }
func Month(t time.Time) string { return t.UTC().Format("2006-01") }

// PeriodOf names the period of a limit that t is in.
func PeriodOf(per string, t time.Time) string {
	if per == PerDay {
		return Day(t)
	}
	return Month(t)
}

// PeriodEnd is when the period of a limit that t is in ends.
func PeriodEnd(per string, t time.Time) time.Time {
	t = t.UTC()
	if per == PerDay {
		return time.Date(t.Year(), t.Month(), t.Day()+1, 0, 0, 0, 0, time.UTC)
	}
	return time.Date(t.Year(), t.Month()+1, 1, 0, 0, 0, 0, time.UTC)
}

// Counter counts what an edge's tenants use, until the agent takes it, and
// knows what they used before, cluster wide: a tenant's use is its total
// as the agent last said, plus what this edge counted since.
type Counter struct {
	edge string

	mu sync.Mutex
	// pending is what was counted since the report in flight was made.
	pending map[key]int64
	// inflight is the report sent and not yet taken, sent again as it is
	// until it is; nil for none.
	inflight *Report
	seq      int64
	// totals are the agent's, as of report taken: they include every
	// report up to it.
	totals map[totalKey]int64
	taken  int64
}

// NewCounter counts for the edge process named edge.
func NewCounter(edge string) *Counter {
	return &Counter{edge: edge, pending: map[key]int64{}, totals: map[totalKey]int64{}}
}

// Edge names the edge process the counter counts for.
func (c *Counter) Edge() string { return c.edge }

// maxPending is how many counts a report holds: past it, what a tenant
// uses is counted with the detail "other".
const maxPending = 4096

// Add counts amounts.
func (c *Counter) Add(counts ...Count) {
	c.mu.Lock()
	defer c.mu.Unlock()
	for _, count := range counts {
		if count.Amount <= 0 {
			continue
		}
		k := count.key()
		if _, ok := c.pending[k]; !ok && len(c.pending) >= maxPending {
			k.detail = "other"
		}
		// Past MaxCount the agent would keep none of it: a count stops
		// there, far past any limit.
		c.pending[k] = min(c.pending[k]+min(count.Amount, MaxCount), MaxCount)
	}
}

// Drop forgets the report in flight, which the agent refused as it is: it
// would be refused again.
func (c *Counter) Drop(seq int64) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.inflight != nil && c.inflight.Seq == seq {
		c.inflight = nil
	}
}

// Next is the report to send: the one in flight, again, or a new one of
// what was counted since; ok is false when there is nothing to send.
func (c *Counter) Next() (Report, bool) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.inflight == nil {
		if len(c.pending) == 0 {
			return Report{}, false
		}
		c.seq++
		report := &Report{Edge: c.edge, Seq: c.seq, Counts: countsOf(c.pending)}
		c.inflight, c.pending = report, map[key]int64{}
	}
	return *c.inflight, true
}

// Took records the agent's answer to a report: it counted it, and these
// are the totals since.
func (c *Counter) Took(totals Totals) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if totals.Edge != c.edge {
		return
	}
	if c.inflight != nil && totals.Seq >= c.inflight.Seq {
		c.inflight = nil
	}
	if totals.Seq < c.taken {
		return // an older answer than one already had
	}
	c.taken = totals.Seq
	c.totals = map[totalKey]int64{}
	for _, total := range totals.Totals {
		c.totals[totalKey{total.Tenant, total.Plugin, total.Period, total.Metric}] = total.Amount
	}
}

// Used is what a tenant used of a plugin's metric in the period of per
// that now is in: the agent's total, and what this edge counted since.
func (c *Counter) Used(tenant, plugin, metric, per string, now time.Time) int64 {
	period := PeriodOf(per, now)
	c.mu.Lock()
	defer c.mu.Unlock()
	used := c.totals[totalKey{tenant, plugin, period, metric}]
	inPeriod := func(k key) bool {
		return k.tenant == tenant && k.plugin == plugin && k.metric == metric && strings.HasPrefix(k.day, period)
	}
	for k, amount := range c.pending {
		if inPeriod(k) {
			used += amount
		}
	}
	if c.inflight != nil && c.inflight.Seq > c.taken {
		for _, count := range c.inflight.Counts {
			if inPeriod(count.key()) {
				used += count.Amount
			}
		}
	}
	return used
}

// countsOf lists counts in a stable order.
func countsOf(amounts map[key]int64) []Count {
	keys := slices.SortedFunc(maps.Keys(amounts), func(a, b key) int {
		return strings.Compare(a.tenant+"\x00"+a.plugin+"\x00"+a.day+"\x00"+a.metric+"\x00"+a.detail, b.tenant+"\x00"+b.plugin+"\x00"+b.day+"\x00"+b.metric+"\x00"+b.detail)
	})
	counts := make([]Count, 0, len(keys))
	for _, k := range keys {
		counts = append(counts, Count{Tenant: k.tenant, Plugin: k.plugin, Day: k.day, Metric: k.metric, Detail: k.detail, Amount: amounts[k]})
	}
	return counts
}

// WriteLimits writes, for each limit of the node's tenants, what they used
// of it and the limit, so an alert can follow a soft limit: the period's
// use is murder_plugin_used, the limit murder_plugin_limit.
func (c *Counter) WriteLimits(out *metrics.Writer, config Config, now time.Time) {
	type sample struct {
		labels []string
		used   int64
		amount int64
	}
	var samples []sample
	for _, tenant := range slices.Sorted(maps.Keys(config.Tenants)) {
		grants := config.Tenants[tenant].Grants
		for _, plugin := range slices.Sorted(maps.Keys(grants)) {
			for _, limit := range grants[plugin].Limits {
				mode := limit.Mode
				if mode == "" {
					mode = ModeHard
				}
				samples = append(samples, sample{
					labels: []string{"tenant", tenant, "plugin", plugin, "metric", limit.Metric, "per", limit.Per, "mode", mode},
					used:   c.Used(tenant, plugin, limit.Metric, limit.Per, now),
					amount: limit.Amount,
				})
			}
		}
	}
	if len(samples) == 0 {
		return
	}
	out.Family("murder_plugin_used", "gauge", "What a tenant used of a plugin's metric in the period of one of its limits, cluster wide as the agent last said, and since.")
	for _, s := range samples {
		out.Sample("murder_plugin_used", float64(s.used), s.labels...)
	}
	out.Family("murder_plugin_limit", "gauge", "A tenant's limit of a plugin's metric in a period: hard refuses requests past it, soft only says so.")
	for _, s := range samples {
		out.Sample("murder_plugin_limit", float64(s.amount), s.labels...)
	}
}
