// Package usage measures what each tenant uses: CPU, memory, network and
// disk from its container's cgroup, its data from the volume, and its
// requests from the node's edge. It keeps a day of it a minute at a time,
// in memory: a restarted agent starts the day again. Long histories and
// alerts are for Prometheus, which reads the same from /metrics.
package usage

import (
	"math"
	"slices"
	"sync"
	"time"

	"github.com/PowerPenguini/murder/internal/metrics"
)

// Kept is how long a tenant's minutes are kept.
const Kept = 24 * time.Hour

const minutes = int(Kept / time.Minute)

// Raw is one reading of a tenant's container: counters since it started,
// and its memory now.
type Raw struct {
	At time.Time `json:"at"`
	// Container names the container read: a new one starts its counters
	// at zero.
	Container string `json:"container"`
	// CPU is the processor time used, in microseconds.
	CPU int64 `json:"cpu"`
	// Memory is what it uses now; MemoryLimit its limit, 0 if none.
	Memory      int64 `json:"memory"`
	MemoryLimit int64 `json:"memoryLimit"`
	// NetRx and NetTx are bytes received and sent; DiskRead and
	// DiskWrite bytes read and written.
	NetRx     int64 `json:"netRx"`
	NetTx     int64 `json:"netTx"`
	DiskRead  int64 `json:"diskRead"`
	DiskWrite int64 `json:"diskWrite"`
}

// Point is one minute of a tenant's use, or several minutes summed (CPU
// and memory then their mean and highest).
type Point struct {
	At time.Time `json:"at"`
	// CPU is the cores used, on average.
	CPU float64 `json:"cpu"`
	// Memory is the most it used, in bytes.
	Memory int64 `json:"memory"`
	// Bytes received, sent, read and written.
	NetRx     int64 `json:"netRx"`
	NetTx     int64 `json:"netTx"`
	DiskRead  int64 `json:"diskRead"`
	DiskWrite int64 `json:"diskWrite"`
	// Requests answered, and of them errors: 5xx answers, among them
	// those for requests the edge could not pass on.
	Requests int64 `json:"requests"`
	Errors   int64 `json:"errors"`
	// P95 is the time to the response headers that 95% of requests did
	// better than, in seconds; 0 without requests. Summed, it is the
	// highest minute's.
	P95 float64 `json:"p95"`
	// Minutes are how many minutes the point sums anything was measured
	// in, Read how many its container was read in, and CPURead how many
	// its CPU was: rates are per minute measured, and CPU the mean of the
	// minutes it was read in.
	Minutes int `json:"minutes"`
	Read    int `json:"read"`
	CPURead int `json:"cpuRead"`
}

// Now is a tenant's use as last measured: its last whole minute, with
// memory as last read.
type Now struct {
	At          time.Time `json:"at"`
	CPU         float64   `json:"cpu"`
	Memory      int64     `json:"memory"`
	MemoryLimit int64     `json:"memoryLimit,omitempty"`
	CPULimit    float64   `json:"cpuLimit,omitempty"`
	NetRx       int64     `json:"netRx"`
	NetTx       int64     `json:"netTx"`
	DiskRead    int64     `json:"diskRead"`
	DiskWrite   int64     `json:"diskWrite"`
	// DiskUsed is its data's size in bytes; -1 when the volume driver
	// cannot tell.
	DiskUsed int64 `json:"diskUsed"`
	// DiskLimit is the most its data may take, in bytes; 0 if unlimited.
	DiskLimit int64   `json:"diskLimit,omitempty"`
	Requests  int64   `json:"requests"`
	Errors    int64   `json:"errors"`
	P95       float64 `json:"p95"`
	// Counters are its container's counters as last read, since it
	// started: for Prometheus, whose rate() takes a restart in its stride.
	Counters Raw `json:"counters"`
}

// Host is the node itself.
type Host struct {
	CPUs int `json:"cpus"`
	// CPU is the cores busy over the last sample.
	CPU             float64 `json:"cpu"`
	MemoryTotal     int64   `json:"memoryTotal"`
	MemoryAvailable int64   `json:"memoryAvailable"`
	Load1           float64 `json:"load1"`
}

// Report is a node's use now: the host's and each of its tenants'.
type Report struct {
	Node    string         `json:"node"`
	At      time.Time      `json:"at"`
	Host    Host           `json:"host"`
	Tenants map[string]Now `json:"tenants"`
}

// Series is a tenant's use over the kept day, in steps.
type Series struct {
	Tenant string `json:"tenant"`
	Node   string `json:"node"`
	// Step is the seconds each point sums.
	Step   int     `json:"step"`
	Points []Point `json:"points"`
	Now    Now     `json:"now"`
}

// Recorder keeps the tenants' use.
type Recorder struct {
	node    string
	mu      sync.Mutex
	tenants map[string]*history
	host    Host
	hostAt  time.Time
}

// NewRecorder keeps node's use.
func NewRecorder(node string) *Recorder {
	return &Recorder{node: node, tenants: map[string]*history{}}
}

// history is one tenant's kept minutes, the minute being measured, and
// what its counters said last.
type history struct {
	points []Point // oldest first, at most minutes
	cur    Point
	// samples and cpuSum measure cur's CPU mean; read says its
	// container was read at all.
	samples int
	cpuSum  float64
	read    bool
	last    Raw
	// edge is the edge's counts as last told; requests and waits are
	// cur's.
	edge     metrics.Counts
	edgeSeen bool
	waits    [len(metrics.Buckets) + 1]uint64
	now      Now
	limitCPU float64
}

// Sample records a reading of tenant's container. cpuLimit is the cores
// it may use, 0 if unlimited.
func (r *Recorder) Sample(tenant string, raw Raw, cpuLimit float64) {
	r.mu.Lock()
	defer r.mu.Unlock()
	h := r.history(tenant, raw.At)
	h.limitCPU = cpuLimit
	if h.last.Container == raw.Container && !h.last.At.IsZero() && raw.At.After(h.last.At) {
		seconds := raw.At.Sub(h.last.At).Seconds()
		h.cpuSum += float64(delta(h.last.CPU, raw.CPU)) / 1e6 / seconds
		h.samples++
		h.cur.NetRx += delta(h.last.NetRx, raw.NetRx)
		h.cur.NetTx += delta(h.last.NetTx, raw.NetTx)
		h.cur.DiskRead += delta(h.last.DiskRead, raw.DiskRead)
		h.cur.DiskWrite += delta(h.last.DiskWrite, raw.DiskWrite)
	}
	h.cur.Memory = max(h.cur.Memory, raw.Memory)
	h.read = true
	h.now.Memory, h.now.MemoryLimit = raw.Memory, raw.MemoryLimit
	h.last = raw
}

// Stopped says tenant's container does not run: what it used is no longer
// what it uses.
func (r *Recorder) Stopped(tenant string) {
	r.mu.Lock()
	defer r.mu.Unlock()
	if h := r.tenants[tenant]; h != nil {
		h.now = Now{DiskUsed: h.now.DiskUsed, DiskLimit: h.now.DiskLimit}
		h.last = Raw{}
	}
}

// Disk records the size of tenant's data, -1 when it cannot be told, and
// its limit, 0 if none.
func (r *Recorder) Disk(tenant string, used, limit int64, at time.Time) {
	r.mu.Lock()
	defer r.mu.Unlock()
	h := r.history(tenant, at)
	h.now.DiskUsed, h.now.DiskLimit = used, limit
}

// Edge records the edge's counts of tenant's requests, as they stand.
func (r *Recorder) Edge(tenant string, counts metrics.Counts, at time.Time) {
	r.mu.Lock()
	defer r.mu.Unlock()
	h := r.history(tenant, at)
	// The first counts told hold everything since the edge started: what
	// follows them is counted, as the agent did not see the rest.
	if !h.edgeSeen {
		h.edge, h.edgeSeen = counts, true
		return
	}
	// Counts that went back belong to an edge that restarted: they all
	// count anew.
	before := h.edge
	if counts.Count < before.Count {
		before = metrics.Counts{}
	}
	for class := range counts.ByClass {
		n := int64(delta(int64(before.ByClass[class]), int64(counts.ByClass[class])))
		h.cur.Requests += n
		// A request the edge could not pass on is answered 502: counted
		// here, not again as a backend error.
		if class == 4 {
			h.cur.Errors += n
		}
	}
	for i := range counts.Waits {
		var was uint64
		if i < len(before.Waits) {
			was = before.Waits[i]
		}
		if i < len(h.waits) {
			h.waits[i] += uint64(delta(int64(was), int64(counts.Waits[i])))
		}
	}
	h.edge = counts
}

// Host records the node's own use.
func (r *Recorder) Host(host Host, at time.Time) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.host, r.hostAt = host, at
}

// Keep forgets the tenants keep refuses: those no longer on the node.
func (r *Recorder) Keep(keep func(tenant string) bool) {
	r.mu.Lock()
	defer r.mu.Unlock()
	for name := range r.tenants {
		if !keep(name) {
			delete(r.tenants, name)
		}
	}
}

// Tick closes the minutes that ended by at, for every tenant.
func (r *Recorder) Tick(at time.Time) {
	r.mu.Lock()
	defer r.mu.Unlock()
	for _, h := range r.tenants {
		h.roll(at)
	}
}

// history returns tenant's, closing its minute if at is past it.
func (r *Recorder) history(tenant string, at time.Time) *history {
	h := r.tenants[tenant]
	if h == nil {
		h = &history{cur: Point{At: at.Truncate(time.Minute)}, now: Now{DiskUsed: -1}}
		r.tenants[tenant] = h
	}
	h.roll(at)
	return h
}

// roll closes the minute being measured once at is past it. A minute
// nothing was measured in is kept as no point at all.
func (h *history) roll(at time.Time) {
	minute := at.Truncate(time.Minute)
	if !minute.After(h.cur.At) {
		return
	}
	if h.read || h.cur.Requests > 0 {
		h.cur.Minutes = 1
		h.cur.P95 = percentile(h.waits[:], 0.95)
		next := h.now
		next.At, next.CPULimit = h.cur.At, h.limitCPU
		next.Requests, next.Errors, next.P95 = h.cur.Requests, h.cur.Errors, h.cur.P95
		if h.read {
			h.cur.Read = 1
		}
		// What only a second reading tells, a minute with one reading does
		// not: the minutes before say it.
		if h.samples > 0 {
			h.cur.CPU, h.cur.CPURead = h.cpuSum/float64(h.samples), 1
			next.CPU, next.NetRx, next.NetTx = h.cur.CPU, h.cur.NetRx, h.cur.NetTx
			next.DiskRead, next.DiskWrite = h.cur.DiskRead, h.cur.DiskWrite
		}
		h.points = append(h.points, h.cur)
		if len(h.points) > minutes {
			h.points = slices.Delete(h.points, 0, len(h.points)-minutes)
		}
		h.now = next
	}
	h.cur, h.samples, h.cpuSum, h.read, h.waits = Point{At: minute}, 0, 0, false, [len(metrics.Buckets) + 1]uint64{}
}

// Report is the node's use now.
func (r *Recorder) Report(at time.Time) Report {
	r.mu.Lock()
	defer r.mu.Unlock()
	report := Report{Node: r.node, At: at, Host: r.host, Tenants: make(map[string]Now, len(r.tenants))}
	for name, h := range r.tenants {
		h.roll(at)
		if !h.now.At.IsZero() {
			now := h.now
			now.Counters = h.last
			report.Tenants[name] = now
		}
	}
	return report
}

// Series is tenant's kept day in steps of step, ending with the step now
// runs in; ok is false for a tenant not measured here.
func (r *Recorder) Series(tenant string, step time.Duration, at time.Time) (Series, bool) {
	r.mu.Lock()
	defer r.mu.Unlock()
	h := r.tenants[tenant]
	if h == nil {
		return Series{}, false
	}
	h.roll(at)
	step = max(time.Minute, step.Truncate(time.Minute))
	series := Series{Tenant: tenant, Node: r.node, Step: int(step / time.Second), Now: h.now, Points: []Point{}}
	series.Now.Counters = h.last
	for _, p := range h.points {
		start := p.At.Truncate(step)
		n := len(series.Points)
		if n == 0 || !series.Points[n-1].At.Equal(start) {
			series.Points = append(series.Points, Point{At: start})
			n++
		}
		sum := &series.Points[n-1]
		sum.Minutes += p.Minutes
		sum.Read += p.Read
		sum.CPURead += p.CPURead
		sum.CPU += p.CPU
		sum.Memory = max(sum.Memory, p.Memory)
		sum.NetRx += p.NetRx
		sum.NetTx += p.NetTx
		sum.DiskRead += p.DiskRead
		sum.DiskWrite += p.DiskWrite
		sum.Requests += p.Requests
		sum.Errors += p.Errors
		sum.P95 = max(sum.P95, p.P95)
	}
	for i := range series.Points {
		if read := series.Points[i].CPURead; read > 0 {
			series.Points[i].CPU /= float64(read)
		}
	}
	return series, true
}

// delta is how far a counter went from before to now: all of now when it
// went back, as a restarted container's or edge's does.
func delta(before, now int64) int64 {
	if now < before {
		return now
	}
	return now - before
}

// percentile reads the q quantile off a histogram of request waits (per
// bucket, the last past the highest bound), within the bucket it falls
// in, linearly.
func percentile(waits []uint64, q float64) float64 {
	var total uint64
	for _, n := range waits {
		total += n
	}
	if total == 0 {
		return 0
	}
	target := q * float64(total)
	var seen float64
	lower := 0.0
	for i, n := range waits {
		upper := math.Inf(1)
		if i < len(metrics.Buckets) {
			upper = metrics.Buckets[i]
		}
		if seen+float64(n) >= target && n > 0 {
			if math.IsInf(upper, 1) {
				return lower
			}
			return lower + (upper-lower)*(target-seen)/float64(n)
		}
		seen += float64(n)
		if !math.IsInf(upper, 1) {
			lower = upper
		}
	}
	return lower
}
