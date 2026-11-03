package usage

import (
	"context"
	"io"
	"log/slog"
	"math"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/PowerPenguini/murder/internal/metrics"
)

var t0 = time.Date(2026, 11, 2, 12, 0, 0, 0, time.UTC)

func near(a, b float64) bool { return math.Abs(a-b) < 1e-9 }

// Readings 15 s apart make a minute: CPU as cores on average, memory at
// its highest, bytes summed; a minute closes once a reading is past it.
func TestAMinuteOfReadings(t *testing.T) {
	r := NewRecorder("n1")
	for i := range 5 {
		at := t0.Add(time.Duration(i) * 15 * time.Second)
		// Half a core: 7.5 s of CPU every 15 s.
		r.Sample("web", Raw{At: at, Container: "c1", CPU: int64(i) * 7_500_000, Memory: int64(100+i*10) << 20, MemoryLimit: 512 << 20,
			NetRx: int64(i) * 1000, NetTx: int64(i) * 500, DiskWrite: int64(i) * 4096}, 1)
	}
	series, ok := r.Series("web", time.Minute, t0.Add(61*time.Second))
	if !ok || len(series.Points) != 1 {
		t.Fatalf("series = %+v, %v", series, ok)
	}
	p := series.Points[0]
	// The fifth reading (at 60 s) opens the next minute; the first four
	// made three intervals in the first.
	if !near(p.CPU, 0.5) || p.Memory != 130<<20 || p.NetRx != 3000 || p.NetTx != 1500 || p.DiskWrite != 3*4096 {
		t.Fatalf("the minute: %+v", p)
	}
	now := r.Report(t0.Add(61 * time.Second)).Tenants["web"]
	if !near(now.CPU, 0.5) || now.Memory != 140<<20 || now.MemoryLimit != 512<<20 || now.CPULimit != 1 || now.DiskUsed != -1 {
		t.Fatalf("now: %+v", now)
	}
}

// A new container starts its counters at zero: nothing is counted across
// it, and a counter that went back counts anew.
func TestCountersThatStartAgain(t *testing.T) {
	r := NewRecorder("n1")
	r.Sample("web", Raw{At: t0, Container: "c1", CPU: 50_000_000, NetRx: 9000}, 0)
	r.Sample("web", Raw{At: t0.Add(15 * time.Second), Container: "c2", CPU: 1_000_000, NetRx: 100}, 0)
	r.Sample("web", Raw{At: t0.Add(30 * time.Second), Container: "c2", CPU: 4_000_000, NetRx: 400}, 0)
	series, _ := r.Series("web", time.Minute, t0.Add(time.Minute))
	if p := series.Points[0]; !near(p.CPU, 0.2) || p.NetRx != 300 {
		t.Fatalf("across a new container: %+v", p)
	}
	if delta(10, 3) != 3 || delta(3, 10) != 7 {
		t.Fatal("delta")
	}
}

// The edge's counts become requests, errors and a 95th percentile per
// minute; an edge that restarted (of a release that says no ID) counts
// anew.
func TestRequestsFromTheEdge(t *testing.T) {
	r := NewRecorder("n1")
	waits := func(fast, slow uint64) []uint64 {
		w := make([]uint64, len(metrics.Buckets)+1)
		w[0], w[5] = fast, slow // up to 5 ms; 100-250 ms
		return w
	}
	r.Edge(EdgeInstance{}, map[string]metrics.Counts{"web": {ByClass: [5]uint64{0, 100, 0, 0, 0}, Waits: waits(100, 0), Count: 100}}, t0)
	// In the first minute: 90 more 2xx, 10 more 5xx and 2 refused; 80 fast, 20 slow.
	r.Edge(EdgeInstance{}, map[string]metrics.Counts{"web": {ByClass: [5]uint64{0, 190, 0, 0, 10}, BackendErrors: 2, Waits: waits(180, 20), Count: 200}}, t0.Add(30*time.Second))
	// The edge restarted: 5 requests since.
	r.Edge(EdgeInstance{}, map[string]metrics.Counts{"web": {ByClass: [5]uint64{0, 5, 0, 0, 0}, Waits: waits(5, 0), Count: 5}}, t0.Add(70*time.Second))
	series, _ := r.Series("web", time.Minute, t0.Add(2*time.Minute))
	if len(series.Points) != 2 {
		t.Fatalf("points: %+v", series.Points)
	}
	first, second := series.Points[0], series.Points[1]
	// The first counts are only where counting starts: everything since
	// the edge started. The 2 it could not pass on were answered 502:
	// among the 10 errors, not counted again.
	if first.Requests != 100 || first.Errors != 10 || second.Requests != 5 || second.Errors != 0 || first.Minutes != 1 || first.Read != 0 {
		t.Fatalf("requests: %+v %+v", first, second)
	}
	// 80 of 100 within 5 ms, 20 at 100-250 ms: the 95th is three quarters
	// into that bucket.
	if !near(first.P95, 0.1+0.15*0.75) || !near(second.P95, 0.005*0.95) {
		t.Fatalf("p95: %v, %v", first.P95, second.P95)
	}
}

// Minutes add up into steps: CPU's mean, memory's highest, the rest
// summed; a minute without readings is no point.
func TestSteps(t *testing.T) {
	r := NewRecorder("n1")
	for i := range 21 {
		at := t0.Add(time.Duration(i) * 30 * time.Second)
		if i >= 6 && i < 10 { // minutes 3 and 4 measured nothing
			continue
		}
		r.Sample("web", Raw{At: at, Container: "c1", CPU: int64(i) * 30_000_000, Memory: int64(i) << 20, NetRx: int64(i) * 10}, 0)
	}
	series, _ := r.Series("web", 5*time.Minute, t0.Add(11*time.Minute))
	// The reading at 10 min opened the third step.
	if series.Step != 300 || len(series.Points) != 3 {
		t.Fatalf("series: %+v", series)
	}
	// A core throughout: the minutes not measured do not lower the mean.
	if first := series.Points[0]; !first.At.Equal(t0) || first.Memory != 5<<20 || !near(first.CPU, 1) {
		t.Fatalf("the first five minutes: %+v", first)
	}
	if missing := r.tenants["web"].points; len(missing) != 9 {
		t.Fatalf("minutes kept: %d", len(missing))
	}
}

// While one edge hands over to the next, both tell their counts, each
// since it started: each edge's requests count once, and the new edge's
// from its start.
func TestAHandoverCountsEachEdgeOnce(t *testing.T) {
	r := NewRecorder("n1")
	r.started = t0.Add(-time.Hour)
	parent := EdgeInstance{ID: "parent", Started: t0.Add(-2 * time.Hour)}
	child := EdgeInstance{ID: "child", Started: t0.Add(15 * time.Second)}
	tell := func(from EdgeInstance, n uint64, seconds int) {
		r.Edge(from, map[string]metrics.Counts{"web": {Count: n, ByClass: [5]uint64{0, n, 0, 0, 0}}}, t0.Add(time.Duration(seconds)*time.Second))
	}
	tell(parent, 1000, 0) // where counting starts: the parent ran before the agent
	tell(parent, 1010, 15)
	tell(child, 10, 16) // its first 10
	tell(parent, 1010, 30)
	tell(child, 20, 31)
	series, _ := r.Series("web", time.Minute, t0.Add(time.Minute))
	if len(series.Points) != 1 || series.Points[0].Requests != 30 {
		t.Fatalf("requests through a handover: %+v", series.Points)
	}
	// A tenant the child tells of only later may have been here before
	// (it moved away and back): its first counts are where counting starts.
	r.Edge(child, map[string]metrics.Counts{"api": {Count: 50, ByClass: [5]uint64{0, 50, 0, 0, 0}}}, t0.Add(45*time.Second))
	if series, _ := r.Series("api", time.Minute, t0.Add(time.Minute)); len(series.Points) != 0 {
		t.Fatalf("requests of a tenant told of later: %+v", series.Points)
	}
	// The parent, gone, is forgotten once it said nothing for long.
	tell(child, 20, int((edgeRetired + time.Minute).Seconds()))
	if edges := r.tenants["web"].edges; len(edges) != 1 || edges["child"] == nil || len(r.edges) != 1 {
		t.Fatalf("edges kept: %v, %v", edges, r.edges)
	}
}

// Kept is a day: older minutes go, also those of a tenant measured no
// more, such as one stopped.
func TestADayIsKept(t *testing.T) {
	r := NewRecorder("n1")
	minutes := int(Kept / time.Minute)
	for i := range minutes + 30 {
		r.Sample("web", Raw{At: t0.Add(time.Duration(i) * time.Minute), Container: "c1", Memory: 1}, 0)
	}
	r.Tick(t0.Add(time.Duration(minutes+30) * time.Minute))
	if n := len(r.tenants["web"].points); n != minutes {
		t.Fatalf("%d minutes kept", n)
	}
	r.Sample("stopped", Raw{At: t0, Container: "c1", Memory: 1}, 0)
	r.Tick(t0.Add(time.Minute))
	if series, _ := r.Series("stopped", time.Minute, t0.Add(Kept)); len(series.Points) != 1 {
		t.Fatalf("a day on: %+v", series.Points)
	}
	if series, _ := r.Series("stopped", time.Minute, t0.Add(2*Kept)); len(series.Points) != 0 {
		t.Fatalf("two days on: %+v", series.Points)
	}
	r.Keep(func(string) bool { return false })
	if _, ok := r.Series("web", time.Minute, t0); ok {
		t.Fatal("a tenant gone is still kept")
	}
}

func TestPercentile(t *testing.T) {
	w := make([]uint64, len(metrics.Buckets)+1)
	if percentile(w, 0.95) != 0 {
		t.Fatal("without requests")
	}
	w[len(w)-1] = 10 // all past 30 s
	if percentile(w, 0.95) != 30 {
		t.Fatalf("past the last bound: %v", percentile(w, 0.95))
	}
}

// The reader reads a container's cgroup and network namespace, and the
// host, as the kernel lays them out.
func TestReadingTheKernel(t *testing.T) {
	root := t.TempDir()
	proc, cgroup := filepath.Join(root, "proc"), filepath.Join(root, "cgroup")
	write := func(path, text string) {
		_ = os.MkdirAll(filepath.Dir(path), 0o755)
		if err := os.WriteFile(path, []byte(text), 0o644); err != nil {
			t.Fatal(err)
		}
	}
	scope := "/system.slice/docker-abc123.scope"
	write(filepath.Join(cgroup, "cgroup.controllers"), "cpu io memory pids\n")
	write(filepath.Join(cgroup, scope, "memory.stat"), "anon 60000000\nfile 13400320\ninactive_file 3400320\n")
	write(filepath.Join(proc, "42", "cgroup"), "0::"+scope+"\n")
	write(filepath.Join(cgroup, scope, "cpu.stat"), "usage_usec 1500000\nuser_usec 1000000\nsystem_usec 500000\n")
	write(filepath.Join(cgroup, scope, "memory.current"), "73400320\n")
	write(filepath.Join(cgroup, scope, "memory.max"), "536870912\n")
	write(filepath.Join(cgroup, scope, "io.stat"), "8:0 rbytes=4096 wbytes=8192 rios=1 wios=2 dbytes=0 dios=0\n259:0 rbytes=100 wbytes=0 rios=1 wios=0\n")
	write(filepath.Join(proc, "42", "net", "dev"), "Inter-|   Receive                                                |  Transmit\n face |bytes    packets errs drop fifo frame compressed multicast|bytes    packets errs drop fifo colls carrier compressed\n    lo:    999      1    0    0    0     0          0         0      999      1    0    0    0     0       0          0\n  eth0:  12345     10    0    0    0     0          0         0     6789      8    0    0    0     0       0          0\n")
	write(filepath.Join(proc, "7", "cgroup"), "12:cpu:/docker/x\n")

	reader := Reader{Proc: proc, Cgroup: cgroup}
	path, err := reader.CgroupOf(42)
	if err != nil || path != scope {
		t.Fatalf("cgroup of 42: %q, %v", path, err)
	}
	raw, err := reader.Container(42, path, t0)
	// Memory leaves out the files it no longer uses: 70 MB less 3.4 MB.
	if err != nil || raw.CPU != 1_500_000 || raw.Memory != 70000000 || raw.MemoryLimit != 512<<20 || raw.DiskRead != 4196 || raw.DiskWrite != 8192 || raw.NetRx != 12345 || raw.NetTx != 6789 {
		t.Fatalf("raw = %+v, %v", raw, err)
	}
	if _, err := reader.CgroupOf(7); err != ErrNoCgroup2 {
		t.Fatalf("cgroup v1: %v", err)
	}

	write(filepath.Join(proc, "stat"), "cpu  100 0 100 700 100 0 0 0 0 0\ncpu0 50 0 50 350 50 0 0 0 0 0\ncpu1 50 0 50 350 50 0 0 0 0 0\nintr 1\n")
	write(filepath.Join(proc, "meminfo"), "MemTotal:        4000000 kB\nMemFree:         1000000 kB\nMemAvailable:    3000000 kB\n")
	write(filepath.Join(proc, "loadavg"), "0.42 0.30 0.20 1/100 1234\n")
	host, times, err := reader.Host(CPUTimes{})
	if err != nil || host.CPUs != 2 || host.CPU != 0 || host.MemoryTotal != 4000000*1024 || host.MemoryAvailable != 3000000*1024 || host.Load1 != 0.42 || times.Busy != 200 || times.Total != 1000 {
		t.Fatalf("host = %+v %+v, %v", host, times, err)
	}
	write(filepath.Join(proc, "stat"), "cpu  250 0 250 800 100 0 0 0 0 0\ncpu0 1\ncpu1 1\n")
	if host, _, _ = reader.Host(times); !near(host.CPU, 2*300.0/400) {
		t.Fatalf("busy cores: %v", host.CPU)
	}

	// The collector reads it all into the recorder, and finds a process
	// again once its PID is another's.
	recorder := NewRecorder("n1")
	pids := 0
	collector := &Collector{Recorder: recorder, Reader: reader, Log: slog.New(slog.NewTextHandler(io.Discard, nil)), Now: func() time.Time { return t0 },
		Tenants: func() []Tenant {
			return []Tenant{{Name: "web", Container: "abc123", CPULimit: 2, DiskLimit: 10 << 30}, {Name: "idle"}}
		},
		Pid:  func(context.Context, string) (int, error) { pids++; return 42, nil },
		Disk: func(_ context.Context, tenant string) (int64, bool, error) { return 1 << 30, tenant == "web", nil }}
	collector.Collect(context.Background())
	collector.Now = func() time.Time { return t0.Add(time.Minute) }
	collector.Collect(context.Background())
	report := recorder.Report(t0.Add(time.Minute))
	if web := report.Tenants["web"]; web.Memory != 70000000 || web.DiskUsed != 1<<30 || web.DiskLimit != 10<<30 || web.CPULimit != 2 || pids != 1 || report.Host.CPUs != 2 || web.Counters.Container != "abc123/42" {
		t.Fatalf("report: %+v, %d lookups", report, pids)
	}
	write(filepath.Join(proc, "42", "cgroup"), "0:"+":/system.slice/other.scope\n")
	collector.Collect(context.Background())
	if pids != 2 {
		t.Fatalf("a PID given to another process was trusted: %d lookups", pids)
	}
}

// A stopped container uses nothing; one started again with the same ID
// starts its counters again; a minute its CPU was not read in does not
// count as idle.
func TestStoppedRestartedAndUnread(t *testing.T) {
	r := NewRecorder("n1")
	r.Sample("web", Raw{At: t0, Container: "c/1", CPU: 0, Memory: 100}, 0)
	r.Sample("web", Raw{At: t0.Add(30 * time.Second), Container: "c/1", CPU: 30_000_000, Memory: 100}, 0)
	// Started again: a new process, its counters from zero.
	r.Sample("web", Raw{At: t0.Add(60 * time.Second), Container: "c/2", CPU: 1_000_000, Memory: 50}, 0)
	r.Sample("web", Raw{At: t0.Add(150 * time.Second), Container: "c/2", CPU: 91_000_000, Memory: 50}, 0)
	series, _ := r.Series("web", 5*time.Minute, t0.Add(3*time.Minute))
	p := series.Points[0]
	// Minute 1 had one reading only: no CPU. Minutes 0 and 2 a core each.
	if p.Minutes != 3 || p.Read != 3 || p.CPURead != 2 || !near(p.CPU, 1) {
		t.Fatalf("the step: %+v", p)
	}
	if now := r.Report(t0.Add(3 * time.Minute)).Tenants["web"]; now.Memory != 50 || !near(now.CPU, 1) {
		t.Fatalf("now: %+v", now)
	}
	r.Disk("web", 7, 0, t0.Add(3*time.Minute))
	r.Stopped("web")
	if now, ok := r.Report(t0.Add(3 * time.Minute)).Tenants["web"]; ok {
		t.Fatalf("a stopped tenant uses %+v", now)
	}
	if series, _ := r.Series("web", time.Minute, t0.Add(3*time.Minute)); series.Now.DiskUsed != 7 {
		t.Fatalf("a stopped tenant's data was forgotten: %+v", series.Now)
	}
}

// Without a cgroup v2 hierarchy, containers are not read, and it is said
// once.
func TestAHostWithoutCgroup2(t *testing.T) {
	root := t.TempDir()
	recorder := NewRecorder("n1")
	looked := 0
	collector := &Collector{Recorder: recorder, Reader: Reader{Proc: filepath.Join(root, "proc"), Cgroup: filepath.Join(root, "cgroup")},
		Log: slog.New(slog.NewTextHandler(io.Discard, nil)), Now: func() time.Time { return t0 },
		Tenants: func() []Tenant { return []Tenant{{Name: "web", Container: "abc"}} },
		Pid:     func(context.Context, string) (int, error) { looked++; return 1, nil }}
	collector.Collect(context.Background())
	collector.Collect(context.Background())
	if looked != 0 || collector.cgroup2 {
		t.Fatalf("containers were read on a host without cgroup v2: %d", looked)
	}
}
