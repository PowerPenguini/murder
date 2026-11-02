package failover

import (
	"context"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"net/netip"
	"path/filepath"
	"slices"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/PowerPenguini/murder/internal/metrics"
	"github.com/PowerPenguini/murder/internal/spec"
	"github.com/PowerPenguini/murder/internal/store"
)

type fixture struct {
	mu       sync.Mutex
	clock    time.Time
	down     map[string]bool
	leader   bool
	fenced   []string
	fenceErr error
	// on is a node the provider reports on again after its power-off.
	on string
	// onWait runs during the settle wait.
	onWait  func()
	waited  []time.Duration
	store   *store.SQLite
	monitor *Monitor
}

func (f *fixture) now() time.Time {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.clock
}

func (f *fixture) advance(d time.Duration) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.clock = f.clock.Add(d)
}

func (f *fixture) PowerOff(_ context.Context, node store.Node) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.fenced = append(f.fenced, node.Name)
	return f.fenceErr
}

func (f *fixture) Off(_ context.Context, node store.Node) (bool, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.on != node.Name, nil
}

// newFixture is node n1, leading a cluster of n1, n2 and n3; n2 runs
// tenants a and b with replicas, and lone without one.
func newFixture(t *testing.T) *fixture {
	t.Helper()
	path := filepath.Join(t.TempDir(), "murder.db")
	if err := store.Init(path, "n1"); err != nil {
		t.Fatal(err)
	}
	st, err := store.Open(path, store.Options{NetworkPool: netip.MustParsePrefix("10.240.0.0/16"), Sealer: func(string, string, spec.Secret) (string, error) { return "sealed", nil }})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { st.Close() })
	for i, name := range []string{"n1", "n2", "n3"} {
		if err := st.PutNode(store.Node{Name: name, Address: fmt.Sprintf("10.0.0.%d:7400", i+1)}, "test"); err != nil {
			t.Fatal(err)
		}
	}
	app := spec.Tenant{Image: "sha256:" + strings.Repeat("a", 64), HealthPath: "/health", User: "1000:1000"}
	if _, err := st.PutApp(store.App{Name: "web", Spec: app}, "test"); err != nil {
		t.Fatal(err)
	}
	for _, name := range []string{"a", "b"} {
		if _, err := st.Create(store.NewTenant{Name: name, App: "web", Node: "n2"}, "test"); err != nil {
			t.Fatal(err)
		}
	}
	if _, err := st.PlaceReplicas("test"); err != nil {
		t.Fatal(err)
	}
	// Their copies hold the whole data, as n2 reports once it sent them.
	for _, name := range []string{"a", "b"} {
		record, _ := st.Get(name)
		if err := st.ReplicaReady(name, record.Replica); err != nil {
			t.Fatal(err)
		}
	}
	if _, err := st.Create(store.NewTenant{Name: "lone", App: "web", Node: "n2"}, "test"); err != nil {
		t.Fatal(err)
	}
	f := &fixture{clock: time.Date(2026, 11, 2, 10, 0, 0, 0, time.UTC), down: map[string]bool{}, leader: true, store: st}
	f.monitor = &Monitor{
		Store: st, Fence: f, Log: slog.New(slog.NewTextHandler(io.Discard, nil)), Now: f.now,
		Leader: func() bool { f.mu.Lock(); defer f.mu.Unlock(); return f.leader },
		Probe: func(_ context.Context, node string) error {
			f.mu.Lock()
			defer f.mu.Unlock()
			if f.down[node] {
				return errors.New("no answer")
			}
			return nil
		},
		Wait: func(_ context.Context, d time.Duration) error {
			f.mu.Lock()
			f.waited = append(f.waited, d)
			f.clock = f.clock.Add(d)
			onWait := f.onWait
			f.mu.Unlock()
			if onWait != nil {
				onWait()
			}
			return nil
		},
	}
	return f
}

// check runs one round and waits for the failover it started, if any.
func (f *fixture) check(t *testing.T) {
	t.Helper()
	f.monitor.Check(context.Background())
	deadline := time.Now().Add(5 * time.Second)
	for {
		f.monitor.mu.Lock()
		fencing := f.monitor.fencing
		f.monitor.mu.Unlock()
		if fencing == "" {
			return
		}
		if time.Now().After(deadline) {
			t.Fatal("the failover does not end")
		}
		time.Sleep(time.Millisecond)
	}
}

func (f *fixture) nodeOf(t *testing.T, tenant string) string {
	t.Helper()
	record, err := f.store.Get(tenant)
	if err != nil {
		t.Fatal(err)
	}
	return record.Node
}

// A node unanswered for 30 s is powered off; its tenants run from their
// replicas once the power-off is confirmed and 60 s have passed since it
// last answered. One without a replica stays.
func TestADeadNodeFailsOverOnceConfirmedOff(t *testing.T) {
	f := newFixture(t)
	f.check(t) // n2 answers
	f.down["n2"] = true
	for range 14 {
		f.advance(2 * time.Second)
		f.check(t)
	}
	if len(f.fenced) != 0 || f.nodeOf(t, "a") != "n2" {
		t.Fatalf("acted after 28 s: fenced %v", f.fenced)
	}
	f.advance(2 * time.Second)
	f.check(t)
	if !slices.Equal(f.fenced, []string{"n2"}) {
		t.Fatalf("fenced %v", f.fenced)
	}
	// Last answered 30 s ago: 30 s more until 60 s.
	if !slices.Equal(f.waited, []time.Duration{30 * time.Second}) {
		t.Fatalf("waited %v", f.waited)
	}
	for _, name := range []string{"a", "b"} {
		if node := f.nodeOf(t, name); node == "n2" {
			t.Fatalf("%s still runs on n2", name)
		}
	}
	if f.nodeOf(t, "lone") != "n2" {
		t.Fatal("a tenant without a replica moved")
	}
	// Failed: neither powered off nor failed over again.
	f.advance(2 * time.Second)
	f.check(t)
	if len(f.fenced) != 1 {
		t.Fatalf("fenced again: %v", f.fenced)
	}
	// Back: it keeps copies again.
	f.down["n2"] = false
	f.check(t)
	for _, node := range f.store.Nodes() {
		if node.Failed() {
			t.Fatalf("node %s still failed", node.Name)
		}
	}
}

// Without a confirmed power-off nothing runs elsewhere, and the metrics
// say why; nor without a provider at all.
func TestNoConfirmedPowerOffNoFailover(t *testing.T) {
	f := newFixture(t)
	f.fenceErr = errors.New("401 unauthorized")
	f.check(t)
	f.down["n2"] = true
	f.advance(31 * time.Second)
	f.check(t)
	if len(f.fenced) != 1 || f.nodeOf(t, "a") != "n2" {
		t.Fatalf("fenced %v, a on %s", f.fenced, f.nodeOf(t, "a"))
	}
	var page strings.Builder
	f.monitor.WriteMetrics(metrics.NewWriter(&page))
	if !strings.Contains(page.String(), `murder_failover_blocked{node="n2"} 1`) || !strings.Contains(page.String(), `murder_node_unanswered_seconds{node="n2"} 31`) {
		t.Fatalf("metrics:\n%s", page.String())
	}
	// Tried again a minute later, not at every probe.
	for range 29 {
		f.advance(2 * time.Second)
		f.check(t)
	}
	if len(f.fenced) != 1 {
		t.Fatalf("powered off %d times within a minute", len(f.fenced))
	}
	f.advance(2 * time.Second)
	f.check(t)
	if len(f.fenced) != 2 {
		t.Fatalf("powered off %d times after a minute", len(f.fenced))
	}
	f.monitor.Fence = nil
	f.advance(2 * time.Minute)
	f.check(t)
	if f.nodeOf(t, "a") != "n2" {
		t.Fatal("failed over without a provider")
	}
	// Once an operator failed it over, the alert ends: nothing waits.
	if _, err := ManualFailOver(context.Background(), f.store, f.monitor.Probe, "n2", "alice"); err != nil {
		t.Fatal(err)
	}
	f.advance(2 * time.Minute)
	f.check(t)
	page.Reset()
	f.monitor.WriteMetrics(metrics.NewWriter(&page))
	if !strings.Contains(page.String(), `murder_failover_blocked{node="n2"} 0`) {
		t.Fatalf("metrics after the operator's failover:\n%s", page.String())
	}
}

// Nothing fails over while changes are frozen for an upgrade, which
// restarts agents on purpose; once thawed, it does.
func TestNothingFailsOverDuringAnUpgrade(t *testing.T) {
	f := newFixture(t)
	f.check(t)
	if err := f.store.Freeze("upgrade", "test"); err != nil {
		t.Fatal(err)
	}
	f.down["n2"] = true
	f.advance(40 * time.Second)
	f.check(t)
	if len(f.fenced) != 0 {
		t.Fatalf("fenced during an upgrade: %v", f.fenced)
	}
	if err := f.store.Thaw("test"); err != nil {
		t.Fatal(err)
	}
	f.check(t)
	if len(f.fenced) != 1 || f.nodeOf(t, "a") == "n2" {
		t.Fatalf("after the upgrade: fenced %v", f.fenced)
	}
}

// Only the leader acts, and a new leader counts from its own first probe.
func TestOnlyTheLeaderActsAndANewOneCountsAfresh(t *testing.T) {
	f := newFixture(t)
	f.leader = false
	f.down["n2"] = true
	f.check(t)
	f.advance(60 * time.Second)
	f.check(t)
	if len(f.fenced) != 0 {
		t.Fatalf("a follower fenced: %v", f.fenced)
	}
	f.leader = true
	f.check(t)
	f.advance(20 * time.Second)
	f.check(t)
	if len(f.fenced) != 0 {
		t.Fatal("a new leader counted from before it led")
	}
	f.advance(10 * time.Second)
	f.check(t)
	if len(f.fenced) != 1 {
		t.Fatalf("fenced %v", f.fenced)
	}
}

// An operator's failover is refused while the node answers.
func TestAManualFailoverNeedsTheNodeSilent(t *testing.T) {
	f := newFixture(t)
	if _, err := ManualFailOver(context.Background(), f.store, f.monitor.Probe, "n2", "alice"); !errors.Is(err, store.ErrConflict) {
		t.Fatalf("a failover of a node that answers: %v", err)
	}
	f.down["n2"] = true
	outcome, err := ManualFailOver(context.Background(), f.store, f.monitor.Probe, "n2", "alice")
	if err != nil || len(outcome.Promoted) != 2 {
		t.Fatalf("failover: %+v, %v", outcome, err)
	}
}

// The Hetzner client finds the server by name, powers it off unless it is
// off, and returns once its status reads off.
func TestHetznerPowersOffAndConfirms(t *testing.T) {
	var mu sync.Mutex
	status := map[string]string{"murder-n2": "running", "murder-n3": "off"}
	var calls []string
	reads := 0
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		mu.Lock()
		defer mu.Unlock()
		calls = append(calls, r.Method+" "+r.URL.Path)
		if r.Header.Get("Authorization") != "Bearer secret" {
			w.WriteHeader(http.StatusUnauthorized)
			_, _ = io.WriteString(w, `{"error":{"code":"unauthorized","message":"unable to authenticate"}}`)
			return
		}
		ids := map[string]int{"murder-n2": 2, "murder-n3": 3}
		names := map[string]string{"2": "murder-n2", "3": "murder-n3"}
		switch {
		case r.URL.Path == "/servers":
			name := r.URL.Query().Get("name")
			if _, ok := ids[name]; !ok {
				_, _ = io.WriteString(w, `{"servers":[]}`)
				return
			}
			fmt.Fprintf(w, `{"servers":[{"id":%d,"name":%q,"status":%q,"private_net":[{"ip":"10.0.0.%d"}]}]}`, ids[name], name, status[name], ids[name])
		case strings.HasSuffix(r.URL.Path, "/actions/poweroff"):
			status[names[strings.Split(r.URL.Path, "/")[2]]] = "stopping"
			_, _ = io.WriteString(w, `{"action":{"id":1,"status":"running"}}`)
		default:
			name := names[strings.TrimPrefix(r.URL.Path, "/servers/")]
			reads++
			if reads == 2 {
				w.WriteHeader(http.StatusServiceUnavailable) // once: tried again
				return
			}
			if status[name] == "stopping" && reads >= 3 {
				status[name] = "off"
			}
			fmt.Fprintf(w, `{"server":{"id":%d,"name":%q,"status":%q}}`, ids[name], name, status[name])
		}
	}))
	defer server.Close()
	h := &Hetzner{Token: "secret", Endpoint: server.URL, Servers: map[string]string{"n2": "murder-n2", "n3": "murder-n3"}, Poll: time.Millisecond}
	n2, n3 := store.Node{Name: "n2", Address: "10.0.0.2:7400"}, store.Node{Name: "n3", Address: "10.0.0.3:7400"}
	if err := h.PowerOff(context.Background(), n2); err != nil {
		t.Fatal(err)
	}
	if status["murder-n2"] != "off" || !slices.Contains(calls, "POST /servers/2/actions/poweroff") {
		t.Fatalf("calls %v, status %v", calls, status)
	}
	calls = nil
	if off, err := h.Off(context.Background(), n3); err != nil || !off {
		t.Fatalf("off: %v, %v", off, err)
	}
	if err := h.PowerOff(context.Background(), n3); err != nil || slices.Contains(calls, "POST /servers/3/actions/poweroff") {
		t.Fatalf("a server off already: %v, %v", calls, err)
	}
	if err := h.PowerOff(context.Background(), store.Node{Name: "n9", Address: "10.0.0.9:7400"}); err == nil {
		t.Fatal("a node without a server was confirmed off")
	}
	// A server that does not have the node's address is not the node's.
	calls = nil
	if err := h.PowerOff(context.Background(), store.Node{Name: "n2", Address: "10.0.0.7:7400"}); err == nil || slices.ContainsFunc(calls, func(call string) bool { return strings.HasPrefix(call, "POST") }) {
		t.Fatalf("another machine's server: %v, %v", err, calls)
	}
	wrong := &Hetzner{Token: "wrong", Endpoint: server.URL, Servers: h.Servers}
	if err := wrong.PowerOff(context.Background(), n2); err == nil || !strings.Contains(err.Error(), "401") {
		t.Fatalf("a wrong token: %v", err)
	}
}

// A leader that hears too few nodes may be the one cut off: it powers no
// one off, rather than every other node in turn.
func TestALeaderThatHearsTooFewPowersNoOneOff(t *testing.T) {
	f := newFixture(t)
	f.check(t)
	f.down["n2"], f.down["n3"] = true, true
	f.advance(40 * time.Second)
	f.check(t)
	f.advance(40 * time.Second)
	f.check(t)
	if len(f.fenced) != 0 {
		t.Fatalf("fenced %v while hearing only itself", f.fenced)
	}
	var page strings.Builder
	f.monitor.WriteMetrics(metrics.NewWriter(&page))
	if !strings.Contains(page.String(), `murder_failover_blocked{node="n2"} 1`) {
		t.Fatalf("metrics:\n%s", page.String())
	}
}

// A node restarting on purpose, its agent stopped cleanly, is left alone
// until its away mark ends.
func TestANodeAwayIsLeftAlone(t *testing.T) {
	f := newFixture(t)
	f.check(t)
	if err := f.store.Away("n2", f.now().Add(10*time.Minute)); err != nil {
		t.Fatal(err)
	}
	f.down["n2"] = true
	f.advance(5 * time.Minute)
	f.check(t)
	if len(f.fenced) != 0 {
		t.Fatalf("a node away was powered off: %v", f.fenced)
	}
	// Left alone on purpose: no alert.
	var page strings.Builder
	f.monitor.WriteMetrics(metrics.NewWriter(&page))
	if !strings.Contains(page.String(), `murder_failover_blocked{node="n2"} 0`) {
		t.Fatalf("metrics:\n%s", page.String())
	}
	f.advance(6 * time.Minute)
	f.check(t)
	if len(f.fenced) != 1 {
		t.Fatalf("after its away mark: fenced %v", f.fenced)
	}
}

// A node that answers again, or is no longer off, before its tenants run
// elsewhere keeps them.
func TestANodeBackInTimeKeepsItsTenants(t *testing.T) {
	for name, back := range map[string]func(f *fixture){
		"answers": func(f *fixture) {
			f.down["n2"] = false
			f.monitor.Check(context.Background())
		},
		"powered on": func(f *fixture) { f.on = "n2" },
	} {
		t.Run(name, func(t *testing.T) {
			f := newFixture(t)
			f.check(t)
			f.down["n2"] = true
			f.onWait = func() { back(f) }
			f.advance(30 * time.Second)
			f.check(t)
			if len(f.fenced) != 1 || f.nodeOf(t, "a") != "n2" {
				t.Fatalf("fenced %v, a on %s", f.fenced, f.nodeOf(t, "a"))
			}
		})
	}
}

// A silent node whose tenants have no whole copy is not powered off:
// nothing could run instead. A pause keeps its copies; only after five
// minutes of silence are they given to other nodes.
func TestASilentNodeWithNothingToPromoteIsNotPoweredOff(t *testing.T) {
	f := newFixture(t)
	f.check(t)
	f.down["n3"] = true // runs nothing; keeps a's or b's copy
	f.advance(31 * time.Second)
	f.check(t)
	failed := func() bool {
		for _, node := range f.store.Nodes() {
			if node.Name == "n3" {
				return node.Failed()
			}
		}
		return false
	}
	if len(f.fenced) != 0 || failed() {
		t.Fatalf("after 31 s: fenced %v, failed %v", f.fenced, failed())
	}
	f.advance(5 * time.Minute)
	f.check(t)
	if len(f.fenced) != 0 || !failed() {
		t.Fatalf("after 5 min: fenced %v, failed %v", f.fenced, failed())
	}
	for _, record := range f.store.List() {
		if record.Replica == "n3" {
			t.Fatalf("tenant %s keeps its copy on a node silent for 5 min", record.Spec.Name)
		}
	}
}
