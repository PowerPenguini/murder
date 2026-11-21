package plugins

import (
	"errors"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strconv"
	"sync"
	"testing"
	"time"
)

func TestParseLimitTakesSuffixesAndModes(t *testing.T) {
	limit, err := ParseLimit("tokens=5M/month")
	if err != nil || limit != (Limit{Metric: "tokens", Per: PerMonth, Amount: 5_000_000, Mode: ModeHard}) {
		t.Fatalf("%+v, %v", limit, err)
	}
	limit, err = ParseLimit("requests=1000/day:soft")
	if err != nil || limit.Amount != 1000 || limit.Hard() || limit.String() != "requests=1000/day:soft" {
		t.Fatalf("%+v, %v", limit, err)
	}
	for _, bad := range []string{"tokens", "tokens=5M", "tokens=-1/day", "tokens=xM/day", "tokens=99999999999G/day"} {
		if _, err := ParseLimit(bad); err == nil {
			t.Errorf("%q was taken", bad)
		}
	}
}

func TestACounterSendsAReportAgainUntilTheAgentTookIt(t *testing.T) {
	counter := NewCounter("edge-1")
	now := time.Date(2026, 11, 20, 12, 0, 0, 0, time.UTC)
	day := Day(now)
	counter.Add(Count{Tenant: "acme", Plugin: "openai", Day: day, Metric: "tokens", Detail: "gpt", Amount: 10})
	first, ok := counter.Next()
	if !ok || first.Seq != 1 || len(first.Counts) != 1 {
		t.Fatalf("first %+v", first)
	}
	counter.Add(Count{Tenant: "acme", Plugin: "openai", Day: day, Metric: "tokens", Detail: "gpt", Amount: 5})
	// Not taken: sent again as it was.
	again, _ := counter.Next()
	if again.Seq != 1 || again.Counts[0].Amount != 10 {
		t.Fatalf("again %+v", again)
	}
	if used := counter.Used("acme", "openai", "tokens", PerDay, now); used != 15 {
		t.Fatalf("used %d before the agent took any", used)
	}
	// Taken: the agent's total holds it, and what came since is the next.
	counter.Took(Totals{Edge: "edge-1", Seq: 1, Totals: []Total{{Tenant: "acme", Plugin: "openai", Period: day, Metric: "tokens", Amount: 110}}})
	if used := counter.Used("acme", "openai", "tokens", PerDay, now); used != 115 {
		t.Fatalf("used %d after the agent took the first", used)
	}
	if used := counter.Used("acme", "openai", "tokens", PerMonth, now); used != 5 {
		t.Fatalf("used %d this month: the agent named no month", used)
	}
	next, _ := counter.Next()
	if next.Seq != 2 || next.Counts[0].Amount != 5 {
		t.Fatalf("next %+v", next)
	}
	// Another edge's answer, or an older one, changes nothing.
	counter.Took(Totals{Edge: "edge-0", Seq: 9})
	counter.Took(Totals{Edge: "edge-1", Seq: 0})
	if used := counter.Used("acme", "openai", "tokens", PerDay, now); used != 115 {
		t.Fatalf("used %d after foreign answers", used)
	}
	counter.Took(Totals{Edge: "edge-1", Seq: 2, Totals: []Total{{Tenant: "acme", Plugin: "openai", Period: day, Metric: "tokens", Amount: 115}}})
	if _, ok := counter.Next(); ok {
		t.Fatal("a report with nothing in it")
	}
}

// fixture is a proxy for tenant acme at 127.0.0.1, with a plugin p
// reaching upstream.
type fixture struct {
	proxy   *Proxy
	counter *Counter
	server  *httptest.Server
	config  Config
	mu      sync.Mutex
}

func newFixture(t *testing.T, plugin Spec, grant Grant, upstream *httptest.Server) *fixture {
	t.Helper()
	f := &fixture{counter: NewCounter("edge-1")}
	f.config = Config{
		Plugins: map[string]Plugin{"p": {Spec: plugin.WithDefaults(), Key: "sealed-operator"}},
		Tenants: map[string]Tenant{"acme": {Address: "127.0.0.1", Grants: map[string]Grant{"p": grant}}},
	}
	open := func(context, sealed string) (string, error) {
		switch {
		case sealed == "sealed-operator" && context == KeyContext("p"):
			return "operator-key", nil
		case sealed == "sealed-own" && context == TenantKeyContext("p", "acme"):
			return "own-key", nil
		}
		return "", errors.New("wrong context")
	}
	f.proxy = NewProxy(func() Config { f.mu.Lock(); defer f.mu.Unlock(); return f.config }, open, f.counter, slog.New(slog.DiscardHandler))
	// The upstream is plain HTTP, and every host is reached through it.
	target, _ := url.Parse(upstream.URL)
	f.proxy.Transport = toUpstream{target: target.Host, next: upstream.Client().Transport}
	f.server = httptest.NewServer(f.proxy)
	t.Cleanup(f.server.Close)
	return f
}

// toUpstream sends every request to the test's upstream, over plain
// HTTP, with the host it was for.
type toUpstream struct {
	target string
	next   http.RoundTripper
}

func (u toUpstream) RoundTrip(r *http.Request) (*http.Response, error) {
	r = r.Clone(r.Context())
	r.Host, r.URL.Scheme, r.URL.Host = r.URL.Host, "http", u.target
	return u.next.RoundTrip(r)
}

// used is what the counter holds of a metric, today.
func (f *fixture) used(metric string) int64 {
	return f.counter.Used("acme", "p", metric, PerDay, time.Now())
}

func TestOnlyTheTenantsOfTheNodeReachTheirGrantedPlugins(t *testing.T) {
	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {}))
	defer upstream.Close()
	f := newFixture(t, Spec{Kind: KindContainer, Image: testImage}, Grant{}, upstream)
	cases := map[string]int{"/q/v1/models": http.StatusNotFound}
	for path, want := range cases {
		response, err := http.Get(f.server.URL + path)
		if err != nil {
			t.Fatal(err)
		}
		response.Body.Close()
		if response.StatusCode != want {
			t.Errorf("%s: %d", path, response.StatusCode)
		}
	}
	f.mu.Lock()
	f.config.Tenants["acme"] = Tenant{Address: "127.0.0.1", Grants: map[string]Grant{}}
	f.mu.Unlock()
	response, _ := http.Get(f.server.URL + "/p/v1/models")
	response.Body.Close()
	if response.StatusCode != http.StatusForbidden {
		t.Fatalf("a plugin the app does not list: %d", response.StatusCode)
	}
	f.mu.Lock()
	f.config.Tenants = map[string]Tenant{"other": {Address: "127.0.0.2", Grants: map[string]Grant{"p": {}}}}
	f.mu.Unlock()
	response, _ = http.Get(f.server.URL + "/p/v1/models")
	response.Body.Close()
	if response.StatusCode != http.StatusForbidden {
		t.Fatalf("an address of no tenant: %d", response.StatusCode)
	}
	if f.used(MetricRequests) != 0 {
		t.Fatal("refused requests were counted")
	}
}

func TestACounterKeepsItsReportsBounded(t *testing.T) {
	counter := NewCounter("e")
	for i := range maxPending + 100 {
		counter.Add(Count{Tenant: "acme", Plugin: "p", Day: "2026-11-20", Metric: MetricRequests, Detail: strconv.Itoa(i), Amount: 1})
	}
	report, _ := counter.Next()
	if len(report.Counts) != maxPending+1 {
		t.Fatalf("%d counts", len(report.Counts))
	}
	counter.Drop(report.Seq)
	if _, ok := counter.Next(); ok {
		t.Fatal("a dropped report is sent again")
	}
}

func TestOneTenantsBodiesLeaveRoomForTheOthers(t *testing.T) {
	p := NewProxy(func() Config { return Config{} }, nil, NewCounter("e"), slog.New(slog.DiscardHandler))
	for p.reserve("acme", bodyChunk) {
	}
	if p.byTenant["acme"] != MaxBufferedPerTenant {
		t.Fatalf("acme holds %d", p.byTenant["acme"])
	}
	if !p.reserve("beta", bodyChunk) {
		t.Fatal("beta finds no room while acme holds its share")
	}
	p.unreserve("acme", MaxBufferedPerTenant)
	p.unreserve("beta", bodyChunk)
	if p.buffered != 0 || len(p.byTenant) != 0 {
		t.Fatalf("left %d, %v", p.buffered, p.byTenant)
	}
}

// A body is read in room taken as it comes, not as it says it is long.
