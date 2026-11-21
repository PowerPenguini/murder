package plugins

import (
	"bufio"
	"encoding/json"
	"errors"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"
)

func TestParseFillsInTheKindsDefaults(t *testing.T) {
	spec, err := Parse([]byte("kind: openai\nlimits:\n  - {metric: tokens, per: month, amount: 1000}\n"))
	if err != nil {
		t.Fatal(err)
	}
	if spec.Upstream != "https://api.openai.com" || spec.Limits[0].Mode != ModeHard {
		t.Fatalf("spec %+v", spec)
	}
	here, err := Parse([]byte("kind: here\n"))
	if err != nil || !here.AllowsHost("geocode.search.hereapi.com") || here.AllowsHost("geofencing.hereapi.com") {
		t.Fatalf("here %+v, %v", here, err)
	}
	for _, bad := range []string{
		"kind: gemini\n",
		"kind: openai\nupstream: ftp://x\n",
		"kind: openai\nupstream: https://api.openai.com/v1\n",
		"kind: openai\nlimits:\n  - {metric: transactions, per: day, amount: 1}\n",
		"kind: here\nlimits:\n  - {metric: requests, per: week, amount: 1}\n",
		"kind: here\nlimits:\n  - {metric: requests, per: day, amount: 1}\n  - {metric: requests, per: day, amount: 2}\n",
		"kind: here\nhosts: ['*.']\n",
		"kind: here\nupstream: https://x.example\n",
		"kind: openai\nkey: secret\n",
	} {
		if _, err := Parse([]byte(bad)); err == nil {
			t.Errorf("%q was taken", bad)
		}
	}
}

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

func TestAllowsHostMatchesNamesBelowTheDomainOnly(t *testing.T) {
	spec := Spec{Kind: KindHERE, Hosts: []string{"*.hereapi.com", "account.api.here.com"}}
	for host, want := range map[string]bool{
		"geocode.search.hereapi.com": true,
		"hereapi.com":                false,
		"evilhereapi.com":            false,
		"x.hereapi.com.evil.com":     false,
		"account.api.here.com":       true,
		"api.here.com":               false,
		"a_b.hereapi.com":            false,
	} {
		if spec.AllowsHost(host) != want {
			t.Errorf("%s: %v", host, !want)
		}
	}
}

func TestACounterSendsAReportAgainUntilTheAgentTookIt(t *testing.T) {
	counter := NewCounter("edge-1")
	now := time.Date(2026, 11, 20, 12, 0, 0, 0, time.UTC)
	day := Day(now)
	counter.Add(Count{Tenant: "acme", Plugin: "openai", Day: day, Metric: MetricTokens, Detail: "gpt", Amount: 10})
	first, ok := counter.Next()
	if !ok || first.Seq != 1 || len(first.Counts) != 1 {
		t.Fatalf("first %+v", first)
	}
	counter.Add(Count{Tenant: "acme", Plugin: "openai", Day: day, Metric: MetricTokens, Detail: "gpt", Amount: 5})
	// Not taken: sent again as it was.
	again, _ := counter.Next()
	if again.Seq != 1 || again.Counts[0].Amount != 10 {
		t.Fatalf("again %+v", again)
	}
	if used := counter.Used("acme", "openai", MetricTokens, PerDay, now); used != 15 {
		t.Fatalf("used %d before the agent took any", used)
	}
	// Taken: the agent's total holds it, and what came since is the next.
	counter.Took(Totals{Edge: "edge-1", Seq: 1, Totals: []Total{{Tenant: "acme", Plugin: "openai", Period: day, Metric: MetricTokens, Amount: 110}}})
	if used := counter.Used("acme", "openai", MetricTokens, PerDay, now); used != 115 {
		t.Fatalf("used %d after the agent took the first", used)
	}
	if used := counter.Used("acme", "openai", MetricTokens, PerMonth, now); used != 5 {
		t.Fatalf("used %d this month: the agent named no month", used)
	}
	next, _ := counter.Next()
	if next.Seq != 2 || next.Counts[0].Amount != 5 {
		t.Fatalf("next %+v", next)
	}
	// Another edge's answer, or an older one, changes nothing.
	counter.Took(Totals{Edge: "edge-0", Seq: 9})
	counter.Took(Totals{Edge: "edge-1", Seq: 0})
	if used := counter.Used("acme", "openai", MetricTokens, PerDay, now); used != 115 {
		t.Fatalf("used %d after foreign answers", used)
	}
	counter.Took(Totals{Edge: "edge-1", Seq: 2, Totals: []Total{{Tenant: "acme", Plugin: "openai", Period: day, Metric: MetricTokens, Amount: 115}}})
	if _, ok := counter.Next(); ok {
		t.Fatal("a report with nothing in it")
	}
}

// fixture is a proxy for tenant acme at 127.0.0.1, with a plugin of the
// kind given reaching upstream.
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
	if plugin.Kind == KindOpenAI {
		plugin.Upstream = upstream.URL
	}
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
	// The upstream is plain HTTP, and HERE's hosts are reached through it.
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

func TestOpenAITakesTheTenantsKeyOffAndCountsTheUsage(t *testing.T) {
	var seen http.Header
	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		seen = r.Header.Clone()
		if r.URL.Path != "/v1/chat/completions" {
			t.Errorf("path %s", r.URL.Path)
		}
		w.Header().Set("Content-Type", "application/json")
		io.WriteString(w, `{"id":"x","model":"gpt-5-2026","choices":[],"usage":{"prompt_tokens":12,"completion_tokens":30,"total_tokens":42,"prompt_tokens_details":{"cached_tokens":4}}}`)
	}))
	defer upstream.Close()
	f := newFixture(t, Spec{Kind: KindOpenAI}, Grant{}, upstream)
	request, _ := http.NewRequest(http.MethodPost, f.server.URL+"/p/v1/chat/completions", strings.NewReader(`{"model":"gpt-5","messages":[]}`))
	request.Header.Set("Content-Type", "application/json")
	request.Header.Set("Authorization", "Bearer anything")
	request.Header.Set("OpenAI-Organization", "theirs")
	response, err := http.DefaultClient.Do(request)
	if err != nil {
		t.Fatal(err)
	}
	body, _ := io.ReadAll(response.Body)
	response.Body.Close()
	if response.StatusCode != 200 || !strings.Contains(string(body), `"total_tokens":42`) {
		t.Fatalf("%d %s", response.StatusCode, body)
	}
	if seen.Get("Authorization") != "Bearer operator-key" || seen.Get("Openai-Organization") != "" {
		t.Fatalf("upstream saw %v", seen)
	}
	for metric, want := range map[string]int64{MetricRequests: 1, MetricInputTokens: 12, MetricOutputTokens: 30, MetricTokens: 42, MetricCachedTokens: 4, MetricEstimatedTokens: 0} {
		if got := f.used(metric); got != want {
			t.Errorf("%s: %d, not %d", metric, got, want)
		}
	}
	report, _ := f.counter.Next()
	for _, count := range report.Counts {
		if count.Metric != MetricRequests && count.Detail != "gpt-5-2026" {
			t.Errorf("%s counted for %q, not the model that answered", count.Metric, count.Detail)
		}
	}
}

func TestATenantsOwnKeyIsUsedInsteadOfTheOperators(t *testing.T) {
	var auth string
	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		auth = r.Header.Get("Authorization")
		w.Header().Set("Content-Type", "application/json")
		io.WriteString(w, `{}`)
	}))
	defer upstream.Close()
	f := newFixture(t, Spec{Kind: KindOpenAI}, Grant{Key: "sealed-own"}, upstream)
	response, err := http.Get(f.server.URL + "/p/v1/models")
	if err != nil {
		t.Fatal(err)
	}
	response.Body.Close()
	if response.StatusCode != 200 || auth != "Bearer own-key" {
		t.Fatalf("%d, upstream saw %q", response.StatusCode, auth)
	}
}

// chatStream answers a streamed chat completion, with its usage chunk if
// asked for, and records whether it was.
func chatStream(asked *bool) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		var request struct {
			StreamOptions struct {
				IncludeUsage bool `json:"include_usage"`
			} `json:"stream_options"`
			Temperature float64 `json:"temperature"`
		}
		_ = json.NewDecoder(r.Body).Decode(&request)
		*asked = request.StreamOptions.IncludeUsage && request.Temperature == 0.5
		w.Header().Set("Content-Type", "text/event-stream")
		for _, word := range []string{"Hel", "lo"} {
			io.WriteString(w, `data: {"model":"gpt-5","choices":[{"delta":{"content":"`+word+`"}}],"usage":null}`+"\n\n")
			w.(http.Flusher).Flush()
		}
		if request.StreamOptions.IncludeUsage {
			io.WriteString(w, `data: {"model":"gpt-5","choices":[],"usage":{"prompt_tokens":7,"completion_tokens":2}}`+"\n\n")
		}
		io.WriteString(w, "data: [DONE]\n\n")
	}
}

func streamed(t *testing.T, f *fixture, body string) []string {
	t.Helper()
	request, _ := http.NewRequest(http.MethodPost, f.server.URL+"/p/v1/chat/completions", strings.NewReader(body))
	request.Header.Set("Content-Type", "application/json")
	response, err := http.DefaultClient.Do(request)
	if err != nil {
		t.Fatal(err)
	}
	defer response.Body.Close()
	var events []string
	scanner := bufio.NewScanner(response.Body)
	for scanner.Scan() {
		if line := scanner.Text(); line != "" {
			events = append(events, line)
		}
	}
	return events
}

func TestAStreamIsAskedForItsUsageWhichTheAppDidNotAskFor(t *testing.T) {
	var asked bool
	upstream := httptest.NewServer(chatStream(&asked))
	defer upstream.Close()
	f := newFixture(t, Spec{Kind: KindOpenAI}, Grant{}, upstream)
	events := streamed(t, f, `{"model":"gpt-5","stream":true,"temperature":0.5,"messages":[]}`)
	if !asked {
		t.Fatal("the stream was not asked for its usage, or the rest of the request changed")
	}
	if len(events) != 3 || strings.Contains(strings.Join(events, "\n"), `"prompt_tokens"`) || events[2] != "data: [DONE]" {
		t.Fatalf("the app got %q", events)
	}
	if f.used(MetricTokens) != 9 || f.used(MetricEstimatedTokens) != 0 {
		t.Fatalf("tokens %d, estimated %d", f.used(MetricTokens), f.used(MetricEstimatedTokens))
	}
	// An app that asked gets the chunk.
	events = streamed(t, f, `{"model":"gpt-5","stream":true,"temperature":0.5,"stream_options":{"include_usage":true},"messages":[]}`)
	if len(events) != 4 || !strings.Contains(events[2], `"prompt_tokens":7`) {
		t.Fatalf("the app that asked got %q", events)
	}
	if f.used(MetricTokens) != 18 {
		t.Fatalf("tokens %d", f.used(MetricTokens))
	}
}

func TestAStreamThatEndsWithoutUsageIsEstimated(t *testing.T) {
	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "text/event-stream")
		io.WriteString(w, `data: {"type":"response.output_text.delta","delta":"`+strings.Repeat("x", 400)+`"}`+"\n\n")
		// The stream breaks off: no response.completed.
	}))
	defer upstream.Close()
	f := newFixture(t, Spec{Kind: KindOpenAI}, Grant{}, upstream)
	body := `{"model":"gpt-5","stream":true,"input":"` + strings.Repeat("y", 77) + `"}`
	request, _ := http.NewRequest(http.MethodPost, f.server.URL+"/p/v1/responses", strings.NewReader(body))
	request.Header.Set("Content-Type", "application/json")
	response, err := http.DefaultClient.Do(request)
	if err != nil {
		t.Fatal(err)
	}
	io.Copy(io.Discard, response.Body)
	response.Body.Close()
	wantInput, wantOutput := int64((len(body)+3)/4), int64(100)
	if f.used(MetricInputTokens) != wantInput || f.used(MetricOutputTokens) != wantOutput || f.used(MetricEstimatedTokens) != wantInput+wantOutput {
		t.Fatalf("input %d, output %d, estimated %d", f.used(MetricInputTokens), f.used(MetricOutputTokens), f.used(MetricEstimatedTokens))
	}
}

func TestTheResponsesAPIsCompletedEventCarriesTheUsage(t *testing.T) {
	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "text/event-stream")
		io.WriteString(w, "event: response.output_text.delta\n"+`data: {"type":"response.output_text.delta","delta":"Hi"}`+"\n\n")
		io.WriteString(w, "event: response.completed\n"+`data: {"type":"response.completed","response":{"model":"gpt-5-x","output":[{"content":"`+strings.Repeat("z", 100<<10)+`"}],"usage":{"input_tokens":20,"output_tokens":5,"input_tokens_details":{"cached_tokens":3}}}}`+"\n\n")
	}))
	defer upstream.Close()
	f := newFixture(t, Spec{Kind: KindOpenAI}, Grant{}, upstream)
	request, _ := http.NewRequest(http.MethodPost, f.server.URL+"/p/v1/responses", strings.NewReader(`{"model":"gpt-5","stream":true}`))
	request.Header.Set("Content-Type", "application/json")
	response, err := http.DefaultClient.Do(request)
	if err != nil {
		t.Fatal(err)
	}
	got, _ := io.ReadAll(response.Body)
	response.Body.Close()
	if !strings.Contains(string(got), "response.completed") || len(got) < 100<<10 {
		t.Fatalf("the app got %d bytes", len(got))
	}
	if f.used(MetricTokens) != 25 || f.used(MetricCachedTokens) != 3 {
		t.Fatalf("tokens %d, cached %d", f.used(MetricTokens), f.used(MetricCachedTokens))
	}
}

func TestTheUsageOfALargeAnswerIsReadFromItsTail(t *testing.T) {
	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		io.WriteString(w, `{"object":"list","model":"text-embedding-3-small","data":[`)
		for i := range 40000 {
			if i > 0 {
				io.WriteString(w, ",")
			}
			io.WriteString(w, `{"embedding":[0.0123,0.0456,0.0789,0.1011,0.1213]}`)
		}
		io.WriteString(w, `],"usage":{"prompt_tokens":5000,"total_tokens":5000}}`)
	}))
	defer upstream.Close()
	f := newFixture(t, Spec{Kind: KindOpenAI}, Grant{}, upstream)
	request, _ := http.NewRequest(http.MethodPost, f.server.URL+"/p/v1/embeddings", strings.NewReader(`{"model":"text-embedding-3-small","input":["a"]}`))
	request.Header.Set("Content-Type", "application/json")
	response, err := http.DefaultClient.Do(request)
	if err != nil {
		t.Fatal(err)
	}
	io.Copy(io.Discard, response.Body)
	response.Body.Close()
	if f.used(MetricInputTokens) != 5000 || f.used(MetricOutputTokens) != 0 {
		t.Fatalf("input %d, output %d", f.used(MetricInputTokens), f.used(MetricOutputTokens))
	}
}

func TestAHardLimitRefusesTheNextRequestAndASoftOneDoesNot(t *testing.T) {
	calls := 0
	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		calls++
		w.Header().Set("Content-Type", "application/json")
		io.WriteString(w, `{"model":"m","usage":{"prompt_tokens":60,"completion_tokens":40}}`)
	}))
	defer upstream.Close()
	f := newFixture(t, Spec{Kind: KindOpenAI}, Grant{Limits: []Limit{{Metric: MetricTokens, Per: PerMonth, Amount: 150, Mode: ModeHard}, {Metric: MetricRequests, Per: PerDay, Amount: 1, Mode: ModeSoft}}}, upstream)
	ask := func() *http.Response {
		request, _ := http.NewRequest(http.MethodPost, f.server.URL+"/p/v1/chat/completions", strings.NewReader(`{"model":"m"}`))
		request.Header.Set("Content-Type", "application/json")
		response, err := http.DefaultClient.Do(request)
		if err != nil {
			t.Fatal(err)
		}
		io.Copy(io.Discard, response.Body)
		response.Body.Close()
		return response
	}
	// 100 then 200 tokens: the second goes past the limit, but was let
	// through, as its cost was not known before.
	for range 2 {
		if response := ask(); response.StatusCode != 200 {
			t.Fatalf("status %d before the limit", response.StatusCode)
		}
	}
	response := ask()
	if response.StatusCode != http.StatusTooManyRequests || response.Header.Get("X-Murder-Plugin-Error") != "murder_limit_reached" || response.Header.Get("Retry-After") == "" {
		t.Fatalf("status %d, %v", response.StatusCode, response.Header)
	}
	if calls != 2 {
		t.Fatalf("upstream called %d times", calls)
	}
}

func TestOnlyTheTenantsOfTheNodeReachTheirGrantedPlugins(t *testing.T) {
	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {}))
	defer upstream.Close()
	f := newFixture(t, Spec{Kind: KindOpenAI}, Grant{}, upstream)
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

func TestHEREGetsTheKeyInItsQueryAndCountsTransactions(t *testing.T) {
	var seen []*url.URL
	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		seen = append(seen, &url.URL{Host: r.Host, Path: r.URL.Path, RawQuery: r.URL.RawQuery})
		switch r.Host {
		case "geocode.search.hereapi.com":
			w.Header().Set("Location", "https://geocode.search.hereapi.com/v1/geocode?apiKey=operator-key")
			io.WriteString(w, `{"items":[]}`)
		case "matrix.router.hereapi.com":
			io.WriteString(w, `{"matrix":{}}`)
		default:
			w.WriteHeader(http.StatusUnauthorized)
			io.WriteString(w, `{"error":"bad key operator-key for `+r.URL.String()+`"}`)
		}
	}))
	defer upstream.Close()
	f := newFixture(t, Spec{Kind: KindHERE}, Grant{}, upstream)
	response, err := http.Get(f.server.URL + "/p/geocode.search.hereapi.com/v1/geocode?q=Berlin&apiKey=theirs&apikey=theirs")
	if err != nil {
		t.Fatal(err)
	}
	response.Body.Close()
	if response.StatusCode != 200 || strings.Contains(response.Header.Get("Location"), "operator-key") {
		t.Fatalf("%d, Location %q", response.StatusCode, response.Header.Get("Location"))
	}
	query := seen[0].Query()
	if seen[0].Host != "geocode.search.hereapi.com" || seen[0].Path != "/v1/geocode" || query.Get("apiKey") != "operator-key" || query.Get("apikey") != "" || query.Get("q") != "Berlin" {
		t.Fatalf("upstream saw %s", seen[0])
	}
	matrix := `{"origins":[{},{},{},{},{},{}],"destinations":[{},{},{},{},{},{},{}]}`
	response, err = http.Post(f.server.URL+"/p/matrix.router.hereapi.com/v8/matrix?async=false", "application/json", strings.NewReader(matrix))
	if err != nil {
		t.Fatal(err)
	}
	response.Body.Close()
	response, err = http.Get(f.server.URL + "/p/router.hereapi.com/v8/routes")
	if err != nil {
		t.Fatal(err)
	}
	body, _ := io.ReadAll(response.Body)
	response.Body.Close()
	if response.StatusCode != 401 || strings.Contains(string(body), "operator-key") {
		t.Fatalf("an error told the key: %s", body)
	}
	// One for the geocode, 5 x 7 for the matrix, none for the refusal.
	if f.used(MetricRequests) != 3 || f.used(MetricTransactions) != 36 {
		t.Fatalf("requests %d, transactions %d", f.used(MetricRequests), f.used(MetricTransactions))
	}
	report, _ := f.counter.Next()
	services := map[string]bool{}
	for _, count := range report.Counts {
		services[count.Detail] = true
	}
	if !services["geocode.search"] || !services["matrix.router"] || !services["router"] {
		t.Fatalf("services %v", services)
	}
	for _, path := range []string{"/p/evil.com/x", "/p/hereapi.com/x", "/p/geocode.search.hereapi.com/../x"} {
		response, err := http.Get(f.server.URL + path)
		if err != nil {
			t.Fatal(err)
		}
		response.Body.Close()
		if response.StatusCode != http.StatusBadRequest {
			t.Errorf("%s: %d", path, response.StatusCode)
		}
	}
}

func TestHERETransactionsFollowItsPricing(t *testing.T) {
	cases := []struct {
		host  string
		query url.Values
		body  string
		want  int64
	}{
		{"geocode.search.hereapi.com", nil, "", 1},
		{"matrix.router.hereapi.com", nil, `{"origins":[{},{},{}],"destinations":[{},{}]}`, 6},
		{"matrix.router.hereapi.com", nil, `{"origins":[{},{},{},{}]}`, 16},
		{"matrix.router.hereapi.com", nil, `{"origins":[{},{},{},{},{}],"destinations":[{},{},{},{},{},{},{},{},{},{}]}`, 50},
		{"isoline.router.hereapi.com", url.Values{"range[values]": {"300,600,900"}}, "", 3},
		{"multi-revgeocode.search.hereapi.com", nil, "id=1&at=1,2\nid=2&at=3,4\n", 2},
		{"wps.hereapi.com", url.Values{"start": {"a"}, "destination1": {"b"}, "end": {"c"}}, "", 1},
	}
	for _, c := range cases {
		if got := hereTransactions(c.host, c.query, []byte(c.body)); got != c.want {
			t.Errorf("%s %v %s: %d, not %d", c.host, c.query, c.body, got, c.want)
		}
	}
}

func TestOnlyTheEndpointsWhoseUseIsCountedGoOn(t *testing.T) {
	calls := 0
	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { calls++ }))
	defer upstream.Close()
	f := newFixture(t, Spec{Kind: KindOpenAI, Allow: []string{"GET /v1/batches/*"}}, Grant{}, upstream)
	for _, c := range []struct {
		method, path string
		want         int
	}{
		{http.MethodGet, "/p/v1/files", http.StatusForbidden},
		{http.MethodPost, "/p/v1/files", http.StatusForbidden},
		{http.MethodGet, "/p/v1/chat/completions", http.StatusForbidden},
		{http.MethodGet, "/p/v1/responses/resp_1", http.StatusForbidden},
		{http.MethodPost, "/p/v1/audio/speech", http.StatusForbidden},
		{http.MethodGet, "/p/v1/batches", http.StatusForbidden},
		{http.MethodGet, "/p/v1/batches/b1/cancel", http.StatusForbidden},
		{http.MethodGet, "/p/v1/batches/b1", http.StatusOK},
		{http.MethodGet, "/p/v1/models/gpt-5", http.StatusOK},
	} {
		request, _ := http.NewRequest(c.method, f.server.URL+c.path, nil)
		response, err := http.DefaultClient.Do(request)
		if err != nil {
			t.Fatal(err)
		}
		response.Body.Close()
		if response.StatusCode != c.want {
			t.Errorf("%s %s: %d, not %d", c.method, c.path, response.StatusCode, c.want)
		}
	}
	request, _ := http.NewRequest(http.MethodGet, f.server.URL+"/p/v1/models", nil)
	request.Header.Set("Connection", "Upgrade")
	request.Header.Set("Upgrade", "websocket")
	response, err := http.DefaultClient.Do(request)
	if err != nil {
		t.Fatal(err)
	}
	response.Body.Close()
	if response.StatusCode != http.StatusBadRequest || calls != 2 {
		t.Fatalf("a WebSocket: %d, upstream called %d times", response.StatusCode, calls)
	}
}

func TestAModelATenantMakesUpIsNoDetailOfItsOwn(t *testing.T) {
	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "text/event-stream")
		io.WriteString(w, `data: {"choices":[{"delta":{"content":"hi"}}]}`+"\n\n")
	}))
	defer upstream.Close()
	f := newFixture(t, Spec{Kind: KindOpenAI}, Grant{}, upstream)
	model := strings.Repeat("m", 200)
	request, _ := http.NewRequest(http.MethodPost, f.server.URL+"/p/v1/chat/completions", strings.NewReader(`{"model":"`+model+`","stream":true}`))
	request.Header.Set("Content-Type", "application/json")
	response, err := http.DefaultClient.Do(request)
	if err != nil {
		t.Fatal(err)
	}
	io.Copy(io.Discard, response.Body)
	response.Body.Close()
	report, _ := f.counter.Next()
	for _, count := range report.Counts {
		if count.Detail != "" && count.Detail != "other" {
			t.Errorf("%s counted for %q", count.Metric, count.Detail)
		}
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

func TestHEREGetsTheQueryAsItWasWritten(t *testing.T) {
	var raw string
	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { raw = r.URL.RawQuery }))
	defer upstream.Close()
	f := newFixture(t, Spec{Kind: KindHERE}, Grant{}, upstream)
	response, err := http.Get(f.server.URL + "/p/geocode.search.hereapi.com/v1/geocode?qq=city=Berlin;country=Germany&apiKey=theirs&lang=de")
	if err != nil {
		t.Fatal(err)
	}
	response.Body.Close()
	if raw != "qq=city=Berlin;country=Germany&lang=de&apiKey=operator-key" {
		t.Fatalf("upstream got %q", raw)
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
func TestABodyTakesRoomAsItComes(t *testing.T) {
	var got int
	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		body, _ := io.ReadAll(r.Body)
		got = len(body)
		w.Header().Set("Content-Type", "application/json")
		io.WriteString(w, `{}`)
	}))
	defer upstream.Close()
	f := newFixture(t, Spec{Kind: KindOpenAI}, Grant{}, upstream)
	body := `{"model":"m","input":"` + strings.Repeat("x", 3*bodyChunk) + `"}`
	request, _ := http.NewRequest(http.MethodPost, f.server.URL+"/p/v1/embeddings", strings.NewReader(body))
	request.Header.Set("Content-Type", "application/json")
	response, err := http.DefaultClient.Do(request)
	if err != nil {
		t.Fatal(err)
	}
	response.Body.Close()
	if response.StatusCode != 200 || got != len(body) {
		t.Fatalf("%d, upstream got %d of %d bytes", response.StatusCode, got, len(body))
	}
	// The room is given back as the handler returns, which may be just
	// after the tenant has the answer.
	for deadline := time.Now().Add(5 * time.Second); ; time.Sleep(10 * time.Millisecond) {
		f.proxy.mu.Lock()
		held := f.proxy.buffered
		f.proxy.mu.Unlock()
		if held == 0 {
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("%d bytes still held", held)
		}
	}
}

// A provider's trailers, which could say the key, are not passed on; nor
// is a count past what the agent keeps.
func TestTrailersStayAndCountsStopAtTheirBound(t *testing.T) {
	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Trailer", "X-Echo")
		w.Header().Set("Content-Type", "application/json")
		io.WriteString(w, `{"object":"list","data":[]}`)
		w.(http.Flusher).Flush()
		w.Header().Set("X-Echo", r.Header.Get("Authorization"))
	}))
	defer upstream.Close()
	f := newFixture(t, Spec{Kind: KindOpenAI}, Grant{}, upstream)
	response, err := http.Get(f.server.URL + "/p/v1/models")
	if err != nil {
		t.Fatal(err)
	}
	body, _ := io.ReadAll(response.Body)
	response.Body.Close()
	if response.StatusCode != http.StatusOK || len(body) == 0 || len(response.Trailer) != 0 {
		t.Fatalf("%d, trailers %v", response.StatusCode, response.Trailer)
	}
	counter := NewCounter("e")
	day := Day(time.Now())
	counter.Add(Count{Tenant: "a", Plugin: "p", Day: day, Metric: "m", Amount: MaxCount}, Count{Tenant: "a", Plugin: "p", Day: day, Metric: "m", Amount: MaxCount})
	if report, _ := counter.Next(); report.Counts[0].Amount != MaxCount {
		t.Fatalf("a count past its bound: %+v", report.Counts)
	}
}
