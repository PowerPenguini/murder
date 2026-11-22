package plugins

import (
	"bufio"
	"encoding/json"
	"io"
	"net/http"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"
)

// The tests of Murder's own plugins, openai and here, as the edge runs
// them: each plugin's program on its socket, as its container would, and
// the edge between the tenant and the provider.

func post(t *testing.T, w *containerFixture, path, contentType, body string) (*http.Response, string) {
	t.Helper()
	request, _ := http.NewRequest(http.MethodPost, w.server.URL+"/p"+path, strings.NewReader(body))
	if contentType != "" {
		request.Header.Set("Content-Type", contentType)
	}
	response := w.as(t, "127.0.0.1", request)
	got, _ := io.ReadAll(response.Body)
	return response, string(got)
}

func TestOpenAITakesTheTenantsKeyOffAndCountsTheUsage(t *testing.T) {
	w := newPluginFixture(t, "openai", Grant{}, func(rw http.ResponseWriter, r *http.Request) {
		rw.Header().Set("Content-Type", "application/json")
		io.WriteString(rw, `{"id":"x","model":"gpt-5-2026","choices":[],"usage":{"prompt_tokens":12,"completion_tokens":30,"total_tokens":42,"prompt_tokens_details":{"cached_tokens":4}}}`)
	}, nil)
	request, _ := http.NewRequest(http.MethodPost, w.server.URL+"/p/v1/chat/completions", strings.NewReader(`{"model":"gpt-5","messages":[]}`))
	request.Header.Set("Content-Type", "application/json")
	request.Header.Set("Authorization", "Bearer anything")
	request.Header.Set("OpenAI-Organization", "theirs")
	response := w.as(t, "127.0.0.1", request)
	body, _ := io.ReadAll(response.Body)
	if response.StatusCode != 200 || !strings.Contains(string(body), `"total_tokens":42`) {
		t.Fatalf("%d %s", response.StatusCode, body)
	}
	seen := w.upstream(t)
	if seen.Host != "api.openai.com" || seen.URL.Path != "/v1/chat/completions" || seen.Header.Get("Authorization") != "Bearer operator-key" || seen.Header.Get("Openai-Organization") != "" {
		t.Fatalf("upstream saw %s %s %v", seen.Host, seen.URL.Path, seen.Header)
	}
	for metric, want := range map[string]int64{MetricRequests: 1, "tokens.input": 12, "tokens.output": 30, "tokens": 42, "tokens.cached": 4, "tokens.estimated": 0} {
		if got := w.used(metric); got != want {
			t.Errorf("%s: %d, not %d", metric, got, want)
		}
	}
	report, _ := w.counter.Next()
	for _, count := range report.Counts {
		if count.Metric != MetricRequests && count.Detail != "gpt-5-2026" {
			t.Errorf("%s counted for %q, not the model that answered", count.Metric, count.Detail)
		}
	}
}

func TestATenantsOwnKeyIsUsedInsteadOfTheOperators(t *testing.T) {
	w := newPluginFixture(t, "openai", Grant{Key: "sealed-own"}, func(rw http.ResponseWriter, r *http.Request) {
		rw.Header().Set("Content-Type", "application/json")
		io.WriteString(rw, `{}`)
	}, nil)
	if response, body := w.get(t, "127.0.0.1", "/v1/models"); response.StatusCode != 200 {
		t.Fatalf("%d %s", response.StatusCode, body)
	}
	if auth := w.upstream(t).Header.Get("Authorization"); auth != "Bearer own-key" {
		t.Fatalf("upstream saw %q", auth)
	}
}

// Another provider that speaks OpenAI's API is its host, in the spec, and
// the path its API is under, in the config.
func TestOpenAIsUpstreamIsAHostOfTheSpecsAndAPath(t *testing.T) {
	w := newPluginFixture(t, "openai", Grant{}, func(rw http.ResponseWriter, r *http.Request) {
		rw.Header().Set("Content-Type", "application/json")
		io.WriteString(rw, `{"model":"llama","usage":{"prompt_tokens":1,"completion_tokens":2}}`)
	}, func(spec *Spec) {
		spec.Hosts = []string{"api.groq.com"}
		spec.Config = map[string]any{"upstream": "https://api.groq.com/openai"}
	})
	if response, body := post(t, w, "/v1/chat/completions", "application/json", `{"model":"llama"}`); response.StatusCode != 200 {
		t.Fatalf("%d %s", response.StatusCode, body)
	}
	if seen := w.upstream(t); seen.Host != "api.groq.com" || seen.URL.Path != "/openai/v1/chat/completions" {
		t.Fatalf("went to %s%s", seen.Host, seen.URL.Path)
	}
	if w.used("tokens") != 3 {
		t.Fatalf("tokens %d", w.used("tokens"))
	}
	// Without an upstream, the first host is it; an upstream not among the
	// hosts gets no key.
	w.mu.Lock()
	plugin := w.config.Plugins["p"]
	plugin.Spec.Config = map[string]any{"upstream": "https://evil.example.org"}
	w.config.Plugins["p"] = plugin
	w.mu.Unlock()
	if response, _ := post(t, w, "/v1/chat/completions", "application/json", `{"model":"llama"}`); response.StatusCode != http.StatusBadGateway {
		t.Fatalf("an upstream of no host of the plugin's: %d", response.StatusCode)
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

func streamed(t *testing.T, w *containerFixture, body string) []string {
	t.Helper()
	request, _ := http.NewRequest(http.MethodPost, w.server.URL+"/p/v1/chat/completions", strings.NewReader(body))
	request.Header.Set("Content-Type", "application/json")
	response := w.as(t, "127.0.0.1", request)
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
	w := newPluginFixture(t, "openai", Grant{}, chatStream(&asked), nil)
	events := streamed(t, w, `{"model":"gpt-5","stream":true,"temperature":0.5,"messages":[]}`)
	if !asked {
		t.Fatal("the stream was not asked for its usage, or the rest of the request changed")
	}
	if len(events) != 3 || strings.Contains(strings.Join(events, "\n"), `"prompt_tokens"`) || events[2] != "data: [DONE]" {
		t.Fatalf("the app got %q", events)
	}
	if w.used("tokens") != 9 || w.used("tokens.estimated") != 0 {
		t.Fatalf("tokens %d, estimated %d", w.used("tokens"), w.used("tokens.estimated"))
	}
	// An app that asked gets the chunk, and one that named stream_options
	// gets them as it named them, with the usage asked for.
	events = streamed(t, w, `{"model":"gpt-5","stream":true,"temperature":0.5,"stream_options":{"include_usage":true},"messages":[]}`)
	if len(events) != 4 || !strings.Contains(events[2], `"prompt_tokens":7`) {
		t.Fatalf("the app that asked got %q", events)
	}
	events = streamed(t, w, `{"model":"gpt-5","stream":true,"temperature":0.5,"stream_options":null,"messages":[]}`)
	if !asked || len(events) != 3 {
		t.Fatalf("stream_options of null: asked %v, the app got %q", asked, events)
	}
	if w.used("tokens") != 27 {
		t.Fatalf("tokens %d", w.used("tokens"))
	}
}

func TestAStreamThatEndsWithoutUsageIsEstimated(t *testing.T) {
	w := newPluginFixture(t, "openai", Grant{}, func(rw http.ResponseWriter, r *http.Request) {
		rw.Header().Set("Content-Type", "text/event-stream")
		io.WriteString(rw, `data: {"type":"response.output_text.delta","delta":"`+strings.Repeat("x", 400)+`"}`+"\n\n")
		// The stream breaks off: no response.completed.
	}, nil)
	body := `{"model":"gpt-5","stream":true,"input":"` + strings.Repeat("y", 77) + `"}`
	post(t, w, "/v1/responses", "application/json", body)
	wantInput, wantOutput := int64((len(body)+3)/4), int64(100)
	if w.used("tokens.input") != wantInput || w.used("tokens.output") != wantOutput || w.used("tokens.estimated") != wantInput+wantOutput {
		t.Fatalf("input %d, output %d, estimated %d", w.used("tokens.input"), w.used("tokens.output"), w.used("tokens.estimated"))
	}
}

func TestTheResponsesAPIsCompletedEventCarriesTheUsage(t *testing.T) {
	w := newPluginFixture(t, "openai", Grant{}, func(rw http.ResponseWriter, r *http.Request) {
		rw.Header().Set("Content-Type", "text/event-stream")
		io.WriteString(rw, "event: response.output_text.delta\n"+`data: {"type":"response.output_text.delta","delta":"Hi"}`+"\n\n")
		io.WriteString(rw, "event: response.completed\n"+`data: {"type":"response.completed","response":{"model":"gpt-5-x","output":[{"content":"`+strings.Repeat("z", 100<<10)+`"}],"usage":{"input_tokens":20,"output_tokens":5,"input_tokens_details":{"cached_tokens":3}}}}`+"\n\n")
	}, nil)
	_, got := post(t, w, "/v1/responses", "application/json", `{"model":"gpt-5","stream":true}`)
	if !strings.Contains(got, "response.completed") || len(got) < 100<<10 {
		t.Fatalf("the app got %d bytes", len(got))
	}
	if w.used("tokens") != 25 || w.used("tokens.cached") != 3 {
		t.Fatalf("tokens %d, cached %d", w.used("tokens"), w.used("tokens.cached"))
	}
}

func TestTheUsageOfALargeAnswerIsReadFromItsTail(t *testing.T) {
	w := newPluginFixture(t, "openai", Grant{}, func(rw http.ResponseWriter, r *http.Request) {
		rw.Header().Set("Content-Type", "application/json")
		io.WriteString(rw, `{"object":"list","model":"text-embedding-3-small","data":[`)
		for i := range 100000 {
			if i > 0 {
				io.WriteString(rw, ",")
			}
			io.WriteString(rw, `{"embedding":[0.0123,0.0456,0.0789,0.1011,0.1213]}`)
		}
		io.WriteString(rw, `],"usage":{"prompt_tokens":5000,"total_tokens":5000}}`)
	}, nil)
	post(t, w, "/v1/embeddings", "application/json", `{"model":"text-embedding-3-small","input":["a"]}`)
	if w.used("tokens.input") != 5000 || w.used("tokens.output") != 0 {
		t.Fatalf("input %d, output %d", w.used("tokens.input"), w.used("tokens.output"))
	}
}

func TestAHardLimitRefusesTheNextRequestAndASoftOneDoesNot(t *testing.T) {
	w := newPluginFixture(t, "openai", Grant{Limits: []Limit{{Metric: "tokens", Per: PerMonth, Amount: 150, Mode: ModeHard}, {Metric: MetricRequests, Per: PerDay, Amount: 1, Mode: ModeSoft}}}, func(rw http.ResponseWriter, r *http.Request) {
		rw.Header().Set("Content-Type", "application/json")
		io.WriteString(rw, `{"model":"m","usage":{"prompt_tokens":60,"completion_tokens":40}}`)
	}, nil)
	// 100 then 200 tokens: the second goes past the limit, but was let
	// through, as its cost was not known before.
	for range 2 {
		if response, body := post(t, w, "/v1/chat/completions", "application/json", `{"model":"m"}`); response.StatusCode != 200 {
			t.Fatalf("status %d before the limit: %s", response.StatusCode, body)
		}
		w.upstream(t)
	}
	response, _ := post(t, w, "/v1/chat/completions", "application/json", `{"model":"m"}`)
	if response.StatusCode != http.StatusTooManyRequests || response.Header.Get("X-Murder-Plugin-Error") != "murder_limit_reached" || response.Header.Get("Retry-After") == "" {
		t.Fatalf("status %d, %v", response.StatusCode, response.Header)
	}
	select {
	case <-w.seen:
		t.Fatal("the upstream was called past the limit")
	case <-time.After(100 * time.Millisecond):
	}
}

func TestOnlyTheEndpointsWhoseUseIsCountedGoOn(t *testing.T) {
	w := newPluginFixture(t, "openai", Grant{}, ok, func(spec *Spec) {
		spec.Config = map[string]any{"allow": []any{"GET /v1/batches/*"}}
	})
	for _, c := range []struct {
		method, path string
		want         int
	}{
		{http.MethodGet, "/v1/files", http.StatusForbidden},
		{http.MethodPost, "/v1/files", http.StatusForbidden},
		{http.MethodGet, "/v1/chat/completions", http.StatusForbidden},
		{http.MethodGet, "/v1/responses/resp_1", http.StatusForbidden},
		{http.MethodPost, "/v1/audio/speech", http.StatusForbidden},
		{http.MethodGet, "/v1/batches", http.StatusForbidden},
		{http.MethodGet, "/v1/batches/b1/cancel", http.StatusForbidden},
		{http.MethodGet, "/v1/%62atches/b1/cancel", http.StatusForbidden},
		{http.MethodGet, "/v1/batches/b1", http.StatusOK},
		{http.MethodGet, "/v1/models/gpt-5", http.StatusOK},
	} {
		request, _ := http.NewRequest(c.method, w.server.URL+"/p"+c.path, nil)
		response := w.as(t, "127.0.0.1", request)
		if response.StatusCode != c.want {
			t.Errorf("%s %s: %d, not %d", c.method, c.path, response.StatusCode, c.want)
		}
		if c.want == http.StatusOK {
			w.upstream(t)
		}
	}
	request, _ := http.NewRequest(http.MethodGet, w.server.URL+"/p/v1/models", nil)
	request.Header.Set("Connection", "Upgrade")
	request.Header.Set("Upgrade", "websocket")
	if response := w.as(t, "127.0.0.1", request); response.StatusCode != http.StatusBadRequest {
		t.Fatalf("a WebSocket: %d", response.StatusCode)
	}
	select {
	case r := <-w.seen:
		t.Fatalf("the upstream got %s %s", r.Method, r.URL.Path)
	case <-time.After(100 * time.Millisecond):
	}
}

func TestAModelATenantMakesUpIsNoDetailOfItsOwn(t *testing.T) {
	w := newPluginFixture(t, "openai", Grant{}, func(rw http.ResponseWriter, r *http.Request) {
		rw.Header().Set("Content-Type", "text/event-stream")
		io.WriteString(rw, `data: {"choices":[{"delta":{"content":"hi"}}]}`+"\n\n")
	}, nil)
	post(t, w, "/v1/chat/completions", "application/json", `{"model":"`+strings.Repeat("m", 200)+`","stream":true}`)
	report, _ := w.counter.Next()
	if len(report.Counts) < 2 {
		t.Fatalf("counts %+v", report.Counts)
	}
	for _, count := range report.Counts {
		if count.Detail != "" && count.Detail != "other" {
			t.Errorf("%s counted for %q", count.Metric, count.Detail)
		}
	}
}

// A body of a type the plugin does not read passes on as it comes, at any
// size; so does one of JSON longer than it reads, as base64 files are,
// which it passes unread (passLongBodies).
func TestABodyOfAnotherTypePassesOnUnread(t *testing.T) {
	var got int
	w := newPluginFixture(t, "openai", Grant{}, func(rw http.ResponseWriter, r *http.Request) {
		body, _ := io.ReadAll(r.Body)
		got = len(body)
		rw.Header().Set("Content-Type", "application/json")
		io.WriteString(rw, `{"text":"hi","usage":{"input_tokens":3,"output_tokens":1}}`)
	}, nil)
	audio := strings.Repeat("a", MaxRequestBody+1<<20)
	response, body := post(t, w, "/v1/audio/transcriptions", "multipart/form-data; boundary=x", audio)
	if response.StatusCode != 200 || got != len(audio) {
		t.Fatalf("%d %s: upstream got %d of %d bytes", response.StatusCode, body, got, len(audio))
	}
	w.upstream(t)
	if w.used("tokens") != 4 {
		t.Fatalf("tokens %d", w.used("tokens"))
	}
	large := `{"model":"m","input":"` + strings.Repeat("x", MaxRequestBody) + `"}`
	if response, body := post(t, w, "/v1/embeddings", "application/json", large); response.StatusCode != 200 || got != len(large) {
		t.Fatalf("JSON past what the plugin reads: %d %s, upstream got %d of %d bytes", response.StatusCode, body, got, len(large))
	}
}

// A streamed chat completion too long to be asked for its usage within
// what the plugin reads goes on as it was, its use estimated.
func TestAStreamTooLongToAskForItsUsageIsEstimated(t *testing.T) {
	var asked bool
	var got int
	w := newPluginFixture(t, "openai", Grant{}, func(rw http.ResponseWriter, r *http.Request) {
		body, _ := io.ReadAll(r.Body)
		got = len(body)
		r.Body = io.NopCloser(strings.NewReader(string(body)))
		chatStream(&asked)(rw, r)
	}, nil)
	head := `{"model":"gpt-5","stream":true,"messages":[{"role":"user","content":"`
	body := head + strings.Repeat("x", MaxRequestBody-len(head)-len(`"}]}`)-8) + `"}]}`
	if response, said := post(t, w, "/v1/chat/completions", "application/json", body); response.StatusCode != 200 || !strings.Contains(said, "[DONE]") {
		t.Fatalf("%d %.200s", response.StatusCode, said)
	}
	if asked || got != len(body) {
		t.Fatalf("asked %v; the upstream got %d of %d bytes", asked, got, len(body))
	}
	if w.used("tokens.estimated") == 0 {
		t.Fatal("not estimated")
	}
}

// A container may replace an answer whole; one it could not replace,
// as one too long or one it failed on, is withheld, never passed as it
// was.
func TestAnAnswerToReplaceIsReplacedOrWithheld(t *testing.T) {
	w := newContainerFixture(t, func(rw http.ResponseWriter, r *http.Request) {
		if strings.HasSuffix(r.URL.Path, "long") {
			io.WriteString(rw, strings.Repeat("secret ", (maxReplaced/7)+10))
			return
		}
		io.WriteString(rw, "a secret of the provider's")
	}, nil)
	if response, body := w.get(t, "127.0.0.1", "/replace"); response.StatusCode != 200 || body != "A SECRET OF THE PROVIDER'S" {
		t.Fatalf("replaced: %d %q", response.StatusCode, body)
	}
	w.upstream(t)
	for _, path := range []string{"/replacefail", "/replacelong"} {
		response, body := w.get(t, "127.0.0.1", path)
		if response.StatusCode != http.StatusBadGateway || strings.Contains(strings.ToLower(body), "secret") {
			t.Errorf("%s: %d %.100s", path, response.StatusCode, body)
		}
		w.upstream(t)
	}
	report, _ := w.counter.Next()
	replaced := false
	for _, count := range report.Counts {
		replaced = replaced || count.Metric == "bytes" && count.Detail == "replaced" && count.Amount == int64(len("a secret of the provider's"))
	}
	if !replaced {
		t.Fatalf("counts %+v", report.Counts)
	}
}

// A body is read in room taken as it comes, not as it says it is long,
// and the room is given back.
func TestABodyTakesRoomAsItComes(t *testing.T) {
	var got int
	w := newPluginFixture(t, "openai", Grant{}, func(rw http.ResponseWriter, r *http.Request) {
		body, _ := io.ReadAll(r.Body)
		got = len(body)
		rw.Header().Set("Content-Type", "application/json")
		io.WriteString(rw, `{}`)
	}, nil)
	body := `{"model":"m","input":"` + strings.Repeat("x", 3*bodyChunk) + `"}`
	if response, said := post(t, w, "/v1/embeddings", "application/json", body); response.StatusCode != 200 || got != len(body) {
		t.Fatalf("%d %s, upstream got %d of %d bytes", response.StatusCode, said, got, len(body))
	}
	// The room is given back as the handler returns, which may be just
	// after the tenant has the answer.
	for deadline := time.Now().Add(5 * time.Second); ; time.Sleep(10 * time.Millisecond) {
		w.proxy.mu.Lock()
		held := w.proxy.buffered
		w.proxy.mu.Unlock()
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
	w := newPluginFixture(t, "openai", Grant{}, func(rw http.ResponseWriter, r *http.Request) {
		rw.Header().Set("Trailer", "X-Echo")
		rw.Header().Set("Content-Type", "application/json")
		io.WriteString(rw, `{"object":"list","data":[]}`)
		rw.(http.Flusher).Flush()
		rw.Header().Set("X-Echo", r.Header.Get("Authorization"))
	}, nil)
	response, body := w.get(t, "127.0.0.1", "/v1/models")
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

func TestHEREGetsTheKeyInItsQueryAndCountsTransactions(t *testing.T) {
	w := newPluginFixture(t, "here", Grant{}, func(rw http.ResponseWriter, r *http.Request) {
		switch r.Host {
		case "geocode.search.hereapi.com":
			rw.Header().Set("Location", "https://geocode.search.hereapi.com/v1/geocode?apiKey=operator-key")
			io.WriteString(rw, `{"items":[]}`)
		case "matrix.router.hereapi.com":
			io.WriteString(rw, `{"matrix":{}}`)
		default:
			rw.WriteHeader(http.StatusUnauthorized)
			io.WriteString(rw, `{"error":"bad key operator-key for `+r.URL.String()+`"}`)
		}
	}, nil)
	response, _ := w.get(t, "127.0.0.1", "/geocode.search.hereapi.com/v1/geocode?q=Berlin&apiKey=theirs&apikey=theirs")
	if response.StatusCode != 200 || strings.Contains(response.Header.Get("Location"), "operator-key") {
		t.Fatalf("%d, Location %q", response.StatusCode, response.Header.Get("Location"))
	}
	seen := w.upstream(t)
	query := seen.URL.Query()
	if seen.Host != "geocode.search.hereapi.com" || seen.URL.Path != "/v1/geocode" || query.Get("apiKey") != "operator-key" || query.Get("apikey") != "" || query.Get("q") != "Berlin" {
		t.Fatalf("upstream saw %s%s?%s", seen.Host, seen.URL.Path, seen.URL.RawQuery)
	}
	matrix := `{"origins":[{},{},{},{},{},{}],"destinations":[{},{},{},{},{},{},{}]}`
	if response, body := post(t, w, "/matrix.router.hereapi.com/v8/matrix?async=false", "application/json", matrix); response.StatusCode != 200 {
		t.Fatalf("matrix: %d %s", response.StatusCode, body)
	}
	if seen := w.upstream(t); seen.Host != "matrix.router.hereapi.com" {
		t.Fatalf("matrix went to %s", seen.Host)
	}
	response, body := w.get(t, "127.0.0.1", "/router.hereapi.com/v8/routes")
	if response.StatusCode != 401 || strings.Contains(body, "operator-key") {
		t.Fatalf("an error told the key: %s", body)
	}
	w.upstream(t)
	// One for the geocode, 5 x 7 for the matrix, none for the refusal.
	if w.used(MetricRequests) != 3 || w.used("transactions") != 36 {
		t.Fatalf("requests %d, transactions %d", w.used(MetricRequests), w.used("transactions"))
	}
	report, _ := w.counter.Next()
	services := map[string]bool{}
	for _, count := range report.Counts {
		services[count.Detail] = true
	}
	if !services["geocode.search"] || !services["matrix.router"] || !services["router"] {
		t.Fatalf("services %v", services)
	}
	for _, path := range []string{"/evil.com/x", "/hereapi.com/x", "/geocode.search.hereapi.com/../x", "/geocode.search.hereapi.com/%2e%2e/x"} {
		if response, body := w.get(t, "127.0.0.1", path); response.StatusCode != http.StatusBadRequest {
			t.Errorf("%s: %d %s", path, response.StatusCode, body)
		}
	}
}

// A wildcard of the spec's hosts reaches the services below it, counted
// by the wildcard: tenants cannot make up services without end.
func TestHEREsHostsOfTheSpecReplaceItsOwn(t *testing.T) {
	w := newPluginFixture(t, "here", Grant{}, ok, func(spec *Spec) {
		spec.Hosts = []string{"*.hereapi.com"}
	})
	if response, body := w.get(t, "127.0.0.1", "/positioning.hereapi.com/v2/locate"); response.StatusCode != 200 {
		t.Fatalf("%d %s", response.StatusCode, body)
	}
	w.upstream(t)
	report, _ := w.counter.Next()
	for _, count := range report.Counts {
		if count.Detail != "*.hereapi.com" {
			t.Errorf("%s counted for %q", count.Metric, count.Detail)
		}
	}
	if response, _ := w.get(t, "127.0.0.1", "/hereapi.com/x"); response.StatusCode != http.StatusBadRequest {
		t.Fatalf("the domain itself: %d", response.StatusCode)
	}
}

func TestHEREGetsTheQueryAsItWasWritten(t *testing.T) {
	w := newPluginFixture(t, "here", Grant{}, ok, nil)
	w.get(t, "127.0.0.1", "/geocode.search.hereapi.com/v1/geocode?qq=city=Berlin;country=Germany&apiKey=theirs&lang=de")
	if raw := w.upstream(t).URL.RawQuery; raw != "qq=city=Berlin;country=Germany&lang=de&apiKey=operator-key" {
		t.Fatalf("upstream got %q", raw)
	}
}

// One tenant's bodies under way to a container leave room for the
// others'.
func TestOneTenantsBodiesInAContainerLeaveRoomForTheOthers(t *testing.T) {
	c := &container{byTenant: map[string]int64{}}
	var releases []func()
	for {
		release, ok := c.hold("acme", maxRequestCall, 0)
		if !ok {
			break
		}
		releases = append(releases, release)
	}
	if c.byTenant["acme"] != ContainerBodiesPerTenant {
		t.Fatalf("acme holds %d", c.byTenant["acme"])
	}
	release, ok := c.hold("beta", maxRequestCall, 0)
	if !ok {
		t.Fatal("beta finds no room while acme holds its share")
	}
	releases = append(releases, release)
	if _, ok := c.hold("delta", 1, 20*time.Millisecond); ok {
		t.Fatal("room past what the container takes")
	}
	// One that waits gets the room given back while it waits.
	go func() { time.Sleep(20 * time.Millisecond); releases[0]() }()
	release, ok = c.hold("delta", 1, 5*time.Second)
	if !ok {
		t.Fatal("no room once some was given back")
	}
	releases[0] = release
	for _, release := range releases {
		release()
	}
	if c.held != 0 || len(c.byTenant) != 0 {
		t.Fatalf("left %d, %v", c.held, c.byTenant)
	}
}

// The examples' files are plugins' files apply takes, of Murder's images
// or the example's.
func TestTheExamplesFilesAreTaken(t *testing.T) {
	files, err := filepath.Glob("../../examples/plugins/*.yaml")
	if err != nil || len(files) < 2 {
		t.Fatalf("%v %v", files, err)
	}
	more, _ := filepath.Glob("../../examples/plugins/*/*.yaml")
	for _, file := range append(files, more...) {
		data, err := os.ReadFile(file)
		if err != nil {
			t.Fatal(err)
		}
		spec, err := Decode(data)
		if err == nil {
			err = spec.ValidateUnpinned()
		}
		if err != nil {
			t.Errorf("%s: %v", file, err)
		}
	}
	// A file of a kind Murder's plugins were is told the image instead.
	if _, err := Parse([]byte("kind: openai\n")); err == nil || !strings.Contains(err.Error(), "ghcr.io/powerpenguini/murder-openai") {
		t.Fatalf("kind openai: %v", err)
	}
}

// birCall is a tenant's call to BIR1, as its client writes it: to the
// plugin's address, with a key of its own if any.
func birCall(operation, body string) string {
	return `<soap:Envelope xmlns:soap="http://www.w3.org/2003/05/soap-envelope" xmlns:ns="http://CIS/BIR/PUBL/2014/07" xmlns:dat="http://CIS/BIR/PUBL/2014/07/DataContract">` +
		`<soap:Header xmlns:wsa="http://www.w3.org/2005/08/addressing"><wsa:To>http://10.240.0.1:4180/gus/wsBIR/UslugaBIRzewnPubl.svc</wsa:To>` +
		`<wsa:Action>http://CIS/BIR/PUBL/2014/07/IUslugaBIRzewnPubl/` + operation + `</wsa:Action></soap:Header><soap:Body>` + body + `</soap:Body></soap:Envelope>`
}

// birAnswer answers as BIR1 does, an envelope in a multipart of XOP.
func birAnswer(rw http.ResponseWriter, result string) {
	rw.Header().Set("Content-Type", `multipart/related; type="application/xop+xml";start="<http://tempuri.org/0>";boundary="uuid:x";start-info="application/soap+xml"`)
	io.WriteString(rw, "--uuid:x\r\nContent-ID: <http://tempuri.org/0>\r\nContent-Type: application/xop+xml;charset=utf-8;type=\"application/soap+xml\"\r\n\r\n"+
		`<s:Envelope xmlns:s="http://www.w3.org/2003/05/soap-envelope"><s:Body>`+result+`</s:Body></s:Envelope>`+"\r\n--uuid:x--\r\n")
}

// A tenant's client logs in to BIR1 with any key, and searches with the
// session it got: the login goes with the operator's key, in the body the
// plugin wrote, every call to the service's own address; searches count
// an entity a number.
func TestGUSLogsInWithTheKeyInTheBodyAndCountsSearches(t *testing.T) {
	var mu sync.Mutex
	var sent []string
	w := newPluginFixture(t, "gus", Grant{}, func(rw http.ResponseWriter, r *http.Request) {
		body, _ := io.ReadAll(r.Body)
		mu.Lock()
		sent = append(sent, string(body))
		mu.Unlock()
		switch {
		case strings.Contains(string(body), "Zaloguj"):
			birAnswer(rw, `<ZalogujResponse xmlns="http://CIS/BIR/PUBL/2014/07"><ZalogujResult>ey6nu6e2428xp3b434gx</ZalogujResult></ZalogujResponse>`)
		case strings.Contains(string(body), "Raport"):
			rw.WriteHeader(http.StatusInternalServerError)
			io.WriteString(rw, `<s:Fault>bad session for key operator-key</s:Fault>`)
		default:
			birAnswer(rw, `<DaneSzukajPodmiotyResponse xmlns="http://CIS/BIR/PUBL/2014/07"><DaneSzukajPodmiotyResult>&lt;root&gt;&lt;dane&gt;&lt;Regon&gt;000331501&lt;/Regon&gt;&lt;/dane&gt;&lt;/root&gt;</DaneSzukajPodmiotyResult></DaneSzukajPodmiotyResponse>`)
		}
	}, nil)
	const svc = "/wsBIR/UslugaBIRzewnPubl.svc"
	response, body := post(t, w, svc, "application/soap+xml; charset=utf-8", birCall("Zaloguj", `<ns:Zaloguj><ns:pKluczUzytkownika>murder-key-guess</ns:pKluczUzytkownika></ns:Zaloguj>`))
	// The tenant gets a session of the plugin's, never BIR1's.
	start, end := strings.Index(body, "<ZalogujResult>"), strings.Index(body, "</ZalogujResult>")
	if response.StatusCode != 200 || start < 0 || end != start+len("<ZalogujResult>")+20 || strings.Contains(body, "ey6nu6e2428xp3b434gx") ||
		response.Header.Get("Content-Length") != strconv.Itoa(len(body)) {
		t.Fatalf("login: %d %v %s", response.StatusCode, response.Header, body)
	}
	token := body[start+len("<ZalogujResult>") : end]
	login := w.upstream(t)
	mu.Lock()
	first := sent[0]
	mu.Unlock()
	if login.Host != "wyszukiwarkaregon.stat.gov.pl" || login.URL.Path != svc || !strings.Contains(first, "<ns:pKluczUzytkownika>operator-key</ns:pKluczUzytkownika>") ||
		strings.Contains(first, "murder-key") || !strings.Contains(first, "<wsa:To>https://wyszukiwarkaregon.stat.gov.pl"+svc+"</wsa:To>") ||
		!strings.HasPrefix(login.Header.Get("Content-Type"), "application/soap+xml") {
		t.Fatalf("GUS got %s%s: %s", login.Host, login.URL.Path, first)
	}
	request, _ := http.NewRequest(http.MethodPost, w.server.URL+"/p"+svc, strings.NewReader(birCall("DaneSzukajPodmioty", `<ns:DaneSzukajPodmioty><ns:pParametryWyszukiwania><dat:Nipy>5261040828,5261040829</dat:Nipy></ns:pParametryWyszukiwania></ns:DaneSzukajPodmioty>`)))
	request.Header.Set("Content-Type", "application/soap+xml; charset=utf-8")
	request.Header.Set("sid", token)
	if response := w.as(t, "127.0.0.1", request); response.StatusCode != 200 {
		t.Fatalf("search: %d", response.StatusCode)
	}
	if sid := w.upstream(t).Header.Get("sid"); sid != "ey6nu6e2428xp3b434gx" {
		t.Fatalf("the session went as %q", sid)
	}
	// A fault that says the key is answered without it, and counts no
	// report.
	response, body = post(t, w, svc, "application/soap+xml", birCall("DanePobierzPelnyRaport", `<ns:DanePobierzPelnyRaport><ns:pRegon>000331501</ns:pRegon><ns:pNazwaRaportu>BIR11OsPrawna</ns:pNazwaRaportu></ns:DanePobierzPelnyRaport>`))
	if response.StatusCode != 500 || strings.Contains(body, "operator-key") {
		t.Fatalf("a fault: %d %s", response.StatusCode, body)
	}
	w.upstream(t)
	// BIR1's session, were the tenant to know it, is no session of the
	// plugin's: it goes no further.
	request, _ = http.NewRequest(http.MethodPost, w.server.URL+"/p"+svc, strings.NewReader(birCall("GetValue", `<ns:GetValue xmlns:ns="http://CIS/BIR/2014/07"><ns:pNazwaParametru>StatusSesji</ns:pNazwaParametru></ns:GetValue>`)))
	request.Header.Set("Content-Type", "application/soap+xml")
	request.Header.Set("sid", "ey6nu6e2428xp3b434gx")
	w.as(t, "127.0.0.1", request)
	if sid := w.upstream(t).Header.Get("sid"); sid != "" {
		t.Fatalf("BIR1's own session went on: %q", sid)
	}
	if w.used("searches") != 2 || w.used("reports") != 0 || w.used(MetricRequests) != 4 {
		t.Fatalf("searches %d, reports %d, requests %d", w.used("searches"), w.used("reports"), w.used(MetricRequests))
	}
	// What is not a call of BIR1's is refused before it goes.
	if response, _ := post(t, w, svc, "application/json", `{"nip":"5261040828"}`); response.StatusCode != http.StatusUnsupportedMediaType {
		t.Fatalf("JSON: %d", response.StatusCode)
	}
	if response, _ := post(t, w, svc, "application/soap+xml", birCall("Usun", `<ns:Usun/>`)); response.StatusCode != http.StatusBadRequest {
		t.Fatalf("an operation unknown: %d", response.StatusCode)
	}
}

// With MURDER_TEST_GUS=1, a tenant logs in to GUS's own test service and
// finds GUS by its NIP, through the edge, with the service's published test
// key.
func TestGUSsTestServiceThroughTheEdge(t *testing.T) {
	if os.Getenv("MURDER_TEST_GUS") != "1" {
		t.Skip("reaches wyszukiwarkaregontest.stat.gov.pl: MURDER_TEST_GUS=1")
	}
	w := newPluginFixture(t, "gus", Grant{}, ok, func(spec *Spec) { spec.Hosts = []string{"wyszukiwarkaregontest.stat.gov.pl"} })
	w.proxy.Transport = NewTransport()
	w.proxy.open = func(string, string) (string, error) { return "abcde12345abcde12345", nil }
	const svc = "/wsBIR/UslugaBIRzewnPubl.svc"
	_, body := post(t, w, svc, "application/soap+xml; charset=utf-8", birCall("Zaloguj", `<ns:Zaloguj><ns:pKluczUzytkownika>any</ns:pKluczUzytkownika></ns:Zaloguj>`))
	start := strings.Index(body, "<ZalogujResult>")
	end := strings.Index(body, "</ZalogujResult>")
	if start < 0 || end < start+len("<ZalogujResult>")+1 {
		t.Fatalf("no session: %s", body)
	}
	sid := body[start+len("<ZalogujResult>") : end] // the plugin's
	ask := func(operation, body, want string) {
		t.Helper()
		request, _ := http.NewRequest(http.MethodPost, w.server.URL+"/p"+svc, strings.NewReader(birCall(operation, body)))
		request.Header.Set("Content-Type", "application/soap+xml; charset=utf-8")
		request.Header.Set("sid", sid)
		response := w.as(t, "127.0.0.1", request)
		answer, _ := io.ReadAll(response.Body)
		if response.StatusCode != 200 || !strings.Contains(string(answer), want) {
			t.Fatalf("%s: %d %.600s", operation, response.StatusCode, answer)
		}
	}
	ask("DaneSzukajPodmioty", `<ns:DaneSzukajPodmioty><ns:pParametryWyszukiwania><dat:Nip>5261040828</dat:Nip></ns:pParametryWyszukiwania></ns:DaneSzukajPodmioty>`, "000331501")
	ask("DaneSzukajPodmioty", `<ns:DaneSzukajPodmioty><ns:pParametryWyszukiwania><dat:Nipy>5261040828,5250001009</dat:Nipy></ns:pParametryWyszukiwania></ns:DaneSzukajPodmioty>`, "000331501")
	ask("DanePobierzPelnyRaport", `<ns:DanePobierzPelnyRaport><ns:pRegon>000331501</ns:pRegon><ns:pNazwaRaportu>BIR11OsPrawna</ns:pNazwaRaportu></ns:DanePobierzPelnyRaport>`, "praw_nip")
	ask("GetValue", `<ns:GetValue xmlns:ns="http://CIS/BIR/2014/07"><ns:pNazwaParametru>StatusSesji</ns:pNazwaParametru></ns:GetValue>`, "<GetValueResult>1</GetValueResult>")
	ask("Wyloguj", `<ns:Wyloguj><ns:pIdentyfikatorSesji>`+sid+`</ns:pIdentyfikatorSesji></ns:Wyloguj>`, "<WylogujResult>true</WylogujResult>")
	if w.used("searches") != 3 || w.used("reports") != 1 {
		t.Fatalf("searches %d, reports %d", w.used("searches"), w.used("reports"))
	}
}
