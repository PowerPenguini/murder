package pluginsdk

import (
	"context"
	"io"
	"net"
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

type tokens struct{ body []byte }

func (m *tokens) Response(r *Response, c *Counter) bool {
	c.Count("calls", "", 1)
	return r.Status == 200
}

func (m *tokens) Body(chunk []byte, _ *Counter) bool {
	m.body = append(m.body, chunk...)
	return true
}

func (m *tokens) End(complete bool, c *Counter) {
	if complete {
		c.Count("tokens", "m", int64(len(m.body)))
	}
}

// events reads a stream event by event, and takes out those of usage.
type events struct {
	tokens
	seen int
}

func (m *events) Event(data []byte, c *Counter) bool {
	m.seen++
	c.Count("events", "", 1)
	return !strings.Contains(string(data), "usage")
}

func testPlugin() *plugin {
	return newPlugin(Manifest{ABI: ABI, Name: "t"}, func(r *Request) (*Forward, error) {
		switch r.Path {
		case "/refuse":
			return nil, Refuse(429, "slow down, %s", r.Tenant)
		case "/panic":
			panic("on purpose")
		case "/plain":
			return &Forward{URL: "https://api.example.com/plain"}, nil
		case "/stream":
			return &Forward{URL: "https://api.example.com/stream", Meter: &events{}}, nil
		}
		return &Forward{URL: "https://api.example.com" + r.Path, Body: r.Body, Meter: &tokens{}}, nil
	})
}

// serve serves p on a socket, as Run does, and calls it there.
func serve(t *testing.T, p *plugin) func(path, session string, body string) map[string]any {
	t.Helper()
	dir, err := os.MkdirTemp("/tmp", "sdk")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { os.RemoveAll(dir) })
	socket := filepath.Join(dir, "plugin.sock")
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- Serve(ctx, socket, p.handler()) }()
	t.Cleanup(func() { cancel(); <-done })
	client := &http.Client{Transport: &http.Transport{DialContext: func(ctx context.Context, _, _ string) (net.Conn, error) {
		return (&net.Dialer{}).DialContext(ctx, "unix", socket)
	}}}
	for deadline := time.Now().Add(5 * time.Second); ; time.Sleep(5 * time.Millisecond) {
		if info, err := os.Stat(socket); err == nil {
			if info.Mode().Perm() != 0o666 {
				t.Fatalf("the socket's mode %v: the edge may not reach it", info.Mode().Perm())
			}
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("no socket")
		}
	}
	return func(path, session, body string) map[string]any {
		t.Helper()
		method := http.MethodPost
		if path == "/manifest" {
			method = http.MethodGet
		}
		request, _ := http.NewRequest(method, "http://plugin"+path, strings.NewReader(body))
		request.Header.Set("Murder-Session", session)
		response, err := client.Do(request)
		if err != nil {
			t.Fatal(err)
		}
		defer response.Body.Close()
		data, err := io.ReadAll(response.Body)
		if err != nil {
			t.Fatal(err)
		}
		var answer map[string]any
		rest, err := splitCall(data, &answer)
		if err != nil {
			t.Fatalf("%v: %q", err, data)
		}
		if rest != nil {
			answer["rest"] = string(rest)
		}
		return answer
	}
}

// A request metered is a session from its forward to its end: its
// counts are its own, and it is let go of once it ended.
func TestAMeteredRequestIsASessionUntilItEnds(t *testing.T) {
	p := testPlugin()
	call := serve(t, p)
	if manifest := call("/manifest", "", ""); manifest["name"] != "t" || manifest["abi"] != float64(ABI) {
		t.Fatalf("manifest %v", manifest)
	}
	// The body follows the JSON, as it is, and so does the one that
	// replaces it.
	forward := call("/request", "a", "{\"tenant\":\"acme\",\"path\":\"/v1/x\",\"bodyFollows\":true}\nhello\n")
	if forward["url"] != "https://api.example.com/v1/x" || forward["meter"] != true || forward["replaceBody"] != true || forward["rest"] != "hello\n" {
		t.Fatalf("forward %v", forward)
	}
	// A body that does not follow is none: nothing replaces it.
	if none := call("/request", "c", `{"path":"/v1/x"}`); none["replaceBody"] != nil || none["rest"] != nil {
		t.Fatalf("no body: %v", none)
	}
	call("/end", "c", `{"complete":false}`)
	call("/request", "b", `{"tenant":"beta","path":"/v1/y"}`)
	if response := call("/response", "a", `{"status":200}`); response["body"] != true || len(response["counts"].([]any)) != 1 {
		t.Fatalf("response %v", response)
	}
	if chunk := call("/body", "a", "four"); chunk["more"] != true {
		t.Fatalf("chunk %v", chunk)
	}
	end := call("/end", "a", `{"complete":true}`)
	counts := end["counts"].([]any)
	if len(counts) != 1 || counts[0].(map[string]any)["amount"] != float64(4) {
		t.Fatalf("end %v", end)
	}
	// b, which read no body, ends counting nothing.
	if response := call("/response", "b", `{"status":500}`); response["body"] == true {
		t.Fatalf("b's response %v", response)
	}
	if end := call("/end", "b", `{"complete":false}`); end["counts"] != nil || end["error"] != nil {
		t.Fatalf("b's end %v", end)
	}
	if len(p.sessions) != 0 {
		t.Fatalf("sessions kept: %v", p.sessions)
	}
	if again := call("/body", "a", "x"); !strings.Contains(again["error"].(string), "no such session") {
		t.Fatalf("an ended session: %v", again)
	}
}

func TestARefusalAndAPanicAreToldApart(t *testing.T) {
	call := serve(t, testPlugin())
	refused := call("/request", "", `{"tenant":"acme","path":"/refuse"}`)
	if refusal := refused["refuse"].(map[string]any); refusal["status"] != float64(429) || refusal["message"] != "slow down, acme" {
		t.Fatalf("refused %v", refused)
	}
	if panicked := call("/request", "", `{"path":"/panic"}`); !strings.Contains(panicked["error"].(string), "panic: on purpose") {
		t.Fatalf("panicked %v", panicked)
	}
	// A request not metered needs no session.
	if plain := call("/request", "", `{"path":"/plain"}`); plain["url"] != "https://api.example.com/plain" || plain["meter"] != nil {
		t.Fatalf("plain %v", plain)
	}
	if metered := call("/request", "", `{"path":"/x"}`); !strings.Contains(metered["error"].(string), "no session") {
		t.Fatalf("metered without a session: %v", metered)
	}
}

// A meter that reads events is shown a stream event by event, and says
// which stay; an answer of another type it reads as a body.
func TestAStreamIsReadEventByEvent(t *testing.T) {
	call := serve(t, testPlugin())
	call("/request", "s", `{"path":"/stream"}`)
	response := call("/response", "s", `{"status":200,"header":{"Content-Type":["text/event-stream; charset=utf-8"]}}`)
	if response["body"] != true || response["events"] != true {
		t.Fatalf("response %v", response)
	}
	if kept := call("/event", "s", `{"choices":[]}`); kept["keep"] != true {
		t.Fatalf("an event of text: %v", kept)
	}
	if dropped := call("/event", "s", `{"usage":{}}`); dropped["keep"] != false {
		t.Fatalf("an event of usage: %v", dropped)
	}
	// Two events counted, and the end.
	if end := call("/end", "s", `{"complete":true}`); len(end["counts"].([]any)) != 3 {
		t.Fatalf("end %v", end)
	}
	call("/request", "j", `{"path":"/stream"}`)
	if response := call("/response", "j", `{"status":200,"header":{"Content-Type":["application/json"]}}`); response["body"] != true || response["events"] != nil {
		t.Fatalf("a JSON answer: %v", response)
	}
	// Of a meter that reads no events, an event stays.
	call("/request", "t", `{"path":"/v1/t"}`)
	call("/response", "t", `{"status":200,"header":{"Content-Type":["text/event-stream"]}}`)
	if kept := call("/event", "t", `{"usage":{}}`); kept["keep"] != true {
		t.Fatalf("an event of a meter of bodies: %v", kept)
	}
}

// The edge leaves a session when it stops; it is let go of in time.
func TestASessionLeftIsLetGoOf(t *testing.T) {
	p := testPlugin()
	call := serve(t, p)
	call("/request", "left", `{"path":"/x"}`)
	p.mu.Lock()
	p.sessions["left"].used.Store(time.Now().Add(-2 * sessionIdle).UnixNano())
	p.swept = time.Now().Add(-2 * time.Minute)
	p.mu.Unlock()
	call("/request", "next", `{"path":"/x"}`)
	p.mu.Lock()
	defer p.mu.Unlock()
	if _, ok := p.sessions["left"]; ok || len(p.sessions) != 1 {
		t.Fatalf("sessions %v", p.sessions)
	}
}
