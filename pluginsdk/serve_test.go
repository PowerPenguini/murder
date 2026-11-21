package pluginsdk

import (
	"context"
	"encoding/json"
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

func testPlugin() *plugin {
	return newPlugin(Manifest{ABI: ABI, Name: "t"}, func(r *Request) (*Forward, error) {
		switch r.Path {
		case "/refuse":
			return nil, Refuse(429, "slow down, %s", r.Tenant)
		case "/panic":
			panic("on purpose")
		case "/plain":
			return &Forward{URL: "https://api.example.com/plain"}, nil
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
		var answer map[string]any
		if err := json.NewDecoder(response.Body).Decode(&answer); err != nil {
			t.Fatal(err)
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
	forward := call("/request", "a", `{"tenant":"acme","path":"/v1/x","body":"aGVsbG8="}`)
	if forward["url"] != "https://api.example.com/v1/x" || forward["meter"] != true || forward["replaceBody"] != true || forward["body"] != "aGVsbG8=" {
		t.Fatalf("forward %v", forward)
	}
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
