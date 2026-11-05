package domains

import (
	"log/slog"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/PowerPenguini/murder/internal/store"
)

// The probe waits until every edge answers the challenge for the domain,
// and goes on all the same once one never does.
func TestProbeWaitsForEveryEdge(t *testing.T) {
	challenge := store.Challenge{Token: "tok", Domain: "app.customer.com", KeyAuth: "tok.thumb"}
	var ready atomic.Bool
	edge := func(answers func() bool) *httptest.Server {
		server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			if r.Host != challenge.Domain || r.URL.Path != "/.well-known/acme-challenge/tok" || !answers() {
				http.NotFound(w, r)
				return
			}
			_, _ = w.Write([]byte(challenge.KeyAuth))
		}))
		t.Cleanup(server.Close)
		return server
	}
	quick := edge(func() bool { return true })
	slow := edge(ready.Load)
	addresses := func() []string {
		return []string{strings.TrimPrefix(quick.URL, "http://"), strings.TrimPrefix(slow.URL, "http://")}
	}
	probe := ProbeEdges(addresses, slog.New(slog.DiscardHandler))
	time.AfterFunc(500*time.Millisecond, func() { ready.Store(true) })
	started := time.Now()
	if err := probe(t.Context(), challenge); err != nil {
		t.Fatal(err)
	}
	if waited := time.Since(started); waited < 400*time.Millisecond {
		t.Fatalf("the probe did not wait for the slow edge: %v", waited)
	}
	probeWait = 300 * time.Millisecond
	t.Cleanup(func() { probeWait = 30 * time.Second })
	never := edge(func() bool { return false })
	started = time.Now()
	if err := ProbeEdges(func() []string { return []string{strings.TrimPrefix(never.URL, "http://")} }, slog.New(slog.DiscardHandler))(t.Context(), challenge); err != nil {
		t.Fatal(err)
	}
	if time.Since(started) > 5*time.Second {
		t.Fatal("the probe waited past its bound")
	}
}

func TestEdgeAddresses(t *testing.T) {
	nodes := []store.Node{{Name: "a", Address: "10.0.0.2:7400"}, {Name: "b", Address: "10.0.0.3:7400", FailedAt: time.Now()}, {Name: "c", Address: "[fd00::4]:7400"}}
	if got := strings.Join(EdgeAddresses(nodes, "80"), " "); got != "10.0.0.2:80 [fd00::4]:80" {
		t.Errorf("in a cluster: %s", got)
	}
	if got := strings.Join(EdgeAddresses(nil, "8080"), " "); got != "127.0.0.1:8080" {
		t.Errorf("alone: %s", got)
	}
}
