package panel

import (
	"context"
	"errors"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/PowerPenguini/murder/internal/api"
	"github.com/PowerPenguini/murder/internal/store"
)

var demoNow = time.Date(2026, 11, 2, 12, 0, 0, 0, time.UTC)

func newTestServer(t *testing.T, backend Backend) *httptest.Server {
	t.Helper()
	s, err := New(backend, slog.New(slog.NewTextHandler(io.Discard, nil)))
	if err != nil {
		t.Fatal(err)
	}
	s.now = func() time.Time { return demoNow }
	server := httptest.NewServer(s.Handler())
	t.Cleanup(server.Close)
	return server
}

func get(t *testing.T, url string) (*http.Response, string) {
	t.Helper()
	response, err := http.Get(url)
	if err != nil {
		t.Fatal(err)
	}
	defer response.Body.Close()
	body, _ := io.ReadAll(response.Body)
	return response, string(body)
}

// The overview shows every node and tenant, as tags, and the counts.
func TestTheOverviewShowsTheCluster(t *testing.T) {
	server := newTestServer(t, Demo{Now: demoNow})
	response, body := get(t, server.URL+"/")
	if response.StatusCode != http.StatusOK {
		t.Fatalf("status %d", response.StatusCode)
	}
	for _, want := range []string{
		"fsn-1", "nbg-1", "hel-1", ">FAILED<", ">leader<",
		`href="/tenants/kowalski-trans"`, "fsn-1 -&gt; hel-1", ">UNHEALTHY<", ">ARCHIVED<", ">SUSPENDED<",
		"2<small>/3</small>", // nodes up
		"5<small>/6</small>", // healthy of those meant to run
		"copy moves from fsn-1 to hel-1",
	} {
		if !strings.Contains(body, want) {
			t.Errorf("the overview lacks %q", want)
		}
	}
	// Nothing is loaded from elsewhere, nor framed.
	if csp := response.Header.Get("Content-Security-Policy"); !strings.Contains(csp, "default-src 'none'") || !strings.Contains(csp, "frame-ancestors 'none'") {
		t.Errorf("CSP %q", csp)
	}
	if response.Header.Get("Cache-Control") != "no-store" {
		t.Errorf("pages are cached: %q", response.Header.Get("Cache-Control"))
	}
	static, _ := get(t, server.URL+"/static/panel.css")
	if static.StatusCode != http.StatusOK || !strings.HasPrefix(static.Header.Get("Cache-Control"), "public") {
		t.Errorf("static: %d %q", static.StatusCode, static.Header.Get("Cache-Control"))
	}
}

func TestATenantsPage(t *testing.T) {
	server := newTestServer(t, Demo{Now: demoNow})
	_, body := get(t, server.URL+"/tenants/baltic-freight")
	for _, want := range []string{"baltic-freight", ">UNHEALTHY<", "exit 137", "SESSION_KEY", "secret · random", "Europe/Warsaw", "256 MB"} {
		if !strings.Contains(body, want) {
			t.Errorf("the tenant's page lacks %q", want)
		}
	}
	if response, _ := get(t, server.URL+"/tenants/nobody"); response.StatusCode != http.StatusNotFound {
		t.Errorf("an unknown tenant: %d", response.StatusCode)
	}
}

// fakeBackend wraps the demo, with a failure or extra events.
type fakeBackend struct {
	Demo
	fail   error
	events []store.Event
}

func (f fakeBackend) Nodes(ctx context.Context) (api.Nodes, error) {
	if f.fail != nil {
		return api.Nodes{}, f.fail
	}
	return f.Demo.Nodes(ctx)
}

func (f fakeBackend) Events(ctx context.Context, query store.EventQuery) ([]store.Event, error) {
	if f.events == nil {
		return f.Demo.Events(ctx, query)
	}
	var out []store.Event
	for _, e := range f.events {
		if query.Before == 0 || e.ID < query.Before {
			out = append(out, e)
		}
		if len(out) == query.Limit {
			break
		}
	}
	return out, nil
}

// Events page back a hundred at a time; what an event says is escaped.
func TestTheEventsPage(t *testing.T) {
	var events []store.Event
	for id := int64(150); id > 0; id-- {
		events = append(events, store.Event{ID: id, At: demoNow, Actor: "root", Action: "tenant.created", Detail: "<b>bold</b>"})
	}
	server := newTestServer(t, fakeBackend{Demo: Demo{Now: demoNow}, events: events})
	_, body := get(t, server.URL+"/events")
	if !strings.Contains(body, "before=51") || strings.Contains(body, "<b>bold</b>") || !strings.Contains(body, "&lt;b&gt;bold&lt;/b&gt;") {
		t.Fatalf("events page:\n%s", body)
	}
	_, older := get(t, server.URL+"/events?before=51")
	if strings.Contains(older, "before=") || strings.Count(older, "tenant.created") != 50 {
		t.Fatalf("the older page: %d events", strings.Count(older, "tenant.created"))
	}
}

// When the agent does not answer, the page says so.
func TestAnAgentThatFails(t *testing.T) {
	server := newTestServer(t, fakeBackend{Demo: Demo{Now: demoNow}, fail: errors.New("reach the murder agent: connection refused")})
	response, body := get(t, server.URL+"/")
	if response.StatusCode != http.StatusBadGateway || !strings.Contains(body, "connection refused") {
		t.Fatalf("status %d:\n%s", response.StatusCode, body)
	}
}

func TestAgo(t *testing.T) {
	for _, c := range []struct {
		d    time.Duration
		want string
	}{{5 * time.Second, "5s"}, {3 * time.Minute, "3m"}, {5 * time.Hour, "5h"}, {72 * time.Hour, "3d"}, {-time.Second, "now"}} {
		if got := ago(demoNow, demoNow.Add(-c.d)); got != c.want {
			t.Errorf("ago(%s) = %q, want %q", c.d, got, c.want)
		}
	}
	if ago(demoNow, time.Time{}) != "—" || shortImage("sha256:c23c323553d0a47e") != "c23c323553d0" {
		t.Error("zero time or image")
	}
}
