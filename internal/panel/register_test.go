package panel

import (
	"context"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strings"
	"testing"
	"time"

	"github.com/PowerPenguini/murder/internal/access"
	"github.com/PowerPenguini/murder/internal/api"
	"github.com/PowerPenguini/murder/internal/store"
)

// openLogs records, as the panel's form does, why a tenant's logs are
// read, so the tests read them after.
func openLogs(t *testing.T, client *http.Client, server *httptest.Server, tenant string) {
	t.Helper()
	response, body := submit(t, client, server, "/tenants/"+tenant+"/logs", url.Values{"purpose": {"a test"}, "ticket": {"incident"}}, server.URL)
	if response.StatusCode != http.StatusOK || response.Request.URL.Path != "/tenants/"+tenant+"/logs" || strings.Contains(body, `name="purpose"`) {
		t.Fatalf("the logs of %s did not open: %d %s\n%s", tenant, response.StatusCode, response.Request.URL, body)
	}
}

// accesses are the demo's access events of tenant.
func accesses(t *testing.T, cluster *DemoCluster, tenant string) []access.Entry {
	t.Helper()
	events, err := cluster.Events(context.Background(), store.EventQuery{Tenant: tenant})
	if err != nil {
		t.Fatal(err)
	}
	var entries []access.Entry
	for _, e := range events {
		if entry, ok := access.FromEvent(e); ok {
			entries = append(entries, entry)
		}
	}
	return entries
}

// A tenant's logs open only once the person says why, which the register
// records; then they read them for a while without saying it again, and
// the page names the entry they read under.
func TestLogsAskWhyBeforeTheyOpen(t *testing.T) {
	cluster, server, client := newActingPanel(t)
	response, err := client.Get(server.URL + "/tenants/atlas/logs?level=warn")
	if err != nil {
		t.Fatal(err)
	}
	page, _ := io.ReadAll(response.Body)
	response.Body.Close()
	if !strings.Contains(string(page), `name="purpose"`) || !strings.Contains(string(page), "access register") || strings.Contains(string(page), `id="log-lines"`) {
		t.Fatalf("logs shown before saying why:\n%s", page)
	}
	if got := accesses(t, cluster, "atlas"); len(got) != 0 {
		t.Fatalf("an access recorded for a look at the form: %+v", got)
	}
	// Without a ticket, refused, what was given kept.
	response, body := submit(t, client, server, "/tenants/atlas/logs?level=warn", url.Values{"purpose": {"export fails"}}, server.URL)
	if response.StatusCode != http.StatusUnprocessableEntity || !strings.Contains(body, "-ticket") || !strings.Contains(body, `value="export fails"`) || strings.Contains(body, `id="log-lines"`) {
		t.Fatalf("taken without a ticket: %d\n%s", response.StatusCode, body)
	}
	// From elsewhere, nothing.
	if response, _ := submit(t, client, server, "/tenants/atlas/logs", url.Values{"purpose": {"x"}, "ticket": {"incident"}}, "https://evil.example"); response.StatusCode != http.StatusForbidden {
		t.Fatalf("an access recorded from elsewhere: %d", response.StatusCode)
	}
	if got := accesses(t, cluster, "atlas"); len(got) != 0 {
		t.Fatalf("refused accesses recorded: %+v", got)
	}
	response, body = submit(t, client, server, "/tenants/atlas/logs?level=warn", url.Values{"purpose": {"export fails"}, "ticket": {"ZD-1234"}}, server.URL)
	if response.Request.URL.RequestURI() != "/tenants/atlas/logs?level=warn" || strings.Contains(body, `name="purpose"`) || !strings.Contains(body, "Read under access") || !strings.Contains(body, "ZD-1234") {
		t.Fatalf("the logs, after saying why: %s\n%s", response.Request.URL, body)
	}
	got := accesses(t, cluster, "atlas")
	if len(got) != 1 || got[0].Kind != access.Logs || got[0].Purpose != "export fails" || got[0].Ticket != "ZD-1234" || got[0].Actor != "murder-panel socket via murder-panel" {
		t.Fatalf("recorded: %+v", got)
	}
	// Read again, and followed: the same entry.
	for range 3 {
		if response, err := client.Get(server.URL + "/tenants/atlas/logs?follow=1"); err != nil || response.StatusCode != http.StatusOK {
			t.Fatalf("read again: %v", err)
		} else {
			response.Body.Close()
		}
	}
	if got := accesses(t, cluster, "atlas"); len(got) != 1 {
		t.Fatalf("each read recorded again: %d", len(got))
	}
	// Another tenant's logs ask again.
	if response, err := client.Get(server.URL + "/tenants/acme-logistics/logs"); err != nil {
		t.Fatal(err)
	} else {
		page, _ := io.ReadAll(response.Body)
		response.Body.Close()
		if !strings.Contains(string(page), `name="purpose"`) {
			t.Fatal("another tenant's logs opened under atlas's access")
		}
	}
	// The events page says what was read, why, at whose request.
	if response, err := client.Get(server.URL + "/events"); err != nil {
		t.Fatal(err)
	} else {
		page, _ := io.ReadAll(response.Body)
		response.Body.Close()
		if !strings.Contains(string(page), "had its logs read") || !strings.Contains(string(page), "“export fails” · ticket ZD-1234") {
			t.Fatalf("the events page:\n%s", page)
		}
	}
}

// refusingAgent refuses every read of logs, as the agent does once an
// access ends.
type refusingAgent struct{ ActionAgent }

func (refusingAgent) Logs(context.Context, string, int, access.Proof) ([]byte, error) {
	return nil, &api.Error{Status: http.StatusForbidden, Message: "not recorded in the access register: access 7 was recorded at 2026-11-07T10:00:00Z and serves 1h0m0s: say why again"}
}

// An access ends: in the panel a minute before the agent ends it, and at
// once when the agent refuses it; the page asks again, saying why.
func TestAnEndedAccessAsksAgain(t *testing.T) {
	cluster := NewDemoCluster()
	s, err := New(cluster, slog.New(slog.NewTextHandler(io.Discard, nil)))
	if err != nil {
		t.Fatal(err)
	}
	now := time.Now()
	s.now = func() time.Time { return now }
	refuse := false
	s.Actions = func(actor string) ActionAgent {
		if refuse {
			return refusingAgent{cluster.As(actor)}
		}
		return cluster.As(actor)
	}
	server := httptest.NewServer(s.Handler())
	t.Cleanup(server.Close)
	client := server.Client()
	openLogs(t, client, server, "atlas")
	now = now.Add(access.GrantFor - 30*time.Second)
	if _, page := get(t, server.URL+"/tenants/atlas/logs"); !strings.Contains(page, `name="purpose"`) {
		t.Fatal("an access about to end still read under")
	}
	// The demo stamps entries by the system's clock.
	now = time.Now()
	openLogs(t, client, server, "atlas")
	refuse = true
	_, page := get(t, server.URL+"/tenants/atlas/logs")
	if !strings.Contains(page, `name="purpose"`) || !strings.Contains(page, "say why again") || !strings.Contains(page, `value="a test"`) {
		t.Fatalf("a refused access:\n%s", page)
	}
	if got := accesses(t, cluster, "atlas"); len(got) != 2 {
		t.Fatalf("accesses: %+v", got)
	}
}
