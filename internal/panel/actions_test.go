package panel

import (
	"context"
	"io"
	"log/slog"
	"net/http"
	"net/http/cookiejar"
	"net/http/httptest"
	"net/url"
	"regexp"
	"strings"
	"testing"
	"time"

	"github.com/PowerPenguini/murder/internal/store"
)

// A panel acting on the demo cluster, as a browser on its own pages.
func newActingPanel(t *testing.T) (*DemoCluster, *httptest.Server, *http.Client) {
	t.Helper()
	cluster := NewDemoCluster()
	s, err := New(cluster, slog.New(slog.NewTextHandler(io.Discard, nil)))
	if err != nil {
		t.Fatal(err)
	}
	s.Actions = cluster.As
	server := httptest.NewServer(s.Handler())
	t.Cleanup(server.Close)
	jar, _ := cookiejar.New(nil)
	return cluster, server, &http.Client{Jar: jar}
}

func submit(t *testing.T, client *http.Client, server *httptest.Server, path string, form url.Values, origin string) (*http.Response, string) {
	t.Helper()
	request, _ := http.NewRequest(http.MethodPost, server.URL+path, strings.NewReader(form.Encode()))
	request.Header.Set("Content-Type", "application/x-www-form-urlencoded")
	request.Header.Set("Origin", origin)
	response, err := client.Do(request)
	if err != nil {
		t.Fatal(err)
	}
	defer response.Body.Close()
	body, _ := io.ReadAll(response.Body)
	return response, string(body)
}

// Each action does its one thing, says so once on the page it sends to,
// and is in the audit log; dangerous ones need the tenant's name typed.
func TestActingOnTenants(t *testing.T) {
	cluster, server, client := newActingPanel(t)
	ctx := context.Background()
	// Suspending stops it serving: the name is typed.
	if _, body := submit(t, client, server, "/tenants/atlas", url.Values{"do": {"suspend"}}, server.URL); !strings.Contains(body, "type the tenant&#39;s name to suspend it") {
		t.Fatalf("suspended without the name:\n%s", body)
	}
	_, body := submit(t, client, server, "/tenants/atlas", url.Values{"do": {"suspend"}, "confirm": {"atlas"}}, server.URL)
	if !strings.Contains(body, "atlas is suspended") || !strings.Contains(body, ">suspended<") {
		t.Fatalf("after suspending:\n%s", body)
	}
	// Said once: a reload does not say it again.
	if response, err := client.Get(server.URL + "/tenants/atlas"); err == nil {
		again, _ := io.ReadAll(response.Body)
		response.Body.Close()
		if strings.Contains(string(again), "atlas is suspended") {
			t.Fatal("the action's notice shown twice")
		}
	}
	if events, _ := cluster.Events(ctx, store.EventQuery{Tenant: "atlas", Limit: 1}); events[0].Action != "tenant.suspended" || who(events[0].Actor) != "the local panel" {
		t.Fatalf("event %+v", events[0])
	}
	// Archiving without the name typed does nothing.
	_, body = submit(t, client, server, "/tenants/atlas", url.Values{"do": {"archive"}, "confirm": {"atla"}}, server.URL)
	if !strings.Contains(body, "type the tenant&#39;s name") {
		t.Fatalf("an archive unconfirmed:\n%s", body)
	}
	if tenant, _ := cluster.Get(ctx, "atlas"); tenant.Archived() {
		t.Fatal("archived unconfirmed")
	}
	submit(t, client, server, "/tenants/atlas", url.Values{"do": {"archive"}, "confirm": {"atlas"}}, server.URL)
	if tenant, _ := cluster.Get(ctx, "atlas"); !tenant.Archived() {
		t.Fatal("not archived")
	}
	// Purged, it is gone, and the page is the tenants'.
	response, body := submit(t, client, server, "/tenants/atlas", url.Values{"do": {"purge"}, "confirm": {"atlas"}}, server.URL)
	if response.Request.URL.Path != "/tenants" || !strings.Contains(body, "atlas is purged") {
		t.Fatalf("after purging: %s", response.Request.URL)
	}
	if _, err := cluster.Get(ctx, "atlas"); err == nil {
		t.Fatal("purged, still there")
	}
	// A move, and a tenant made.
	// A move goes on after the page is back: the page says it began.
	if _, body := submit(t, client, server, "/tenants/acme-logistics", url.Values{"do": {"move"}, "to": {"nbg-1"}}, server.URL); !strings.Contains(body, "moves to nbg-1") && !strings.Contains(body, "Moved to nbg-1") {
		t.Fatalf("after a move began:\n%s", body)
	}
	for i := 0; ; i++ {
		if tenant, _ := cluster.Get(ctx, "acme-logistics"); tenant.Node == "nbg-1" {
			break
		}
		if i == 100 {
			t.Fatal("not moved")
		}
		time.Sleep(10 * time.Millisecond)
	}
	// A page following the cluster leaves an action's notice to the page
	// it was meant for.
	cookieURL, _ := url.Parse(server.URL)
	client.Jar.SetCookies(cookieURL, []*http.Cookie{{Name: flashCookie, Value: url.QueryEscape("done:moved"), Path: "/"}})
	live, _ := http.NewRequest(http.MethodGet, server.URL+"/", nil)
	live.Header.Set("X-Murder-Live", "1")
	if response, err := client.Do(live); err == nil {
		page, _ := io.ReadAll(response.Body)
		response.Body.Close()
		if strings.Contains(string(page), `class="notice" role="status">moved<`) {
			t.Fatal("a live fetch took the notice")
		}
	}
	if response, err := client.Get(server.URL + "/"); err == nil {
		page, _ := io.ReadAll(response.Body)
		response.Body.Close()
		if !strings.Contains(string(page), `class="notice" role="status">moved<`) {
			t.Fatal("the notice was lost")
		}
	}
	response, body = submit(t, client, server, "/tenants", url.Values{"name": {"newco"}, "app": {"web"}}, server.URL)
	if response.Request.URL.Path != "/tenants/newco" || !strings.Contains(body, "newco is created") {
		t.Fatalf("created: %s", response.Request.URL)
	}
	// Without a node chosen, the least busy one; or the one named.
	if _, page := get(t, server.URL+"/tenants"); !strings.Contains(page, `<option value="auto" selected>`) {
		t.Fatalf("the form offers no node:\n%s", page)
	}
	if !regexp.MustCompile(`newco is created on [a-z]+-1`).MatchString(body) {
		t.Fatalf("created where: %s", body)
	}
	if _, body = submit(t, client, server, "/tenants", url.Values{"name": {"there"}, "app": {"web"}, "node": {"nbg-1"}}, server.URL); !strings.Contains(body, "there is created on nbg-1") {
		t.Fatalf("created on a named node:\n%s", body)
	}
	if _, body = submit(t, client, server, "/tenants", url.Values{"name": {"Bad Name"}, "app": {"web"}}, server.URL); !strings.Contains(body, "lowercase letters") {
		t.Fatal("a bad name taken")
	}
	// From elsewhere, nothing.
	if response, _ := submit(t, client, server, "/tenants/newco", url.Values{"do": {"suspend"}}, "https://evil.example"); response.StatusCode != http.StatusForbidden {
		t.Fatalf("an action from elsewhere: %d", response.StatusCode)
	}
	// Logs, without terminal escapes.
	response, err := client.Get(server.URL + "/tenants/newco/logs")
	if err != nil {
		t.Fatal(err)
	}
	logs, _ := io.ReadAll(response.Body)
	response.Body.Close()
	if !strings.Contains(string(logs), "slow query") || strings.Contains(string(logs), "\x1b") {
		t.Fatalf("logs:\n%s", logs)
	}
}
