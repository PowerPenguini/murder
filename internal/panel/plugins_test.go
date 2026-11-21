package panel

import (
	"context"
	"encoding/csv"
	"io"
	"log/slog"
	"net/http"
	"net/http/cookiejar"
	"net/http/httptest"
	"net/url"
	"strings"
	"testing"
	"time"

	"github.com/PowerPenguini/murder/internal/api"
	"github.com/PowerPenguini/murder/internal/plugins"
	"github.com/PowerPenguini/murder/internal/store"
)

// A panel with the Plugins pages over the demo cluster.
func newPluginsPanel(t *testing.T) (*DemoCluster, *httptest.Server, *http.Client) {
	t.Helper()
	cluster := NewDemoCluster()
	s, err := New(cluster, slog.New(slog.NewTextHandler(io.Discard, nil)))
	if err != nil {
		t.Fatal(err)
	}
	s.Actions = cluster.As
	s.Plugins = cluster.AsPlugins
	server := httptest.NewServer(s.Handler())
	t.Cleanup(server.Close)
	jar, _ := cookiejar.New(nil)
	return cluster, server, &http.Client{Jar: jar}
}

// The Plugins page lists the plugins, what kind each is, and the tenants
// at a limit or near one, the one refused first; the overview says them
// too, and the side bar counts the refused one as broken.
func TestThePluginsPageSaysWhoIsAtALimit(t *testing.T) {
	_, server, _ := newPluginsPanel(t)
	_, body := get(t, server.URL+"/plugins")
	until := plugins.PeriodEnd(plugins.PerMonth, time.Now()).UTC().Format("2 Jan 15:04")
	for _, want := range []string{
		`href="/plugins/anthropic"`, "container · anthropic 1.0.0", "Anthropic",
		`href="/plugins/openai"`, "5M tokens a month", "20k requests a day, soft",
		"acme-logistics</a> used all its 1M tokens a month of", "its requests to it are refused until " + until + " UTC",
		"baltic-freight</a> used 214k of its 250k transactions a month",
	} {
		if !strings.Contains(body, want) {
			t.Errorf("the Plugins page lacks %q", want)
		}
	}
	if strings.Index(body, "acme-logistics</a> used all") > strings.Index(body, "baltic-freight</a> used") {
		t.Error("a tenant refused is not said first")
	}
	_, overview := get(t, server.URL+"/")
	if !strings.Contains(overview, "used all its 1M tokens a month of plugin openai: its requests to it are refused") || !strings.Contains(overview, `href="/plugins/openai?tenant=acme-logistics#edit"`) {
		t.Errorf("the overview does not say the tenant refused:\n%s", overview)
	}
	if !strings.Contains(overview, `<a href="/plugins"`) {
		t.Error("no Plugins in the side bar")
	}
}

// Without the Plugins pages, nothing of them shows.
func TestNoPluginsPagesWithoutTheAgentsPlugins(t *testing.T) {
	server := newTestServer(t, NewDemoCluster())
	if _, body := get(t, server.URL+"/"); strings.Contains(body, `href="/plugins"`) || strings.Contains(body, "of plugin") {
		t.Fatal("the overview says plugins it was not given")
	}
	if response, _ := get(t, server.URL+"/plugins"); response.StatusCode != http.StatusNotFound {
		t.Fatalf("/plugins answered %d", response.StatusCode)
	}
}

// A plugin's page says it as its module's UI does: its title, its
// metrics' labels, the field of its config, and what counts are kept by.
func TestAPluginsPageIsShownAsItsModuleSays(t *testing.T) {
	_, server, _ := newPluginsPanel(t)
	_, body := get(t, server.URL+"/plugins/anthropic")
	for _, want := range []string{
		"Claude through Anthropic&#39;s Messages API", ">Read from the cache<", ">Written to the cache<", "More endpoints",
		"POST /v1/messages/batches", "api.anthropic.com", "header X-Api-Key: KEY", "By model", "claude-sonnet-5-5",
		"vistula-cargo", "Tokens, by day", ">Set the key",
	} {
		if !strings.Contains(body, want) {
			t.Errorf("the anthropic page lacks %q", want)
		}
	}
	_, here := get(t, server.URL+"/plugins/here?metric=requests")
	if !strings.Contains(here, "Requests, by day") || !strings.Contains(here, "By service") || !strings.Contains(here, "geocode.search") {
		t.Errorf("here's requests:\n%s", here)
	}
	if response, _ := get(t, server.URL+"/plugins/nothing"); response.StatusCode != http.StatusNotFound {
		t.Fatalf("a plugin not there: %d", response.StatusCode)
	}
}

// A tenant's limits are changed from the plugin's page: refused as typed
// when they cannot be, kept and in the audit log when they can.
func TestATenantsLimitsChangeFromThePluginsPage(t *testing.T) {
	cluster, server, client := newPluginsPanel(t)
	ctx := context.Background()
	_, form := get(t, server.URL+"/plugins/openai?tenant=acme-logistics")
	if !strings.Contains(form, "acme-logistics's limits of openai") || !strings.Contains(form, "tokens=1000000/month:hard") {
		t.Fatalf("the form:\n%s", form)
	}
	response, body := submit(t, client, server, "/plugins/openai", url.Values{"do": {"limits"}, "tenant": {"acme-logistics"}, "mode": {"own"}, "limits": {"transactions=5/day"}}, server.URL)
	if response.StatusCode != http.StatusUnprocessableEntity || !strings.Contains(body, "transactions") || !strings.Contains(body, "transactions=5/day") {
		t.Fatalf("a metric openai does not count: %d\n%s", response.StatusCode, body)
	}
	_, body = submit(t, client, server, "/plugins/openai", url.Values{"do": {"limits"}, "tenant": {"acme-logistics"}, "mode": {"own"}, "limits": {"tokens=5M/month\nrequests=1000/day:soft"}}, server.URL)
	if !strings.Contains(body, "acme-logistics has limits of its own of openai") {
		t.Fatalf("after setting limits:\n%s", body)
	}
	uses, _ := cluster.AsPlugins("x").PluginUses(ctx)
	for _, use := range uses {
		if use.Tenant == "acme-logistics" && use.Plugin == "openai" && (len(use.Limits) != 2 || use.Limits[1].Amount != 5_000_000 || use.Limits[0].Hard()) {
			t.Fatalf("its limits %+v", use.Limits)
		}
	}
	if events, _ := cluster.Events(ctx, store.EventQuery{Tenant: "acme-logistics", Limit: 1}); events[0].Action != "plugin.tenant-limits-set" || !strings.Contains(events[0].Detail, "tokens=5000000/month:hard") {
		t.Fatalf("event %+v", events[0])
	}
	// The plugin's again.
	if _, body = submit(t, client, server, "/plugins/openai", url.Values{"do": {"limits"}, "tenant": {"acme-logistics"}, "mode": {"plugin"}}, server.URL); !strings.Contains(body, "acme-logistics has the plugin&#39;s limits of openai") {
		t.Fatalf("back to the plugin's:\n%s", body)
	}
}

// A plugin's key is set from its page, and removed only with its name
// typed; a key is never shown.
func TestAPluginsKeyIsSetAndRemovedFromItsPage(t *testing.T) {
	cluster, server, client := newPluginsPanel(t)
	ctx := context.Background()
	_, body := submit(t, client, server, "/plugins/anthropic", url.Values{"do": {"key"}, "key": {"sk-ant-secret"}}, server.URL)
	if !strings.Contains(body, "plugin anthropic&#39;s key is set") || strings.Contains(body, "sk-ant-secret") {
		t.Fatalf("after setting the key:\n%s", body)
	}
	_, body = submit(t, client, server, "/plugins/anthropic", url.Values{"do": {"key-remove"}}, server.URL)
	if !strings.Contains(body, "type anthropic to remove its key") {
		t.Fatalf("removed without the name:\n%s", body)
	}
	if list, _ := cluster.AsPlugins("x").Plugins(ctx); !list[0].HasKey {
		t.Fatal("the key went without the name typed")
	}
	_, body = submit(t, client, server, "/plugins/anthropic", url.Values{"do": {"key-remove"}, "confirm": {"anthropic"}}, server.URL)
	if list, _ := cluster.AsPlugins("x").Plugins(ctx); list[0].HasKey || !strings.Contains(body, "answered 503") {
		t.Fatalf("after removing the key:\n%s", body)
	}
	// A tenant's own.
	_, body = submit(t, client, server, "/plugins/openai", url.Values{"do": {"key"}, "tenant": {"atlas"}, "key": {"sk-own"}}, server.URL)
	if !strings.Contains(body, "atlas has a key of its own of openai") {
		t.Fatalf("a tenant's own key:\n%s", body)
	}
}

// The export is CSV of each day's counts, this month's by default.
func TestAPluginsUsageExportsAsCSV(t *testing.T) {
	_, server, _ := newPluginsPanel(t)
	now := time.Now().UTC()
	response, body := get(t, server.URL+"/plugins/here/usage.csv?from="+plugins.Month(now)+"-01&to="+plugins.Day(now))
	if response.Header.Get("Content-Type") != "text/csv; charset=utf-8" || !strings.Contains(response.Header.Get("Content-Disposition"), "here-") {
		t.Fatalf("headers %v", response.Header)
	}
	rows, err := csv.NewReader(strings.NewReader(body)).ReadAll()
	if err != nil || len(rows) < 2 || strings.Join(rows[0], ",") != "tenant,plugin,day,metric,detail,amount" {
		t.Fatalf("%v %v", rows, err)
	}
	for _, row := range rows[1:] {
		if row[1] != "here" || !strings.HasPrefix(row[2], plugins.Month(now)) {
			t.Fatalf("row %v", row)
		}
	}
}

// A tenant's page shows its plugins against its limits, the refused one
// said so.
func TestATenantsPageShowsItsPlugins(t *testing.T) {
	_, server, _ := newPluginsPanel(t)
	_, body := get(t, server.URL+"/tenants/acme-logistics")
	until := plugins.PeriodEnd(plugins.PerMonth, time.Now()).UTC().Format("2 Jan 15:04")
	for _, want := range []string{`id="plugins"`, `href="/plugins/openai"`, "1M of 1M tokens a month", "refused until " + until + " UTC", "of 250k transactions a month"} {
		if !strings.Contains(body, want) {
			t.Errorf("the tenant's page lacks %q", want)
		}
	}
	if _, body := get(t, server.URL+"/tenants/test-staging"); strings.Contains(body, `id="plugins"`) {
		t.Error("a tenant that uses no plugin has a Plugins box")
	}
}

// Amounts read short, limits with their units, and a limit near its
// amount is said near.
func TestAmountsAndLimitsAreSaidShort(t *testing.T) {
	for n, want := range map[int64]string{950: "950", 12_500: "12.5k", 1_000_000: "1M", 3_250_000: "3.2M", 250_000: "250k", 1_100_000_000: "1.1G"} {
		if got := amountText(n); got != want {
			t.Errorf("%d said %q, want %q", n, got, want)
		}
	}
	ui := plugins.UIOf(plugins.Spec{Kind: plugins.KindContainer}, &demoOpenAI)
	l := limitViewOf(api.LimitUse{Limit: plugins.Limit{Metric: "tokens", Per: plugins.PerMonth, Amount: 1000}, Used: 850}, ui, time.Date(2026, 11, 20, 0, 0, 0, 0, time.UTC))
	if l.Said != "1000 tokens a month" || !l.Near || l.Reached || l.Until != time.Date(2026, 12, 1, 0, 0, 0, 0, time.UTC) {
		t.Fatalf("%+v", l)
	}
}
