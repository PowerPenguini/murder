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

	"github.com/PowerPenguini/murder/internal/api"
)

// An app's change is reviewed before it is stored: the lines it changes,
// the tenants it reaches with what they keep, and its waves; going back to
// an older revision too. A spec that does not read is said beside it.
func TestAppChangesAreReviewed(t *testing.T) {
	cluster, server, client := newAppsPanel(t)
	ctx := context.Background()
	agent := cluster.AsApps("test")
	_, page := get(t, server.URL+"/apps/web")
	updated := between(page, `name="updated" value="`, `"`)
	file := between(page, `aria-label="the app's spec">`, "</textarea>")
	changed := strings.Replace(file, "memoryMB: 256", "memoryMB: 512", 1)
	_, body := submit(t, client, server, "/apps/web", url.Values{"do": {"review"}, "file": {changed}, "updated": {updated}}, server.URL)
	for _, want := range []string{"What changes", `<span class="tx" role="cell">  memoryMB: 512</span>`, "1 line out, 1 line in", "Canaries", "atlas", "FEATURE_PLANNER", `name="do" value="save"`, "Roll out to 6 tenants", "Held, and left out until released", "A rollout of revision 3 runs"} {
		if !strings.Contains(body, want) {
			t.Fatalf("the review lacks %q:\n%s", want, body)
		}
	}
	if app, _ := agent.App(ctx, "web"); app.Revision != 3 {
		t.Fatalf("a review stored the change: revision %d", app.Revision)
	}
	// Back to editing keeps the text.
	if _, body = submit(t, client, server, "/apps/web", url.Values{"do": {"edit"}, "file": {changed}, "updated": {updated}}, server.URL); !strings.Contains(body, "memoryMB: 512") {
		t.Fatalf("back to editing lost the text:\n%s", body)
	}
	// A spec that does not read is said beside the editor, the text kept.
	_, body = submit(t, client, server, "/apps/web", url.Values{"do": {"review"}, "file": {strings.Replace(file, "port: 8080", "port: eighty", 1)}, "updated": {updated}}, server.URL)
	if !strings.Contains(body, `id="spec-problem"`) || !strings.Contains(body, "port: eighty") || !strings.Contains(body, `aria-invalid="true"`) {
		t.Fatalf("a spec that does not read:\n%s", body)
	}
	// Going back is reviewed too, its image's line changed.
	_, body = submit(t, client, server, "/apps/web", url.Values{"do": {"review-restore"}, "revision": {"2"}, "updated": {updated}}, server.URL)
	if !strings.Contains(body, "Going back to revision 2") || !strings.Contains(body, strings.Repeat("a1", 32)) || !strings.Contains(body, `name="revision" value="2"`) || !strings.Contains(body, `value="restore"`) {
		t.Fatalf("going back's review:\n%s", body)
	}
	// Rolled out from the review, it is stored.
	_, body = submit(t, client, server, "/apps/web", url.Values{"do": {"save"}, "file": {changed}, "updated": {updated}}, server.URL)
	if app, _ := agent.App(ctx, "web"); app.Revision != 4 || app.Spec.Limits.MemoryMB != 512 {
		t.Fatalf("not stored: %+v\n%s", app, body)
	}
}

// A rollout's page shows its waves, each tenant's place and what blocks it.
func TestRolloutProgress(t *testing.T) {
	_, server, _ := newAppsPanel(t)
	_, page := get(t, server.URL+"/apps/web")
	for _, want := range []string{`<li class="done"><span class="wn">Canaries</span>`, `<li class="now"><span class="wn">A tenth</span>`, "moved, not healthy: blocks the next wave", "waits for wave 3", "stays: it is held", "Half (kowalski-trans) moves once baltic-freight is healthy", "Roll out to the rest now", "Restore data snapshot"} {
		if !strings.Contains(page, want) {
			t.Fatalf("the rollout lacks %q:\n%s", want, page)
		}
	}
}

func TestPlanWaves(t *testing.T) {
	cluster := NewDemoCluster()
	tenants, _ := cluster.List(context.Background())
	var live []api.Tenant
	for _, t := range tenants {
		if !t.Archived() && !t.Hold {
			live = append(live, t)
		}
	}
	waves := planWaves(live)
	var said []string
	for _, w := range waves {
		said = append(said, w.Name+": "+strings.Join(w.Tenants, " "))
	}
	if got := strings.Join(said, "; "); got != "Canaries: acme-logistics atlas; A tenth: baltic-freight; Half: kowalski-trans; The rest: nordwind vistula-cargo" {
		t.Fatalf("waves: %s", got)
	}
}

// A tenant's settings say each value in effect and its source; a value
// switched off takes the app's; a save is reviewed first.
func TestSettingsSayTheirSource(t *testing.T) {
	cluster, server, client := newAppsPanel(t)
	ctx := context.Background()
	_, page := get(t, server.URL+"/tenants/atlas/settings")
	for _, want := range []string{"768 MB", "its own", "from app web", "FEATURE_PLANNER", "over the app's", "Review changes", "Save rollout flags"} {
		if !strings.Contains(page, want) {
			t.Fatalf("the settings lack %q:\n%s", want, page)
		}
	}
	generation := between(page, `name="generation" value="`, `"`)
	form := url.Values{"owns": {"1"}, "generation": {generation}, "app": {"web"}, "memoryMB": {"768"}, "env": {"TZ=Europe/Berlin\nFEATURE_PLANNER=on"}, "secrets": {"ADMIN_PASSWORD password 20"}}
	form.Set("do", "review")
	_, body := submit(t, client, server, "/tenants/atlas/settings", form, server.URL)
	if !strings.Contains(body, "Review: 1 change") || !strings.Contains(body, "768 MB, its own") || !strings.Contains(body, "256 MB, app web&#39;s") || !strings.Contains(body, "Save and restart atlas") {
		t.Fatalf("the review:\n%s", body)
	}
	if tenant, _ := cluster.Get(ctx, "atlas"); tenant.Overrides.Limits.MemoryMB != 768 {
		t.Fatal("a review saved")
	}
	form.Set("do", "save")
	submit(t, client, server, "/tenants/atlas/settings", form, server.URL)
	if tenant, _ := cluster.Get(ctx, "atlas"); tenant.Overrides.Limits.MemoryMB != 0 || tenant.Spec.Limits.MemoryMB != 256 {
		t.Fatalf("switched off, memory is still its own: %+v", tenant.Overrides.Limits)
	}
}

// A failed node's page says what failed over from it and where it runs
// now, and what to do; a node's backups and a tenant's show.
func TestNodePageAndBackups(t *testing.T) {
	cluster := NewDemoCluster()
	server := newServerOf(t, cluster)
	_, page := get(t, server.URL+"/nodes/hel-1")
	for _, want := range []string{"Failed over from hel-1", `href="/tenants/vistula-cargo"`, "nbg-1", "sudo murder invite", "Remove hel-1 from the cluster", `name="from" value="node"`} {
		if !strings.Contains(page, want) {
			t.Fatalf("the failed node's page lacks %q:\n%s", want, page)
		}
	}
	if response, _ := get(t, server.URL+"/nodes/nowhere"); response.StatusCode != http.StatusNotFound {
		t.Fatalf("an unknown node: %d", response.StatusCode)
	}
	_, page = get(t, server.URL+"/cluster")
	for _, want := range []string{`id="backups"`, "every 1h", "1 failing", "kowalski-trans</a> its last backup failed", "sudo murder backups restore"} {
		if !strings.Contains(page, want) {
			t.Fatalf("the cluster page lacks %q:\n%s", want, page)
		}
	}
	_, page = get(t, server.URL+"/tenants/kowalski-trans")
	if !strings.Contains(page, "Newest backup") || !strings.Contains(page, "last try failed") {
		t.Fatalf("the tenant's backup:\n%s", page)
	}
	if _, page = get(t, server.URL+"/"); !strings.Contains(page, "kowalski-trans</a> its last backup failed") {
		t.Fatal("the overview does not say a backup fails")
	}
}

// Logs filter on the server by loudness and text, say how many match, and
// say when none do.
func TestLogsFilter(t *testing.T) {
	cluster := NewDemoCluster()
	server := newServerOf(t, cluster)
	_, page := get(t, server.URL+"/tenants/baltic-freight/logs?level=error")
	if !strings.Contains(page, "1 of 15 lines match") || !strings.Contains(page, "cannot allocate memory") || strings.Contains(page, "slow query") {
		t.Fatalf("errors only:\n%s", page)
	}
	if !strings.Contains(page, `class="t-short"`) || !strings.Contains(page, "Restart container") || !strings.Contains(page, ">unhealthy<") {
		t.Fatalf("times or the header:\n%s", page)
	}
	if _, page = get(t, server.URL+"/tenants/baltic-freight/logs?q=nothing-like-this"); !strings.Contains(page, "No lines match") {
		t.Fatalf("no match:\n%s", page)
	}
	if _, page = get(t, server.URL+"/tenants/baltic-freight/logs?follow=1"); !strings.Contains(page, "data-follow") || !strings.Contains(page, "Pause") {
		t.Fatalf("following:\n%s", page)
	}
}

// newServerOf serves the demo cluster with every part of the panel.
func newServerOf(t *testing.T, cluster *DemoCluster) *httptest.Server {
	t.Helper()
	s, err := New(cluster, slog.New(slog.NewTextHandler(io.Discard, nil)))
	if err != nil {
		t.Fatal(err)
	}
	s.Actions, s.Apps, s.Cluster = cluster.As, cluster.AsApps, cluster.AsCluster
	server := httptest.NewServer(s.Handler())
	t.Cleanup(server.Close)
	return server
}
