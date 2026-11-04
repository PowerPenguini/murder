package panel

import (
	"context"
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

// A rollout's page shows its waves, each tenant's place and what blocks it;
// moving the next wave past its health gate needs the app's name.
func TestRolloutProgress(t *testing.T) {
	cluster, server, client := newAppsPanel(t)
	if _, body := submit(t, client, server, "/apps/web", url.Values{"do": {"promote"}}, server.URL); !strings.Contains(body, "type the app&#39;s name to move the next wave") {
		t.Fatalf("moved a wave without the name:\n%s", body)
	}
	if app, _ := cluster.AsApps("t").App(context.Background(), "web"); app.Rollout.Wave != 2 {
		t.Fatalf("moved a wave without the name: wave %d", app.Rollout.Wave)
	}
	_, page := get(t, server.URL+"/apps/web")
	for _, want := range []string{`<li class="done"><span class="wn">Canaries</span>`, `<li class="now"><span class="wn">A tenth</span>`, "moved, not healthy: blocks the next wave", "waits for wave 3", "stays: it is held", "Half (kowalski-trans) moves once baltic-freight is healthy", "Move the next wave now", "baltic-freight is not healthy on revision 3. Moving the next wave now risks the same for kowalski-trans", "Restore data snapshot"} {
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
// not overridden is inherited; a save is reviewed first, and saves exactly
// what was reviewed.
func TestSettingsSayTheirSource(t *testing.T) {
	cluster, server, client := newAppsPanel(t)
	ctx := context.Background()
	_, page := get(t, server.URL+"/tenants/atlas/settings")
	for _, want := range []string{"768 MB", "Tenant override", "Inherited from web", "FEATURE_PLANNER", "over the app's", "Override app value", "Review changes", "Save rollout flags"} {
		if !strings.Contains(page, want) {
			t.Fatalf("the settings lack %q:\n%s", want, page)
		}
	}
	generation := between(page, `name="generation" value="`, `"`)
	form := url.Values{"owns": {"1"}, "generation": {generation}, "app": {"web"}, "own-memoryMB": {"on"}, "memoryMB": {"1100"}, "env": {"TZ=Europe/Berlin\nFEATURE_PLANNER=on"}, "secrets": {"ADMIN_PASSWORD password 20"}}
	form.Set("do", "review")
	_, body := submit(t, client, server, "/tenants/atlas/settings", form, server.URL)
	if !strings.Contains(body, "Review 1 change to atlas") || !strings.Contains(body, "768 MB, tenant override") || !strings.Contains(body, "1100 MB, tenant override") || !strings.Contains(body, "Save and restart atlas") {
		t.Fatalf("the review:\n%s", body)
	}
	// The review is not the editor: the values it shows are what it sends.
	if strings.Contains(body, `id="f-memoryMB"`) || !strings.Contains(body, `name="memoryMB" value="1100"`) {
		t.Fatalf("the review holds an editable form:\n%s", body)
	}
	token := between(body, `name="reviewed" value="`, `"`)
	if tenant, _ := cluster.Get(ctx, "atlas"); tenant.Overrides.Limits.MemoryMB != 768 {
		t.Fatal("a review saved")
	}
	// A save of other values than reviewed is refused.
	saved := url.Values{"generation": {generation}, "app": {"web"}, "memoryMB": {"1200"}, "env": {"TZ=Europe/Berlin\nFEATURE_PLANNER=on"}, "secrets": {"ADMIN_PASSWORD password 20"}, "do": {"save"}, "reviewed": {token}}
	if _, body = submit(t, client, server, "/tenants/atlas/settings", saved, server.URL); !strings.Contains(body, "these are not the values you reviewed") {
		t.Fatalf("saved other values than reviewed:\n%s", body)
	}
	if tenant, _ := cluster.Get(ctx, "atlas"); tenant.Overrides.Limits.MemoryMB != 768 {
		t.Fatalf("saved other values than reviewed: %d", tenant.Overrides.Limits.MemoryMB)
	}
	// Keep editing goes back to the editor, the values kept.
	saved.Set("memoryMB", "1100")
	saved.Set("do", "edit")
	if _, body = submit(t, client, server, "/tenants/atlas/settings", saved, server.URL); !strings.Contains(body, `id="f-memoryMB" name="memoryMB" value="1100"`) {
		t.Fatalf("keep editing:\n%s", body)
	}
	saved.Set("do", "save")
	submit(t, client, server, "/tenants/atlas/settings", saved, server.URL)
	if tenant, _ := cluster.Get(ctx, "atlas"); tenant.Overrides.Limits.MemoryMB != 1100 {
		t.Fatalf("the reviewed values were not saved: %d", tenant.Overrides.Limits.MemoryMB)
	}
	// A value no longer overridden is inherited again.
	page = get2(t, server.URL+"/tenants/atlas/settings")
	generation = between(page, `name="generation" value="`, `"`)
	off := url.Values{"owns": {"1"}, "generation": {generation}, "app": {"web"}, "memoryMB": {"1100"}, "env": {"TZ=Europe/Berlin\nFEATURE_PLANNER=on"}, "secrets": {"ADMIN_PASSWORD password 20"}}
	submit(t, client, server, "/tenants/atlas/settings", reviewed(t, client, server, "/tenants/atlas/settings", off), server.URL)
	if tenant, _ := cluster.Get(ctx, "atlas"); tenant.Overrides.Limits.MemoryMB != 0 || tenant.Spec.Limits.MemoryMB != 256 {
		t.Fatalf("switched off, memory is still overridden: %+v", tenant.Overrides.Limits)
	}
	// "Raise its memory limit" proposes twice the limit, to review.
	if _, page = get(t, server.URL+"/tenants/baltic-freight/settings?memoryMB=512"); !strings.Contains(page, `name="memoryMB" value="512"`) || !strings.Contains(page, "Proposed: memory 512 MB") {
		t.Fatalf("the proposed limit:\n%s", page)
	}
}

func get2(t *testing.T, url string) string {
	_, page := get(t, url)
	return page
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
	for _, want := range []string{`id="backups"`, "every 1h", "1 failing", "sudo murder backups restore"} {
		if !strings.Contains(page, want) {
			t.Fatalf("the cluster page lacks %q:\n%s", want, page)
		}
	}
	_, page = get(t, server.URL+"/tenants/kowalski-trans")
	if !strings.Contains(page, "Newest backup") || !strings.Contains(page, "last try failed") {
		t.Fatalf("the tenant's backup:\n%s", page)
	}
	if _, page = get(t, server.URL+"/"); !strings.Contains(page, "kowalski-trans</a>&#39;s last backup failed; the newest is 3h old") {
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

// A broken tenant, the rollout it blocks and the app-wide cause point at
// each other: on the overview, the tenant and the app.
func TestCauseTenantAndRolloutJoin(t *testing.T) {
	server := newServerOf(t, NewDemoCluster())
	_, page := get(t, server.URL+"/")
	for _, want := range []string{"waits for baltic-freight, not healthy on it", "it blocks web&#39;s rollout of revision 3", "web</a>: all 7 tenants ran out of memory", "Raise web&#39;s memory limit", "/apps/web?memoryMB=512#spec"} {
		if !strings.Contains(page, want) {
			t.Fatalf("the overview lacks %q:\n%s", want, page)
		}
	}
	_, page = get(t, server.URL+"/tenants/baltic-freight")
	for _, want := range []string{"It blocks web's rollout", `href="/apps/web#r-baltic-freight"`, "The cause is likely web's", "Raise web's, for all its tenants"} {
		if !strings.Contains(page, want) {
			t.Fatalf("the tenant lacks %q:\n%s", want, page)
		}
	}
	_, page = get(t, server.URL+"/apps/web?memoryMB=512")
	for _, want := range []string{"memoryMB: 512", "Proposed in the editor below", "also ran out of memory", `id="r-baltic-freight"`} {
		if !strings.Contains(page, want) {
			t.Fatalf("the app lacks %q:\n%s", want, page)
		}
	}
}

// After an action, the tenant's page follows how it goes.
func TestOperationIsFollowed(t *testing.T) {
	cluster := NewDemoCluster()
	server := newServerOf(t, cluster)
	jar, _ := cookiejar.New(nil)
	client := &http.Client{Jar: jar}
	_, body := submit(t, client, server, "/tenants/acme-logistics", url.Values{"do": {"roll"}}, server.URL)
	if !strings.Contains(body, `class="opline"`) || !strings.Contains(body, "its new container is starting") {
		t.Fatalf("after a restart:\n%s", body)
	}
	// Settled, it says so.
	tenant, _ := cluster.Get(context.Background(), "acme-logistics")
	status := *tenant.Status
	status.CheckedAt = status.CheckedAt.Add(-time.Minute)
	cluster.mu.Lock()
	tenant.Status = &status
	cluster.tenants["acme-logistics"] = tenant
	cluster.mu.Unlock()
	if _, page := get(t, server.URL+"/tenants/acme-logistics"); !strings.Contains(page, "done: it runs healthy") {
		t.Fatalf("after it settled:\n%s", page)
	}
}
