package panel

import (
	"context"
	"fmt"
	"html"
	"io"
	"log/slog"
	"net/http"
	"net/http/cookiejar"
	"net/http/httptest"
	"net/url"
	"os"
	"regexp"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/PowerPenguini/murder/internal/api"
	"github.com/PowerPenguini/murder/internal/reconcile"
	"github.com/PowerPenguini/murder/internal/spec"
	"github.com/PowerPenguini/murder/internal/store"
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
	for _, want := range []string{"What changes", `<span class="tx" role="cell">  memoryMB: 512</span>`, "1 line out, 1 line in", `name="do" value="save"`, "Held, and left out until released", "A rollout of revision 3 is blocked in wave 2 (a tenth) by baltic-freight",
		// Only the tenants whose spec changes restart, and are counted:
		// atlas keeps its own memory, so the change leaves it.
		"5 restart · 1 unchanged · 1 held, left out", `Memory <span class="nowrap">256 MB</span> <span class="nowrap">→ 512 MB</span>`, "Unchanged: memory stays 768 MB",
		// A memory fix for the broken baltic-freight, whose own change is
		// memory only: it now, the rest in waves, is proposed; three
		// tenants on revision 2 take revision 3's image with it, named.
		`name="pace" value="first" checked`, "baltic-freight now, the rest in waves <span class=\"dim\">(proposed)</span>",
		"3 tenants also take a new image with it:</span> kowalski-trans, nordwind, vistula-cargo",
		`<span class="wn">1. First, now</span><span class="wc">baltic-freight</span>`, `<span class="wn">2. Canaries</span><span class="wc">acme-logistics</span>`, "Restart baltic-freight now, the rest in waves",
		`name="first" value="baltic-freight"`, `name="pace" value="first">`,
		// One line first, and the proposed pace's button filled.
		"Memory 256 MB → 512 MB; 5 restart, 3 also take a new image; baltic-freight first, the rest in waves.", `class="primary">Restart baltic-freight now`} {
		if !strings.Contains(body, want) {
			t.Fatalf("the review lacks %q:\n%s", want, body)
		}
	}
	if strings.Contains(between(body, "How it rolls out", "Its data"), "atlas") || strings.Contains(body, `value="now" checked`) {
		t.Fatalf("atlas in its waves, or every tenant now proposed:\n%s", body)
	}
	// Each pace's page describes that pace, and only it.
	_, waves := submit(t, client, server, "/apps/web", url.Values{"do": {"review"}, "file": {changed}, "updated": {updated}, "pace": {"waves"}}, server.URL)
	if !strings.Contains(waves, `<span class="wn">1. Canaries</span>`) || !strings.Contains(waves, "Roll out to 5 tenants in waves") || strings.Contains(waves, "Together, now") || strings.Contains(waves, "First, now") {
		t.Fatalf("the waves review:\n%s", waves)
	}
	_, now := submit(t, client, server, "/apps/web", url.Values{"do": {"review"}, "file": {changed}, "updated": {updated}, "pace": {"now"}}, server.URL)
	if !strings.Contains(now, "Together, now:</span> acme-logistics, baltic-freight, kowalski-trans, nordwind, vistula-cargo restart at once") || !strings.Contains(now, "No health gate") ||
		!strings.Contains(now, "Restart 5 tenants now") || strings.Contains(now, `class="waves plan"`) || strings.Contains(now, "starts again with the canaries") ||
		!strings.Contains(now, "Every tenant now gives them that image at once") || !strings.Contains(now, `class="unproposed">Restart 5 tenants now`) ||
		!strings.Contains(now, "Not the pace proposed: no health gate, and 3 tenants take a new image at once.") {
		t.Fatalf("the now review:\n%s", now)
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
	// The three that run revision 2 already keep their containers; the
	// three the blocked rollout moved return, and Abort is said beside it.
	for _, want := range []string{"Return 3 tenants to revision 2", "3 restart · 3 keep their container", "Runs it already: keeps its container", `nordwind, vistula-cargo: <span class="faint">no restart, take revision 2 in place</span>`,
		"Or abort that rollout instead: it takes the 3 tenants it moved (acme-logistics, atlas, baltic-freight) back", `name="pace" value="waves" checked`} {
		if !strings.Contains(body, want) {
			t.Fatalf("going back's review lacks %q:\n%s", want, body)
		}
	}
	// Only what differs is said to change: the image, not the secrets.
	if strings.Contains(between(body, "Affected tenants", "How it rolls out"), "(secret)") {
		t.Fatalf("a secret said to change:\n%s", body)
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
	for _, want := range []string{`<li class="done"><span class="wn">1. Canaries</span>`, `<li class="blocked"><span class="wn">2. A tenth</span><span class="wc">1 tenant · blocked by baltic-freight</span>`, "moved, not healthy: blocks the next wave", "waits for wave 3 (half)", "stays: it is held", "so wave 3 (half) waits. It moves once baltic-freight has been healthy on revision 3 for a minute", "Move the next wave now", "baltic-freight is not healthy on revision 3. Moving the next wave now risks the same for kowalski-trans", "Restore data snapshot"} {
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
	for _, want := range []string{"768 MB", "Tenant override", "Inherited from web", "FEATURE_PLANNER", "over the app's", `aria-label="Override Memory"><span class="sw" aria-hidden="true">Override</span>`, "Review changes", "One review for the whole page", "Add a variable"} {
		if !strings.Contains(page, want) {
			t.Fatalf("the settings lack %q:\n%s", want, page)
		}
	}
	generation := between(page, `name="generation" value="`, `"`)
	form := url.Values{"owns": {"1"}, "generation": {generation}, "app": {"web"}, "own-memoryMB": {"on"}, "memoryMB": {"1100"}, "env": {"TZ=Europe/Berlin\nFEATURE_PLANNER=on"}, "secrets": {"ADMIN_PASSWORD password 20"}}
	form.Set("do", "review")
	_, body := submit(t, client, server, "/tenants/atlas/settings", form, server.URL)
	body = said(body)
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
	openLogs(t, http.DefaultClient, server, "baltic-freight")
	openLogs(t, http.DefaultClient, server, "acme-logistics")
	_, page := get(t, server.URL+"/tenants/baltic-freight/logs?level=error")
	if !strings.Contains(page, "0 of 16 lines match") || strings.Contains(page, "slow query") {
		t.Fatalf("errors only, of the current container:\n%s", page)
	}
	// The container the kernel killed: its lines up to the kill, kept by
	// its node, with the same filters and their own counts.
	_, page = get(t, server.URL+"/tenants/baltic-freight/logs?level=error&container=previous")
	if !strings.Contains(page, "1 of 19 lines match") || !strings.Contains(page, "cannot allocate memory") || !strings.Contains(page, "Previous<span class=\"desktop-inline\"> container <span class=\"n\">exit 137, 4m ago</span></span><span class=\"phone-inline\"> · <span class=\"n\">137, 4m</span></span>") ||
		!strings.Contains(page, "was killed (exit 137), most likely for using more than its 256 MB of memory 4m ago") || strings.Contains(page, "Follow new lines") {
		t.Fatalf("errors only, of the previous container:\n%s", page)
	}
	// A tenant whose node kept nothing says so, and why it may not have.
	if _, other := get(t, server.URL+"/tenants/acme-logistics/logs"); strings.Contains(other, "Previous container") || !strings.Contains(other, "its node kept none of the one before") {
		t.Fatalf("no previous container:\n%s", other)
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
// each other, said once, under the tenant, and only as far as they are true:
// the app's limit reaches the tenants that use it, not one with its own.
func TestCauseTenantAndRolloutJoin(t *testing.T) {
	server := newServerOf(t, NewDemoCluster())
	_, page := get(t, server.URL+"/")
	page = said(page)
	for _, want := range []string{"It blocks web&#39;s rollout of revision 3: baltic-freight moved in wave 2 (a tenth) and is not healthy, so wave 3 (half) waits.", "web&#39;s limit is likely too low: 5 of the 6 tenants on its 256 MB limit ran out of memory",
		"atlas keeps its own 768 MB", "test-staging is held on its revision", `class="button primary">Raise web&#39;s memory limit</a>`, "/apps/web?memoryMB=512#spec", "Only this tenant: raise its limit",
		"baltic-freight</a> is unhealthy: out of memory 4m ago,", "Likely web&#39;s 256 MB limit: 5 of the 6 tenants on it ran out of memory in 24 h."} {
		if !strings.Contains(page, want) {
			t.Fatalf("the overview lacks %q:\n%s", want, page)
		}
	}
	// Every red row is counted, and only those: hel-1, baltic-freight,
	// kowalski-trans.
	if n := strings.Count(page, `<li class="broken">`); n != 3 || !strings.Contains(page, `<div class="n">3</div><div class="l">broken</div>`) {
		t.Fatalf("%d red rows", n)
	}
	_, page = get(t, server.URL+"/tenants/baltic-freight")
	for _, want := range []string{"It blocks web's rollout", `href="/apps/web#r-baltic-freight"`} {
		if !strings.Contains(page, want) {
			t.Fatalf("the tenant lacks %q:\n%s", want, page)
		}
	}
	_, page = get(t, server.URL+"/apps/web?memoryMB=512")
	for _, want := range []string{"memoryMB: 512", "Proposed in the editor below", "also ran out of memory", `id="r-baltic-freight"`, "reaches 5 tenants now", "may stop these restarts", "atlas keeps its own 768 MB"} {
		if !strings.Contains(page, want) {
			t.Fatalf("the app lacks %q:\n%s", want, page)
		}
	}
	if strings.Contains(page, "fixes them all") {
		t.Fatal("the app promises a fix")
	}
	// A tenant on its own limit is not said to be caused by the app's.
	if _, page = get(t, server.URL+"/tenants/atlas"); strings.Contains(page, "web's limit") {
		t.Fatalf("atlas is said to be on web's limit:\n%s", page)
	}
	// The apps list says what is wrong, and Apps counts it.
	_, page = get(t, server.URL+"/apps")
	if !strings.Contains(page, "rollout blocked by baltic-freight") || !strings.Contains(page, "5 of 6 tenants out of memory in 24 h") || !strings.Contains(page, `Apps<span class="count"`) {
		t.Fatalf("the apps list:\n%s", page)
	}
}

// The app-wide memory advice counts from the tenants as they are: those on
// the app's limit, those with their own, held and suspended ones.
func TestAppMemoryAdvice(t *testing.T) {
	now := time.Date(2026, 11, 4, 12, 0, 0, 0, time.UTC)
	app := api.App{Name: "web"}
	app.Spec.Limits.MemoryMB = 256
	mk := func(name string, own int, hold, suspended bool) api.Tenant {
		var t api.Tenant
		t.Spec.Name, t.App, t.Hold, t.Spec.Suspended = name, "web", hold, suspended
		t.Overrides.Limits.MemoryMB = own
		t.Status = &reconcile.Status{Phase: "running", Healthy: name != "a"}
		return t
	}
	tenants := []api.Tenant{mk("a", 0, false, false), mk("b", 0, false, false), mk("c", 0, false, false), mk("d", 768, false, false), mk("e", 0, true, false), mk("f", 0, false, true)}
	// As the agent records a kill: at the limit the container ran under.
	killAt := func(name string, ago time.Duration, mb int) store.Event {
		return store.Event{At: now.Add(-ago), Actor: "murder", Action: "tenant.restarted", Tenant: name, App: "web",
			Detail: fmt.Sprintf("exit 137: killed (SIGKILL), most likely for using more than its %d MB of memory", mb)}
	}
	oom := func(name string, ago time.Duration) store.Event {
		mb := 256
		if name == "d" {
			mb = 768
		}
		return killAt(name, ago, mb)
	}
	events := []store.Event{oom("a", time.Hour), oom("b", 2*time.Hour), oom("d", 3*time.Hour), oom("c", 30*time.Hour)}
	if m := appMemoryOf(app, tenants, events, now); m != nil {
		t.Fatalf("two of five on the limit is not the app's: %+v", m)
	}
	events = append(events, oom("c", 3*time.Hour), oom("e", 4*time.Hour))
	m := appMemoryOf(app, tenants, events, now)
	if m == nil || len(m.Hit) != 4 || m.Inherit != 5 || !m.Bad || m.ProposeMB != 512 {
		t.Fatalf("advice %+v", m)
	}
	if strings.Join(m.Reaches, " ") != "a b c" || strings.Join(m.Held, " ") != "e" || strings.Join(m.Suspended, " ") != "f" || m.Own["d"] != 768 || strings.Join(m.OwnHit, " ") != "d" {
		t.Fatalf("reach %+v", m)
	}
	if m.Text() != ": 4 of the 5 tenants on its 256 MB limit ran out of memory in the last 24 hours" || !strings.Contains(m.Exceptions(), "d keeps its own 768 MB (and ran out of it too)") {
		t.Fatalf("%q / %q", m.Text(), m.Exceptions())
	}
	// The fix applied: the app's limit is 512 MB. The kills at 256 MB say
	// nothing of it, so the advice is gone...
	app.Spec.Limits.MemoryMB = 512
	if m := appMemoryOf(app, tenants, events, now); m != nil {
		t.Fatalf("advice from kills at the old limit: %+v", m)
	}
	// ...until it happens again, to three tenants, at 512 MB.
	events = append(events, killAt("a", time.Minute, 512), killAt("b", time.Minute, 512))
	if m := appMemoryOf(app, tenants, events, now); m != nil {
		t.Fatalf("two at the new limit: %+v", m)
	}
	events = append(events, killAt("c", time.Minute, 512))
	if m := appMemoryOf(app, tenants, events, now); m == nil || len(m.Hit) != 3 || m.LimitMB != 512 || m.ProposeMB != 1024 || len(m.OwnHit) != 1 {
		t.Fatalf("at the new limit: %+v", m)
	}
}

// After an action, the tenant's page follows how it goes.
func TestOperationIsFollowed(t *testing.T) {
	cluster := NewDemoCluster()
	server := newServerOf(t, cluster)
	jar, _ := cookiejar.New(nil)
	client := &http.Client{Jar: jar}
	// Restarting stops it serving for a moment: the name is typed.
	if _, body := submit(t, client, server, "/tenants/acme-logistics", url.Values{"do": {"roll"}}, server.URL); !strings.Contains(body, "type the tenant&#39;s name to restart it") {
		t.Fatalf("restarted without the name:\n%s", body)
	}
	_, body := submit(t, client, server, "/tenants/acme-logistics", url.Values{"do": {"roll"}, "confirm": {"acme-logistics"}}, server.URL)
	// One line says it: no flash, no "It is starting" box beside it.
	if !strings.Contains(body, "its new container is starting") || strings.Contains(body, "is restarting in a new container") || strings.Contains(body, "It is starting") ||
		!strings.Contains(body, "No figures yet: they show once it reports.") || !strings.Contains(body, "previous container, at ") || strings.Contains(body, `data-rest="now `) {
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

// The cluster says what it can take, why a backup failed, and how to
// restore the tenant asked about; a backup job ends in a table.
func TestClusterResilienceAndBackups(t *testing.T) {
	cluster := NewDemoCluster()
	cluster.Pace = 0
	server := newServerOf(t, cluster)
	_, page := get(t, server.URL+"/cluster?tenant=kowalski-trans")
	for _, want := range []string{"Changes go through: 2 of 3 voters answer; a majority is 2", "One more node down would stop all changes", "nordwind and vistula-cargo are still catching up",
		"What nbg-1 said", "What to check", "403 Forbidden", "sudo murder backups now",
		// A refused key: what to fix first, not to try again.
		"nbg-1&#39;s backup key cannot write to the bucket (403).", "it needs s3:PutObject on the bucket&#39;s backups/ prefix", "Once fixed, back it up", "How to restore kowalski-trans", "backups restore kowalski-trans -at 20", `open id="restore"`} {
		if !strings.Contains(page, want) {
			t.Fatalf("the cluster page lacks %q:\n%s", want, page)
		}
	}
	jar, _ := cookiejar.New(nil)
	client := &http.Client{Jar: jar}
	response, _ := submit(t, client, server, "/cluster", url.Values{"do": {"backups"}}, server.URL)
	var body string
	for i := 0; i < 100; i++ {
		_, body = get(t, response.Request.URL.String())
		if strings.Contains(body, ">done<") {
			break
		}
		time.Sleep(20 * time.Millisecond)
	}
	if !strings.Contains(body, "backed up</h2>") || !strings.Contains(body, `<span class="st ok">backed up</span>`) || !strings.Contains(body, "Its whole output") {
		t.Fatalf("the backup job:\n%s", body)
	}
}

func TestSpecProblemsInPlainWords(t *testing.T) {
	file := "image: web:1\nport: eighty\nhelthPath: /\n"
	if text, line := specProblem("the spec: yaml: unmarshal errors:\n  line 2: cannot unmarshal !!str `eighty` into int", file); text != `Line 2 (port): a whole number is needed here, not "eighty"` || line != 2 {
		t.Fatalf("%q %d", text, line)
	}
	if text, line := specProblem("the spec: yaml: unmarshal errors:\n  line 3: field helthPath not found in type spec.Tenant", file); !strings.HasPrefix(text, `Line 3: "helthPath" is not a setting`) || line != 3 {
		t.Fatalf("%q %d", text, line)
	}
}

// A tenant's settings say what is saved; a proposal shows as proposed.
func TestSettingsProposalIsNotInEffect(t *testing.T) {
	server := newServerOf(t, NewDemoCluster())
	_, page := get(t, server.URL+"/tenants/baltic-freight/settings?memoryMB=512")
	row := between(page, `<tr class="setting" id="memory">`, "</tr>")
	if !strings.Contains(row, "Inherited from web") || strings.Contains(row, "Tenant override") || !strings.Contains(row, `<span class="caption" id="p-memoryMB">proposed; 256 MB now</span>`) || strings.Contains(row, `class="proposed"`) {
		t.Fatalf("the memory row: %s", row)
	}
	if !strings.Contains(page, `data-proposed`) || !strings.Contains(page, "proposed-notice") || !strings.Contains(page, "raising web's limit may be the better fix") {
		t.Fatalf("the proposal:\n%s", page)
	}
}

// Every tenant that needs attention says why on its row.
func TestTenantRowsSayWhy(t *testing.T) {
	server := newServerOf(t, NewDemoCluster())
	_, page := get(t, server.URL+"/tenants?show=broken")
	if !strings.Contains(page, `<div class="reason bad"><span class="st bad"></span>backup failed; newest 3h old</div>`) || !strings.Contains(page, "out of memory 4m ago; blocks web&#39;s rollout") {
		t.Fatalf("broken rows:\n%s", page)
	}
	_, page = get(t, server.URL+"/tenants?show=risk")
	if !strings.Contains(page, `<div class="reason"><span class="st warn"></span>copy catching up</div>`) || strings.Contains(page, "kowalski-trans") {
		t.Fatalf("at risk rows:\n%s", page)
	}
}

// Draining says which tenants it would leave without a copy elsewhere, and
// Remove is offered only once it can be done.
func TestDrainSaysWhatItLeavesUnprotected(t *testing.T) {
	server := newServerOf(t, NewDemoCluster())
	_, page := get(t, server.URL+"/nodes/nbg-1")
	if !strings.Contains(page, "baltic-freight, kowalski-trans, vistula-cargo would run on fsn-1 without a ready copy on another node") || !strings.Contains(page, "Now it runs 3 tenants (and 1 archived) and keeps 4 copies.") || strings.Contains(page, `value="remove"`) {
		t.Fatalf("the drain:\n%s", page)
	}
	if _, page = get(t, server.URL+"/nodes/hel-1"); !strings.Contains(page, `value="remove"`) {
		t.Fatal("an empty failed node cannot be removed")
	}
}

// Jobs' records outlive a restart of the panel: one that ended stays as it
// ended, one that ran is interrupted, which is not failed.
func TestJobsSurviveARestart(t *testing.T) {
	path := t.TempDir() + "/jobs.json"
	now := func() time.Time { return time.Date(2026, 11, 4, 12, 0, 0, 0, time.UTC) }
	log := slog.New(slog.NewTextHandler(io.Discard, nil))
	first := &Jobs{}
	if err := first.Persist(path, now()); err != nil {
		t.Fatal(err)
	}
	done, _ := first.Start(JobBackups, "Back up the tenants of fsn-1", "jan (panel)", now, log, func(_ context.Context, out io.Writer) error {
		// More than a record keeps: only its end is kept.
		if _, err := io.WriteString(out, strings.Repeat("x", jobOutputKept)+"\n"); err != nil {
			return err
		}
		_, err := io.WriteString(out, "atlas: backups/atlas/20261104T120000Z.db.zst\n")
		return err
	})
	release := make(chan struct{})
	running, _ := first.Start(JobDrain, "Drain nbg-1 to fsn-1", "jan (panel)", now, log, func(context.Context, io.Writer) error {
		<-release
		return nil
	})
	<-done.done
	// The running job's record is written as it begins; the panel stops.
	for i := 0; i < 100; i++ {
		data, _ := os.ReadFile(path)
		if strings.Contains(string(data), running.ID) && strings.Contains(string(data), "20261104T120000Z") {
			break
		}
		time.Sleep(10 * time.Millisecond)
	}
	if info, err := os.Stat(path); err != nil || info.Mode().Perm() != 0o600 || info.Size() > 2*jobOutputKept {
		t.Fatalf("the jobs' file: %v, %v", info, err)
	}
	second := &Jobs{}
	if err := second.Persist(path, now()); err != nil {
		t.Fatal(err)
	}
	close(release)
	got, gone := second.find(done.ID), second.find(running.ID)
	if got == nil || !got.Done() || got.Interrupted || got.Failed != "" || !strings.Contains(got.Output(), "atlas: backups") || len(got.Output()) > jobOutputKept {
		t.Fatalf("the ended job after a restart: %+v", got)
	}
	if gone == nil || !gone.Done() || !gone.Interrupted || gone.Failed != "" {
		t.Fatalf("the running job after a restart: %+v", gone)
	}
	// Its page says so, at the same address.
	s, err := New(NewDemoCluster(), log)
	if err != nil {
		t.Fatal(err)
	}
	cluster := NewDemoCluster()
	s.Cluster, s.Jobs = cluster.AsCluster, second
	server := httptest.NewServer(s.Handler())
	defer server.Close()
	if _, page := get(t, server.URL+"/jobs/"+running.ID); !strings.Contains(page, ">interrupted<") || !strings.Contains(page, "did not fail as such") || !strings.Contains(page, "across restarts") {
		t.Fatalf("the interrupted job's page:\n%s", page)
	}
}

// A restore follows the time chosen, says where each step runs and what
// is lost; a review of a new image shows both references whole.
func TestRestoreAtAChosenTimeAndImageReferences(t *testing.T) {
	cluster := NewDemoCluster()
	server, client := newServerOf(t, cluster), &http.Client{}
	_, page := get(t, server.URL+"/cluster?tenant=kowalski-trans&at=2026-11-04+09:00")
	for _, want := range []string{"backups restore kowalski-trans -at 2026-11-04T09:00:00Z", "On nbg-1, archive it", "That backup may be older than the time chosen", `href="/tenants/kowalski-trans/logs"`} {
		if !strings.Contains(page, want) {
			t.Fatalf("the restore lacks %q:\n%s", want, page)
		}
	}
	if _, page = get(t, server.URL+"/cluster?tenant=kowalski-trans&at=yesterday"); !strings.Contains(page, "is not a time") {
		t.Fatalf("a time that is not one:\n%s", page)
	}
	_, page = get(t, server.URL+"/apps/web")
	updated := between(page, `name="updated" value="`, `"`)
	_, body := submit(t, client, server, "/apps/web", url.Values{"do": {"review-update"}, "updated": {updated}}, server.URL)
	if !strings.Contains(body, "Full image reference") || !strings.Contains(body, strings.Repeat("5e", 32)) || !strings.Contains(body, "is blocked in wave 2 (a tenth) by baltic-freight") {
		t.Fatalf("the image review:\n%s", body)
	}
}

// The 3 a.m. path, followed through: the panel's own fix, raising web's
// limit at once, is applied; baltic-freight comes back on 512 MB; and the
// advice is not given again against the new limit for kills at the old.
func TestTheFixIsAppliedAndTheAdviceGoes(t *testing.T) {
	cluster, server, client := newAppsPanel(t)
	_, page := get(t, server.URL+"/apps/web?memoryMB=512")
	updated := between(page, `name="updated" value="`, `"`)
	file := between(page, `aria-label="the app's spec">`, "</textarea>")
	if !strings.Contains(file, "memoryMB: 512") {
		t.Fatalf("the proposal is not in the editor:\n%s", file)
	}
	_, body := submit(t, client, server, "/apps/web", url.Values{"do": {"save"}, "file": {file}, "updated": {updated}, "pace": {"now"}}, server.URL)
	if !strings.Contains(body, "app web is changed; it rolls out to 5 tenants") {
		t.Fatalf("after the fix:\n%s", body)
	}
	// The new containers start, and settle.
	cluster.mu.Lock()
	for name, tenant := range cluster.tenants {
		if tenant.Status != nil && tenant.Status.Phase == "starting" {
			status := *tenant.Status
			status.CheckedAt = status.CheckedAt.Add(-time.Minute)
			tenant.Status = &status
			cluster.tenants[name] = tenant
		}
	}
	cluster.mu.Unlock()
	baltic, _ := cluster.Get(context.Background(), "baltic-freight")
	if baltic.Spec.Limits.MemoryMB != 512 || baltic.Status == nil || !baltic.Status.Healthy {
		t.Fatalf("baltic-freight after the fix: %+v", baltic.Status)
	}
	_, page = get(t, server.URL+"/")
	if strings.Contains(page, "limit is likely too low") || strings.Contains(page, "Likely web") || strings.Contains(page, "baltic-freight</a> is unhealthy") {
		t.Fatalf("the advice after the fix:\n%s", page)
	}
	if _, page = get(t, server.URL+"/apps/web"); strings.Contains(page, "memory limit is likely too low") {
		t.Fatalf("the app's advice after the fix:\n%s", page)
	}
	// It says the fix held: on the app, and on a tenant it reached.
	if !strings.Contains(page, "Revision 4 (memory 256 MB → 512 MB)</span>, by you") || !strings.Contains(page, "reached 5 tenants: 5 healthy; no memory kill since") {
		t.Fatalf("the outcome on the app:\n%s", page)
	}
	if _, page = get(t, server.URL+"/tenants/baltic-freight"); !strings.Contains(page, "web's revision 4 (memory 256 MB → 512 MB)</span> reached it, by you") || !strings.Contains(page, "healthy, no memory kill since") {
		t.Fatalf("the outcome on the tenant:\n%s", page)
	}
}

// A moved tenant is said healthy only for a check that passes now; one
// starting waits for its check, and one whose node is silent is not known.
func TestMovedResultsFromLiveHealth(t *testing.T) {
	now := time.Date(2026, 11, 4, 12, 0, 0, 0, time.UTC)
	tenant := func(status *reconcile.Status) api.Tenant {
		var t api.Tenant
		t.Spec.Name, t.Generation, t.Status = "a", 3, status
		return t
	}
	for _, c := range []struct {
		status *reconcile.Status
		want   string
	}{
		{&reconcile.Status{Phase: "running", Healthy: true, Generation: 3, CheckedAt: now.Add(-10 * time.Second)}, "moved, healthy"},
		{&reconcile.Status{Phase: "starting", Generation: 3, CheckedAt: now}, "moved, starting: waiting for its health check"},
		{&reconcile.Status{Phase: "running", Healthy: true, Generation: 2, CheckedAt: now}, "moved, starting: waiting for its health check"},
		{&reconcile.Status{Phase: "running", Healthy: true, Generation: 3, CheckedAt: now.Add(-9 * time.Minute)}, "moved; its last check passed 9m ago, none since"},
		{&reconcile.Status{Phase: "running", Generation: 3, CheckedAt: now}, "moved, not healthy: blocks the next wave"},
		{nil, "moved; how it runs is not known: its node does not answer"},
	} {
		tt := tenant(c.status)
		if got, _ := movedResult(tt, tenantState(tt), now); got != c.want {
			t.Errorf("%+v: %q, want %q", c.status, got, c.want)
		}
	}
}

// A headline figure opens the set it counts, and a change's scope is the
// same number on the app's page and in its review.
func TestFiguresAndScopesAgree(t *testing.T) {
	cluster, server, client := newAppsPanel(t)
	_ = cluster
	_, page := get(t, server.URL+"/")
	if !strings.Contains(page, `href="/tenants?show=unhealthy"`) || !strings.Contains(page, "5 of 6 serving tenants healthy: show the 1 not healthy") {
		t.Fatalf("the healthy figure:\n%s", page)
	}
	_, page = get(t, server.URL+"/tenants?show=unhealthy")
	if !strings.Contains(page, "1 of 8 match") || !strings.Contains(page, `href="/tenants/baltic-freight"`) || strings.Contains(page, `href="/tenants/kowalski-trans"`) {
		t.Fatalf("the tenants not healthy:\n%s", page)
	}
	_, page = get(t, server.URL+"/apps/web")
	if !strings.Contains(page, "6 of 7 live tenants restart; 1 held, left out") {
		t.Fatalf("the update's scope on the app's page:\n%s", page)
	}
	updated := between(page, `name="updated" value="`, `"`)
	_, body := submit(t, client, server, "/apps/web", url.Values{"do": {"review-update"}, "updated": {updated}}, server.URL)
	// A newer image: waves are proposed, even for the broken tenant.
	if !strings.Contains(body, "6 restart · 1 held, left out") || !strings.Contains(body, "Roll out to 6 tenants in waves") || !strings.Contains(body, `value="waves" checked`) {
		t.Fatalf("the update's scope in its review:\n%s", body)
	}
}

// The audit log's actors read as people say them: the person who acted in
// the panel, or the local panel through its socket; a person's new
// container is a restart.
func TestActorsAndRestartsInTheAuditLog(t *testing.T) {
	for actor, want := range map[string]string{
		"jan (panel) via murder-panel":         "jan (panel)",
		"murder-panel socket via murder-panel": "the local panel",
		"murder-panel socket":                  "the local panel",
		"jan via murder-panel":                 "jan (panel)",
		"root":                                 "root",
		"murder (rollout)":                     "murder (rollout)",
	} {
		if got := who(actor); got != want {
			t.Errorf("who(%q) = %q, want %q", actor, got, want)
		}
	}
	if !sameActor("jan (panel) via murder-panel", "jan (panel)") || sameActor("anna (panel) via murder-panel", "jan (panel)") {
		t.Fatal("sameActor")
	}
	cluster := NewDemoCluster()
	server := newServerOf(t, cluster)
	jar, _ := cookiejar.New(nil)
	client := &http.Client{Jar: jar}
	submit(t, client, server, "/tenants/acme-logistics", url.Values{"do": {"roll"}, "confirm": {"acme-logistics"}}, server.URL)
	_, page := get(t, server.URL+"/events")
	// The reader's own actions are "you"; others' are said by name.
	if !strings.Contains(page, `acme-logistics</a> was restarted`) || !strings.Contains(page, `<td class="by hide-narrow">you</td>`) || !strings.Contains(page, `<td class="by hide-narrow">jan (panel)</td>`) || strings.Contains(page, "murder-panel socket") {
		t.Fatalf("a restart in the audit log:\n%s", page)
	}
	if _, page = get(t, server.URL+"/tenants/acme-logistics"); !strings.Contains(page, "by you") {
		t.Fatalf("one's own restart:\n%s", page)
	}
}

// A backup's failure is read for its cause: what to fix comes first for a
// failure trying again will not mend.
func TestBackupCauses(t *testing.T) {
	for text, want := range map[string]string{
		"put backups/x.db.zst: 403 Forbidden: the bucket refused the upload": "n's backup key cannot write to the bucket (403).",
		"put: AccessDenied: Access Denied":                                   "n's backup key cannot write to the bucket (403).",
		"NoSuchBucket: The specified bucket does not exist":                  "The bucket n writes to is not there (404).",
		"dial tcp: lookup s3.example.com: no such host":                      "n could not reach the bucket.",
		"write /var/tmp/x: no space left on device":                          "n ran out of disk while it made the backup.",
		"something else entirely":                                            "",
	} {
		if got, _ := (backupView{Node: "n", Error: text, Every: time.Hour}).Cause(); got != want {
			t.Errorf("%q: %q, want %q", text, got, want)
		}
	}
}

// An interrupted drain says what moved and what did not, offers to drain
// the rest with the same typed name and warning as the node's page, and
// links what it changed in the audit log, while it ran.
func TestAnInterruptedDrainIsFinished(t *testing.T) {
	path := t.TempDir() + "/jobs.json"
	if err := SeedDemoJobs(path, time.Now()); err != nil {
		t.Fatal(err)
	}
	cluster := NewDemoCluster()
	s, err := New(cluster, slog.New(slog.NewTextHandler(io.Discard, nil)))
	if err != nil {
		t.Fatal(err)
	}
	s.Actions, s.Apps, s.Cluster = cluster.As, cluster.AsApps, cluster.AsCluster
	s.Jobs = &Jobs{}
	if err := s.Jobs.Persist(path, time.Now()); err != nil {
		t.Fatal(err)
	}
	server := httptest.NewServer(s.Handler())
	defer server.Close()
	_, page := get(t, server.URL+"/jobs/5eedd8a1n0000001")
	for _, want := range []string{`<a href="/tenants/atlas">atlas</a>`, `<span class="st ok">moved</span>`, "still on nbg-1", `<span class="st warn">not moved</span>`,
		"Drain nbg-1 again", `data-confirm="nbg-1"`, "would run on fsn-1 without a ready copy on another node", "the audit log while it ran", "since="} {
		if !strings.Contains(page, want) {
			t.Fatalf("the interrupted drain lacks %q:\n%s", want, page)
		}
	}
	if strings.Count(page, `<span class="st ok">moved</span>`) != 1 {
		t.Fatalf("more than atlas moved:\n%s", page)
	}
	link := html.UnescapeString(between(page, `<a href="/events?show=all&amp;since=`, `">What it changed`))
	_, events := get(t, server.URL+"/events?show=all&since="+strings.ReplaceAll(link, "&", "&"))
	if !strings.Contains(events, "atlas</a> was moved") || strings.Contains(events, "baltic-freight</a> restarted") || !strings.Contains(events, "Only the events from") {
		t.Fatalf("the audit log while it ran:\n%s", events)
	}
}

// A name typed on a phone, its first letter capitalised and a space after
// it, confirms as it does on a desktop; another name does not. Every
// confirmation's field asks the phone not to change what is typed.
func TestTypedNamesOnPhones(t *testing.T) {
	if !typed(" Baltic-freight ", "baltic-freight") || typed("baltic-fr", "baltic-freight") || typed("", "") {
		t.Fatal("typed")
	}
	cluster := NewDemoCluster()
	server := newServerOf(t, cluster)
	jar, _ := cookiejar.New(nil)
	client := &http.Client{Jar: jar}
	submit(t, client, server, "/tenants/atlas", url.Values{"do": {"suspend"}, "confirm": {"Atlas "}}, server.URL)
	if tenant, _ := cluster.Get(context.Background(), "atlas"); !tenant.Spec.Suspended {
		t.Fatal("a capitalised name did not confirm")
	}
	for _, path := range []string{"/tenants/baltic-freight", "/apps/web", "/nodes/nbg-1", "/cluster"} {
		_, page := get(t, server.URL+path)
		if n, safe := strings.Count(page, "data-confirm="), strings.Count(page, `autocapitalize="none" autocorrect="off" spellcheck="false"`); n == 0 && path != "/cluster" || safe < n {
			t.Fatalf("%s: %d confirmations, %d safe on a phone", path, n, safe)
		}
	}
}

// Every event's anchor is its own, a run's wrapper apart from its first
// event; a chart's mark says its event and has a touch area within the
// plot.
func TestEventAnchorsAndMarks(t *testing.T) {
	server := newServerOf(t, NewDemoCluster())
	_, page := get(t, server.URL+"/events?show=all")
	seen := map[string]bool{}
	for _, m := range regexp.MustCompile(`id="(ev-\d+)"`).FindAllStringSubmatch(page, -1) {
		if seen[m[1]] {
			t.Fatalf("%s twice", m[1])
		}
		seen[m[1]] = true
	}
	if len(seen) < 20 || !strings.Contains(page, `id="evs-`) {
		t.Fatalf("%d anchors", len(seen))
	}
	_, page = get(t, server.URL+"/tenants/baltic-freight")
	if !strings.Contains(page, "UTC: Restarted after it exited: exit 137, out of memory") || !strings.Contains(page, `class="hit"`) || !strings.Contains(page, `<div class="annot" aria-live="polite"></div>`) {
		t.Fatalf("the marks:\n%s", page)
	}
	for _, m := range regexp.MustCompile(`class="hit" x="([-0-9.]+)"`).FindAllStringSubmatch(page, -1) {
		if x, _ := strconv.ParseFloat(m[1], 64); x < 0 || x > 1000-markHit {
			t.Fatalf("a touch area at %s, outside the plot", m[1])
		}
	}
}

// Settings are one form with one review: the environment edited in its
// table, a variable added in its last row, the rollout flags beside them.
// Flags alone change no container.
func TestSettingsAsOneForm(t *testing.T) {
	cluster, server, client := newAppsPanel(t)
	ctx := context.Background()
	before, _ := cluster.Get(ctx, "atlas")
	page := func(form url.Values) url.Values {
		form.Set("generation", strconv.FormatInt(before.Generation, 10))
		form.Set("app", "web")
		form.Set("owns", "1")
		form.Set("own-memoryMB", "on")
		form.Set("memoryMB", "768")
		form.Set("secrets", "ADMIN_PASSWORD password 20")
		return form
	}
	// TZ cleared (Berlin to the app's Warsaw), FEATURE_PLANNER kept,
	// LOG_LEVEL added with a space kept, the canary flag off.
	// Two variables added in one review: LOG_LEVEL and API_URL.
	form := page(url.Values{"do": {"review"}, "envrows": {"1"}, "flags": {"1"}, "hold": {""},
		"envname": {"DATA_DIR", "FEATURE_PLANNER", "TZ"}, "envvalue": {"/data", "on", "Europe/Berlin"}, "envown": {"FEATURE_PLANNER"},
		"envnewname": {"LOG_LEVEL", "API_URL", ""}, "envnewvalue": {" debug", "https://api.example.com", ""}})
	_, body := submit(t, client, server, "/tenants/atlas/settings", form, server.URL)
	for _, want := range []string{"Environment TZ", "Europe/Warsaw, inherited from web", "Environment LOG_LEVEL", "Environment API_URL", "Canary", "no: with the others", "Save and restart atlas"} {
		if !strings.Contains(body, want) {
			t.Fatalf("the review lacks %q:\n%s", want, body)
		}
	}
	token := between(body, `name="reviewed" value="`, `"`)
	env := html.UnescapeString(between(body, `<input type="hidden" name="env" value="`, `"`))
	save := page(url.Values{"do": {"save"}, "reviewed": {token}, "env": {env}, "flags": {"1"}, "canary": {""}, "hold": {""}})
	_, body = submit(t, client, server, "/tenants/atlas/settings", save, server.URL)
	if !strings.Contains(body, "it gets a new container; atlas takes rollouts with the others") {
		t.Fatalf("after saving:\n%s", body)
	}
	after, _ := cluster.Get(ctx, "atlas")
	if after.Overrides.Env["LOG_LEVEL"] != " debug" || after.Overrides.Env["API_URL"] != "https://api.example.com" || after.Overrides.Env["TZ"] != "" || after.Overrides.Env["FEATURE_PLANNER"] != "on" || after.Canary {
		t.Fatalf("atlas after saving: %v, canary %v", after.Overrides.Env, after.Canary)
	}
	// The flag alone: no new container, a plain Save.
	_, body = submit(t, client, server, "/tenants/acme-logistics/settings", url.Values{"do": {"review"}, "generation": {"3"}, "app": {"web"}, "flags": {"1"}, "hold": {"on"}, "canary": {"on"}}, server.URL)
	acme, _ := cluster.Get(ctx, "acme-logistics")
	if !strings.Contains(body, "Rollout flags change no container") || strings.Contains(body, "Save and restart") {
		t.Fatalf("a flag's review:\n%s", body)
	}
	token = between(body, `name="reviewed" value="`, `"`)
	_, body = submit(t, client, server, "/tenants/acme-logistics/settings", url.Values{"do": {"save"}, "reviewed": {token}, "generation": {strconv.FormatInt(acme.Generation, 10)}, "app": {"web"}, "flags": {"1"}, "hold": {"on"}, "canary": {"on"}}, server.URL)
	if now, _ := cluster.Get(ctx, "acme-logistics"); !now.Hold || now.Status == nil || now.Status.Phase == "starting" || strings.Contains(body, "new container") {
		t.Fatalf("after holding: hold %v, %+v:\n%s", now.Hold, now.Status, body)
	}
}

// The pace proposed is the safe one: never every tenant at once when an
// image changes with it; the broken first when their change is a fix.
func TestPacesProposed(t *testing.T) {
	for _, c := range []struct {
		restart, broken, images []string
		want                    string
		offered                 int
	}{
		{[]string{"a", "b", "c"}, []string{"b"}, []string{"c"}, "first", 3},
		{[]string{"a", "b", "c"}, []string{"b"}, []string{"b", "c"}, "waves", 3},
		{[]string{"a", "b"}, nil, nil, "waves", 2},
		{[]string{"b"}, []string{"b"}, nil, "now", 2},
		{[]string{"b"}, []string{"b"}, []string{"b"}, "waves", 2},
	} {
		paces, got := pacesOf(c.restart, c.broken, c.images)
		if got != c.want || len(paces) != c.offered {
			t.Errorf("%+v: %q of %d", c, got, len(paces))
		}
	}
}

// The proposed pace, rolled out: the broken tenant moves now, the rest
// wait for their waves; no other tenant restarts.
func TestTheBrokenFirstPace(t *testing.T) {
	cluster, server, client := newAppsPanel(t)
	_, page := get(t, server.URL+"/apps/web?memoryMB=512")
	updated := between(page, `name="updated" value="`, `"`)
	file := between(page, `aria-label="the app's spec">`, "</textarea>")
	_, body := submit(t, client, server, "/apps/web", url.Values{"do": {"save"}, "file": {file}, "updated": {updated}, "pace": {"first"}, "first": {"baltic-freight"}}, server.URL)
	if !strings.Contains(body, "app web is changed: baltic-freight restarts now, 4 more in waves") {
		t.Fatalf("after rolling out:\n%s", body)
	}
	// The waves are named as the plan named them, on every page: the
	// first, then the canaries.
	for _, want := range []string{`<span class="wn">1. First, now</span><span class="wc">1 tenant · moving, watched</span>`, `<span class="wn">2. Canaries</span>`,
		"Canaries (acme-logistics) moves once baltic-freight has stayed healthy for two minutes, about "} {
		if !strings.Contains(body, want) {
			t.Fatalf("the rollout lacks %q:\n%s", want, body)
		}
	}
	if _, page := get(t, server.URL+"/tenants/baltic-freight"); !strings.Contains(page, `it moved from revision 3 to 4 in wave 1 (first, now).`) {
		t.Fatalf("baltic-freight's wave:\n%s", page)
	}
	if _, page := get(t, server.URL+"/tenants/acme-logistics"); !strings.Contains(page, `moves to 4 in wave 2 (canaries).`) {
		t.Fatalf("acme-logistics's wave:\n%s", page)
	}
	// atlas's own memory covers the change: its page says so, not a bare
	// link.
	if _, page := get(t, server.URL+"/tenants/atlas"); !strings.Contains(page, "web&#39;s revision 4 changes nothing for atlas: its own memory (768 MB) stays over what it changed, so it kept its container.") {
		t.Fatalf("atlas, unchanged:\n%s", page)
	}
	ctx := context.Background()
	baltic, _ := cluster.Get(ctx, "baltic-freight")
	if baltic.Spec.Limits.MemoryMB != 512 || baltic.AppRevision != 0 {
		t.Fatalf("baltic-freight: %+v", baltic)
	}
	for _, name := range []string{"kowalski-trans", "nordwind", "vistula-cargo", "acme-logistics"} {
		if tenant, _ := cluster.Get(ctx, name); tenant.Spec.Limits.MemoryMB == 512 || tenant.Status.Phase == "starting" {
			t.Fatalf("%s moved with it: %+v", name, tenant.Spec.Limits)
		}
	}
	if app, _ := cluster.AsApps("t").App(ctx, "web"); app.Rollout == nil || app.Rollout.Wave != 1 || app.Rollout.ToRevision != 4 {
		t.Fatalf("the rollout: %+v", app.Rollout)
	}
}

// The demo tells its incident in its figures: baltic-freight's memory
// climbs to its limit, drops at each kill, and settles well under 512 MB
// once it has them.
func TestDemoIncidentTimeline(t *testing.T) {
	d := NewDemoCluster()
	kills := d.killsOf("baltic-freight")
	if len(kills) == 0 {
		t.Fatal("no kill")
	}
	kill := kills[0]
	before, after, later := d.demoMemory("baltic-freight", 256, kill.Add(-time.Second)), d.demoMemory("baltic-freight", 256, kill.Add(20*time.Second)), d.demoMemory("baltic-freight", 256, kill.Add(10*time.Minute))
	if before < 240 || after > 150 || later < 240 {
		t.Fatalf("at 256 MB: %.0f before the kill, %.0f after, %.0f later", before, after, later)
	}
	if fixed := d.demoMemory("baltic-freight", 512, kill.Add(10*time.Minute)); fixed > 300 || fixed < 200 {
		t.Fatalf("at 512 MB: %.0f", fixed)
	}
	if other := d.demoMemory("atlas", 768, kill); other != 0 {
		t.Fatal("atlas has no incident")
	}
}

// Without the page's script, "Add a variable" asks the server for another
// row and keeps the draft; secrets are one table in the same form: a
// switch, a kind and a length, and a row to add one.
func TestSettingsRowsWithoutScript(t *testing.T) {
	cluster, server, client := newAppsPanel(t)
	atlas, _ := cluster.Get(context.Background(), "atlas")
	base := url.Values{"generation": {strconv.FormatInt(atlas.Generation, 10)}, "app": {"web"}, "owns": {"1"}, "own-memoryMB": {"on"}, "memoryMB": {"768"},
		"envrows": {"1"}, "secretrows": {"1"}, "flags": {"1"}, "canary": {"on"},
		"envname": {"DATA_DIR", "FEATURE_PLANNER", "TZ"}, "envvalue": {"/data", "on", "Europe/Berlin"}, "envown": {"FEATURE_PLANNER", "TZ"},
		"envnewname": {"LOG_LEVEL"}, "envnewvalue": {"debug"},
		"secretname": {"ADMIN_PASSWORD", "SESSION_KEY"}, "secretkind": {"password", "hex"}, "secretbytes": {"20", "32"}, "secretown": {"ADMIN_PASSWORD"},
		"secretnewname": {"API_TOKEN"}, "secretnewkind": {"base64"}, "secretnewbytes": {"48"}}
	form := url.Values{}
	for k, v := range base {
		form[k] = v
	}
	form.Set("do", "addrow")
	_, body := submit(t, client, server, "/tenants/atlas/settings", form, server.URL)
	// The draft is kept: LOG_LEVEL and API_TOKEN as rows of their own,
	// and an empty row to add another.
	for _, want := range []string{`name="envown" value="LOG_LEVEL" class="own-switch" data-key="LOG_LEVEL" checked aria-label=`, "Added in this draft",
		`name="secretown" value="API_TOKEN" class="own-switch" data-key="API_TOKEN" checked aria-label=`, `name="envnewname"`} {
		if !strings.Contains(body, want) {
			t.Fatalf("after adding a row, the page lacks %q:\n%s", want, body)
		}
	}
	if strings.Count(body, `name="envnewname"`) != 1 || strings.Contains(body, "<textarea name=\"secrets\"") {
		t.Fatalf("rows:\n%s", body)
	}
	form.Set("do", "review")
	_, body = submit(t, client, server, "/tenants/atlas/settings", form, server.URL)
	for _, want := range []string{"Environment LOG_LEVEL", "Secret API_TOKEN", "made once, base64"} {
		if !strings.Contains(body, want) {
			t.Fatalf("the review lacks %q:\n%s", want, body)
		}
	}
	if !strings.Contains(between(body, `name="secrets" value="`, `"`), "API_TOKEN base64 48") {
		t.Fatalf("the reviewed secrets:\n%s", body)
	}
}

// The events' links keep their scope: the tenant, the app and the time
// window survive "Include them" and "Older events".
func TestEventLinksKeepTheirScope(t *testing.T) {
	d := eventsData{Tenant: "atlas", Show: "people", Since: time.Date(2026, 11, 4, 1, 0, 0, 0, time.UTC), Until: time.Date(2026, 11, 4, 2, 0, 0, 0, time.UTC)}
	if got := d.Link("show", "all"); got != "/events?show=all&since=2026-11-04T01%3A00%3A00Z&tenant=atlas&until=2026-11-04T02%3A00%3A00Z" {
		t.Fatalf("include them: %s", got)
	}
	if got := d.Link("before", "9"); !strings.Contains(got, "before=9") || !strings.Contains(got, "since=") || !strings.Contains(got, "tenant=atlas") || !strings.Contains(got, "show=people") {
		t.Fatalf("older: %s", got)
	}
	if got := d.Link("since", "", "until", ""); got != "/events?show=people&tenant=atlas" {
		t.Fatalf("every time: %s", got)
	}
}

// A review is shown at its own address, after the form that asked for it
// (post, redirect, get): a reload shows it again, and the pace is a link.
// A secret's "New value…" is a cell of its own at every width.
func TestReviewsHaveTheirOwnAddress(t *testing.T) {
	_, server, client := newAppsPanel(t)
	_, page := get(t, server.URL+"/apps/web?memoryMB=512")
	updated := between(page, `name="updated" value="`, `"`)
	file := between(page, `aria-label="the app's spec">`, "</textarea>")
	noFollow := *client
	noFollow.CheckRedirect = func(*http.Request, []*http.Request) error { return http.ErrUseLastResponse }
	response, _ := submit(t, &noFollow, server, "/apps/web", url.Values{"do": {"review"}, "file": {file}, "updated": {updated}}, server.URL)
	where := response.Header.Get("Location")
	if response.StatusCode != http.StatusSeeOther || !strings.HasPrefix(where, "/apps/web/review?") || !strings.Contains(where, "draft=") {
		t.Fatalf("after asking for the review: %d %q", response.StatusCode, where)
	}
	_, body := get(t, server.URL+where)
	if !strings.Contains(body, "Your change to web&#39;s spec") || !strings.Contains(body, `form method="get" action="/apps/web/review#pace"`) {
		t.Fatalf("the review at its address:\n%s", body)
	}
	if _, again := get(t, server.URL+where+"&pace=now"); !strings.Contains(again, "Together, now") {
		t.Fatalf("the review at another pace:\n%s", again)
	}
	// A draft not kept is said so, not shown empty.
	gone, err := client.Get(server.URL + "/apps/web/review?do=review&updated=" + url.QueryEscape(updated) + "&draft=nothing")
	if err != nil {
		t.Fatal(err)
	}
	data, _ := io.ReadAll(gone.Body)
	gone.Body.Close()
	if body = string(data); gone.Request.URL.Path != "/apps/web" || !strings.Contains(body, "that review is no longer kept") {
		t.Fatalf("an unknown draft:\n%s", body)
	}
	_, body = get(t, server.URL+"/tenants/atlas/settings")
	if !strings.Contains(body, `<span class="row-act"><details class="menu-text">`) || strings.Contains(body, `class="end l-more"`) {
		t.Fatalf("the secrets' actions:\n%s", body)
	}
}

// Several secrets in one review, as variables: "Add a secret" without the
// script keeps the draft and gives another row.
func TestSeveralSecretsInOneReview(t *testing.T) {
	cluster, server, client := newAppsPanel(t)
	atlas, _ := cluster.Get(context.Background(), "atlas")
	form := url.Values{"generation": {strconv.FormatInt(atlas.Generation, 10)}, "app": {"web"}, "secretrows": {"1"},
		"secretname": {"ADMIN_PASSWORD", "SESSION_KEY"}, "secretkind": {"password", "hex"}, "secretbytes": {"20", "32"}, "secretown": {"ADMIN_PASSWORD"},
		"secretnewname": {"API_TOKEN"}, "secretnewkind": {"base64"}, "secretnewbytes": {"48"}, "do": {"addsecret"}}
	_, body := submit(t, client, server, "/tenants/atlas/settings", form, server.URL)
	if strings.Count(body, `name="secretnewname"`) != 1 || !strings.Contains(body, `data-key="API_TOKEN" checked`) {
		t.Fatalf("after adding a secret row:\n%s", body)
	}
	form["secretnewname"] = []string{"API_TOKEN", "WEBHOOK_KEY"}
	form["secretnewkind"] = []string{"base64", "hex"}
	form["secretnewbytes"] = []string{"48", ""}
	form.Set("do", "review")
	_, body = submit(t, client, server, "/tenants/atlas/settings", form, server.URL)
	for _, want := range []string{"Secret API_TOKEN", "Secret WEBHOOK_KEY", "made once, base64", "made once, hex"} {
		if !strings.Contains(body, want) {
			t.Fatalf("the review lacks %q:\n%s", want, body)
		}
	}
}

// Another app is chosen from the tenant's menu and reviewed on its own;
// the settings page holds only overrides.
func TestChangingATenantsApp(t *testing.T) {
	cluster, server, client := newAppsPanel(t)
	ctx := context.Background()
	_, body := get(t, server.URL+"/tenants/atlas")
	if !strings.Contains(body, `action="/tenants/atlas/settings#review"`) || !strings.Contains(body, "Change its app…</span>") {
		t.Fatalf("the menu's app change:\n%s", body)
	}
	_, body = get(t, server.URL+"/tenants/atlas/settings")
	if strings.Contains(between(body, `id="settings"`, `</form>`), `<select name="app">`) {
		t.Fatalf("the settings form still chooses the app:\n%s", body)
	}
	// The review composes what atlas would run: each setting now and after,
	// its own settings kept, what goes with web, and a warning that its data
	// was written by another user than testapp runs as.
	_, body = get(t, server.URL+"/tenants/atlas/settings?do=review&app=testapp")
	for _, want := range []string{"Review changing atlas to app testapp", "<th>Now, on web</th><th>After, on testapp</th>",
		"App web → testapp: 6 settings change; 3 get new values and 3 go: the disk limit, DATA_DIR and the secret SESSION_KEY; atlas restarts", "atlas would run with no disk limit:", "Set it in",
		"<td class=\"l-main\">Disk</td><td class=\"dim l-sub\" data-label=\"Now\">10\u00a0GB</td><td class=\"l-sub\" data-label=\"After\"><span class=\"st warn\">no limit (no default on <span class=\"nowrap\">fsn-1</span>)</span>",
		`<td class="l-main">Processes</td><td class="dim l-sub" data-label="Now">256</td><td class="l-sub" data-label="After">512 (node default on <span class="nowrap">fsn-1</span>)</td>`,
		`<tr class="same"><td class="l-main">CPUs</td><td class="dim l-sub" data-label="Now">1</td><td class="l-sub" data-label="After"><span class="dim">same: 1 (node default on <span class="nowrap">fsn-1</span>)</span>`,
		"testapp runs as 10001:10001, but atlas's data at /data was written as 1000:1000.",
		`<td class="l-main">User</td><td class="dim l-sub" data-label="Now">1000:1000</td><td class="l-sub" data-label="After">10001:10001</td>`,
		`<td class="l-main">Image</td>`, "Its own, kept over testapp's", `<span class="nowrap">memory 768 MB</span>`, `<span class="nowrap">TZ</span>`,
		`<td class="l-main">SESSION_KEY (secret)</td><td class="dim l-sub" data-label="Now">hex, 32 bytes</td><td class="l-sub" data-label="After">not set</td>`,
		"Gone with web", `<span class="nowrap">DATA_DIR</span>`, `<span class="nowrap">SESSION_KEY (secret)</span>`,
		"The values of the secrets that go are kept by murder, not given to testapp: changing back to web gives them back",
		"Type atlas to confirm.", `data-confirm="atlas"`, `class="danger">Change atlas to testapp</button>`, "data-review"} {
		if !strings.Contains(body, want) {
			t.Fatalf("the app change's review lacks %q:\n%s", want, body)
		}
	}
	form := url.Values{"do": {"save"}}
	for _, name := range []string{"generation", "reviewed", "app", "image", "memoryMB", "cpus", "pids", "diskGB", "env", "secrets", "flags", "canary", "hold"} {
		form.Set(name, html.UnescapeString(between(body, `name="`+name+`" value="`, `"`)))
	}
	// Without its name typed, nothing changes.
	if _, refused := submit(t, client, server, "/tenants/atlas/settings", form, server.URL); !strings.Contains(refused, "type atlas to change its app") {
		t.Fatalf("saved without the name typed:\n%s", refused)
	}
	if still, _ := cluster.Get(ctx, "atlas"); still.App != "web" {
		t.Fatalf("atlas moved to %q without its name typed", still.App)
	}
	form.Set("confirm", "atlas")
	// After it: its Overview, the change said with a way back, the data's
	// owner kept in view until it runs healthy, and no "0 B" of data.
	_, after := submit(t, client, server, "/tenants/atlas/settings", form, server.URL)
	for _, want := range []string{"Changed from app web to testapp</span> by you", "Change it back to web</a>",
		"testapp runs as 10001:10001, but its data at /data was written as 1000:1000: if it does not start"} {
		if !strings.Contains(after, want) {
			t.Fatalf("after the change lacks %q:\n%s", want, after)
		}
	}
	if strings.Contains(after, `<div class="v">0 B`) {
		t.Fatalf("its data said as 0 B:\n%s", after)
	}
	// The disk limit it lost stays said, as at risk, until one is set: on
	// its page, in the tenants list and on the overview.
	if !strings.Contains(after, "It runs with no disk limit</span>: the other tenants have 10 GB.") || !strings.Contains(after, `href="/tenants/atlas/settings?set=diskGB#diskGB" class="button small">Set its disk limit</a>`) {
		t.Fatalf("after the change, its lost limit is not said:\n%s", after)
	}
	// Its Container section says each limit as the reviews do: CPUs too.
	for _, want := range []string{`<dt>CPU limit</dt><dd class="num">1 <span class="dim">node default, <span class="nowrap">fsn-1</span></span>`, `<dt>Processes, most</dt><dd class="num">512 <span class="dim">node default, <span class="nowrap">fsn-1</span></span>`,
		`<dt>Disk limit</dt><dd class="num"><span class="st warn">no limit</span> <span class="dim">no default on <span class="nowrap">fsn-1</span></span>`, `<dt>Memory limit</dt><dd class="num">768 MB</dd>`} {
		if !strings.Contains(after, want) {
			t.Fatalf("its Container section lacks %q:\n%s", want, after)
		}
	}
	// Settings say them the same way, the risk on its row, which the link
	// opens.
	_, settings := get(t, server.URL+"/tenants/atlas/settings?set=diskGB")
	for _, want := range []string{`<tr class="setting at-risk" id="diskGB">`, `<span class="st warn">no limit</span>`, "None: fsn-1 has no default", "Node default (fsn-1)",
		`name="own-diskGB" value="on" class="own-switch" checked`, `id="f-diskGB" name="diskGB" value="10" data-inherit="10" min="0" step="1" class="short" autocomplete="off" spellcheck="false" data-focus>`,
		"suggested: 10\u00a0GB, as the other tenants have; its data: 1.4\u00a0GB"} {
		if !strings.Contains(settings, want) {
			t.Fatalf("its settings lack %q:\n%s", want, settings)
		}
	}
	// The at-risk line says what to set; the review of a limit says both
	// sides as everywhere else.
	if !strings.Contains(after, "It runs with no disk limit</span>: the other tenants have 10 GB.") {
		t.Fatalf("the risk does not say what to set:\n%s", after)
	}
	review := url.Values{"do": {"review"}, "generation": {between(settings, `name="generation" value="`, `"`)}, "app": {"testapp"}, "owns": {"1"}, "own-memoryMB": {"on"}, "memoryMB": {"768"}, "own-diskGB": {"on"}, "diskGB": {"20"}}
	if _, body := submit(t, client, server, "/tenants/atlas/settings", review, server.URL); !strings.Contains(said(body), `<td class="l-main">Disk</td><td class="dim l-sub" data-label="Now"><span class="st warn">no limit (no default on fsn-1)</span></td><td class="l-sub" data-label="After">20 GB, tenant override</td>`) || strings.Contains(body, "the node&#39;s default") || !strings.Contains(body, `<p class="summary-line">It ends the missing disk limit; `) {
		t.Fatalf("the settings review of a disk limit:\n%s", body)
	}
	if _, list := get(t, server.URL+"/tenants?show=risk"); !strings.Contains(list, ">no disk limit</div>") {
		t.Fatalf("the tenants at risk lack atlas:\n%s", list)
	}
	if _, overview := get(t, server.URL+"/"); !strings.Contains(overview, "runs with no disk limit, which the other tenants have") {
		t.Fatalf("the overview lacks atlas's lost limit:\n%s", overview)
	}
	// Changing back: who wrote the data was read (1000:1000), the user web
	// runs as, so nothing is red; SESSION_KEY gets the value it kept.
	_, back := get(t, server.URL+"/tenants/atlas/settings?do=review&app=web")
	for _, want := range []string{"Review changing atlas to app web", "atlas&#39;s data at /data was written as 1000:1000, the user web runs as.",
		"Given back", "SESSION_KEY (secret)</span>: its value from before", "Back: keep testapp</a>",
		// The limits it runs with on testapp, said as everywhere else.
		`<td class="l-main">Processes</td><td class="dim l-sub" data-label="Now">512 (node default on <span class="nowrap">fsn-1</span>)</td><td class="l-sub" data-label="After">256</td>`,
		"<td class=\"l-main\">Disk</td><td class=\"dim l-sub\" data-label=\"Now\"><span class=\"st warn\">no limit (no default on <span class=\"nowrap\">fsn-1</span>)</span></td><td class=\"l-sub\" data-label=\"After\">10\u00a0GB</td>",
		`<tr class="same"><td class="l-main">CPUs</td><td class="dim l-sub" data-label="Now">1 (node default on <span class="nowrap">fsn-1</span>)</td>`,
		"App testapp → web: 6 settings change; it ends the missing disk limit; atlas restarts"} {
		if !strings.Contains(back, want) {
			t.Fatalf("the change back lacks %q:\n%s", want, back)
		}
	}
	if strings.Contains(back, `class="notice bad"`) || strings.Contains(back, "Made new for it") {
		t.Fatalf("the change back warns of a guess, or makes a kept secret new:\n%s", back)
	}
	if after, _ := cluster.Get(ctx, "atlas"); after.App != "testapp" {
		t.Fatalf("atlas runs app %q", after.App)
	}
}

// An Override sent on with no value is said on its row, never taken as
// "nothing changes".
func TestAnEmptyOverrideIsSaid(t *testing.T) {
	cluster, server, client := newAppsPanel(t)
	acme, _ := cluster.Get(context.Background(), "acme-logistics")
	form := url.Values{"do": {"review"}, "generation": {strconv.FormatInt(acme.Generation, 10)}, "app": {"web"}, "owns": {"1"}, "own-memoryMB": {"on"}, "memoryMB": {""}}
	_, body := submit(t, client, server, "/tenants/acme-logistics/settings", form, server.URL)
	for _, want := range []string{"Memory: Override is on but has no value. Type one, or turn Override off.", `<tr class="setting missing" id="memory">`,
		`name="own-memoryMB" value="on" class="own-switch" checked`, `aria-invalid="true" data-focus`, "Give a value, or turn Override off", `data-inherit="256"`} {
		if !strings.Contains(body, want) {
			t.Fatalf("an empty override lacks %q:\n%s", want, body)
		}
	}
	if strings.Contains(body, "Nothing changes: the form holds") {
		t.Fatalf("an empty override said nothing changes:\n%s", body)
	}
}

// Events name a rollout's waves as the plan and the pages do: "wave 2, a
// tenth"; an older event that said only the number is named the same way.
func TestEventsNameTheirWave(t *testing.T) {
	server := newServerOf(t, NewDemoCluster())
	_, page := get(t, server.URL+"/events?show=all")
	if !strings.Contains(page, "wave 2 (a tenth): revision 2 → 3") || strings.Contains(page, "wave 2: revision") {
		t.Fatalf("events' waves:\n%s", page)
	}
	if got := namedWave(store.Event{Action: "app.rolled-out", Detail: "wave 3: revision 1 → 2"}); got != "wave 3 (half): revision 1 → 2" {
		t.Fatalf("an older event: %q", got)
	}
}

// The data's owner is warned of only as read: a different owner is red, an
// owner not read is said in grey, never guessed from the app's user.
func TestAnAppChangeWarnsOfTheOwnerAsRead(t *testing.T) {
	now := api.Tenant{App: "web", Spec: spec.Tenant{Name: "atlas", User: "1000:1000", DataPath: "/data"}}
	to := api.App{Name: "testapp", Spec: spec.Tenant{Image: "x", User: "10001:10001"}}
	if c := appChangeOf(now, to, settingsForm{}, nil); c.OwnerAfter != "" || !strings.Contains(c.OwnerNote, "is not known: its node has not said") {
		t.Fatalf("no status: %+v", c)
	}
	now.Status = &reconcile.Status{}
	if c := appChangeOf(now, to, settingsForm{}, nil); c.OwnerAfter != "" || !strings.Contains(c.OwnerNote, "was not read (it may be empty)") {
		t.Fatalf("not read: %+v", c)
	}
	now.Status.DataOwner, now.Status.DataMixed = "1000:1000", true
	if c := appChangeOf(now, to, settingsForm{}, nil); c.OwnerNow != "1000:1000" || c.OwnerAfter != "10001:10001" || !c.OwnerMixed {
		t.Fatalf("read, another owner: %+v", c)
	}
	now.Status.DataOwner = "10001:10001"
	if c := appChangeOf(now, to, settingsForm{}, nil); c.OwnerAfter != "" || !strings.Contains(c.OwnerNote, "the user testapp runs as") {
		t.Fatalf("read, the new app's own user: %+v", c)
	}
}

// A blocked wave is said one way on the tenant's page, the overview and
// the app's page.
func TestOneBlockedWaveSentence(t *testing.T) {
	server := newServerOf(t, NewDemoCluster())
	const sentence = "baltic-freight moved in wave 2 (a tenth) and is not healthy, so wave 3 (half) waits."
	for _, path := range []string{"/tenants/baltic-freight", "/", "/apps/web"} {
		if _, page := get(t, server.URL+path); !strings.Contains(said(page), sentence) {
			t.Fatalf("%s does not say %q:\n%s", path, sentence, page)
		}
	}
}

// A node's tenants are counted one way: its page, its menu and the
// overview count the live ones, archived ones said apart.
func TestOneCountPerNode(t *testing.T) {
	server := newServerOf(t, NewDemoCluster())
	_, node := get(t, server.URL+"/nodes/nbg-1")
	if !strings.Contains(node, `Runs</div><div class="v num">3 tenants</div><div class="dim small">and 1 archived</div>`) || !strings.Contains(node, "Now it runs 3 tenants (and 1 archived) and keeps 4 copies.") ||
		!strings.Contains(node, `Keeps copies of</div><div class="v num">4 tenants`) {
		t.Fatalf("nbg-1's page:\n%s", node)
	}
	if _, overview := get(t, server.URL+"/"); !strings.Contains(between(overview, `href="/nodes/nbg-1">nbg-1</a>`, "</tr>"), `3 <span class="dim">· 4 copies</span>`) {
		t.Fatalf("nbg-1 on the overview:\n%s", between(overview, `href="/nodes/nbg-1">nbg-1</a>`, "</tr>"))
	}
}

// A limit in effect is said one way, with where it comes from: the
// tenant's own, its app's, its node's default, or none, a risk when other
// tenants have one; not known when its node has not said its defaults.
func TestLimitsInEffect(t *testing.T) {
	status := &reconcile.Status{Defaults: spec.Limits{MemoryMB: 512, CPUs: 1, PIDs: 512}}
	got := limitsInEffect(spec.Limits{MemoryMB: 768, PIDs: 256}, spec.Limits{MemoryMB: 768}, "web", status, "fsn-1", map[string]bool{"disk": true})
	want := []struct{ from, said, source, inEffect string }{
		{fromOwn, "768 MB", "Tenant override", "768 MB"},
		{fromNode, "1", "Node default (fsn-1)", "1 (node default on fsn-1)"},
		{fromApp, "256", "Inherited from web", "256"},
		{fromNone, "", "None: fsn-1 has no default", "no limit (no default on fsn-1)"},
	}
	for i, w := range want {
		if l := got[i]; l.From != w.from || l.Said != w.said || l.Source() != w.source || l.InEffect() != w.inEffect {
			t.Fatalf("%s: %+v, source %q, in effect %q", l.Key, l, l.Source(), l.InEffect())
		}
	}
	if !got[3].Risk || got[0].Risk || got[1].Risk {
		t.Fatalf("risks: %+v", got)
	}
	// No other tenant has a disk limit: none is no risk.
	if l := limitsInEffect(spec.Limits{}, spec.Limits{}, "web", status, "fsn-1", map[string]bool{})[3]; l.Risk {
		t.Fatalf("a limit nobody has said to be a risk: %+v", l)
	}
	// Its node has not said its defaults.
	if l := limitsInEffect(spec.Limits{}, spec.Limits{}, "web", nil, "fsn-1", nil)[1]; l.From != fromUnknown || l.Risk || l.Short() != "not known" {
		t.Fatalf("defaults not known: %+v", l)
	}
	if keys, field := risks(got); len(keys) != 1 || keys[0] != "disk" || field != "diskGB" {
		t.Fatalf("risks %v, field %q", keys, field)
	}
}

// settingsValues is a tenant's settings form as it stands, as the review
// sends it: the values a test changes before reviewing.
func settingsValues(t api.Tenant) url.Values {
	f := formOf(t)
	return url.Values{"generation": {strconv.FormatInt(f.Generation, 10)}, "app": {f.App}, "image": {f.Image}, "env": {f.Env}, "secrets": {f.Secrets},
		"memoryMB": {f.MemoryMB}, "cpus": {f.CPUs}, "pids": {f.PIDs}, "diskGB": {f.DiskGB}, "flags": {"1"}, "canary": {f.Canary}, "hold": {f.Hold}}
}

// A disk limit at or below the data a tenant holds is one its node does
// not set (ZFS refuses such a refquota): the review says so in red, offers
// a limit that leaves room, and nothing saves it. One set behind the
// panel's back is said as its node refused it: it keeps the one it had.
func TestADiskLimitBelowItsDataIsNotSet(t *testing.T) {
	cluster, server, client := newAppsPanel(t)
	atlas, _ := cluster.Get(context.Background(), "atlas")
	form := settingsValues(atlas)
	form.Set("do", "review")
	form.Set("diskGB", "1")
	_, body := submit(t, client, server, "/tenants/atlas/settings", form, server.URL)
	body = said(body)
	for _, want := range []string{"1 GB is less than the 1.4 GB atlas holds: its node does not set a disk limit below its data.", "10 GB is what the other tenants have: it leaves room to grow.",
		`<td class="l-main">Disk</td><td class="dim l-sub" data-label="Now">10 GB, inherited from web</td><td class="l-sub" data-label="After">1 GB, tenant override</td>`,
		`<input type="hidden" name="suggest" value="10"><button type="submit" name="do" value="usedisk" class="primary">Use 10 GB instead</button>`} {
		if !strings.Contains(body, want) {
			t.Fatalf("the review of a too small disk limit lacks %q:\n%s", want, body)
		}
	}
	if strings.Contains(body, `value="save"`) {
		t.Fatalf("a disk limit below its data can be saved:\n%s", between(body, `<div class="actions">`, "</div>"))
	}
	fields := func(page string) url.Values {
		values := url.Values{}
		for _, name := range []string{"generation", "reviewed", "app", "image", "memoryMB", "cpus", "pids", "diskGB", "env", "secrets", "flags", "canary", "hold", "suggest"} {
			values.Set(name, html.UnescapeString(between(page, `name="`+name+`" value="`, `"`)))
		}
		return values
	}
	save := fields(body)
	save.Set("do", "save")
	save.Set("confirm", "atlas")
	if _, refused := submit(t, client, server, "/tenants/atlas/settings", save, server.URL); !strings.Contains(refused, "its node does not set a disk limit below its data: give atlas more than 1.4 GB") {
		t.Fatalf("a disk limit below its data was saved:\n%s", refused)
	}
	if still, _ := cluster.Get(context.Background(), "atlas"); still.Overrides.Limits.DiskGB != 0 {
		t.Fatalf("saved a disk limit of %d GB below its data", still.Overrides.Limits.DiskGB)
	}
	// "Use 10 GB instead": the same review with 10 GB, which saves at once,
	// with no restart: a disk limit alone is set on its data as it runs.
	use := fields(body)
	use.Set("do", "usedisk")
	_, again := submit(t, client, server, "/tenants/atlas/settings", use, server.URL)
	again = said(again)
	for _, want := range []string{`data-label="After">10 GB, tenant override</td>`, "Saving sets it on atlas's data at once: atlas keeps running, nothing restarts.", `class="primary">Save</button>`} {
		if !strings.Contains(again, want) {
			t.Fatalf("the review with the suggested limit lacks %q:\n%s", want, again)
		}
	}
	saved := fields(again)
	saved.Set("do", "save")
	// Saved, it returns to atlas's Settings, where it was asked, saying
	// so; its Overview's save line says it too.
	response, landed := submit(t, client, server, "/tenants/atlas/settings", saved, server.URL)
	if response.Request.URL.Path != "/tenants/atlas/settings" || !strings.Contains(landed, "atlas&#39;s disk limit is set on its data: nothing restarted") {
		t.Fatalf("after the save: at %s\n%s", response.Request.URL, landed)
	}
	if after, _ := cluster.Get(context.Background(), "atlas"); after.Overrides.Limits.DiskGB != 10 || after.Status.Phase != "running" || after.Status.Generation != after.Generation {
		t.Fatalf("after a disk limit alone: %+v, status %+v", after.Overrides.Limits, after.Status)
	}
	if _, page := get(t, server.URL+"/tenants/atlas"); !strings.Contains(between(page, `id="operation"`, "</div>"), `<span class="lead-in">Disk limit set to 10 GB</span>`) ||
		!strings.Contains(between(page, `id="operation"`, "</div>"), "done: nothing restarted") {
		t.Fatalf("a disk limit set live is not done:\n%s", between(page, `id="operation"`, "</div>"))
	}
	// Set behind the panel's back, below its data: its node did not set
	// it, and it is not broken.
	atlas, _ = cluster.Get(context.Background(), "atlas")
	overrides := atlas.Overrides
	overrides.Limits.DiskGB = 1
	if _, err := cluster.AsApps("root").Configure(context.Background(), "atlas", api.TenantRequest{App: atlas.App, Overrides: overrides, Generation: atlas.Generation}); err != nil {
		t.Fatal(err)
	}
	_, page := get(t, server.URL+"/tenants/atlas")
	if !strings.Contains(page, "Its node did not set its 1 GB disk limit</span>: ZFS sets none below what its data holds, 1.4 GB. It keeps the limit it had: 10 GB.") || strings.Contains(page, "cannot write") {
		t.Fatalf("a refused disk limit:\n%s", between(page, `<div class="notices"`, "<section"))
	}
}

// A tenant whose data grows to fill its disk limit cannot write: broken
// everywhere, said once on its page, with the fix that lands in the field,
// until the limit is raised.
func TestADiskLimitItsDataFills(t *testing.T) {
	cluster, server, client := newAppsPanel(t)
	atlas, _ := cluster.Get(context.Background(), "atlas")
	overrides := atlas.Overrides
	overrides.Limits.DiskGB = 2
	if _, err := cluster.AsApps("jan").Configure(context.Background(), "atlas", api.TenantRequest{App: atlas.App, Overrides: overrides, Generation: atlas.Generation}); err != nil {
		t.Fatal(err)
	}
	if err := cluster.FillDisk("atlas"); err != nil {
		t.Fatal(err)
	}
	_, page := get(t, server.URL+"/tenants/atlas")
	page = said(page)
	for _, want := range []string{"It cannot write: its data, 2 GB, fills its 2 GB disk limit.", `class="button primary">Raise its disk limit</a>`} {
		if !strings.Contains(page, want) {
			t.Fatalf("atlas's page lacks %q:\n%s", want, page)
		}
	}
	// The red notice says it once; the operation line only records the
	// save, in grey: never "done".
	if strings.Contains(page, "done:") || !strings.Contains(page, `<div class="nrow" role="status" id="operation"><span class="lead-in">Disk limit set to 2 GB</span> by jan`) ||
		!strings.Contains(between(page, `id="operation"`, "</div>"), ": applied, nothing restarted. ") {
		t.Fatalf("a tenant that cannot write said done:\n%s", between(page, `id="operation"`, "</div>"))
	}
	if notices := between(page, `<div class="notices"`, "<section"); strings.Count(notices, "cannot write") != 1 || strings.Index(notices, "It cannot write") > strings.Index(notices, `id="operation"`) {
		t.Fatalf("the notices say it cannot write other than once, first:\n%s", notices)
	}
	// Its Data figure comes first in Use now.
	if use := between(page, `<h2 class="caps">Use now</h2>`, "</section>"); !strings.HasPrefix(strings.TrimSpace(between(use, `<div class="stats">`, "</div></div>")), `<div class="stat hot"><div class="caps">Data</div>`) {
		t.Fatalf("Use now does not begin with its data:\n%s", use)
	}
	// "Raise its disk limit" lands in the field: its row opened, the
	// caret in it with a value that leaves room, the limit its data fills
	// said in red.
	_, raise := get(t, server.URL+setLimit("atlas", "diskGB"))
	disk := between(raise, `id="diskGB"`, "</tr>")
	for _, want := range []string{`<span class="st bad">2 GB</span>`, "<span class=\"risk-why bad\">Its data, 2\u00a0GB, fills it: it cannot write.</span>", `id="f-diskGB" name="diskGB" value="10"`, "data-focus",
		"suggested: 10\u00a0GB, as the other tenants have</span>"} {
		if !strings.Contains(disk, want) {
			t.Fatalf("the Raise landing's Disk row lacks %q:\n%s", want, disk)
		}
	}
	if _, list := get(t, server.URL+"/tenants?show=broken"); !strings.Contains(list, ">data fills its disk limit") {
		t.Fatalf("the broken tenants lack atlas:\n%s", list)
	}
	_, overview := get(t, server.URL+"/")
	overview = said(overview)
	if !strings.Contains(overview, "atlas</a> cannot write: its data, 2 GB, fills its 2 GB disk limit") || !strings.Contains(overview, "Raise its disk limit") {
		t.Fatalf("the overview lacks atlas:\n%s", overview)
	}
	if !strings.Contains(overview, `title="3 tenants broken"`) {
		t.Fatalf("the broken count does not count atlas:\n%s", between(overview, `<nav class="menu"`, "</nav>"))
	}
	// Why, in a line: who gave it the limit its data fills.
	if item := between(overview, `>atlas</a> cannot write`, "</li>"); !strings.Contains(item, "Its 2 GB limit was set by jan (panel) ") {
		t.Fatalf("the overview does not say whose limit it is:\n%s", item)
	}
	// Not healthy: in the overview's figure, the tenants list's count, its
	// State, its head; its Data on a phone's card.
	if !strings.Contains(overview, `<div class="n">4<small> / 6</small></div><div class="l">tenants healthy</div>`) {
		t.Fatalf("the overview counts atlas healthy:\n%s", between(overview, `class="figure"`, "</section>"))
	}
	_, list := get(t, server.URL+"/tenants")
	row := between(list, `href="/tenants/atlas">atlas</a>`, "</tr>")
	if !strings.Contains(list, "· 4 healthy") || !strings.Contains(row, `<span class="st bad">cannot write</span>`) || !strings.Contains(said(row), `class="barcell l-sub phone-only" data-label="Data"`) ||
		!strings.Contains(said(row), `<span class="num red">2 GB <span class="dim of">of 2 GB</span></span>`) || !strings.Contains(list, "</html>") {
		t.Fatalf("the tenants list counts atlas healthy:\n%s\n%s", between(list, `<p class="lead">`, "</p>"), row)
	}
	if !strings.Contains(between(page, `<div class="title">`, "</div>"), `<span class="st bad">cannot write</span>`) {
		t.Fatalf("atlas's head:\n%s", between(page, `<div class="title">`, "</div>"))
	}
	// Raised from where the fix landed, 10 as suggested: it writes again,
	// at once, and nothing restarted.
	atlas, _ = cluster.Get(context.Background(), "atlas")
	form := settingsValues(atlas)
	form.Set("do", "review")
	form.Set("diskGB", between(disk, `id="f-diskGB" name="diskGB" value="`, `"`))
	_, review := submit(t, client, server, "/tenants/atlas/settings", form, server.URL)
	if !strings.Contains(said(review), `data-label="Now"><span class="st bad">2 GB, tenant override</span><span class="risk-why bad">Its data fills it.</span></td>`) ||
		!strings.Contains(said(review), `data-label="After">10 GB, tenant override</td>`) || !strings.Contains(review, "nothing restarts") || !strings.Contains(review, `<p class="summary-line">It lets atlas write again; atlas keeps running.</p>`) {
		t.Fatalf("the raise's review:\n%s", between(review, "<h2>Review", "</form>"))
	}
	save := url.Values{"do": {"save"}}
	for _, name := range []string{"generation", "reviewed", "app", "image", "memoryMB", "cpus", "pids", "diskGB", "env", "secrets", "flags", "canary", "hold"} {
		save.Set(name, html.UnescapeString(between(review, `name="`+name+`" value="`, `"`)))
	}
	// The save returns to its Settings, where it was asked, saying it can
	// write again; on its Overview, the red notice is gone, the save's line
	// saying what it did.
	response, saved := submit(t, client, server, "/tenants/atlas/settings", save, server.URL)
	if response.Request.URL.Path != "/tenants/atlas/settings" || strings.Contains(saved, "cannot write") || !strings.Contains(saved, "atlas&#39;s disk limit is set on its data: it can write again, nothing restarted") {
		t.Fatalf("raised, atlas's settings: at %s\n%s", response.Request.URL, saved)
	}
	if _, page = get(t, server.URL+"/tenants/atlas"); strings.Contains(page, "cannot write") ||
		!strings.Contains(said(between(page, `id="operation"`, "</div>")), `<span class="lead-in">Disk limit raised to 10 GB</span> by `) ||
		!strings.Contains(between(page, `id="operation"`, "</div>"), ": it can write again, nothing restarted. ") {
		t.Fatalf("raised, atlas:\n%s", between(page, `<div class="notices"`, "<section"))
	}
}

// A browser sends a form's line breaks as CRLF. For a tenant with two env
// overrides and a secret, a review sent back so (as "Use 10 GB instead"
// does) changes the disk limit alone: nothing restarts; and a save of its
// Held flag alone saves the flag, and no settings.
func TestFormsAsABrowserSendsThem(t *testing.T) {
	cluster, server, client := newAppsPanel(t)
	atlas, _ := cluster.Get(context.Background(), "atlas")
	if len(atlas.Overrides.Env) < 2 || len(atlas.Overrides.Secrets) < 1 {
		t.Fatalf("atlas's overrides: %+v", atlas.Overrides)
	}
	crlf := func(values url.Values) url.Values {
		for key, list := range values {
			for i, v := range list {
				list[i] = strings.ReplaceAll(v, "\n", "\r\n")
			}
			values[key] = list
		}
		return values
	}
	// As the review's hidden fields come back from a browser.
	fieldsOf := func(page string) url.Values {
		values := url.Values{}
		for _, name := range []string{"generation", "reviewed", "app", "image", "memoryMB", "cpus", "pids", "diskGB", "env", "secrets", "flags", "canary", "hold", "suggest"} {
			values.Set(name, html.UnescapeString(between(page, `name="`+name+`" value="`, `"`)))
		}
		return crlf(values)
	}
	form := crlf(settingsValues(atlas))
	form.Set("do", "review")
	form.Set("diskGB", "1")
	_, small := submit(t, client, server, "/tenants/atlas/settings", form, server.URL)
	if !strings.Contains(fieldsOf(small).Get("env"), "\r\n") {
		t.Fatalf("the env sent back has no CRLF: %q", fieldsOf(small).Get("env"))
	}
	use := fieldsOf(small)
	use.Set("do", "usedisk")
	_, again := submit(t, client, server, "/tenants/atlas/settings", use, server.URL)
	if !strings.Contains(again, "nothing restarts") || !strings.Contains(again, `class="primary">Save</button>`) || strings.Contains(again, "Save and restart") || !strings.Contains(again, "Review 1 change to atlas") {
		t.Fatalf("the review sent back with CRLF:\n%s", between(again, "<h2>Review", "</form>"))
	}
	// Held alone, its review and its save as a browser sends them.
	atlas, _ = cluster.Get(context.Background(), "atlas")
	held := crlf(settingsValues(atlas))
	held.Set("do", "review")
	held.Set("hold", "on")
	_, review := submit(t, client, server, "/tenants/atlas/settings", held, server.URL)
	save := fieldsOf(review)
	save.Set("do", "save")
	_, body := submit(t, client, server, "/tenants/atlas/settings", save, server.URL)
	if !strings.Contains(body, "atlas is held: it takes no rollouts until released") || strings.Contains(body, "settings are") {
		t.Fatalf("a Held save sent with CRLF:\n%s", between(body, `role="status"`, "</div>"))
	}
	events, _ := cluster.Events(context.Background(), store.EventQuery{Tenant: "atlas"})
	if after, _ := cluster.Get(context.Background(), "atlas"); !after.Hold || events[0].Action != "tenant.flags" || events[1].Action == "tenant.updated" && events[1].At.After(time.Now().Add(-time.Second)) {
		t.Fatalf("held %v; the newest events %s, %s: the settings were saved too", after.Hold, events[0].Action, events[1].Action)
	}
}
