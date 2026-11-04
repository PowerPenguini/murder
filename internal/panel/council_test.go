package panel

import (
	"context"
	"io"
	"log/slog"
	"net/http"
	"net/http/cookiejar"
	"net/http/httptest"
	"net/url"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/PowerPenguini/murder/internal/api"
	"github.com/PowerPenguini/murder/internal/reconcile"
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
	for _, want := range []string{"What changes", `<span class="tx" role="cell">  memoryMB: 512</span>`, "1 line out, 1 line in", "Canaries", "atlas", "FEATURE_PLANNER", `name="do" value="save"`, "Roll out to 6 tenants", "Held, and left out until released", "A rollout of revision 3 is blocked at A tenth by baltic-freight"} {
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
	for _, want := range []string{`<li class="done"><span class="wn">Canaries</span>`, `<li class="now"><span class="wn">A tenth</span>`, "moved, not healthy: blocks the next wave", "waits for half", "stays: it is held", "Half (kowalski-trans) moves once baltic-freight is healthy", "Move the next wave now", "baltic-freight is not healthy on revision 3. Moving the next wave now risks the same for kowalski-trans", "Restore data snapshot"} {
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
// each other, said once, under the tenant, and only as far as they are true:
// the app's limit reaches the tenants that use it, not one with its own.
func TestCauseTenantAndRolloutJoin(t *testing.T) {
	server := newServerOf(t, NewDemoCluster())
	_, page := get(t, server.URL+"/")
	for _, want := range []string{"It blocks web&#39;s rollout of revision 3: the next wave (half) waits for it.", "web&#39;s limit is likely too low: 5 of the 6 tenants on its 256 MB limit ran out of memory",
		"atlas keeps its own 768 MB", "test-staging is held on its revision", "Raise web&#39;s memory limit (review)", "/apps/web?memoryMB=512#spec", "Only this tenant: raise its limit"} {
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
	oom := func(name string, ago time.Duration) store.Event {
		return store.Event{At: now.Add(-ago), Action: "tenant.restarted", Tenant: name, App: "web", Detail: "exit 137 (most likely out of memory)"}
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
	if !strings.Contains(body, "its new container is starting") || strings.Contains(body, "is restarting in a new container") || strings.Contains(body, "It is starting") {
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
		"Why, as nbg-1 said", "403 Forbidden", "sudo murder backups now", "How to restore kowalski-trans", "backups restore kowalski-trans -at 20", `open id="restore"`} {
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
	if !strings.Contains(row, "Inherited from web") || strings.Contains(row, "Tenant override") || !strings.Contains(row, "512 MB, proposed") {
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
	if !strings.Contains(page, `<div class="reason bad"><span class="st bad"></span>last backup failed, newest 3h old</div>`) || !strings.Contains(page, "blocks its app&#39;s rollout") {
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
	if !strings.Contains(page, "baltic-freight, kowalski-trans, vistula-cargo would run on fsn-1 without a ready copy on another node") || !strings.Contains(page, "(now 3 and 4)") || strings.Contains(page, `value="remove"`) {
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
	for _, want := range []string{"backups restore kowalski-trans -at 2026-11-04T09:00:00Z", "On nbg-1, archive it", "Everything it wrote after that time is replaced", `href="/tenants/kowalski-trans/logs"`} {
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
	if !strings.Contains(body, "Full image reference") || !strings.Contains(body, strings.Repeat("5e", 32)) || !strings.Contains(body, "is blocked at A tenth by baltic-freight") {
		t.Fatalf("the image review:\n%s", body)
	}
}
