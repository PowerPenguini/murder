package panel

import (
	"io"
	"log/slog"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/PowerPenguini/murder/internal/api"
	"github.com/PowerPenguini/murder/internal/reconcile"
	"github.com/PowerPenguini/murder/internal/store"
)

// What needs attention is said plainly, the broken first; what runs as it
// should, or is meant not to run, is not.
func TestAttention(t *testing.T) {
	now := time.Date(2026, 11, 2, 12, 0, 0, 0, time.UTC)
	failed := nodeView{NodeState: api.NodeState{Node: store.Node{Name: "n3", FailedAt: now.Add(-6 * time.Minute)}}}
	failed.State = nodeState(failed.NodeState, now)
	up := nodeView{NodeState: api.NodeState{Node: store.Node{Name: "n1"}, Answers: true}}
	up.State = nodeState(up.NodeState, now)
	tenant := func(name string, status *reconcile.Status, replicaReady bool) tenantRow {
		t := api.Tenant{Node: "n1", Replica: "n2", ReplicaReady: replicaReady, Status: status}
		t.Spec.Name = name
		return tenantRow{Tenant: t, State: tenantState(t), Replica: replicaState(t, true)}
	}
	healthy := &reconcile.Status{Phase: "running", Healthy: true}
	// The agent's own words for a kill (reconcile.lastExit).
	sick := tenant("sick", &reconcile.Status{Phase: "running", LastExit: "killed (SIGKILL) at " + now.Add(-4*time.Minute).UTC().Format(time.RFC3339) + ", most likely for using more than its 256 MB of memory"}, true)
	full := tenant("full", healthy, true)
	full.Use = &tenantUse{MemoryText: "240 MB", MemoryLimit: "256 MB", Memory: meterOf(240, 256)}
	alone := tenant("alone", healthy, false)
	alone.Tenant.Replica = ""
	alone.Replica = replicaState(alone.Tenant, true)
	suspended := tenant("resting", &reconcile.Status{Phase: "stopped"}, true)
	suspended.Spec.Suspended = true
	got := attentionOf([]nodeView{up, failed}, []tenantRow{tenant("fine", healthy, true), full, sick, alone, suspended}, true, now)
	var said []string
	for _, n := range got {
		said = append(said, n.Subject+n.Text)
	}
	text := strings.Join(said, "\n")
	want := []string{
		"n3 failed 6m ago: its tenants run from their copies, and it keeps none",
		"sick is unhealthy: it does not answer its health check; it was last killed (exit 137) at 11:56:00 UTC, most likely for using more than its 256 MB of memory",
		"full uses 240 MB of its 256 MB of memory",
		"alone has no copy: if n1 fails, it stops",
	}
	if len(got) != len(want) {
		t.Fatalf("attention:\n%s", text)
	}
	for _, w := range want {
		if !strings.Contains(text, w) {
			t.Errorf("missing %q in:\n%s", w, text)
		}
	}
	if !got[0].Bad || !got[1].Bad || got[2].Bad {
		t.Fatalf("the broken are not first: %+v", got)
	}
	// Each leads with a headline of 12 words at most, then its fix; the
	// rest is evidence. Never "exit: exit".
	heads := map[string]string{}
	for _, n := range got {
		heads[n.Subject] = n.Subject + n.Head
		if words := len(strings.Fields(n.Subject + n.Head)); words > 12 {
			t.Errorf("%q has %d words", n.Subject+n.Head, words)
		}
		if strings.Contains(n.Head+n.Text+strings.Join(n.Evidence, " "), "exit: exit") {
			t.Errorf("exit: exit in %+v", n)
		}
	}
	if heads["sick"] != "sick is unhealthy: out of memory 4m ago" || heads["full"] != "full uses 240 of 256 MB of memory" {
		t.Fatalf("headlines: %q", heads)
	}
	if e := strings.Join(got[1].Evidence, " "); !strings.Contains(e, "It does not answer its health check.") || !strings.Contains(e, "most likely for using more than its 256 MB of memory") {
		t.Fatalf("sick's evidence: %q", e)
	}
	// A tenant with several things wrong is one notice, saying them all,
	// with a way to fix each; a copy still catching up is a risk too.
	both := tenant("both", &reconcile.Status{Phase: "running"}, false)
	both.Use = full.Use
	got = attentionOf(nil, []tenantRow{both}, true, now)
	if len(got) != 1 || !got[0].Bad || !strings.Contains(got[0].Text, "is unhealthy") || !strings.Contains(got[0].Text, "; it uses 240 MB of its 256 MB") || !strings.Contains(got[0].Text, "still catching up") {
		t.Fatalf("one tenant, several causes: %+v", got)
	}
	if len(got[0].Fixes) != 1 || got[0].Fixes[0].Href != "/tenants/both/logs" || got[0].Primary == nil || got[0].Primary.Href != "/tenants/both/settings#memory" {
		t.Fatalf("fixes: %+v, primary %+v", got[0].Fixes, got[0].Primary)
	}
	if got[0].Head != " is unhealthy: it fails its health check, 240 of 256 MB" || len(got[0].Evidence) != 2 || !strings.Contains(got[0].Evidence[1], "Its copy on n2 is still catching up") {
		t.Fatalf("several causes, the worst first: %q, %q", got[0].Head, got[0].Evidence)
	}
}

// The overview says what needs attention; a tenant's page says what is
// wrong with it, with the way to fix it.
func TestAttentionOnThePages(t *testing.T) {
	cluster := NewDemoCluster()
	s, err := New(cluster, slog.New(slog.NewTextHandler(io.Discard, nil)))
	if err != nil {
		t.Fatal(err)
	}
	s.Actions, s.Apps = cluster.As, cluster.AsApps
	server := httptest.NewServer(s.Handler())
	defer server.Close()
	_, page := get(t, server.URL+"/")
	page = said(page)
	if !strings.Contains(page, `<ul class="attention">`) || !strings.Contains(page, "hel-1</a> failed") || !strings.Contains(page, "baltic-freight</a> is unhealthy") || !strings.Contains(page, "It blocks web&#39;s rollout of revision 3") {
		t.Fatalf("the overview does not say what needs attention:\n%s", page)
	}
	_, page = get(t, server.URL+"/tenants/baltic-freight")
	page = said(page)
	if !strings.Contains(page, `class="nrow bad"`) || !strings.Contains(page, "<strong>It is unhealthy: out of memory 4m ago,") || !strings.Contains(page, `class="button primary">Raise web's memory limit</a>`) ||
		!strings.Contains(page, "Only this tenant: raise its limit") || !strings.Contains(page, `href="/tenants/baltic-freight/logs?container=previous">Its last lines before the exit`) ||
		!strings.Contains(page, "Likely web&#39;s 256 MB limit: 5 of the 6 tenants on it ran out of memory in 24 h.") || !strings.Contains(page, "<summary>Evidence and affected tenants</summary>") {
		t.Fatalf("the tenant's page does not say what is wrong:\n%s", page)
	}
	// A dangerous action's button waits for the name typed.
	if !strings.Contains(page, `data-confirm="baltic-freight"`) {
		t.Fatalf("archiving does not wait for the name:\n%s", page)
	}
	if _, page = get(t, server.URL+"/tenants/atlas"); strings.Contains(page, `class="problem`) {
		t.Fatalf("a tenant that runs as it should is said to have a problem:\n%s", page)
	}
}

// A phone's header says something is broken elsewhere even on a page with
// its own count: a failed node shows from a tenant's page.
func TestElsewhereShowsBesideAPagesOwnCount(t *testing.T) {
	a := navAlerts{Overview: 3, Tenants: 1, Cluster: 2}
	if !a.Elsewhere("tenants") || !a.Elsewhere("events") || a.Elsewhere("overview") {
		t.Fatalf("elsewhere: tenants %v, events %v, overview %v", a.Elsewhere("tenants"), a.Elsewhere("events"), a.Elsewhere("overview"))
	}
	if only := (navAlerts{Overview: 1, Tenants: 1}); only.Elsewhere("tenants") {
		t.Fatal("the tenants' own problem said to be elsewhere")
	}
}
