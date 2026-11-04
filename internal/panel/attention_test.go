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
	sick := tenant("sick", &reconcile.Status{Phase: "running", LastExit: "exit 137 (most likely out of memory) 4m ago"}, true)
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
		"sick is unhealthy: it does not answer its health check; last exit: exit 137 (most likely out of memory) 4m ago",
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
	// A tenant with several things wrong is one notice, saying them all,
	// with a way to fix each; a copy still catching up is a risk too.
	both := tenant("both", &reconcile.Status{Phase: "running"}, false)
	both.Use = full.Use
	got = attentionOf(nil, []tenantRow{both}, true, now)
	if len(got) != 1 || !got[0].Bad || !strings.Contains(got[0].Text, "is unhealthy") || !strings.Contains(got[0].Text, "; it uses 240 MB of its 256 MB") || !strings.Contains(got[0].Text, "still catching up") {
		t.Fatalf("one tenant, several causes: %+v", got)
	}
	if len(got[0].Fixes) != 2 || got[0].Fixes[0].Href != "/tenants/both/logs" || got[0].Fixes[1].Href != "/tenants/both/settings#memory" {
		t.Fatalf("fixes: %+v", got[0].Fixes)
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
	if !strings.Contains(page, `<ul class="attention">`) || !strings.Contains(page, "hel-1</a> failed") || !strings.Contains(page, "baltic-freight</a> is unhealthy") || !strings.Contains(page, "web</a> rolls out revision 3") {
		t.Fatalf("the overview does not say what needs attention:\n%s", page)
	}
	_, page = get(t, server.URL+"/tenants/baltic-freight")
	if !strings.Contains(page, `class="problem bad"`) || !strings.Contains(page, "It is unhealthy") || !strings.Contains(page, "Raise its memory limit") {
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
