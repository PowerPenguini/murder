package panel

import (
	"fmt"
	"io"
	"log/slog"
	"net/http/httptest"
	"strings"
	"testing"
	"time"
)

// A backend that measures tenants shows what they use; one that does not
// shows the pages as before.
func TestUsePages(t *testing.T) {
	cluster := NewDemoCluster()
	s, err := New(cluster, slog.New(slog.NewTextHandler(io.Discard, nil)))
	if err != nil {
		t.Fatal(err)
	}
	server := httptest.NewServer(s.Handler())
	defer server.Close()
	_, page := get(t, server.URL+"/")
	if !strings.Contains(page, "of 8 GB") {
		t.Fatalf("the overview lacks the nodes' memory:\n%s", page)
	}
	_, page = get(t, server.URL+"/tenants")
	for _, want := range []string{" MB <span class=\"dim of\">of 256 MB</span>", "Requests/min"} {
		if !strings.Contains(page, want) {
			t.Fatalf("the tenants' page lacks %q:\n%s", want, page)
		}
	}
	_, page = get(t, server.URL+"/tenants/baltic-freight")
	for _, want := range []string{"Use, last 24 hours", "limit 256 MB", "Response time, 95th percentile", `class="chart"`, ">Data</div>", "1.4 GB <small>of 10 GB</small>"} {
		if !strings.Contains(page, want) {
			t.Fatalf("the tenant's page lacks %q:\n%s", want, page)
		}
	}
	// An archived tenant is measured no more.
	if _, page = get(t, server.URL+"/tenants/old-demo"); strings.Contains(page, "Use, last 24 hours") {
		t.Fatal("an archived tenant's page shows its use")
	}

	plain := newTestServer(t, Demo{Now: demoNow})
	if _, page = get(t, plain.URL+"/tenants/atlas"); strings.Contains(page, "Use, last 24 hours") {
		t.Fatal("a backend that measures nothing shows use")
	}
}

// The demo's numbers agree: a tenant's newest measure on its own page is
// the one the cluster's use report, behind the list and the overview,
// gives for it, at every range.
func TestDemoUseAgrees(t *testing.T) {
	cluster := NewDemoCluster()
	// Both read the minute before now: across a minute's turn they rightly
	// differ, so a run the minute turned in is run again.
	for try := 0; ; try++ {
		minute := time.Now().Truncate(time.Minute)
		problem := demoUseDisagrees(t, cluster)
		if problem == "" {
			return
		}
		if try > 0 || time.Now().Truncate(time.Minute).Equal(minute) {
			t.Fatal(problem)
		}
	}
}

func demoUseDisagrees(t *testing.T, cluster *DemoCluster) string {
	ctx := t.Context()
	report, err := cluster.Usage(ctx)
	if err != nil {
		t.Fatal(err)
	}
	for _, step := range []time.Duration{time.Minute, 5 * time.Minute, 15 * time.Minute} {
		series, err := cluster.TenantUsage(ctx, "baltic-freight", step)
		if err != nil {
			t.Fatal(err)
		}
		for _, node := range report.Nodes {
			if now, ok := node.Tenants["baltic-freight"]; ok && (now.Memory != series.Now.Memory || now.Requests != series.Now.Requests || now.CPU != series.Now.CPU) {
				return fmt.Sprintf("at %s the page says %d B, %d requests, %.2f cores; the report %d B, %d requests, %.2f cores", step, series.Now.Memory, series.Now.Requests, series.Now.CPU, now.Memory, now.Requests, now.CPU)
			}
		}
	}
	return ""
}
