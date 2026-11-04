package panel

import (
	"io"
	"log/slog"
	"net/http/httptest"
	"strings"
	"testing"
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
	for _, want := range []string{" MB <span class=\"dim\">of 256 MB</span>", "Requests a minute"} {
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
