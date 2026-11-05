package panel

import (
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/PowerPenguini/murder/internal/store"
)

func TestActionsAndPeople(t *testing.T) {
	if actionText("tenant.replica-ready") != "copy ready" || actionText("node.unheard-of") != "unheard of" {
		t.Fatalf("actions: %q, %q", actionText("tenant.replica-ready"), actionText("node.unheard-of"))
	}
	for actor, want := range map[string]bool{"murder": false, "murder (failover)": false, "jan (panel)": true, "root": true, "murder-panel socket via root": true} {
		if byPeople(store.Event{Actor: actor}) != want {
			t.Errorf("byPeople(%q) != %v", actor, want)
		}
	}
	lines := logLines("ok\nWARN slow query\nlevel=error broke\n")
	if len(lines) != 3 || lines[0].Level != "" || lines[1].Level != "warn" || lines[2].Level != "error" || logLines("") != nil {
		t.Fatalf("lines: %+v", lines)
	}
}

func TestReleases(t *testing.T) {
	one, mixed := releaseOf([]nodeRow{{Version: "abc", Schema: 13}, {Version: "abc", Schema: 13}, {}})
	if mixed || one != "Every node that answers runs release abc (schema 13)." {
		t.Fatalf("one release: %q", one)
	}
	two, mixed := releaseOf([]nodeRow{{Version: "abc", Schema: 13}, {Version: "def", Schema: 14}})
	if !mixed || !strings.Contains(two, "run abc (schema 13); ") || !strings.Contains(two, "run it again") {
		t.Fatalf("two releases: %q", two)
	}
}

// The events page shows what people did when asked, offers the names to
// filter by, and a page that is not there says so.
func TestEventsAndErrorsPages(t *testing.T) {
	cluster := NewDemoCluster()
	s, err := New(cluster, slog.New(slog.NewTextHandler(io.Discard, nil)))
	if err != nil {
		t.Fatal(err)
	}
	server := httptest.NewServer(s.Handler())
	defer server.Close()
	_, all := get(t, server.URL+"/events?show=all")
	_, people := get(t, server.URL+"/events?people=1")
	_, shown := get(t, server.URL+"/events")
	if !strings.Contains(all, "has its copy ready") || strings.Contains(people, "has its copy ready") || !strings.Contains(people, "</a> was suspended") {
		t.Fatalf("people's events:\n%s", people)
	}
	// By default, what people did and what broke; the cluster's keeping
	// is a click away.
	if strings.Contains(shown, "has its copy ready") || !strings.Contains(shown, "</a> was suspended") || !strings.Contains(shown, "failed over to its copy") || strings.Contains(people, "failed over to its copy") || !strings.Contains(shown, "Include them") {
		t.Fatalf("the default events:\n%s", shown)
	}
	if !strings.Contains(all, `<option value="baltic-freight">`) || !strings.Contains(all, `<option value="web">`) {
		t.Fatalf("the filters offer no names:\n%s", all)
	}
	response, page := get(t, server.URL+"/tenants/nobody")
	if response.StatusCode != http.StatusNotFound || !strings.Contains(page, "<h1>Not found</h1>") || !strings.Contains(page, "the overview lists what there is") {
		t.Fatalf("a tenant not there: %d\n%s", response.StatusCode, page)
	}
}
