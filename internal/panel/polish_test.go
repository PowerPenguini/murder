package panel

import (
	"context"
	"html"
	"net/http"
	"net/url"
	"strings"
	"testing"
)

// A limit lowered to under twice what a tenant uses of it now is said in
// its review, in amber, and still saved; one raised, or one with room, is
// not; a disk limit under its data is refused as before, not warned of.
func TestTightLimits(t *testing.T) {
	limit := func(key, label string, value float64, said string) limitInEffect {
		return limitInEffect{Key: key, Label: label, Value: value, Said: said, From: fromOwn}
	}
	now := []limitInEffect{limit("memory", "Memory", 512, "512 MB"), limit("disk", "Disk", 10, "10 GB")}
	cases := []struct {
		name         string
		then         []limitInEffect
		memory, disk int64
		want         []string
	}{
		{"memory with room", []limitInEffect{limit("memory", "Memory", 400, "400 MB"), now[1]}, 150 << 20, 1 << 30, nil},
		{"memory tight", []limitInEffect{limit("memory", "Memory", 256, "256 MB"), now[1]}, 200 << 20, 1 << 30, []string{"256\u00a0MB leaves atlas little room:"}},
		{"memory under use", []limitInEffect{limit("memory", "Memory", 128, "128 MB"), now[1]}, 200 << 20, 1 << 30, []string{"128\u00a0MB is less than atlas uses now, 200\u00a0MB:"}},
		{"memory not measured", []limitInEffect{limit("memory", "Memory", 128, "128 MB"), now[1]}, 0, 1 << 30, nil},
		{"disk tight", []limitInEffect{now[0], limit("disk", "Disk", 2, "2 GB")}, 100 << 20, 1536 << 20, []string{"2\u00a0GB leaves atlas little room:"}},
		{"disk under data: refused apart", []limitInEffect{now[0], limit("disk", "Disk", 1, "1 GB")}, 100 << 20, 1536 << 20, nil},
		{"disk raised", []limitInEffect{now[0], limit("disk", "Disk", 12, "12 GB")}, 100 << 20, 7 << 30, nil},
		{"disk none", []limitInEffect{now[0], {Key: "disk", Label: "Disk", From: fromNone}}, 100 << 20, 9 << 30, nil},
	}
	for _, c := range cases {
		got := tightLimits("atlas", now, c.then, c.memory, c.disk)
		if len(got) != len(c.want) {
			t.Fatalf("%s: %+v", c.name, got)
		}
		for i, l := range got {
			if l.Lead != c.want[i] {
				t.Fatalf("%s: %q", c.name, l.Lead)
			}
		}
	}
	// A limit set where there was none counts as lowered.
	none := []limitInEffect{{Key: "memory", Label: "Memory", From: fromNone}, now[1]}
	if got := tightLimits("atlas", none, []limitInEffect{limit("memory", "Memory", 256, "256 MB"), now[1]}, 200<<20, 1<<30); len(got) != 1 {
		t.Fatalf("a limit set where there was none: %+v", got)
	}
}

// The review of a disk limit under twice its data warns, and saves; of a
// memory limit under what it uses, too.
func TestAReviewWarnsOfATightLimit(t *testing.T) {
	cluster, server, client := newAppsPanel(t)
	atlas, _ := cluster.Get(context.Background(), "atlas")
	form := settingsValues(atlas)
	form.Set("do", "review")
	form.Set("diskGB", "2")
	_, review := submit(t, client, server, "/tenants/atlas/settings", form, server.URL)
	review = said(review)
	for _, want := range []string{`<span class="lead-in">2 GB leaves atlas little room:</span> its data is 1.4 GB now, more than half of it. Once its data fills the limit, atlas cannot write; 3 GB is twice its data.`,
		`data-label="After"><span class="st warn">2 GB, tenant override</span>`, `value="save" class="primary">Save</button>`} {
		if !strings.Contains(review, want) {
			t.Fatalf("the review lacks %q:\n%s", want, between(review, "<h2>Review", "</form>"))
		}
	}
	if strings.Contains(between(review, "<h2>Review", "</form>"), "notice bad") {
		t.Fatalf("a tight disk limit is said as wrong:\n%s", between(review, "<h2>Review", "</form>"))
	}
	form.Set("diskGB", "")
	form.Set("memoryMB", "1")
	_, review = submit(t, client, server, "/tenants/atlas/settings", form, server.URL)
	if review = said(review); !strings.Contains(review, `<span class="lead-in">1 MB is less than atlas uses now, `) || !strings.Contains(review, "Save and restart atlas") {
		t.Fatalf("the review of a memory limit under its use:\n%s", between(review, "<h2>Review", "</form>"))
	}
}

// An action from a tenant's page returns to that page, its choices kept,
// and says there what it did; an address that is not one of its pages is
// not followed.
func TestActionsReturnWhereTheyWereAsked(t *testing.T) {
	_, server, client := newAppsPanel(t)
	noFollow := *client
	noFollow.CheckRedirect = func(*http.Request, []*http.Request) error { return http.ErrUseLastResponse }
	where := func(form url.Values) string {
		t.Helper()
		response, _ := submit(t, &noFollow, server, "/tenants/atlas", form, server.URL)
		if response.StatusCode != http.StatusSeeOther {
			t.Fatalf("%v: %d", form, response.StatusCode)
		}
		return response.Header.Get("Location")
	}
	roll := func(back string) url.Values {
		return url.Values{"do": {"roll"}, "confirm": {"atlas"}, "back": {back}}
	}
	for back, want := range map[string]string{
		"/tenants/atlas/logs?level=warn&q=a+b": "/tenants/atlas/logs?level=warn&q=a+b",
		"/tenants/atlas?range=1h":              "/tenants/atlas?range=1h",
		"/tenants/atlas/settings?memoryMB=512": "/tenants/atlas/settings",
		"":                                     "/tenants/atlas",
		"//evil.example/tenants/atlas":         "/tenants/atlas",
		"https://evil.example/tenants/atlas":   "/tenants/atlas",
		"/\\evil.example":                      "/tenants/atlas",
		"/tenants/other":                       "/tenants/atlas",
		"/tenants/atlas/../../cluster":         "/tenants/atlas",
		"/tenants/atlas/logs#x":                "/tenants/atlas",
		"/tenants/atlas/logs\r\nSet-Cookie: x": "/tenants/atlas",
		"/tenants/atlas%2Flogs":                "/tenants/atlas",
		"/apps/web":                            "/tenants/atlas",
		"javascript:alert(1)":                  "/tenants/atlas",
	} {
		if got := where(roll(back)); got != want {
			t.Fatalf("back %q: to %q, want %q", back, got, want)
		}
	}
	// Followed, the page says what it did.
	response, body := submit(t, client, server, "/tenants/atlas", roll("/tenants/atlas/logs?level=warn"), server.URL)
	if response.Request.URL.RequestURI() != "/tenants/atlas/logs?level=warn" || !strings.Contains(body, "atlas is restarting in a new container") {
		t.Fatalf("restarted from its logs: at %s\n%s", response.Request.URL, body)
	}
	// Its pages' forms carry their address.
	if _, page := get(t, server.URL+"/tenants/atlas/logs?level=warn"); !strings.Contains(page, `<input type="hidden" name="do" value="roll"><input type="hidden" name="back" value="/tenants/atlas/logs?level=warn">`) {
		t.Fatalf("the logs' restart form:\n%s", between(page, `value="roll"`, "</form>"))
	}
	if _, page := get(t, server.URL+"/tenants/atlas/settings?memoryMB=512"); !strings.Contains(page, `<input type="hidden" name="do" value="roll"><input type="hidden" name="back" value="/tenants/atlas/settings">`) {
		t.Fatalf("the settings' restart form:\n%s", between(page, `value="roll"`, "</form>"))
	}
	// Suspended from its Logs, which it then has none of: to its Overview.
	if got := where(url.Values{"do": {"suspend"}, "confirm": {"atlas"}, "back": {"/tenants/atlas/logs"}}); got != "/tenants/atlas" {
		t.Fatalf("suspended from its logs: to %q", got)
	}
	if got := where(url.Values{"do": {"resume"}, "back": {"/tenants/atlas/settings"}}); got != "/tenants/atlas/settings" {
		t.Fatalf("resumed from its settings: to %q", got)
	}
}

// A save of a tenant's settings returns to them, saying there what it
// did; a change of its app asked from another page's menu returns there.
func TestSavesReturnWhereTheyWereAsked(t *testing.T) {
	cluster, server, client := newAppsPanel(t)
	fields := func(page string) url.Values {
		// The review's own form: the head's forms carry a back of theirs.
		page = between(page, `id="review"`, "</form>")
		values := url.Values{"do": {"save"}}
		for _, name := range []string{"generation", "reviewed", "app", "image", "memoryMB", "cpus", "pids", "diskGB", "env", "secrets", "flags", "canary", "hold", "back"} {
			values.Set(name, html.UnescapeString(between(page, `name="`+name+`" value="`, `"`)))
		}
		return values
	}
	atlas, _ := cluster.Get(context.Background(), "atlas")
	form := settingsValues(atlas)
	form.Set("do", "review")
	form.Set("diskGB", "20")
	_, review := submit(t, client, server, "/tenants/atlas/settings", form, server.URL)
	response, body := submit(t, client, server, "/tenants/atlas/settings", fields(review), server.URL)
	if response.Request.URL.Path != "/tenants/atlas/settings" || !strings.Contains(body, "atlas&#39;s disk limit is set on its data: nothing restarted") {
		t.Fatalf("a disk limit saved: at %s\n%s", response.Request.URL, body)
	}
	// Its app changed from its Logs' menu: the review's way back, and the
	// save, return to its Logs.
	_, review = get(t, server.URL+"/tenants/atlas/settings?do=review&app=testapp&back="+url.QueryEscape("/tenants/atlas/logs?level=error"))
	if !strings.Contains(review, `<a href="/tenants/atlas/logs?level=error" class="button">Back: keep web</a>`) {
		t.Fatalf("the way back from a review asked from its logs:\n%s", between(review, `<div class="actions">`, "</div>"))
	}
	save := fields(review)
	save.Set("confirm", "atlas")
	response, body = submit(t, client, server, "/tenants/atlas/settings", save, server.URL)
	if response.Request.URL.RequestURI() != "/tenants/atlas/logs?level=error" || !strings.Contains(body, "atlas now runs app testapp") {
		t.Fatalf("its app changed from its logs: at %s\n%s", response.Request.URL, body)
	}
}
