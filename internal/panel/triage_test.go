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
)

// The tenants' page filters on the server, so it works without a script:
// by what needs attention, and by text, saying how many match.
func TestTenantFilters(t *testing.T) {
	server := newTestServer(t, NewDemoCluster())
	rows := func(page string) []string {
		var names []string
		for _, part := range strings.Split(page, `class="row-link" href="/tenants/`)[1:] {
			names = append(names, part[:strings.Index(part, `"`)])
		}
		return names
	}
	_, page := get(t, server.URL+"/tenants")
	if names := rows(page); len(names) != 8 || names[0] != "baltic-freight" || names[len(names)-1] != "old-demo" {
		t.Fatalf("problems first, the archived last: %v", names)
	}
	_, page = get(t, server.URL+"/tenants?sort=name")
	if names := rows(page); names[0] != "acme-logistics" {
		t.Fatalf("by name: %v", names)
	}
	_, page = get(t, server.URL+"/tenants?show=unprotected")
	if names := strings.Join(rows(page), " "); names != "nordwind vistula-cargo" || !strings.Contains(page, "2 of 8 match") {
		t.Fatalf("unprotected: %s", names)
	}
	_, page = get(t, server.URL+"/tenants?show=attention")
	if names := strings.Join(rows(page), " "); names != "baltic-freight nordwind vistula-cargo" {
		t.Fatalf("needs attention: %s", names)
	}
	_, page = get(t, server.URL+"/tenants?q=NORD")
	if names := strings.Join(rows(page), " "); names != "nordwind" {
		t.Fatalf("text: %s", names)
	}
	_, page = get(t, server.URL+"/tenants?show=archived&q=atlas")
	if len(rows(page)) != 0 || !strings.Contains(page, "No tenants match") || !strings.Contains(page, "Clear the filter") {
		t.Fatalf("no match:\n%s", page)
	}
}

// What is broken is counted beside its page in the side bar, and in the
// title, from any page.
func TestSideBarCountsWhatIsBroken(t *testing.T) {
	server := newTestServer(t, NewDemoCluster())
	for _, path := range []string{"/", "/events", "/tenants/atlas"} {
		_, page := get(t, server.URL+path)
		if !strings.Contains(page, "<title>(2) ") || !strings.Contains(page, `Cluster<span class="count" title="1 broken">1</span>`) || !strings.Contains(page, `Tenants<span class="count" title="1 broken">1</span>`) {
			t.Fatalf("%s does not count what is broken:\n%s", path, page[:min(len(page), 3000)])
		}
	}
	healthy := newTestServer(t, Demo{})
	if _, page := get(t, healthy.URL+"/events"); strings.Contains(page, `class="count"`) && !strings.Contains(page, "<title>(") {
		t.Fatal("counts without a title")
	}
}

// Removing a person, a passkey or a device needs its name typed, checked
// by the server too.
func TestRemovingAccessNeedsTheName(t *testing.T) {
	s, err := New(Demo{Now: demoNow}, slog.New(slog.NewTextHandler(io.Discard, nil)))
	if err != nil {
		t.Fatal(err)
	}
	access := NewDemoAccess(NewMemoryAuth())
	access.Seed()
	s.Access = func(string) AccessAgent { return access }
	server := httptest.NewServer(s.Handler())
	defer server.Close()
	jar, _ := cookiejar.New(nil)
	client := &http.Client{Jar: jar}
	ctx := context.Background()
	for _, c := range []struct {
		form, problem string
		gone          func() bool
	}{
		{"do=remove-device&name=jan-phone", "type the device&#39;s name", func() bool { d, _ := access.Devices(ctx); return len(d) == 1 }},
		{"do=remove-passkey&id=demo-phone&confirm=laptop", "type the passkey&#39;s name", func() bool { u, _ := access.PanelUsers(ctx); return len(u[1].Passkeys) == 1 }},
		{"do=remove-person&name=anna&confirm=ann", "type the person&#39;s name", func() bool { u, _ := access.PanelUsers(ctx); return len(u) == 1 }},
	} {
		form, _ := url.ParseQuery(c.form)
		if _, body := submit(t, client, server, "/access", form, server.URL); !strings.Contains(body, c.problem) || c.gone() {
			t.Fatalf("%s without the name: removed, or not said", c.form)
		}
		switch form.Get("do") {
		case "remove-device":
			form.Set("confirm", "jan-phone")
		case "remove-passkey":
			form.Set("confirm", "phone")
		default:
			form.Set("confirm", "anna")
		}
		if _, body := submit(t, client, server, "/access", form, server.URL); !c.gone() {
			t.Fatalf("%s with the name: not removed:\n%s", c.form, body)
		}
	}
}
