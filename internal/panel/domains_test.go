package panel

import (
	"html"
	"net/url"
	"strings"
	"testing"
	"time"

	"github.com/PowerPenguini/murder/internal/api"
	"github.com/PowerPenguini/murder/internal/store"
)

// A tenant's page shows its domains in a box: each with its state, the
// DNS record to create and its certificate.
func TestTheTenantPageShowsItsDomains(t *testing.T) {
	_, server, _ := newActingPanel(t)
	_, page := get(t, server.URL+"/tenants/acme-logistics")
	section := between(page, `<section id="domains">`, `</section>`)
	for _, want := range []string{
		"portal.acme-logistics.com", "Active", "CNAME acme-logistics.example.com", "until ",
		"acme-logistics.com", "Waiting for its DNS", "A 203.0.113.10", "AAAA 2001:db8::10", "it points at 192.0.2.80, which no node of the cluster has",
		`name="do" value="domain-add"`, "Check its DNS now", "Remove…",
	} {
		if !strings.Contains(section, want) {
			t.Errorf("the domains box lacks %q:\n%s", want, section)
		}
	}
	// Red is for what is wrong: a domain waiting for its DNS is not.
	if strings.Contains(section, "red-text") || strings.Contains(section, "st bad") {
		t.Errorf("a domain waiting for its DNS shows as wrong:\n%s", section)
	}
	_, other := get(t, server.URL+"/tenants/atlas")
	if section := between(other, `<section id="domains">`, `</section>`); !strings.Contains(section, "None: it answers at") {
		t.Errorf("a tenant without domains:\n%s", section)
	}
}

// A domain is added after a review that shows its DNS record; the add
// carries the review's token, so only what was reviewed is added. A name
// that cannot be a domain is said so, in red, with what was typed kept.
func TestDomainsAreAddedAfterAReview(t *testing.T) {
	cluster, server, client := newActingPanel(t)
	_, page := get(t, server.URL+"/tenants/atlas?do=domain-add&domain="+url.QueryEscape(" Shop.Atlas-Cargo.EU "))
	review := between(page, `id="domain-review"`, `</form>`)
	if !strings.Contains(review, "Add <span class=\"mono\">shop.atlas-cargo.eu</span> to atlas?") || !strings.Contains(review, "shop.atlas-cargo.eu CNAME") || !strings.Contains(review, "atlas.example.com") {
		t.Fatalf("the review:\n%s", review)
	}
	token := between(review, `name="reviewed" value="`, `"`)
	if token == "" {
		t.Fatalf("no token in:\n%s", review)
	}
	// Another name than the one reviewed is refused.
	_, body := submit(t, client, server, "/tenants/atlas", url.Values{"do": {"domain-add"}, "domain": {"else.atlas-cargo.eu"}, "reviewed": {token}}, server.URL)
	if !strings.Contains(html.UnescapeString(body), "review the domain before adding it") {
		t.Fatalf("an add of a name not reviewed:\n%s", between(body, `<main`, `</main>`))
	}
	_, body = submit(t, client, server, "/tenants/atlas", url.Values{"do": {"domain-add"}, "domain": {"shop.atlas-cargo.eu"}, "reviewed": {token}}, server.URL)
	if !strings.Contains(body, "shop.atlas-cargo.eu is added to atlas") {
		t.Fatalf("after adding:\n%s", between(body, `<main`, `</main>`))
	}
	tenant, _ := cluster.Get(t.Context(), "atlas")
	if len(tenant.Domains) != 1 || tenant.Domains[0].State != store.DomainVerifying {
		t.Fatalf("domains %+v", tenant.Domains)
	}
	// The demo's leader checks it within seconds.
	if _, page := get(t, server.URL+"/tenants/atlas"); !strings.Contains(between(page, `<section id="domains">`, `</section>`), "Checking its DNS") {
		t.Fatalf("a domain just added:\n%s", between(page, `<section id="domains">`, `</section>`))
	}
	for given, why := range map[string]string{
		"https://shop.example.com/": "without https://",
		"*.atlas-cargo.eu":          "wildcard",
		"other.example.com":         "base domain",
		"shop.atlas-cargo.eu":       "one of atlas's domains already",
	} {
		_, page := get(t, server.URL+"/tenants/atlas?do=domain-add&domain="+url.QueryEscape(given))
		problem := between(page, `class="nrow bad"`, `</div>`)
		if !strings.Contains(problem, why) || strings.Contains(page, `name="reviewed"`) {
			t.Errorf("%s: %s", given, problem)
		}
		if !strings.Contains(page, `value="`+html.EscapeString(given)+`"`) {
			t.Errorf("%s: what was typed is not kept", given)
		}
	}
}

// A domain is removed with its name typed.
func TestDomainsAreRemovedWithTheirNameTyped(t *testing.T) {
	cluster, server, client := newActingPanel(t)
	_, page := get(t, server.URL+"/tenants/acme-logistics?do=domain-remove&domain=portal.acme-logistics.com")
	if review := between(page, `id="domain-review"`, `</form>`); !strings.Contains(review, "Remove <span class=\"mono\">portal.acme-logistics.com</span> from acme-logistics?") {
		t.Fatalf("the review:\n%s", review)
	}
	_, body := submit(t, client, server, "/tenants/acme-logistics", url.Values{"do": {"domain-remove"}, "domain": {"portal.acme-logistics.com"}, "confirm": {"acme-logistics"}}, server.URL)
	if !strings.Contains(body, "type the domain to remove it") {
		t.Fatal("removed without its name typed")
	}
	_, body = submit(t, client, server, "/tenants/acme-logistics", url.Values{"do": {"domain-remove"}, "domain": {"portal.acme-logistics.com"}, "confirm": {" Portal.acme-logistics.com "}}, server.URL)
	if !strings.Contains(body, "portal.acme-logistics.com is removed from acme-logistics") {
		t.Fatalf("after removing:\n%s", between(body, `<main`, `</main>`))
	}
	tenant, _ := cluster.Get(t.Context(), "acme-logistics")
	if len(tenant.Domains) != 1 || tenant.Domains[0].Name != "acme-logistics.com" {
		t.Fatalf("domains %+v", tenant.Domains)
	}
	if _, body := submit(t, client, server, "/tenants/acme-logistics", url.Values{"do": {"domain-check"}, "domain": {"acme-logistics.com"}}, server.URL); !strings.Contains(body, "DNS is checked within seconds") {
		t.Fatal("check now did not say so")
	}
}

func TestDomainStates(t *testing.T) {
	now := time.Now()
	for _, c := range []struct {
		domain api.Domain
		label  string
		bad    bool
	}{
		{api.Domain{State: store.DomainActive}, "Active", false},
		{api.Domain{State: store.DomainPending}, "Waiting for its DNS", false},
		{api.Domain{State: store.DomainPending, VerifiedAt: now}, "Served, no certificate", false},
		{api.Domain{State: store.DomainIssuing}, "Getting its certificate", false},
		{api.Domain{State: store.DomainRenewing}, "Renewing", false},
		{api.Domain{State: store.DomainRenewing, Attempts: 2, NotAfter: now.Add(time.Hour)}, "Renewal failing", true},
		{api.Domain{State: store.DomainRenewing, Attempts: 2, NotAfter: now.Add(-time.Hour)}, "Certificate expired", true},
		{api.Domain{State: store.DomainFailed}, "Certificate failed", true},
	} {
		state, bad := domainState(c.domain, now)
		if state.Label != c.label || bad != c.bad || bad != (state.Kind == "bad") {
			t.Errorf("%+v: %+v %v", c.domain, state, bad)
		}
	}
}

// The audit log says what happened to a tenant's domains in words, and a
// failed certificate among the failures.
func TestDomainEventsAreSaid(t *testing.T) {
	for action, want := range map[string]string{"domain.added": "domain added", "domain.issued": "domain's certificate issued", "domain.failed": "domain's certificate failed"} {
		if got := actionText(action); got != want {
			t.Errorf("%s said %q", action, got)
		}
	}
	if !failureEvent("domain.failed") || failureEvent("domain.pending") {
		t.Error("the failures are not the failed certificates")
	}
	if got := (eventRow{Event: store.Event{Tenant: "acme", Action: "domain.renewed"}}).Predicate(); got != "had a domain's certificate renewed" {
		t.Errorf("predicate %q", got)
	}
}
