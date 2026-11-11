package apiserver

import (
	"context"
	"net/netip"
	"testing"
	"time"

	"github.com/PowerPenguini/murder/internal/store"
)

// Domains are added normalised, shown with the DNS record to create, and
// removed; names that cannot be a tenant's are refused.
func TestTenantsDomainsThroughTheAPI(t *testing.T) {
	f := withApp(t)
	ctx := context.Background()
	for _, name := range []string{"acme", "beta"} {
		if _, err := f.client.Create(ctx, tenantOf(name)); err != nil {
			t.Fatal(err)
		}
	}
	added, err := f.client.AddDomain(ctx, "acme", " App.Customer.COM. ")
	if err != nil || !added.Added || added.Domain.Name != "app.customer.com" || added.Domain.State != store.DomainVerifying {
		t.Fatalf("add: %+v %v", added, err)
	}
	if records := added.Domain.Records; len(records) != 1 || records[0].Type != "CNAME" || records[0].Value != "acme.example.test" {
		t.Fatalf("records %+v", records)
	}
	if again, err := f.client.AddDomain(ctx, "acme", "app.customer.com"); err != nil || again.Added {
		t.Fatalf("add again: %+v %v", again, err)
	}
	_, err = f.client.AddDomain(ctx, "beta", "app.customer.com")
	checkStatus(t, "another tenant's domain", err, 409)
	for _, bad := range []string{"https://x.customer.com/", "*.customer.com", "x.example.test", "203.0.113.4", "customer"} {
		_, err := f.client.AddDomain(ctx, "acme", bad)
		checkStatus(t, bad, err, 400)
	}
	_, err = f.client.AddDomain(ctx, "nobody", "x.customer.com")
	checkStatus(t, "no such tenant", err, 404)
	// The apex gets A and AAAA records with the nodes' addresses.
	if err := f.store.PutNode(store.Node{Name: "test-node", Address: "10.0.0.2:7400"}, "test"); err != nil {
		t.Fatal(err)
	}
	if err := f.store.SetPublic("test-node", []netip.Addr{netip.MustParseAddr("203.0.113.10"), netip.MustParseAddr("2001:db8::10")}); err != nil {
		t.Fatal(err)
	}
	apex, err := f.client.AddDomain(ctx, "acme", "customer.com")
	if err != nil || len(apex.Domain.Records) != 2 || apex.Domain.Records[0].Type != "A" || apex.Domain.Records[1].Value != "2001:db8::10" {
		t.Fatalf("the apex: %+v %v", apex, err)
	}
	list, err := f.client.Domains(ctx, "acme")
	if err != nil || len(list) != 2 || list[0].Name != "app.customer.com" || list[1].Name != "customer.com" {
		t.Fatalf("list: %+v %v", list, err)
	}
	if tenant, err := f.client.Get(ctx, "acme"); err != nil || len(tenant.Domains) != 2 {
		t.Fatalf("the tenant shows its domains: %+v %v", tenant.Domains, err)
	}
	if err := f.client.CheckDomain(ctx, "acme", "app.customer.com"); err != nil {
		t.Fatal(err)
	}
	// A canonical host: one of its domains, as given, or its own name;
	// the tenant, its domain and its route show it.
	checkStatus(t, "another tenant's host", f.client.SetCanonical(ctx, "acme", "beta.example.test"), 400)
	checkStatus(t, "a host it does not have", f.client.SetCanonical(ctx, "acme", "else.customer.com"), 400)
	checkStatus(t, "no such tenant", f.client.SetCanonical(ctx, "nobody", ""), 404)
	if err := f.client.SetCanonical(ctx, "acme", "acme.example.test"); err != nil {
		t.Fatal(err)
	}
	if err := f.client.SetCanonical(ctx, "acme", " App.Customer.com. "); err != nil {
		t.Fatal(err)
	}
	if tenant, err := f.client.Get(ctx, "acme"); err != nil || tenant.Canonical != "app.customer.com" || !tenant.Domains[0].Canonical || tenant.Domains[1].Canonical {
		t.Fatalf("the tenant's canonical host: %+v %v", tenant, err)
	}
	if route, ok := (Routes{Store: f.store}).Route("acme"); !ok || route.Canonical != "app.customer.com" {
		t.Fatalf("the route: %+v %v", route, ok)
	}
	checkStatus(t, "remove another tenant's", f.client.RemoveDomain(ctx, "beta", "app.customer.com"), 404)
	if err := f.client.RemoveDomain(ctx, "acme", "APP.customer.com"); err != nil {
		t.Fatal(err)
	}
	if list, _ := f.client.Domains(ctx, "acme"); len(list) != 1 {
		t.Fatalf("after the remove: %+v", list)
	}
	if tenant, _ := f.client.Get(ctx, "acme"); tenant.Canonical != "" {
		t.Fatalf("the removed domain stays canonical: %q", tenant.Canonical)
	}
	events, _ := f.client.Events(ctx, store.EventQuery{Tenant: "acme", Limit: 1})
	if events[0].Action != "domain.removed" || events[0].Detail != "app.customer.com" {
		t.Fatalf("events %+v", events)
	}
	if err := f.client.SetCanonical(ctx, "acme", ""); err != nil {
		t.Fatal(err)
	}
}

// The edge's table routes a domain once it was seen to point at the
// cluster, with its certificate until that expires, and carries the
// challenges under way.
func TestTheRoutesCarryDomainsAndChallenges(t *testing.T) {
	f := withApp(t)
	ctx := context.Background()
	if _, err := f.client.Create(ctx, tenantOf("acme")); err != nil {
		t.Fatal(err)
	}
	domain, _, err := f.store.AddDomain("acme", "app.customer.com", "alice")
	if err != nil {
		t.Fatal(err)
	}
	routes := Routes{Store: f.store}
	if table := routes.Table(); len(table.Domains) != 0 {
		t.Fatalf("a domain not seen to point here is routed: %+v", table.Domains)
	}
	domain.VerifiedAt, domain.State = time.Now(), store.DomainIssuing
	if err := f.store.UpdateDomain(domain, store.DomainEvent{}, "murder"); err != nil {
		t.Fatal(err)
	}
	if err := f.store.PutChallenge(store.Challenge{Token: "tok", Domain: domain.Name, KeyAuth: "tok.x", ExpiresAt: time.Now().Add(time.Minute)}); err != nil {
		t.Fatal(err)
	}
	table := routes.Table()
	if route := table.Domains["app.customer.com"]; route.Tenant != "acme" || route.Certificate != "" || table.Challenges["tok"].KeyAuth != "tok.x" {
		t.Fatalf("table %+v", table)
	}
	if route, ok := routes.Domain("app.customer.com"); !ok || route.Tenant != "acme" {
		t.Fatalf("Domain: %+v %v", route, ok)
	}
	if challenge, ok := routes.Challenge("tok"); !ok || challenge.Domain != "app.customer.com" {
		t.Fatalf("Challenge: %+v %v", challenge, ok)
	}
	domain.Certificate, domain.Key, domain.NotAfter = "CERT", "KEY", time.Now().Add(time.Hour)
	if err := f.store.UpdateDomain(domain, store.DomainEvent{}, "murder"); err != nil {
		t.Fatal(err)
	}
	if route := routes.Table().Domains["app.customer.com"]; route.Certificate != "CERT" || route.Key != "KEY" {
		t.Fatalf("with its certificate: %+v", route)
	}
	domain.NotAfter = time.Now().Add(-time.Minute)
	if err := f.store.UpdateDomain(domain, store.DomainEvent{}, "murder"); err != nil {
		t.Fatal(err)
	}
	if route := routes.Table().Domains["app.customer.com"]; route.Tenant != "acme" || route.Certificate != "" {
		t.Fatalf("an expired certificate is served: %+v", route)
	}
	// An archived tenant's domains lead nowhere.
	if err := f.client.Archive(ctx, "acme"); err != nil {
		t.Fatal(err)
	}
	if table := routes.Table(); len(table.Domains) != 0 {
		t.Fatalf("an archived tenant's domain is routed: %+v", table.Domains)
	}
	if _, ok := routes.Domain("app.customer.com"); ok {
		t.Fatal("Domain routes an archived tenant's domain")
	}
}
