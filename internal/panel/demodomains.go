package panel

import (
	"context"
	"net/netip"
	"slices"
	"strings"
	"time"

	"github.com/PowerPenguini/murder/internal/api"
	"github.com/PowerPenguini/murder/internal/domains"
	"github.com/PowerPenguini/murder/internal/store"
)

// demoPublic are the demo nodes' public addresses, which an apex domain's
// records name.
var demoPublic = []netip.Addr{netip.MustParseAddr("203.0.113.10"), netip.MustParseAddr("203.0.113.11"), netip.MustParseAddr("2001:db8::10")}

// demoDomain is a demo tenant's domain as the agent shows it.
func demoDomain(t api.Tenant, name, state string, since time.Time) api.Domain {
	d := api.Domain{Name: name, Tenant: t.Spec.Name, State: state, CreatedAt: since, CreatedBy: "jan (panel)", Records: domains.Records(name, t.Host, demoPublic)}
	if display := domains.Display(name); display != name {
		d.Display = display
	}
	return d
}

// demoDomains gives acme-logistics two domains: one served with its
// certificate, one whose DNS points elsewhere. The caller holds d.mu, or
// the cluster is not shared yet.
func (d *DemoCluster) demoDomains() {
	t, ok := d.tenants["acme-logistics"]
	if !ok {
		return
	}
	now := time.Now()
	active := demoDomain(t, "portal.acme-logistics.com", store.DomainActive, now.Add(-40*24*time.Hour))
	active.VerifiedAt, active.CheckedAt = active.CreatedAt.Add(time.Minute), active.CreatedAt.Add(time.Minute)
	active.IssuedAt, active.NotAfter = active.CreatedAt.Add(2*time.Minute), active.CreatedAt.Add(90*24*time.Hour)
	pending := demoDomain(t, "acme-logistics.com", store.DomainPending, now.Add(-3*time.Hour))
	pending.Detail, pending.Attempts = "it points at 192.0.2.80, which no node of the cluster has", 6
	pending.CheckedAt, pending.NextAt = now.Add(-20*time.Minute), now.Add(40*time.Minute)
	t.Domains = []api.Domain{pending, active}
	d.tenants[t.Spec.Name] = t
}

// settleDomains moves a demo tenant's new domains on as the leader would,
// their DNS set up already: checked within seconds, then given a
// certificate. The caller holds d.mu.
func (d *DemoCluster) settleDomains(t api.Tenant) api.Tenant {
	if len(t.Domains) == 0 {
		return t
	}
	t.Domains = slices.Clone(t.Domains)
	for i, domain := range t.Domains {
		age := time.Since(domain.CreatedAt)
		switch {
		case domain.State == store.DomainVerifying && age > 3*time.Second:
			domain.State, domain.VerifiedAt, domain.CheckedAt = store.DomainIssuing, time.Now(), time.Now()
			domain.Detail = "ordering a certificate from https://acme-v02.api.letsencrypt.org/directory"
			d.note("murder", "domain.verified", t.Spec.Name, t.App, domain.Name+": a CNAME leads it to "+t.Host)
		case domain.State == store.DomainIssuing && age > 8*time.Second:
			domain.State, domain.Detail, domain.IssuedAt, domain.NotAfter = store.DomainActive, "", time.Now(), time.Now().Add(90*24*time.Hour)
			d.note("murder", "domain.issued", t.Spec.Name, t.App, domain.Name+": valid until "+domain.NotAfter.UTC().Format(time.DateOnly))
		}
		t.Domains[i] = domain
	}
	return t
}

func (a *demoActor) AddDomain(_ context.Context, tenant, name string) (api.DomainAdded, error) {
	a.mu.Lock()
	defer a.mu.Unlock()
	t, ok := a.tenants[tenant]
	if !ok {
		return api.DomainAdded{}, &api.Error{Status: 404, Message: "tenant " + tenant + ": not found"}
	}
	normalized, err := domains.Normalize(name, strings.TrimPrefix(t.Host, tenant+"."))
	if err != nil {
		return api.DomainAdded{}, &api.Error{Status: 400, Message: err.Error()}
	}
	for _, other := range a.tenants {
		for _, domain := range other.Domains {
			if domain.Name != normalized {
				continue
			}
			if other.Spec.Name != tenant {
				return api.DomainAdded{}, &api.Error{Status: 409, Message: "conflicting change: domain " + normalized + " belongs to tenant " + other.Spec.Name + "; remove it there first"}
			}
			return api.DomainAdded{Domain: domain}, nil
		}
	}
	if len(t.Domains) >= store.MaxDomains {
		return api.DomainAdded{}, &api.Error{Status: 409, Message: "tenant " + tenant + " has as many domains as a tenant may"}
	}
	domain := demoDomain(t, normalized, store.DomainVerifying, time.Now())
	domain.CreatedBy = a.actor + " via murder-panel"
	t.Domains = append(slices.Clone(t.Domains), domain)
	slices.SortFunc(t.Domains, func(x, y api.Domain) int { return strings.Compare(x.Name, y.Name) })
	a.tenants[tenant] = t
	a.record("domain.added", tenant, normalized)
	return api.DomainAdded{Domain: domain, Added: true}, nil
}

func (a *demoActor) RemoveDomain(_ context.Context, tenant, name string) error {
	a.mu.Lock()
	defer a.mu.Unlock()
	t, ok := a.tenants[tenant]
	index := -1
	if ok {
		index = slices.IndexFunc(t.Domains, func(d api.Domain) bool { return d.Name == name })
	}
	if index < 0 {
		return &api.Error{Status: 404, Message: "tenant " + tenant + " has no domain " + name + ": not found"}
	}
	t.Domains = slices.Delete(slices.Clone(t.Domains), index, index+1)
	a.tenants[tenant] = t
	a.record("domain.removed", tenant, name)
	return nil
}

func (a *demoActor) CheckDomain(_ context.Context, tenant, name string) error {
	a.mu.Lock()
	defer a.mu.Unlock()
	t, ok := a.tenants[tenant]
	if !ok || !slices.ContainsFunc(t.Domains, func(d api.Domain) bool { return d.Name == name }) {
		return &api.Error{Status: 404, Message: "tenant " + tenant + " has no domain " + name + ": not found"}
	}
	return nil
}
