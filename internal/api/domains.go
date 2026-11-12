package api

import (
	"context"
	"net/http"
	"net/url"
)

// Domains lists a tenant's own domains.
func (c *Client) Domains(ctx context.Context, tenant string) ([]Domain, error) {
	var domains []Domain
	return domains, c.do(ctx, http.MethodGet, tenantPath(tenant)+"/domains", nil, &domains)
}

// AddDomain gives a tenant a domain of its own; the cluster's leader then
// checks that it points at the cluster and orders its certificate.
func (c *Client) AddDomain(ctx context.Context, tenant, name string) (DomainAdded, error) {
	var added DomainAdded
	return added, c.do(ctx, http.MethodPost, tenantPath(tenant)+"/domains", DomainRequest{Name: name}, &added)
}

// RemoveDomain takes a domain from a tenant, with its certificate.
func (c *Client) RemoveDomain(ctx context.Context, tenant, name string) error {
	return c.do(ctx, http.MethodDelete, tenantPath(tenant)+"/domains/"+url.PathEscape(name), nil, nil)
}

// SetCanonical names the host the edges redirect a tenant's other hosts
// to: one of its domains, or its own name below the base domain; "" for
// none.
func (c *Client) SetCanonical(ctx context.Context, tenant, host string) error {
	return c.do(ctx, http.MethodPut, tenantPath(tenant)+"/canonical", CanonicalRequest{Host: host}, nil)
}

// CheckDomain has the cluster's leader check a domain at once, as after
// its DNS records were changed.
func (c *Client) CheckDomain(ctx context.Context, tenant, name string) error {
	return c.do(ctx, http.MethodPost, tenantPath(tenant)+"/domains/"+url.PathEscape(name)+"/check", nil, nil)
}
