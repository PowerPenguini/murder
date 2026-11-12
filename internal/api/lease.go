package api

import (
	"context"
	"net/http"
	"net/url"

	"github.com/PowerPenguini/murder/internal/store"
)

// LeaseHeader carries the operation lease a request acts under: the agent
// refuses a move, a node's removal or a reboot while another operation
// holds the cluster, and takes them from the one that holds it.
const LeaseHeader = "X-Murder-Lease"

// leaseKey is the context key of the lease a request acts under.
type leaseKey struct{}

// WithLease has every request made with ctx act under the lease id.
func WithLease(ctx context.Context, id string) context.Context {
	return context.WithValue(ctx, leaseKey{}, id)
}

// LeaseOf is the lease ctx acts under; empty for none.
func LeaseOf(ctx context.Context) string {
	id, _ := ctx.Value(leaseKey{}).(string)
	return id
}

// LeaseRequest asks for the cluster's operation lease, for an operation of
// Kind on Detail.
type LeaseRequest struct {
	Kind   string `json:"kind"`
	Detail string `json:"detail,omitempty"`
}

// Lease is the cluster's operation lease; Held says whether it holds now,
// as the agent's clock has it. ID is empty when none was taken.
type Lease struct {
	store.Lease
	Held bool `json:"held"`
}

// Lease returns the cluster's operation lease.
func (c *Client) Lease(ctx context.Context) (Lease, error) {
	var lease Lease
	return lease, c.do(ctx, http.MethodGet, "/v1/cluster/lease", nil, &lease)
}

// TakeLease has an operation of kind hold the cluster; it is refused with
// a 409 that names the holder while another one holds it, or while changes
// are frozen.
func (c *Client) TakeLease(ctx context.Context, kind, detail string) (store.Lease, error) {
	var lease store.Lease
	return lease, c.do(ctx, http.MethodPost, "/v1/cluster/lease", LeaseRequest{Kind: kind, Detail: detail}, &lease)
}

// RenewLease keeps the lease id for another store.LeaseTTL; a 410 says it
// is no longer the operation's.
func (c *Client) RenewLease(ctx context.Context, id string) (store.Lease, error) {
	var lease store.Lease
	return lease, c.do(ctx, http.MethodPost, "/v1/cluster/lease/"+url.PathEscape(id)+"/renew", nil, &lease)
}

// ReleaseLease lets go of the lease id; one that is no longer id stays.
func (c *Client) ReleaseLease(ctx context.Context, id string) error {
	return c.do(ctx, http.MethodDelete, "/v1/cluster/lease/"+url.PathEscape(id), nil, nil)
}

// BreakLease takes the lease id away from its holder, and only that one.
func (c *Client) BreakLease(ctx context.Context, id string) (store.Lease, error) {
	var lease store.Lease
	return lease, c.do(ctx, http.MethodPost, "/v1/cluster/lease/"+url.PathEscape(id)+"/break", nil, &lease)
}
