package api

import (
	"context"
	"github.com/PowerPenguini/murder/internal/store"
	"net/http"
)

// MoveRequest asks for a tenant to move to node To.
type MoveRequest struct {
	To string `json:"to"`
	// From, if set, is the node the tenant must run on for the move to
	// begin: where an operation planned it (see store.Fence).
	From string `json:"from,omitempty"`
}

// Move moves a tenant to another node of the cluster and returns once it
// runs there.
// BalanceMoves are the moves that would balance the nodes' tenants.
func (c *Client) BalanceMoves(ctx context.Context) ([]store.Move, error) {
	var moves []store.Move
	err := c.do(ctx, http.MethodGet, "/v1/cluster/balance", nil, &moves)
	return moves, err
}

func (c *Client) Move(ctx context.Context, tenant, to string) (Tenant, error) {
	return c.MoveFrom(ctx, tenant, "", to)
}

// MoveFrom is Move, only if the tenant runs on node from as the move
// begins ("" for any): an operation's plan made from where it ran.
func (c *Client) MoveFrom(ctx context.Context, tenant, from, to string) (Tenant, error) {
	var view Tenant
	err := c.do(ctx, http.MethodPost, tenantPath(tenant)+"/move", MoveRequest{To: to, From: from}, &view)
	return view, err
}
