package api

import (
	"context"
	"github.com/PowerPenguini/murder/internal/store"
	"net/http"
)

// MoveRequest asks for a tenant to move to node To.
type MoveRequest struct {
	To string `json:"to"`
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
	var view Tenant
	err := c.do(ctx, http.MethodPost, tenantPath(tenant)+"/move", MoveRequest{To: to}, &view)
	return view, err
}
