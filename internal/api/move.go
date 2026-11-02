package api

import (
	"context"
	"net/http"
)

// MoveRequest asks for a tenant to move to node To.
type MoveRequest struct {
	To string `json:"to"`
}

// Move moves a tenant to another node of the cluster and returns once it
// runs there.
func (c *Client) Move(ctx context.Context, tenant, to string) (Tenant, error) {
	var view Tenant
	err := c.do(ctx, http.MethodPost, tenantPath(tenant)+"/move", MoveRequest{To: to}, &view)
	return view, err
}
