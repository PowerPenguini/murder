package api

import (
	"context"
	"net/http"
	"net/url"

	"github.com/PowerPenguini/murder/internal/edge"
)

// Routes returns the node's routes; with after set, it waits for routes of
// another version, up to a while.
func (c *Client) Routes(ctx context.Context, after string) (edge.Table, error) {
	return c.RoutesAs(ctx, "", after)
}

// RoutesAs is Routes for the edge process named instance, which serves from
// the table after names: the agent knows then what each edge serves.
func (c *Client) RoutesAs(ctx context.Context, instance, after string) (edge.Table, error) {
	var table edge.Table
	err := c.doWith(ctx, http.MethodGet, "/v1/routes?after="+url.QueryEscape(after), nil, &table, map[string]string{"X-Murder-Edge": instance})
	return table, err
}
