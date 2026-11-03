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
	var table edge.Table
	err := c.do(ctx, http.MethodGet, "/v1/routes?after="+url.QueryEscape(after), nil, &table)
	return table, err
}
