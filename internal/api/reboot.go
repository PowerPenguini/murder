package api

import (
	"context"
	"net/http"
	"net/url"

	"github.com/PowerPenguini/murder/internal/reboot"
)

// Reboot asks node ("-" is this one) to reboot its host, or with
// request.Check only whether it could: a node that would not come back as
// it runs refuses (422).
func (c *Client) Reboot(ctx context.Context, node string, request reboot.Request) error {
	return c.do(ctx, http.MethodPost, "/v1/nodes/"+url.PathEscape(node)+"/reboot", request, nil)
}
