package api

import (
	"context"
	"fmt"
	"io"
	"net/http"
	"net/url"

	"github.com/PowerPenguini/murder/internal/upgrade"
)

// ShaHeader carries the SHA-256 an uploaded binary must have.
const ShaHeader = "X-Murder-Sha256"

// Freeze refuses changes, on every node, until Thaw.
func (c *Client) Freeze(ctx context.Context, reason string) error {
	return c.do(ctx, http.MethodPost, "/v1/upgrade/freeze?reason="+url.QueryEscape(reason), nil, nil)
}

func (c *Client) Thaw(ctx context.Context) error {
	return c.do(ctx, http.MethodPost, "/v1/upgrade/thaw", nil, nil)
}

// NodeInfo reports on a node for an upgrade.
func (c *Client) NodeInfo(ctx context.Context, node string) (upgrade.Info, error) {
	var info upgrade.Info
	err := c.do(ctx, http.MethodGet, "/v1/upgrade/nodes/"+url.PathEscape(node), nil, &info)
	return info, err
}

// Install sends a binary to a node, which installs it and restarts into it.
func (c *Client) Install(ctx context.Context, node string, binary io.Reader, sha string) error {
	request, err := http.NewRequestWithContext(ctx, http.MethodPut, c.base+"/v1/upgrade/nodes/"+url.PathEscape(node), binary)
	if err != nil {
		return err
	}
	request.Header.Set(ShaHeader, sha)
	if c.Actor != "" {
		request.Header.Set(ActorHeader, c.Actor)
	}
	response, err := c.http.Do(request)
	if err != nil {
		return fmt.Errorf("reach the murder agent: %w", err)
	}
	defer response.Body.Close()
	return responseError(response)
}
