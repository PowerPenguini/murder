package api

import (
	"context"
	"fmt"
	"io"
	"net/http"
	"net/url"

	"github.com/PowerPenguini/murder/internal/upgrade"
)

// ShaHeader carries the SHA-256 an uploaded binary must have; FreezeHeader
// the freeze of the upgrade that sends it, which the node checks it still
// holds.
const (
	ShaHeader    = "X-Murder-Sha256"
	FreezeHeader = "X-Murder-Freeze"
)

// Freeze refuses changes, on every node, until Thaw.
func (c *Client) Freeze(ctx context.Context, reason string) error {
	return c.do(ctx, http.MethodPost, "/v1/upgrade/freeze?reason="+url.QueryEscape(reason), nil, nil)
}

// TakeOverFreeze puts the upgrade freeze reason in place of the upgrade
// freeze from, and only of that one.
func (c *Client) TakeOverFreeze(ctx context.Context, from, reason string) error {
	return c.do(ctx, http.MethodPost, "/v1/upgrade/freeze?reason="+url.QueryEscape(reason)+"&replaces="+url.QueryEscape(from), nil, nil)
}

// Thaw takes changes again, whoever froze them.
func (c *Client) Thaw(ctx context.Context) error {
	return c.do(ctx, http.MethodPost, "/v1/upgrade/thaw", nil, nil)
}

// ThawIf takes changes again only if they are still frozen for reason.
func (c *Client) ThawIf(ctx context.Context, reason string) error {
	return c.do(ctx, http.MethodPost, "/v1/upgrade/thaw?if="+url.QueryEscape(reason), nil, nil)
}

// ThawOwn takes changes again only if they are still frozen for reason;
// it fails, changing nothing, if they are frozen for another.
func (c *Client) ThawOwn(ctx context.Context, reason string) error {
	return c.do(ctx, http.MethodPost, "/v1/upgrade/thaw?reason="+url.QueryEscape(reason), nil, nil)
}

// NodeInfo reports on a node for an upgrade.
func (c *Client) NodeInfo(ctx context.Context, node string) (upgrade.Info, error) {
	var info upgrade.Info
	err := c.do(ctx, http.MethodGet, "/v1/upgrade/nodes/"+url.PathEscape(node), nil, &info)
	return info, err
}

// Install sends a binary to a node, which installs it and restarts into it,
// if changes are still frozen for freeze.
func (c *Client) Install(ctx context.Context, node string, binary io.Reader, sha, freeze string) error {
	request, err := http.NewRequestWithContext(ctx, http.MethodPut, c.base+"/v1/upgrade/nodes/"+url.PathEscape(node), binary)
	if err != nil {
		return err
	}
	request.Header.Set(ShaHeader, sha)
	if freeze != "" {
		request.Header.Set(FreezeHeader, freeze)
	}
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
