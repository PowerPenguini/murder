package api

import (
	"context"
	"net/http"
	"net/netip"
	"net/url"

	"github.com/PowerPenguini/murder/internal/store"
)

// DeviceRequest adds a device that may reach the panel.
type DeviceRequest struct {
	Name string `json:"name"`
	// Key is its WireGuard public key; Network the tunnel's network, from
	// which it gets an address.
	Key     string       `json:"key"`
	Network netip.Prefix `json:"network"`
}

// SetTunnel records this node's end of the panel's tunnel.
func (c *Client) SetTunnel(ctx context.Context, tunnel store.Tunnel) error {
	return c.do(ctx, http.MethodPut, "/v1/panel/tunnel", tunnel, nil)
}

// Devices lists the devices that may reach the panel.
func (c *Client) Devices(ctx context.Context) ([]store.Device, error) {
	var devices []store.Device
	err := c.do(ctx, http.MethodGet, "/v1/panel/devices", nil, &devices)
	return devices, err
}

// AddDevice lets a device reach the panel.
func (c *Client) AddDevice(ctx context.Context, request DeviceRequest) (store.Device, error) {
	var device store.Device
	err := c.do(ctx, http.MethodPost, "/v1/panel/devices", request, &device)
	return device, err
}

// RemoveDevice stops a device reaching the panel.
func (c *Client) RemoveDevice(ctx context.Context, name string) error {
	return c.do(ctx, http.MethodDelete, "/v1/panel/devices/"+url.PathEscape(name), nil, nil)
}
