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

func (s *Server) panelRoutes(mux *http.ServeMux) {
	mux.HandleFunc("PUT /v1/panel/tunnel", rootOnly(s.setTunnel))
	mux.HandleFunc("GET /v1/panel/devices", rootOnly(s.devices))
	mux.HandleFunc("POST /v1/panel/devices", rootOnly(s.addDevice))
	mux.HandleFunc("DELETE /v1/panel/devices/{name}", rootOnly(s.removeDevice))
}

// setTunnel records this node's end of the panel's tunnel.
func (s *Server) setTunnel(w http.ResponseWriter, r *http.Request) {
	var tunnel store.Tunnel
	if !readJSON(w, r, &tunnel) {
		return
	}
	if err := s.store.SetTunnel(s.store.Node(), tunnel); err != nil {
		writeError(w, err)
		return
	}
	w.WriteHeader(http.StatusNoContent)
}

func (s *Server) devices(w http.ResponseWriter, _ *http.Request) {
	writeJSON(w, http.StatusOK, s.store.Devices())
}

func (s *Server) addDevice(w http.ResponseWriter, r *http.Request) {
	var request DeviceRequest
	if !readJSON(w, r, &request) {
		return
	}
	device, err := s.store.AddDevice(request.Name, request.Key, request.Network, actor(r))
	if err != nil {
		writeError(w, err)
		return
	}
	writeJSON(w, http.StatusCreated, device)
}

func (s *Server) removeDevice(w http.ResponseWriter, r *http.Request) {
	if err := s.store.RemoveDevice(r.PathValue("name"), actor(r)); err != nil {
		writeError(w, err)
		return
	}
	w.WriteHeader(http.StatusNoContent)
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
