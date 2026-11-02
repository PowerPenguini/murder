package apiserver

import (
	"net/http"

	"github.com/PowerPenguini/murder/internal/api"
	"github.com/PowerPenguini/murder/internal/store"
)

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

// devices lists the devices as the cluster has them, not as a node cut
// off from it last heard: a device removed meanwhile must not reach its
// panel. A node that cannot catch up answers an error, and its panel
// then lets no device in.
func (s *Server) devices(w http.ResponseWriter, _ *http.Request) {
	if s.cluster != nil && s.store.Cluster() != "" {
		if err := s.cluster.CatchUp(); err != nil {
			writeError(w, err)
			return
		}
	}
	writeJSON(w, http.StatusOK, s.store.Devices())
}

func (s *Server) addDevice(w http.ResponseWriter, r *http.Request) {
	var request api.DeviceRequest
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
