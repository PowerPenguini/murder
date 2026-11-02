package apiserver

import (
	"net/http"

	"github.com/PowerPenguini/murder/internal/api"
)

func (s *Server) panelAuthRoutes(mux *http.ServeMux) {
	mux.HandleFunc("GET /v1/panel/users", rootOnly(func(w http.ResponseWriter, r *http.Request) {
		if handle := r.URL.Query().Get("handle"); handle != "" {
			s.panelAnswer(w, r, func() (any, error) { return s.store.PanelUserByHandle(handle) })
			return
		}
		writeJSON(w, http.StatusOK, s.store.PanelUsers())
	}))
	mux.HandleFunc("GET /v1/panel/users/{name}", rootOnly(func(w http.ResponseWriter, r *http.Request) {
		s.panelAnswer(w, r, func() (any, error) { return s.store.PanelUser(r.PathValue("name")) })
	}))
	mux.HandleFunc("POST /v1/panel/users/{name}/invite", rootOnly(func(w http.ResponseWriter, r *http.Request) {
		var request api.InviteRequest
		if readJSON(w, r, &request) {
			s.panelAnswer(w, r, func() (any, error) {
				return s.store.InvitePanelUser(r.PathValue("name"), request.Hash, request.Expires, actor(r))
			})
		}
	}))
	mux.HandleFunc("PUT /v1/panel/users/{name}/recovery", rootOnly(func(w http.ResponseWriter, r *http.Request) {
		var hashes []string
		if readJSON(w, r, &hashes) {
			s.panelDone(w, s.store.SetRecoveryCodes(r.PathValue("name"), hashes, actor(r)))
		}
	}))
	mux.HandleFunc("DELETE /v1/panel/users/{name}", rootOnly(func(w http.ResponseWriter, r *http.Request) {
		s.panelDone(w, s.store.RemovePanelUser(r.PathValue("name"), actor(r)))
	}))
	mux.HandleFunc("GET /v1/panel/enrollments/{hash}", rootOnly(func(w http.ResponseWriter, r *http.Request) {
		s.panelAnswer(w, r, func() (any, error) { return s.store.Enrollment(r.PathValue("hash")) })
	}))
	mux.HandleFunc("POST /v1/panel/passkeys", rootOnly(func(w http.ResponseWriter, r *http.Request) {
		var request api.PasskeyRequest
		if readJSON(w, r, &request) {
			s.panelDone(w, s.store.AddPasskey(request.Hash, request.Passkey))
		}
	}))
	mux.HandleFunc("PUT /v1/panel/passkeys/{id}", rootOnly(func(w http.ResponseWriter, r *http.Request) {
		var data string
		if readJSON(w, r, &data) {
			s.panelDone(w, s.store.UsePasskey(r.PathValue("id"), data))
		}
	}))
	mux.HandleFunc("DELETE /v1/panel/passkeys/{id}", rootOnly(func(w http.ResponseWriter, r *http.Request) {
		s.panelDone(w, s.store.RemovePasskey(r.PathValue("id"), actor(r)))
	}))
	mux.HandleFunc("POST /v1/panel/sessions", rootOnly(func(w http.ResponseWriter, r *http.Request) {
		var request api.SessionRequest
		if readJSON(w, r, &request) {
			s.panelDone(w, s.store.StartPanelSession(request))
		}
	}))
	mux.HandleFunc("GET /v1/panel/sessions/{hash}", rootOnly(func(w http.ResponseWriter, r *http.Request) {
		// Before a change, the session is checked as the cluster has it:
		// one ended elsewhere must not act on a node behind.
		if r.URL.Query().Get("fresh") == "1" && s.cluster != nil && s.store.Cluster() != "" {
			if err := s.cluster.CatchUp(); err != nil {
				writeError(w, err)
				return
			}
		}
		s.panelAnswer(w, r, func() (any, error) { return s.store.PanelSession(r.PathValue("hash")) })
	}))
	mux.HandleFunc("DELETE /v1/panel/sessions/{hash}", rootOnly(func(w http.ResponseWriter, r *http.Request) {
		s.panelDone(w, s.store.EndPanelSession(r.PathValue("hash")))
	}))
	mux.HandleFunc("GET /v1/panel/secrets/{name}", rootOnly(func(w http.ResponseWriter, r *http.Request) {
		s.panelAnswer(w, r, func() (any, error) { return s.store.PanelSecret(r.PathValue("name")) })
	}))
}

func (s *Server) panelAnswer(w http.ResponseWriter, _ *http.Request, answer func() (any, error)) {
	value, err := answer()
	if err != nil {
		writeError(w, err)
		return
	}
	writeJSON(w, http.StatusOK, value)
}

func (s *Server) panelDone(w http.ResponseWriter, err error) {
	if err != nil {
		writeError(w, err)
		return
	}
	w.WriteHeader(http.StatusNoContent)
}
