package apiserver

import (
	"context"
	"net/http"

	"github.com/PowerPenguini/murder/internal/api"
	"github.com/PowerPenguini/murder/internal/rotation"
)

// Keys rotates the master key of the nodes.
type Keys interface {
	Rings(ctx context.Context) []api.NodeKeys
	Rotate(ctx context.Context, actor string) (rotation.Result, error)
}

// WithKeys serves master key rotations from this node.
func (s *Server) WithKeys(keys Keys) *Server {
	s.keys = keys
	return s
}

// keyRoutes are root's alone: a rotation hands every node a new master key.
func (s *Server) keyRoutes(mux *http.ServeMux) {
	mux.HandleFunc("GET /v1/keys", rootOnly(func(w http.ResponseWriter, r *http.Request) {
		if s.keys == nil {
			writeError(w, errNoKeys)
			return
		}
		writeJSON(w, http.StatusOK, s.keys.Rings(r.Context()))
	}))
	mux.HandleFunc("POST /v1/keys/rotate", rootOnly(func(w http.ResponseWriter, r *http.Request) {
		if s.keys == nil {
			writeError(w, errNoKeys)
			return
		}
		result, err := s.keys.Rotate(r.Context(), actor(r))
		if err != nil {
			writeError(w, statusError{http.StatusConflict, err})
			return
		}
		writeJSON(w, http.StatusOK, result)
	}))
}
