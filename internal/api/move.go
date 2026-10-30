package api

import (
	"context"
	"errors"
	"net/http"
)

// Moves moves tenants between the nodes of a cluster.
type Moves interface {
	Move(ctx context.Context, tenant, to, actor string) error
}

// WithMoves moves tenants on request.
func (s *Server) WithMoves(m Moves) *Server {
	s.moves = m
	return s
}

// MoveRequest asks for a tenant to move to node To.
type MoveRequest struct {
	To string `json:"to"`
}

// move moves a tenant, from whichever node runs it, and answers once it
// runs on its new node.
func (s *Server) move(w http.ResponseWriter, r *http.Request) {
	var request MoveRequest
	if !readJSON(w, r, &request) {
		return
	}
	if request.To == "" {
		writeError(w, badRequest(errors.New("a move needs the node to move to")))
		return
	}
	if s.moves == nil {
		writeError(w, statusError{http.StatusConflict, errors.New("this node moves no tenants")})
		return
	}
	name := r.PathValue("name")
	if err := s.moves.Move(r.Context(), name, request.To, actor(r)); err != nil {
		writeError(w, err)
		return
	}
	record, err := s.store.Get(name)
	if err != nil {
		writeError(w, err)
		return
	}
	writeJSON(w, http.StatusOK, s.view(record))
}

// Move moves a tenant to another node of the cluster and returns once it
// runs there.
func (c *Client) Move(ctx context.Context, tenant, to string) (Tenant, error) {
	var view Tenant
	err := c.do(ctx, http.MethodPost, tenantPath(tenant)+"/move", MoveRequest{To: to}, &view)
	return view, err
}
