package api

import (
	"context"
	"errors"
	"net/http"
	"time"
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
	// Moved by another node, the move reaches this one's state a moment
	// later.
	record, err := s.store.Get(name)
	for deadline := time.Now().Add(15 * time.Second); err == nil && record.Node != request.To && time.Now().Before(deadline); {
		time.Sleep(100 * time.Millisecond)
		record, err = s.store.Get(name)
	}
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

// Replicas reports on the copies of this node's tenants.
type Replicas interface {
	// Synced is when the tenant's copy last matched its data; zero if not
	// known.
	Synced(tenant string) time.Time
}

// WithReplicas reports on the copies of this node's tenants.
func (s *Server) WithReplicas(r Replicas) *Server {
	s.replicas = r
	return s
}
