package apiserver

import (
	"context"
	"errors"
	"io"
	"net/http"
	"time"

	"github.com/PowerPenguini/murder/internal/api"
	"github.com/PowerPenguini/murder/internal/cluster"
	"github.com/PowerPenguini/murder/internal/failover"
	"github.com/PowerPenguini/murder/internal/store"
	"github.com/PowerPenguini/murder/internal/upgrade"
)

// Cluster is this node's part of a cluster.
type Cluster interface {
	Status() (cluster.Status, error)
	NodeInfo(ctx context.Context, node string) (upgrade.Info, error)
	Install(ctx context.Context, node string, binary io.Reader, sha, freeze string) error
	Bootstrap(ctx context.Context, actor string) error
	Join(ctx context.Context, peer, actor string) error
	Remove(ctx context.Context, node, actor string) error
	// StatusOn decodes the status of a tenant node runs into out.
	StatusOn(ctx context.Context, node, tenant string, out any) error
	// Ping reports whether node answers.
	Ping(ctx context.Context, node string) error
	// CatchUp applies here what the cluster has applied.
	CatchUp() error
	// CreateOn has node create tenant to run there.
	CreateOn(ctx context.Context, node string, tenant store.NewTenant, actor string) (store.Record, error)
}

// Invites makes invitations for hosts to join the cluster as nodes.
type Invites interface {
	Create(valid time.Duration, actor string) (cluster.Invite, time.Time, error)
}

// WithInvites makes invitations to this node.
func (s *Server) WithInvites(invites Invites) *Server {
	s.invites = invites
	return s
}

// WithCluster serves the cluster's commands from this node.
func (s *Server) WithCluster(c Cluster) *Server {
	s.cluster = c
	return s
}

func (s *Server) clusterRoutes(mux *http.ServeMux) {
	mux.HandleFunc("GET /v1/cluster", s.clusterStatus)
	mux.HandleFunc("POST /v1/cluster/bootstrap", s.bootstrap)
	mux.HandleFunc("POST /v1/cluster/join", s.join)
	mux.HandleFunc("DELETE /v1/cluster/nodes/{name}", s.removeNode)
	mux.HandleFunc("POST /v1/cluster/nodes/{name}/failover", rootOnly(s.failOver))
	mux.HandleFunc("POST /v1/cluster/invites", rootOnly(s.invite))
	mux.HandleFunc("GET /v1/cluster/balance", s.balance)
}

// balance plans the moves that would leave no node running two tenants
// more than another; the client makes them, one after the other.
func (s *Server) balance(w http.ResponseWriter, _ *http.Request) {
	moves := store.BalanceMoves(s.store.Nodes(), s.store.List())
	if moves == nil {
		moves = []store.Move{}
	}
	writeJSON(w, http.StatusOK, moves)
}

// invite makes an invitation: it hands the master key to whoever brings
// it, so only root makes one.
func (s *Server) invite(w http.ResponseWriter, r *http.Request) {
	if s.invites == nil || s.cluster == nil {
		writeError(w, errNoCluster)
		return
	}
	var request api.NodeInviteRequest
	if !readJSON(w, r, &request) {
		return
	}
	valid := time.Duration(request.ValidSeconds) * time.Second
	if valid == 0 {
		valid = time.Hour
	}
	invite, expires, err := s.invites.Create(valid, actor(r))
	if err != nil {
		writeError(w, statusError{http.StatusConflict, err})
		return
	}
	writeJSON(w, http.StatusCreated, api.NodeInvitation{Invitation: invite.String(), Expires: expires})
}

var errNoKeys = statusError{http.StatusConflict, errors.New("this agent does not rotate master keys")}

var errNoCluster = statusError{http.StatusConflict, errors.New(`this node is not set up for a cluster: add "cluster:" to its config and restart the agent`)}

func (s *Server) clusterStatus(w http.ResponseWriter, _ *http.Request) {
	if s.cluster == nil {
		writeError(w, errNoCluster)
		return
	}
	status, err := s.cluster.Status()
	if err != nil {
		writeError(w, err)
		return
	}
	writeJSON(w, http.StatusOK, status)
}

func (s *Server) bootstrap(w http.ResponseWriter, r *http.Request) {
	if s.cluster == nil {
		writeError(w, errNoCluster)
		return
	}
	if err := s.cluster.Bootstrap(r.Context(), actor(r)); err != nil {
		writeError(w, err)
		return
	}
	s.clusterStatus(w, r)
}

func (s *Server) join(w http.ResponseWriter, r *http.Request) {
	if s.cluster == nil {
		writeError(w, errNoCluster)
		return
	}
	var request api.JoinRequest
	if !readJSON(w, r, &request) {
		return
	}
	if request.Peer == "" {
		writeError(w, badRequest(errors.New("peer is the address of a node of the cluster")))
		return
	}
	if err := s.cluster.Join(r.Context(), request.Peer, actor(r)); err != nil {
		writeError(w, err)
		return
	}
	s.clusterStatus(w, r)
}

func (s *Server) removeNode(w http.ResponseWriter, r *http.Request) {
	if s.cluster == nil {
		writeError(w, errNoCluster)
		return
	}
	if err := s.cluster.Remove(r.Context(), r.PathValue("name"), actor(r)); err != nil {
		writeError(w, err)
		return
	}
	w.WriteHeader(http.StatusNoContent)
}

// failOver runs a node's tenants from their replicas, once an operator has
// made sure the node is powered off; refused while it answers.
func (s *Server) failOver(w http.ResponseWriter, r *http.Request) {
	if s.cluster == nil {
		writeError(w, errNoCluster)
		return
	}
	outcome, err := failover.ManualFailOver(r.Context(), s.store, s.cluster.Ping, r.PathValue("name"), actor(r))
	if err != nil {
		writeError(w, err)
		return
	}
	writeJSON(w, http.StatusOK, outcome)
}
