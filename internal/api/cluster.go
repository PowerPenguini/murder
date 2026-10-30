package api

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"net/http"

	"github.com/PowerPenguini/murder/internal/cluster"
	"github.com/PowerPenguini/murder/internal/store"
)

// Cluster is this node's part of a cluster.
type Cluster interface {
	Status() (cluster.Status, error)
	Bootstrap(ctx context.Context, actor string) error
	Join(ctx context.Context, peer, actor string) error
	Remove(ctx context.Context, node, actor string) error
}

// JoinRequest asks the node to join the cluster of the node at Peer.
type JoinRequest struct {
	Peer string `json:"peer"`
}

// WithCluster serves the cluster's commands from this node.
func (s *Server) WithCluster(c Cluster) *Server {
	s.cluster = c
	return s
}

// NewWitnessServer serves a witness: it runs no tenants, so it serves the
// cluster's commands, the audit log and backups only.
func NewWitnessServer(st store.Store, c Cluster, log *slog.Logger) http.Handler {
	s := &Server{store: st, cluster: c, log: log}
	mux := http.NewServeMux()
	s.clusterRoutes(mux)
	mux.HandleFunc("GET /v1/events", s.events)
	mux.HandleFunc("GET /v1/backup", s.backup)
	mux.HandleFunc("/", func(w http.ResponseWriter, _ *http.Request) {
		writeError(w, statusError{http.StatusConflict, fmt.Errorf("node %s is a witness and runs no tenants: manage apps and tenants on a worker", st.Node())})
	})
	return mux
}

func (s *Server) clusterRoutes(mux *http.ServeMux) {
	mux.HandleFunc("GET /v1/cluster", s.clusterStatus)
	mux.HandleFunc("POST /v1/cluster/bootstrap", s.bootstrap)
	mux.HandleFunc("POST /v1/cluster/join", s.join)
	mux.HandleFunc("DELETE /v1/cluster/nodes/{name}", s.removeNode)
}

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
	var request JoinRequest
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

// ClusterStatus returns the node's view of its cluster.
func (c *Client) ClusterStatus(ctx context.Context) (cluster.Status, error) {
	var status cluster.Status
	err := c.do(ctx, http.MethodGet, "/v1/cluster", nil, &status)
	return status, err
}

// Bootstrap starts a cluster of this node alone.
func (c *Client) Bootstrap(ctx context.Context) (cluster.Status, error) {
	var status cluster.Status
	err := c.do(ctx, http.MethodPost, "/v1/cluster/bootstrap", nil, &status)
	return status, err
}

// Join adds the node to the cluster of the node at peer.
func (c *Client) Join(ctx context.Context, peer string) (cluster.Status, error) {
	var status cluster.Status
	err := c.do(ctx, http.MethodPost, "/v1/cluster/join", JoinRequest{Peer: peer}, &status)
	return status, err
}

// RemoveNode takes a node out of the cluster.
func (c *Client) RemoveNode(ctx context.Context, node string) error {
	return c.do(ctx, http.MethodDelete, "/v1/cluster/nodes/"+node, nil, nil)
}
