package apiserver

import (
	"context"
	"crypto/ed25519"
	"encoding/base64"
	"errors"
	"io"
	"net/http"
	"time"

	"github.com/PowerPenguini/murder/internal/api"
	"github.com/PowerPenguini/murder/internal/cluster"
	"github.com/PowerPenguini/murder/internal/failover"
	"github.com/PowerPenguini/murder/internal/reboot"
	"github.com/PowerPenguini/murder/internal/store"
	"github.com/PowerPenguini/murder/internal/upgrade"
)

// Cluster is this node's part of a cluster.
type Cluster interface {
	Status() (cluster.Status, error)
	NodeInfo(ctx context.Context, node string) (upgrade.Info, error)
	Install(ctx context.Context, node string, binary io.Reader, sha, freeze string) error
	// RebootOn asks node to reboot its host.
	RebootOn(ctx context.Context, node string, request reboot.Request) error
	Bootstrap(ctx context.Context, actor string, restored bool) error
	// ReplaceKey gives a node a new identity key, in place of one it lost.
	ReplaceKey(ctx context.Context, name string, key ed25519.PublicKey, actor string) error
	// Join joins the cluster of the node at peer, once an invitation
	// admitted this node; members are the identity keys it named.
	Join(ctx context.Context, peer string, members map[string]string, actor string) error
	// JoinInvited joins the cluster an invitation names.
	JoinInvited(ctx context.Context, invite cluster.Invite, actor string) error
	Remove(ctx context.Context, node, actor string) error
	// StatusOn decodes the status of a tenant node runs into out.
	StatusOn(ctx context.Context, node, tenant string, out any) error
	// Ping reports whether node answers.
	Ping(ctx context.Context, node string) error
	// CatchUp applies here what the cluster has applied.
	CatchUp() error
	// EveryNodeRuns returns nil once every other node runs a release of
	// schema version schema or later.
	EveryNodeRuns(ctx context.Context, schema int) error
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
	mux.HandleFunc("PUT /v1/cluster/nodes/{name}/key", rootOnly(s.replaceNodeKey))
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
	if lease := s.store.Lease(); lease.Held(time.Now()) {
		status.Lease = &lease
	}
	writeJSON(w, http.StatusOK, status)
}

func (s *Server) bootstrap(w http.ResponseWriter, r *http.Request) {
	if s.cluster == nil {
		writeError(w, errNoCluster)
		return
	}
	var request api.BootstrapRequest
	if r.ContentLength != 0 && !readJSON(w, r, &request) {
		return
	}
	if err := s.cluster.Bootstrap(r.Context(), actor(r), request.Restored); err != nil {
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
	var err error
	switch {
	case request.Invitation != "":
		var invite cluster.Invite
		if invite, err = cluster.ParseInvite(request.Invitation); err != nil {
			writeError(w, badRequest(err))
			return
		}
		err = s.cluster.JoinInvited(r.Context(), invite, actor(r))
	case request.Peer != "" && len(request.Members) > 0:
		err = s.cluster.Join(r.Context(), request.Peer, request.Members, actor(r))
	default:
		writeError(w, badRequest(errors.New("a node joins with an invitation: run `murder cluster invite` on a node of the cluster, then `murder cluster join <invitation>` here")))
		return
	}
	if err != nil {
		writeError(w, err)
		return
	}
	s.clusterStatus(w, r)
}

// replaceNodeKey gives a node that lost its identity key the new one it
// shows: only root does, as it decides which node the cluster takes.
func (s *Server) replaceNodeKey(w http.ResponseWriter, r *http.Request) {
	if s.cluster == nil {
		writeError(w, errNoCluster)
		return
	}
	var request api.NodeKeyRequest
	if !readJSON(w, r, &request) {
		return
	}
	key, err := base64.StdEncoding.DecodeString(request.Key)
	if err != nil || len(key) != ed25519.PublicKeySize {
		writeError(w, badRequest(errors.New("an identity key is an ed25519 public key in base64, as `murder node key` prints it on the node")))
		return
	}
	if err := s.cluster.ReplaceKey(r.Context(), r.PathValue("name"), key, actor(r)); err != nil {
		writeError(w, err)
		return
	}
	w.WriteHeader(http.StatusNoContent)
}

func (s *Server) removeNode(w http.ResponseWriter, r *http.Request) {
	if s.cluster == nil {
		writeError(w, errNoCluster)
		return
	}
	if err := s.leased(r); err != nil {
		writeError(w, err)
		return
	}
	ctx := store.WithFence(r.Context(), store.Fence{Lease: r.Header.Get(api.LeaseHeader)})
	if err := s.cluster.Remove(ctx, r.PathValue("name"), actor(r)); err != nil {
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
