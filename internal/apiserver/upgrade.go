package apiserver

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net/http"

	"github.com/PowerPenguini/murder/internal/api"
	"github.com/PowerPenguini/murder/internal/upgrade"
)

// Upgrader is this node's part in an upgrade.
type Upgrader interface {
	Info() upgrade.Info
	Install(ctx context.Context, binary io.Reader, sha, freeze string) error
}

// WithUpgrader takes upgrades on this node.
func (s *Server) WithUpgrader(u Upgrader) *Server {
	s.upgrader = u
	return s
}

// maxBinary bounds a binary sent for an upgrade.
const maxBinary = 512 << 20

// Upgrades run code as root on every node: only root asks for them, not
// members of the socket's group.
func (s *Server) upgradeRoutes(mux *http.ServeMux) {
	mux.HandleFunc("POST /v1/upgrade/freeze", rootOnly(s.freeze))
	mux.HandleFunc("POST /v1/upgrade/thaw", rootOnly(s.thaw))
	mux.HandleFunc("GET /v1/upgrade/nodes/{node}", s.nodeInfo)
	mux.HandleFunc("PUT /v1/upgrade/nodes/{node}", rootOnly(s.installOn))
}

func rootOnly(handler http.HandlerFunc) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		if !root(r) {
			writeError(w, statusError{http.StatusForbidden, errors.New("only root (the agent's user) may do this")})
			return
		}
		handler(w, r)
	}
}

func (s *Server) freeze(w http.ResponseWriter, r *http.Request) {
	reason := r.URL.Query().Get("reason")
	if reason == "" {
		writeError(w, badRequest(errors.New("a freeze needs a reason")))
		return
	}
	var err error
	if from := r.URL.Query().Get("replaces"); from != "" {
		err = s.store.TakeOverFreeze(from, reason, actor(r))
	} else {
		err = s.store.Freeze(reason, actor(r))
	}
	if err != nil {
		writeError(w, err)
		return
	}
	w.WriteHeader(http.StatusNoContent)
}

// thaw takes changes again. With ?if= (the panel) or ?reason= (an
// upgrade's coordinator), only if the freeze is still that one: another,
// a rotation's or an upgrade that took this one over, stays and the thaw is
// refused with 409.
func (s *Server) thaw(w http.ResponseWriter, r *http.Request) {
	query := r.URL.Query()
	var err error
	switch {
	case query.Has("if"):
		err = s.store.ThawIf(query.Get("if"), actor(r))
	case query.Get("reason") != "":
		err = s.store.ThawIf(query.Get("reason"), actor(r))
	default:
		err = s.store.Thaw(actor(r))
	}
	if err != nil {
		writeError(w, err)
		return
	}
	w.WriteHeader(http.StatusNoContent)
}

// local reports whether node is this one; "-" names this node too.
func (s *Server) local(node string) bool { return node == "-" || node == s.store.Node() }

func (s *Server) nodeInfo(w http.ResponseWriter, r *http.Request) {
	node := r.PathValue("node")
	switch {
	case s.local(node) && s.upgrader != nil:
		writeJSON(w, http.StatusOK, s.upgrader.Info())
	case s.local(node) || s.cluster == nil:
		writeError(w, statusError{http.StatusConflict, fmt.Errorf("node %s takes no upgrades from here", node)})
	default:
		info, err := s.cluster.NodeInfo(r.Context(), node)
		if err != nil {
			writeError(w, err)
			return
		}
		writeJSON(w, http.StatusOK, info)
	}
}

// installOn installs a binary on this node, or sends it to another node of
// the cluster.
func (s *Server) installOn(w http.ResponseWriter, r *http.Request) {
	node, sha, freeze := r.PathValue("node"), r.Header.Get(api.ShaHeader), r.Header.Get(api.FreezeHeader)
	if sha == "" {
		writeError(w, badRequest(errors.New("the binary's SHA-256 is missing")))
		return
	}
	body := http.MaxBytesReader(w, r.Body, maxBinary)
	var err error
	switch {
	case s.local(node) && s.upgrader != nil:
		if err = s.upgrader.Install(r.Context(), body, sha, freeze); err != nil {
			err = statusError{installStatus(err), err}
		}
	case s.local(node) || s.cluster == nil:
		err = statusError{http.StatusConflict, fmt.Errorf("node %s takes no upgrades from here", node)}
	default:
		err = s.cluster.Install(r.Context(), node, body, sha, freeze)
	}
	if err != nil {
		writeError(w, err)
		return
	}
	if node == "-" {
		node = s.store.Node()
	}
	s.log.Info("installed a new binary", "node", node, "binary", sha, "actor", actor(r))
	if err := s.store.Note("node.upgraded", node+" to "+sha, actor(r)); err != nil {
		s.log.Warn("record the upgrade", "node", node, "error", err)
	}
	w.WriteHeader(http.StatusNoContent)
}

// installStatus tells a node that refused a binary, and runs its old one
// (422), from one that took no binary because the upgrade no longer holds
// the freeze (412), or another install restarts it (409).
func installStatus(err error) int {
	switch {
	case errors.Is(err, upgrade.ErrNotOwner):
		return http.StatusPreconditionFailed
	case errors.Is(err, upgrade.ErrRestarting):
		return http.StatusConflict
	}
	return http.StatusUnprocessableEntity
}
