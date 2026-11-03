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
	Install(ctx context.Context, binary io.Reader, sha string) error
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
	if err := s.store.Freeze(reason, actor(r)); err != nil {
		writeError(w, err)
		return
	}
	w.WriteHeader(http.StatusNoContent)
}

// thaw takes changes again; with ?if=, only if the freeze is still that
// one.
func (s *Server) thaw(w http.ResponseWriter, r *http.Request) {
	thaw := func() error { return s.store.Thaw(actor(r)) }
	if r.URL.Query().Has("if") {
		thaw = func() error { return s.store.ThawIf(r.URL.Query().Get("if"), actor(r)) }
	}
	if err := thaw(); err != nil {
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
	node, sha := r.PathValue("node"), r.Header.Get(api.ShaHeader)
	if sha == "" {
		writeError(w, badRequest(errors.New("the binary's SHA-256 is missing")))
		return
	}
	body := http.MaxBytesReader(w, r.Body, maxBinary)
	var err error
	switch {
	case s.local(node) && s.upgrader != nil:
		if err = s.upgrader.Install(r.Context(), body, sha); err != nil {
			err = statusError{http.StatusUnprocessableEntity, err}
		}
	case s.local(node) || s.cluster == nil:
		err = statusError{http.StatusConflict, fmt.Errorf("node %s takes no upgrades from here", node)}
	default:
		err = s.cluster.Install(r.Context(), node, body, sha)
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
