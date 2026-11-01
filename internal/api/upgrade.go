package api

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"

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

// ShaHeader carries the SHA-256 an uploaded binary must have.
const ShaHeader = "X-Murder-Sha256"

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

func (s *Server) thaw(w http.ResponseWriter, r *http.Request) {
	if err := s.store.Thaw(actor(r)); err != nil {
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
	node, sha := r.PathValue("node"), r.Header.Get(ShaHeader)
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

// Freeze refuses changes, on every node, until Thaw.
func (c *Client) Freeze(ctx context.Context, reason string) error {
	return c.do(ctx, http.MethodPost, "/v1/upgrade/freeze?reason="+url.QueryEscape(reason), nil, nil)
}

func (c *Client) Thaw(ctx context.Context) error {
	return c.do(ctx, http.MethodPost, "/v1/upgrade/thaw", nil, nil)
}

// NodeInfo reports on a node for an upgrade.
func (c *Client) NodeInfo(ctx context.Context, node string) (upgrade.Info, error) {
	var info upgrade.Info
	err := c.do(ctx, http.MethodGet, "/v1/upgrade/nodes/"+url.PathEscape(node), nil, &info)
	return info, err
}

// Install sends a binary to a node, which installs it and restarts into it.
func (c *Client) Install(ctx context.Context, node string, binary io.Reader, sha string) error {
	request, err := http.NewRequestWithContext(ctx, http.MethodPut, c.base+"/v1/upgrade/nodes/"+url.PathEscape(node), binary)
	if err != nil {
		return err
	}
	request.Header.Set(ShaHeader, sha)
	if c.Actor != "" {
		request.Header.Set(ActorHeader, c.Actor)
	}
	response, err := c.http.Do(request)
	if err != nil {
		return fmt.Errorf("reach the murder agent: %w", err)
	}
	defer response.Body.Close()
	return responseError(response)
}
