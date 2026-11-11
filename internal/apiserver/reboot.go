package apiserver

import (
	"context"
	"errors"
	"fmt"
	"net/http"

	"github.com/PowerPenguini/murder/internal/reboot"
)

// Rebooter reboots this node's host.
type Rebooter interface {
	Reboot(ctx context.Context, request reboot.Request) error
}

// WithRebooter takes reboots of this node's host.
func (s *Server) WithRebooter(r Rebooter) *Server {
	s.rebooter = r
	return s
}

// rebootRoutes reboot hosts: only root asks, as for an upgrade.
func (s *Server) rebootRoutes(mux *http.ServeMux) {
	mux.HandleFunc("POST /v1/nodes/{node}/reboot", rootOnly(s.rebootNode))
}

// rebootNode reboots this node's host, or asks another node of the cluster
// to reboot its own; with check, it only asks whether it could.
func (s *Server) rebootNode(w http.ResponseWriter, r *http.Request) {
	var request reboot.Request
	if !readJSON(w, r, &request) {
		return
	}
	node := r.PathValue("node")
	// Only a reboot that happens: a check changes nothing.
	if !request.Check {
		if err := s.leased(r); err != nil {
			writeError(w, err)
			return
		}
	}
	var err error
	switch {
	case s.local(node) && s.rebooter != nil:
		err = s.rebooter.Reboot(r.Context(), request)
		switch {
		case errors.As(err, new(*reboot.NotReadyError)):
			err = statusError{http.StatusUnprocessableEntity, err}
		case errors.Is(err, reboot.ErrRebooting):
			err = statusError{http.StatusConflict, err}
		}
	case s.local(node) || s.cluster == nil:
		err = statusError{http.StatusConflict, fmt.Errorf("node %s takes no reboots from here", node)}
	default:
		err = s.cluster.RebootOn(r.Context(), node, request)
	}
	if err != nil {
		writeError(w, err)
		return
	}
	if request.Check {
		w.WriteHeader(http.StatusNoContent)
		return
	}
	if node == "-" {
		node = s.store.Node()
	}
	s.log.Info("a node reboots", "node", node, "actor", actor(r))
	if err := s.store.Note("node.rebooting", node, actor(r)); err != nil {
		s.log.Warn("record the reboot", "node", node, "error", err)
	}
	w.WriteHeader(http.StatusNoContent)
}
