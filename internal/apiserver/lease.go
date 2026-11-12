package apiserver

import (
	"errors"
	"fmt"
	"net/http"
	"time"

	"github.com/PowerPenguini/murder/internal/api"
	"github.com/PowerPenguini/murder/internal/store"
)

// leaseRoutes serve the cluster's operation lease. Taking, renewing and
// releasing one is for whoever may move tenants, as the operations that
// hold it do; breaking another's is root's alone.
func (s *Server) leaseRoutes(mux *http.ServeMux) {
	mux.HandleFunc("GET /v1/cluster/lease", s.lease)
	mux.HandleFunc("POST /v1/cluster/lease", s.takeLease)
	mux.HandleFunc("POST /v1/cluster/lease/{id}/renew", s.renewLease)
	mux.HandleFunc("DELETE /v1/cluster/lease/{id}", s.releaseLease)
	mux.HandleFunc("POST /v1/cluster/lease/{id}/break", rootOnly(s.breakLease))
}

func (s *Server) lease(w http.ResponseWriter, _ *http.Request) {
	lease := s.store.Lease()
	writeJSON(w, http.StatusOK, api.Lease{Lease: lease, Held: lease.Held(time.Now())})
}

// takeLease has an operation hold the cluster. In a cluster, every node
// must run a release that keeps the lease first: one before would stop at
// the write.
func (s *Server) takeLease(w http.ResponseWriter, r *http.Request) {
	var request api.LeaseRequest
	if !readJSON(w, r, &request) {
		return
	}
	if request.Kind == "" {
		writeError(w, badRequest(errors.New("a lease needs the operation it is for")))
		return
	}
	if s.cluster != nil && len(s.store.Nodes()) > 1 {
		if err := s.cluster.EveryNodeRuns(r.Context(), store.LeaseSchema); err != nil {
			writeError(w, statusError{http.StatusConflict, fmt.Errorf("%w: finish upgrading the cluster first, then try again", err)})
			return
		}
	}
	lease, err := s.store.TakeLease(request.Kind, request.Detail, actor(r))
	if err != nil {
		writeError(w, err)
		return
	}
	s.log.Info("an operation holds the cluster", "operation", request.Kind, "detail", request.Detail, "lease", lease.ID, "actor", actor(r))
	writeJSON(w, http.StatusOK, lease)
}

// renewLease keeps a lease for its holder. One that is no longer the
// holder's is gone (410), which its holder stops at; any other failure,
// such as a cluster with no leader for a moment, is tried again.
func (s *Server) renewLease(w http.ResponseWriter, r *http.Request) {
	lease, err := s.store.RenewLease(r.PathValue("id"))
	if errors.Is(err, store.ErrLeaseLost) {
		err = statusError{http.StatusGone, err}
	}
	if err != nil {
		writeError(w, err)
		return
	}
	writeJSON(w, http.StatusOK, lease)
}

func (s *Server) releaseLease(w http.ResponseWriter, r *http.Request) {
	if err := s.store.ReleaseLease(r.PathValue("id"), actor(r)); err != nil {
		writeError(w, err)
		return
	}
	w.WriteHeader(http.StatusNoContent)
}

func (s *Server) breakLease(w http.ResponseWriter, r *http.Request) {
	lease, err := s.store.BreakLease(r.PathValue("id"), actor(r))
	if err != nil {
		writeError(w, err)
		return
	}
	s.log.Warn("the cluster's operation lease was broken", "lease", lease.ID, "operation", lease.Kind, "holder", lease.Actor, "actor", actor(r))
	writeJSON(w, http.StatusOK, lease)
}

// leased refuses a change of the cluster that request makes while an
// operation holds the lease, unless the request acts under that lease:
// a single move or removal goes on beside nothing that plans over the
// whole cluster.
func (s *Server) leased(r *http.Request) error {
	lease := s.store.Lease()
	if !lease.Held(time.Now()) || r.Header.Get(api.LeaseHeader) == lease.ID {
		return nil
	}
	return fmt.Errorf("%w: %s; wait until it ends, or if it is gone for good, \"murder cluster unlock\" breaks its lease", store.ErrLeased, lease)
}
