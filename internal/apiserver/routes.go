package apiserver

import (
	"errors"
	"net/http"
	"time"

	"github.com/PowerPenguini/murder/internal/edge"
)

// routesWait is how long a request for routes newer than the client's waits
// for a change before it answers with the same ones.
var routesWait = 25 * time.Second

// Table is every route this node serves, with its version.
func (r Routes) Table() edge.Table {
	routes := map[string]edge.Route{}
	for _, record := range r.Store.List() {
		if route, ok := r.Route(record.Spec.Name); ok {
			routes[record.Spec.Name] = route
		}
	}
	return edge.NewTable(routes)
}

// routes answers the edge: the route table, or, with after naming the
// version the edge has, the next one once the routes change, or the same
// one after routesWait.
func (s *Server) routes(w http.ResponseWriter, r *http.Request) {
	// A node set up for a cluster has the cluster's tenants only once it
	// is part of it.
	if s.cluster != nil && s.store.Cluster() == "" {
		writeError(w, statusError{http.StatusServiceUnavailable, errors.New("this node is not part of a cluster yet: it has no routes")})
		return
	}
	timeout := time.NewTimer(routesWait)
	defer timeout.Stop()
	// Nor does it name routes before it has caught up with the cluster
	// since it started: until then its state is what it had when it went
	// down, and a tenant may have failed over or moved since. An edge
	// started with it, as after a reboot, holds its requests for a while
	// meanwhile; an edge that already had routes from it keeps them.
	if s.cluster != nil {
		select {
		case <-s.caughtUp:
		case <-timeout.C:
			writeError(w, statusError{http.StatusServiceUnavailable, errors.New("this node has not caught up with its cluster since it started: its routes may be out of date")})
			return
		case <-r.Context().Done():
			return
		case <-s.stopping:
			writeError(w, statusError{http.StatusServiceUnavailable, errors.New("the agent is stopping")})
			return
		}
	}
	routes := Routes{Store: s.store, RelayPort: s.relayPort, Replacing: s.reconciler.Replacing}
	after := r.URL.Query().Get("after")
	for {
		// Watch before reading, so no change between the two is missed.
		changed, replacing := s.store.Watch(), s.reconciler.WatchReplacing()
		table := routes.Table()
		if table.Version != after {
			writeJSON(w, http.StatusOK, table)
			return
		}
		select {
		case <-changed:
		case <-replacing:
		case <-timeout.C:
			writeJSON(w, http.StatusOK, table)
			return
		case <-r.Context().Done():
			return
		case <-s.stopping:
			writeJSON(w, http.StatusOK, table)
			return
		}
	}
}
