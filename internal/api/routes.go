package api

import (
	"context"
	"net/http"
	"net/url"
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
	routes := Routes{Store: s.store}
	after := r.URL.Query().Get("after")
	timeout := time.NewTimer(routesWait)
	defer timeout.Stop()
	for {
		// Watch before reading, so no change between the two is missed.
		changed := s.store.Watch()
		table := routes.Table()
		if table.Version != after {
			writeJSON(w, http.StatusOK, table)
			return
		}
		select {
		case <-changed:
		case <-timeout.C:
			writeJSON(w, http.StatusOK, table)
			return
		case <-r.Context().Done():
			return
		}
	}
}

// Routes returns the node's routes; with after set, it waits for routes of
// another version, up to a while.
func (c *Client) Routes(ctx context.Context, after string) (edge.Table, error) {
	var table edge.Table
	err := c.do(ctx, http.MethodGet, "/v1/routes?after="+url.QueryEscape(after), nil, &table)
	return table, err
}
