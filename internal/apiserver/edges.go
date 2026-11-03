package apiserver

import (
	"context"
	"sync"
	"time"

	"github.com/PowerPenguini/murder/internal/edge"
)

// EdgeHeader names the edge process asking for routes, so the agent knows
// which table each edge serves from.
const EdgeHeader = "X-Murder-Edge"

// edges is what this node's edges serve from, as their requests for routes
// say: each asks for the table after the one it has.
type edges struct {
	mu sync.Mutex
	// held are the tenants each recent table holds requests for.
	held  map[string]map[string]bool
	order []string
	// has is the version each edge last said it serves, and when.
	has map[string]edgeHas
	// changed is closed, and replaced, when an edge says what it has.
	changed chan struct{}
}

type edgeHas struct {
	version string
	at      time.Time
}

// tablesKept bounds the tables remembered.
const tablesKept = 64

// edgeLive is how long an edge that has not asked for routes still counts:
// a live one asks at least every routesWait.
func edgeLive() time.Duration { return 2 * routesWait }

func (e *edges) init() {
	if e.held == nil {
		e.held, e.has, e.changed = map[string]map[string]bool{}, map[string]edgeHas{}, make(chan struct{})
	}
}

// sent remembers a table given to an edge.
func (e *edges) sent(table edge.Table) {
	e.mu.Lock()
	defer e.mu.Unlock()
	e.init()
	if _, ok := e.held[table.Version]; ok {
		return
	}
	held := map[string]bool{}
	for tenant, route := range table.Routes {
		if route.Moving {
			held[tenant] = true
		}
	}
	e.held[table.Version] = held
	e.order = append(e.order, table.Version)
	if len(e.order) > tablesKept {
		delete(e.held, e.order[0])
		e.order = e.order[1:]
	}
}

// says records that edge serves from version.
func (e *edges) says(edge, version string, now time.Time) {
	e.mu.Lock()
	defer e.mu.Unlock()
	e.init()
	e.has[edge] = edgeHas{version: version, at: now}
	close(e.changed)
	e.changed = make(chan struct{})
}

// hold reports whether every edge that serves holds tenant's requests:
// none if none serves, as one starting would serve from what it saved.
func (e *edges) hold(tenant string, now time.Time) (bool, <-chan struct{}) {
	e.mu.Lock()
	defer e.mu.Unlock()
	e.init()
	live := 0
	for name, has := range e.has {
		if now.Sub(has.at) > edgeLive() {
			delete(e.has, name)
			continue
		}
		live++
		if held, ok := e.held[has.version]; !ok || !held[tenant] {
			return false, e.changed
		}
	}
	return live > 0, e.changed
}

// EdgesHold waits until every edge of this node holds the requests for
// tenant, so none reaches its container; false if ctx ends first.
func (s *Server) EdgesHold(ctx context.Context, tenant string) bool {
	for {
		held, changed := s.edges.hold(tenant, time.Now())
		if held {
			return true
		}
		select {
		case <-ctx.Done():
			return false
		case <-changed:
		case <-time.After(100 * time.Millisecond):
		}
	}
}
