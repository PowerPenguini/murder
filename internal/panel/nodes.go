package panel

import (
	"context"
	"net/http"
	"net/url"
	"slices"
	"strings"
	"time"

	"github.com/PowerPenguini/murder/internal/api"
	"github.com/PowerPenguini/murder/internal/store"
)

// nodeData is a node's page: its state, what it runs and keeps, what ran
// on it before it failed and where that runs now, and what can be done.
type nodeData struct {
	Node   api.NodeState
	State  tag
	Leader bool
	Self   bool
	// Clustered says there are other nodes; Targets are those it may be
	// drained to; Acts says the person may act on the cluster.
	Clustered bool
	Targets   []string
	Acts      bool
	// Runs are the tenants it runs, Keeps those it keeps a copy of.
	Runs, Keeps []tenantRow
	// Moved are the tenants that failed over from it, and where each runs
	// now, as the audit log says.
	Moved []movedTenant
	Use   *nodeUse
	// Backups are its backups; nil if it reports none.
	Backups *nodeBackups
	Events  []store.Event
}

// movedTenant is a tenant that left a node that failed: where it went,
// and where it runs now.
type movedTenant struct {
	Tenant, To, Now string
	At              time.Time
	Row             *tenantRow
}

// nodeEvents bounds the events a node's page reads for its own.
const nodeEvents = 400

func (s *Server) node(w http.ResponseWriter, r *http.Request) {
	ctx, cancel := context.WithTimeout(r.Context(), 10*time.Second)
	defer cancel()
	name := r.PathValue("name")
	var nodes api.Nodes
	var tenants []api.Tenant
	var events []store.Event
	if err := all(
		func() (err error) { nodes, err = s.backend.Nodes(ctx); return },
		func() (err error) { tenants, err = s.backend.List(ctx); return },
		func() (err error) { events, err = s.backend.Events(ctx, store.EventQuery{Limit: nodeEvents}); return },
	); err != nil {
		s.fail(w, r, err)
		return
	}
	i := slices.IndexFunc(nodes.Nodes, func(n api.NodeState) bool { return n.Name == name })
	if i < 0 {
		s.fail(w, r, &api.Error{Status: http.StatusNotFound, Message: "node " + name + ": not in the cluster"})
		return
	}
	archived := map[string]bool{}
	for _, t := range tenants {
		archived[t.Spec.Name] = t.Archived()
	}
	use := s.usageNow(ctx, archived)
	node := nodes.Nodes[i]
	now := s.now()
	data := nodeData{Node: node, State: nodeState(node, now), Leader: node.Name == nodes.Leader, Self: node.Name == nodes.Self,
		Clustered: len(nodes.Nodes) > 1, Acts: s.Cluster != nil}
	for _, other := range nodes.Nodes {
		if other.Name != name && other.Answers && !other.Failed() {
			data.Targets = append(data.Targets, other.Name)
		}
	}
	if u, ok := use.Nodes[name]; ok {
		data.Use = &u
	}
	if b, ok := use.NodeBackups[name]; ok {
		data.Backups = &b
	}
	rows := map[string]*tenantRow{}
	slices.SortFunc(tenants, func(a, b api.Tenant) int { return strings.Compare(a.Spec.Name, b.Spec.Name) })
	for _, t := range tenants {
		row := tenantRow{Tenant: t, State: tenantState(t), Replica: replicaState(t, data.Clustered)}
		if u, ok := use.Tenants[t.Spec.Name]; ok && !t.Archived() {
			row.Use = &u
		}
		if b, ok := use.Backups[t.Spec.Name]; ok {
			b.Stale = b.Stale && !t.Archived()
			row.Backup = &b
		}
		rows[t.Spec.Name] = &row
		switch {
		case t.Node == name:
			data.Runs = append(data.Runs, row)
		case t.Replica == name || t.ReplicaNext == name:
			data.Keeps = append(data.Keeps, row)
		}
	}
	// What failed over from it: "hel-1 → nbg-1" in the audit log.
	seen := map[string]bool{}
	for _, e := range events {
		mentions := strings.HasPrefix(e.Detail, name+" ") || strings.HasPrefix(e.Detail, name+":") || e.Detail == name ||
			strings.Contains(e.Detail, " "+name+" ") || strings.HasSuffix(e.Detail, " "+name) || strings.Contains(e.Detail, " "+name+",")
		if e.Action == "tenant.failed-over" && strings.HasPrefix(e.Detail, name+" ") && !seen[e.Tenant] {
			seen[e.Tenant] = true
			_, to, _ := strings.Cut(e.Detail, "→ ")
			m := movedTenant{Tenant: e.Tenant, To: strings.TrimSpace(to), At: e.At, Row: rows[e.Tenant]}
			if m.Row != nil {
				m.Now = m.Row.Node
			}
			data.Moved = append(data.Moved, m)
		}
		if mentions && len(data.Events) < 20 {
			data.Events = append(data.Events, e)
		}
	}
	s.write(w, "node", s.pageFor(w, r, name, "cluster", nodes, data), http.StatusOK)
}

// nodeBack is where an action on a node sends back to: its page, when the
// form came from there.
func nodeBack(r *http.Request, node string) string {
	if r.FormValue("from") == "node" && node != "" {
		return "/nodes/" + url.PathEscape(node)
	}
	return "/cluster"
}
