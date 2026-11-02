package panel

import (
	"context"
	"fmt"
	"html/template"
	"net/http"
	"slices"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/PowerPenguini/murder/internal/api"
	"github.com/PowerPenguini/murder/internal/store"
)

// tag is a status as the panel shows it: a word and its weight.
type tag struct {
	Label string
	// Kind is ok, warn, bad or mute.
	Kind string
}

// nodeView is one node on the overview.
type nodeView struct {
	api.NodeState
	Leader, Self bool
	State        tag
	Tenants      int
	Copies       int
}

func nodeState(node api.NodeState, now time.Time) tag {
	switch {
	case node.Failed():
		return tag{"FAILED", "bad"}
	case node.Answers:
		return tag{"UP", "ok"}
	case now.Before(node.AwayUntil):
		return tag{"RESTARTING", "warn"}
	}
	return tag{"DOWN", "bad"}
}

// tenantState is a tenant's state as one tag.
func tenantState(t api.Tenant) tag {
	switch {
	case t.Archived():
		return tag{"ARCHIVED", "mute"}
	case t.Spec.Suspended:
		return tag{"SUSPENDED", "mute"}
	case t.Status == nil:
		return tag{"UNKNOWN", "warn"}
	case t.Status.Phase == "running" && t.Status.Healthy:
		return tag{"RUNNING", "ok"}
	case t.Status.Phase == "running":
		return tag{"UNHEALTHY", "warn"}
	case t.Status.Phase == "failed":
		return tag{"FAILED", "bad"}
	}
	return tag{strings.ToUpper(t.Status.Phase), "warn"}
}

// replicaState says where the tenant's copy is, and whether it could take
// over.
func replicaState(t api.Tenant, cluster bool) tag {
	switch {
	case t.ReplicaNext != "":
		return tag{t.Replica + " -> " + t.ReplicaNext, "warn"}
	case t.Replica != "" && t.ReplicaReady:
		return tag{t.Replica, "ok"}
	case t.Replica != "":
		return tag{t.Replica + " · syncing", "warn"}
	case cluster && !t.Archived():
		return tag{"none", "bad"}
	}
	return tag{"—", "mute"}
}

type tenantRow struct {
	api.Tenant
	State, Replica tag
}

type overviewData struct {
	Nodes   []nodeView
	Tenants []tenantRow
	Events  []store.Event
	// Counts of tenants: all live ones, those meant to run (not
	// suspended), those running healthy, and those without a copy that
	// could take over.
	Live, Serving, Healthy, Unprotected int
	// Apps a tenant may be created of.
	Apps []string
}

func (s *Server) overview(w http.ResponseWriter, r *http.Request) {
	ctx, cancel := context.WithTimeout(r.Context(), 10*time.Second)
	defer cancel()
	// Asked at once: each may wait on a node that does not answer.
	var nodes api.Nodes
	var tenants []api.Tenant
	var events []store.Event
	if err := all(
		func() (err error) { nodes, err = s.backend.Nodes(ctx); return },
		func() (err error) { tenants, err = s.backend.List(ctx); return },
		func() (err error) { events, err = s.backend.Events(ctx, store.EventQuery{Limit: 12}); return },
	); err != nil {
		s.fail(w, r, err)
		return
	}
	now := s.now()
	clustered := len(nodes.Nodes) > 1
	data := overviewData{Events: events}
	byName := map[string]*nodeView{}
	for _, node := range nodes.Nodes {
		data.Nodes = append(data.Nodes, nodeView{NodeState: node, Leader: node.Name == nodes.Leader, Self: node.Name == nodes.Self, State: nodeState(node, now)})
	}
	for i := range data.Nodes {
		byName[data.Nodes[i].Name] = &data.Nodes[i]
	}
	slices.SortFunc(tenants, func(a, b api.Tenant) int { return strings.Compare(a.Spec.Name, b.Spec.Name) })
	for _, t := range tenants {
		row := tenantRow{Tenant: t, State: tenantState(t), Replica: replicaState(t, clustered)}
		data.Tenants = append(data.Tenants, row)
		if t.Archived() {
			continue
		}
		data.Live++
		if !t.Spec.Suspended {
			data.Serving++
		}
		if row.State.Kind == "ok" {
			data.Healthy++
		}
		if clustered && !t.ReplicaReady {
			data.Unprotected++
		}
		if n := byName[t.Node]; n != nil {
			n.Tenants++
		}
		if n := byName[t.Replica]; n != nil {
			n.Copies++
		}
	}
	if s.Actions != nil {
		if apps, err := s.Actions(s.actor(userOf(r))).Apps(ctx); err == nil {
			for _, app := range apps {
				data.Apps = append(data.Apps, app.Name)
			}
		}
	}
	s.write(w, "overview", s.pageFor(w, r, "Overview", "overview", nodes, data), http.StatusOK)
}

type tenantData struct {
	Tenant         api.Tenant
	State, Replica tag
	Events         []store.Event
	Env            []envVar
	// Targets are the nodes the tenant may move to.
	Targets []string
}

type envVar struct{ Key, Value string }

func (s *Server) tenant(w http.ResponseWriter, r *http.Request) {
	ctx, cancel := context.WithTimeout(r.Context(), 10*time.Second)
	defer cancel()
	name := r.PathValue("name")
	var t api.Tenant
	var events []store.Event
	var nodes api.Nodes
	if err := all(
		func() (err error) { t, err = s.backend.Get(ctx, name); return },
		func() (err error) {
			events, err = s.backend.Events(ctx, store.EventQuery{Tenant: name, Limit: 30})
			return
		},
		func() (err error) { nodes, err = s.backend.Nodes(ctx); return },
	); err != nil {
		s.fail(w, r, err)
		return
	}
	data := tenantData{Tenant: t, State: tenantState(t), Replica: replicaState(t, len(nodes.Nodes) > 1), Events: events}
	for key, value := range t.Spec.Env {
		data.Env = append(data.Env, envVar{key, value})
	}
	slices.SortFunc(data.Env, func(a, b envVar) int { return strings.Compare(a.Key, b.Key) })
	for _, node := range nodes.Nodes {
		if node.Name != t.Node && !node.Failed() {
			data.Targets = append(data.Targets, node.Name)
		}
	}
	s.write(w, "tenant", s.pageFor(w, r, t.Spec.Name, "overview", nodes, data), http.StatusOK)
}

type eventsData struct {
	Events      []store.Event
	Tenant, App string
	// Older is the ID to page back from; zero when there are no more.
	Older int64
}

const eventsPage = 100

func (s *Server) events(w http.ResponseWriter, r *http.Request) {
	query := store.EventQuery{Tenant: r.URL.Query().Get("tenant"), App: r.URL.Query().Get("app"), Limit: eventsPage}
	if before, err := strconv.ParseInt(r.URL.Query().Get("before"), 10, 64); err == nil && before > 0 {
		query.Before = before
	}
	ctx, cancel := context.WithTimeout(r.Context(), 10*time.Second)
	defer cancel()
	events, err := s.backend.Events(ctx, query)
	if err != nil {
		s.fail(w, r, err)
		return
	}
	data := eventsData{Events: events, Tenant: query.Tenant, App: query.App}
	if len(events) == eventsPage {
		data.Older = events[len(events)-1].ID
	}
	s.render(w, r, "events", "Events", "events", data)
}

// all runs calls at once and returns the first of their errors.
func all(calls ...func() error) error {
	errs := make([]error, len(calls))
	var wg sync.WaitGroup
	for i, call := range calls {
		wg.Add(1)
		go func() {
			defer wg.Done()
			errs[i] = call()
		}()
	}
	wg.Wait()
	for _, err := range errs {
		if err != nil {
			return err
		}
	}
	return nil
}

func (s *Server) funcs() template.FuncMap {
	return template.FuncMap{
		"static": staticURL,
		"lines":  func(text string) int { return strings.Count(text, "\n") + 1 },
		"ago":    func(t time.Time) string { return ago(s.now(), t) },
		"stamp": func(t time.Time) string {
			if t.IsZero() {
				return "—"
			}
			return t.UTC().Format("2006-01-02 15:04:05")
		},
		"short": shortImage,
		"add1":  func(n int) int { return n + 1 },
		"addrs": func(n api.NodeState) string {
			var out []string
			for _, a := range n.Public {
				out = append(out, a.String())
			}
			if len(out) == 0 {
				return "—"
			}
			return strings.Join(out, " ")
		},
		"limit": func(v any) string {
			switch v := v.(type) {
			case int:
				if v == 0 {
					return "—"
				}
				return strconv.Itoa(v)
			case float64:
				if v == 0 {
					return "—"
				}
				return strconv.FormatFloat(v, 'f', -1, 64)
			}
			return fmt.Sprint(v)
		},
	}
}

// ago is how long before now t was, in its largest unit.
func ago(now, t time.Time) string {
	if t.IsZero() {
		return "—"
	}
	d := now.Sub(t)
	switch {
	case d < 0:
		return "now"
	case d < time.Minute:
		return strconv.Itoa(int(d.Seconds())) + "s"
	case d < time.Hour:
		return strconv.Itoa(int(d.Minutes())) + "m"
	case d < 48*time.Hour:
		return strconv.Itoa(int(d.Hours())) + "h"
	}
	return strconv.Itoa(int(d.Hours()/24)) + "d"
}

// shortImage shortens a digest to its first 12 hex digits.
func shortImage(image string) string {
	if _, digest, ok := strings.Cut(image, "sha256:"); ok && len(digest) >= 12 {
		return digest[:12]
	}
	return image
}
