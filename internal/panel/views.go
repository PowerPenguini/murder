package panel

import (
	"context"
	"fmt"
	"html/template"
	"maps"
	"math"
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
	// MemoryMB is what its serving tenants' limits reserve.
	MemoryMB int
	// The bars of the nodes' table, each against the largest node's.
	TenantBar, CopyBar, MemoryBar meter
	// Use is its host as measured; nil if it was not.
	Use *nodeUse
}

func nodeState(node api.NodeState, now time.Time) tag {
	switch {
	case node.Failed():
		return tag{"failed", "bad"}
	case node.Answers:
		return tag{"up", "ok"}
	case now.Before(node.AwayUntil):
		return tag{"restarting", "warn"}
	}
	return tag{"down", "bad"}
}

// tenantState is a tenant's state as one tag.
func tenantState(t api.Tenant) tag {
	switch {
	case t.Archived():
		return tag{"archived", "mute"}
	case t.Spec.Suspended:
		return tag{"suspended", "mute"}
	case t.Status == nil:
		return tag{"unknown", "warn"}
	case t.Status.Phase == "running" && t.Status.Healthy:
		return tag{"running", "ok"}
	case t.Status.Phase == "running":
		return tag{"unhealthy", "warn"}
	case t.Status.Phase == "failed":
		return tag{"failed", "bad"}
	}
	return tag{t.Status.Phase, "warn"}
}

// replicaState says where the tenant's copy is, and whether it could take
// over.
func replicaState(t api.Tenant, cluster bool) tag {
	switch {
	case t.ReplicaNext != "":
		return tag{t.Replica + " → " + t.ReplicaNext, "warn"}
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
	// Placements say, per node, what the tenant has there.
	Placements []placement
	// Use is what it uses now; nil if it was not measured.
	Use *tenantUse
}

type overviewData struct {
	Nodes   []nodeView
	Tenants []tenantRow
	Events  []store.Event
	// States splits the live tenants by what they need; Activity counts
	// the events of the last day, by hour.
	States   segmentedBar
	Activity activity
	// Counts of tenants: all live ones, those meant to run (not
	// suspended), those running healthy, and those without a copy that
	// could take over.
	Live, Serving, Healthy, Unprotected int
	// Apps a tenant may be created of.
	Apps []string
	// Attention is what needs it, the broken first.
	Attention []notice
}

func (s *Server) overview(w http.ResponseWriter, r *http.Request) {
	ctx, cancel := context.WithTimeout(r.Context(), 10*time.Second)
	defer cancel()
	// Asked at once: each may wait on a node that does not answer.
	var nodes api.Nodes
	var tenants []api.Tenant
	var events []store.Event
	// Use is shown when it can be had: a page without it is still the page.
	var nodeUses map[string]nodeUse
	var tenantUses map[string]tenantUse
	if err := all(
		func() error { nodeUses, tenantUses = s.usageNow(ctx); return nil },
		func() (err error) { nodes, err = s.backend.Nodes(ctx); return },
		func() (err error) { tenants, err = s.backend.List(ctx); return },
		func() (err error) {
			events, err = s.backend.Events(ctx, store.EventQuery{Limit: activityEvents})
			return
		},
	); err != nil {
		s.fail(w, r, err)
		return
	}
	now := s.now()
	clustered := len(nodes.Nodes) > 1
	data := overviewData{Events: events[:min(len(events), 8)],
		Activity: activityOf(events, now, 24*time.Hour, time.Hour, len(events) < activityEvents, hourLabel)}
	byName := map[string]*nodeView{}
	for _, node := range nodes.Nodes {
		data.Nodes = append(data.Nodes, nodeView{NodeState: node, Leader: node.Name == nodes.Leader, Self: node.Name == nodes.Self, State: nodeState(node, now)})
	}
	for i := range data.Nodes {
		byName[data.Nodes[i].Name] = &data.Nodes[i]
		if use, ok := nodeUses[data.Nodes[i].Name]; ok {
			data.Nodes[i].Use = &use
		}
	}
	slices.SortFunc(tenants, func(a, b api.Tenant) int { return strings.Compare(a.Spec.Name, b.Spec.Name) })
	for _, t := range tenants {
		row := tenantRow{Tenant: t, State: tenantState(t), Replica: replicaState(t, clustered)}
		if use, ok := tenantUses[t.Spec.Name]; ok && !t.Archived() {
			row.Use = &use
		}
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
			if !t.Spec.Suspended {
				n.MemoryMB += t.Spec.Limits.MemoryMB
			}
		}
		if n := byName[t.Replica]; n != nil {
			n.Copies++
		}
	}
	var most, mostMemory int
	for _, n := range data.Nodes {
		most, mostMemory = max(most, n.Tenants, n.Copies), max(mostMemory, n.MemoryMB)
	}
	for i := range data.Nodes {
		n := &data.Nodes[i]
		n.TenantBar, n.CopyBar, n.MemoryBar = meterOf(n.Tenants, most), meterOf(n.Copies, most), meterOf(n.MemoryMB, mostMemory)
	}
	for i := range data.Tenants {
		data.Tenants[i].Placements = placementsOf(data.Tenants[i], data.Nodes)
	}
	data.States = stateBar(data.Tenants)
	data.Attention = attentionOf(data.Nodes, data.Tenants, clustered, now)
	if s.Actions != nil {
		if apps, err := s.Actions(s.actor(userOf(r))).Apps(ctx); err == nil {
			for _, app := range apps {
				data.Apps = append(data.Apps, app.Name)
			}
		}
	}
	s.write(w, "overview", s.pageFor(w, r, "Overview", "overview", nodes, data), http.StatusOK)
}

// activityEvents is how many of the newest events the activity charts
// read: the store's most.
const activityEvents = 1000

type tenantData struct {
	Tenant         api.Tenant
	State, Replica tag
	Events         []store.Event
	// Activity counts its events of the last fortnight, by day.
	Activity activity
	// Usage is its last day, drawn; nil if it was not measured.
	Usage *tenantUsage
	// Problem says what is wrong with it, if anything; Broken whether it
	// does not serve; MemoryHigh whether it nears its memory limit.
	Problem    string
	Broken     bool
	MemoryHigh bool
	Env        []envVar
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
	var use *tenantUsage
	if err := all(
		// An archived tenant is measured no more: it has no day to show.
		func() error { use = s.tenantUsage(ctx, name); return nil },
		func() (err error) { t, err = s.backend.Get(ctx, name); return },
		func() (err error) {
			events, err = s.backend.Events(ctx, store.EventQuery{Tenant: name, Limit: activityEvents})
			return
		},
		func() (err error) { nodes, err = s.backend.Nodes(ctx); return },
	); err != nil {
		s.fail(w, r, err)
		return
	}
	data := tenantData{Tenant: t, State: tenantState(t), Replica: replicaState(t, len(nodes.Nodes) > 1), Events: events[:min(len(events), 30)],
		Activity: activityOf(events, s.now(), 14*24*time.Hour, 24*time.Hour, len(events) < activityEvents, dayLabel), Usage: use}
	if !t.Archived() && !t.Spec.Suspended {
		data.Problem, data.Broken = tenantProblem(t, data.State)
		if data.Problem != "" {
			data.Problem = "It " + data.Problem + "."
		}
		if use != nil && use.Memory.MemoryLimit != "" && use.Memory.Memory.Width >= memoryWarn {
			data.MemoryHigh = true
			data.Problem = strings.TrimSpace(data.Problem + " It uses " + use.Memory.MemoryText + " of its " + use.Memory.MemoryLimit + " of memory: the kernel kills it at the limit.")
		}
	}
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
	// People shows only what people did; Tenants and Apps are the names
	// the filters offer.
	People        bool
	Tenants, Apps []string
	// Older is the ID to page back from; zero when there are no more.
	Older int64
}

const eventsPage = 100

func (s *Server) events(w http.ResponseWriter, r *http.Request) {
	query := store.EventQuery{Tenant: r.URL.Query().Get("tenant"), App: r.URL.Query().Get("app"), Limit: eventsPage}
	if before, err := strconv.ParseInt(r.URL.Query().Get("before"), 10, 64); err == nil && before > 0 {
		query.Before = before
	}
	people := r.URL.Query().Get("people") != ""
	if people {
		// What people did is found among more of the cluster's own.
		query.Limit = activityEvents
	}
	ctx, cancel := context.WithTimeout(r.Context(), 10*time.Second)
	defer cancel()
	var events []store.Event
	var tenants []api.Tenant
	if err := all(
		func() (err error) { events, err = s.backend.Events(ctx, query); return },
		// The filters' names: a list that fails leaves them to be typed.
		func() error { tenants, _ = s.backend.List(ctx); return nil },
	); err != nil {
		s.fail(w, r, err)
		return
	}
	data := eventsData{Tenant: query.Tenant, App: query.App, People: people}
	apps := map[string]bool{}
	for _, t := range tenants {
		data.Tenants = append(data.Tenants, t.Spec.Name)
		apps[t.App] = true
	}
	slices.Sort(data.Tenants)
	data.Apps = slices.Sorted(maps.Keys(apps))
	read := events
	if people {
		events = slices.DeleteFunc(slices.Clone(events), func(e store.Event) bool { return !byPeople(e) })
	}
	switch {
	case len(events) > eventsPage:
		events = events[:eventsPage]
		data.Older = events[len(events)-1].ID
	case len(read) == query.Limit:
		data.Older = read[len(read)-1].ID
	}
	data.Events = events
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
		"static":   staticURL,
		"action":   actionText,
		"loglines": logLines,
		"head": func(name string, state any, on string, logs, settings bool) tenantHead {
			h := tenantHead{Name: name, On: on, Logs: logs, Settings: settings}
			if t, ok := state.(tag); ok {
				h.State = &t
			}
			return h
		},
		"mem":   memory,
		"lines": func(text string) int { return strings.Count(text, "\n") + 1 },
		"ago":   func(t time.Time) string { return ago(s.now(), t) },
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

// memory says an amount of memory as people do: 768 MB, 1.5 GB.
func memory(mb int) string {
	if mb < 1024 {
		return strconv.Itoa(mb) + " MB"
	}
	return strconv.FormatFloat(math.Round(float64(mb)/1024*10)/10, 'f', -1, 64) + " GB"
}

// tenantHead is the head of a tenant's pages: where it is, its name and
// state, and its pages.
type tenantHead struct {
	Name, On       string
	State          *tag
	Logs, Settings bool
}
