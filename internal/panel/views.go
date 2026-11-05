package panel

import (
	"context"
	"fmt"
	"html/template"
	"maps"
	"math"
	"net/http"
	"net/url"
	"regexp"
	"slices"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/PowerPenguini/murder/internal/api"
	"github.com/PowerPenguini/murder/internal/spec"
	"github.com/PowerPenguini/murder/internal/store"
	"github.com/PowerPenguini/murder/internal/volume"
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
		return tag{"unhealthy", "bad"}
	case t.Status.Phase == "failed":
		return tag{"failed", "bad"}
	}
	return tag{t.Status.Phase, "warn"}
}

// writable is a tenant's state with what its use says: one whose data
// fills its disk limit runs, but cannot write, and is not healthy.
func writable(state tag, use *tenantUse) tag {
	if use != nil && use.DiskFull() && state.Kind == "ok" {
		return tag{"cannot write", "bad"}
	}
	return state
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
	return tag{"none", "mute"}
}

type tenantRow struct {
	api.Tenant
	State, Replica tag
	// Use is what it uses now; nil if it was not measured.
	Use *tenantUse
	// Backup is its newest backup; nil if its node backs up nothing, or
	// did not say.
	Backup *backupView
	// Reason says why it needs attention, as its row shows it; ReasonBad
	// that it is broken; Attention that it needs it at all.
	Reason               string
	ReasonBad, Attention bool
}

type overviewData struct {
	Nodes   []nodeView
	Tenants []tenantRow
	Events  []store.Event
	// Activity counts the events of the last day, by hour.
	Activity activity
	// Counts of tenants: all live ones, those meant to run (not
	// suspended), those running healthy, and those without a copy that
	// could take over.
	Live, Serving, Healthy, Unprotected int
	// Apps a tenant may be created of.
	Apps []string
	// Attention is what needs it, the broken first; Broken counts the
	// broken among it.
	Attention []notice
	Broken    int
	// Rollouts are the apps' rollouts under way.
	Rollouts []rolloutNote
	// FailingBackups counts the nodes whose backups fail; AppStates are
	// each app's problems, by name.
	FailingBackups int
	AppStates      map[string]appState
	// Clustered says there is more than one node.
	Clustered bool
	// The tenants' page's filter: Show is all, attention, unprotected,
	// suspended or archived; Query the text asked for; ByName sorts by name
	// rather than problems first. Shown are the tenants it lets through.
	Show, Query string
	ByName      bool
	Shown       []tenantRow
	// Counts of each filter, for its link.
	Counts map[string]int
}

// rolloutNote is an app's rollout under way, as the overview says it.
type rolloutNote struct {
	App, Text string
	// Paused says it was paused; Blocked that a tenant it moved is not
	// healthy, which is wrong.
	Paused, Blocked bool
}

func (s *Server) overview(w http.ResponseWriter, r *http.Request) {
	ctx, cancel := context.WithTimeout(r.Context(), 10*time.Second)
	defer cancel()
	nodes, data, err := s.overviewOf(ctx, r)
	if err != nil {
		s.fail(w, r, err)
		return
	}
	s.write(w, "overview", s.pageFor(w, r, "Overview", "overview", nodes, data), http.StatusOK)
}

// tenants is every tenant, with what each uses, and the way to make one.
func (s *Server) tenants(w http.ResponseWriter, r *http.Request) {
	ctx, cancel := context.WithTimeout(r.Context(), 10*time.Second)
	defer cancel()
	nodes, data, err := s.overviewOf(ctx, r)
	if err != nil {
		s.fail(w, r, err)
		return
	}
	s.write(w, "tenants", s.pageFor(w, r, "Tenants", "tenants", nodes, data), http.StatusOK)
}

// overviewOf is the cluster as the overview and the tenants' page show it.
func (s *Server) overviewOf(ctx context.Context, r *http.Request) (api.Nodes, overviewData, error) {
	// Asked at once: each may wait on a node that does not answer.
	var nodes api.Nodes
	var tenants []api.Tenant
	var events []store.Event
	// Use is shown when it can be had: a page without it is still the page.
	var use clusterUse
	if err := all(
		func() error { use = s.usageNow(ctx); return nil },
		func() (err error) { nodes, err = s.backend.Nodes(ctx); return },
		func() (err error) { tenants, err = s.backend.List(ctx); return },
		func() (err error) {
			events, err = s.backend.Events(ctx, store.EventQuery{Limit: activityEvents})
			return
		},
	); err != nil {
		return nodes, overviewData{}, err
	}
	now := s.now()
	clustered := len(nodes.Nodes) > 1
	data := overviewData{Events: shownEvents(events, "", 8),
		Activity: activityOf(events, now, 24*time.Hour, time.Hour, len(events) < activityEvents, hourLabel)}
	byName := map[string]*nodeView{}
	for _, node := range nodes.Nodes {
		data.Nodes = append(data.Nodes, nodeView{NodeState: node, Leader: node.Name == nodes.Leader, Self: node.Name == nodes.Self, State: nodeState(node, now)})
	}
	for i := range data.Nodes {
		byName[data.Nodes[i].Name] = &data.Nodes[i]
		if u, ok := use.Nodes[data.Nodes[i].Name]; ok {
			data.Nodes[i].Use = &u
		}
	}
	slices.SortFunc(tenants, func(a, b api.Tenant) int { return strings.Compare(a.Spec.Name, b.Spec.Name) })
	for _, t := range tenants {
		row := tenantRow{Tenant: t, State: tenantState(t), Replica: replicaState(t, clustered)}
		if u, ok := use.Tenants[t.Spec.Name]; ok && !t.Archived() {
			row.Use = &u
		}
		row.State = writable(row.State, row.Use)
		if b, ok := use.Backups[t.Spec.Name]; ok {
			// An archived tenant is backed up once, as it was archived.
			b.Stale = b.Stale && !t.Archived()
			row.Backup = &b
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
	data.Clustered = clustered
	data.Attention = attentionOf(data.Nodes, data.Tenants, clustered, now)
	data.FailingBackups = failingNodes(use)
	for _, n := range data.Attention {
		if n.Bad {
			data.Broken++
		}
	}
	if s.Actions != nil {
		if apps, err := s.Actions(s.actor(userOf(r))).Apps(ctx); err == nil {
			slices.SortFunc(apps, func(a, b api.App) int { return strings.Compare(a.Name, b.Name) })
			for _, app := range apps {
				data.Apps = append(data.Apps, app.Name)
			}
			data.Attention, data.Rollouts, data.AppStates = joinApps(data.Attention, apps, tenants, events, now, s.actor(userOf(r)))
		}
	}
	data.Show, data.Query, data.ByName = tenantFilter(r)
	attention := map[string]int{}
	for _, n := range data.Attention {
		if n.Node || n.App {
			continue
		}
		attention[n.Subject] = map[bool]int{true: 0, false: 1}[n.Bad]
		for i := range data.Tenants {
			if data.Tenants[i].Spec.Name != n.Subject {
				continue
			}
			reasons := slices.Clone(n.Reasons)
			for _, sub := range n.Subs {
				if strings.HasPrefix(sub.Text, "It blocks ") {
					reasons = append(reasons, "blocks "+data.Tenants[i].App+"'s rollout")
				}
			}
			data.Tenants[i].Reason, data.Tenants[i].ReasonBad, data.Tenants[i].Attention = strings.Join(reasons, "; "), n.Bad, true
		}
	}
	data.Shown, data.Counts = filterTenants(data.Tenants, data.Show, data.Query, data.ByName, clustered, attention)
	return nodes, data, nil
}

// triage is what needs attention in the cluster, as the overview says it,
// how many nodes' backups fail, and how many apps are broken: for the side
// bar's counts on any page.
func (s *Server) triage(nodes api.Nodes, tenants []api.Tenant, use clusterUse, apps []api.App, events []store.Event) ([]notice, int, int) {
	now := s.now()
	clustered := len(nodes.Nodes) > 1
	var views []nodeView
	for _, n := range nodes.Nodes {
		views = append(views, nodeView{NodeState: n, State: nodeState(n, now)})
	}
	var rows []tenantRow
	for _, t := range tenants {
		row := tenantRow{Tenant: t, State: tenantState(t), Replica: replicaState(t, clustered)}
		if u, ok := use.Tenants[t.Spec.Name]; ok && !t.Archived() {
			row.Use = &u
		}
		row.State = writable(row.State, row.Use)
		if b, ok := use.Backups[t.Spec.Name]; ok {
			b.Stale = b.Stale && !t.Archived()
			row.Backup = &b
		}
		rows = append(rows, row)
	}
	notices, _, states := joinApps(attentionOf(views, rows, clustered, now), apps, tenants, events, now, "")
	return notices, failingNodes(use), badApps(states)
}

// diskWhy says where a tenant's disk limit comes from, in one line: "Its
// 2 GB limit was set by jan (panel) 3m ago." (by you, when me set it),
// "Its 2 GB limit is web's."; "" when it has none.
func diskWhy(t api.Tenant, events []store.Event, now time.Time, me string) string {
	l := limitsOf(t, nil)[3]
	if l.Value <= 0 {
		return ""
	}
	said := "Its " + nbsp(l.Said) + " limit"
	switch l.From {
	case fromOwn:
		// The change that gave it: the oldest of the newest changes of its
		// settings that all name it (each names every limit it has).
		mark := "diskGB=" + strconv.Itoa(int(l.Value))
		var set *store.Event
		for i, e := range events {
			if e.Tenant != t.Spec.Name || (e.Action != "tenant.updated" && e.Action != "tenant.created") {
				continue
			}
			if !strings.Contains(e.Detail, mark) {
				break
			}
			set = &events[i]
		}
		if set != nil {
			by := who(set.Actor)
			if sameActor(set.Actor, me) {
				by = "you"
			}
			return said + " was set by " + by + " " + ago(now, set.At) + " ago."
		}
		return said + " is its own."
	case fromApp:
		return said + " is " + t.App + "'s."
	case fromNode:
		return said + " is " + t.Node + "'s default."
	}
	return ""
}

// badApps counts the apps something is wrong with.
func badApps(states map[string]appState) int {
	n := 0
	for _, state := range states {
		if state.Bad {
			n++
		}
	}
	return n
}

// failingNodes counts the nodes whose backups fail.
func failingNodes(use clusterUse) int {
	n := 0
	for _, b := range use.NodeBackups {
		if b.Failing > 0 {
			n++
		}
	}
	return n
}

// appState is what is wrong with an app: its rollout blocked, its
// tenants out of memory; Bad when either is.
type appState struct {
	Blockers   []string
	ToRevision int64
	Memory     *appMemory
	Bad        bool
}

// joinApps puts what apps say into the notices of the tenants they explain:
// a blocked rollout and an app-wide memory cause are said under the broken
// tenant, once, red as it is, so every red row is one counted thing. An
// app-wide cause that explains no broken tenant stays its own at-risk row.
// It returns the notices, the rollouts under way that are not blocked, and
// each app's state.
func joinApps(notices []notice, apps []api.App, tenants []api.Tenant, events []store.Event, now time.Time, me string) ([]notice, []rolloutNote, map[string]appState) {
	states := map[string]appState{}
	var notes []rolloutNote
	find := func(name string) *notice {
		for i := range notices {
			if !notices[i].Node && !notices[i].App && notices[i].Subject == name {
				return &notices[i]
			}
		}
		return nil
	}
	// A tenant whose data fills its disk limit: where that limit comes
	// from, and who set it when.
	for i := range notices {
		n := &notices[i]
		if n.Node || n.App || n.Primary == nil || n.Primary.Label != "Raise its disk limit" {
			continue
		}
		for _, t := range tenants {
			if t.Spec.Name == n.Subject {
				if why := diskWhy(t, events, now, me); why != "" {
					n.Subs = append([]sub{{Short: why, Text: why}}, n.Subs...)
				}
			}
		}
	}
	for _, app := range apps {
		state := appState{}
		appHref := "/apps/" + url.PathEscape(app.Name)
		if m := appMemoryOf(app, tenants, events, now); m != nil {
			state.Memory = m
			raise := fix{"Raise " + app.Name + "'s memory limit", m.Href()}
			cause := sub{Bad: m.Bad, Short: m.Cause(), Text: m.Evidence()}
			placed := false
			for _, name := range m.HitNames() {
				if n := find(name); n != nil && n.Bad && !placed {
					n.Subs = append(n.Subs, cause)
					// The app's fix is the fix; the tenant's own the last
					// resort, in its evidence.
					n.Primary = &raise
					placed = true
				}
			}
			if !placed {
				notices = append(notices, notice{App: true, Subject: app.Name, Href: appHref, Text: m.Text(), Head: "'s memory limit is likely too low", Primary: &raise,
					Subs: []sub{{Short: m.Cause(), Text: m.Evidence()}}})
			}
		}
		if ro := app.Rollout; ro != nil && ro.Active() {
			view := rolloutOf(app, tenants, events, now)
			if view != nil && len(view.Blockers) > 0 {
				state.Blockers, state.ToRevision = view.Blockers, ro.ToRevision
				for _, name := range view.Blockers {
					if n := find(name); n != nil {
						text := fmt.Sprintf("It blocks %s's rollout of revision %d: %s", app.Name, ro.ToRevision, rolloutBlocked(*ro, view.Blockers))
						n.Subs = append(n.Subs, sub{Bad: true, Short: text, Text: text, Fixes: []fix{{"The rollout", appHref + "#rollout"}}})
					}
				}
			} else {
				note := rolloutNote{App: app.Name, Paused: ro.State == store.RolloutPaused}
				if note.Paused {
					note.Text = fmt.Sprintf("'s rollout of revision %d is paused", ro.ToRevision)
					if ro.Reason != "" {
						note.Text += ": " + ro.Reason
					}
				} else {
					note.Text = fmt.Sprintf(" rolls out revision %d: %s, for %s", ro.ToRevision, strings.ToLower(ro.WaveName(max(ro.Wave, 1))), ago(now, ro.WaveStarted))
				}
				notes = append(notes, note)
			}
		}
		state.Bad = len(state.Blockers) > 0 || (state.Memory != nil && state.Memory.Bad)
		states[app.Name] = state
	}
	// A broken tenant the app's limit explains leads with the app's fix;
	// its own limit is the last resort.
	for i := range notices {
		n := &notices[i]
		if n.Primary != nil && strings.HasSuffix(n.Primary.Label, "'s memory limit") && !n.App {
			n.Fixes = slices.DeleteFunc(n.Fixes, func(f fix) bool { return f.Label == "Raise its memory limit" })
			n.More = append(n.More, fix{"Only this tenant: raise its limit", raiseMemory(tenantNamed(tenants, n.Subject))})
		}
	}
	return notices, notes, states
}

// tenantNamed is the tenant of that name among tenants.
func tenantNamed(tenants []api.Tenant, name string) api.Tenant {
	for _, t := range tenants {
		if t.Spec.Name == name {
			return t
		}
	}
	var t api.Tenant
	t.Spec.Name = name
	return t
}

// rolloutWaves is how many waves a rollout moves in: canaries, a tenth,
// half, the rest.
const rolloutWaves = 4

// appContext puts in a tenant's page what its app says of it: the
// rollout it is in, and whether its app's tenants run out of memory.
func (s *Server) appContext(ctx context.Context, r *http.Request, t api.Tenant, data *tenantData) {
	if s.Actions == nil || t.Archived() {
		return
	}
	var apps []api.App
	var tenants []api.Tenant
	var events []store.Event
	if all(
		func() (err error) { apps, err = s.Actions(s.actor(userOf(r))).Apps(ctx); return },
		func() (err error) { tenants, err = s.backend.List(ctx); return },
		func() (err error) {
			events, err = s.backend.Events(ctx, store.EventQuery{App: t.App, Limit: activityEvents})
			return
		},
	) != nil {
		return
	}
	// The limits it runs with, by the one account every page gives; those
	// it runs without that other running tenants have are its risk.
	data.Limits = limitsOf(t, runningLimits(tenants))
	if !t.Spec.Suspended {
		missing, field := risks(data.Limits)
		data.Unlimited, data.RiskField = strings.Join(missing, ", "), field
		// What the others have, for one limit missing: what to set.
		if len(missing) == 1 {
			for _, d := range limitDefs {
				if d.key == missing[0] {
					if v := othersHave(t, tenants, d.key); v > 0 {
						data.RiskOthers = d.said(v)
					}
				}
			}
		}
	}
	i := slices.IndexFunc(apps, func(a api.App) bool { return a.Name == t.App })
	if i < 0 {
		return
	}
	now := s.now()
	if v := rolloutOf(apps[i], tenants, events, now); v != nil {
		data.Rollout = v
		for j := range v.Rows {
			if v.Rows[j].Name == t.Spec.Name {
				data.Place = &v.Rows[j]
			}
		}
	}
	if p := data.Place; p != nil && !p.Moved && !p.Held && p.From == p.To && s.Apps != nil {
		if revisions, err := s.Apps(s.actor(userOf(r))).AppRevisions(ctx, t.App); err == nil {
			data.Unchanged = unchangedBy(t, apps[i], revisions)
		}
	}
	// The app's limit is said to be the cause only of a tenant that uses it,
	// ran out of memory, and is broken now: not of one merely starting.
	if m := appMemoryOf(apps[i], tenants, events, now); m != nil && m.Hit[t.Spec.Name] > 0 && data.Broken {
		data.AppMemory = m
	}
	// A person's change of its app that reached it: whether it held.
	if o := outcomeOf(apps[i], nil, tenants, events, now); o != nil && slices.Contains(o.Reached, t.Spec.Name) {
		if s.Apps != nil {
			if revisions, err := s.Apps(s.actor(userOf(r))).AppRevisions(ctx, t.App); err == nil {
				o = outcomeOf(apps[i], revisions, tenants, events, now)
			}
		}
		data.Outcome = o
	}
}

// tenantFilters are the tenants' page's filters, in order.
var tenantFilters = []string{"all", "broken", "risk", "attention", "unhealthy", "unprotected", "suspended", "archived"}

// tenantFilter reads the tenants' page's filter from its address.
func tenantFilter(r *http.Request) (show, query string, byName bool) {
	show = r.URL.Query().Get("show")
	if !slices.Contains(tenantFilters, show) {
		show = "all"
	}
	return show, strings.TrimSpace(r.URL.Query().Get("q")), r.URL.Query().Get("sort") == "name"
}

// rank orders tenants by what they need: the broken, those at risk, those
// that run, the suspended, the archived.
func (row tenantRow) rank(clustered bool) int {
	switch {
	case row.Archived():
		return 4
	case row.Spec.Suspended:
		return 3
	case row.State.Kind == "bad":
		return 0
	case row.State.Kind == "warn" || row.MemoryHigh() || row.Unprotected(clustered):
		return 1
	}
	return 2
}

// MemoryHigh says it nears its memory limit.
func (row tenantRow) MemoryHigh() bool {
	return row.Use != nil && row.Use.MemoryLimit != "" && row.Use.Memory.Width >= memoryWarn
}

// Unprotected says no copy could take over, should its node fail.
func (row tenantRow) Unprotected(clustered bool) bool {
	return clustered && !row.Archived() && !row.ReplicaReady
}

// filterTenants is the tenants a filter lets through, problems first
// unless byName, and how many each filter lets through.
// attention ranks the tenants the overview lists: 0 broken, 1 at risk.
func filterTenants(rows []tenantRow, show, query string, byName, clustered bool, attention map[string]int) ([]tenantRow, map[string]int) {
	query = strings.ToLower(query)
	counts := map[string]int{}
	var out []tenantRow
	for _, row := range rows {
		in := map[string]bool{
			"all":       true,
			"attention": hasKey2(attention, row.Spec.Name),
			"broken":    hasKey2(attention, row.Spec.Name) && attention[row.Spec.Name] == 0,
			"risk":      hasKey2(attention, row.Spec.Name) && attention[row.Spec.Name] == 1,
			// The serving tenants not healthy: what the overview's "5 / 6
			// tenants healthy" leaves out.
			"unhealthy":   !row.Archived() && !row.Spec.Suspended && row.State.Kind != "ok",
			"unprotected": row.Unprotected(clustered) && !row.Spec.Suspended,
			"suspended":   row.Spec.Suspended && !row.Archived(),
			"archived":    row.Archived(),
		}
		for name, ok := range in {
			if ok {
				counts[name]++
			}
		}
		text := strings.ToLower(strings.Join([]string{row.Spec.Name, row.App, row.Node, row.Tenant.Replica}, " "))
		if in[show] && (query == "" || strings.Contains(text, query)) {
			out = append(out, row)
		}
	}
	if !byName {
		rank := func(row tenantRow) int {
			if r, ok := attention[row.Spec.Name]; ok {
				return r
			}
			return max(2, row.rank(clustered))
		}
		slices.SortStableFunc(out, func(a, b tenantRow) int { return rank(a) - rank(b) })
	}
	return out, counts
}

// activityEvents is how many of the newest events the activity charts
// read: the store's most.
const activityEvents = 1000

type tenantData struct {
	Tenant         api.Tenant
	State, Replica tag
	// Row is the tenant as the overview's table has it, for its copy.
	Row    tenantRow
	Events []store.Event
	// Activity counts its events of the last fortnight, by day.
	Activity activity
	// Usage is its last day, drawn; nil if it was not measured.
	Usage *tenantUsage
	// StartingNow says its new container starts: the old one's figures
	// are not shown as its own.
	StartingNow bool
	// Outcome is a person's change of its app that reached it, lately.
	Outcome *outcome
	// Evidence says the problem whole, sentence by sentence; Exited that
	// its last exit is known, and with it maybe its last lines.
	Evidence []string
	Exited   bool
	// Problem says what is wrong with it, if anything; Broken whether it
	// does not serve; MemoryHigh whether it nears its memory limit.
	Problem    string
	Broken     bool
	MemoryHigh bool
	Env        []envVar
	// ErrorsBad says its errors are past errorShare of its requests.
	ErrorsBad bool
	// BackupsKnown says the cluster said how its backups go.
	BackupsKnown bool
	// Rollout is its app's rollout under way, and Place its own row in it.
	Rollout *rolloutView
	Place   *rolloutRow
	// Unchanged says why the rollout leaves it as it runs, when it does.
	Unchanged string
	// DiskFull says its data fills its disk limit ("cannot write: its
	// data, 1.4 GB, fills its 1 GB disk limit"), "" if not.
	DiskFull string
	// DiskRefused is the disk limit its node did not set ("1 GB"), one
	// below its data, which ZFS refuses; Kept the limit its data has.
	DiskRefused, DiskKept string
	// Limits are the limits it runs with, as every page says them;
	// Unlimited names those it runs without that others have ("disk"), and
	// RiskField the settings row of the first.
	Limits    []limitInEffect
	Unlimited string
	RiskField string
	// RiskOthers is the value the other tenants have of the one limit it
	// runs without ("10 GB").
	RiskOthers string
	// AppMemory says its app's tenants run out of memory, if they do.
	AppMemory *appMemory
	// Operation is what a person did to it lately, and how it went; Me is
	// who looks, as the audit log names them.
	Operation *operation
	Me        string
	Head      tenantHead
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
	var cluster clusterUse
	if err := all(
		// An archived tenant is measured no more: it has no day to show.
		func() error { use = s.tenantUsage(ctx, name, r.URL.Query().Get("range")); return nil },
		func() error { cluster = s.usageNow(ctx); return nil },
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
	data := tenantData{Tenant: t, State: tenantState(t), Replica: replicaState(t, len(nodes.Nodes) > 1), Events: shownEvents(events, "", 20),
		Activity: activityOf(events, s.now(), 14*24*time.Hour, 24*time.Hour, len(events) < activityEvents, dayLabel), Usage: use}
	data.Row = tenantRow{Tenant: t, State: data.State, Replica: data.Replica}
	if b, ok := cluster.Backups[name]; ok {
		b.Stale = b.Stale && !t.Archived()
		data.Row.Backup = &b
	}
	data.BackupsKnown = cluster.Measured
	if !t.Archived() && !t.Spec.Suspended {
		var problem string
		problem, data.Broken = tenantProblem(t, data.State)
		var used, limit string
		if use != nil && use.Memory.MemoryLimit != "" && use.Memory.Memory.Width >= memoryWarn {
			data.MemoryHigh = true
			used, limit = use.Memory.MemoryText, use.Memory.MemoryLimit
		}
		switch {
		case problem != "":
			head, whole := tenantHeadline(t, data.State, used, limit, s.now())
			data.Problem, data.Evidence = "It"+head+".", whole
		case data.MemoryHigh:
			data.Problem = "It uses " + usedOf(used, limit) + " of memory."
			data.Evidence = []string{"The kernel kills it at the limit: it would restart, and lose what it held in memory."}
		}
		if _, ok := parseExit(statusExit(t)); ok && data.Broken {
			data.Exited = true
		}
	}
	for key, value := range t.Spec.Env {
		data.Env = append(data.Env, envVar{key, value})
	}
	slices.SortFunc(data.Env, func(a, b envVar) int { return strings.Compare(a.Key, b.Key) })
	if use != nil {
		data.ErrorsBad = use.ErrorsBad
	}
	// Its limits, said even when the other tenants could not be read.
	data.Limits = limitsOf(t, map[string]bool{})
	s.appContext(ctx, r, t, &data)
	data.Operation = operationOf(t, events, s.now())
	data.Me = s.actor(userOf(r))
	// After another app, until it runs healthy: who wrote its data, as its
	// node read it, when that is not the user it now runs as.
	if op := data.Operation; op != nil && op.From != "" && op.Kind != "ok" && t.Status != nil {
		if read := t.Status.DataOwner; read != "" && t.Spec.User != "" && read != t.Spec.User && t.Spec.DataPath != "" {
			op.Owner = t.App + " runs as " + t.Spec.User + ", but its data at " + t.Spec.DataPath + " was written as " + read + ": if it does not start, that is the likely cause."
		}
	}
	if use != nil {
		for i := range use.Charts {
			use.Charts[i].Marks = use.Charts[i].marksOf(events)
		}
	}
	// A container that cannot write is not "done": its data fills its
	// disk limit. The red notice says so, once; the operation line only
	// records what was done.
	if use != nil && use.Full != "" {
		data.DiskFull = use.Full
		if op := data.Operation; op != nil && op.Kind == "ok" {
			op.State, op.Kind = "applied", "warn"
			if op.Live {
				op.State = "applied, nothing restarted"
			}
		}
	}
	// A disk limit raised over the data that filled the one before: it
	// writes again, and nothing restarted.
	if op := data.Operation; op != nil && op.Live && op.Kind == "ok" && use != nil && data.DiskFull == "" && op.DiskFrom > 0 && int64(op.DiskFrom)<<30 <= use.DataUsed {
		op.State = "it can write again, nothing restarted"
	}
	// A disk limit its node did not set: it runs with the one it had.
	if st := t.Status; st != nil {
		if rest, ok := strings.CutPrefix(st.Warning, volume.ErrDiskLimit.Error()+": "); ok {
			data.DiskRefused, _, _ = strings.Cut(rest, ": ")
			data.DiskKept = "none"
			if use != nil && use.DataLimit != "" {
				data.DiskKept = use.DataLimit
			}
		}
	}
	// While the new container a person asked for starts, the operation
	// line says it all: what was wrong was of the old one.
	if op := data.Operation; op != nil && op.Kind == "busy" {
		data.Problem, data.Broken, data.AppMemory, data.Evidence = "", false, nil, nil
	}
	if st := t.Status; st != nil && !t.Spec.Suspended && st.Phase == "starting" {
		data.StartingNow = true
		// The charts' last values are the old container's.
		if use != nil {
			for i := range use.Charts {
				if !use.Charts[i].LastAt.IsZero() {
					use.Charts[i].NowSaid = "previous container, at " + use.Charts[i].LastAt.UTC().Format("15:04") + ":"
				}
			}
		}
	}
	// A tenant a rollout just moved, starting: its rollout line says so in
	// a word ("Starting: it moved…"), not a notice of its own.
	if data.StartingNow && !data.Broken && data.Place != nil && data.Place.Moved && strings.HasPrefix(data.Problem, "It is starting") {
		data.Problem = ""
	}
	data.Head = s.withApps(ctx, r, s.headOf(t, nodes, "overview"))
	data.Head.Back = r.URL.RequestURI()
	if data.DiskFull != "" && data.Head.State.Kind == "ok" {
		data.State, data.Head.State = tag{"cannot write", "bad"}, tag{"cannot write", "bad"}
	}
	p := s.pageFor(w, r, t.Spec.Name, "tenants", nodes, data)
	// What the action did is said by the operation line, not twice.
	if data.Operation != nil && s.now().Sub(data.Operation.Event.At) < time.Minute {
		p.Done = ""
	}
	s.write(w, "tenant", p, http.StatusOK)
}

// Link is the events page's address with some of its filters changed,
// the others kept: pairs of a name (tenant, app, show, since, until,
// before) and its value, an empty value dropping it.
func (d eventsData) Link(pairs ...string) string {
	values := url.Values{}
	set := func(name, value string) {
		if value == "" {
			values.Del(name)
		} else {
			values.Set(name, value)
		}
	}
	set("tenant", d.Tenant)
	set("app", d.App)
	set("show", d.Show)
	if !d.Since.IsZero() {
		set("since", d.Since.UTC().Format(time.RFC3339))
	}
	if !d.Until.IsZero() {
		set("until", d.Until.UTC().Format(time.RFC3339))
	}
	for i := 0; i+1 < len(pairs); i += 2 {
		set(pairs[i], pairs[i+1])
	}
	if len(values) == 0 {
		return "/events"
	}
	return "/events?" + values.Encode()
}

type eventsData struct {
	// Since and Until bound the events shown, when a window was asked for.
	Since, Until time.Time
	Events       []store.Event
	Tenant, App  string
	// Show is what is shown: "" for what people did and failures, people
	// for what people did only, all for the cluster's own records too.
	Show string
	// Tenants and Apps are the names the filters offer.
	Tenants, Apps []string
	// Older is the ID to page back from; zero when there are no more.
	Older int64
	// Hidden counts the cluster's own records left out of those read.
	Hidden int
}

const eventsPage = 100

// eventShown says whether an event is shown: show is "" for what people
// did and what broke, people for what people did, all for every event.
func eventShown(e store.Event, show string) bool {
	switch show {
	case "all":
		return true
	case "people":
		return byPeople(e)
	}
	return byPeople(e) || failureEvent(e.Action) || e.Action == "app.rolled-out"
}

// shownEvents keeps the events show lets through, at most n.
func shownEvents(events []store.Event, show string, n int) []store.Event {
	var out []store.Event
	for _, e := range events {
		if eventShown(e, show) {
			out = append(out, e)
			if len(out) == n {
				break
			}
		}
	}
	return out
}

func (s *Server) events(w http.ResponseWriter, r *http.Request) {
	q := r.URL.Query()
	query := store.EventQuery{Tenant: q.Get("tenant"), App: q.Get("app"), Limit: eventsPage}
	if before, err := strconv.ParseInt(q.Get("before"), 10, 64); err == nil && before > 0 {
		query.Before = before
	}
	show := q.Get("show")
	switch {
	case q.Get("all") != "":
		show = "all"
	case q.Get("people") != "":
		show = "people"
	case show != "all" && show != "people":
		show = ""
	}
	// A window of time, as a job's page links to what it changed.
	since, _ := time.Parse(time.RFC3339, q.Get("since"))
	until, _ := time.Parse(time.RFC3339, q.Get("until"))
	if show != "all" || !since.IsZero() || !until.IsZero() {
		// What is shown is found among more of the cluster's own.
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
	data := eventsData{Tenant: query.Tenant, App: query.App, Show: show}
	apps := map[string]bool{}
	for _, t := range tenants {
		data.Tenants = append(data.Tenants, t.Spec.Name)
		apps[t.App] = true
	}
	slices.Sort(data.Tenants)
	data.Apps = slices.Sorted(maps.Keys(apps))
	if !since.IsZero() || !until.IsZero() {
		events = slices.DeleteFunc(events, func(e store.Event) bool {
			return (!since.IsZero() && e.At.Before(since)) || (!until.IsZero() && e.At.After(until))
		})
		data.Since, data.Until = since, until
	}
	read := events
	if show != "all" {
		events = slices.DeleteFunc(slices.Clone(events), func(e store.Event) bool { return !eventShown(e, show) })
		data.Hidden = len(read) - len(events)
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
		"failure":  failureEvent,
		"nodeActs": func(n api.NodeState, self bool, targets []string, all []api.Tenant, from string) nodeActs {
			return drainOf(nodeActs{Name: n.Name, Answers: n.Answers, Failed: n.Failed(), Self: self, Targets: targets, From: from}, all)
		},
		"every":      every,
		"imageRepo":  func(ref string) string { r, _ := ImageParts(ref); return r },
		"imageShort": func(ref string) string { _, short := ImageParts(ref); return short },
		"breakable":  breakable,
		"names":      keepNames,
		"waveSaid":   store.WaveSaid,
		"blocked":    rolloutBlocked,
		"setLimit":   setLimit,
		// A tenant's pages say what an action did under their header, so
		// the header never moves between states of one page.
		"headFirst": func(d any) bool {
			switch d.(type) {
			case tenantData, *tenantData, settingsData, *settingsData, logsData, *logsData:
				return true
			}
			return false
		},
		"marksJSON": func(marks []mark) string { return jsonOf(marks) },
		"groups":    groupEvents,
		"lower":     strings.ToLower,
		"wavename":  waveName,
		"sub":       func(a, b int) int { return a - b },
		"others": func(n api.Nodes) string {
			var out []string
			for _, node := range n.Nodes {
				if node.Name != n.Self && node.Answers && !node.Failed() {
					text := node.Name
					if node.Tunnel != nil {
						text += " (" + node.Tunnel.Address.String() + ")"
					}
					out = append(out, text)
				}
			}
			return strings.Join(out, ", ")
		},
		"raise":       raiseMemory,
		"add":         func(a, b int) int { return a + b },
		"next":        func(n int64) int64 { return n + 1 },
		"list":        func(items ...string) []string { return items },
		"passkeyName": passkeyName,
		"sortName": func(byName bool) string {
			if byName {
				return "name"
			}
			return ""
		},
		"filterLabel": func(name string) string {
			return map[string]string{"all": "All", "broken": "Broken", "risk": "At risk", "attention": "Needs attention", "unhealthy": "Not healthy", "unprotected": "Without a ready copy", "suspended": "Suspended", "archived": "Archived"}[name]
		},
		"node":   nodeHref,
		"plural": plural,
		"utc": func(t time.Time) string {
			if t.IsZero() {
				return "never"
			}
			return t.UTC().Format("2006-01-02 15:04:05") + " UTC"
		},
		"iso":   func(t time.Time) string { return t.UTC().Format(time.RFC3339) },
		"query": func(args ...string) template.URL { return tenantsQuery(args...) },
		"mem":   memory,
		"lines": func(text string) int { return strings.Count(text, "\n") + 1 },
		"ago":   func(t time.Time) string { return ago(s.now(), t) },
		"who":   who,
		// superseded says a failed job was run again since, and that run
		// ended well: its failure is past.
		"superseded": func(job *Job, all []*Job) bool {
			for _, other := range all {
				if other != job && other.Title == job.Title && other.Started.After(job.Started) && other.Done() && other.Failed == "" && !other.Interrupted {
					return true
				}
			}
			return false
		},
		"repeat": func(n int) []int { return make([]int, n) },
		// joins says a review's tenant takes the revision without a new
		// container: it runs it already.
		"joins": func(d reviewData, name string) bool {
			return slices.ContainsFunc(d.Join, func(t reviewTenant) bool { return t.Name == name })
		},
		"envshow": envValue,
		"causeOf": func(b interface{ Cause() (string, string) }) []string {
			cause, fix := b.Cause()
			return []string{cause, fix}
		},
		"same": sameActor,
		// exitsaid says the agent's words for an exit as people do.
		"exitsaid": func(text string) string {
			if e, ok := parseExit(text); ok {
				return e.Said(s.now())
			}
			return text
		},
		"stamp": func(t time.Time) string {
			if t.IsZero() {
				return "never"
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
				return "none"
			}
			return strings.Join(out, " ")
		},
		"limit": func(v any) string {
			switch v := v.(type) {
			case int:
				if v == 0 {
					return "none"
				}
				return strconv.Itoa(v)
			case float64:
				if v == 0 {
					return "none"
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
		return "never"
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
// imageDigest says an image as the panel does everywhere: its digest's
// first 12, "sha256:c23c323553d0".
func imageDigest(ref string) string {
	_, short := ImageParts(ref)
	return short
}

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

// tenantHead is the head of a tenant's pages, the same on each: its name
// and state, its pages, what can be done to it, and where it runs.
type tenantHead struct {
	Tenant api.Tenant
	State  tag
	// On is the page shown: overview, logs or settings.
	On string
	// Acts and Edits say what the person may do; Targets are the nodes the
	// tenant may move to.
	Acts, Edits bool
	Targets     []string
	// Apps are the other apps it could run, for "Change its app".
	Apps []string
	// OutOfMemory says it last exited out of memory and is not healthy:
	// moving it keeps its limit, so the move is no fix.
	OutOfMemory bool
	// Back is the address of the page it heads, for its forms to return
	// to (checked again as they are sent: tenantBack).
	Back string
}

// withApps adds the apps the tenant could change to, for its menu.
func (s *Server) withApps(ctx context.Context, r *http.Request, h tenantHead) tenantHead {
	if s.Apps == nil || h.Tenant.Archived() {
		return h
	}
	apps, err := s.Apps(s.actor(userOf(r))).Apps(ctx)
	if err != nil {
		return h
	}
	for _, app := range apps {
		if app.Name != h.Tenant.App {
			h.Apps = append(h.Apps, app.Name)
		}
	}
	slices.Sort(h.Apps)
	return h
}

func (s *Server) headOf(t api.Tenant, nodes api.Nodes, on string) tenantHead {
	h := tenantHead{Tenant: t, State: tenantState(t), On: on, Acts: s.Actions != nil, Edits: s.Apps != nil}
	if st := t.Status; st != nil && !st.Healthy {
		if e, ok := parseExit(st.LastExit); ok && e.Memory {
			h.OutOfMemory = true
		}
	}
	for _, node := range nodes.Nodes {
		if node.Name != t.Node && node.Answers && !node.Failed() {
			h.Targets = append(h.Targets, node.Name)
		}
	}
	return h
}

// nodeActs is what can be done to a node, for its menu: on the cluster
// page, or its own (From "node").
type nodeActs struct {
	Name                  string
	Answers, Failed, Self bool
	// Targets are the nodes to drain it to; Tenants those it runs.
	Targets, Tenants []string
	From             string
	// Keeps counts the copies it keeps, Archived the archived tenants'
	// data it holds; Unprotected are, by target, the tenants a drain there
	// leaves without a ready copy on another node.
	Keeps, Archived int
	Unprotected     map[string][]string
}

// drainOf says what draining a node would do: the tenants it moves, and,
// for each node it could go to, those left without a copy elsewhere: they
// would run where their copy is, or have none.
func drainOf(a nodeActs, all []api.Tenant) nodeActs {
	a.Unprotected = map[string][]string{}
	slices.SortFunc(all, func(x, y api.Tenant) int { return strings.Compare(x.Spec.Name, y.Spec.Name) })
	for _, t := range all {
		switch {
		case t.Node == a.Name && t.Archived():
			a.Archived++
		case t.Node == a.Name:
			a.Tenants = append(a.Tenants, t.Spec.Name)
			for _, target := range a.Targets {
				if t.Replica == "" || t.Replica == target || !t.ReplicaReady {
					a.Unprotected[target] = append(a.Unprotected[target], t.Spec.Name)
				}
			}
		case t.Replica == a.Name && !t.Archived():
			a.Keeps++
		}
	}
	return a
}

// tenantsQuery is the tenants' page's address with a filter: pairs of a
// name and its value, the empty ones left out.
func tenantsQuery(pairs ...string) template.URL {
	values := url.Values{}
	for i := 0; i+1 < len(pairs); i += 2 {
		if pairs[i+1] != "" && !(pairs[i] == "show" && pairs[i+1] == "all") {
			values.Set(pairs[i], pairs[i+1])
		}
	}
	if len(values) == 0 {
		return "/tenants"
	}
	return template.URL("/tenants?" + values.Encode())
}

func hasKey2(m map[string]int, key string) bool {
	_, ok := m[key]
	return ok
}

// unchangedBy says why app's newest revision leaves tenant t as it runs:
// what the revision changed, and t's own settings that stay over it.
func unchangedBy(t api.Tenant, app api.App, revisions []store.Revision) string {
	var before *spec.Tenant
	for i := range revisions {
		if revisions[i].Revision == app.Revision-1 {
			before = &revisions[i].Spec
		}
	}
	lead := app.Name + "'s revision " + strconv.FormatInt(app.Revision, 10) + " changes nothing for " + t.Spec.Name
	if before == nil {
		return lead + ": its own settings already cover what changed, so it kept its container."
	}
	o := t.Overrides
	var own []string
	for _, d := range diffSpecs(before.WithDefaults(), app.Spec.WithDefaults()) {
		switch d.Key {
		case "memory":
			if o.Limits.MemoryMB != 0 {
				own = append(own, "memory ("+memory(o.Limits.MemoryMB)+")")
			}
		case "CPUs":
			if o.Limits.CPUs != 0 {
				own = append(own, "CPUs ("+strconv.FormatFloat(o.Limits.CPUs, 'f', -1, 64)+")")
			}
		case "processes":
			if o.Limits.PIDs != 0 {
				own = append(own, "processes ("+strconv.Itoa(o.Limits.PIDs)+")")
			}
		case "disk":
			if o.Limits.DiskGB != 0 {
				own = append(own, "disk ("+strconv.Itoa(o.Limits.DiskGB)+" GB)")
			}
		case "image":
			if o.Image != "" {
				own = append(own, "image ("+imageDigest(o.Image)+")")
			}
		default:
			if v, ok := o.Env[d.Key]; ok {
				own = append(own, d.Key+" ("+v+")")
			}
		}
	}
	if len(own) == 0 {
		return lead + ": its own settings already cover what changed, so it kept its container."
	}
	return lead + ": its own " + strings.Join(own, ", ") + " stays over what it changed, so it kept its container."
}

// digestText is a digest as it is written in an event's detail.
var digestText = regexp.MustCompile(`sha256:[0-9a-f]+`)

// breakable says a detail so a narrow line breaks before an image's
// digest ("…/web" | "@sha256:…"), never inside it.
func breakable(text string) template.HTML {
	escaped := template.HTMLEscapeString(text)
	escaped = digestText.ReplaceAllString(escaped, `<span class="nowrap">$0</span>`)
	return template.HTML(strings.ReplaceAll(escaped, "@", "<wbr>@"))
}

// nodeName is a node's or tenant's name in running text: "fsn-1",
// "acme-logistics".
var nodeName = regexp.MustCompile(`\b[a-z][a-z0-9]*(?:-[a-z0-9]+)+\b`)

// sizeSaid is a figure with its unit: "1.4 GB", "256 MB", "3 GB".
var sizeSaid = regexp.MustCompile(`\b(\d+(?:\.\d+)?) (B|KB|MB|GB|TB|cores?)\b`)

// keepNames says text with each such name kept whole on one line, and
// each figure with its unit: a narrow cell or a phone's line breaks around
// "fsn-1" and "1.4 GB", never inside them.
func keepNames(text string) template.HTML {
	kept := nodeName.ReplaceAllString(template.HTMLEscapeString(text), `<span class="nowrap">$0</span>`)
	return template.HTML(sizeSaid.ReplaceAllString(kept, "$1\u00a0$2"))
}
