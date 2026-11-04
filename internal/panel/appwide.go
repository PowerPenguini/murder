package panel

import (
	"maps"
	"net/url"
	"slices"
	"strconv"
	"strings"
	"time"

	"github.com/PowerPenguini/murder/internal/api"
	"github.com/PowerPenguini/murder/internal/store"
)

// When the same failure hits several tenants of one app, the cause is
// most likely the app's, not each tenant's: the panel says so where each
// of them shows, and offers the app-wide fix beside the tenant's own.

// oomSpan is how far back exits out of memory are counted.
const oomSpan = 24 * time.Hour

// oomShare is how many of an app's tenants run out of memory before it is
// said to be the app's: at least this many, and more than one.
const oomTenants = 3

// outOfMemory says an event tells of a container killed out of memory.
func outOfMemory(e store.Event) bool {
	return e.Action == "tenant.restarted" && (strings.Contains(e.Detail, "137") || strings.Contains(e.Detail, "out of memory"))
}

// appMemory is an app whose tenants run out of memory, said from the
// tenants as they are configured now: which use the app's limit, which
// keep their own, and which a change of the app's limit would reach.
type appMemory struct {
	App string
	// Hit are the tenants that use the app's limit and ran out of memory in
	// oomSpan, by name, with how many times; Inherit counts the live
	// tenants that use the app's limit.
	Hit     map[string]int
	Inherit int
	// Own are the tenants that keep a limit of their own, by name, with it
	// in MB; OwnHit those of them that ran out of memory too.
	Own    map[string]int
	OwnHit []string
	// Reaches are the tenants a change of the app's limit reaches now;
	// Held those it reaches only once released, Suspended once resumed.
	Reaches, Held, Suspended []string
	// LimitMB is the app's memory limit; ProposeMB twice it, at least 512.
	LimitMB, ProposeMB int
	// Bad says one of the tenants it explains is broken now: the same red
	// everywhere the problem shows.
	Bad bool
}

// Text says it, as a sentence after the app's name: the counts are of the
// tenants that use the app's limit, which is what its change would touch.
func (m appMemory) Text() string {
	text := ": " + strconv.Itoa(len(m.Hit)) + " of the " + plural(m.Inherit, "tenant", "tenants") + " on its " + memory(m.LimitMB) + " limit ran out of memory in the last 24 hours"
	if len(m.Hit) == m.Inherit {
		text = ": all " + plural(m.Inherit, "tenant", "tenants") + " on its " + memory(m.LimitMB) + " limit ran out of memory in the last 24 hours"
	}
	return text
}

// Cause says it in one line, under the tenant it breaks: "Likely web's
// 256 MB limit: 5 of its 6 tenants on it ran out of memory in 24 h."
func (m appMemory) Cause() string {
	return "Likely " + m.App + "'s " + memory(m.LimitMB) + " limit: " + strconv.Itoa(len(m.Hit)) + " of the " + plural(m.Inherit, "tenant", "tenants") + " on it ran out of memory in 24 h."
}

// Evidence says it whole: who ran out of memory, who its fix reaches, who
// it does not.
func (m appMemory) Evidence() string {
	text := m.App + "'s limit is likely too low" + m.Text() + ": " + strings.Join(m.HitNames(), ", ") + ". Exit 137 is a likely memory kill, not a proof."
	text += " Raising it to " + memory(m.ProposeMB) + " reaches " + plural(len(m.Reaches), "tenant", "tenants") + " now and may stop these restarts."
	if ex := m.Exceptions(); ex != "" {
		text += " Not every tenant: " + ex + "."
	}
	return text
}

// Exceptions says the tenants a change of the app's limit does not reach,
// or not now: "atlas keeps its own 768 MB; test-staging is held".
func (m appMemory) Exceptions() string {
	var parts []string
	for _, name := range slices.Sorted(maps.Keys(m.Own)) {
		part := name + " keeps its own " + memory(m.Own[name])
		if slices.Contains(m.OwnHit, name) {
			part += " (and ran out of it too)"
		}
		parts = append(parts, part)
	}
	for _, name := range m.Held {
		parts = append(parts, name+" is held on its revision")
	}
	for _, name := range m.Suspended {
		parts = append(parts, name+" is suspended, and takes it when resumed")
	}
	return strings.Join(parts, "; ")
}

// Href is where its fix is reviewed: the app's spec with twice its limit.
func (m appMemory) Href() string {
	return "/apps/" + url.PathEscape(m.App) + "?memoryMB=" + strconv.Itoa(m.ProposeMB) + "#spec"
}

// appMemoryOf is the app's tenants that ran out of memory since now
// less oomSpan at the limit in effect, if enough of those on the app's
// limit did to say the limit is likely too low. Exit 137 is a likely memory kill, not a proof: the
// panel proposes, it does not promise.
func appMemoryOf(app api.App, tenants []api.Tenant, events []store.Event, now time.Time) *appMemory {
	m := appMemory{App: app.Name, Hit: map[string]int{}, Own: map[string]int{}, LimitMB: app.Spec.Limits.MemoryMB}
	if m.LimitMB == 0 {
		return nil
	}
	inherits, owns := map[string]bool{}, map[string]bool{}
	broken := map[string]bool{}
	slices.SortFunc(tenants, func(a, b api.Tenant) int { return strings.Compare(a.Spec.Name, b.Spec.Name) })
	for _, t := range tenants {
		if t.App != app.Name || t.Archived() {
			continue
		}
		if mb := t.Overrides.Limits.MemoryMB; mb != 0 {
			m.Own[t.Spec.Name] = mb
			owns[t.Spec.Name] = true
			continue
		}
		inherits[t.Spec.Name] = true
		m.Inherit++
		switch {
		case t.Hold:
			m.Held = append(m.Held, t.Spec.Name)
		case t.Spec.Suspended:
			m.Suspended = append(m.Suspended, t.Spec.Name)
		default:
			m.Reaches = append(m.Reaches, t.Spec.Name)
		}
		if _, bad := tenantProblem(t, tenantState(t)); bad && !t.Spec.Suspended {
			broken[t.Spec.Name] = true
		}
	}
	for _, e := range events {
		if !e.At.After(now.Add(-oomSpan)) || !outOfMemory(e) || (e.App != "" && e.App != app.Name) {
			continue
		}
		// Only kills at the limit in effect count: once it is raised,
		// those at the old one say nothing of the new. An exit that does
		// not say its limit counts.
		ran, _ := parseExit(e.Detail)
		switch {
		case inherits[e.Tenant] && (ran.LimitMB == 0 || ran.LimitMB == m.LimitMB):
			m.Hit[e.Tenant]++
		case owns[e.Tenant] && (ran.LimitMB == 0 || ran.LimitMB == m.Own[e.Tenant]) && !slices.Contains(m.OwnHit, e.Tenant):
			m.OwnHit = append(m.OwnHit, e.Tenant)
		}
	}
	if len(m.Hit) < oomTenants || m.Inherit < 2 {
		return nil
	}
	for name := range m.Hit {
		m.Bad = m.Bad || broken[name]
	}
	slices.Sort(m.OwnHit)
	m.ProposeMB = max(2*m.LimitMB, 512)
	return &m
}

// appMemories are the apps whose tenants run out of memory, by name.
func appMemories(apps []api.App, tenants []api.Tenant, events []store.Event, now time.Time) map[string]*appMemory {
	out := map[string]*appMemory{}
	for _, app := range apps {
		if m := appMemoryOf(app, tenants, events, now); m != nil {
			out[app.Name] = m
		}
	}
	return out
}

// HitNames are the tenants that ran out of memory, by name.
func (m appMemory) HitNames() []string {
	var names []string
	for name := range m.Hit {
		names = append(names, name)
	}
	slices.Sort(names)
	return names
}

// exitsBefore counts a tenant's exits out of memory in the day before at.
func exitsBefore(events []store.Event, tenant string, at time.Time) int {
	n := 0
	for _, e := range events {
		if e.Tenant == tenant && outOfMemory(e) && e.At.Before(at) && e.At.After(at.Add(-oomSpan)) {
			n++
		}
	}
	return n
}

// operation is what a person did to a tenant lately, and how it went.
type operation struct {
	Event store.Event
	// Said says it as done ("Restarted"); State how it goes, Kind its tag.
	Said, State, Kind string
}

// operations are the actions whose outcome a tenant's page follows.
var operations = map[string]string{
	"tenant.rolled": "Restarted", "tenant.moved": "Moved", "tenant.updated": "Settings saved", "tenant.resumed": "Resumed",
	"tenant.restored": "Restored", "secret.rotated": "Secret given a new value",
}

// operationSpan is how long after an action its outcome is followed.
const operationSpan = 15 * time.Minute

// operationOf is the latest action a person took on t, and how it went:
// whether its new container runs, healthy.
func operationOf(t api.Tenant, events []store.Event, now time.Time) *operation {
	for _, e := range events {
		if now.Sub(e.At) > operationSpan {
			return nil
		}
		said, ok := operations[e.Action]
		if !ok || !byPeople(e) || e.Tenant != t.Spec.Name {
			continue
		}
		op := &operation{Event: e, Said: said}
		if e.Action == "tenant.moved" && t.Node != "" {
			op.Said = "Moved to " + t.Node
		}
		status := t.Status
		switch {
		case t.Archived():
			op.State, op.Kind = "archived", "mute"
		case status == nil:
			op.State, op.Kind = "waiting for its node to say how it runs", "warn"
		case status.Phase == "starting" || (status.Generation != 0 && status.Generation < t.Generation):
			op.State, op.Kind = "its new container is starting", "busy"
		case status.Phase == "running" && status.Healthy:
			op.State, op.Kind = "done: it runs healthy", "ok"
		case status.Phase == "running":
			op.State, op.Kind = "it runs, but is not healthy", "bad"
			// The same kill after the restart: restarting did not help.
			if e, ok := parseExit(status.LastExit); ok && e.Memory && e.At.After(op.Event.At) {
				op.State = "restarting did not help: it ran out of memory again" + e.when(now)
			} else if ok {
				op.State += "; its last " + e.Reason(now)
			}
		case status.Phase == "failed":
			op.State, op.Kind = "it failed", "bad"
			if status.Message != "" {
				op.State += ": " + status.Message
			}
		default:
			op.State, op.Kind = "it is "+status.Phase, "busy"
		}
		return op
	}
	return nil
}
