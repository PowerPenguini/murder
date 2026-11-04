package panel

import (
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

// appMemory is an app whose tenants run out of memory.
type appMemory struct {
	App string
	// Hit are the tenants that ran out of memory in oomSpan, by name, with
	// how many times; Live counts the app's live tenants.
	Hit  map[string]int
	Live int
	// LimitMB is the app's memory limit; Propose twice it.
	LimitMB, ProposeMB int
}

// Text says it, as a sentence after the app's name.
func (m appMemory) Text() string {
	text := ": " + strconv.Itoa(len(m.Hit)) + " of its " + plural(m.Live, "tenant", "tenants") + " ran out of memory in the last 24 hours"
	if len(m.Hit) == m.Live {
		text = ": all " + plural(m.Live, "tenant", "tenants") + " ran out of memory in the last 24 hours"
	}
	if m.LimitMB > 0 {
		text += " (limit " + memory(m.LimitMB) + ")"
	}
	return text
}

// Href is where its fix is reviewed: the app's spec with twice its limit.
func (m appMemory) Href() string {
	return "/apps/" + url.PathEscape(m.App) + "?memoryMB=" + strconv.Itoa(m.ProposeMB) + "#spec"
}

// appMemoryOf is the app's tenants that ran out of memory since now
// less oomSpan, if enough did to say it is the app's.
func appMemoryOf(app api.App, tenants []api.Tenant, events []store.Event, now time.Time) *appMemory {
	m := appMemory{App: app.Name, Hit: map[string]int{}, LimitMB: app.Spec.Limits.MemoryMB}
	live := map[string]bool{}
	for _, t := range tenants {
		if t.App == app.Name && !t.Archived() {
			m.Live++
			live[t.Spec.Name] = true
		}
	}
	for _, e := range events {
		if e.At.After(now.Add(-oomSpan)) && outOfMemory(e) && live[e.Tenant] && (e.App == "" || e.App == app.Name) {
			m.Hit[e.Tenant]++
		}
	}
	if len(m.Hit) < oomTenants || m.Live < 2 {
		return nil
	}
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
			if status.LastExit != "" {
				op.State += "; last exit: " + status.LastExit
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
