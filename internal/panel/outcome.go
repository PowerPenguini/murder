package panel

import (
	"slices"
	"strconv"
	"strings"
	"time"

	"github.com/PowerPenguini/murder/internal/api"
	"github.com/PowerPenguini/murder/internal/store"
)

// After a person changes an app, its page and each tenant it reached say
// whether the change held, in one line, until it ages out: how many it
// reached, how they run now, and whether any ran out of memory since.

// outcomeSpan is how long the line stays after the change.
const outcomeSpan = 30 * time.Minute

// outcome is what a person's change of an app did, as its tenants run now.
type outcome struct {
	App      string
	Revision int64
	// Change says what changed, "memory 256 MB → 512 MB"; empty if not known.
	Change string
	Event  store.Event
	// Reached are the tenants it reached; of them, Healthy run healthy,
	// Starting start, Bad do not run as they should, and Killed ran out of
	// memory since it reached them.
	Reached, Healthy, Starting, Bad, Killed []string
}

// Kind is its colour: bad when a tenant it reached is wrong, busy while
// one starts, ok once all run healthy.
func (o outcome) Kind() string {
	switch {
	case len(o.Bad) > 0 || len(o.Killed) > 0:
		return "bad"
	case len(o.Starting) > 0:
		return "busy"
	}
	return "ok"
}

// State says how its tenants run: "5 healthy; no memory kill since".
func (o outcome) State() string {
	var parts []string
	if n := len(o.Healthy); n > 0 {
		parts = append(parts, strconv.Itoa(n)+" healthy")
	}
	if n := len(o.Starting); n > 0 {
		parts = append(parts, strconv.Itoa(n)+" starting")
	}
	if len(o.Bad) > 0 {
		parts = append(parts, "not healthy: "+strings.Join(o.Bad, ", "))
	}
	text := strings.Join(parts, ", ")
	if len(o.Killed) > 0 {
		text += "; out of memory again since: " + strings.Join(o.Killed, ", ")
	} else {
		text += "; no memory kill since"
	}
	return text
}

// For says it of one tenant it reached: "healthy, no memory kill since".
func (o outcome) For(tenant string) string {
	state := "healthy"
	switch {
	case slices.Contains(o.Bad, tenant):
		state = "not healthy"
	case slices.Contains(o.Starting, tenant):
		state = "starting"
	}
	if slices.Contains(o.Killed, tenant) {
		return state + "; it ran out of memory again since"
	}
	return state + ", no memory kill since"
}

// KindFor is its colour for one tenant.
func (o outcome) KindFor(tenant string) string {
	switch {
	case slices.Contains(o.Bad, tenant) || slices.Contains(o.Killed, tenant):
		return "bad"
	case slices.Contains(o.Starting, tenant):
		return "busy"
	}
	return "ok"
}

// outcomeOf is the outcome of the newest change a person made of app in
// outcomeSpan, once no rollout of it runs (the rollout's own block says how
// one goes): nil when there is none, or it reached no tenant.
func outcomeOf(app api.App, revisions []store.Revision, tenants []api.Tenant, events []store.Event, now time.Time) *outcome {
	if app.Rollout != nil && app.Rollout.Active() {
		return nil
	}
	i := slices.IndexFunc(events, func(e store.Event) bool {
		return e.App == app.Name && e.Tenant == "" && e.Action == "app.updated" && byPeople(e)
	})
	if i < 0 || now.Sub(events[i].At) > outcomeSpan {
		return nil
	}
	o := &outcome{App: app.Name, Revision: app.Revision, Event: events[i]}
	var cur, prev *store.Revision
	for j := range revisions {
		switch revisions[j].Revision {
		case app.Revision:
			cur = &revisions[j]
		case app.Revision - 1:
			prev = &revisions[j]
		}
	}
	if cur != nil && prev != nil {
		var changes []string
		for _, d := range diffSpecs(prev.Spec.WithDefaults(), cur.Spec.WithDefaults()) {
			changes = append(changes, d.Text())
		}
		o.Change = strings.Join(changes, ", ")
	}
	// The tenants it reached, and when each moved.
	moved := map[string]time.Time{}
	for _, e := range events {
		if e.App == app.Name && e.Tenant != "" && e.Action == "app.rolled-out" && !e.At.Before(o.Event.At) {
			if at, ok := moved[e.Tenant]; !ok || e.At.Before(at) {
				moved[e.Tenant] = e.At
			}
		}
	}
	for _, t := range tenants {
		name := t.Spec.Name
		at, ok := moved[name]
		if !ok || t.App != app.Name || t.Archived() {
			continue
		}
		o.Reached = append(o.Reached, name)
		state := tenantState(t)
		switch {
		case state.Kind == "bad":
			o.Bad = append(o.Bad, name)
		case state.Kind == "ok":
			o.Healthy = append(o.Healthy, name)
		default:
			o.Starting = append(o.Starting, name)
		}
		for _, e := range events {
			if e.Tenant == name && outOfMemory(e) && e.At.After(at) {
				o.Killed = append(o.Killed, name)
				break
			}
		}
	}
	if len(o.Reached) == 0 {
		return nil
	}
	for _, list := range [][]string{o.Reached, o.Healthy, o.Starting, o.Bad, o.Killed} {
		slices.Sort(list)
	}
	return o
}
