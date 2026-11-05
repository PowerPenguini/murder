package panel

import (
	"strconv"

	"github.com/PowerPenguini/murder/internal/api"
	"github.com/PowerPenguini/murder/internal/reconcile"
	"github.com/PowerPenguini/murder/internal/spec"
)

// Where a limit in effect comes from.
const (
	fromOwn     = "own"     // the tenant's override
	fromApp     = "app"     // its app's spec
	fromNode    = "node"    // its node's default
	fromNone    = "none"    // nothing: no override, no app value, no default
	fromUnknown = "unknown" // the spec sets none, and its node has not said its default
)

// limitInEffect is one limit as a tenant's container runs with it: the one
// account of it every page gives (settings, reviews, its Container section,
// its risks), so they never disagree.
type limitInEffect struct {
	// Key names it in sentences ("memory", "CPU", "process", "disk"); Field
	// is the settings form's field and its row's id; Label its row label.
	Key, Field, Label string
	// Value is its value in its unit (0 none); Said says it ("768 MB").
	Value float64
	Said  string
	// From is where it comes from; App and Node name the app and the node.
	From, App, Node string
	// Risk says it runs without it while other running tenants have one.
	Risk bool
}

// limitDefs are the four limits, in the order every page lists them.
var limitDefs = []struct {
	key, field, label string
	of                func(spec.Limits) float64
	said              func(float64) string
}{
	{"memory", "memoryMB", "Memory", func(l spec.Limits) float64 { return float64(l.MemoryMB) }, func(v float64) string { return memory(int(v)) }},
	{"CPU", "cpus", "CPUs", func(l spec.Limits) float64 { return l.CPUs }, func(v float64) string { return strconv.FormatFloat(v, 'f', -1, 64) }},
	{"process", "pids", "Processes", func(l spec.Limits) float64 { return float64(l.PIDs) }, func(v float64) string { return strconv.Itoa(int(v)) }},
	{"disk", "diskGB", "Disk", func(l spec.Limits) float64 { return float64(l.DiskGB) }, func(v float64) string { return strconv.Itoa(int(v)) + " GB" }},
}

// limitsInEffect says each limit a tenant runs with, and where it comes
// from: composed is its spec's limits (its app's with its own over them),
// own its overrides', app the app's name; status is its node's word on
// its defaults (nil, or none said: not known), node that node's name.
// limited names the limits ("memory", "CPU", ...) some running tenant
// has; one it runs without is a risk (nil: any limit it runs without).
func limitsInEffect(composed, own spec.Limits, app string, status *reconcile.Status, node string, limited map[string]bool) []limitInEffect {
	knows := status != nil && (status.Defaults != (spec.Limits{}) || status.Limits != (spec.Limits{}))
	var out []limitInEffect
	for _, d := range limitDefs {
		l := limitInEffect{Key: d.key, Field: d.field, Label: d.label, App: app, Node: node}
		switch {
		case d.of(own) > 0:
			l.Value, l.From = d.of(own), fromOwn
		case d.of(composed) > 0:
			l.Value, l.From = d.of(composed), fromApp
		case !knows:
			l.From = fromUnknown
		case d.of(status.Defaults) > 0:
			l.Value, l.From = d.of(status.Defaults), fromNode
		default:
			l.From = fromNone
			l.Risk = limited == nil || limited[d.key]
		}
		if l.Value > 0 {
			l.Said = d.said(l.Value)
		}
		out = append(out, l)
	}
	return out
}

// limitsOf says the limits tenant t runs with now.
func limitsOf(t api.Tenant, limited map[string]bool) []limitInEffect {
	return limitsInEffect(t.Spec.Limits, t.Overrides.Limits, t.App, t.Status, t.Node, limited)
}

// Source says where it comes from, as the settings' Source column does.
func (l limitInEffect) Source() string {
	switch l.From {
	case fromOwn:
		return "Tenant override"
	case fromApp:
		return "Inherited from " + l.App
	case fromNode:
		return "Node default (" + l.Node + ")"
	case fromNone:
		return "None: " + l.Node + " has no default"
	}
	return "Node default, not known: " + l.Node + " has not said"
}

// Value says it with where it comes from when that is not the tenant's
// own or its app's, as a review's cell and its Container section do:
// "768 MB", "1 (node default on fsn-1)", "no limit (no default on fsn-1)".
func (l limitInEffect) InEffect() string {
	switch l.From {
	case fromNode:
		return l.Said + " (node default on " + l.Node + ")"
	case fromNone:
		return "no limit (no default on " + l.Node + ")"
	case fromUnknown:
		return "the node's default (not known)"
	}
	return l.Said
}

// Short says it for a figure: its value, or "none", or "not known".
func (l limitInEffect) Short() string {
	switch l.From {
	case fromNone:
		return "none"
	case fromUnknown:
		return "not known"
	}
	return l.Said
}

// runningLimits names the limits some running tenant of tenants has, as
// their nodes say them.
func runningLimits(tenants []api.Tenant) map[string]bool {
	limited := map[string]bool{}
	for _, t := range tenants {
		if t.Archived() || t.Spec.Suspended {
			continue
		}
		for _, l := range limitsOf(t, map[string]bool{}) {
			if l.Value > 0 {
				limited[l.Key] = true
			}
		}
	}
	return limited
}

// risks names the limits t runs without that others have: "disk", "CPU
// and disk"; and the field of the first, for a link to set it.
func risks(limits []limitInEffect) (keys []string, field string) {
	for _, l := range limits {
		if l.Risk {
			keys = append(keys, l.Key)
			if field == "" {
				field = l.Field
			}
		}
	}
	return keys, field
}

// Heading names it in a tenant's Container section.
func (l limitInEffect) Heading() string {
	return map[string]string{"memory": "Memory limit", "CPU": "CPU limit", "process": "Processes, most", "disk": "Disk limit"}[l.Key]
}
