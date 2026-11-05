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
	{"memory", "memoryMB", "Memory", func(l spec.Limits) float64 { return float64(l.MemoryMB) }, func(v float64) string { return exactMemory(int(v)) }},
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
		return "not known (" + l.Node + " has not said its default)"
	}
	return l.Said
}

// WithSource says it with where it comes from, always, as a settings
// review does: "768 MB, tenant override", "10 GB, inherited from web",
// "1 (node default on fsn-1)", "no limit (no default on fsn-1)".
func (l limitInEffect) WithSource() string {
	switch l.From {
	case fromOwn:
		return l.Said + ", tenant override"
	case fromApp:
		return l.Said + ", inherited from " + l.App
	}
	return l.InEffect()
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

// othersHave is the value of a limit ("disk", ...) most of the other
// running tenants run with, 0 if none has one: the larger on a tie.
func othersHave(t api.Tenant, tenants []api.Tenant, key string) float64 {
	count := map[float64]int{}
	for _, o := range tenants {
		if o.Spec.Name == t.Spec.Name || o.Archived() || o.Spec.Suspended {
			continue
		}
		for _, l := range limitsOf(o, map[string]bool{}) {
			if l.Key == key && l.Value > 0 {
				count[l.Value]++
			}
		}
	}
	best, most := 0.0, 0
	for v, n := range count {
		if n > most || n == most && v > best {
			best, most = v, n
		}
	}
	return best
}

// diskRoom is what a disk limit must leave a tenant: its data's size in
// bytes (-1 not measured), what the others have, and a value to suggest:
// what the others have, or twice its data, whichever is more.
type diskRoom struct {
	Used          int64
	Others, Least int
}

func diskRoomOf(t api.Tenant, tenants []api.Tenant, used int64) diskRoom {
	r := diskRoom{Used: used, Others: int(othersHave(t, tenants, "disk"))}
	if used > 0 {
		r.Least = int((2*used + (1 << 30) - 1) >> 30)
	}
	return r
}

// Suggest is a disk limit, in GB, that leaves it room: 0 if nothing says.
func (r diskRoom) Suggest() int { return max(r.Others, r.Least) }

// Fills says its data fills a disk limit of gb: it cannot write (0 is
// no limit).
func (r diskRoom) Fills(gb int) bool { return gb > 0 && r.Used > 0 && int64(gb)<<30 <= r.Used }

// Short says why a disk limit of gb is too small for its data, "" if it
// is not (0 is no limit).
func (r diskRoom) Short(tenant string, gb int) string {
	if !r.Fills(gb) {
		return ""
	}
	return strconv.Itoa(gb) + " GB is less than the " + size(float64(r.Used)) + " " + tenant + " holds: it could no longer write until the limit is raised"
}
