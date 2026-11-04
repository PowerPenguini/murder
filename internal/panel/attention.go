package panel

import (
	"net/url"
	"slices"
	"strconv"
	"strings"
	"time"

	"github.com/PowerPenguini/murder/internal/api"
)

// notice is one thing that needs attention, a node or a tenant, with all
// that is wrong with it said plainly, and the ways to fix it.
type notice struct {
	// Bad is for what is broken; the rest may need a look.
	Bad bool
	// Subject is the node or tenant it is about; Href its page.
	Subject, Href string
	// Text is what is wrong, every cause in one sentence, the worst first.
	Text string
	// Fixes are where to act on it.
	Fixes []fix
}

// fix is a way to act on what needs attention.
type fix struct{ Label, Href string }

// memoryWarn is the share of its memory limit a tenant uses before it is
// said: past it, the kernel soon kills it.
const memoryWarn = 90.0

// attentionOf is what on the overview needs attention, one notice for each
// node or tenant affected, the broken first.
func attentionOf(nodes []nodeView, rows []tenantRow, clustered bool, now time.Time) []notice {
	var out []notice
	failed := map[string]bool{}
	for _, n := range nodes {
		href := nodeHref(n.Name)
		switch {
		case n.Failed():
			failed[n.Name] = true
			out = append(out, notice{Bad: true, Subject: n.Name, Href: href, Text: "failed " + ago(now, n.FailedAt) + " ago: its tenants run from their copies, and it keeps none",
				Fixes: []fix{{"What to do", href}}})
		case n.State.Kind == "bad":
			out = append(out, notice{Bad: true, Subject: n.Name, Href: href, Text: "does not answer: its tenants stop until it is failed over",
				Fixes: []fix{{"Fail it over", href}}})
		case n.State.Kind == "warn":
			out = append(out, notice{Subject: n.Name, Href: href, Text: "is " + n.State.Label})
		}
	}
	for _, row := range rows {
		if row.Archived() || row.Spec.Suspended {
			continue
		}
		name := row.Spec.Name
		href := "/tenants/" + url.PathEscape(name)
		n := notice{Subject: name, Href: href}
		var causes []string
		if problem, bad := tenantProblem(row.Tenant, row.State); problem != "" {
			causes = append(causes, problem)
			n.Bad = n.Bad || bad
			n.Fixes = append(n.Fixes, fix{"Logs", href + "/logs"})
		}
		if row.Use != nil && row.Use.MemoryLimit != "" && row.Use.Memory.Width >= memoryWarn {
			causes = append(causes, "uses "+row.Use.MemoryText+" of its "+row.Use.MemoryLimit+" of memory")
			n.Fixes = append(n.Fixes, fix{"Raise its memory limit", href + "/settings#memory"})
		}
		if clustered {
			switch {
			case row.Replica.Kind == "bad":
				causes = append(causes, "has no copy: should "+row.Node+" fail, it stops")
			case row.Tenant.Replica != "" && !row.ReplicaReady && row.ReplicaNext == "":
				causes = append(causes, "its copy on "+row.Tenant.Replica+" is still catching up: should "+row.Node+" fail now, it stops")
			case row.ReplicaNext != "" && failed[row.ReplicaNext]:
				causes = append(causes, "its copy moves to "+row.ReplicaNext+", which failed")
			}
		}
		if len(causes) == 0 {
			continue
		}
		n.Text = strings.Join(causes, "; ")
		out = append(out, n)
	}
	slices.SortStableFunc(out, func(a, b notice) int {
		switch {
		case a.Bad == b.Bad:
			return 0
		case a.Bad:
			return -1
		}
		return 1
	})
	return out
}

// navAlerts are the counts of what is broken, beside the side bar's pages,
// so that a failure is seen from any page.
type navAlerts struct {
	// Tenants that do not serve as they should, nodes that failed or do not
	// answer, and both, as the overview counts them.
	Tenants, Nodes, Overview int
}

// alertsOf counts what is broken among nodes and tenants.
func alertsOf(nodes []api.NodeState, tenants []api.Tenant, now time.Time) navAlerts {
	var a navAlerts
	for _, n := range nodes {
		if nodeState(n, now).Kind == "bad" {
			a.Nodes++
		}
	}
	for _, t := range tenants {
		if t.Archived() || t.Spec.Suspended {
			continue
		}
		if _, bad := tenantProblem(t, tenantState(t)); bad {
			a.Tenants++
		}
	}
	a.Overview = a.Nodes + a.Tenants
	return a
}

// Title is the count a page's title begins with, as "(2) ".
func (a navAlerts) Title() string {
	if a.Overview == 0 {
		return ""
	}
	return "(" + strconv.Itoa(a.Overview) + ") "
}

// tenantProblem says what is wrong with a tenant that is meant to run, and
// whether it is broken; "" if nothing is.
func tenantProblem(t api.Tenant, state tag) (string, bool) {
	status := t.Status
	switch {
	case state.Kind == "ok" || state.Kind == "mute":
		return "", false
	case status == nil:
		return "is in a state not known: its node does not answer", false
	case state.Label == "unhealthy":
		text := "is unhealthy: it does not answer its health check"
		if status.LastExit != "" {
			text += "; last exit: " + status.LastExit
		}
		return text, true
	case state.Kind == "bad":
		text := "failed"
		if status.Message != "" {
			text += ": " + status.Message
		} else if status.LastExit != "" {
			text += "; last exit: " + status.LastExit
		}
		return text, true
	}
	text := "is " + state.Label
	if status.Message != "" {
		text += ": " + status.Message
	}
	return text, false
}

// nodeHref is where a node is shown.
func nodeHref(name string) string { return "/cluster#node-" + url.PathEscape(name) }
