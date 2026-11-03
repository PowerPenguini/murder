package panel

import (
	"slices"
	"time"

	"github.com/PowerPenguini/murder/internal/api"
)

// notice is one thing that needs attention, said plainly: what, about
// whom, and where to see it.
type notice struct {
	// Bad is for what is broken; the rest may need a look.
	Bad bool
	// Subject is the node or tenant it is about; Href its page.
	Subject, Href string
	Text          string
}

// memoryWarn is the share of its memory limit a tenant uses before it is
// said: past it, the kernel soon kills it.
const memoryWarn = 90.0

// attentionOf is what on the overview needs attention, the broken first.
func attentionOf(nodes []nodeView, rows []tenantRow, clustered bool, now time.Time) []notice {
	var out []notice
	for _, n := range nodes {
		switch {
		case n.Failed():
			out = append(out, notice{Bad: true, Subject: n.Name, Href: "/cluster", Text: "failed " + ago(now, n.FailedAt) + " ago: its tenants run from their copies, and it keeps none"})
		case n.State.Kind == "bad":
			out = append(out, notice{Bad: true, Subject: n.Name, Href: "/cluster", Text: "does not answer"})
		case n.State.Kind == "warn":
			out = append(out, notice{Subject: n.Name, Href: "/cluster", Text: "is " + n.State.Label})
		}
	}
	for _, row := range rows {
		if row.Archived() || row.Spec.Suspended {
			continue
		}
		href := "/tenants/" + row.Spec.Name
		if problem, bad := tenantProblem(row.Tenant, row.State); problem != "" {
			out = append(out, notice{Bad: bad, Subject: row.Spec.Name, Href: href, Text: problem})
		}
		if row.Use != nil && row.Use.MemoryLimit != "" && row.Use.Memory.Width >= memoryWarn {
			out = append(out, notice{Subject: row.Spec.Name, Href: href, Text: "uses " + row.Use.MemoryText + " of its " + row.Use.MemoryLimit + " of memory"})
		}
		if clustered && row.Replica.Kind == "bad" {
			out = append(out, notice{Subject: row.Spec.Name, Href: href, Text: "has no copy: should " + row.Node + " fail, it stops"})
		}
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
