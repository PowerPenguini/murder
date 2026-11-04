package panel

import (
	"regexp"
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
	// Text is what is wrong, every cause in one sentence, the worst first,
	// as it follows the subject's name: " is unhealthy...", "'s copy...".
	Text string
	// Fixes are where to act on it.
	Fixes []fix
	// Node says it is about a node, App about an app, not a tenant.
	Node, App bool
	// Reasons say, short, what its row in a list shows: "last backup failed
	// 3h ago", "copy catching up".
	Reasons []string
	// Subs are what else it explains or causes, said under it: an app's
	// limit that is likely too low, a rollout it blocks.
	Subs []sub
}

// sub is a line under a notice, with its own fixes.
type sub struct {
	Text  string
	Bad   bool
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
			out = append(out, notice{Bad: true, Node: true, Subject: n.Name, Href: href, Text: " failed " + ago(now, n.FailedAt) + " ago: its tenants run from their copies, and it keeps none",
				Fixes: []fix{{"What to do", href}}})
		case n.State.Kind == "bad":
			out = append(out, notice{Bad: true, Node: true, Subject: n.Name, Href: href, Text: " does not answer: its tenants stop until it is failed over",
				Fixes: []fix{{"Fail it over", href}}})
		case n.State.Kind == "warn":
			out = append(out, notice{Node: true, Subject: n.Name, Href: href, Text: " is " + n.State.Label})
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
		if problem, _ := tenantProblem(row.Tenant, row.State); problem != "" && row.Status != nil && row.Status.LastExit != "" {
			n.Reasons = append(n.Reasons, "last "+parenthetical.ReplaceAllString(row.Status.LastExit, ""))
		}
		if row.Use != nil && row.Use.MemoryLimit != "" && row.Use.Memory.Width >= memoryWarn {
			n.Reasons = append(n.Reasons, "memory "+row.Use.MemoryText+" of "+row.Use.MemoryLimit)
			causes = append(causes, "uses "+row.Use.MemoryText+" of its "+row.Use.MemoryLimit+" of memory")
			n.Fixes = append(n.Fixes, fix{"Raise its memory limit", raiseMemory(row.Tenant)})
		}
		if clustered {
			switch {
			case row.Replica.Kind == "bad":
				n.Reasons = append(n.Reasons, "no copy")
				causes = append(causes, "has no copy: if "+row.Node+" fails, it stops")
			case row.Tenant.Replica != "" && !row.ReplicaReady && row.ReplicaNext == "":
				n.Reasons = append(n.Reasons, "copy catching up")
				causes = append(causes, "its copy on "+row.Tenant.Replica+" is still catching up: if "+row.Node+" fails now, it stops")
			case row.ReplicaNext != "" && failed[row.ReplicaNext]:
				n.Reasons = append(n.Reasons, "copy moving to a failed node")
				causes = append(causes, "its copy moves to "+row.ReplicaNext+", which failed")
			}
		}
		if b := row.Backup; b != nil && b.Bad() {
			if b.Failing {
				// A tenant without a fresh backup is wrong, not in progress.
				n.Bad = true
				text := "its last backup failed"
				if !b.None {
					text += "; the newest is " + ago(now, b.Last) + " old"
					n.Reasons = append(n.Reasons, "last backup failed, newest "+ago(now, b.Last)+" old")
				} else {
					n.Reasons = append(n.Reasons, "last backup failed, none yet")
				}
				causes = append(causes, text)
			} else {
				n.Reasons = append(n.Reasons, "newest backup "+ago(now, b.Last)+" old")
				causes = append(causes, "its newest backup is "+ago(now, b.Last)+" old, though they run every "+every(b.Every))
			}
			n.Fixes = append(n.Fixes, fix{"Backups", "/cluster#backups"})
		}
		if len(causes) == 0 {
			continue
		}
		n.Text = sentence(causes)
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
// so that a failure is seen from any page. They are counted from the same
// notices as the overview's: one definition of broken everywhere.
type navAlerts struct {
	// Overview counts the broken things, nodes and tenants: the red rows of
	// the overview, an app's cause or blocked rollout being said under the
	// tenant it breaks. Tenants counts the broken tenants; Cluster the
	// broken nodes and the nodes whose backups fail; Apps the apps whose
	// rollout is blocked or whose limit breaks a tenant.
	Overview, Tenants, Cluster, Apps int
}

// alertsOf counts what is broken among notices; failing are the nodes
// whose backups fail.
func alertsOf(notices []notice, failing int) navAlerts {
	a := navAlerts{Cluster: failing}
	for _, n := range notices {
		if !n.Bad {
			continue
		}
		if n.App {
			continue
		}
		a.Overview++
		if n.Node {
			a.Cluster++
		} else {
			a.Tenants++
		}
	}
	return a
}

// Of is a section's own count.
func (a navAlerts) Of(section string) int {
	switch section {
	case "overview":
		return a.Overview
	case "tenants":
		return a.Tenants
	case "cluster":
		return a.Cluster
	case "apps":
		return a.Apps
	}
	return 0
}

// Elsewhere says another section than section has something broken.
func (a navAlerts) Elsewhere(section string) bool {
	return a.Overview > 0 && a.Of(section) == 0
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
func nodeHref(name string) string { return "/nodes/" + url.PathEscape(name) }

// sentence joins what is wrong with a tenant into one sentence that
// follows its name: the first cause speaks of it by name, the others as
// "it".
func sentence(causes []string) string {
	var b strings.Builder
	for i, c := range causes {
		switch {
		case i > 0:
			b.WriteString("; ")
			if !strings.HasPrefix(c, "its ") {
				b.WriteString("it ")
			}
			b.WriteString(c)
		case strings.HasPrefix(c, "its "):
			b.WriteString("'s " + c[len("its "):])
		default:
			b.WriteString(" " + c)
		}
	}
	return b.String()
}

// parenthetical is an aside in brackets, left out of a short reason.
var parenthetical = regexp.MustCompile(` \([^)]*\)`)

// every says how often something runs: 6h, 30m, a day.
func every(d time.Duration) string {
	switch {
	case d <= 0:
		return "now and then"
	case d%(24*time.Hour) == 0:
		return plural(int(d/(24*time.Hour)), "day", "days")
	case d%time.Hour == 0:
		return strconv.Itoa(int(d/time.Hour)) + "h"
	}
	return strconv.Itoa(int(d/time.Minute)) + "m"
}

// raiseMemory is where a tenant's memory limit is raised: its settings,
// proposing twice the limit it has, as an override to review.
func raiseMemory(t api.Tenant) string {
	href := "/tenants/" + url.PathEscape(t.Spec.Name) + "/settings"
	if mb := t.Spec.Limits.MemoryMB; mb > 0 {
		href += "?memoryMB=" + strconv.Itoa(2*mb)
	}
	return href + "#memory"
}
