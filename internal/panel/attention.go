package panel

import (
	"net/url"
	"regexp"
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
	// Head is what is wrong in a few words, as it follows the subject's
	// name: " is unhealthy: out of memory 4m ago, 248 of 256 MB". Evidence
	// is the whole of it, sentence by sentence, behind a disclosure, with
	// More, the fixes of last resort.
	Head     string
	Evidence []string
	More     []fix
	// Primary is the fix of what is wrong, the page's filled button.
	Primary *fix
}

// sub is a line under a notice, with its own fixes: Short says it in one
// line, Text whole, in the notice's evidence.
type sub struct {
	Short, Text string
	Bad         bool
	Fixes       []fix
}

// EvidenceLabel names a notice's disclosure: the affected tenants are in
// it when an app's cause is.
func (n notice) EvidenceLabel() string {
	for _, s := range n.Subs {
		if s.Text != "" && s.Text != s.Short {
			return "Evidence and affected tenants"
		}
	}
	return "Evidence"
}

// HasEvidence says the notice has more to say than its lines.
func (n notice) HasEvidence() bool {
	if len(n.Evidence) > 0 || len(n.More) > 0 {
		return true
	}
	for _, s := range n.Subs {
		if s.Text != "" && s.Text != s.Short {
			return true
		}
	}
	return false
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
				Head: " failed " + ago(now, n.FailedAt) + " ago: its tenants run from their copies", Primary: &fix{"What to do", href},
				Evidence: []string{"It stopped answering at " + n.FailedAt.UTC().Format("15:04:05") + " UTC; the cluster failed its tenants over to their copies on the other nodes, and it keeps no copy now."}})
		case n.State.Kind == "bad":
			out = append(out, notice{Bad: true, Node: true, Subject: n.Name, Href: href, Text: " does not answer: its tenants stop until it is failed over",
				Primary: &fix{"Fail it over", href}})
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
		// Each cause: in a few words, for the headline, and whole.
		type cause struct{ head, whole string }
		var causes []cause
		var texts []string
		memHigh := row.Use != nil && row.Use.MemoryLimit != "" && row.Use.Memory.Width >= memoryWarn
		var used, limit string
		if memHigh {
			used, limit = row.Use.MemoryText, row.Use.MemoryLimit
		}
		if problem, bad := tenantProblem(row.Tenant, row.State); problem != "" {
			texts = append(texts, problem)
			n.Bad = n.Bad || bad
			head, whole := tenantHeadline(row.Tenant, row.State, used, limit, now)
			causes = append(causes, cause{head, strings.Join(whole, " ")})
			if e, ok := parseExit(statusExit(row.Tenant)); ok {
				n.Reasons = append(n.Reasons, "last "+e.Reason(now))
				n.Fixes = append(n.Fixes, fix{"Its last lines before the exit", href + "/logs?container=previous"})
			} else {
				n.Fixes = append(n.Fixes, fix{"Logs", href + "/logs"})
			}
		}
		if memHigh {
			n.Reasons = append(n.Reasons, "memory "+used+" of "+limit)
			texts = append(texts, "uses "+used+" of its "+limit+" of memory")
			if len(causes) == 0 {
				causes = append(causes, cause{" uses " + usedOf(used, limit) + " of memory", "It uses " + used + " of its " + limit + " of memory: the kernel kills it at the limit."})
			}
			fixed := fix{"Raise its memory limit", raiseMemory(row.Tenant)}
			n.Fixes = append(n.Fixes, fixed)
		}
		if clustered {
			var text string
			switch {
			case row.Replica.Kind == "bad":
				n.Reasons = append(n.Reasons, "no copy")
				text = "has no copy: if " + row.Node + " fails, it stops"
			case row.Tenant.Replica != "" && !row.ReplicaReady && row.ReplicaNext == "":
				n.Reasons = append(n.Reasons, "copy catching up")
				text = "its copy on " + row.Tenant.Replica + " is still catching up: if " + row.Node + " fails now, it stops"
			case row.ReplicaNext != "" && failed[row.ReplicaNext]:
				n.Reasons = append(n.Reasons, "copy moving to a failed node")
				text = "its copy moves to " + row.ReplicaNext + ", which failed"
			}
			if text != "" {
				texts = append(texts, text)
				causes = append(causes, cause{sentence([]string{text}), itSentence(text)})
			}
		}
		if b := row.Backup; b != nil && b.Bad() {
			var text string
			if b.Failing {
				// A tenant without a fresh backup is wrong, not in progress.
				n.Bad = true
				text = "its last backup failed"
				if !b.None {
					text += "; the newest is " + ago(now, b.Last) + " old"
					n.Reasons = append(n.Reasons, "last backup failed, newest "+ago(now, b.Last)+" old")
				} else {
					n.Reasons = append(n.Reasons, "last backup failed, none yet")
				}
				if n.Primary == nil {
					n.Primary = &fix{"Why its backup failed", "/cluster#backups"}
				}
			} else {
				n.Reasons = append(n.Reasons, "newest backup "+ago(now, b.Last)+" old")
				text = "its newest backup is " + ago(now, b.Last) + " old, though they run every " + every(b.Every)
				n.Fixes = append(n.Fixes, fix{"Backups", "/cluster#backups"})
			}
			texts = append(texts, text)
			causes = append(causes, cause{sentence([]string{text}), itSentence(text)})
		}
		if len(causes) == 0 {
			continue
		}
		n.Text = sentence(texts)
		// The worst cause leads; the evidence says each whole, but for one
		// its headline says all of.
		n.Head = causes[0].head
		for i, c := range causes {
			if i == 0 && len(causes) == 1 && c.whole == itSentence(strings.TrimPrefix(c.head, " ")) {
				continue
			}
			n.Evidence = append(n.Evidence, c.whole)
		}
		if memHigh && n.Primary == nil {
			n.Primary = &fix{"Raise its memory limit", raiseMemory(row.Tenant)}
			n.Fixes = slices.DeleteFunc(n.Fixes, func(f fix) bool { return f.Label == "Raise its memory limit" })
		}
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
		if e, ok := parseExit(status.LastExit); ok {
			text += "; it was last " + e.Said(time.Time{})
		}
		return text, true
	case state.Kind == "bad":
		text := "failed"
		if status.Message != "" {
			text += ": " + status.Message
		} else if e, ok := parseExit(status.LastExit); ok {
			text += "; it was last " + e.Said(time.Time{})
		}
		return text, true
	}
	text := "is " + state.Label
	if status.Message != "" {
		text += ": " + status.Message
	}
	return text, false
}

// statusExit is the agent's words for a tenant's last exit, if any.
func statusExit(t api.Tenant) string {
	if t.Status == nil {
		return ""
	}
	return t.Status.LastExit
}

// tenantHeadline says what is wrong with a tenant in a few words, after its
// name (" is unhealthy: out of memory 4m ago, 248 of 256 MB"), and whole,
// sentence by sentence. used and limit are its memory, when near the limit.
func tenantHeadline(t api.Tenant, state tag, used, limit string, now time.Time) (string, []string) {
	status := t.Status
	e, exited := parseExit(statusExit(t))
	var whole []string
	said := func() {
		if exited {
			whole = append(whole, "Its last exit: "+e.Said(now)+".")
		}
		if used != "" {
			whole = append(whole, "It uses "+used+" of its "+limit+" of memory: the kernel kills it at the limit.")
		}
	}
	memory := ""
	if used != "" {
		memory = ", " + usedOf(used, limit)
	}
	switch {
	case status == nil:
		return " is in a state not known: its node does not answer", nil
	case state.Label == "unhealthy":
		whole = append(whole, "It does not answer its health check.")
		said()
		if exited {
			return " is unhealthy: " + e.Short(now) + memory, whole
		}
		return " is unhealthy: it fails its health check" + memory, whole
	case state.Kind == "bad":
		if status.Message != "" {
			whole = append(whole, "It failed: "+status.Message+".")
		}
		said()
		switch {
		case exited:
			return " failed: " + e.Short(now), whole
		case status.Message != "":
			return " failed: " + firstWords(status.Message, 8), whole
		}
		return " failed", whole
	}
	text := " is " + state.Label
	if status.Message != "" {
		whole = append(whole, "It is "+state.Label+": "+status.Message+".")
		text += ": " + firstWords(status.Message, 8)
	}
	said()
	return text, whole
}

// usedOf says memory used of a limit, the unit once when it is the same:
// "248 of 256 MB".
func usedOf(used, limit string) string {
	if u, ok := strings.CutSuffix(used, " MB"); ok && strings.HasSuffix(limit, " MB") {
		return u + " of " + limit
	}
	if u, ok := strings.CutSuffix(used, " GB"); ok && strings.HasSuffix(limit, " GB") {
		return u + " of " + limit
	}
	return used + " of " + limit
}

// firstWords is text cut to its first n words, or its first clause.
func firstWords(text string, n int) string {
	if i := strings.IndexAny(text, ":;"); i > 0 {
		text = text[:i]
	}
	words := strings.Fields(text)
	if len(words) > n {
		return strings.Join(words[:n], " ") + "…"
	}
	return strings.Join(words, " ")
}

// itSentence makes a cause said after a name ("its copy ...", "has no
// copy ...") a sentence of its own: "Its copy ...", "It has no copy ...".
func itSentence(text string) string {
	if rest, ok := strings.CutPrefix(text, "'s "); ok {
		return "Its " + rest + "."
	}
	if rest, ok := strings.CutPrefix(text, "its "); ok {
		return "Its " + rest + "."
	}
	return "It " + text + "."
}

// capital is text with its first letter a capital.
func capital(text string) string {
	if text == "" {
		return text
	}
	return strings.ToUpper(text[:1]) + text[1:]
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
