package panel

import (
	"regexp"
	"slices"
	"strconv"
	"strings"
	"time"

	"github.com/PowerPenguini/murder/internal/access"
	"github.com/PowerPenguini/murder/internal/store"
)

// actions say each audit log action as people do; the code stays beside
// it, for searching and the CLI.
var actions = map[string]string{
	"app.created": "app created", "app.updated": "app changed", "app.rolled-out": "app rolled out to a tenant", "app.deleted": "app deleted",
	"changes.frozen": "changes frozen", "changes.thawed": "changes taken again",
	"cluster.started": "cluster started",
	"device.added":    "device added", "device.removed": "device removed",
	"domain.added": "domain added", "domain.removed": "domain removed", "domain.verified": "domain points here", "domain.pending": "domain waits for its DNS",
	"domain.issued": "domain's certificate issued", "domain.renewed": "domain's certificate renewed", "domain.failed": "domain's certificate failed",
	"acme.account-created": "ACME account made",
	"node.joined":          "node joined", "node.removed": "node removed", "node.changed": "node changed",
	"node.failed": "node failed", "node.failed-over": "node's tenants failed over", "node.back": "node back",
	"node.away": "node restarting", "node.upgrading": "node upgrading", "node.upgraded": "node upgraded",
	"node.public": "node's public addresses", "node.tunnel": "node in the tunnel",
	"panel.invited": "person invited", "panel.user-added": "person added", "panel.user-removed": "person removed",
	"panel.passkey-added": "passkey added", "panel.passkey-removed": "passkey removed", "panel.recovery-codes": "recovery codes made",
	"panel.signed-in": "signed in", "panel.signed-out": "signed out",
	"secret.rotated": "secret given a new value",
	"tenant.created": "created", "tenant.updated": "settings changed", "tenant.rolled": "new container",
	"tenant.suspended": "suspended", "tenant.resumed": "resumed", "tenant.archived": "archived", "tenant.restored": "restored", "tenant.purged": "purged",
	"tenant.moving": "moving", "tenant.moved": "moved", "tenant.move-cancelled": "move undone",
	"tenant.failed-over": "failed over to its copy", "tenant.restarted": "restarted after it exited",
	"tenant.replica": "copy placed", "tenant.replica-moving": "copy moving", "tenant.replica-ready": "copy ready",
	"access.state": "copy of the whole state read", "access.secret": "secret read", "access.logs": "logs read",
	"access.backup-verify": "backup read through", "access.backup-restore": "backup put back", "access.backup-forget": "backups deleted",
	"access.manual": "data reached outside Murder",
}

// actionText is an action as people say it.
func actionText(action string) string {
	if text, ok := actions[action]; ok {
		return text
	}
	_, rest, _ := strings.Cut(action, ".")
	return strings.ReplaceAll(rest, "-", " ")
}

// byPeople says an event was something a person did, not the cluster's
// own keeping: the agent's actor is "murder", or "murder (what)".
// who names an actor of the audit log as people read it. The agent writes
// a person acting in the panel as "jan (panel) via murder-panel": the
// panel's own system user signs for them. The panel's socket, where no one
// signs in, is "the local panel".
func who(actor string) string {
	name, viaPanel := strings.CutSuffix(actor, " via murder-panel")
	switch {
	case name == "murder-panel socket":
		return "the local panel"
	case strings.HasSuffix(name, " (panel)"), strings.HasSuffix(name, " (panel socket)"):
		return name
	case viaPanel:
		return name + " (panel)"
	}
	return name
}

// sameActor says two of the audit log's actors are one.
func sameActor(a, b string) bool { return a != "" && who(a) == who(b) }

func byPeople(e store.Event) bool {
	return e.Actor != "murder" && !strings.HasPrefix(e.Actor, "murder (")
}

// logLine is one line of a container's output, and how loud it is; a
// line that begins with its time has it apart, Short as hh:mm:ss.
type logLine struct {
	Text, Level  string
	Stamp, Short string
}

// logLines splits output into lines, marking those that warn or tell of
// an error, as apps commonly write them.
func logLines(output string) []logLine {
	output = strings.TrimRight(output, "\n")
	if output == "" {
		return nil
	}
	var out []logLine
	for _, line := range strings.Split(output, "\n") {
		upper := strings.ToUpper(line)
		level := ""
		switch {
		case strings.Contains(upper, "ERROR") || strings.Contains(upper, "FATAL") || strings.Contains(upper, "PANIC") || strings.Contains(upper, "LEVEL=ERR"):
			level = "error"
		case strings.Contains(upper, "WARN"):
			level = "warn"
		}
		l := logLine{Text: line, Level: level}
		if stamp, rest, ok := strings.Cut(line, " "); ok && len(stamp) >= 20 && stamp[4] == '-' && stamp[10] == 'T' {
			if at, err := time.Parse(time.RFC3339Nano, stamp); err == nil {
				l.Stamp, l.Short, l.Text = stamp, at.UTC().Format("15:04:05"), rest
			}
		}
		out = append(out, l)
	}
	return out
}

// failures are the actions that tell of something that broke.
var failures = map[string]bool{
	"node.failed": true, "node.failed-over": true, "tenant.failed-over": true, "tenant.restarted": true,
	"tenant.move-cancelled": true, "rollout.paused": true, "rollout.rolled-back": true, "rollout.restored": true,
	"backup.failed": true, "domain.failed": true,
}

// failureEvent says an event tells of something that broke.
func failureEvent(action string) bool { return failures[action] }

// predicates say a tenant's event after its name: "atlas was suspended".
var predicates = map[string]string{
	"tenant.created": "was created", "tenant.updated": "had its settings changed", "tenant.rolled": "got a new container",
	"tenant.suspended": "was suspended", "tenant.resumed": "was resumed", "tenant.archived": "was archived", "tenant.restored": "was restored",
	"tenant.purged": "was purged", "tenant.moving": "is moving", "tenant.moved": "was moved", "tenant.move-cancelled": "had its move undone",
	"tenant.failed-over": "failed over to its copy", "tenant.restarted": "restarted after it exited", "tenant.replica": "had its copy placed",
	"tenant.replica-moving": "has its copy moving", "tenant.replica-ready": "has its copy ready", "app.rolled-out": "took its app's new revision",
	"secret.rotated": "had a secret given a new value", "tenant.flags": "had its rollout flags changed",
	"domain.added": "was given a domain", "domain.removed": "had a domain removed", "domain.verified": "has a domain pointing here",
	"domain.pending": "has a domain waiting for its DNS", "domain.issued": "got a domain's certificate", "domain.renewed": "had a domain's certificate renewed",
	"domain.failed": "could not get a domain's certificate",
	"access.secret": "had a secret read", "access.logs": "had its logs read", "access.backup-verify": "had its backup read through",
	"access.backup-restore": "had its data put back from a backup", "access.backup-forget": "had its backups deleted",
	"access.manual": "had its data reached outside Murder",
}

// eventRow is one row of an events table: an event, or a run of the same
// event one after the other, newest first.
type eventRow struct {
	store.Event
	// Group are the events of a run, Event the newest of them; nil for
	// one event alone.
	Group []store.Event
	// Tenants names the tenants of a run.
	Tenants []string
}

// Predicate says the event after its tenant's name, "" for one without.
func (r eventRow) Predicate() string {
	if r.Tenant == "" {
		return ""
	}
	// A new container a person asked for is a restart; the cluster's own
	// are new containers.
	if r.Action == "tenant.rolled" && byPeople(r.Event) {
		return "was restarted"
	}
	return predicates[r.Action]
}

// plurals say a run's predicate of several tenants.
var plurals = map[string]string{
	"tenant.restarted": "restarted after they exited", "tenant.failed-over": "failed over to their copies", "tenant.rolled": "got new containers",
	"tenant.replica": "had their copies placed", "tenant.replica-ready": "have their copies ready", "app.rolled-out": "took their app's new revision",
	"tenant.moved": "were moved", "tenant.created": "were created", "tenant.updated": "had their settings changed",
}

// PredicatePlural says a run's event after "N tenants".
func (r eventRow) PredicatePlural() string {
	if r.Action == "tenant.rolled" && byPeople(r.Event) {
		return "were restarted"
	}
	if p, ok := plurals[r.Action]; ok {
		return p
	}
	return actionText(r.Action)
}

// nodePredicates say a node's event after its name.
var nodePredicates = map[string]string{
	"node.failed-over": "'s tenants failed over to their copies", "node.failed": " failed", "node.back": " is back", "node.away": " is restarting",
	"node.upgraded": " was upgraded", "node.upgrading": " is upgrading", "node.joined": " joined the cluster", "node.removed": " was removed from the cluster",
}

// Node is the node a node's event is about, from its detail ("hel-1: 2
// tenants...", "fsn-1 to a10e..."), and NodePredicate what it says of it;
// both "" for other events.
func (r eventRow) Node() string {
	if _, ok := nodePredicates[r.Action]; !ok || r.Detail == "" {
		return ""
	}
	name, _, _ := strings.Cut(r.Detail, " ")
	return strings.TrimSuffix(name, ":")
}

func (r eventRow) NodePredicate() string { return nodePredicates[r.Action] }

// NodeDetail is its detail without the node's name.
func (r eventRow) NodeDetail() string {
	_, rest, _ := strings.Cut(r.Detail, " ")
	return rest
}

// appPredicates say an app's event after its name.
var appPredicates = map[string]string{
	"app.created": " was created", "app.updated": "'s spec changed", "app.deleted": " was deleted",
	"rollout.pause": "'s rollout was paused", "rollout.resume": "'s rollout went on", "rollout.abort": "'s rollout was aborted",
	"rollout.promote": "'s next wave was moved early", "rollout.restored": "'s tenants went back with their data", "rollout.paused": "'s rollout paused itself",
	"rollout.rolled-back": "'s tenants went back to the revision before",
}

// AppPredicate says an app's event after its name; "" for other events.
func (r eventRow) AppPredicate() string {
	if r.Tenant != "" || r.App == "" {
		return ""
	}
	return appPredicates[r.Action]
}

// Brief is its detail in a line: for a change of a tenant's settings,
// what changed ("disk limit 2 GB → 10 GB"), its whole own set left to the
// opened record.
func (r eventRow) Brief() string {
	if r.Action != "tenant.updated" && r.Action != "tenant.created" {
		return r.Detail
	}
	// What comes before its whole own set: another app, what changed.
	var parts []string
	for _, part := range strings.Split(r.Detail, "; ") {
		if strings.HasPrefix(part, "its own: ") || part == "no overrides" {
			break
		}
		parts = append(parts, strings.TrimPrefix(part, "changes: "))
	}
	if len(parts) == 0 {
		return r.Detail
	}
	return strings.Join(parts, "; ")
}

// Oldest is the oldest event of a run.
func (r eventRow) Oldest() store.Event {
	if len(r.Group) == 0 {
		return r.Event
	}
	return r.Group[len(r.Group)-1]
}

// groupEvents puts runs of the same action, said the same way, into one
// row each: the cluster's own keeping and repeated failures read as one
// line, which opens on each of them.
func groupEvents(events []store.Event) []eventRow {
	var rows []eventRow
	for _, e := range events {
		e.Detail = overridesSaid(namedWave(e), e.Action)
		// An access's detail is JSON, for the register: said as a line.
		if entry, ok := access.FromEvent(e); ok {
			e.Detail = accessSaid(entry)
		}
		if n := len(rows); n > 0 {
			last := &rows[n-1]
			if last.Action == e.Action && last.Detail == e.Detail && byPeople(last.Event) == byPeople(e) && last.Actor == e.Actor {
				if last.Group == nil {
					last.Group = []store.Event{last.Event}
					if last.Tenant != "" {
						last.Tenants = []string{last.Tenant}
					}
				}
				last.Group = append(last.Group, e)
				if e.Tenant != "" && !slices.Contains(last.Tenants, e.Tenant) {
					last.Tenants = append(last.Tenants, e.Tenant)
				}
				continue
			}
		}
		rows = append(rows, eventRow{Event: e})
	}
	return rows
}

// storedLimits is the store's word for a tenant's own limits in its
// settings' events: "limits memoryMB=768 cpus=0 pids=0 diskGB=2".
var storedLimits = regexp.MustCompile(`limits memoryMB=(\d+) cpus=([\d.]+) pids=(\d+) diskGB=(\d+)`)

// overridesSaid says a tenant's settings event as people read it: "its
// own: env FEATURE_PLANNER, TZ; secret ADMIN_PASSWORD; memory 768 MB, disk
// 2 GB", not the store's "overrides: env FEATURE_PLANNER,TZ; secrets
// ADMIN_PASSWORD; limits memoryMB=768 cpus=0 pids=0 diskGB=2".
func overridesSaid(detail, action string) string {
	if action != "tenant.updated" && action != "tenant.created" {
		return detail
	}
	var parts []string
	for _, part := range strings.Split(detail, "; ") {
		part = strings.Replace(part, "overrides: ", "its own: ", 1)
		if m := storedLimits.FindStringSubmatchIndex(part); m != nil {
			var limits []string
			for i, unit := range []string{"memory %s MB", "CPUs %s", "processes %s", "disk %s GB"} {
				if value := part[m[2+2*i]:m[3+2*i]]; value != "0" {
					limits = append(limits, strings.Replace(unit, "%s", value, 1))
				}
			}
			if len(limits) == 0 {
				limits = []string{"no limits"}
			}
			part = part[:m[0]] + strings.Join(limits, ", ") + part[m[1]:]
		}
		for _, kind := range []string{"env ", "secrets "} {
			if rest, ok := strings.CutPrefix(strings.TrimPrefix(part, "its own: "), kind); ok {
				names := strings.Split(rest, ",")
				label := strings.TrimSpace(kind)
				if kind == "secrets " && len(names) == 1 {
					label = "secret"
				}
				part = strings.TrimSuffix(part, kind+rest) + label + " " + strings.Join(names, ", ")
			}
		}
		parts = append(parts, part)
	}
	return strings.Join(parts, "; ")
}

// numberedWave is a rollout's move as events said it before they named
// their wave: "wave 2: revision 2 → 3"; namedComma as they said it for a
// while: "wave 2, a tenth: revision 2 → 3".
var (
	numberedWave = regexp.MustCompile(`^wave (\d+): `)
	namedComma   = regexp.MustCompile(`^wave (\d+), ([^:(]+): `)
)

// namedWave says an older move as newer ones are recorded: "wave 2 (a
// tenth): revision 2 → 3". Rollouts recorded by number moved no tenant
// first, so their waves are named from the canaries.
func namedWave(e store.Event) string {
	if e.Action != "app.rolled-out" {
		return e.Detail
	}
	if m := numberedWave.FindStringSubmatch(e.Detail); m != nil {
		n, _ := strconv.Atoi(m[1])
		return store.WaveSaid(n, store.WaveName(n-1)) + ": " + e.Detail[len(m[0]):]
	}
	if m := namedComma.FindStringSubmatch(e.Detail); m != nil {
		n, _ := strconv.Atoi(m[1])
		name := m[2]
		if name == "first now" {
			name = "first, now"
		}
		return store.WaveSaid(n, name) + ": " + e.Detail[len(m[0]):]
	}
	return e.Detail
}

// rolloutBlocked says a blocked wave the one way every page does:
// "baltic-freight moved in wave 2 (a tenth) and is not healthy, so wave 3
// (half) waits."
func rolloutBlocked(r store.Rollout, blockers []string) string {
	who, verb := strings.Join(blockers, ", "), "is"
	if len(blockers) > 1 {
		who, verb = strings.Join(blockers[:len(blockers)-1], ", ")+" and "+blockers[len(blockers)-1], "are"
	}
	return who + " moved in " + store.WaveSaid(r.Wave, r.WaveName(r.Wave)) + " and " + verb + " not healthy, so " + store.WaveSaid(r.Wave+1, r.WaveName(r.Wave+1)) + " waits."
}

// accessSaid is an access of the register as its event's detail says it:
// what, why and at whose request.
func accessSaid(e access.Entry) string {
	said := ""
	if e.What != "" {
		said = e.What + " · "
	}
	said += "“" + e.Purpose + "” · ticket " + e.Ticket
	if !e.AccessedAt.IsZero() {
		said += " · accessed " + e.AccessedAt.UTC().Format("2006-01-02 15:04") + " UTC"
	}
	if e.Offline != "" {
		said += " · recorded offline first"
	}
	return said
}
