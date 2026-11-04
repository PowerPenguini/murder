package panel

import (
	"strings"
	"time"

	"github.com/PowerPenguini/murder/internal/store"
)

// actions say each audit log action as people do; the code stays beside
// it, for searching and the CLI.
var actions = map[string]string{
	"app.created": "app created", "app.updated": "app changed", "app.rolled-out": "app rolled out to a tenant", "app.deleted": "app deleted",
	"changes.frozen": "changes frozen", "changes.thawed": "changes taken again",
	"cluster.started": "cluster started",
	"device.added":    "device added", "device.removed": "device removed",
	"node.joined": "node joined", "node.removed": "node removed", "node.changed": "node changed",
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
	"backup.failed": true,
}

// failureEvent says an event tells of something that broke.
func failureEvent(action string) bool { return failures[action] }
