package panel

import (
	"regexp"
	"strconv"
	"strings"
	"time"
)

// exit is what the panel reads of an app's exit from the agent's words: a
// tenant's last exit ("killed (SIGKILL) at 2026-11-04T15:20:00Z, most
// likely for using more than its 256 MB of memory") or the audit log's
// ("exit 137: killed for using more than its 256 MB of memory").
type exit struct {
	// Code is the exit code; -1 when not known.
	Code int
	// Memory says it was most likely for its memory; Sure that the kernel
	// said so.
	Memory, Sure bool
	// LimitMB is the memory limit it ran under, if said.
	LimitMB int
	// At is when, if said.
	At time.Time
}

var (
	exitCode  = regexp.MustCompile(`\b(?:exit|exited with code) (\d+)`)
	exitAt    = regexp.MustCompile(`\bat (\d{4}-\d\d-\d\dT\d\d:\d\d:\d\d(?:\.\d+)?Z)`)
	exitLimit = regexp.MustCompile(`\bits (\d+) MB of memory`)
)

// parseExit reads an exit from the agent's words; ok is false for none.
func parseExit(text string) (exit, bool) {
	if strings.TrimSpace(text) == "" {
		return exit{}, false
	}
	e := exit{Code: -1}
	switch m := exitCode.FindStringSubmatch(text); {
	case m != nil:
		e.Code, _ = strconv.Atoi(m[1])
	case strings.HasPrefix(text, "killed"):
		e.Code = 137
	}
	e.Memory = strings.Contains(text, "memory")
	e.Sure = strings.Contains(text, "killed for using more than")
	if m := exitLimit.FindStringSubmatch(text); m != nil {
		e.LimitMB, _ = strconv.Atoi(m[1])
	}
	if m := exitAt.FindStringSubmatch(text); m != nil {
		e.At, _ = time.Parse(time.RFC3339Nano, m[1])
	}
	return e, true
}

// when is how long ago it was, " 4m ago", or nothing when not said.
func (e exit) when(now time.Time) string {
	if e.At.IsZero() {
		return ""
	}
	return " " + ago(now, e.At) + " ago"
}

// Short says it in a few words, for a headline: "out of memory 4m ago",
// "exit 2 4m ago".
func (e exit) Short(now time.Time) string {
	switch {
	case e.Memory:
		return "out of memory" + e.when(now)
	case e.Code >= 0:
		return "exit " + strconv.Itoa(e.Code) + e.when(now)
	}
	return "exited" + e.when(now)
}

// Reason says it for a list's row: "exit 137 4m ago".
func (e exit) Reason(now time.Time) string {
	if e.Code < 0 {
		return "exited" + e.when(now)
	}
	return "exit " + strconv.Itoa(e.Code) + e.when(now)
}

// Said says it whole, as a sentence's end: "killed (exit 137) 4m ago, at
// 15:20:00 UTC, most likely for using more than its 256 MB of memory";
// with no now, without how long ago.
func (e exit) Said(now time.Time) string {
	var b strings.Builder
	switch {
	case e.Code == 137:
		b.WriteString("killed (exit 137)")
	case e.Code >= 0:
		b.WriteString("exited with code " + strconv.Itoa(e.Code))
	default:
		b.WriteString("exited, its code not known")
	}
	switch {
	case e.At.IsZero():
	case now.IsZero():
		b.WriteString(" at " + e.At.UTC().Format("15:04:05") + " UTC")
	default:
		b.WriteString(e.when(now) + ", at " + e.At.UTC().Format("15:04:05") + " UTC")
	}
	switch {
	case e.Sure && e.LimitMB > 0:
		b.WriteString(", for using more than its " + memory(e.LimitMB) + " of memory")
	case e.Sure:
		b.WriteString(", for using more than its memory limit")
	case e.Memory && e.LimitMB > 0:
		b.WriteString(", most likely for using more than its " + memory(e.LimitMB) + " of memory")
	case e.Memory:
		b.WriteString(", most likely out of memory")
	}
	return b.String()
}
