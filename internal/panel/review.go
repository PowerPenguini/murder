package panel

import (
	"context"
	"fmt"
	"maps"
	"net/http"
	"slices"
	"strconv"
	"strings"
	"time"

	"github.com/PowerPenguini/murder/internal/api"
	"github.com/PowerPenguini/murder/internal/rollout"
	"github.com/PowerPenguini/murder/internal/spec"
	"github.com/PowerPenguini/murder/internal/store"
)

// An app's change is reviewed before it rolls out: what changes in its
// spec, the tenants it reaches and what of theirs overrides it, the waves
// it moves in, and what becomes of their data. Nothing is stored until
// the review's button is pressed.

// diffLine is one line of a spec's change: kept, taken out or put in.
type diffLine struct {
	// Op is " ", "-" or "+".
	Op   string
	Text string
	// Old and New are its line numbers, 0 where it has none.
	Old, New int
}

// lineDiff is the change from a to b, line by line, the longest common
// run kept. Specs are short, so the plain quadratic way does.
func lineDiff(a, b string) []diffLine {
	x, y := splitLines(a), splitLines(b)
	n, m := len(x), len(y)
	lcs := make([][]int, n+1)
	for i := range lcs {
		lcs[i] = make([]int, m+1)
	}
	for i := n - 1; i >= 0; i-- {
		for j := m - 1; j >= 0; j-- {
			if x[i] == y[j] {
				lcs[i][j] = lcs[i+1][j+1] + 1
			} else {
				lcs[i][j] = max(lcs[i+1][j], lcs[i][j+1])
			}
		}
	}
	var out []diffLine
	i, j := 0, 0
	for i < n || j < m {
		switch {
		case i < n && j < m && x[i] == y[j]:
			out = append(out, diffLine{" ", x[i], i + 1, j + 1})
			i, j = i+1, j+1
		case i < n && (j == m || lcs[i+1][j] >= lcs[i][j+1]):
			out = append(out, diffLine{"-", x[i], i + 1, 0})
			i++
		default:
			out = append(out, diffLine{"+", y[j], 0, j + 1})
			j++
		}
	}
	return out
}

func splitLines(text string) []string {
	text = strings.TrimRight(strings.ReplaceAll(text, "\r\n", "\n"), "\n")
	if text == "" {
		return nil
	}
	return strings.Split(text, "\n")
}

// changedOnly keeps the changed lines and context lines about them,
// marking a gap with an empty Op.
func changedOnly(lines []diffLine, context int) []diffLine {
	keep := make([]bool, len(lines))
	for i, l := range lines {
		if l.Op == " " {
			continue
		}
		for k := max(0, i-context); k <= min(len(lines)-1, i+context); k++ {
			keep[k] = true
		}
	}
	var out []diffLine
	for i, l := range lines {
		if keep[i] {
			if i > 0 && !keep[i-1] && len(out) > 0 {
				out = append(out, diffLine{Op: ""})
			}
			out = append(out, l)
		}
	}
	if len(out) > 0 && !keep[0] {
		out = append([]diffLine{{Op: ""}}, out...)
	}
	return out
}

// reviewTenant is a tenant a change reaches, and what of its own the
// change does not touch.
type reviewTenant struct {
	Name  string
	State tag
	// Overrides name the settings it keeps over the app's.
	Overrides []string
	Canary    bool
	// Wave is the wave it moves in, from 1.
	Wave int
}

// wave is one wave of a rollout's plan.
type wave struct {
	Name    string
	Tenants []string
}

// reviewData is a change of an app, to look at before it rolls out.
type reviewData struct {
	App api.App
	// Do is what the button does: save (a new spec), restore (an older
	// revision) or update (a newer image); Revision is the one restored.
	Do       string
	Revision int64
	// Title says the change; Diff its lines, Changed whether there are any.
	Title   string
	Diff    []diffLine
	Changed bool
	Added   int
	Removed int
	// File and Updated are the form's, carried to the button.
	File, Updated string
	Tenants       []reviewTenant
	Waves         []wave
	Held          []string
	Archived      []string
	// Overridden are the tenants whose own settings win over a changed
	// field.
	Overridden int
}

// waveNames say each wave as the rollout's plan does.
var waveNames = []string{"Canaries", "A tenth", "Half", "The rest"}

// planWaves splits the live tenants into the waves a rollout moves them
// in, as the rollout driver will.
func planWaves(tenants []api.Tenant) []wave {
	var pending []store.Record
	for _, t := range tenants {
		pending = append(pending, store.Record{Spec: t.Spec, Canary: t.Canary})
	}
	return planFrom(0, len(pending), 0, pending)
}

// planFrom is the waves after wave, of total tenants of which moved have
// moved and pending wait.
func planFrom(from, total, moved int, pending []store.Record) []wave {
	var waves []wave
	for w := from; len(pending) > 0 && w < from+20; w++ {
		next := rollout.NextWave(w, total, moved, pending)
		waves = append(waves, wave{Name: waveName(w), Tenants: next})
		moved += len(next)
		pending = slices.DeleteFunc(pending, func(r store.Record) bool { return slices.Contains(next, r.Spec.Name) })
	}
	return waves
}

// waveName says a wave, from 0.
func waveName(w int) string {
	if w < len(waveNames) {
		return waveNames[w]
	}
	return "Wave " + strconv.Itoa(w+1)
}

// overridesOf names what of its own a tenant keeps over its app's spec.
func overridesOf(o spec.Overrides) []string {
	var out []string
	if o.Image != "" {
		out = append(out, "image")
	}
	for _, key := range slices.Sorted(maps.Keys(o.Env)) {
		out = append(out, key)
	}
	for _, key := range slices.Sorted(maps.Keys(o.Secrets)) {
		out = append(out, key+" (secret)")
	}
	l := o.Limits
	if l.MemoryMB != 0 {
		out = append(out, "memory "+memory(l.MemoryMB))
	}
	if l.CPUs != 0 {
		out = append(out, strconv.FormatFloat(l.CPUs, 'f', -1, 64)+" CPUs")
	}
	if l.PIDs != 0 {
		out = append(out, strconv.Itoa(l.PIDs)+" processes")
	}
	if l.DiskGB != 0 {
		out = append(out, strconv.Itoa(l.DiskGB)+" GB disk")
	}
	return out
}

// review shows a change of app before it is made: from the file shown
// now to file.
func (s *Server) review(w http.ResponseWriter, r *http.Request, app api.App, data reviewData, from, to string) {
	ctx, cancel := context.WithTimeout(r.Context(), 10*time.Second)
	defer cancel()
	tenants, err := s.backend.List(ctx)
	if err != nil {
		s.fail(w, r, err)
		return
	}
	data.App = app
	all := lineDiff(from, to)
	for _, l := range all {
		switch l.Op {
		case "+":
			data.Added++
		case "-":
			data.Removed++
		}
	}
	data.Changed = data.Added+data.Removed > 0
	data.Diff = changedOnly(all, 3)
	var live []api.Tenant
	slices.SortFunc(tenants, func(a, b api.Tenant) int { return strings.Compare(a.Spec.Name, b.Spec.Name) })
	for _, t := range tenants {
		switch {
		case t.App != app.Name:
		case t.Archived():
			data.Archived = append(data.Archived, t.Spec.Name)
		case t.Hold:
			data.Held = append(data.Held, t.Spec.Name)
		default:
			live = append(live, t)
		}
	}
	data.Waves = planWaves(live)
	waveOf := map[string]int{}
	for i, w := range data.Waves {
		for _, name := range w.Tenants {
			waveOf[name] = i + 1
		}
	}
	for _, t := range live {
		rt := reviewTenant{Name: t.Spec.Name, State: tenantState(t), Overrides: overridesOf(t.Overrides), Canary: t.Canary, Wave: waveOf[t.Spec.Name]}
		if len(rt.Overrides) > 0 {
			data.Overridden++
		}
		data.Tenants = append(data.Tenants, rt)
	}
	s.render(w, r, "review", "Review "+app.Name, "apps", data)
}

// rolloutView is an app's rollout under way, for its page: its waves as a
// sequence, each tenant's place in it, and what the next wave waits for.
type rolloutView struct {
	store.Rollout
	Steps []rolloutStep
	Rows  []rolloutRow
	// Blockers are the moved tenants not healthy; Next the tenants of the
	// next wave; Waits says what it waits for.
	Blockers []string
	Next     []string
	Waits    string
	// Moved and Total count the tenants it moves.
	Moved, Total int
	// Before says a blocker broke the same way before the rollout began,
	// so the new revision is likely not the cause.
	Before string
}

type rolloutStep struct {
	Name  string
	Count int
	// State is done, now, paused or next.
	State string
}

type rolloutRow struct {
	Name     string
	State    tag
	From, To int64
	// Wave is the wave it moved in or will, from 1; 0 if not known.
	Wave int
	// Moved says it runs the new revision; Held that it stays.
	Moved, Held, Blocks bool
}

// rolloutOf reads an app's rollout from its tenants and the audit log.
func rolloutOf(app api.App, tenants []api.Tenant, events []store.Event, now time.Time) *rolloutView {
	r := app.Rollout
	if r == nil || !r.Active() {
		return nil
	}
	v := &rolloutView{Rollout: *r}
	movedIn := map[string]int{}
	for _, e := range events {
		if e.Action != "app.rolled-out" || e.App != app.Name {
			continue
		}
		var w int
		if _, err := fmtSscan(e.Detail, &w); err == nil && movedIn[e.Tenant] == 0 {
			movedIn[e.Tenant] = w
		}
	}
	var pending []store.Record
	slices.SortFunc(tenants, func(a, b api.Tenant) int { return strings.Compare(a.Spec.Name, b.Spec.Name) })
	waveCount := map[int]int{}
	for _, t := range tenants {
		if t.App != app.Name || t.Archived() {
			continue
		}
		row := rolloutRow{Name: t.Spec.Name, State: tenantState(t), To: r.ToRevision}
		switch {
		case t.Hold:
			row.Held, row.From, row.To = true, t.AppRevision, t.AppRevision
		case t.AppRevision != 0:
			row.From = t.AppRevision
			pending = append(pending, store.Record{Spec: t.Spec, Canary: t.Canary})
			v.Total++
		case t.BeforeRevision != 0:
			row.Moved, row.From, row.Wave = true, t.BeforeRevision, movedIn[t.Spec.Name]
			if row.Wave == 0 {
				row.Wave = r.Wave
			}
			waveCount[row.Wave]++
			v.Moved++
			v.Total++
			// A tenant still starting is not yet a blocker: one broken is.
			if row.State.Kind == "bad" {
				row.Blocks = true
				v.Blockers = append(v.Blockers, t.Spec.Name)
			}
		default:
			row.From = r.ToRevision
		}
		v.Rows = append(v.Rows, row)
	}
	plan := planFrom(r.Wave, v.Total, v.Moved, pending)
	planned := map[string]int{}
	for i, w := range plan {
		for _, name := range w.Tenants {
			planned[name] = r.Wave + i + 1
		}
	}
	if len(plan) > 0 {
		v.Next = plan[0].Tenants
	}
	for i := range v.Rows {
		if w, ok := planned[v.Rows[i].Name]; ok {
			v.Rows[i].Wave = w
		}
	}
	for w := 1; w <= r.Wave; w++ {
		step := rolloutStep{Name: waveName(w - 1), Count: waveCount[w], State: "done"}
		if w == r.Wave {
			step.State = "now"
			if r.State == store.RolloutPaused {
				step.State = "paused"
			}
		}
		v.Steps = append(v.Steps, step)
	}
	for i, w := range plan {
		v.Steps = append(v.Steps, rolloutStep{Name: waveName(r.Wave + i), Count: len(w.Tenants), State: "next"})
	}
	for _, b := range v.Blockers {
		if n := exitsBefore(events, b, r.StartedAt); n > 0 {
			v.Before = fmt.Sprintf("%s also ran out of memory %s in the 24 hours before revision %d began: the new revision is likely not the cause.", b, plural(n, "time", "times"), r.ToRevision)
			break
		}
	}
	soak := "a minute"
	if r.Wave == 1 {
		soak = "two minutes"
	}
	next := "The rollout ends"
	if len(v.Next) > 0 {
		next = waveName(r.Wave) + " (" + strings.Join(v.Next, ", ") + ") moves"
	}
	switch {
	case r.State == store.RolloutPaused:
		v.Waits = "Paused: nothing moves until it is resumed."
		if r.Reason != "" {
			v.Waits = "Paused: " + r.Reason + ". Nothing moves until it is resumed."
		}
	case len(v.Blockers) > 0:
		v.Waits = next + " once " + strings.Join(v.Blockers, ", ") + " " + map[bool]string{true: "is", false: "are"}[len(v.Blockers) == 1] +
			" healthy on revision " + strconv.FormatInt(r.ToRevision, 10) + " for " + soak + ". A tenant not healthy within 5 minutes of its move pauses the rollout."
	default:
		v.Waits = next + " once the wave that moved " + ago(now, r.WaveStarted) + " ago has stayed healthy for " + soak + "."
	}
	return v
}

// fmtSscan reads the wave from an app.rolled-out event's detail, "wave 2:
// revision 2 → 3".
func fmtSscan(detail string, wave *int) (int, error) { return fmt.Sscanf(detail, "wave %d:", wave) }
