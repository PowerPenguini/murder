package panel

import (
	"context"
	"fmt"
	"maps"
	"net/http"
	"regexp"
	"slices"
	"strconv"
	"strings"
	"time"

	"github.com/PowerPenguini/murder/internal/api"
	"github.com/PowerPenguini/murder/internal/rollout"
	"github.com/PowerPenguini/murder/internal/spec"
	"github.com/PowerPenguini/murder/internal/store"
	"github.com/PowerPenguini/murder/internal/strictyaml"
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
	// Short is Text with long digests cut, "" when it has none.
	Short string
	// Old and New are its line numbers, 0 where it has none.
	Old, New int
}

// digest is a long image digest, shown cut with its whole on demand.
var digest = regexp.MustCompile(`sha256:[0-9a-f]{64}`)

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
			out = append(out, diffLine{Op: " ", Text: x[i], Old: i + 1, New: j + 1})
			i, j = i+1, j+1
		case i < n && (j == m || lcs[i+1][j] >= lcs[i][j+1]):
			out = append(out, diffLine{Op: "-", Text: x[i], Old: i + 1})
			i++
		default:
			out = append(out, diffLine{Op: "+", Text: y[j], New: j + 1})
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
	// Wave is the wave it moves in, from 1; 0 when the change leaves it.
	Wave int
	// Scope is what the change does to it.
	Scope scope
}

// reviewGroup is the tenants a change does the same to, said once.
type reviewGroup struct {
	Tenants []reviewTenant
	Result  string
	// Kind is restart, join or unchanged.
	Kind string
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
	// field; Whole the others, which take the spec whole.
	Overridden int
	Whole      []reviewTenant
	// OldImage and NewImage are the image references, whole, when the
	// change is of the image.
	OldImage, NewImage string
	// Blocked names the tenants that block the rollout this change would
	// replace, and the wave it is at; Moved the tenants that rollout moved,
	// which its Abort takes back.
	Blocked []string
	AtWave  string
	Moved   []string
	// Restart are the tenants that get a new container; Join those that
	// run it already, which the rollout moves on paper; Unchanged those it
	// leaves; Groups all of them, by what it does to them.
	Restart, Join, Unchanged []reviewTenant
	Groups                   []reviewGroup
	// Broken are the restarting tenants broken now, BrokenWave the wave
	// the first moves in with the waves.
	Broken     []string
	BrokenWave string
	// Pace is the pace the page describes: waves, first (the broken now,
	// the rest in waves) or now; Paces those offered, the default marked.
	// ImageTenants are the restarting tenants whose image changes too,
	// ImageChange how (the first of them).
	Pace         string
	Paces        []paceOption
	ImageTenants []string
	ImageChange  string
	// ReviewDo is the action that shows this review again, for another
	// pace; Draft names the spec under review, kept for its address.
	ReviewDo string
	Draft    string
}

// paceOption is one way a change can reach its tenants.
type paceOption struct {
	Value, Label string
	Default      bool
}

// pacesOf are the paces a change can take, and the one proposed: the safe
// one. Every tenant at once is never proposed when an image changes with
// it; the broken tenants first, then the rest in waves, is proposed for a
// change that reaches a broken tenant without a new image for it.
// A broken tenant that would take a new image with it is not rushed: the
// image is not proven, so waves are proposed.
func pacesOf(restart, broken, images []string) ([]paceOption, string) {
	proposed := "waves"
	others := len(broken) > 0 && len(broken) < len(restart)
	brokenImage := slices.ContainsFunc(broken, func(name string) bool { return slices.Contains(images, name) })
	switch {
	case others && !brokenImage:
		proposed = "first"
	case len(broken) > 0 && len(images) == 0:
		proposed = "now"
	}
	paces := []paceOption{{Value: "waves", Label: "In waves"}}
	if others {
		paces = append(paces, paceOption{Value: "first", Label: strings.Join(broken, ", ") + " now, the rest in waves"})
	}
	paces = append(paces, paceOption{Value: "now", Label: "Every tenant now"})
	for i := range paces {
		paces[i].Default = paces[i].Value == proposed
	}
	return paces, proposed
}

// imageOf is the image a spec's file names.
func imageOf(file string) string {
	for _, line := range splitLines(file) {
		if value, ok := strings.CutPrefix(line, "image:"); ok {
			value, _, _ = strings.Cut(value, " #")
			return strings.Trim(strings.TrimSpace(value), `"'`)
		}
	}
	return ""
}

// ImageParts splits an image reference into its repository and a short
// digest or tag.
func ImageParts(ref string) (repository, short string) {
	if repo, digest, ok := strings.Cut(ref, "@sha256:"); ok {
		return repo, "sha256:" + digest[:min(12, len(digest))]
	}
	if strings.HasPrefix(ref, "sha256:") {
		return "", ref[:min(len(ref), 19)]
	}
	if i := strings.LastIndex(ref, ":"); i > strings.LastIndex(ref, "/") {
		return ref[:i], ref[i+1:]
	}
	return ref, ""
}

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

// waveName says a wave by its place among the waves after any moved
// first, from 0: the canaries first (store.WaveName).
func waveName(w int) string { return store.WaveName(w) }

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
	if old, next := imageOf(from), imageOf(to); old != next {
		data.OldImage, data.NewImage = old, next
	}
	if ro := app.Rollout; ro != nil && ro.Active() {
		if v := rolloutOf(app, tenants, nil, s.now()); v != nil {
			data.Blocked, data.AtWave = v.Blockers, ro.WaveName(max(ro.Wave, 1))
			for _, row := range v.Rows {
				if row.Moved {
					data.Moved = append(data.Moved, row.Name)
				}
			}
		}
	}
	// The spec as the store will keep it: its defaults, and the tag the app
	// follows kept at the image it pinned.
	var next spec.Tenant
	_ = strictyaml.Decode([]byte(to), &next)
	if !spec.Pinned(next.Image) && next.Image == app.Tag {
		next.Image = app.Spec.Image
	}
	next = next.WithDefaults()
	for i := range data.Diff {
		if short := digest.ReplaceAllStringFunc(data.Diff[i].Text, func(d string) string { return d[:len("sha256:")+12] + "…" }); short != data.Diff[i].Text {
			data.Diff[i].Short = short
		}
	}
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
	// The rollout moves the tenants whose spec changes, and those it runs
	// already but on another revision; the others it leaves.
	var moving []api.Tenant
	scopes := map[string]scope{}
	for _, t := range live {
		sc := scopeOf(t, app.Spec, next)
		scopes[t.Spec.Name] = sc
		if sc.Restarts || sc.Joins {
			moving = append(moving, t)
		}
	}
	data.Waves = planWaves(moving)
	waveOf := map[string]int{}
	for i, w := range data.Waves {
		for _, name := range w.Tenants {
			waveOf[name] = i + 1
		}
	}
	groups := map[string]int{}
	for _, t := range live {
		sc := scopes[t.Spec.Name]
		rt := reviewTenant{Name: t.Spec.Name, State: tenantState(t), Overrides: overridesOf(t.Overrides), Canary: t.Canary, Wave: waveOf[t.Spec.Name], Scope: sc}
		if len(rt.Overrides) > 0 {
			data.Overridden++
		} else {
			data.Whole = append(data.Whole, rt)
		}
		data.Tenants = append(data.Tenants, rt)
		kind := "unchanged"
		switch {
		case sc.Restarts:
			kind = "restart"
			data.Restart = append(data.Restart, rt)
			if rt.State.Kind == "bad" {
				data.Broken = append(data.Broken, rt.Name)
				if data.BrokenWave == "" && rt.Wave > 0 && rt.Wave <= len(data.Waves) {
					data.BrokenWave = data.Waves[rt.Wave-1].Name
				}
			}
		case sc.Joins:
			kind = "join"
			data.Join = append(data.Join, rt)
		default:
			data.Unchanged = append(data.Unchanged, rt)
		}
		key := kind + "\x00" + sc.Result()
		if i, ok := groups[key]; ok {
			data.Groups[i].Tenants = append(data.Groups[i].Tenants, rt)
			continue
		}
		groups[key] = len(data.Groups)
		data.Groups = append(data.Groups, reviewGroup{Tenants: []reviewTenant{rt}, Result: sc.Result(), Kind: kind})
	}
	order := map[string]int{"restart": 0, "join": 1, "unchanged": 2}
	slices.SortStableFunc(data.Groups, func(a, b reviewGroup) int { return order[a.Kind] - order[b.Kind] })
	// Which tenants take a new image with it: their own composed result
	// says, not this diff (a tenant still on an older revision takes the
	// newer image with a limits fix).
	var restartNames []string
	for _, rt := range data.Restart {
		restartNames = append(restartNames, rt.Name)
		for _, c := range rt.Scope.Changes {
			if c.Key == "image" {
				data.ImageTenants = append(data.ImageTenants, rt.Name)
				if data.ImageChange == "" {
					data.ImageChange = c.From + " → " + c.To
				}
			}
		}
	}
	var proposed string
	data.Paces, proposed = pacesOf(restartNames, data.Broken, data.ImageTenants)
	data.Pace = proposed
	if asked := r.FormValue("pace"); slices.ContainsFunc(data.Paces, func(p paceOption) bool { return p.Value == asked }) {
		data.Pace = asked
	}
	data.ReviewDo = map[string]string{"save": "review", "restore": "review-restore", "update": "review-update"}[data.Do]
	// The plan the page shows is the pace's.
	switch data.Pace {
	case "now":
		data.Waves = nil
	case "first":
		var pending []store.Record
		for _, t := range moving {
			if !slices.Contains(data.Broken, t.Spec.Name) {
				pending = append(pending, store.Record{Spec: t.Spec, Canary: t.Canary})
			}
		}
		// The canaries' wave comes next, as the driver moves it.
		data.Waves = append([]wave{{Name: "First, now", Tenants: data.Broken}}, planFrom(0, len(moving), len(data.Broken), pending)...)
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
	// State is done, now, blocked, paused or next; Blockers block it.
	State    string
	Blockers []string
}

// Said says how the wave goes: "moved", "blocked by baltic-freight".
func (s rolloutStep) Said() string {
	switch s.State {
	case "done":
		return "moved"
	case "now":
		return "moving, watched"
	case "blocked":
		return "blocked by " + strings.Join(s.Blockers, ", ")
	case "paused":
		return "paused"
	}
	return "waits"
}

type rolloutRow struct {
	Name     string
	State    tag
	From, To int64
	// Wave is the wave it moved in or will, from 1; 0 if not known.
	Wave int
	// Moved says it runs the new revision; Held that it stays. WaveName
	// names its wave as the rollout does.
	Moved, Held, Blocks bool
	WaveName            string
	// Result says how its move goes, from its health now, and Kind its
	// colour: "moved, healthy" only for a check that passes now.
	Result, Kind string
}

// healthFresh is how old a passing check may be for a moved tenant to be
// said healthy: older, it is not known.
const healthFresh = 2 * time.Minute

// movedResult says how a moved tenant does on its new revision, from how
// it runs now.
func movedResult(t api.Tenant, state tag, now time.Time) (string, string) {
	s := t.Status
	switch {
	case s == nil:
		return "moved; how it runs is not known: its node does not answer", "warn"
	case state.Kind == "bad":
		return "moved, not healthy: blocks the next wave", "bad"
	case s.Phase == "starting" || (s.Generation != 0 && s.Generation < t.Generation) || state.Kind == "busy":
		return "moved, starting: waiting for its health check", "busy"
	case s.Phase == "running" && s.Healthy && !s.CheckedAt.IsZero() && now.Sub(s.CheckedAt) <= healthFresh:
		return "moved, healthy", "ok"
	case s.Phase == "running" && s.Healthy:
		return "moved; its last check passed " + ago(now, s.CheckedAt) + " ago, none since", "warn"
	}
	return "moved, " + state.Label, state.Kind
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
			row.Result, row.Kind = movedResult(t, row.State, now)
			if row.State.Kind == "bad" {
				row.Blocks = true
				v.Blockers = append(v.Blockers, t.Spec.Name)
			}
		default:
			row.From = r.ToRevision
		}
		v.Rows = append(v.Rows, row)
	}
	// Planned as the driver moves them: waves count after those moved first.
	plan := planFrom(r.Wave-r.First, v.Total, v.Moved, pending)
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
		if v.Rows[i].Wave > 0 {
			v.Rows[i].WaveName = r.WaveName(v.Rows[i].Wave)
		}
	}
	for w := 1; w <= r.Wave; w++ {
		step := rolloutStep{Name: r.WaveName(w), Count: waveCount[w], State: "done"}
		if w == r.Wave {
			step.State = "now"
			switch {
			case r.State == store.RolloutPaused:
				step.State = "paused"
			case len(v.Blockers) > 0:
				step.State, step.Blockers = "blocked", v.Blockers
			}
		}
		v.Steps = append(v.Steps, step)
	}
	for i, w := range plan {
		v.Steps = append(v.Steps, rolloutStep{Name: r.WaveName(r.Wave + 1 + i), Count: len(w.Tenants), State: "next"})
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
		next = r.WaveName(r.Wave+1) + " (" + strings.Join(v.Next, ", ") + ") moves"
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
func fmtSscan(detail string, wave *int) (int, error) { return fmt.Sscanf(detail, "wave %d", wave) }
