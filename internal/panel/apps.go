package panel

import (
	"context"
	"errors"
	"fmt"
	"maps"
	"math"
	"mime"
	"net/http"
	"net/url"
	"slices"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/PowerPenguini/murder/internal/api"
	"github.com/PowerPenguini/murder/internal/spec"
	"github.com/PowerPenguini/murder/internal/store"
	"github.com/PowerPenguini/murder/internal/strictyaml"
)

// AppAgent is what editing apps and tenants' settings asks of the agent,
// for the person signed in. Secrets' values are never asked for: the panel
// shows their names, and rotates them.
type AppAgent interface {
	Apps(ctx context.Context) ([]api.App, error)
	App(ctx context.Context, name string) (api.App, error)
	PutAppIf(ctx context.Context, name string, app spec.Tenant, cond store.AppIf) (api.AppApplied, error)
	DeleteApp(ctx context.Context, name string) error
	CheckApp(ctx context.Context, name string) (api.AppCheck, error)
	UpdateApp(ctx context.Context, name string, updatedAt time.Time) (api.AppApplied, error)
	AppRevisions(ctx context.Context, name string) ([]store.Revision, error)
	RestoreRevision(ctx context.Context, name string, revision int64, updatedAt time.Time) (api.AppApplied, error)
	Get(ctx context.Context, name string) (api.Tenant, error)
	Configure(ctx context.Context, name string, request api.TenantRequest) (api.Applied, error)
	Rotate(ctx context.Context, name, key string) (api.Tenant, error)
	RolloutAction(ctx context.Context, name, action string) (api.App, error)
	RestoreRollout(ctx context.Context, name string, tenants []string) (api.RolloutRestored, error)
	SetFlags(ctx context.Context, name string, flags api.TenantFlags) (api.Tenant, error)
}

// paced is the agent an app change asked for goes through: one that moves
// every tenant at once when the review chose "now", else in waves.
func paced(agent AppAgent, r *http.Request) AppAgent {
	switch r.FormValue("pace") {
	case "now":
		switch a := agent.(type) {
		case interface{ AtOnce() *api.Client }:
			return a.AtOnce()
		case interface{ AppsAtOnce() AppAgent }:
			return a.AppsAtOnce()
		}
	case "first":
		// The tenants the review named; the store moves only those the
		// change reaches.
		var first []string
		for _, name := range strings.Split(r.FormValue("first"), ",") {
			if name = strings.TrimSpace(name); name != "" {
				first = append(first, name)
			}
		}
		switch a := agent.(type) {
		case interface{ First([]string) *api.Client }:
			return a.First(first)
		case interface{ AppsFirst([]string) AppAgent }:
			return a.AppsFirst(first)
		}
	}
	return agent
}

// formLimit bounds an app's or settings' form.
const formLimit = 256 << 10

// newAppFile is what a new app's form starts with.
const newAppFile = `image: ghcr.io/example/web:1.0   # a tag; pinned to a digest when stored
user: "10001:10001"              # numeric uid:gid, never root
healthPath: /
env:
  PUBLIC_URL: "{{url}}"
secrets:
  SESSION_SECRET: hex
`

type appsData struct {
	Apps []api.App
	// Bars draw each app's tenants against the most any app has.
	Bars []meter
	// Name and File are a new app's form, kept when it was refused.
	Name, File string
	// States are what is wrong with each app, by name.
	States map[string]appState
}

func (s *Server) apps(w http.ResponseWriter, r *http.Request) {
	s.appsPage(w, r, appsData{File: newAppFile}, "", http.StatusOK)
}

func (s *Server) appsPage(w http.ResponseWriter, r *http.Request, data appsData, problem string, status int) {
	ctx, cancel := context.WithTimeout(r.Context(), 10*time.Second)
	defer cancel()
	apps, err := s.Apps(s.actor(userOf(r))).Apps(ctx)
	if err != nil {
		s.fail(w, r, err)
		return
	}
	var tenants []api.Tenant
	var events []store.Event
	_ = all(
		func() (err error) { tenants, err = s.backend.List(ctx); return },
		func() (err error) {
			events, err = s.backend.Events(ctx, store.EventQuery{Limit: activityEvents})
			return
		},
	)
	_, _, data.States = joinApps(nil, apps, tenants, events, s.now(), "")
	// The broken first, then by name.
	slices.SortFunc(apps, func(a, b api.App) int {
		if x, y := data.States[a.Name].Bad, data.States[b.Name].Bad; x != y {
			if x {
				return -1
			}
			return 1
		}
		return strings.Compare(a.Name, b.Name)
	})
	data.Apps = apps
	most := 0
	for _, app := range apps {
		most = max(most, len(app.Tenants))
	}
	for _, app := range apps {
		data.Bars = append(data.Bars, meterOf(len(app.Tenants), most))
	}
	s.renderWith(w, r, "apps", "Apps", "apps", data, problem, status)
}

// createApp stores a new app; one of the name stored meanwhile is not
// replaced.
func (s *Server) createApp(w http.ResponseWriter, r *http.Request) {
	if !readForm(w, r) {
		return
	}
	agent := s.Apps(s.actor(userOf(r)))
	name, file := strings.TrimSpace(r.FormValue("name")), r.FormValue("file")
	refuse := func(problem string) {
		s.appsPage(w, r, appsData{Name: name, File: file}, problem, http.StatusUnprocessableEntity)
	}
	app, err := readApp(name, file)
	if err != nil {
		refuse(err.Error())
		return
	}
	ctx, cancel := context.WithTimeout(r.Context(), 2*time.Minute)
	defer cancel()
	if _, err := agent.App(ctx, name); err == nil {
		refuse("app " + name + " exists already: edit it on its page")
		return
	} else if !api.IsNotFound(err) {
		refuse(err.Error())
		return
	}
	// Checked again as it is stored: someone may create it meanwhile.
	applied, err := agent.PutAppIf(ctx, name, app, store.AppIf{Absent: true})
	if err != nil {
		refuse(err.Error())
		return
	}
	flash(w, r, "done", appOutcome(name, applied))
	http.Redirect(w, r, "/apps/"+url.PathEscape(name), http.StatusSeeOther)
}

// readApp reads an app's file as `murder app apply` does.
func readApp(name, file string) (spec.Tenant, error) {
	var app spec.Tenant
	if err := spec.ValidLabel(name); err != nil {
		return app, fmt.Errorf("the app's name: %w", err)
	}
	if err := strictyaml.Decode([]byte(file), &app); err != nil {
		return app, fmt.Errorf("the spec: %w", err)
	}
	return app, spec.CheckApp(name, app, spec.Tenant.ValidateUnpinned)
}

func appOutcome(name string, applied api.AppApplied) string {
	var message string
	switch {
	case applied.Created:
		message = "app " + name + " is created"
	case !applied.Changed:
		message = "app " + name + " is unchanged"
	case len(applied.Tenants) == 0:
		message = "app " + name + " is changed; no live tenant runs it"
	case applied.Rollout && applied.App.Rollout != nil && applied.App.Rollout.First > 0:
		// Those moved first, by name.
		var first []string
		for _, t := range applied.Tenants {
			if t.AppRevision == 0 && t.BeforeRevision != 0 {
				first = append(first, t.Spec.Name)
			}
		}
		message = fmt.Sprintf("app %s is changed: %s restart%s now, %d more in waves", name, strings.Join(first, ", "), map[bool]string{true: "s", false: ""}[len(first) == 1], len(applied.Tenants)-len(first))
	case applied.Rollout:
		message = fmt.Sprintf("app %s is changed; it rolls out in waves to %d tenants", name, len(applied.Tenants))
	default:
		message = fmt.Sprintf("app %s is changed; it rolls out to %d tenants", name, len(applied.Tenants))
	}
	if applied.Note != "" {
		message += " (" + applied.Note + ")"
	}
	return message
}

type appData struct {
	App api.App
	// File is the spec in the editor: the stored one, or what was refused.
	File string
	// Updated is when the app read changed last, to refuse a save over
	// someone else's change.
	Updated string
	// Archived are its archived tenants: they keep it from being deleted.
	Archived []string
	// Revisions are its recent revisions, newest first.
	Revisions []store.Revision
	// Restoring are its tenants waiting for their data to go back.
	Restoring []string
	// Rollout is its rollout under way, if one is.
	Rollout *rolloutView
	// EditorProblem is what is wrong with the spec in the editor, said
	// beside it; EditorLine its line, 0 if none.
	EditorProblem string
	EditorLine    int
	EditorLead    string
	// Rows are its tenants, with their state.
	Rows []tenantRow
	// Memory says its tenants run out of memory; Proposed that the editor
	// holds a proposed change, from a link.
	Memory   *appMemory
	Proposed string
	// UpdateRestarts and UpdateHeld count, for the newer image, the live
	// tenants it restarts and those held and left out, as its review does.
	UpdateRestarts, UpdateHeld int
	// Outcome is what a person's change of it did, lately; Me who reads.
	Outcome *outcome
	Me      string
}

// Red says something on the app's page is wrong: a blocked rollout, a
// limit that breaks a tenant. Its fix is then the page's filled button,
// not the newer image's review.
func (d appData) Red() bool {
	return (d.Rollout != nil && len(d.Rollout.Blockers) > 0) || (d.Memory != nil && d.Memory.Bad)
}

func (s *Server) app(w http.ResponseWriter, r *http.Request) {
	ctx, cancel := context.WithTimeout(r.Context(), 10*time.Second)
	defer cancel()
	app, err := s.Apps(s.actor(userOf(r))).App(ctx, r.PathValue("name"))
	if err != nil {
		s.fail(w, r, err)
		return
	}
	file, err := shownFile(app)
	if err != nil {
		s.fail(w, r, err)
		return
	}
	data := appData{App: app, File: file, Updated: stampOf(app.UpdatedAt)}
	// A link that proposes a memory limit ("Raise web's memory limit")
	// puts it in the editor, to be reviewed.
	if mb, err := strconv.Atoi(r.URL.Query().Get("memoryMB")); err == nil && mb > 0 && mb <= 1<<20 && mb != app.Spec.Limits.MemoryMB {
		proposed := app
		proposed.Spec.Limits.MemoryMB = mb
		if file, err := shownFile(proposed); err == nil {
			data.File = file
			data.Proposed = "Proposed in the editor below: memory " + memory(mb) + " for every tenant of " + app.Name + ", up from " + memory(app.Spec.Limits.MemoryMB) + ". Nothing changes until you review it and roll it out."
		}
	}
	s.appPage(w, r, data, "", http.StatusOK)
}

// shownFile is an app's spec as its editor shows it. An app given a tag
// shows the tag: saving the file keeps the image it is pinned to, and the
// tag is still asked about.
func shownFile(app api.App) (string, error) {
	shown := app.Spec
	if app.Tag != "" {
		shown.Image = app.Tag
	}
	file, err := spec.AppFile(shown)
	return string(file), err
}

// appPage shows an app, with the archived tenants that keep it.
func (s *Server) appPage(w http.ResponseWriter, r *http.Request, data appData, problem string, status int) {
	ctx, cancel := context.WithTimeout(r.Context(), 10*time.Second)
	defer cancel()
	var tenants []api.Tenant
	var events []store.Event
	if err := all(
		func() (err error) { tenants, err = s.backend.List(ctx); return },
		func() (err error) {
			events, err = s.backend.Events(ctx, store.EventQuery{App: data.App.Name, Limit: activityEvents})
			return
		},
	); err != nil {
		s.fail(w, r, err)
		return
	}
	data.Rollout = rolloutOf(data.App, tenants, events, s.now())
	data.Memory = appMemoryOf(data.App, tenants, events, s.now())
	slices.SortFunc(tenants, func(a, b api.Tenant) int { return strings.Compare(a.Spec.Name, b.Spec.Name) })
	for _, t := range tenants {
		if t.App == data.App.Name {
			data.Rows = append(data.Rows, tenantRow{Tenant: t, State: tenantState(t)})
		}
		if t.App == data.App.Name && t.Archived() {
			data.Archived = append(data.Archived, t.Spec.Name)
		}
		if t.App == data.App.Name && t.Restoring {
			data.Restoring = append(data.Restoring, t.Spec.Name)
		}
	}
	slices.Sort(data.Archived)
	slices.Sort(data.Restoring)
	// What the newer image's review will count: the tenants it restarts,
	// of the live ones; the held ones left out.
	if data.App.Latest != "" {
		next := data.App.Spec
		next.Image = data.App.Latest
		for _, row := range data.Rows {
			t := row.Tenant
			switch {
			case t.Archived():
			case t.Hold:
				data.UpdateHeld++
			case scopeOf(t, data.App.Spec, next).Restarts:
				data.UpdateRestarts++
			}
		}
	}
	var err error
	if data.Revisions, err = s.Apps(s.actor(userOf(r))).AppRevisions(ctx, data.App.Name); err != nil {
		s.fail(w, r, err)
		return
	}
	data.Outcome = outcomeOf(data.App, data.Revisions, tenants, events, s.now())
	data.Me = s.actor(userOf(r))
	// What is wrong with the spec is said beside the editor, in plain
	// words, with its line.
	if data.File != "" && problem != "" {
		data.EditorLead = "Not taken."
		data.EditorProblem = problem
		if strings.HasPrefix(problem, "the spec") {
			data.EditorLead = "Specification has errors."
			data.EditorProblem, data.EditorLine = specProblem(problem, data.File)
		}
		problem = ""
	}
	s.renderWith(w, r, "app", data.App.Name, "apps", data, problem, status)
}

func stampOf(t time.Time) string { return t.UTC().Format(time.RFC3339Nano) }

func (s *Server) appAction(w http.ResponseWriter, r *http.Request) {
	if !readForm(w, r) {
		return
	}
	name := r.PathValue("name")
	agent := s.Apps(s.actor(userOf(r)))
	ctx, cancel := context.WithTimeout(r.Context(), 2*time.Minute)
	defer cancel()
	current, err := agent.App(ctx, name)
	if err != nil {
		s.fail(w, r, err)
		return
	}
	back := func(kind, message string) {
		flash(w, r, kind, message)
		http.Redirect(w, r, "/apps/"+url.PathEscape(name), http.StatusSeeOther)
	}
	switch r.FormValue("do") {
	case "edit":
		// Back from a review to the editor, the text kept.
		s.appPage(w, r, appData{App: current, File: r.FormValue("file"), Updated: r.FormValue("updated")}, "", http.StatusOK)
	case "review":
		// Checked here; shown at its own address, so that a reload or the
		// back button shows it again rather than sending the form again.
		file := r.FormValue("file")
		refuse := func(problem string) {
			s.appPage(w, r, appData{App: current, File: file, Updated: r.FormValue("updated")}, problem, http.StatusUnprocessableEntity)
		}
		if current.UpdatedAt.IsZero() || r.FormValue("updated") != stampOf(current.UpdatedAt) {
			refuse("someone changed the app since you opened it: your text is below; reload the page for theirs")
			return
		}
		if _, err := readApp(name, file); err != nil {
			refuse(err.Error())
			return
		}
		q := url.Values{"do": {"review"}, "updated": {r.FormValue("updated")}, "draft": {s.drafts.keep(name, file)}}
		if pace := r.FormValue("pace"); pace != "" {
			q.Set("pace", pace)
		}
		http.Redirect(w, r, "/apps/"+url.PathEscape(name)+"/review?"+q.Encode(), http.StatusSeeOther)
	case "review-restore", "review-update":
		q := url.Values{"do": {r.FormValue("do")}, "updated": {r.FormValue("updated")}}
		for _, key := range []string{"revision", "pace"} {
			if v := r.FormValue(key); v != "" {
				q.Set(key, v)
			}
		}
		http.Redirect(w, r, "/apps/"+url.PathEscape(name)+"/review?"+q.Encode(), http.StatusSeeOther)
	case "save":
		file := r.FormValue("file")
		refuse := func(problem string) {
			s.appPage(w, r, appData{App: current, File: file, Updated: r.FormValue("updated")}, problem, http.StatusUnprocessableEntity)
		}
		// Without its time, the store could not refuse a change made meanwhile.
		if current.UpdatedAt.IsZero() || r.FormValue("updated") != stampOf(current.UpdatedAt) {
			refuse("someone changed the app since you opened it: your text is below; reload the page for theirs")
			return
		}
		app, err := readApp(name, file)
		if err != nil {
			refuse(err.Error())
			return
		}
		// Checked again as it is stored, after the image is pulled.
		applied, err := paced(agent, r).PutAppIf(ctx, name, app, store.AppIf{UpdatedAt: current.UpdatedAt})
		if err != nil {
			refuse(err.Error())
			return
		}
		back("done", appOutcome(name, applied))
	case "pause", "resume", "abort", "promote":
		action := r.FormValue("do")
		if action == "abort" && !typed(r.FormValue("confirm"), name) {
			back("problem", "type the app's name to take every tenant back")
			return
		}
		// Moving the next wave skips its health gate: the name is typed.
		if action == "promote" && !typed(r.FormValue("confirm"), name) {
			back("problem", "type the app's name to move the next wave without waiting for its health check")
			return
		}
		if _, err := agent.RolloutAction(ctx, name, action); err != nil {
			back("problem", err.Error())
			return
		}
		back("done", map[string]string{"pause": "the rollout is paused", "resume": "the rollout goes on", "abort": "the rollout is aborted: its tenants go back, their data as it is", "promote": "the next wave moves now, without waiting for the one before to stay healthy"}[action])
	case "restore-data":
		if !typed(r.FormValue("confirm"), name) {
			back("problem", "type the app's name to take its tenants back with their data")
			return
		}
		restored, err := agent.RestoreRollout(ctx, name, nil)
		if err != nil {
			back("problem", err.Error())
			return
		}
		back("done", strings.Join(restored.Tenants, ", ")+" go back to the revision before with their data from before they moved; the rollout is paused")
	case "check":
		check, err := agent.CheckApp(ctx, name)
		switch {
		case err != nil:
			back("problem", err.Error())
		case check.Newer:
			back("done", "a newer image of "+check.App.Tag+" is in its registry: "+imageDigest(check.Image))
		default:
			back("done", "app "+name+" runs the newest image of "+check.App.Tag)
		}
	case "update", "restore":
		// An update or a revision is taken over the app as it was shown.
		if current.UpdatedAt.IsZero() || r.FormValue("updated") != stampOf(current.UpdatedAt) {
			back("problem", "someone changed the app since you opened it: look again, then try again")
			return
		}
		var applied api.AppApplied
		if r.FormValue("do") == "update" {
			applied, err = paced(agent, r).UpdateApp(ctx, name, current.UpdatedAt)
		} else {
			revision, parseErr := strconv.ParseInt(r.FormValue("revision"), 10, 64)
			if parseErr != nil {
				back("problem", "no such revision")
				return
			}
			applied, err = paced(agent, r).RestoreRevision(ctx, name, revision, current.UpdatedAt)
		}
		if err != nil {
			back("problem", err.Error())
			return
		}
		back("done", appOutcome(name, applied))
	case "delete":
		if !typed(r.FormValue("confirm"), name) {
			back("problem", "type the app's name to delete it")
			return
		}
		if err := agent.DeleteApp(ctx, name); err != nil {
			back("problem", err.Error())
			return
		}
		flash(w, r, "done", "app "+name+" is deleted")
		http.Redirect(w, r, "/apps", http.StatusSeeOther)
	default:
		back("problem", "unknown action")
	}
}

// settingsForm is a tenant's settings as the form shows them.
type settingsForm struct {
	App, Image, Env, Secrets     string
	MemoryMB, CPUs, PIDs, DiskGB string
	Generation                   int64
	// Canary and Hold are its rollout flags, "on" or not; Flags says the
	// form said them.
	Canary, Hold string
	Flags        bool
	// Empty are the settings whose Override is on with no value: said
	// back, never taken as inheriting again. Open is a limit's field asked
	// open for a value of its own, from a link (?set=diskGB).
	Empty string
	Open  string
}

// empty lists the settings sent with Override on and no value.
func (f settingsForm) empty() []string {
	if f.Empty == "" {
		return nil
	}
	return strings.Split(f.Empty, ",")
}

// onOff is a flag as a form says it.
func onOff(on bool) string {
	if on {
		return "on"
	}
	return ""
}

// withFlags is the form with the tenant's flags, when it did not say them.
func (f settingsForm) withFlags(t api.Tenant) settingsForm {
	if !f.Flags {
		f.Canary, f.Hold, f.Flags = onOff(t.Canary), onOff(t.Hold), true
	}
	return f
}

// flagsChanged says the form changes the tenant's rollout flags.
func (f settingsForm) flagsChanged(t api.Tenant) bool {
	return f.Canary != onOff(t.Canary) || f.Hold != onOff(t.Hold)
}

// containerChanged says the form changes what the tenant's container is
// made from: its app or overrides, not its rollout flags.
func (f settingsForm) containerChanged(t api.Tenant) bool {
	was := formOf(t)
	f.Canary, f.Hold, f.Flags, f.Empty, f.Open = was.Canary, was.Hold, was.Flags, "", ""
	return f != was
}

// diskOnly says the form changes the tenant's disk limit and nothing else
// its container is made of: its node sets the limit on its data as it
// runs (the reconciler leaves it out of the container's inputs).
func (f settingsForm) diskOnly(t api.Tenant) bool {
	was := formOf(t)
	if f.DiskGB == was.DiskGB {
		return false
	}
	f.DiskGB = was.DiskGB
	return !f.containerChanged(t)
}

func formOf(t api.Tenant) settingsForm {
	o := t.Overrides
	form := settingsForm{App: t.App, Image: o.Image, Generation: t.Generation, Canary: onOff(t.Canary), Hold: onOff(t.Hold), Flags: true}
	var lines []string
	for _, key := range slices.Sorted(maps.Keys(o.Env)) {
		lines = append(lines, key+"="+envValue(o.Env[key]))
	}
	form.Env = strings.Join(lines, "\n")
	lines = nil
	for _, key := range slices.Sorted(maps.Keys(o.Secrets)) {
		line := key + " " + o.Secrets[key].Kind
		if n := o.Secrets[key].Bytes; n > 0 {
			line += " " + strconv.Itoa(n)
		}
		lines = append(lines, line)
	}
	form.Secrets = strings.Join(lines, "\n")
	number := func(n int) string {
		if n == 0 {
			return ""
		}
		return strconv.Itoa(n)
	}
	form.MemoryMB, form.PIDs, form.DiskGB = number(o.Limits.MemoryMB), number(o.Limits.PIDs), number(o.Limits.DiskGB)
	if o.Limits.CPUs != 0 {
		form.CPUs = strconv.FormatFloat(o.Limits.CPUs, 'f', -1, 64)
	}
	return form
}

func formFrom(r *http.Request) settingsForm {
	generation, _ := strconv.ParseInt(r.FormValue("generation"), 10, 64)
	form := settingsForm{
		App: r.FormValue("app"), Image: strings.TrimSpace(r.FormValue("image")), Env: r.FormValue("env"), Secrets: r.FormValue("secrets"),
		MemoryMB: strings.TrimSpace(r.FormValue("memoryMB")), CPUs: strings.TrimSpace(r.FormValue("cpus")),
		PIDs: strings.TrimSpace(r.FormValue("pids")), DiskGB: strings.TrimSpace(r.FormValue("diskGB")), Generation: generation,
	}
	// Its rollout flags, when the form says them.
	if r.FormValue("flags") != "" {
		form.Canary, form.Hold, form.Flags = onOff(r.FormValue("canary") == "on"), onOff(r.FormValue("hold") == "on"), true
	}
	// The environment as the page's table posts it, a row each: a name
	// and its own value, empty to take the app's; the last row adds one.
	// Read as the NAME=value lines the review carries.
	if r.FormValue("envrows") != "" {
		// A row's value is its own while its switch is on; off, the app's
		// is used again. Rows added say a name and a value.
		owned := map[string]bool{}
		for _, name := range r.Form["envown"] {
			owned[name] = true
		}
		var lines []string
		line := func(name, value string) {
			// A value shown quoted, as one with a line break is, reads
			// back as it was shown; any other as typed.
			if !strings.HasPrefix(value, `"`) {
				value = envValue(value)
			}
			lines = append(lines, name+"="+value)
		}
		names, values := r.Form["envname"], r.Form["envvalue"]
		for i, name := range names {
			if i < len(values) && owned[name] {
				line(name, values[i])
			}
		}
		news, newValues := r.Form["envnewname"], r.Form["envnewvalue"]
		for i, name := range news {
			if name = strings.TrimSpace(name); name != "" && i < len(newValues) {
				line(name, newValues[i])
			}
		}
		form.Env = strings.Join(lines, "\n")
	}
	// The secrets as the page's table posts them: a switch, a kind and a
	// length for each, and rows added; read as the lines the review carries.
	if r.FormValue("secretrows") != "" {
		owned := map[string]bool{}
		for _, name := range r.Form["secretown"] {
			owned[name] = true
		}
		var lines []string
		line := func(name, kind, bytes string) {
			text := name + " " + strings.TrimSpace(kind)
			if b := strings.TrimSpace(bytes); b != "" && b != "0" {
				text += " " + b
			}
			lines = append(lines, text)
		}
		names, kinds, sizes := r.Form["secretname"], r.Form["secretkind"], r.Form["secretbytes"]
		for i, name := range names {
			if owned[name] && i < len(kinds) && i < len(sizes) {
				line(name, kinds[i], sizes[i])
			}
		}
		news, newKinds, newSizes := r.Form["secretnewname"], r.Form["secretnewkind"], r.Form["secretnewbytes"]
		for i, name := range news {
			if name = strings.TrimSpace(name); name != "" && i < len(newKinds) && i < len(newSizes) {
				line(name, newKinds[i], newSizes[i])
			}
		}
		form.Secrets = strings.Join(lines, "\n")
	}
	// The page's form says, by a switch each, which values are the
	// tenant's own: one switched off takes the app's again, whatever its
	// field holds.
	if r.FormValue("owns") != "" {
		for _, name := range []string{"image", "memoryMB", "cpus", "pids", "diskGB"} {
			field := map[string]*string{"image": &form.Image, "memoryMB": &form.MemoryMB, "cpus": &form.CPUs, "pids": &form.PIDs, "diskGB": &form.DiskGB}[name]
			switch {
			case r.FormValue("own-"+name) == "":
				*field = ""
			case *field == "":
				form.Empty = strings.TrimPrefix(form.Empty+","+name, ",")
			}
		}
	}
	return form
}

// overrides reads the form: env as NAME=value lines, secrets as "NAME
// [kind [bytes]]" lines, limits as numbers, empty for the app's.
func (f settingsForm) overrides() (spec.Overrides, error) {
	o := spec.Overrides{Image: f.Image}
	var problems []string
	for i, line := range strings.Split(f.Env, "\n") {
		line = strings.TrimRight(line, "\r")
		if strings.TrimSpace(line) == "" || strings.HasPrefix(strings.TrimSpace(line), "#") {
			continue
		}
		key, value, ok := strings.Cut(line, "=")
		key, value = strings.TrimSpace(key), strings.TrimSpace(value)
		if strings.HasPrefix(value, `"`) {
			unquoted, err := strconv.Unquote(value)
			if err != nil {
				problems = append(problems, fmt.Sprintf(`env line %d: a value beginning with " is quoted, as "a\nb"`, i+1))
				continue
			}
			value = unquoted
		}
		switch {
		case !ok || key == "":
			problems = append(problems, fmt.Sprintf("env line %d: NAME=value", i+1))
		case hasKey(o.Env, key):
			problems = append(problems, fmt.Sprintf("env line %d: %s is set twice", i+1, key))
		default:
			if o.Env == nil {
				o.Env = map[string]string{}
			}
			o.Env[key] = value
		}
	}
	for i, line := range strings.Split(f.Secrets, "\n") {
		fields := strings.Fields(line)
		if len(fields) == 0 || strings.HasPrefix(fields[0], "#") {
			continue
		}
		secret := spec.Secret{Kind: "hex"}
		if len(fields) > 1 {
			secret.Kind = fields[1]
		}
		if len(fields) > 2 {
			n, err := strconv.Atoi(fields[2])
			if err != nil || n < 1 {
				problems = append(problems, fmt.Sprintf("secrets line %d: the length is a positive number", i+1))
				continue
			}
			secret.Bytes = n
		}
		if len(fields) > 3 {
			problems = append(problems, fmt.Sprintf("secrets line %d: NAME [kind [length]]", i+1))
			continue
		}
		if _, twice := o.Secrets[fields[0]]; twice {
			problems = append(problems, fmt.Sprintf("secrets line %d: %s is listed twice", i+1, fields[0]))
			continue
		}
		if o.Secrets == nil {
			o.Secrets = spec.Secrets{}
		}
		o.Secrets[fields[0]] = secret
	}
	whole := func(field, value string, into *int) {
		if value == "" {
			return
		}
		n, err := strconv.Atoi(value)
		if err != nil || n < 0 {
			problems = append(problems, field+": a whole number, or empty for the app's")
			return
		}
		*into = n
	}
	whole("memory", f.MemoryMB, &o.Limits.MemoryMB)
	whole("pids", f.PIDs, &o.Limits.PIDs)
	whole("disk", f.DiskGB, &o.Limits.DiskGB)
	if f.CPUs != "" {
		cpus, err := strconv.ParseFloat(f.CPUs, 64)
		if err != nil || cpus < 0 || math.IsNaN(cpus) || math.IsInf(cpus, 0) {
			problems = append(problems, "cpus: a number, or empty for the app's")
		}
		o.Limits.CPUs = cpus
	}
	if len(problems) > 0 {
		return o, errors.New(strings.Join(problems, "; "))
	}
	return o, nil
}

// envValue is a value as an env line says it: quoted, as Go quotes, when
// it would not read back as itself (a line break, a space at an end, a
// quote first).
func envValue(value string) string {
	if strings.ContainsAny(value, "\r\n") || strings.TrimSpace(value) != value || strings.HasPrefix(value, `"`) {
		return strconv.Quote(value)
	}
	return value
}

// readForm reads a form of at most formLimit, saying so if it is larger.
func readForm(w http.ResponseWriter, r *http.Request) bool {
	if r.PostForm == nil {
		r.Body = http.MaxBytesReader(w, r.Body, formLimit)
	}
	if err := r.ParseForm(); err != nil {
		status := http.StatusBadRequest
		if errors.As(err, new(*http.MaxBytesError)) {
			status = http.StatusRequestEntityTooLarge
		}
		http.Error(w, fmt.Sprintf("the form is not read: %v (at most %d KB)", err, formLimit>>10), status)
		return false
	}
	// A browser sends a form's line breaks as CRLF (a textarea, a hidden
	// field that holds lines): read as the LF the panel writes, so a value
	// sent back unchanged is unchanged.
	for _, values := range []url.Values{r.Form, r.PostForm} {
		for key, list := range values {
			for i, v := range list {
				if strings.ContainsRune(v, '\r') {
					list[i] = strings.ReplaceAll(strings.ReplaceAll(v, "\r\n", "\n"), "\r", "\n")
				}
			}
			values[key] = list
		}
	}
	return true
}

// formText reads every form a page posts as readForm does, before its
// handler: line breaks as LF, wherever the form is read.
func formText(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method == http.MethodPost {
			if media, _, _ := mime.ParseMediaType(r.Header.Get("Content-Type")); media == "application/x-www-form-urlencoded" && !readForm(w, r) {
				return
			}
		}
		next.ServeHTTP(w, r)
	})
}

func hasKey(m map[string]string, key string) bool {
	_, ok := m[key]
	return ok
}

// secretRow is one of a tenant's secrets, by name only.
type secretRow struct {
	Name, Kind string
	Bytes      int
	// Own is a secret of the tenant, not of its app.
	Own bool
}

type settingsData struct {
	Head    tenantHead
	Tenant  api.Tenant
	Form    settingsForm
	Secrets []secretRow
	// Fields are its settings that may be its own, with the values in
	// effect; Env the environment it gets.
	Fields []settingField
	Env    []envRow
	// Review lists what the form changes, once asked to review it;
	// Reviewing says it was.
	Review    []settingChange
	Reviewing bool
	// Live says they change only its disk limit, which its node sets as
	// it runs: nothing restarts. Fixes says what they put right ("It lets
	// atlas write again; atlas keeps running"), "" if nothing.
	Live  bool
	Fixes string
	// Token binds the save to the values reviewed; Restarts says they
	// give it a new container, not only new rollout flags.
	Token    string
	Restarts bool
	// SecretFields are its secrets as edited, Kinds those a secret can be;
	// Blank and BlankSecret are how many empty rows to add variables and
	// secrets in.
	SecretFields []secretField
	Kinds        []string
	Blank        int
	BlankSecret  int
	// Suggest says a value proposed by the link that led here; SuggestApp
	// the app's limit, when it is likely the better fix.
	Suggest    string
	SuggestApp *appMemory
	// Disk is what a disk limit must leave it; DiskShort says, in a review,
	// why the one reviewed is too small.
	Disk      diskRoom
	DiskShort string
	// AppChange is what another app does to it, when the form moves it;
	// FromMenu says the review was asked from its menu, not an editor.
	AppChange *appChange
	FromMenu  bool
}

func (s *Server) settings(w http.ResponseWriter, r *http.Request) {
	ctx, cancel := context.WithTimeout(r.Context(), 10*time.Second)
	defer cancel()
	t, err := s.Apps(s.actor(userOf(r))).Get(ctx, r.PathValue("name"))
	if err != nil {
		s.fail(w, r, err)
		return
	}
	form := formOf(t)
	// A link that proposes a memory limit ("Raise its memory limit")
	// fills it in as an override, to be reviewed.
	if mb, err := strconv.Atoi(r.URL.Query().Get("memoryMB")); err == nil && mb > 0 && mb <= 1<<20 {
		form.MemoryMB = strconv.Itoa(mb)
	}
	// A link to set a limit opens its row.
	if set := r.URL.Query().Get("set"); slices.Contains([]string{"memoryMB", "cpus", "pids", "diskGB"}, set) {
		form.Open = set
	}
	// "Change its app" in the tenant's menu: another app, to review.
	if app := r.URL.Query().Get("app"); app != "" {
		form.App = app
	}
	s.settingsPage(w, r, t, form, "", http.StatusOK)
}

func (s *Server) settingsPage(w http.ResponseWriter, r *http.Request, t api.Tenant, form settingsForm, problem string, status int) {
	ctx, cancel := context.WithTimeout(r.Context(), 10*time.Second)
	defer cancel()
	apps, err := s.Apps(s.actor(userOf(r))).Apps(ctx)
	if err != nil {
		s.fail(w, r, err)
		return
	}
	data := settingsData{Tenant: t, Form: form}
	var appSpec spec.Tenant
	for _, app := range apps {
		if app.Name == t.App {
			appSpec = app.Spec
		}
	}
	// The limits other running tenants have, for its own risks.
	var limited map[string]bool
	var everyone []api.Tenant
	if listed, err := s.backend.List(ctx); err == nil {
		everyone, limited = listed, runningLimits(listed)
	} else {
		limited = map[string]bool{}
	}
	// What a disk limit must leave it: its data, and what the others have.
	data.Disk = s.diskRoom(ctx, t, everyone)
	data.Fields = settingFields(t, appSpec, form, limited, data.Disk)
	if r.Method == http.MethodGet && form.MemoryMB != formOf(t).MemoryMB {
		data.Suggest = "Proposed: memory " + memory(atoi(form.MemoryMB)) + " as a tenant override, up from " + memory(t.Spec.Limits.MemoryMB) + ". Nothing changes until you review and save it."
		var tenants []api.Tenant
		var events []store.Event
		if all(
			func() (err error) { tenants, err = s.backend.List(ctx); return },
			func() (err error) {
				events, err = s.backend.Events(ctx, store.EventQuery{App: t.App, Limit: activityEvents})
				return
			},
		) == nil {
			for _, app := range apps {
				if app.Name == t.App {
					if m := appMemoryOf(app, tenants, events, s.now()); m != nil && m.Hit[t.Spec.Name] > 0 {
						data.SuggestApp = m
					}
				}
			}
		}
	}
	// The form's environment and secrets, as edited, beside what is saved.
	drafted, _ := form.overrides()
	data.Env = envRows(appSpec, t.Overrides.Env, drafted.Env)
	data.SecretFields = secretFields(appSpec, t.Overrides.Secrets, drafted.Secrets)
	data.Kinds = secretKinds
	data.Blank = max(1, atoi(r.FormValue("blank")))
	data.BlankSecret = max(1, atoi(r.FormValue("blanksecret")))
	// Another app: the whole spec it would run, reviewed field by field.
	if form.App != t.App {
		found := false
		for _, app := range apps {
			if app.Name == form.App {
				change := appChangeOf(t, app, form, limited)
				data.AppChange, found = &change, true
				data.FromMenu = r.Method == http.MethodGet
			}
		}
		if !found && problem == "" {
			problem, status = "there is no app "+form.App+": choose one of the apps listed", http.StatusUnprocessableEntity
			form.App = t.App
			data.Form = form
		}
	}
	if r.FormValue("do") == "review" {
		data.Reviewing = true
		data.Review = settingChanges(t, appSpec, form, limited)
		for i := range data.Review {
			if data.Review[i].Field == "Disk" && data.Disk.Full() {
				data.Review[i].FromFull = true
			}
		}
		data.DiskShort = data.Disk.Short(t.Spec.Name, diskAfter(t, appSpec, form))
		data.Token = reviewToken(t.Spec.Name, form)
		data.Live = form.diskOnly(t)
		data.Restarts = form.containerChanged(t) && !data.Live
		// What it puts right, said first: a limit it no longer lacks, a
		// disk it can write to again.
		var fixes []string
		if gb := diskAfter(t, appSpec, form); data.Disk.Full() && int64(gb)<<30 > data.Disk.Used {
			fixes = append(fixes, "it lets "+t.Spec.Name+" write again")
		}
		for _, c := range data.Review {
			if c.FromRisk && !c.Risk {
				fixes = append(fixes, "it ends the missing "+strings.ToLower(c.Field)+" limit")
			}
		}
		if len(fixes) > 0 && data.DiskShort == "" {
			data.Fixes = strings.Join(fixes, "; ")
			switch {
			case data.Restarts:
				data.Fixes += "; " + t.Spec.Name + " restarts, its data stays"
			case data.Live && !t.Archived():
				data.Fixes += "; " + t.Spec.Name + " keeps running"
			}
			data.Fixes = strings.ToUpper(data.Fixes[:1]) + data.Fixes[1:]
		}
	}
	if nodes, err := s.backend.Nodes(ctx); err == nil {
		data.Head = s.headOf(t, nodes, "settings")
		if data.Disk.Full() && data.Head.State.Kind == "ok" {
			data.Head.State = tag{"cannot write", "bad"}
		}
	} else {
		data.Head = s.headOf(t, api.Nodes{}, "settings")
	}
	for _, app := range apps {
		if app.Name != t.App && !t.Archived() {
			data.Head.Apps = append(data.Head.Apps, app.Name)
		}
	}
	slices.Sort(data.Head.Apps)
	for _, key := range slices.Sorted(maps.Keys(t.Spec.Secrets)) {
		_, own := t.Overrides.Secrets[key]
		secret := t.Spec.Secrets[key]
		data.Secrets = append(data.Secrets, secretRow{Name: key, Kind: secret.Kind, Bytes: secret.Bytes, Own: own})
	}
	s.renderWith(w, r, "settings", t.Spec.Name+" · settings", "tenants", data, problem, status)
}

func (s *Server) settingsAction(w http.ResponseWriter, r *http.Request) {
	if !readForm(w, r) {
		return
	}
	name := r.PathValue("name")
	agent := s.Apps(s.actor(userOf(r)))
	ctx, cancel := context.WithTimeout(r.Context(), 2*time.Minute)
	defer cancel()
	t, err := agent.Get(ctx, name)
	if err != nil {
		s.fail(w, r, err)
		return
	}
	back := func(kind, message string) {
		flash(w, r, kind, message)
		http.Redirect(w, r, "/tenants/"+url.PathEscape(name)+"/settings", http.StatusSeeOther)
	}
	switch r.FormValue("do") {
	case "addrow", "addsecret":
		// One more row to add a variable or a secret in, the draft kept:
		// for a page without its script, which adds rows itself.
		form := formFrom(r).withFlags(t)
		blanks := func(field string) int {
			empty := 0
			for _, name := range r.Form[field] {
				if strings.TrimSpace(name) == "" {
					empty++
				}
			}
			return empty
		}
		env, secrets := blanks("envnewname"), blanks("secretnewname")
		if r.FormValue("do") == "addrow" {
			env++
		} else {
			secrets++
		}
		r.Form.Set("do", "")
		r.Form.Set("blank", strconv.Itoa(env))
		r.Form.Set("blanksecret", strconv.Itoa(secrets))
		s.settingsPage(w, r, t, form, "", http.StatusOK)
	case "review", "edit", "usedisk":
		// What the form changes, shown before it is saved; or back to it.
		form := formFrom(r).withFlags(t)
		// A disk limit its data fills, reviewed again with the one
		// suggested ("Use 10 GB instead").
		if r.FormValue("do") == "usedisk" {
			if gb, err := strconv.Atoi(r.FormValue("suggest")); err == nil && gb > 0 {
				form.DiskGB = strconv.Itoa(gb)
			}
			r.Form.Set("do", "review")
		}
		if _, err := form.overrides(); err != nil {
			s.settingsPage(w, r, t, form, err.Error(), http.StatusUnprocessableEntity)
			return
		}
		// An Override on with no value is said, on its row.
		if form.Empty != "" && r.FormValue("do") == "review" {
			var said []string
			for _, name := range form.empty() {
				said = append(said, settingLabel(name))
			}
			r.Form.Set("do", "")
			s.settingsPage(w, r, t, form, strings.Join(said, ", ")+": Override is on but has no value. Type one, or turn Override off.", http.StatusUnprocessableEntity)
			return
		}
		if r.FormValue("do") == "edit" {
			r.Form.Set("do", "")
		}
		s.settingsPage(w, r, t, form, "", http.StatusOK)
	case "save":
		form := formFrom(r).withFlags(t)
		refuse := func(problem string) { s.settingsPage(w, r, t, form, problem, http.StatusUnprocessableEntity) }
		overrides, err := form.overrides()
		if err != nil {
			refuse(err.Error())
			return
		}
		if form.Generation != t.Generation {
			refuse("someone changed " + name + " since you opened this page: your settings are below; reload the page for theirs")
			return
		}
		// Only what was reviewed is saved.
		if r.FormValue("reviewed") != reviewToken(name, form) {
			refuse("these are not the values you reviewed: review them again before saving")
			return
		}
		// A disk limit its data already fills is one its node does not set
		// (ZFS refuses a refquota below what the dataset holds): not saved.
		if room := s.diskRoom(ctx, t, nil); room.Short(name, diskAfter(t, s.appSpecOf(ctx, r, t.App), form)) != "" {
			r.Form.Set("do", "review")
			refuse("its node does not set a disk limit below its data: give " + name + " more than " + size(float64(room.Used)))
			return
		}
		// Another app is a new spec on its data: its name typed, as for
		// any change that can stop it serving.
		if form.App != t.App && strings.TrimSpace(r.FormValue("confirm")) != name {
			r.Form.Set("do", "review")
			refuse("type " + name + " to change its app")
			return
		}
		// Its container's settings, then its rollout flags, which change
		// no container.
		var said []string
		live := form.diskOnly(t)
		if form.containerChanged(t) {
			applied, err := agent.Configure(ctx, name, api.TenantRequest{App: form.App, Overrides: overrides, Generation: form.Generation})
			if err != nil {
				refuse(err.Error())
				return
			}
			switch {
			case !applied.Changed:
				said = append(said, name+"'s settings are unchanged")
			case applied.Tenant.Archived():
				said = append(said, name+"'s settings are changed; it runs them once restored")
			case live:
				said = append(said, name+"'s disk limit is set on its data: nothing restarted")
			default:
				said = append(said, name+"'s settings are changed: it gets a new container")
			}
		}
		if form.flagsChanged(t) {
			canary, hold := form.Canary == "on", form.Hold == "on"
			if _, err := agent.SetFlags(ctx, name, api.TenantFlags{Canary: &canary, Hold: &hold}); err != nil {
				refuse(err.Error())
				return
			}
			said = append(said, flagsSaid(name, canary, hold))
		}
		if len(said) == 0 {
			said = append(said, name+"'s settings are unchanged")
		}
		// Its disk limit alone: to its Overview, where the save's line says
		// it was set and nothing restarted, and a full disk's red notice
		// is gone.
		if live && !form.flagsChanged(t) && !t.Archived() {
			http.Redirect(w, r, "/tenants/"+url.PathEscape(name)+"#operation", http.StatusSeeOther)
			return
		}
		// Another app: to its Overview, where the new container is followed.
		if form.App != t.App {
			flash(w, r, "done", name+" now runs app "+form.App+": its new container starts")
			// Its own fragment: a redirect without one keeps the review's.
			http.Redirect(w, r, "/tenants/"+url.PathEscape(name)+"#operation", http.StatusSeeOther)
			return
		}
		back("done", strings.Join(said, "; "))
	case "flags":
		canary, hold := r.FormValue("canary") == "on", r.FormValue("hold") == "on"
		if _, err := agent.SetFlags(ctx, name, api.TenantFlags{Canary: &canary, Hold: &hold}); err != nil {
			back("problem", err.Error())
			return
		}
		back("done", "rollout flags saved: "+flagsSaid(name, canary, hold))
	case "rotate":
		key := r.FormValue("key")
		if !typed(r.FormValue("confirm"), key) {
			back("problem", "type the secret's name to give it a new value")
			return
		}
		if _, err := agent.Rotate(ctx, name, key); err != nil {
			back("problem", err.Error())
			return
		}
		back("done", key+" has a new value: "+name+" gets a new container with it")
	default:
		back("problem", "unknown action")
	}
}

// flagsSaid says how a tenant takes its app's rollouts: "atlas is held:
// it takes no rollouts until released".
func flagsSaid(name string, canary, hold bool) string {
	switch {
	case hold:
		return name + " is held: it takes no rollouts until released"
	case canary:
		return name + " is a canary: it takes rollouts first"
	}
	return name + " takes rollouts with the others"
}

func atoi(s string) int {
	n, _ := strconv.Atoi(s)
	return n
}

// appReview shows a change of an app before it is made, at its own address:
// an edited spec (kept as a draft), a revision gone back to, or the newer
// image; with ?pace= the pace its plan describes.
func (s *Server) appReview(w http.ResponseWriter, r *http.Request) {
	name := r.PathValue("name")
	agent := s.Apps(s.actor(userOf(r)))
	ctx, cancel := context.WithTimeout(r.Context(), time.Minute)
	defer cancel()
	current, err := agent.App(ctx, name)
	if err != nil {
		s.fail(w, r, err)
		return
	}
	back := func(kind, message string) {
		flash(w, r, kind, message)
		http.Redirect(w, r, "/apps/"+url.PathEscape(name), http.StatusSeeOther)
	}
	q := r.URL.Query()
	if current.UpdatedAt.IsZero() || q.Get("updated") != stampOf(current.UpdatedAt) {
		back("problem", "someone changed the app since you opened it: look again, then try again")
		return
	}
	switch q.Get("do") {
	case "review":
		file, ok := s.drafts.get(name, q.Get("draft"))
		if !ok {
			back("problem", "that review is no longer kept: review the change again")
			return
		}
		from, err := shownFile(current)
		if err != nil {
			s.fail(w, r, err)
			return
		}
		s.review(w, r, current, reviewData{Do: "save", Title: "Your change to " + name + "'s spec", File: file, Updated: q.Get("updated"), Draft: q.Get("draft")}, from, file)
	case "review-restore", "review-update":
		from, err := spec.AppFile(current.Spec)
		if err != nil {
			s.fail(w, r, err)
			return
		}
		data := reviewData{Do: "update", Title: "The newer image of " + current.Tag, Updated: q.Get("updated")}
		next := current.Spec
		if q.Get("do") == "review-restore" {
			revision, _ := strconv.ParseInt(q.Get("revision"), 10, 64)
			revisions, err := agent.AppRevisions(ctx, name)
			if err != nil {
				back("problem", err.Error())
				return
			}
			i := slices.IndexFunc(revisions, func(rev store.Revision) bool { return rev.Revision == revision })
			if i < 0 {
				back("problem", "no such revision")
				return
			}
			next = revisions[i].Spec
			data.Do, data.Revision, data.Title = "restore", revision, "Going back to revision "+strconv.FormatInt(revision, 10)
		} else if current.Latest != "" {
			next.Image = current.Latest
		}
		to, err := spec.AppFile(next)
		if err != nil {
			s.fail(w, r, err)
			return
		}
		s.review(w, r, current, data, string(from), string(to))
	default:
		back("problem", "nothing to review")
	}
}

// reviewDrafts keeps the specs being reviewed, for their review's own
// address: a few, for an hour, in this panel only.
type reviewDrafts struct {
	mu    sync.Mutex
	kept  map[string]reviewDraft
	order []string
}

type reviewDraft struct {
	app, file string
	at        time.Time
}

// draftsKept and draftLife bound what is kept.
const (
	draftsKept = 200
	draftLife  = time.Hour
)

// keep keeps a draft of app, and names it.
func (d *reviewDrafts) keep(app, file string) string {
	d.mu.Lock()
	defer d.mu.Unlock()
	if d.kept == nil {
		d.kept = map[string]reviewDraft{}
	}
	id := Token(12)
	d.kept[id] = reviewDraft{app: app, file: file, at: time.Now()}
	d.order = append(d.order, id)
	for len(d.order) > draftsKept {
		delete(d.kept, d.order[0])
		d.order = d.order[1:]
	}
	return id
}

// get is the draft named id, of app, while it is kept.
func (d *reviewDrafts) get(app, id string) (string, bool) {
	d.mu.Lock()
	defer d.mu.Unlock()
	draft, ok := d.kept[id]
	if !ok || draft.app != app || time.Since(draft.at) > draftLife {
		return "", false
	}
	return draft.file, true
}

// diskRoom reads what a disk limit must leave tenant t: its data's size, as
// its node measured it, and what the other tenants of all have.
func (s *Server) diskRoom(ctx context.Context, t api.Tenant, all []api.Tenant) diskRoom {
	used, limit := int64(-1), int64(0)
	if u, ok := s.usageNow(ctx).Tenants[t.Spec.Name]; ok {
		used, limit = u.DiskUsed, u.DiskLimit
	}
	return diskRoomOf(t, all, used, limit)
}

// appSpecOf is app's spec, empty if it cannot be read.
func (s *Server) appSpecOf(ctx context.Context, r *http.Request, app string) spec.Tenant {
	if apps, err := s.Apps(s.actor(userOf(r))).Apps(ctx); err == nil {
		for _, a := range apps {
			if a.Name == app {
				return a.Spec
			}
		}
	}
	return spec.Tenant{}
}

// diskAfter is the disk limit, in GB, the form would give the tenant: its
// own, else its app's; 0 for none.
func diskAfter(t api.Tenant, app spec.Tenant, form settingsForm) int {
	own, _ := form.overrides()
	if own.Limits.DiskGB > 0 {
		return own.Limits.DiskGB
	}
	if form.App == t.App || form.App == "" {
		return app.Limits.DiskGB
	}
	return 0
}
