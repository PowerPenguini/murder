package panel

import (
	"context"
	"errors"
	"fmt"
	"maps"
	"math"
	"net/http"
	"net/url"
	"slices"
	"strconv"
	"strings"
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
	slices.SortFunc(apps, func(a, b api.App) int { return strings.Compare(a.Name, b.Name) })
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
	// beside it.
	EditorProblem string
	// Rows are its tenants, with their state.
	Rows []tenantRow
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
	s.appPage(w, r, appData{App: app, File: file, Updated: stampOf(app.UpdatedAt)}, "", http.StatusOK)
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
	var err error
	if data.Revisions, err = s.Apps(s.actor(userOf(r))).AppRevisions(ctx, data.App.Name); err != nil {
		s.fail(w, r, err)
		return
	}
	// What is wrong with the spec is said beside the editor.
	if data.File != "" && problem != "" {
		data.EditorProblem, problem = problem, ""
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
		from, err := shownFile(current)
		if err != nil {
			s.fail(w, r, err)
			return
		}
		s.review(w, r, current, reviewData{Do: "save", Title: "Your change to " + name + "'s spec", File: file, Updated: r.FormValue("updated")}, from, file)
	case "review-restore", "review-update":
		if current.UpdatedAt.IsZero() || r.FormValue("updated") != stampOf(current.UpdatedAt) {
			back("problem", "someone changed the app since you opened it: look again, then try again")
			return
		}
		from, err := spec.AppFile(current.Spec)
		if err != nil {
			s.fail(w, r, err)
			return
		}
		data := reviewData{Do: "update", Title: "The newer image of " + current.Tag, Updated: r.FormValue("updated")}
		next := current.Spec
		if r.FormValue("do") == "review-restore" {
			revision, _ := strconv.ParseInt(r.FormValue("revision"), 10, 64)
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
		applied, err := agent.PutAppIf(ctx, name, app, store.AppIf{UpdatedAt: current.UpdatedAt})
		if err != nil {
			refuse(err.Error())
			return
		}
		back("done", appOutcome(name, applied))
	case "pause", "resume", "abort", "promote":
		action := r.FormValue("do")
		if action == "abort" && strings.TrimSpace(r.FormValue("confirm")) != name {
			back("problem", "type the app's name to take every tenant back")
			return
		}
		if _, err := agent.RolloutAction(ctx, name, action); err != nil {
			back("problem", err.Error())
			return
		}
		back("done", map[string]string{"pause": "the rollout is paused", "resume": "the rollout goes on", "abort": "the rollout is aborted: its tenants go back, their data as it is", "promote": "every waiting tenant moves now"}[action])
	case "restore-data":
		if strings.TrimSpace(r.FormValue("confirm")) != name {
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
			back("done", "a newer image of "+check.App.Tag+" is in its registry: "+shortImage(check.Image))
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
			applied, err = agent.UpdateApp(ctx, name, current.UpdatedAt)
		} else {
			revision, parseErr := strconv.ParseInt(r.FormValue("revision"), 10, 64)
			if parseErr != nil {
				back("problem", "no such revision")
				return
			}
			applied, err = agent.RestoreRevision(ctx, name, revision, current.UpdatedAt)
		}
		if err != nil {
			back("problem", err.Error())
			return
		}
		back("done", appOutcome(name, applied))
	case "delete":
		if strings.TrimSpace(r.FormValue("confirm")) != name {
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
}

func formOf(t api.Tenant) settingsForm {
	o := t.Overrides
	form := settingsForm{App: t.App, Image: o.Image, Generation: t.Generation}
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
	// The page's form says, by a switch each, which values are the
	// tenant's own: one switched off takes the app's again, whatever its
	// field holds.
	if r.FormValue("owns") != "" {
		for name, field := range map[string]*string{"image": &form.Image, "memoryMB": &form.MemoryMB, "cpus": &form.CPUs, "pids": &form.PIDs, "diskGB": &form.DiskGB} {
			if r.FormValue("own-"+name) == "" {
				*field = ""
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
	r.Body = http.MaxBytesReader(w, r.Body, formLimit)
	if err := r.ParseForm(); err != nil {
		status := http.StatusBadRequest
		if errors.As(err, new(*http.MaxBytesError)) {
			status = http.StatusRequestEntityTooLarge
		}
		http.Error(w, fmt.Sprintf("the form is not read: %v (at most %d KB)", err, formLimit>>10), status)
		return false
	}
	return true
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
	Apps    []string
	Secrets []secretRow
	// Fields are its settings that may be its own, with the values in
	// effect; Env the environment it gets.
	Fields []settingField
	Env    []envRow
	// Review lists what the form changes, once asked to review it;
	// Reviewing says it was.
	Review    []settingChange
	Reviewing bool
}

func (s *Server) settings(w http.ResponseWriter, r *http.Request) {
	ctx, cancel := context.WithTimeout(r.Context(), 10*time.Second)
	defer cancel()
	t, err := s.Apps(s.actor(userOf(r))).Get(ctx, r.PathValue("name"))
	if err != nil {
		s.fail(w, r, err)
		return
	}
	s.settingsPage(w, r, t, formOf(t), "", http.StatusOK)
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
	data.Fields = settingFields(t, appSpec, form)
	data.Env = envRows(appSpec, t.Overrides.Env)
	if r.FormValue("do") == "review" {
		data.Reviewing = true
		data.Review = settingChanges(t, appSpec, form)
	}
	if nodes, err := s.backend.Nodes(ctx); err == nil {
		data.Head = s.headOf(t, nodes, "settings")
	} else {
		data.Head = s.headOf(t, api.Nodes{}, "settings")
	}
	for _, app := range apps {
		data.Apps = append(data.Apps, app.Name)
	}
	slices.Sort(data.Apps)
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
	case "review", "edit":
		// What the form changes, shown before it is saved; or back to it.
		form := formFrom(r)
		if _, err := form.overrides(); err != nil {
			s.settingsPage(w, r, t, form, err.Error(), http.StatusUnprocessableEntity)
			return
		}
		if r.FormValue("do") == "edit" {
			r.Form.Set("do", "")
		}
		s.settingsPage(w, r, t, form, "", http.StatusOK)
	case "save":
		form := formFrom(r)
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
		applied, err := agent.Configure(ctx, name, api.TenantRequest{App: form.App, Overrides: overrides, Generation: form.Generation})
		if err != nil {
			refuse(err.Error())
			return
		}
		switch {
		case !applied.Changed:
			back("done", name+"'s settings are unchanged")
		case applied.Tenant.Archived():
			back("done", name+"'s settings are changed; it runs them once restored")
		default:
			back("done", name+"'s settings are changed: it gets a new container")
		}
	case "flags":
		canary, hold := r.FormValue("canary") == "on", r.FormValue("hold") == "on"
		if _, err := agent.SetFlags(ctx, name, api.TenantFlags{Canary: &canary, Hold: &hold}); err != nil {
			back("problem", err.Error())
			return
		}
		back("done", fmt.Sprintf("rollout flags saved: %s takes rollouts %s", name, map[bool]string{true: "never: it is held", false: map[bool]string{true: "first: it is a canary", false: "with the others"}[canary]}[hold]))
	case "rotate":
		key := r.FormValue("key")
		if strings.TrimSpace(r.FormValue("confirm")) != key {
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
