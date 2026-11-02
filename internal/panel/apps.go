package panel

import (
	"context"
	"errors"
	"fmt"
	"maps"
	"net/http"
	"net/url"
	"slices"
	"strconv"
	"strings"
	"time"

	"github.com/PowerPenguini/murder/internal/api"
	"github.com/PowerPenguini/murder/internal/spec"
	"github.com/PowerPenguini/murder/internal/strictyaml"
)

// AppAgent is what editing apps and tenants' settings asks of the agent,
// for the person signed in. Secrets' values are never asked for: the panel
// shows their names, and rotates them.
type AppAgent interface {
	Apps(ctx context.Context) ([]api.App, error)
	App(ctx context.Context, name string) (api.App, error)
	PutApp(ctx context.Context, name string, app spec.Tenant) (api.AppApplied, error)
	DeleteApp(ctx context.Context, name string) error
	Get(ctx context.Context, name string) (api.Tenant, error)
	Configure(ctx context.Context, name string, request api.TenantRequest) (api.Applied, error)
	Rotate(ctx context.Context, name, key string) (api.Tenant, error)
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
	s.renderWith(w, r, "apps", "Apps", "apps", data, problem, status)
}

// createApp stores a new app; one of the name stored meanwhile is not
// replaced.
func (s *Server) createApp(w http.ResponseWriter, r *http.Request) {
	r.Body = http.MaxBytesReader(w, r.Body, formLimit)
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
	applied, err := agent.PutApp(ctx, name, app)
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
}

func (s *Server) app(w http.ResponseWriter, r *http.Request) {
	ctx, cancel := context.WithTimeout(r.Context(), 10*time.Second)
	defer cancel()
	app, err := s.Apps(s.actor(userOf(r))).App(ctx, r.PathValue("name"))
	if err != nil {
		s.fail(w, r, err)
		return
	}
	file, err := spec.AppFile(app.Spec)
	if err != nil {
		s.fail(w, r, err)
		return
	}
	s.renderWith(w, r, "app", app.Name, "apps", appData{App: app, File: string(file), Updated: stampOf(app.UpdatedAt)}, "", http.StatusOK)
}

func stampOf(t time.Time) string { return t.UTC().Format(time.RFC3339Nano) }

func (s *Server) appAction(w http.ResponseWriter, r *http.Request) {
	r.Body = http.MaxBytesReader(w, r.Body, formLimit)
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
	case "save":
		file := r.FormValue("file")
		refuse := func(problem string) {
			s.renderWith(w, r, "app", name, "apps", appData{App: current, File: file, Updated: r.FormValue("updated")}, problem, http.StatusUnprocessableEntity)
		}
		if r.FormValue("updated") != stampOf(current.UpdatedAt) {
			refuse("someone changed the app since you opened it: your text is below; reload the page for theirs")
			return
		}
		app, err := readApp(name, file)
		if err != nil {
			refuse(err.Error())
			return
		}
		applied, err := agent.PutApp(ctx, name, app)
		if err != nil {
			refuse(err.Error())
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
		lines = append(lines, key+"="+o.Env[key])
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
	return settingsForm{
		App: r.FormValue("app"), Image: strings.TrimSpace(r.FormValue("image")), Env: r.FormValue("env"), Secrets: r.FormValue("secrets"),
		MemoryMB: strings.TrimSpace(r.FormValue("memoryMB")), CPUs: strings.TrimSpace(r.FormValue("cpus")),
		PIDs: strings.TrimSpace(r.FormValue("pids")), DiskGB: strings.TrimSpace(r.FormValue("diskGB")), Generation: generation,
	}
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
		key = strings.TrimSpace(key)
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
		if err != nil || cpus < 0 {
			problems = append(problems, "cpus: a number, or empty for the app's")
		}
		o.Limits.CPUs = cpus
	}
	if len(problems) > 0 {
		return o, errors.New(strings.Join(problems, "; "))
	}
	return o, nil
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
	Tenant  api.Tenant
	Form    settingsForm
	Apps    []string
	Secrets []secretRow
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
	for _, app := range apps {
		data.Apps = append(data.Apps, app.Name)
	}
	slices.Sort(data.Apps)
	for _, key := range slices.Sorted(maps.Keys(t.Spec.Secrets)) {
		_, own := t.Overrides.Secrets[key]
		secret := t.Spec.Secrets[key]
		data.Secrets = append(data.Secrets, secretRow{Name: key, Kind: secret.Kind, Bytes: secret.Bytes, Own: own})
	}
	s.renderWith(w, r, "settings", t.Spec.Name+" · settings", "overview", data, problem, status)
}

func (s *Server) settingsAction(w http.ResponseWriter, r *http.Request) {
	r.Body = http.MaxBytesReader(w, r.Body, formLimit)
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
