package panel

import (
	"context"
	"slices"
	"strconv"
	"strings"
	"time"

	"github.com/PowerPenguini/murder/internal/api"
	"github.com/PowerPenguini/murder/internal/spec"
	"github.com/PowerPenguini/murder/internal/store"
)

// AsApps is the cluster's apps and tenants' settings, edited by actor.
func (d *DemoCluster) AsApps(actor string) AppAgent { return &demoActor{DemoCluster: d, actor: actor} }

// demoApps makes the demo's apps from its tenants, and gives one tenant
// settings of its own.
func (d *DemoCluster) demoApps() {
	d.apps = map[string]api.App{}
	created := time.Now().Add(-40 * 24 * time.Hour)
	for _, t := range d.tenants {
		if _, ok := d.apps[t.App]; ok {
			continue
		}
		app := t.Spec
		app.Name, app.Suspended = "", false
		d.apps[t.App] = api.App{Name: t.App, Spec: app, CreatedAt: created, UpdatedAt: time.Now().Add(-3 * time.Hour)}
	}
	d.revisions = map[string][]store.Revision{}
	for name, app := range d.apps {
		app.Revision = 1
		d.apps[name] = app
		d.revisions[name] = []store.Revision{{Revision: 1, Spec: app.Spec, Tag: app.Tag, CreatedAt: app.UpdatedAt, Actor: "root"}}
	}
	// web follows a tag, has a history, and a newer image waits.
	if web, ok := d.apps["web"]; ok {
		web.Tag = "ghcr.io/example/web:stable"
		older := web.Spec
		older.Image = "ghcr.io/example/web@sha256:" + strings.Repeat("a1", 32)
		oldest := older
		oldest.Image = "ghcr.io/example/web@sha256:" + strings.Repeat("9f", 32)
		d.revisions["web"] = []store.Revision{
			{Revision: 1, Spec: oldest, Tag: web.Tag, CreatedAt: created, Actor: "root"},
			{Revision: 2, Spec: older, Tag: web.Tag, CreatedAt: time.Now().Add(-9 * 24 * time.Hour), Actor: "jan via murder-panel"},
			{Revision: 3, Spec: web.Spec, Tag: web.Tag, CreatedAt: web.UpdatedAt, Actor: "root"},
		}
		web.Revision = 3
		web.Latest = "ghcr.io/example/web@sha256:" + strings.Repeat("5e", 32)
		web.LatestAt = time.Now().Add(-18 * time.Minute)
		// A rollout of revision 3 is on its second wave.
		web.Rollout = &store.Rollout{App: "web", ToRevision: 3, State: store.RolloutRunning, Wave: 2,
			WaveStarted: time.Now().Add(-4*time.Minute - 30*time.Second), StartedAt: time.Now().Add(-10 * time.Minute), UpdatedAt: time.Now().Add(-4*time.Minute - 30*time.Second), Actor: "root"}
		d.apps["web"] = web
	}
	// Its tenants, as that rollout left them: two canaries moved in the
	// first wave, baltic-freight in the second and broke; three wait on
	// revision 2, and test-staging is held there.
	started := time.Now().Add(-10 * time.Minute)
	for name, moved := range map[string]int{"atlas": 1, "acme-logistics": 1, "baltic-freight": 2} {
		t := d.tenants[name]
		t.BeforeRevision, t.Canary = 2, moved == 1
		d.tenants[name] = t
		at := started
		if moved == 2 {
			at = time.Now().Add(-4*time.Minute - 30*time.Second)
		}
		d.events = append(d.events, store.Event{At: at, Actor: "murder (rollout)", Action: "app.rolled-out", Tenant: name, App: "web", Detail: "wave " + strconv.Itoa(moved) + ": revision 2 → 3"})
	}
	for _, name := range []string{"kowalski-trans", "nordwind", "vistula-cargo", "test-staging"} {
		t := d.tenants[name]
		t.AppRevision = 2
		t.Hold = name == "test-staging"
		d.tenants[name] = t
	}
	slices.SortStableFunc(d.events, func(a, b store.Event) int { return b.At.Compare(a.At) })
	for i := range d.events {
		d.events[i].ID = int64(len(d.events) - i)
	}
	d.apps["testapp"] = api.App{Name: "testapp", Spec: spec.Tenant{Image: "ghcr.io/powerpenguini/murder-testapp@sha256:" + strings.Repeat("7", 64), User: "10001:10001", HealthPath: "/health"}.WithDefaults(),
		CreatedAt: created, UpdatedAt: created}
	t := d.tenants["atlas"]
	t.Overrides = spec.Overrides{Env: map[string]string{"TZ": "Europe/Berlin", "FEATURE_PLANNER": "on"}, Secrets: spec.Secrets{"ADMIN_PASSWORD": {Kind: "password", Bytes: 20}}, Limits: spec.Limits{MemoryMB: 768}}
	d.tenants["atlas"] = t
	// Each tenant runs its app's spec, of the revision it is on, with its
	// overrides, composed as the store composes it.
	for name, t := range d.tenants {
		app, ok := d.apps[t.App]
		if !ok {
			continue
		}
		from := app.Spec
		if t.AppRevision != 0 {
			if kept, ok := d.revisionSpec(t.App, t.AppRevision); ok {
				from = kept
			}
		}
		suspended := t.Spec.Suspended
		t.Spec = spec.Compose(from, name, t.Overrides)
		t.Spec.Suspended = suspended
		d.tenants[name] = t
	}
}

// liveTenantsOf lists the live tenants of app; the caller holds d.mu.
func (d *DemoCluster) liveTenantsOf(app string) []string {
	var names []string
	for _, t := range d.tenants {
		if t.App == app && !t.Archived() {
			names = append(names, t.Spec.Name)
		}
	}
	slices.Sort(names)
	return names
}

func (a *demoActor) Apps(context.Context) ([]api.App, error) {
	a.mu.Lock()
	defer a.mu.Unlock()
	var apps []api.App
	for _, app := range a.apps {
		app.Tenants = a.liveTenantsOf(app.Name)
		apps = append(apps, app)
	}
	return apps, nil
}

func (a *demoActor) App(_ context.Context, name string) (api.App, error) {
	a.mu.Lock()
	defer a.mu.Unlock()
	app, ok := a.apps[name]
	if !ok {
		return app, &api.Error{Status: 404, Message: "app " + name + ": not found"}
	}
	app.Tenants = a.liveTenantsOf(name)
	return app, nil
}

func (a *demoActor) PutAppIf(_ context.Context, name string, app spec.Tenant, cond store.AppIf) (api.AppApplied, error) {
	if err := spec.CheckApp(name, app, spec.Tenant.ValidateUnpinned); err != nil {
		return api.AppApplied{}, &api.Error{Status: 422, Message: err.Error()}
	}
	a.mu.Lock()
	defer a.mu.Unlock()
	app = app.WithDefaults()
	old, existed := a.apps[name]
	// As the agent does: the tag the app has keeps its pinned image.
	tag := ""
	if !spec.Pinned(app.Image) {
		tag = app.Image
		if existed && old.Tag == tag {
			app.Image = old.Spec.Image
		}
	}
	return a.storeApp(name, app, tag, cond, "")
}

// storeApp stores an app as its newest revision; the caller holds d.mu.
func (a *demoActor) storeApp(name string, app spec.Tenant, tag string, cond store.AppIf, detail string) (api.AppApplied, error) {
	old, existed := a.apps[name]
	switch {
	case cond.Absent && existed:
		return api.AppApplied{}, &api.Error{Status: 409, Message: "app " + name + " exists already"}
	case !cond.UpdatedAt.IsZero() && (!existed || !old.UpdatedAt.Equal(cond.UpdatedAt)):
		return api.AppApplied{}, &api.Error{Status: 409, Message: "app " + name + " was changed after the version read; read it again"}
	}
	// As the store compares them: with defaults filled in.
	applied := api.AppApplied{Created: !existed, Changed: !existed || !old.Spec.WithDefaults().Equal(app) || old.Tag != tag}
	if !applied.Changed {
		applied.App = old
		applied.App.Tenants = a.liveTenantsOf(name)
		return applied, nil
	}
	now := time.Now()
	stored := api.App{Name: name, Spec: app, Tag: tag, Revision: old.Revision + 1, CreatedAt: now, UpdatedAt: now}
	if existed {
		stored.CreatedAt = old.CreatedAt
		if old.Tag == tag && old.Latest != app.Image {
			stored.Latest, stored.LatestAt = old.Latest, old.LatestAt
		}
	}
	a.revisions[name] = append(a.revisions[name], store.Revision{Revision: stored.Revision, Spec: app, Tag: tag, CreatedAt: now, Actor: a.actor})
	action := "app.updated"
	if !existed {
		action = "app.created"
	}
	a.recordApp(action, name)
	if detail != "" {
		a.events[0].Detail = detail
	}
	// As the store does: a tenant whose spec does not change, on the
	// newest revision, is left as it is; a held one keeps its revision; in
	// waves, the others wait for the rollout, which keeps the revision
	// each runs until it moves them; at once, each moves now.
	waves := existed && !a.atOnce
	for _, tenant := range a.liveTenantsOf(name) {
		t := a.tenants[tenant]
		composed := spec.Compose(app, tenant, t.Overrides)
		composed.Suspended = t.Spec.Suspended
		if composed.Equal(t.Spec) && t.AppRevision == 0 {
			continue
		}
		if t.Hold {
			if t.AppRevision == 0 {
				t.AppRevision = old.Revision
			}
			a.tenants[tenant] = t
			continue
		}
		applied.Tenants = append(applied.Tenants, t)
		if waves {
			if t.AppRevision == 0 {
				t.AppRevision, t.BeforeRevision = old.Revision, 0
			}
			a.tenants[tenant] = t
			continue
		}
		t.AppRevision, t.BeforeRevision = 0, 0
		a.moveTenant(&t, composed, "app changed")
		a.tenants[tenant] = t
	}
	if waves && len(applied.Tenants) > 0 {
		applied.Rollout = true
		stored.Rollout = &store.Rollout{App: name, ToRevision: stored.Revision, State: store.RolloutRunning, WaveStarted: now, StartedAt: now, UpdatedAt: now, Actor: a.actor}
	}
	a.apps[name] = stored
	stored.Tenants = a.liveTenantsOf(name)
	applied.App = stored
	return applied, nil
}

func (a *demoActor) CheckApp(_ context.Context, name string) (api.AppCheck, error) {
	a.mu.Lock()
	defer a.mu.Unlock()
	app, ok := a.apps[name]
	switch {
	case !ok:
		return api.AppCheck{}, &api.Error{Status: 404, Message: "app " + name + ": not found"}
	case app.Tag == "":
		return api.AppCheck{}, &api.Error{Status: 422, Message: "app " + name + " was given a pinned image, not a tag: there is nothing to ask its registry about"}
	}
	app.Tenants = a.liveTenantsOf(name)
	if app.Latest != "" {
		return api.AppCheck{App: app, Image: app.Latest, Newer: true}, nil
	}
	return api.AppCheck{App: app, Image: app.Spec.Image}, nil
}

func (a *demoActor) UpdateApp(_ context.Context, name string, updatedAt time.Time) (api.AppApplied, error) {
	a.mu.Lock()
	defer a.mu.Unlock()
	app, ok := a.apps[name]
	switch {
	case !ok:
		return api.AppApplied{}, &api.Error{Status: 404, Message: "app " + name + ": not found"}
	case app.Tag == "":
		return api.AppApplied{}, &api.Error{Status: 422, Message: "app " + name + " was given a pinned image, not a tag: there is nothing to ask its registry about"}
	}
	next := app.Spec
	if app.Latest != "" {
		next.Image = app.Latest
	}
	return a.storeApp(name, next, app.Tag, store.AppIf{UpdatedAt: updatedAt}, "")
}

func (a *demoActor) AppRevisions(_ context.Context, name string) ([]store.Revision, error) {
	a.mu.Lock()
	defer a.mu.Unlock()
	if _, ok := a.apps[name]; !ok {
		return nil, &api.Error{Status: 404, Message: "app " + name + ": not found"}
	}
	var revisions []store.Revision
	for _, revision := range slices.Backward(a.revisions[name]) {
		revisions = append(revisions, revision)
	}
	return revisions, nil
}

func (a *demoActor) RestoreRevision(_ context.Context, name string, revision int64, updatedAt time.Time) (api.AppApplied, error) {
	a.mu.Lock()
	defer a.mu.Unlock()
	for _, kept := range a.revisions[name] {
		if kept.Revision == revision {
			return a.storeApp(name, kept.Spec, kept.Tag, store.AppIf{UpdatedAt: updatedAt}, "back to revision "+strconv.FormatInt(revision, 10))
		}
	}
	return api.AppApplied{}, &api.Error{Status: 404, Message: "app " + name + " keeps no revision " + strconv.FormatInt(revision, 10)}
}

func (a *demoActor) recordApp(action, app string) {
	a.record(action, "", "")
	a.events[0].App = app
}

func (a *demoActor) DeleteApp(_ context.Context, name string) error {
	a.mu.Lock()
	defer a.mu.Unlock()
	for _, t := range a.tenants {
		if t.App == name {
			return &api.Error{Status: 409, Message: "app " + name + " still has tenant " + t.Spec.Name}
		}
	}
	if _, ok := a.apps[name]; !ok {
		return &api.Error{Status: 404, Message: "app " + name + ": not found"}
	}
	delete(a.apps, name)
	a.recordApp("app.deleted", name)
	return nil
}

func (a *demoActor) Configure(_ context.Context, name string, request api.TenantRequest) (api.Applied, error) {
	a.mu.Lock()
	app, ok := a.apps[request.App]
	t, exists := a.tenants[name]
	a.mu.Unlock()
	if !exists {
		return api.Applied{}, &api.Error{Status: 404, Message: "tenant " + name + ": not found"}
	}
	if !ok {
		return api.Applied{}, &api.Error{Status: 422, Message: "no app " + request.App}
	}
	if request.Generation != 0 && request.Generation != t.Generation {
		return api.Applied{}, &api.Error{Status: 409, Message: "tenant " + name + " changed since generation " + strconv.FormatInt(request.Generation, 10)}
	}
	composed := spec.Compose(app.Spec, name, request.Overrides)
	if err := composed.ValidateUnpinned(); err != nil {
		return api.Applied{}, &api.Error{Status: 422, Message: err.Error()}
	}
	if request.App == t.App && request.Overrides.Equal(t.Overrides) {
		return api.Applied{Tenant: t}, nil
	}
	changed, err := a.change(name, "tenant.updated", "", func(t *api.Tenant) error {
		suspended := t.Spec.Suspended
		t.App, t.Overrides, t.Spec = request.App, request.Overrides.Normalized(), composed
		t.Spec.Suspended = suspended
		return nil
	})
	return api.Applied{Tenant: changed, Changed: err == nil}, err
}

func (a *demoActor) Rotate(_ context.Context, name, key string) (api.Tenant, error) {
	return a.change(name, "secret.rotated", key, func(t *api.Tenant) error {
		if t.Archived() {
			return &api.Error{Status: 409, Message: "tenant " + name + " is archived"}
		}
		if _, ok := t.Spec.Secrets[key]; !ok {
			return &api.Error{Status: 404, Message: "tenant " + name + " has no secret " + key}
		}
		return nil
	})
}

func (a *demoActor) RolloutAction(_ context.Context, name, action string) (api.App, error) {
	a.mu.Lock()
	defer a.mu.Unlock()
	app, ok := a.apps[name]
	if !ok {
		return api.App{}, &api.Error{Status: 404, Message: "app " + name + ": not found"}
	}
	if app.Rollout == nil || !app.Rollout.Active() {
		return api.App{}, &api.Error{Status: 409, Message: "app " + name + " has no rollout under way"}
	}
	rollout := *app.Rollout
	switch action {
	case "pause":
		rollout.State, rollout.Reason = store.RolloutPaused, "paused by "+a.actor
	case "resume":
		rollout.State, rollout.Reason, rollout.WaveStarted = store.RolloutRunning, "", time.Now()
	case "abort":
		rollout.State, rollout.Reason = store.RolloutAborted, "aborted by "+a.actor
		// Every tenant it moved goes back to the revision it ran before.
		for _, tenant := range a.liveTenantsOf(name) {
			t := a.tenants[tenant]
			if t.BeforeRevision == 0 || t.Hold {
				continue
			}
			before, ok := a.revisionSpec(name, t.BeforeRevision)
			if !ok {
				continue
			}
			composed := spec.Compose(before, tenant, t.Overrides)
			composed.Suspended = t.Spec.Suspended
			back := t.BeforeRevision
			t.AppRevision, t.BeforeRevision = back, 0
			a.moveTenant(&t, composed, "aborted: back to revision "+strconv.FormatInt(back, 10))
			a.tenants[tenant] = t
		}
	case "promote":
		if rollout.State != store.RolloutRunning {
			return api.App{}, &api.Error{Status: 409, Message: "the rollout of app " + name + " is " + rollout.State + ": resume it first"}
		}
		rollout.Wave++
		rollout.WaveStarted = time.Now()
	default:
		return api.App{}, &api.Error{Status: 400, Message: "a rollout is paused, resumed, aborted or promoted"}
	}
	app.Rollout = &rollout
	a.apps[name] = app
	a.recordApp("rollout."+action, name)
	app.Tenants = a.liveTenantsOf(name)
	return app, nil
}

func (a *demoActor) RestoreRollout(_ context.Context, name string, tenants []string) (api.RolloutRestored, error) {
	a.mu.Lock()
	defer a.mu.Unlock()
	app, ok := a.apps[name]
	if !ok {
		return api.RolloutRestored{}, &api.Error{Status: 404, Message: "app " + name + ": not found"}
	}
	if app.Rollout == nil || !app.Rollout.Active() {
		return api.RolloutRestored{}, &api.Error{Status: 409, Message: "app " + name + " has no rollout under way"}
	}
	if len(tenants) == 0 {
		tenants = a.liveTenantsOf(name)
	}
	rollout := *app.Rollout
	rollout.State = store.RolloutPaused
	rollout.Reason = strings.Join(tenants, ", ") + " went back to the revision before, data too, as " + a.actor + " asked"
	app.Rollout = &rollout
	a.apps[name] = app
	a.recordApp("rollout.restored", name)
	app.Tenants = a.liveTenantsOf(name)
	return api.RolloutRestored{App: app, Tenants: tenants}, nil
}

func (a *demoActor) SetFlags(_ context.Context, name string, flags api.TenantFlags) (api.Tenant, error) {
	return a.change(name, "tenant.flags", "", func(t *api.Tenant) error {
		if flags.Canary != nil {
			t.Canary = *flags.Canary
		}
		if flags.Hold != nil {
			t.Hold = *flags.Hold
		}
		return nil
	})
}
