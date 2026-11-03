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
func (d *DemoCluster) AsApps(actor string) AppAgent { return &demoActor{d, actor} }

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
	if addhoc, ok := d.apps["addhoc"]; ok {
		addhoc.Tag = "ghcr.io/powerpenguini/addhoc:stable"
		d.apps["addhoc"] = addhoc
	}
	d.apps["testapp"] = api.App{Name: "testapp", Spec: spec.Tenant{Image: "ghcr.io/powerpenguini/murder-testapp@sha256:" + strings.Repeat("7", 64), User: "10001:10001", HealthPath: "/health"}.WithDefaults(),
		CreatedAt: created, UpdatedAt: created}
	t := d.tenants["iqtms"]
	t.Overrides = spec.Overrides{Env: map[string]string{"TZ": "Europe/Berlin", "FEATURE_PLANNER": "on"}, Secrets: spec.Secrets{"ADMIN_PASSWORD": {Kind: "password", Bytes: 20}}, Limits: spec.Limits{MemoryMB: 768}}
	t.Spec = spec.Compose(d.apps[t.App].Spec, t.Spec.Name, t.Overrides)
	d.tenants["iqtms"] = t
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
		return applied, nil
	}
	now := time.Now()
	stored := api.App{Name: name, Spec: app, Tag: tag, CreatedAt: now, UpdatedAt: now}
	if existed {
		stored.CreatedAt = old.CreatedAt
	}
	a.apps[name] = stored
	for _, tenant := range a.liveTenantsOf(name) {
		t := a.tenants[tenant]
		t.Spec = spec.Compose(app, tenant, t.Overrides)
		t.Generation++
		a.tenants[tenant] = t
		applied.Tenants = append(applied.Tenants, t)
	}
	action := "app.updated"
	if !existed {
		action = "app.created"
	}
	a.recordApp(action, name)
	stored.Tenants = a.liveTenantsOf(name)
	applied.App = stored
	return applied, nil
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
