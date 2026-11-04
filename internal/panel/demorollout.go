package panel

import (
	"strconv"
	"time"

	"github.com/PowerPenguini/murder/internal/api"
	"github.com/PowerPenguini/murder/internal/reconcile"
	"github.com/PowerPenguini/murder/internal/rollout"
	"github.com/PowerPenguini/murder/internal/spec"
	"github.com/PowerPenguini/murder/internal/store"
)

// The demo's rollouts move as the cluster's driver moves them
// (internal/rollout), only sooner, so that one can be watched: each wave
// once every tenant moved before it is healthy and has stayed so.
const (
	demoCanarySoak = 20 * time.Second
	demoSoak       = 10 * time.Second
)

// moveTenant gives a demo tenant a spec: a new container when it changes,
// which starts and settles a few seconds later; the caller holds d.mu.
func (d *DemoCluster) moveTenant(t *api.Tenant, composed spec.Tenant, detail string) {
	changed := !composed.Equal(t.Spec)
	before, after := t.Spec.Limits.Or(spec.Limits{MemoryMB: 256}).MemoryMB, composed.Limits.Or(spec.Limits{MemoryMB: 256}).MemoryMB
	if before != after {
		if d.limitChanges == nil {
			d.limitChanges = map[string][]limitChange{}
		}
		d.limitChanges[t.Spec.Name] = append(d.limitChanges[t.Spec.Name], limitChange{at: time.Now(), from: before})
	}
	t.Spec = composed
	t.Generation++
	if changed && !t.Spec.Suspended && t.Status != nil {
		t.Status = &reconcile.Status{Phase: "starting", Generation: t.Generation, Restarts: t.Status.Restarts, LastExit: t.Status.LastExit, CheckedAt: time.Now()}
	}
	d.note("murder (rollout)", "app.rolled-out", t.Spec.Name, t.App, detail)
}

// revisionSpec is an app's revision's spec; the caller holds d.mu.
func (d *DemoCluster) revisionSpec(app string, revision int64) (spec.Tenant, bool) {
	for _, kept := range d.revisions[app] {
		if kept.Revision == revision {
			return kept.Spec, true
		}
	}
	return spec.Tenant{}, false
}

// drive moves each running rollout on, if its last wave is through; the
// caller holds d.mu.
func (d *DemoCluster) drive() {
	now := time.Now()
	for name, app := range d.apps {
		ro := app.Rollout
		if ro == nil || ro.State != store.RolloutRunning {
			continue
		}
		to, ok := d.revisionSpec(name, ro.ToRevision)
		if !ok {
			continue
		}
		var moved, pending []store.Record
		total := 0
		for _, t := range d.tenants {
			if t.App != name || t.Archived() || t.Hold {
				continue
			}
			record := store.Record{Spec: t.Spec, Canary: t.Canary}
			switch {
			case t.AppRevision != 0:
				pending = append(pending, record)
			case t.BeforeRevision != 0:
				moved = append(moved, record)
			default:
				continue
			}
			total++
		}
		if ro.Wave > 0 {
			soak := demoSoak
			if ro.Wave == 1 {
				soak = demoCanarySoak
			}
			through := now.Sub(ro.WaveStarted) >= soak
			for _, r := range moved {
				if s := d.tenants[r.Spec.Name].Status; s == nil || s.Phase != "running" || !s.Healthy {
					through = false
				}
			}
			if !through {
				continue
			}
		}
		next := *ro
		if len(pending) == 0 {
			for _, r := range moved {
				t := d.tenants[r.Spec.Name]
				t.BeforeRevision = 0
				d.tenants[r.Spec.Name] = t
			}
			next.State, next.UpdatedAt = store.RolloutDone, now
			app.Rollout = &next
			d.apps[name] = app
			d.note("murder (rollout)", "rollout.done", "", name, "revision "+strconv.FormatInt(ro.ToRevision, 10)+" reached every tenant")
			continue
		}
		for _, tenant := range rollout.NextWave(ro.Wave, total, len(moved), pending) {
			t := d.tenants[tenant]
			from := t.AppRevision
			t.BeforeRevision, t.AppRevision = from, 0
			composed := spec.Compose(to, tenant, t.Overrides)
			composed.Suspended = t.Spec.Suspended
			d.moveTenant(&t, composed, "wave "+strconv.Itoa(ro.Wave+1)+": revision "+strconv.FormatInt(from, 10)+" → "+strconv.FormatInt(ro.ToRevision, 10))
			d.tenants[tenant] = t
		}
		next.Wave++
		next.WaveStarted, next.UpdatedAt = now, now
		app.Rollout = &next
		d.apps[name] = app
	}
}
