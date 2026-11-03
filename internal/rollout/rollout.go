// Package rollout moves an app's tenants to its newest revision in waves,
// from the cluster's leader: canaries first, then a tenth of the tenants,
// half, and the rest, each wave healthy for a while before the next moves.
// A tenant that never comes up on the newest revision goes back to the one
// before, its data too, and the rollout pauses; one that comes up and then
// fails pauses it, its data left as it is, for someone to look.
package rollout

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"maps"
	"math"
	"slices"
	"strings"
	"sync"
	"time"

	"github.com/PowerPenguini/murder/internal/reconcile"
	"github.com/PowerPenguini/murder/internal/store"
)

// Actor is who the audit log names for what the driver does.
const Actor = "murder (rollout)"

// Store is what rollouts need of the store.
type Store interface {
	// RolloutState is every rollout and tenant, as of one moment.
	RolloutState() ([]store.Rollout, []store.Record)
	AdvanceRollout(app string, at store.RolloutAt, tenants []string, actor string) error
	FinishRollout(app string, at store.RolloutAt, actor string) error
	RollBackTenants(app string, at store.RolloutAt, tenants map[string]int64, restore bool, reason, actor string) error
	PauseRolloutAt(app string, at store.RolloutAt, reason, actor string) error
	MarkServed(app string, at store.RolloutAt, tenant string, generation int64) error
}

// StatusFunc reports a tenant's status from its node; false when unknown.
type StatusFunc func(ctx context.Context, record store.Record) (reconcile.Status, bool)

// Defaults of the driver's timing.
const (
	DefaultEvery      = 5 * time.Second
	DefaultCanarySoak = 2 * time.Minute
	DefaultSoak       = time.Minute
	DefaultFailAfter  = 5 * time.Minute
	// unhealthyGrace is how long a tenant that was healthy may fail its
	// health checks before the rollout pauses: one failed check is not a
	// failure.
	unhealthyGrace = 30 * time.Second
)

// shares are the parts of an app's tenants that have moved once each wave
// after the canaries is through.
var shares = []float64{0.1, 0.5, 1}

// Driver moves the tenants of running rollouts.
type Driver struct {
	Store  Store
	Status StatusFunc
	// Leader reports whether this node leads the cluster; nil outside one.
	Leader func() bool
	Every  time.Duration
	// CanarySoak and Soak are how long a wave's tenants stay healthy
	// before the next wave moves: the canaries', and the others'.
	CanarySoak, Soak time.Duration
	// FailAfter is how long a moved tenant may take to come up healthy.
	FailAfter time.Duration
	Now       func() time.Time
	Log       *slog.Logger

	// step keeps steps one at a time; mu guards watches.
	step sync.Mutex
	mu   sync.Mutex
	// watches follow each moved tenant, at its generation, through the
	// waves, for as long as this node leads.
	watches map[string]*watch
}

// watch is what the driver saw of a moved tenant at one generation.
type watch struct {
	generation int64
	// firstSeen is when its status was first known; gap that it was not
	// known for a while since: the driver cannot say then what happened.
	firstSeen      time.Time
	gap            bool
	container      string
	restarts       int
	healthySince   time.Time
	unhealthySince time.Time
	everHealthy    bool
	// marked says the store knows it served.
	marked bool
}

func (d *Driver) now() time.Time {
	if d.Now != nil {
		return d.Now()
	}
	return time.Now()
}

// Run drives rollouts until ctx ends.
func (d *Driver) Run(ctx context.Context) {
	every := d.Every
	if every == 0 {
		every = DefaultEvery
	}
	ticker := time.NewTicker(every)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
		}
		d.Step(ctx)
	}
}

// Step looks at every running rollout once, on the leader.
func (d *Driver) Step(ctx context.Context) {
	d.step.Lock()
	defer d.step.Unlock()
	if d.Leader != nil && !d.Leader() {
		d.mu.Lock()
		d.watches = nil
		d.mu.Unlock()
		return
	}
	rollouts, records := d.Store.RolloutState()
	for _, rollout := range rollouts {
		if rollout.State != store.RolloutRunning {
			continue
		}
		if err := d.drive(ctx, rollout, records); err != nil && !errors.Is(err, store.ErrConflict) && !errors.Is(err, store.ErrFrozen) {
			d.Log.Warn("drive a rollout", "app", rollout.App, "error", err)
		}
	}
}

// drive moves one rollout on, if its last wave is through.
func (d *Driver) drive(ctx context.Context, rollout store.Rollout, records []store.Record) error {
	var moved, pending []store.Record
	total := 0
	for _, record := range records {
		if record.App != rollout.App || record.Archived() || record.Hold {
			continue
		}
		switch {
		case record.AppRevision != 0:
			pending = append(pending, record)
		case record.BeforeRevision != 0:
			moved = append(moved, record)
		default:
			continue // on the newest already, not by this rollout
		}
		total++
	}
	if rollout.Wave > 0 {
		passed, err := d.judge(ctx, rollout, moved)
		if err != nil || !passed {
			return err
		}
	}
	if len(pending) == 0 {
		d.Log.Info("rollout done", "app", rollout.App, "revision", rollout.ToRevision, "waves", rollout.Wave)
		return d.Store.FinishRollout(rollout.App, rollout.At(), Actor)
	}
	next := nextWave(rollout.Wave, total, len(moved), pending)
	d.Log.Info("rollout moves a wave", "app", rollout.App, "wave", rollout.Wave+1, "tenants", next)
	return d.Store.AdvanceRollout(rollout.App, rollout.At(), next, Actor)
}

// nextWave names the tenants of the wave after wave: the canaries first,
// or the first tenant by name, then a share of all the tenants at a time.
func nextWave(wave, total, moved int, pending []store.Record) []string {
	slices.SortFunc(pending, func(a, b store.Record) int { return strings.Compare(a.Spec.Name, b.Spec.Name) })
	var names []string
	if wave == 0 {
		for _, record := range pending {
			if record.Canary {
				names = append(names, record.Spec.Name)
			}
		}
		if len(names) == 0 {
			names = []string{pending[0].Spec.Name}
		}
		return names
	}
	share := 1.0
	if wave-1 < len(shares) {
		share = shares[wave-1]
	}
	count := max(1, int(math.Ceil(share*float64(total)))-moved)
	for _, record := range pending[:min(count, len(pending))] {
		names = append(names, record.Spec.Name)
	}
	return names
}

// judge reports whether every moved tenant is through: healthy on its new
// revision, without restarting, for as long as its wave soaks. A tenant
// that never served on it, watched by this driver since it moved, goes
// back with its data; any other failure pauses the rollout, data as it
// is, for someone to look.
func (d *Driver) judge(ctx context.Context, rollout store.Rollout, moved []store.Record) (bool, error) {
	soak := d.Soak
	if soak == 0 {
		soak = DefaultSoak
	}
	if rollout.Wave == 1 {
		soak = d.CanarySoak
		if soak == 0 {
			soak = DefaultCanarySoak
		}
	}
	failAfter := d.FailAfter
	if failAfter == 0 {
		failAfter = DefaultFailAfter
	}
	every := d.Every
	if every == 0 {
		every = DefaultEvery
	}
	// How soon after its move the driver must have seen a tenant to know
	// it never served.
	coverage := max(3*every, unhealthyGrace)
	at := rollout.At()
	d.mu.Lock()
	if d.watches == nil {
		d.watches = map[string]*watch{}
	}
	d.mu.Unlock()
	through := true
	var broke []string
	back := map[string]int64{}
	for _, record := range moved {
		name := record.Spec.Name
		if record.Spec.Suspended {
			continue // nothing runs to judge; it starts on the new revision
		}
		d.mu.Lock()
		w := d.watches[name]
		if w == nil || w.generation != record.Generation {
			w = &watch{generation: record.Generation}
			d.watches[name] = w
		}
		d.mu.Unlock()
		now := d.now()
		status, known := d.Status(ctx, record)
		served := record.Served || w.everHealthy
		if !known || status.Generation < record.Generation {
			// Not known, or not yet of this generation: no time counts
			// as healthy, and what happened meanwhile is unknown.
			w.healthySince = time.Time{}
			if !known {
				w.gap = true
			}
			through = false
			if now.Sub(rollout.WaveStarted) > failAfter {
				broke = append(broke, name+"'s node does not say how it runs on this revision")
			}
			continue
		}
		if w.firstSeen.IsZero() {
			w.firstSeen = now
		}
		healthy := status.Healthy && status.Phase == reconcile.PhaseRunning
		restarted := false
		if status.ContainerID != "" {
			switch {
			case w.container != status.ContainerID:
				// Another container: its soak starts over, and one seen
				// first after it restarted restarted.
				w.healthySince = time.Time{}
				restarted = status.Restarts > 0
				w.container, w.restarts = status.ContainerID, status.Restarts
			case status.Restarts > w.restarts:
				w.restarts, restarted = status.Restarts, true
			}
		}
		if restarted {
			healthy = false
			w.healthySince = time.Time{}
			if served {
				broke = append(broke, name+" restarted")
			}
		}
		switch {
		case healthy:
			if w.healthySince.IsZero() {
				w.healthySince = now
			}
			w.everHealthy, w.unhealthySince = true, time.Time{}
			if !record.Served && !w.marked {
				if err := d.Store.MarkServed(rollout.App, at, name, record.Generation); err != nil {
					return false, err
				}
				w.marked = true
			}
			if now.Sub(w.healthySince) < soak {
				through = false
			}
		case served:
			w.healthySince = time.Time{}
			if w.unhealthySince.IsZero() {
				w.unhealthySince = now
			}
			if now.Sub(w.unhealthySince) > unhealthyGrace {
				broke = append(broke, name+" fails its health check")
			}
			through = false
		case now.Sub(rollout.WaveStarted) > failAfter:
			through = false
			if !w.gap && !w.firstSeen.After(record.UpdatedAt.Add(coverage)) {
				back[name] = record.Generation
			} else {
				broke = append(broke, name+" did not come up healthy, but this node did not watch it all along: its data stays (`murder app rollout abort` takes it back as it is)")
			}
		default:
			through = false
		}
	}
	if len(back) > 0 {
		names := slices.Sorted(maps.Keys(back))
		reason := fmt.Sprintf("%s did not come up healthy within %s on revision %d; back to the revision before, data too", strings.Join(names, ", "), failAfter, rollout.ToRevision)
		d.Log.Warn("rollout rolls tenants back", "app", rollout.App, "tenants", names)
		return false, d.Store.RollBackTenants(rollout.App, at, back, true, reason, Actor)
	}
	if len(broke) > 0 {
		reason := fmt.Sprintf("%s on revision %d: paused for someone to look (`murder app rollout resume` or `abort`)", strings.Join(broke, ", "), rollout.ToRevision)
		d.Log.Warn("rollout paused", "app", rollout.App, "reason", reason)
		return false, d.Store.PauseRolloutAt(rollout.App, at, reason, Actor)
	}
	return through, nil
}
