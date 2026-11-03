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
	Rollouts() []store.Rollout
	List() []store.Record
	AdvanceRollout(app string, wave int, tenants []string, actor string) error
	FinishRollout(app string, wave int, actor string) error
	RollBackTenants(app string, tenants []string, restore bool, reason, actor string) error
	PauseRollout(app, reason, actor string) error
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

	mu sync.Mutex
	// watches follow each moved tenant through its wave.
	watches map[string]*watch
}

// watch is what the driver saw of a moved tenant since its wave moved.
type watch struct {
	wave           time.Time
	container      string
	restarts       int
	healthySince   time.Time
	unhealthySince time.Time
	everHealthy    bool
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
	if d.Leader != nil && !d.Leader() {
		d.mu.Lock()
		d.watches = nil
		d.mu.Unlock()
		return
	}
	records := d.Store.List()
	for _, rollout := range d.Store.Rollouts() {
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
		return d.Store.FinishRollout(rollout.App, rollout.Wave, Actor)
	}
	next := nextWave(rollout.Wave, total, len(moved), pending)
	d.Log.Info("rollout moves a wave", "app", rollout.App, "wave", rollout.Wave+1, "tenants", next)
	return d.Store.AdvanceRollout(rollout.App, rollout.Wave, next, Actor)
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
// that fails rolls back or pauses the rollout.
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
	now := d.now()
	d.mu.Lock()
	defer d.mu.Unlock()
	if d.watches == nil {
		d.watches = map[string]*watch{}
	}
	through := true
	var neverUp, broke []string
	for _, record := range moved {
		name := record.Spec.Name
		w := d.watches[name]
		if w == nil || !w.wave.Equal(rollout.WaveStarted) {
			w = &watch{wave: rollout.WaveStarted}
			d.watches[name] = w
		}
		status, known := d.Status(ctx, record)
		healthy := known && status.Generation >= record.Generation && status.Healthy && status.Phase == reconcile.PhaseRunning
		if known && status.ContainerID != "" {
			if w.container != status.ContainerID {
				w.container, w.restarts = status.ContainerID, status.Restarts
			} else if status.Restarts > w.restarts {
				// It exited and was started again: not through.
				w.restarts, healthy = status.Restarts, false
				if w.everHealthy {
					broke = append(broke, name+" restarted")
				}
			}
		}
		switch {
		case healthy:
			if w.healthySince.IsZero() {
				w.healthySince = now
			}
			w.everHealthy, w.unhealthySince = true, time.Time{}
			if now.Sub(w.healthySince) < soak {
				through = false
			}
		case !w.everHealthy && now.Sub(rollout.WaveStarted) > failAfter && known:
			neverUp = append(neverUp, name)
			through = false
		case !known && now.Sub(rollout.WaveStarted) > failAfter:
			// Its node does not answer: it may serve, so nothing goes back.
			broke = append(broke, name+"'s node does not say how it runs")
			through = false
		case w.everHealthy && known:
			w.healthySince = time.Time{}
			if w.unhealthySince.IsZero() {
				w.unhealthySince = now
			}
			if now.Sub(w.unhealthySince) > unhealthyGrace {
				broke = append(broke, name+" fails its health check")
			}
			through = false
		default:
			through = false
		}
	}
	if len(neverUp) > 0 {
		reason := fmt.Sprintf("%s did not come up healthy within %s on revision %d; back to the revision before, data too", strings.Join(neverUp, ", "), failAfter, rollout.ToRevision)
		d.Log.Warn("rollout rolls tenants back", "app", rollout.App, "tenants", neverUp)
		return false, d.Store.RollBackTenants(rollout.App, neverUp, true, reason, Actor)
	}
	if len(broke) > 0 {
		reason := fmt.Sprintf("%s on revision %d: paused for someone to look (`murder app rollout resume` or `abort`)", strings.Join(broke, ", "), rollout.ToRevision)
		d.Log.Warn("rollout paused", "app", rollout.App, "reason", reason)
		return false, d.Store.PauseRollout(rollout.App, reason, Actor)
	}
	return through, nil
}
