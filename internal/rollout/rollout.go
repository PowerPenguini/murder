// Package rollout moves an app's tenants to its newest revision in waves,
// from the cluster's leader: canaries first, then a tenth of the tenants,
// half, and the rest, each wave healthy for a while before the next moves.
// A tenant that does not come up healthy on the newest revision, breaks
// after it came up, or whose node does not say how it runs pauses the
// rollout for someone to decide: abort takes the tenants it moved back to
// the revision before, their data as it is; restore takes them back with
// their data from before they moved. The driver itself takes nothing back.
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
	// RolloutState is every rollout and tenant, as of one moment.
	RolloutState() ([]store.Rollout, []store.Record)
	AdvanceRollout(app string, at store.RolloutAt, tenants []string, actor string) error
	FinishRollout(app string, at store.RolloutAt, actor string) error
	PauseRolloutAt(app string, at store.RolloutAt, reason, actor string) error
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
	// FailAfter is how long a moved tenant may take to come up healthy,
	// and its node not say how it runs.
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
	// lastSeen is when this driver last looked at it, epoch the rollout's
	// then: a soak counts only time watched all along.
	lastSeen time.Time
	epoch    int64
	// checkedAt is the time its node last checked it, as that node says,
	// and checkedChanged when this driver saw it change: a result that does
	// not change for long is old, whatever the two clocks say.
	checkedAt, checkedChanged time.Time
	// soakChecked is checkedAt as its soak started: a soak is through
	// only on a check made since.
	soakChecked time.Time
	// unknownSince is when its node stopped saying how it runs.
	unknownSince   time.Time
	container      string
	restarts       int
	healthySince   time.Time
	unhealthySince time.Time
	// everHealthy says it came up healthy on its new revision.
	everHealthy bool
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
			// Not watched while paused: no soak counts the time.
			d.mu.Lock()
			for _, record := range records {
				if w := d.watches[record.Spec.Name]; w != nil && record.App == rollout.App {
					w.healthySince = time.Time{}
				}
			}
			d.mu.Unlock()
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
	// Waves count after those moved first: the next is the canaries then.
	next := nextWave(rollout.Wave-rollout.First, total, len(moved), pending)
	d.Log.Info("rollout moves a wave", "app", rollout.App, "wave", rollout.Wave+1, "tenants", next)
	if err := d.Store.AdvanceRollout(rollout.App, rollout.At(), next, Actor); err != nil {
		return err
	}
	// Watched from the move on: each moved at the generation after.
	now := d.now()
	generations := map[string]int64{}
	for _, record := range pending {
		generations[record.Spec.Name] = record.Generation
	}
	d.mu.Lock()
	defer d.mu.Unlock()
	if d.watches == nil {
		d.watches = map[string]*watch{}
	}
	// The store took the wave as one more change of the rollout. Looked at
	// from now, with no check seen: a check of the new generation, made
	// after the move, is fresh at the first look.
	for _, name := range next {
		d.watches[name] = &watch{generation: generations[name] + 1, lastSeen: now, epoch: rollout.Epoch + 1, checkedChanged: now}
	}
	return nil
}

// NextWave names the tenants of the wave after wave, as the driver moves
// them: for showing a rollout's plan before it runs.
func NextWave(wave, total, moved int, pending []store.Record) []string {
	return nextWave(wave, total, moved, slices.Clone(pending))
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
// revision, without restarting, for as long as its wave soaks. Any that
// does not come up healthy within FailAfter, breaks after it came up, or
// whose node does not say how it runs pauses the rollout, naming the ways
// on; the driver takes nothing back itself.
func (d *Driver) judge(ctx context.Context, rollout store.Rollout, moved []store.Record) (bool, error) {
	soak := d.Soak
	if soak == 0 {
		soak = DefaultSoak
	}
	// The first wave soaks as canaries do; after tenants moved first, the
	// canaries' wave too.
	if rollout.Wave <= rollout.First+1 {
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
	// The longest the driver may not look at a tenant and still count the
	// time as healthy.
	coverage := max(3*every, unhealthyGrace)
	d.mu.Lock()
	if d.watches == nil {
		d.watches = map[string]*watch{}
	}
	d.mu.Unlock()
	through := true
	var broke []string
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
		if !w.lastSeen.IsZero() && now.Sub(w.lastSeen) > coverage {
			// Not looked at for long: its soak starts over, and a check
			// made meanwhile may be as old as the gap.
			w.healthySince, w.checkedChanged = time.Time{}, time.Time{}
		}
		if w.epoch != rollout.Epoch {
			// The rollout changed meanwhile (a pause and resume, say): its
			// soak starts over.
			w.healthySince = time.Time{}
		}
		w.lastSeen, w.epoch = now, rollout.Epoch
		status, known := d.Status(ctx, record)
		// A result its node has not checked again for long says nothing
		// of now: measured by when this driver saw it change, so the two
		// nodes' clocks need not agree. A result is fresh once it changed
		// since an earlier look: the first look cannot tell how old it is.
		fresh := false
		if known {
			switch {
			case w.checkedChanged.IsZero():
				w.checkedAt, w.checkedChanged = status.CheckedAt, now
			case !status.CheckedAt.Equal(w.checkedAt):
				w.checkedAt, w.checkedChanged, fresh = status.CheckedAt, now, true
			case now.Sub(w.checkedChanged) > coverage:
				known = false
			}
		}
		if !known {
			// No time counts as healthy while its node does not say.
			w.healthySince = time.Time{}
			through = false
			if w.unknownSince.IsZero() {
				w.unknownSince = now
			}
			if now.Sub(later(rollout.WaveStarted, w.unknownSince)) > failAfter {
				broke = append(broke, fmt.Sprintf("%s's node has not said how it runs for %s", name, failAfter))
			}
			continue
		}
		w.unknownSince = time.Time{}
		if status.Generation < record.Generation {
			// Its node has not started the new revision yet.
			w.healthySince = time.Time{}
			through = false
			if now.Sub(rollout.WaveStarted) > failAfter {
				broke = append(broke, fmt.Sprintf("%s did not come up within %s: its node has not started the new revision", name, failAfter))
			}
			continue
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
			if w.everHealthy {
				broke = append(broke, name+" restarted after it came up")
			}
		}
		switch {
		case healthy:
			// Soaked on this driver's clock, from the look that saw it
			// healthy on a fresh check, its node checking it again all
			// along: a result already old at the first look starts
			// nothing, and the same result read again is no more time
			// healthy.
			if w.healthySince.IsZero() && fresh {
				w.healthySince, w.soakChecked = now, status.CheckedAt
			}
			w.everHealthy, w.unhealthySince = true, time.Time{}
			if w.healthySince.IsZero() || now.Sub(w.healthySince) < soak || !status.CheckedAt.After(w.soakChecked) {
				through = false
			}
		case w.everHealthy:
			w.healthySince = time.Time{}
			if w.unhealthySince.IsZero() {
				w.unhealthySince = now
			}
			if now.Sub(w.unhealthySince) > unhealthyGrace && !restarted {
				broke = append(broke, name+" fails its health check after it came up")
			}
			through = false
		case now.Sub(rollout.WaveStarted) > failAfter:
			through = false
			broke = append(broke, fmt.Sprintf("%s did not come up healthy within %s", name, failAfter))
		default:
			through = false
		}
	}
	if len(broke) > 0 {
		reason := fmt.Sprintf("%s, on revision %d. Paused: `murder app rollout abort %s` takes every tenant it moved back to the revision before, their data as it is; `murder app rollout restore %s` takes them back with their data from before they moved (-tenant NAME for some only), losing what they wrote since", strings.Join(broke, "; "), rollout.ToRevision, rollout.App, rollout.App)
		d.Log.Warn("rollout paused", "app", rollout.App, "reason", reason)
		return false, d.Store.PauseRolloutAt(rollout.App, rollout.At(), reason, Actor)
	}
	return through, nil
}

// later is the later of a and b.
func later(a, b time.Time) time.Time {
	if a.After(b) {
		return a
	}
	return b
}
