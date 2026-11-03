package store

import (
	"fmt"
	"maps"
	"slices"
	"time"

	"github.com/PowerPenguini/murder/internal/spec"
)

// Rollout states.
const (
	RolloutRunning = "running"
	RolloutPaused  = "paused"
	RolloutDone    = "done"
	RolloutAborted = "aborted"
)

// Rollout is an app's newest rollout: its tenants move to ToRevision in
// waves, each healthy for a while before the next moves.
type Rollout struct {
	App        string `json:"app"`
	ToRevision int64  `json:"toRevision"`
	State      string `json:"state"`
	// Wave counts the waves moved so far; WaveStarted is when the last
	// one moved.
	Wave        int       `json:"wave"`
	WaveStarted time.Time `json:"waveStarted"`
	// Reason says why it is paused, done or aborted.
	Reason    string    `json:"reason,omitempty"`
	StartedAt time.Time `json:"startedAt"`
	UpdatedAt time.Time `json:"updatedAt"`
	Actor     string    `json:"actor"`
	// Epoch counts its changes of state: each wave, pause and resume.
	Epoch int64 `json:"epoch"`
}

// Active reports whether the rollout may still move tenants.
func (r Rollout) Active() bool { return r.State == RolloutRunning || r.State == RolloutPaused }

// At names the rollout as it is: a change the driver decided on what it
// saw then is refused once the rollout has moved on, been replaced, paused
// or resumed.
func (r Rollout) At() RolloutAt {
	return RolloutAt{ToRevision: r.ToRevision, Wave: r.Wave, WaveStarted: r.WaveStarted, Epoch: r.Epoch}
}

// RolloutAt is a rollout at one wave: its revision, the wave and when it
// started.
type RolloutAt struct {
	ToRevision  int64
	Wave        int
	WaveStarted time.Time
	Epoch       int64
}

// RolloutState is every rollout and tenant as of one moment.
func (s *SQLite) RolloutState() ([]Rollout, []Record) {
	state := s.state.Load()
	rollouts := make([]Rollout, 0, len(state.rollouts))
	for _, app := range slices.Sorted(maps.Keys(state.rollouts)) {
		rollouts = append(rollouts, state.rollouts[app])
	}
	records := make([]Record, 0, len(state.tenants))
	for _, name := range slices.Sorted(maps.Keys(state.tenants)) {
		records = append(records, state.tenants[name].clone())
	}
	return rollouts, records
}

func loadRollouts(q querier, state *snapshot) error {
	state.rollouts = map[string]Rollout{}
	rows, err := q.Query(`SELECT app, to_revision, state, wave, wave_started, reason, started_at, updated_at, actor, epoch FROM rollouts`)
	if err != nil {
		return err
	}
	for rows.Next() {
		var r Rollout
		var waveStarted, started, updated string
		if err := rows.Scan(&r.App, &r.ToRevision, &r.State, &r.Wave, &waveStarted, &r.Reason, &started, &updated, &r.Actor, &r.Epoch); err != nil {
			rows.Close()
			return err
		}
		r.WaveStarted, r.StartedAt, r.UpdatedAt = parseTime(waveStarted), parseTime(started), parseTime(updated)
		state.rollouts[r.App] = r
	}
	return closeRows(rows)
}

// revisionSpec is the spec of app at revision: its newest for 0, or one no
// longer kept.
func revisionSpec(state *snapshot, app string, revision int64) spec.Tenant {
	if revision != 0 {
		for _, kept := range state.revisions[app] {
			if kept.Revision == revision {
				return kept.Spec
			}
		}
	}
	return state.apps[app].Spec
}

// Rollouts lists every app's newest rollout, by app.
func (s *SQLite) Rollouts() []Rollout {
	state := s.state.Load()
	rollouts := make([]Rollout, 0, len(state.rollouts))
	for _, app := range slices.Sorted(maps.Keys(state.rollouts)) {
		rollouts = append(rollouts, state.rollouts[app])
	}
	return rollouts
}

// Rollout is an app's newest rollout.
func (s *SQLite) Rollout(app string) (Rollout, error) {
	rollout, ok := s.state.Load().rollouts[app]
	if !ok {
		return Rollout{}, fmt.Errorf("app %s has no rollout: %w", app, ErrNotFound)
	}
	return rollout, nil
}

// rolloutOf is the active rollout of app, as at says if it is not nil, or
// why not.
func rolloutOf(state *snapshot, app string, at *RolloutAt) (Rollout, error) {
	rollout, ok := state.rollouts[app]
	switch {
	case !ok:
		return Rollout{}, fmt.Errorf("app %s has no rollout: %w", app, ErrNotFound)
	case !rollout.Active():
		return Rollout{}, fmt.Errorf("%w: the rollout of app %s is %s", ErrConflict, app, rollout.State)
	case at != nil && !(rollout.ToRevision == at.ToRevision && rollout.Wave == at.Wave && rollout.WaveStarted.Equal(at.WaveStarted) && rollout.Epoch == at.Epoch):
		return Rollout{}, fmt.Errorf("%w: the rollout of app %s moved on (revision %d, wave %d)", ErrConflict, app, rollout.ToRevision, rollout.Wave)
	}
	return rollout, nil
}

// AdvanceRollout moves tenants of app, as its rollout's wave after the one
// at names, to the app's newest revision. Only a running rollout as at
// says moves.
func (s *SQLite) AdvanceRollout(app string, at RolloutAt, tenants []string, actor string) error {
	wave := at.Wave
	_, err := s.write(true, func(tx *recorder, current *snapshot, now time.Time) error {
		rollout, err := rolloutOf(current, app, &at)
		if err != nil {
			return err
		}
		if rollout.State != RolloutRunning {
			return fmt.Errorf("%w: the rollout of app %s is paused", ErrConflict, app)
		}
		stamp := formatTime(now)
		for _, name := range tenants {
			record, ok := current.tenants[name]
			if !ok || record.App != app || record.Archived() || record.Hold || record.AppRevision == 0 {
				continue
			}
			if err := tx.Exec(`UPDATE tenants SET before_revision = app_revision, app_revision = 0, served = 0, moved_on = node, generation = generation + 1, updated_at = ? WHERE name = ?`, stamp, name); err != nil {
				return err
			}
			if err := event(tx, now, actor, "app.rolled-out", name, app, fmt.Sprintf("wave %d: revision %d → %d", wave+1, record.AppRevision, rollout.ToRevision)); err != nil {
				return err
			}
		}
		return tx.Exec(`UPDATE rollouts SET wave = ?, wave_started = ?, updated_at = ?, epoch = epoch + 1 WHERE app = ?`, wave+1, stamp, stamp, app)
	})
	return err
}

// FinishRollout marks app's rollout done once every tenant it moves runs
// the newest revision: refused while one, released from its hold
// meanwhile say, still waits.
func (s *SQLite) FinishRollout(app string, at RolloutAt, actor string) error {
	_, err := s.write(false, func(tx *recorder, current *snapshot, now time.Time) error {
		rollout, err := rolloutOf(current, app, &at)
		if err != nil {
			return err
		}
		for _, name := range slices.Sorted(maps.Keys(current.tenants)) {
			record := current.tenants[name]
			if record.App == app && !record.Archived() && !record.Hold && record.AppRevision != 0 {
				return fmt.Errorf("%w: tenant %s of app %s still waits for the rollout", ErrConflict, name, app)
			}
		}
		stamp := formatTime(now)
		if err := tx.Exec(`UPDATE tenants SET before_revision = 0, served = 0, moved_on = '' WHERE app = ? AND app_revision = 0`, app); err != nil {
			return err
		}
		if err := tx.Exec(`UPDATE rollouts SET state = 'done', reason = '', updated_at = ?, epoch = epoch + 1 WHERE app = ?`, stamp, app); err != nil {
			return err
		}
		return event(tx, now, actor, "rollout.done", "", app, fmt.Sprintf("revision %d, in %d waves", rollout.ToRevision, rollout.Wave))
	})
	return err
}

// PauseRollout stops app's rollout from moving more tenants, for reason.
func (s *SQLite) PauseRollout(app, reason, actor string) error {
	return s.setRollout(app, nil, RolloutPaused, reason, actor)
}

// PauseRolloutAt is PauseRollout for the rollout as at names it, and none
// other.
func (s *SQLite) PauseRolloutAt(app string, at RolloutAt, reason, actor string) error {
	return s.setRollout(app, &at, RolloutPaused, reason, actor)
}

// ResumeRollout lets app's paused rollout move tenants again: the wave
// that paused it starts over.
func (s *SQLite) ResumeRollout(app, actor string) error {
	return s.setRollout(app, nil, RolloutRunning, "", actor)
}

func (s *SQLite) setRollout(app string, at *RolloutAt, state, reason, actor string) error {
	_, err := s.write(false, func(tx *recorder, current *snapshot, now time.Time) error {
		rollout, err := rolloutOf(current, app, at)
		if err != nil {
			return err
		}
		if rollout.State == state {
			return errUnchanged
		}
		stamp := formatTime(now)
		if err := tx.Exec(`UPDATE rollouts SET state = ?, reason = ?, wave_started = ?, updated_at = ?, epoch = epoch + 1 WHERE app = ?`, state, reason, stamp, stamp, app); err != nil {
			return err
		}
		action := "rollout.resumed"
		if state == RolloutPaused {
			action = "rollout.paused"
		}
		return event(tx, now, actor, action, "", app, reason)
	})
	if err == errUnchanged {
		return nil
	}
	return err
}

// AbortRollout ends app's rollout and takes every tenant it moved back to
// the revision it ran before; their data stays as it is, that of tenants
// still waiting for theirs to go back too.
func (s *SQLite) AbortRollout(app, reason, actor string) ([]string, error) {
	var back []string
	_, err := s.write(true, func(tx *recorder, current *snapshot, now time.Time) error {
		if _, err := rolloutOf(current, app, nil); err != nil {
			return err
		}
		back = nil
		stamp := formatTime(now)
		if err := tx.Exec(`UPDATE tenants SET restore = '' WHERE app = ? AND restore != ''`, app); err != nil {
			return err
		}
		for _, name := range slices.Sorted(maps.Keys(current.tenants)) {
			record := current.tenants[name]
			if record.App != app || record.Archived() || record.AppRevision != 0 || record.BeforeRevision == 0 {
				continue
			}
			if err := tx.Exec(`UPDATE tenants SET app_revision = before_revision, before_revision = 0, served = 0, moved_on = '', generation = generation + 1, updated_at = ? WHERE name = ?`, stamp, name); err != nil {
				return err
			}
			if err := event(tx, now, actor, "rollout.rolled-back", name, app, fmt.Sprintf("back to revision %d: %s", record.BeforeRevision, reason)); err != nil {
				return err
			}
			back = append(back, name)
		}
		if err := tx.Exec(`UPDATE rollouts SET state = 'aborted', reason = ?, updated_at = ?, epoch = epoch + 1 WHERE app = ?`, reason, stamp, app); err != nil {
			return err
		}
		return event(tx, now, actor, "rollout.aborted", "", app, reason)
	})
	return back, err
}

// RollBackTenants takes tenants of app's rollout, as at names it, that
// failed on the newest revision back to the one they ran before, and
// pauses the rollout for reason. tenants are their generations as seen
// failing: one changed since is a change of the rollout, refused. With
// restore, their data goes back too, to the snapshot taken as their image
// changed: for tenants that never served on the newest, on the node they
// moved on, which keeps that snapshot.
func (s *SQLite) RollBackTenants(app string, at RolloutAt, tenants map[string]int64, restore bool, reason, actor string) error {
	_, err := s.write(true, func(tx *recorder, current *snapshot, now time.Time) error {
		if _, err := rolloutOf(current, app, &at); err != nil {
			return err
		}
		stamp := formatTime(now)
		for _, name := range slices.Sorted(maps.Keys(tenants)) {
			record, ok := current.tenants[name]
			if !ok || record.App != app || record.Archived() || record.AppRevision != 0 || record.BeforeRevision == 0 {
				continue
			}
			if record.Generation != tenants[name] {
				return fmt.Errorf("%w: tenant %s changed since it was seen failing", ErrConflict, name)
			}
			image := ""
			if restore {
				// The old image never starts on data the newer one may
				// have changed, unless the data goes back too.
				if record.Served || record.MovedOn != record.Node {
					return fmt.Errorf("%w: tenant %s served on the newest revision or moved since its wave: its data cannot go back", ErrConflict, name)
				}
				image = spec.Compose(revisionSpec(current, app, record.BeforeRevision), name, record.Overrides).Image
			}
			if err := tx.Exec(`UPDATE tenants SET app_revision = before_revision, before_revision = 0, served = 0, moved_on = '', restore = ?, generation = generation + 1, updated_at = ? WHERE name = ?`, image, stamp, name); err != nil {
				return err
			}
			detail := fmt.Sprintf("back to revision %d: %s", record.BeforeRevision, reason)
			if image != "" {
				detail += "; its data goes back to the snapshot taken as its image changed"
			}
			if err := event(tx, now, actor, "rollout.rolled-back", name, app, detail); err != nil {
				return err
			}
		}
		if err := tx.Exec(`UPDATE rollouts SET state = 'paused', reason = ?, updated_at = ?, epoch = epoch + 1 WHERE app = ?`, reason, stamp, app); err != nil {
			return err
		}
		return event(tx, now, actor, "rollout.paused", "", app, reason)
	})
	return err
}

// Served records, from the node that runs it, that tenant at generation
// passed a health check on the revision its rollout moved it to: before it
// is served, so no data it then takes goes back by itself.
func (s *SQLite) Served(tenant string, generation int64) error {
	_, err := s.write(false, func(tx *recorder, current *snapshot, now time.Time) error {
		record, ok := current.tenants[tenant]
		if !ok || record.Served || record.Generation != generation || record.AppRevision != 0 || record.BeforeRevision == 0 {
			return errUnchanged
		}
		return tx.Exec(`UPDATE tenants SET served = 1 WHERE name = ?`, tenant)
	})
	if err == errUnchanged {
		return nil
	}
	return err
}

// MarkServed records that tenant, at generation, was seen healthy on the
// revision app's rollout, as at names it, moved it to.
func (s *SQLite) MarkServed(app string, at RolloutAt, tenant string, generation int64) error {
	_, err := s.write(false, func(tx *recorder, current *snapshot, now time.Time) error {
		if _, err := rolloutOf(current, app, &at); err != nil {
			return err
		}
		record, ok := current.tenants[tenant]
		if !ok || record.App != app || record.Served || record.Generation != generation || record.AppRevision != 0 || record.BeforeRevision == 0 {
			return errUnchanged
		}
		return tx.Exec(`UPDATE tenants SET served = 1 WHERE name = ?`, tenant)
	})
	if err == errUnchanged {
		return nil
	}
	return err
}

// Restored records that a tenant's data went back to image, as its
// Restore asked: nothing changes unless it still asks for image.
func (s *SQLite) Restored(name, image string) error {
	_, err := s.write(false, func(tx *recorder, current *snapshot, now time.Time) error {
		record, ok := current.tenants[name]
		if !ok || record.Restore != image || image == "" {
			return errUnchanged
		}
		if err := tx.Exec(`UPDATE tenants SET restore = '' WHERE name = ?`, name); err != nil {
			return err
		}
		return event(tx, now, "murder", "tenant.data-restored", name, record.App, "back to the snapshot taken before image "+image+" was replaced")
	})
	if err == errUnchanged {
		return nil
	}
	return err
}

// SetFlags makes a tenant a canary or not, held or not. A tenant no longer
// held, while its app rolls nothing out, runs the app's newest revision.
func (s *SQLite) SetFlags(name string, canary, hold *bool, actor string) (Record, error) {
	next, err := s.write(true, func(tx *recorder, current *snapshot, now time.Time) error {
		record, ok := current.tenants[name]
		if !ok {
			return fmt.Errorf("tenant %s: %w", name, ErrNotFound)
		}
		wantCanary, wantHold := record.Canary, record.Hold
		if canary != nil {
			wantCanary = *canary
		}
		if hold != nil {
			wantHold = *hold
		}
		if wantCanary == record.Canary && wantHold == record.Hold {
			return errUnchanged
		}
		stamp := formatTime(now)
		if err := tx.Exec(`UPDATE tenants SET canary = ?, hold = ?, updated_at = ? WHERE name = ?`, wantCanary, wantHold, stamp, name); err != nil {
			return err
		}
		rollout, rolling := current.rollouts[record.App]
		if record.Hold && !wantHold && record.AppRevision != 0 && !record.Archived() && !(rolling && rollout.Active()) {
			if err := tx.Exec(`UPDATE tenants SET app_revision = 0, before_revision = ?, generation = generation + 1 WHERE name = ?`, record.AppRevision, name); err != nil {
				return err
			}
		}
		return event(tx, now, actor, "tenant.flags", name, record.App, fmt.Sprintf("canary %v, hold %v", wantCanary, wantHold))
	})
	if err == errUnchanged {
		return s.Get(name)
	}
	if err != nil {
		return Record{}, err
	}
	return next.tenants[name].clone(), nil
}
