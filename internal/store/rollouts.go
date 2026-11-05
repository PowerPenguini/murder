package store

import (
	"fmt"
	"maps"
	"slices"
	"strconv"
	"strings"
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
	// First counts its leading waves of tenants named to move first: 0 or
	// 1. The wave after them is the canaries.
	First int `json:"first,omitempty"`
}

// WaveName names its wave w, from 1, as its plan does: the tenants moved
// first, the canaries, a tenth, half, the rest.
func (r Rollout) WaveName(w int) string {
	if w <= r.First {
		return "First, now"
	}
	return WaveName(w - 1 - r.First)
}

// WaveName names a rollout's wave by its place among the waves after any
// moved first, from 0: the canaries first.
func WaveName(w int) string {
	names := []string{"Canaries", "A tenth", "Half", "The rest"}
	if w >= 0 && w < len(names) {
		return names[w]
	}
	return "Wave " + strconv.Itoa(w+1)
}

// WaveSaid names wave w as every sentence and event does, its name in
// lower case in parentheses: "wave 2 (a tenth)", "wave 1 (first, now)".
func WaveSaid(w int, name string) string {
	return "wave " + strconv.Itoa(w) + " (" + strings.ToLower(name) + ")"
}

// RestorePending reports whether the tenant's data is to go back to the
// snapshot from before its rollout moved it: until it has, it takes no
// request, so nothing is written to data about to be replaced.
func (r Record) RestorePending() bool { return r.Restore != "" }

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
	rows, err := q.Query(`SELECT app, to_revision, state, wave, wave_started, reason, started_at, updated_at, actor, epoch, first FROM rollouts`)
	if err != nil {
		return err
	}
	for rows.Next() {
		var r Rollout
		var waveStarted, started, updated string
		if err := rows.Scan(&r.App, &r.ToRevision, &r.State, &r.Wave, &waveStarted, &r.Reason, &started, &updated, &r.Actor, &r.Epoch, &r.First); err != nil {
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
			if err := tx.Exec(`UPDATE tenants SET before_revision = app_revision, app_revision = 0, moved_on = node, generation = generation + 1, updated_at = ? WHERE name = ?`, stamp, name); err != nil {
				return err
			}
			if err := event(tx, now, actor, "app.rolled-out", name, app, fmt.Sprintf("%s: revision %d → %d", WaveSaid(wave+1, rollout.WaveName(wave+1)), record.AppRevision, rollout.ToRevision)); err != nil {
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
		if err := tx.Exec(`UPDATE tenants SET before_revision = 0, moved_on = '' WHERE app = ? AND app_revision = 0`, app); err != nil {
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

// pendingRestores names the tenants of app whose data still waits to go
// back, as a restore asked.
func pendingRestores(state *snapshot, app string) []string {
	var names []string
	for _, name := range slices.Sorted(maps.Keys(state.tenants)) {
		if record := state.tenants[name]; record.App == app && record.Restore != "" {
			names = append(names, name)
		}
	}
	return names
}

// noPendingRestore refuses a change of app at once while a tenant of it
// waits for its data to go back: that would leave it waiting, stopped,
// with no rollout left to abort.
func noPendingRestore(state *snapshot, app string) error {
	if names := pendingRestores(state, app); len(names) > 0 {
		return fmt.Errorf("%w: the data of %s of app %s is still going back, as a restore asked; let it finish, or `murder app rollout abort %s` starts them on their data as it is, then change the app", ErrConflict, strings.Join(names, ", "), app, app)
	}
	return nil
}

// AbortRollout ends app's rollout and takes every tenant it moved back to
// the revision it ran before; their data stays as it is, that of tenants
// still waiting for theirs to go back too. With the rollout over, it
// still ends the waits of tenants whose data waits to go back.
func (s *SQLite) AbortRollout(app, reason, actor string) ([]string, error) {
	var back []string
	_, err := s.write(true, func(tx *recorder, current *snapshot, now time.Time) error {
		back = nil
		pending := pendingRestores(current, app)
		if _, err := rolloutOf(current, app, nil); err != nil {
			if len(pending) == 0 {
				return err
			}
			// Over, but tenants wait, stopped, for data that may never go
			// back: they start on it as it is.
			if err := tx.Exec(`UPDATE tenants SET restore = '' WHERE app = ? AND restore != ''`, app); err != nil {
				return err
			}
			back = pending
			return event(tx, now, actor, "rollout.aborted", "", app, fmt.Sprintf("the data of %s stays as it is: %s", strings.Join(pending, ", "), reason))
		}
		stamp := formatTime(now)
		if err := tx.Exec(`UPDATE tenants SET restore = '' WHERE app = ? AND restore != ''`, app); err != nil {
			return err
		}
		for _, name := range slices.Sorted(maps.Keys(current.tenants)) {
			record := current.tenants[name]
			if record.App != app || record.Archived() || record.AppRevision != 0 || record.BeforeRevision == 0 {
				continue
			}
			if err := tx.Exec(`UPDATE tenants SET app_revision = before_revision, before_revision = 0, moved_on = '', generation = generation + 1, updated_at = ? WHERE name = ?`, stamp, name); err != nil {
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

// RestoreTenants takes tenants of app's rollout back to the revision they
// ran before, their data too: to the snapshot their node took as their
// image changed, so what they wrote since is lost. With none named, every
// tenant the rollout moved goes back. It pauses the rollout, and changes
// nothing if one named was not moved by it, or cannot get its data back:
// one that moved to another node since its wave (that node has no snapshot
// from before), one moving now, or one whose image the rollout did not
// change (no snapshot was taken). It returns the tenants taken back.
func (s *SQLite) RestoreTenants(app string, tenants []string, actor string) ([]string, error) {
	var back []string
	_, err := s.write(true, func(tx *recorder, current *snapshot, now time.Time) error {
		rollout, err := rolloutOf(current, app, nil)
		if err != nil {
			return err
		}
		back = nil
		names := slices.Clone(tenants)
		if len(names) == 0 {
			for _, name := range slices.Sorted(maps.Keys(current.tenants)) {
				record := current.tenants[name]
				if record.App == app && !record.Archived() && record.AppRevision == 0 && record.BeforeRevision != 0 {
					names = append(names, name)
				}
			}
			if len(names) == 0 {
				return fmt.Errorf("%w: the rollout of app %s moved no tenant that could go back", ErrConflict, app)
			}
		}
		slices.Sort(names)
		names = slices.Compact(names)
		images := map[string]string{}
		for _, name := range names {
			record, ok := current.tenants[name]
			switch {
			case !ok || record.App != app:
				return fmt.Errorf("%w: tenant %s does not run app %s", ErrConflict, name, app)
			case record.Archived():
				return fmt.Errorf("%w: tenant %s is archived", ErrConflict, name)
			case record.AppRevision != 0 || record.BeforeRevision == 0:
				return fmt.Errorf("%w: the rollout of app %s did not move tenant %s to revision %d", ErrConflict, app, name, rollout.ToRevision)
			case record.MoveTo != "":
				return fmt.Errorf("%w: tenant %s is moving to node %s: its data cannot go back", ErrConflict, name, record.MoveTo)
			case record.MovedOn != record.Node:
				return fmt.Errorf("%w: tenant %s moved to node %s since its wave: that node has no snapshot of its data from before, so its data cannot go back (`murder app rollout abort %s` takes it back to the revision before, data as it is)", ErrConflict, name, record.Node, app)
			}
			image := spec.Compose(revisionSpec(current, app, record.BeforeRevision), name, record.Overrides).Image
			if image == spec.Compose(revisionSpec(current, app, 0), name, record.Overrides).Image {
				return fmt.Errorf("%w: the rollout did not change tenant %s's image, so no snapshot of its data was taken as it moved (`murder app rollout abort %s` takes it back to the revision before, data as it is)", ErrConflict, name, app)
			}
			images[name] = image
		}
		stamp := formatTime(now)
		for _, name := range names {
			record := current.tenants[name]
			if err := tx.Exec(`UPDATE tenants SET app_revision = before_revision, before_revision = 0, moved_on = '', restore = ?, generation = generation + 1, updated_at = ? WHERE name = ?`, images[name], stamp, name); err != nil {
				return err
			}
			if err := event(tx, now, actor, "rollout.restored", name, app, fmt.Sprintf("back to revision %d, its data to the snapshot taken as its image changed: what it wrote since is lost", record.BeforeRevision)); err != nil {
				return err
			}
			back = append(back, name)
		}
		reason := fmt.Sprintf("%s went back to the revision before, data too, as %s asked (`murder app rollout resume %s` moves them again, `abort` ends the rollout)", strings.Join(names, ", "), actor, app)
		if err := tx.Exec(`UPDATE rollouts SET state = 'paused', reason = ?, updated_at = ?, epoch = epoch + 1 WHERE app = ?`, reason, stamp, app); err != nil {
			return err
		}
		return event(tx, now, actor, "rollout.paused", "", app, reason)
	})
	return back, err
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

// FlagsDetail says what a change of a tenant's rollout flags changes:
// "held: no → yes", "canary: yes → no".
func FlagsDetail(canary, hold, wantCanary, wantHold bool) string {
	yes := map[bool]string{true: "yes", false: "no"}
	var parts []string
	if canary != wantCanary {
		parts = append(parts, "canary: "+yes[canary]+" → "+yes[wantCanary])
	}
	if hold != wantHold {
		parts = append(parts, "held: "+yes[hold]+" → "+yes[wantHold])
	}
	return strings.Join(parts, ", ")
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
		return event(tx, now, actor, "tenant.flags", name, record.App, FlagsDetail(record.Canary, record.Hold, wantCanary, wantHold))
	})
	if err == errUnchanged {
		return s.Get(name)
	}
	if err != nil {
		return Record{}, err
	}
	return next.tenants[name].clone(), nil
}
