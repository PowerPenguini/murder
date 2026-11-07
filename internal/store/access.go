package store

import (
	"database/sql"
	"errors"
	"fmt"
	"strings"
	"time"
)

// accessPrefix starts the action of every event of the access register
// (see package access).
const accessPrefix = "access."

// Access records an access to tenant's data (every tenant's, for an empty
// tenant) in the audit log, as an event of action, which starts with
// "access.", and returns the event. It is written as of the first schema
// version of batches, as Note is: every release applies it, so an access
// is recorded while the cluster upgrades. It is refused while changes are
// frozen (ErrFrozen), as other changes are: a rollback of the upgrade
// that froze them puts back the database from before, which would lose
// the access, not the data read.
// With once, an access of the same action, tenant and detail recorded
// before is returned instead of a second: the write is computed against
// the state it is applied to, so two such writes racing on any nodes
// record one.
func (s *SQLite) Access(tenant, action, detail, actor string, once bool) (Event, error) {
	if !strings.HasPrefix(action, accessPrefix) || len(action) == len(accessPrefix) {
		return Event{}, fmt.Errorf("%w: %q is not an access", ErrInvalid, action)
	}
	var at time.Time
	var before int64
	_, err := s.writeAs(firstBatchSchema, true, false, func(tx *recorder, current *snapshot, now time.Time) error {
		at, before = now, 0
		if once {
			err := s.db.QueryRow(`SELECT id FROM events WHERE tenant = ? AND action = ? AND detail = ? AND id <= ? ORDER BY id LIMIT 1`,
				tenant, action, detail, current.lastEvent).Scan(&before)
			if err == nil {
				return errRecordedBefore
			}
			if !errors.Is(err, sql.ErrNoRows) {
				return err
			}
		}
		return event(tx, now, actor, action, tenant, "", detail)
	})
	if errors.Is(err, errRecordedBefore) {
		return s.Event(before)
	}
	if err != nil {
		return Event{}, err
	}
	// The write is applied here before it returns: its event is the
	// newest one alike, which no other write can have made (each batch
	// is stamped as of its own writer's clock, to the nanosecond).
	var e Event
	var stamp string
	err = s.db.QueryRow(`SELECT id, at, actor, action, tenant, app, detail FROM events
		WHERE tenant = ? AND at = ? AND actor = ? AND action = ? AND detail = ? ORDER BY id DESC LIMIT 1`,
		tenant, formatTime(at), actorOf(actor), action, detail).Scan(&e.ID, &stamp, &e.Actor, &e.Action, &e.Tenant, &e.App, &e.Detail)
	if err != nil {
		return Event{}, fmt.Errorf("the access was recorded, but not found again: %w", err)
	}
	e.At = parseTime(stamp)
	return e, nil
}

// errRecordedBefore ends a write of an access recorded before.
var errRecordedBefore = errors.New("recorded before")

// Event is the event of ID id.
func (s *SQLite) Event(id int64) (Event, error) {
	var e Event
	var stamp string
	err := s.db.QueryRow(`SELECT id, at, actor, action, tenant, app, detail FROM events WHERE id = ? AND id <= ?`, id, s.state.Load().lastEvent).
		Scan(&e.ID, &stamp, &e.Actor, &e.Action, &e.Tenant, &e.App, &e.Detail)
	if err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return Event{}, fmt.Errorf("event %d: %w", id, ErrNotFound)
		}
		return Event{}, err
	}
	e.At = parseTime(stamp)
	return e, nil
}

// Accesses are the access register's events, oldest first, recorded
// since since (all for a zero time): those of tenant and those of every
// tenant (a copy of the state), or all of them for an empty tenant.
func (s *SQLite) Accesses(tenant string, since time.Time) ([]Event, error) {
	where, args := `id <= ? AND substr(action, 1, 7) = 'access.'`, []any{s.state.Load().lastEvent}
	if tenant != "" {
		where, args = where+` AND tenant IN (?, '')`, append(args, tenant)
	}
	rows, err := s.db.Query(`SELECT id, at, actor, action, tenant, app, detail FROM events WHERE `+where+` ORDER BY id`, args...)
	if err != nil {
		return nil, err
	}
	events := []Event{}
	for rows.Next() {
		var e Event
		var stamp string
		if err := rows.Scan(&e.ID, &stamp, &e.Actor, &e.Action, &e.Tenant, &e.App, &e.Detail); err != nil {
			rows.Close()
			return nil, err
		}
		e.At = parseTime(stamp)
		// Times are kept as text of varying length: compared as times.
		if !since.IsZero() && e.At.Before(since) {
			continue
		}
		events = append(events, e)
	}
	return events, closeRows(rows)
}
