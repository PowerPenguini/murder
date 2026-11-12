package store

import (
	"context"
	"crypto/rand"
	"database/sql"
	"errors"
	"fmt"
	"time"
)

// LeaseSchema is the schema version that keeps the cluster's operation
// lease: a node takes it only once every node runs a release of this
// version or later, which applies those writes.
const LeaseSchema = 20

// LeaseTTL is how long a lease holds without a renewal. Its holder renews
// it every LeaseTTL/4, so a holder that died (a command line killed, a
// host that lost power) lets go within LeaseTTL, while one that runs keeps
// it through a few writes the cluster could not take, as when its leader
// changes.
const LeaseTTL = 2 * time.Minute

// ErrLeased refuses a change of the cluster while another operation holds
// the lease. It is a conflict: the change waits for the other one.
var ErrLeased = fmt.Errorf("%w: another operation changes the cluster", ErrConflict)

// ErrLeaseLost refuses a renewal of a lease that is no longer its
// holder's: it expired and another took it, or a person broke it.
var ErrLeaseLost = fmt.Errorf("%w: the operation no longer holds the cluster", ErrConflict)

// Lease is the cluster's operation lease: one operation that changes the
// cluster over many steps (a drain, a balance, a reboot, a replacement)
// holds it, so that no other runs meanwhile. An upgrade and a master key
// rotation hold the freeze instead, which excludes a lease as a lease
// excludes a freeze.
type Lease struct {
	// ID is the lease's own: its holder renews, releases and acts under it
	// by ID, and a person breaks it by ID, so as not to break one taken
	// since.
	ID string `json:"id"`
	// Kind is the operation ("drain", "balance", "reboot", "replace"),
	// Detail what it works on ("node n2 to n3").
	Kind   string `json:"kind"`
	Detail string `json:"detail,omitempty"`
	// Actor is who began it; Node the node whose agent took it, where its
	// command line or panel runs.
	Actor   string    `json:"actor"`
	Node    string    `json:"node"`
	Started time.Time `json:"started"`
	Renewed time.Time `json:"renewed"`
	Expires time.Time `json:"expires"`
}

// Held reports whether the lease holds at now: one that expired is no
// one's, though its row stays until another operation takes its place.
func (l Lease) Held(now time.Time) bool { return l.ID != "" && now.Before(l.Expires) }

// String names the operation and its holder, for a refusal.
func (l Lease) String() string {
	what := l.Kind
	if l.Detail != "" {
		what += " " + l.Detail
	}
	return fmt.Sprintf("%s, begun by %s on node %s at %s (lease %s, held until %s unless renewed)", what, l.Actor, l.Node, l.Started.Local().Format(time.DateTime), l.ID, l.Expires.Local().Format(time.TimeOnly))
}

// leaseTable is the table schema 20 added.
const leaseTable = `-- The cluster's operation lease: at most one row, the operation that
-- changes the cluster over many steps now (see store.Lease). A row whose
-- expires_at passed holds nothing; the next operation replaces it.
CREATE TABLE operation_lease (
	slot       INTEGER PRIMARY KEY CHECK (slot = 1),
	id         TEXT NOT NULL,
	kind       TEXT NOT NULL,
	detail     TEXT NOT NULL,
	actor      TEXT NOT NULL,
	node       TEXT NOT NULL,
	started_at TEXT NOT NULL,
	renewed_at TEXT NOT NULL,
	-- Written with every digit of its nanoseconds (formatFixed), so that
	-- it compares as text as it does as a time.
	expires_at TEXT NOT NULL
) STRICT`

func loadLease(q querier, state *snapshot) error {
	state.lease = Lease{}
	var started, renewed, expires string
	err := q.QueryRow(`SELECT id, kind, detail, actor, node, started_at, renewed_at, expires_at FROM operation_lease WHERE slot = 1`).
		Scan(&state.lease.ID, &state.lease.Kind, &state.lease.Detail, &state.lease.Actor, &state.lease.Node, &started, &renewed, &expires)
	if errors.Is(err, sql.ErrNoRows) {
		return nil
	}
	if err != nil {
		return fmt.Errorf("read the operation lease: %w", err)
	}
	state.lease.Started, state.lease.Renewed, state.lease.Expires = parseTime(started), parseTime(renewed), parseTime(expires)
	return nil
}

// Lease is the cluster's operation lease as the state holds it, expired
// or not; zero if none was taken, or the last was released.
func (s *SQLite) Lease() Lease { return s.state.Load().lease }

// TakeLease has this node's operation of kind hold the cluster, for actor,
// until LeaseTTL from now unless renewed. It is refused while changes are
// frozen (ErrFrozen), and while another operation holds the lease
// (ErrLeased, naming it); one that expired is replaced, and the audit log
// says whose it was.
func (s *SQLite) TakeLease(kind, detail, actor string) (Lease, error) {
	if kind == "" {
		return Lease{}, errors.New("a lease needs the operation it is for")
	}
	next, err := s.writeAs(LeaseSchema, true, false, func(tx *recorder, current *snapshot, now time.Time) error {
		if current.lease.Held(now) {
			return fmt.Errorf("%w: %s", ErrLeased, current.lease)
		}
		taken := Lease{ID: rand.Text()[:16], Kind: kind, Detail: detail, Actor: actorOf(actor), Node: current.node, Started: now, Renewed: now, Expires: now.Add(LeaseTTL)}
		if err := tx.Exec(`INSERT INTO operation_lease (slot, id, kind, detail, actor, node, started_at, renewed_at, expires_at) VALUES (1, ?, ?, ?, ?, ?, ?, ?, ?)
			ON CONFLICT (slot) DO UPDATE SET id = excluded.id, kind = excluded.kind, detail = excluded.detail, actor = excluded.actor, node = excluded.node,
			started_at = excluded.started_at, renewed_at = excluded.renewed_at, expires_at = excluded.expires_at`,
			taken.ID, taken.Kind, taken.Detail, taken.Actor, taken.Node, formatTime(now), formatTime(now), formatFixed(taken.Expires)); err != nil {
			return err
		}
		if current.lease.ID != "" {
			if err := event(tx, now, actor, "operation.expired", "", "", current.lease.String()); err != nil {
				return err
			}
		}
		return event(tx, now, actor, "operation.started", "", "", taken.String())
	})
	if err != nil {
		return Lease{}, err
	}
	return next.lease, nil
}

// RenewLease holds the lease id until LeaseTTL from now. It is refused
// with ErrLeaseLost when the lease is no longer id: another operation took
// it once it expired, or a person broke it. One that expired but that no
// one took is renewed: nothing else changed the cluster meanwhile, since
// a freeze taken after it expired refuses the renewal too.
func (s *SQLite) RenewLease(id string) (Lease, error) {
	next, err := s.writeAs(LeaseSchema, false, false, func(tx *recorder, current *snapshot, now time.Time) error {
		if current.lease.ID == "" || current.lease.ID != id {
			return fmt.Errorf("%w: lease %s was broken, or expired and another took its place", ErrLeaseLost, id)
		}
		if current.frozen != "" {
			return fmt.Errorf("%w: lease %s expired, and changes were frozen since (%s)", ErrLeaseLost, id, current.frozen)
		}
		return tx.Exec(`UPDATE operation_lease SET renewed_at = ?, expires_at = ? WHERE slot = 1 AND id = ?`, formatTime(now), formatFixed(now.Add(LeaseTTL)), id)
	})
	if err != nil {
		return Lease{}, err
	}
	return next.lease, nil
}

// ReleaseLease lets go of the lease id once its operation ended, frozen or
// not; a lease that is no longer id stays as it is.
func (s *SQLite) ReleaseLease(id, actor string) error {
	_, err := s.writeAs(LeaseSchema, false, false, func(tx *recorder, current *snapshot, now time.Time) error {
		if current.lease.ID == "" || current.lease.ID != id {
			return errUnchanged
		}
		if err := tx.Exec(`DELETE FROM operation_lease WHERE slot = 1 AND id = ?`, id); err != nil {
			return err
		}
		return event(tx, now, actor, "operation.ended", "", "", current.lease.String())
	})
	if errors.Is(err, errUnchanged) {
		return nil
	}
	return err
}

// BreakLease takes the lease id away from its holder, for a person sure
// that its operation no longer runs (or must stop): its holder's next
// renewal fails, and it stops. Only id is broken, as the person saw it: a
// lease taken since is refused with ErrConflict. The audit log says whose
// it was.
func (s *SQLite) BreakLease(id, actor string) (Lease, error) {
	var broken Lease
	_, err := s.writeAs(LeaseSchema, false, false, func(tx *recorder, current *snapshot, now time.Time) error {
		switch {
		case current.lease.ID == "":
			return fmt.Errorf("%w: no operation holds the cluster", ErrNotFound)
		case current.lease.ID != id:
			return fmt.Errorf("%w: another operation holds the cluster now: %s", ErrConflict, current.lease)
		}
		broken = current.lease
		if err := tx.Exec(`DELETE FROM operation_lease WHERE slot = 1 AND id = ?`, id); err != nil {
			return err
		}
		return event(tx, now, actor, "operation.unlocked", "", "", current.lease.String())
	})
	if err != nil {
		return Lease{}, err
	}
	return broken, nil
}

// leaseRefuses refuses a freeze while an operation holds the lease: the
// freeze would stop it halfway (its moves refused), and an upgrade would
// install new releases under it.
func leaseRefuses(current *snapshot, now time.Time) error {
	if current.lease.Held(now) {
		return fmt.Errorf("%w: %s; freeze once it ends", ErrLeased, current.lease)
	}
	return nil
}

// Fence is what a write made for an operation that holds the cluster's
// lease checks as it is applied, so that an operation that lost its lease
// (broken, or expired and taken by another) changes nothing more, however
// far along a slow step of it was. A move takes its tenant from the node
// the operation planned it on, From, and only from there: one failed over
// meanwhile, or moved by another operation, is no longer the plan's. The
// zero Fence checks nothing, for writes made outside any operation.
type Fence struct {
	Lease string `json:"lease,omitempty"`
	From  string `json:"from,omitempty"`
}

// fenceKey is the context key of the fence a move or removal is made
// under.
type fenceKey struct{}

// WithFence has the move or removal made with ctx checked against fence,
// on this node or on the node it is passed on to.
func WithFence(ctx context.Context, fence Fence) context.Context {
	return context.WithValue(ctx, fenceKey{}, fence)
}

// FenceOf is the fence ctx carries; the zero Fence for none.
func FenceOf(ctx context.Context) Fence {
	fence, _ := ctx.Value(fenceKey{}).(Fence)
	return fence
}

// fenceGuard is a batch's own check that the lease it was made under is
// still the cluster's, unexpired at the time the batch was made: if not,
// it inserts a row the table's CHECK refuses, and every node refuses the
// whole batch alike. The write that adds it checks the same against the
// state it was computed from, and a batch applies only to that version of
// the state, so the guard never fires on a node that agrees; it states the
// condition in the log entry itself, where the apply sees it.
const fenceGuard = `INSERT INTO operation_lease (slot, id, kind, detail, actor, node, started_at, renewed_at, expires_at)
	SELECT 0, '', '', '', '', '', '', '', '' WHERE NOT EXISTS (SELECT 1 FROM operation_lease WHERE slot = 1 AND id = ? AND expires_at > ?)`

// fenced checks a write made under fence against current at now, and adds
// fenceGuard to it.
func fenced(tx *recorder, current *snapshot, now time.Time, fence Fence) error {
	if fence.Lease == "" {
		return nil
	}
	if current.lease.ID != fence.Lease || !current.lease.Held(now) {
		return fmt.Errorf("%w: lease %s was broken, or expired and another took its place, so this step was not made", ErrLeaseLost, fence.Lease)
	}
	return tx.Exec(fenceGuard, fence.Lease, formatFixed(now))
}

// fixedTime is RFC 3339 with every digit of the nanoseconds, so that the
// times it writes compare as text the way they compare as times, as
// fenceGuard compares expires_at.
const fixedTime = "2006-01-02T15:04:05.000000000Z07:00"

func formatFixed(t time.Time) string { return t.UTC().Format(fixedTime) }
