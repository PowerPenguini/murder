// Package access is the register of staff access to tenants' data: who
// read a tenant's secrets, logs or backups, or copied the state, when,
// why, and at whose request. Each access is an event of the audit log,
// replicated and kept like every other, of action "access.KIND" and a
// detail that holds the rest as JSON.
//
// Reading a tenant's data through Murder records the access first: the
// agent refuses the read if it cannot record it. A read either gives its
// purpose and ticket, and the agent records it there and then, or names a
// grant: an access recorded before, by the same person, for the same
// thing, not long ago.
package access

import (
	"encoding/json"
	"errors"
	"fmt"
	"net/url"
	"strconv"
	"strings"
	"time"
	"unicode"
	"unicode/utf8"

	"github.com/PowerPenguini/murder/internal/store"
)

// Kind is what an access reads.
type Kind string

const (
	// Secret is one of a tenant's secret values; What names it.
	Secret Kind = "secret"
	// Logs are the lines of a tenant's container, current or previous.
	Logs Kind = "logs"
	// BackupVerify reads a tenant's newest backup through, decrypted.
	BackupVerify Kind = "backup-verify"
	// BackupRestore puts a tenant's data back from a backup.
	BackupRestore Kind = "backup-restore"
	// BackupForget deletes every backup of a purged tenant.
	BackupForget Kind = "backup-forget"
	// State is a copy of the whole state, every tenant's sealed secrets
	// in it: of no one tenant, so of all.
	State Kind = "state"
	// Manual is an access outside Murder, such as a shell in a tenant's
	// container, recorded by hand; What says what it was.
	Manual Kind = "manual"
)

// Kinds are every kind, in the order the docs give them.
var Kinds = []Kind{Secret, Logs, BackupVerify, BackupRestore, BackupForget, State, Manual}

// ActionPrefix starts the action of every access event.
const ActionPrefix = "access."

// Action is the audit log action of an access of kind k.
func (k Kind) Action() string { return ActionPrefix + string(k) }

func (k Kind) known() bool {
	for _, kind := range Kinds {
		if k == kind {
			return true
		}
	}
	return false
}

// Limits on what a request says, in characters.
const (
	maxPurpose = 500
	maxTicket  = 200
	maxWhat    = 300
)

// Request is an access to record.
type Request struct {
	// Tenant is whose data it reads; empty for State, which is everyone's.
	Tenant string `json:"tenant,omitempty"`
	Kind   Kind   `json:"kind"`
	// What names what of it: a secret's name, or what a manual access did.
	What string `json:"what,omitempty"`
	// Purpose says why; Ticket is the customer's request it answers, or
	// "incident" or "legal" (each perhaps followed by ":" and a
	// reference) when no customer asked.
	Purpose string `json:"purpose"`
	Ticket  string `json:"ticket"`
	// AccessedAt is when the access was, if before it is recorded: a
	// manual access recorded after, or one recorded offline first.
	AccessedAt time.Time `json:"accessedAt,omitzero"`
	// Offline is the ID of the offline record this one was first written
	// as (see OfflineRecord).
	Offline string `json:"offline,omitempty"`
}

// Normalize trims what a request says and checks it.
func (r *Request) Normalize() error {
	r.Tenant = strings.TrimSpace(r.Tenant)
	r.What = strings.TrimSpace(r.What)
	r.Purpose = strings.TrimSpace(r.Purpose)
	r.Ticket = strings.TrimSpace(r.Ticket)
	r.Offline = strings.TrimSpace(r.Offline)
	if lower := strings.ToLower(r.Ticket); lower == "incident" || lower == "legal" {
		r.Ticket = lower
	}
	if !r.AccessedAt.IsZero() {
		r.AccessedAt = r.AccessedAt.UTC()
	}
	return r.Check()
}

// Check says whether a request can be recorded.
func (r Request) Check() error {
	if !r.Kind.known() {
		return fmt.Errorf("unknown kind of access %q", r.Kind)
	}
	switch {
	case r.Kind == State && r.Tenant != "":
		return errors.New("a copy of the state is every tenant's: it names no tenant")
	case r.Kind != State && r.Tenant == "":
		return fmt.Errorf("an access of kind %s names its tenant", r.Kind)
	case (r.Kind == Secret || r.Kind == Manual) && r.What == "":
		if r.Kind == Secret {
			return errors.New("a secret's access names the secret")
		}
		return errors.New("a manual access says what was done (-what)")
	}
	if err := text("tenant", r.Tenant, 64); err != nil {
		return err
	}
	if err := text("what", r.What, maxWhat); err != nil {
		return err
	}
	if r.Purpose == "" {
		return errors.New("an access to a tenant's data says why (-reason): reads are recorded in the access register")
	}
	if err := text("purpose", r.Purpose, maxPurpose); err != nil {
		return err
	}
	if r.Ticket == "" {
		return errors.New(`an access to a tenant's data names the request it answers (-ticket): the customer's ticket, or "incident" or "legal" when no customer asked`)
	}
	if err := text("ticket", r.Ticket, maxTicket); err != nil {
		return err
	}
	if err := text("offline record", r.Offline, 64); err != nil {
		return err
	}
	return nil
}

// text refuses a field longer than max characters, or one with control
// characters: an entry is one line wherever it is shown.
func text(name, value string, max int) error {
	if !utf8.ValidString(value) {
		return fmt.Errorf("%s is not valid UTF-8", name)
	}
	if utf8.RuneCountInString(value) > max {
		return fmt.Errorf("%s is longer than %d characters", name, max)
	}
	if strings.IndexFunc(value, func(r rune) bool { return unicode.IsControl(r) || r == ' ' || r == ' ' }) >= 0 {
		return fmt.Errorf("%s holds a control character", name)
	}
	return nil
}

// detail is what an access event's detail holds.
type detail struct {
	What       string    `json:"what,omitempty"`
	Purpose    string    `json:"purpose"`
	Ticket     string    `json:"ticket"`
	AccessedAt time.Time `json:"accessedAt,omitzero"`
	Offline    string    `json:"offline,omitempty"`
}

// Detail is the audit log detail of the request.
func (r Request) Detail() string {
	data, err := json.Marshal(detail{What: r.What, Purpose: r.Purpose, Ticket: r.Ticket, AccessedAt: r.AccessedAt, Offline: r.Offline})
	if err != nil {
		// Strings and a time always marshal.
		panic(err)
	}
	return string(data)
}

// Entry is one access of the register.
type Entry struct {
	// ID is its event's; At when it was recorded.
	ID    int64     `json:"id"`
	At    time.Time `json:"at"`
	Actor string    `json:"actor"`
	// Tenant is empty for an access to every tenant (State).
	Tenant string `json:"tenant,omitempty"`
	Kind   Kind   `json:"kind"`
	What   string `json:"what,omitempty"`
	// Purpose and Ticket are as the request gave them.
	Purpose string `json:"purpose"`
	Ticket  string `json:"ticket"`
	// AccessedAt is when the access was, when not when it was recorded.
	AccessedAt time.Time `json:"accessedAt,omitzero"`
	// Offline is the offline record it was first written as, if any.
	Offline string `json:"offline,omitempty"`
}

// When is when the access was.
func (e Entry) When() time.Time {
	if !e.AccessedAt.IsZero() {
		return e.AccessedAt
	}
	return e.At
}

// FromEvent reads an access event; ok is false for any other event.
func FromEvent(event store.Event) (Entry, bool) {
	kind, found := strings.CutPrefix(event.Action, ActionPrefix)
	if !found {
		return Entry{}, false
	}
	var d detail
	if err := json.Unmarshal([]byte(event.Detail), &d); err != nil {
		// Never one Murder wrote; shown as it is, not lost.
		d = detail{What: event.Detail}
	}
	return Entry{ID: event.ID, At: event.At, Actor: event.Actor, Tenant: event.Tenant, Kind: Kind(kind),
		What: d.What, Purpose: d.Purpose, Ticket: d.Ticket, AccessedAt: d.AccessedAt, Offline: d.Offline}, true
}

// Said is an access as one line says it, for the audit log's readers:
// "logs, for "a customer's export fails" (ticket ZD-1234)".
func (e Entry) Said() string {
	what := string(e.Kind)
	if e.What != "" {
		what += " " + e.What
	}
	said := fmt.Sprintf("%s, for %q (ticket %s)", what, e.Purpose, e.Ticket)
	if !e.AccessedAt.IsZero() {
		said += ", accessed at " + e.AccessedAt.Format(time.RFC3339)
	}
	if e.Offline != "" {
		said += ", recorded offline first"
	}
	return said
}

// GrantFor is how long a recorded access serves the same person's reads of
// the same thing; a restore's, how long it covers putting the data back
// once its backup is read, which may take hours.
const (
	GrantFor        = time.Hour
	RestoreGrantFor = 24 * time.Hour
)

// ErrNoGrant is a read the register does not cover.
var ErrNoGrant = errors.New("not recorded in the access register")

// Grants says whether entry covers a read of kind, of tenant's what, by
// actor at now. Entries recorded after the access (manual, offline) cover
// none.
func (e Entry) Grants(kind Kind, tenant, what, actor string, now time.Time) error {
	validity := GrantFor
	if kind == BackupRestore {
		validity = RestoreGrantFor
	}
	switch {
	case e.Kind != kind || e.Tenant != tenant || e.What != what:
		return fmt.Errorf("%w: access %d is not one of %s", ErrNoGrant, e.ID, Request{Tenant: tenant, Kind: kind, What: what}.target())
	case e.Actor != actor:
		return fmt.Errorf("%w: access %d was recorded by %s, not %s", ErrNoGrant, e.ID, e.Actor, actor)
	case !e.AccessedAt.IsZero() || e.Offline != "":
		return fmt.Errorf("%w: access %d was recorded after the fact", ErrNoGrant, e.ID)
	case now.Sub(e.At) > validity || e.At.Sub(now) > time.Minute:
		return fmt.Errorf("%w: access %d was recorded at %s and serves %s: say why again", ErrNoGrant, e.ID, e.At.UTC().Format(time.RFC3339), validity)
	}
	return nil
}

// target says what a request reads: "tenant acme's logs".
func (r Request) target() string {
	if r.Tenant == "" {
		return "the state"
	}
	said := "tenant " + r.Tenant + "'s " + string(r.Kind)
	if r.What != "" {
		said += " " + r.What
	}
	return said
}

// Proof is how a read shows it is recorded: a purpose and a ticket the
// agent records it with, or the ID of a grant recorded before.
type Proof struct {
	Purpose string
	Ticket  string
	Grant   int64
}

// Query parameters of a proof.
const (
	purposeParam = "purpose"
	ticketParam  = "ticket"
	grantParam   = "access"
)

// AddTo adds the proof to a request's query.
func (p Proof) AddTo(values url.Values) {
	if p.Grant > 0 {
		values.Set(grantParam, strconv.FormatInt(p.Grant, 10))
		return
	}
	if p.Purpose != "" {
		values.Set(purposeParam, p.Purpose)
	}
	if p.Ticket != "" {
		values.Set(ticketParam, p.Ticket)
	}
}

// Given says whether the proof gives anything at all.
func (p Proof) Given() bool { return p.Grant > 0 || p.Purpose != "" || p.Ticket != "" }

// ProofFrom reads a proof from a request's query.
func ProofFrom(values url.Values) (Proof, error) {
	proof := Proof{Purpose: values.Get(purposeParam), Ticket: values.Get(ticketParam)}
	if grant := values.Get(grantParam); grant != "" {
		id, err := strconv.ParseInt(grant, 10, 64)
		if err != nil || id < 1 {
			return Proof{}, errors.New("access must be the ID of an access recorded before")
		}
		if proof.Purpose != "" || proof.Ticket != "" {
			return Proof{}, errors.New("give either an access recorded before, or a purpose and a ticket, not both")
		}
		proof.Grant = id
	}
	return proof, nil
}
