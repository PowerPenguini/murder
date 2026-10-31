// Package store keeps the desired state: apps, tenants (each an app plus a
// few overrides), the nodes of a cluster and an audit log of every change.
// It is kept in SQLite on every node; in a cluster, every change is a batch
// the cluster's log applies on every node.
package store

import (
	"errors"
	"io"
	"maps"
	"net/netip"
	"time"

	"github.com/PowerPenguini/murder/internal/spec"
)

// ErrNotFound reports a tenant or app that does not exist.
var ErrNotFound = errors.New("not found")

// ErrExists reports a tenant name that is already taken.
var ErrExists = errors.New("tenant already exists")

// ErrArchived reports a name held by an archived tenant.
var ErrArchived = errors.New("tenant is archived; restore it or purge it")

// ErrNotArchived reports a live tenant where an archived one is required.
var ErrNotArchived = errors.New("tenant is not archived")

// ErrConflict reports a change based on an outdated read.
var ErrConflict = errors.New("conflicting change")

// ErrFrozen refuses a change while an upgrade runs: nodes of two releases
// must not exchange changes.
var ErrFrozen = errors.New("changes are frozen")

// ErrInUse reports an app or a node that tenants still run.
var ErrInUse = errors.New("in use")

// App is a spec shared by tenants: everything but the tenant's name. Its
// image is pinned.
type App struct {
	Name      string      `json:"name"`
	Spec      spec.Tenant `json:"spec"`
	CreatedAt time.Time   `json:"createdAt"`
	UpdatedAt time.Time   `json:"updatedAt"`
}

// Record is one tenant's desired state plus what Murder assigned to it.
type Record struct {
	// Spec is what the tenant runs: its app's spec with its overrides, and
	// its suspension.
	Spec      spec.Tenant    `json:"spec"`
	App       string         `json:"app"`
	Overrides spec.Overrides `json:"overrides"`
	// Sealed holds each generated secret, sealed with the master key,
	// including secrets no longer in the spec: values are only replaced by a
	// rotation and only deleted by a purge.
	Sealed map[string]string `json:"-"`
	// Node runs the tenant and keeps its data.
	Node string `json:"node"`
	// Subnet is the tenant network's IPv4 range; the tenant's address in it
	// is fixed (see runtime.Addresses). A tenant keeps it until it is
	// purged, archived or not, so no other tenant can be routed to a
	// container still running at its address.
	Subnet string `json:"subnet"`
	// DataImage is the image whose container last owned the data. It is
	// observed state: the reconciler sets it and it does not bump Generation.
	DataImage string `json:"dataImage,omitempty"`
	// PreviousImage is the image the data had before DataImage, kept on
	// the node so a rollback needs no registry.
	PreviousImage string `json:"previousImage,omitempty"`
	// MoveTo is the node the tenant is moving to, while a move is under
	// way; MoveHeld is set while it runs nowhere and requests wait for it.
	MoveTo   string `json:"moveTo,omitempty"`
	MoveHeld bool   `json:"moveHeld,omitempty"`
	// Rollout counts the rollouts asked for the tenant; each gives it a new
	// container made as this release of Murder makes them.
	Rollout int64 `json:"rollout,omitempty"`
	// Generation counts changes of the desired state.
	Generation int64     `json:"generation"`
	CreatedAt  time.Time `json:"createdAt"`
	UpdatedAt  time.Time `json:"updatedAt"`
	// ArchivedAt is set while the tenant is archived: out of service, with
	// its data, secrets and overrides kept so it can be restored. Its
	// secrets in particular must survive, because the app's data may be
	// encrypted or signed with them. A restore runs the app's current spec.
	ArchivedAt time.Time `json:"archivedAt,omitzero"`
}

// Archived reports whether the tenant is archived.
func (r Record) Archived() bool { return !r.ArchivedAt.IsZero() }

func (r Record) clone() Record {
	r.Spec.Env = maps.Clone(r.Spec.Env)
	r.Spec.Secrets = maps.Clone(r.Spec.Secrets)
	r.Spec.Tmpfs = maps.Clone(r.Spec.Tmpfs)
	r.Overrides.Env = maps.Clone(r.Overrides.Env)
	r.Overrides.Secrets = maps.Clone(r.Overrides.Secrets)
	r.Sealed = maps.Clone(r.Sealed)
	return r
}

// NewTenant is a tenant to create.
type NewTenant struct {
	Name      string
	App       string
	Overrides spec.Overrides
	// Node runs the tenant; this node if empty.
	Node string
}

// AppChange is the outcome of storing an app.
type AppChange struct {
	App     App
	Created bool
	// Changed is false when the app was stored as it already was.
	Changed bool
	// Tenants are the live tenants whose spec changed with the app.
	Tenants []string
}

// Event is one entry of the audit log.
type Event struct {
	ID     int64     `json:"id"`
	At     time.Time `json:"at"`
	Actor  string    `json:"actor"`
	Action string    `json:"action"`
	Tenant string    `json:"tenant,omitempty"`
	App    string    `json:"app,omitempty"`
	Detail string    `json:"detail,omitempty"`
}

// EventQuery selects audit events, newest first.
type EventQuery struct {
	Tenant string
	App    string
	// Before returns only events older than this ID; zero means the newest.
	Before int64
	// Limit caps the result; zero means 100.
	Limit int
}

// Node is one node of a cluster. Every node runs tenants.
type Node struct {
	Name string `json:"name"`
	// Address is where other nodes reach it: host:port on the private
	// network.
	Address   string    `json:"address"`
	CreatedAt time.Time `json:"createdAt"`
}

// Sealer makes a new value for one secret of a tenant and seals it.
type Sealer func(tenant, name string, secret spec.Secret) (string, error)

// Store is the desired state of a node. Reads come from memory and never
// fail; every write is one transaction that also records who made it.
type Store interface {
	Node() string

	Apps() []App
	App(name string) (App, error)
	// CheckApp checks an app whose image may still be a tag against the
	// tenants it would change, before the image is pulled.
	CheckApp(app App) error
	// PutApp creates or replaces an app. Live tenants it would make invalid
	// block it; live ones whose spec changes get a new generation. Archived
	// tenants are checked when restored.
	PutApp(app App, actor string) (AppChange, error)
	// DeleteApp removes an app no tenant runs.
	DeleteApp(name, actor string) error

	// List returns every tenant, archived ones too, by name.
	List() []Record
	// Get returns a tenant, archived or not.
	Get(name string) (Record, error)
	// Live returns a tenant that is not archived; an archived one is
	// refused with ErrArchived. Anything that acts on a running tenant uses
	// it.
	Live(name string) (Record, error)
	// Create adds a tenant, assigning its subnet. A name held
	// by an archived tenant is refused with ErrArchived.
	Create(tenant NewTenant, actor string) (Record, error)
	// Configure gives a tenant, live or archived, another app or other
	// overrides. With a non-zero generation it refuses, with ErrConflict, a
	// tenant that has changed since. It reports false, and changes nothing,
	// when app and overrides are what is stored.
	Configure(name, app string, overrides spec.Overrides, generation int64, actor string) (Record, bool, error)
	// SetSuspended changes only suspension. It does not validate the spec,
	// so a tenant stored under older rules can still be suspended.
	SetSuspended(name string, suspended bool, actor string) (Record, error)
	// Rotate gives one secret of the tenant's spec a new value.
	Rotate(name, secret, actor string) (Record, error)
	// Roll has a live tenant's container replaced as it is: what a new
	// release of Murder changed in containers then applies to it.
	Roll(name, actor string) (Record, error)
	// StartMove marks a tenant of this node, live or archived, as moving to
	// another node; HoldMove has requests wait while it runs nowhere;
	// FinishMove gives it to the target, requests still held until it
	// serves there (ReleaseHold); CancelMove leaves it here, and reports
	// whether there was a move to cancel. Cancelling and releasing work
	// while changes are frozen: a frozen state must not strand a tenant.
	StartMove(name, target, actor string) (Record, error)
	HoldMove(name string) error
	FinishMove(name, actor string) (Record, error)
	ReleaseHold(name string) error
	CancelMove(name, actor string) (bool, error)
	// Observe records the image that now owns a tenant's data; the one
	// before becomes PreviousImage.
	Observe(name, dataImage string) error
	// Images lists every image a tenant was given or ran, still needed or
	// not; ForgetImage drops one that is gone from the node.
	Images() []string
	ForgetImage(ref string) error

	// Archive takes a live tenant out of service in one write; the
	// reconciler then removes its container and network. Restore brings it
	// back; Forget deletes an archived tenant for good.
	Archive(name, actor string) error
	Restore(name, actor string) (Record, error)
	Forget(name, actor string) error

	// Nodes lists the nodes of the cluster by name; a node outside a
	// cluster has none. PutNode adds a node or changes its address;
	// RemoveNode removes one that runs no tenant, archived or not.
	Nodes() []Node
	// Cluster names the cluster the state belongs to; empty outside one.
	Cluster() string
	PutNode(node Node, actor string) error
	RemoveNode(name, actor string) error

	// Frozen says why changes are refused, while an upgrade runs; empty
	// otherwise. Freeze refuses every change but Thaw from then on.
	Frozen() string
	Freeze(reason, actor string) error
	Thaw(actor string) error
	// Note records an event, frozen or not.
	Note(action, detail, actor string) error

	Events(query EventQuery) ([]Event, error)
	// Backup writes a copy of the whole database, as it is at one moment.
	Backup(w io.Writer) error
	// Changed is signalled after every successful change of desired state.
	Changed() <-chan struct{}
	// Watch returns a channel closed at the next change of any state; any
	// number of goroutines may watch.
	Watch() <-chan struct{}
	Close() error
}

// Options sets what a store assigns to new tenants and how it makes secrets.
type Options struct {
	// NetworkPool is split into one /28 per tenant network.
	NetworkPool netip.Prefix
	Sealer      Sealer
}
