// Package store keeps the desired state of a node: its apps, its tenants
// (each an app plus a few overrides) and an audit log of every change. It is
// kept in SQLite; stage 3 replicates the same changes between nodes.
package store

import (
	"errors"
	"maps"
	"net/netip"
	"time"

	"github.com/PowerPenguini/corral/internal/spec"
)

// ErrNotFound reports a tenant or app that does not exist.
var ErrNotFound = errors.New("not found")

// ErrExists reports a tenant name that is already taken.
var ErrExists = errors.New("tenant already exists")

// ErrArchived reports a name held by an archived tenant.
var ErrArchived = errors.New("tenant is archived; restore it or purge it")

// ErrNotArchived reports a live tenant where an archived one is required.
var ErrNotArchived = errors.New("tenant is not archived")

// ErrInUse reports an app that tenants still run.
var ErrInUse = errors.New("app is in use")

// App is a spec shared by tenants: everything but the tenant's name. Its
// image is pinned.
type App struct {
	Name      string      `json:"name"`
	Spec      spec.Tenant `json:"spec"`
	CreatedAt time.Time   `json:"createdAt"`
	UpdatedAt time.Time   `json:"updatedAt"`
}

// Record is one tenant's desired state plus what Corral assigned to it.
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
	// HostPort is the loopback port the edge proxy reaches the tenant on.
	HostPort int `json:"hostPort"`
	// Subnet is the tenant network's IPv4 range.
	Subnet string `json:"subnet"`
	// DataImage is the image whose container last owned the data. It is
	// observed state: the reconciler sets it and it does not bump Generation.
	DataImage string `json:"dataImage,omitempty"`
	// SuspendedBeforeArchive is set while an archive is under way: the
	// suspension to restore the tenant with, kept across a failed attempt.
	SuspendedBeforeArchive *bool `json:"suspendedBeforeArchive,omitempty"`
	// Generation counts changes of the desired state.
	Generation int64     `json:"generation"`
	CreatedAt  time.Time `json:"createdAt"`
	UpdatedAt  time.Time `json:"updatedAt"`
}

func (r Record) clone() Record {
	if r.SuspendedBeforeArchive != nil {
		value := *r.SuspendedBeforeArchive
		r.SuspendedBeforeArchive = &value
	}
	r.Spec.Env = maps.Clone(r.Spec.Env)
	r.Spec.Secrets = maps.Clone(r.Spec.Secrets)
	r.Spec.Tmpfs = maps.Clone(r.Spec.Tmpfs)
	r.Overrides.Env = maps.Clone(r.Overrides.Env)
	r.Overrides.Secrets = maps.Clone(r.Overrides.Secrets)
	r.Sealed = maps.Clone(r.Sealed)
	return r
}

// Archived is an archived tenant: its record, kept with its data so it can
// be restored. Its secrets in particular must survive, because the app's
// data may be encrypted or signed with them. It holds no host port or
// subnet; a restore assigns new ones and runs the app's current spec.
type Archived struct {
	Record     Record    `json:"record"`
	ArchivedAt time.Time `json:"archivedAt"`
}

// NewTenant is a tenant to create.
type NewTenant struct {
	Name      string
	App       string
	Overrides spec.Overrides
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

// Sealer makes a new value for one secret of a tenant and seals it.
type Sealer func(tenant, name string, secret spec.Secret) (string, error)

// Store is the desired state of a node. Reads come from memory and never
// fail; every write is one transaction that also records who made it.
type Store interface {
	Node() string

	Apps() []App
	App(name string) (App, error)
	// PutApp creates or replaces an app. Every tenant of it, live or
	// archived, must stay valid; live ones whose spec changes get a new
	// generation.
	PutApp(app App, actor string) (AppChange, error)
	// DeleteApp removes an app no tenant runs.
	DeleteApp(name, actor string) error

	List() []Record
	Get(name string) (Record, error)
	// Create adds a tenant, assigning its host port and subnet. A name held
	// by an archived tenant is refused with ErrArchived.
	Create(tenant NewTenant, actor string) (Record, error)
	// Configure gives a tenant another app or other overrides. It reports
	// false, and changes nothing, when they are what is stored.
	Configure(name, app string, overrides spec.Overrides, actor string) (Record, bool, error)
	// SetSuspended changes only suspension. It does not validate the spec,
	// so a tenant stored under older rules can still be suspended.
	SetSuspended(name string, suspended bool, actor string) (Record, error)
	// Rotate gives one secret of the tenant's spec a new value.
	Rotate(name, secret, actor string) (Record, error)
	// Observe records the image that now owns a tenant's data.
	Observe(name, dataImage string) error

	// StartArchive suspends a tenant for archiving, remembering (once, even
	// across repeated attempts) whether it was suspended before. Archive
	// then moves it to the archive with that suspension; Restore brings it
	// back with a fresh host port and subnet; Forget deletes it for good.
	StartArchive(name, actor string) (Record, error)
	Archive(name, actor string) error
	Restore(name, actor string) (Record, error)
	Forget(name, actor string) error
	ArchivedTenant(name string) (Archived, bool)
	ListArchived() []Archived

	Events(query EventQuery) ([]Event, error)
	// Changed is signalled after every successful change of desired state.
	Changed() <-chan struct{}
	Close() error
}

// Options sets what a store assigns to new tenants and how it makes secrets.
type Options struct {
	// PortFrom and PortTo bound the loopback ports tenants are published on.
	PortFrom, PortTo int
	// NetworkPool is split into one /28 per tenant network.
	NetworkPool netip.Prefix
	Sealer      Sealer
}
