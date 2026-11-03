// Package api is the agent's local HTTP API as its clients see it: the
// requests and answers on the wire and a client for them. The CLI and
// the panel use it; internal/apiserver serves it.
package api

import (
	"time"

	"github.com/PowerPenguini/murder/internal/reconcile"
	"github.com/PowerPenguini/murder/internal/spec"
	"github.com/PowerPenguini/murder/internal/store"
)

// Tenant is what the API shows about a tenant. Secret values never appear.
type Tenant struct {
	// Spec is what the tenant runs: its app's spec with its overrides.
	Spec      spec.Tenant    `json:"spec"`
	App       string         `json:"app"`
	Overrides spec.Overrides `json:"overrides"`
	// Node runs the tenant. Address is reachable from there only; Status
	// is asked of it when another node answers, and left out if it does
	// not answer in time.
	Node string `json:"node"`
	// Elsewhere is set when another node than this one runs the tenant.
	Elsewhere bool `json:"elsewhere,omitempty"`
	// Replica is the node that keeps a copy of the tenant's data;
	// ReplicaReady says the copy holds the whole data, so the tenant can
	// fail over to it; ReplicaSyncedAt is when the copy last matched the
	// data, as the node that runs the tenant knows it (asked of that node
	// only).
	Replica      string `json:"replica,omitempty"`
	ReplicaReady bool   `json:"replicaReady,omitempty"`
	// ReplicaNext is the node the copy moves to, to even out the copies
	// the nodes keep, until its copy there is whole.
	ReplicaNext     string    `json:"replicaNext,omitempty"`
	ReplicaSyncedAt time.Time `json:"replicaSyncedAt,omitzero"`
	Host            string    `json:"host"`
	URL             string    `json:"url"`
	// Address is where the tenant listens, reachable from this node only.
	Address    string `json:"address,omitempty"`
	Generation int64  `json:"generation"`
	// AppRevision is the app's revision the tenant runs: 0 for the newest.
	// BeforeRevision is the one it ran before its app's rollout moved it.
	AppRevision    int64 `json:"appRevision,omitempty"`
	BeforeRevision int64 `json:"beforeRevision,omitempty"`
	// Canary tenants take an app's rollout first; held ones are left out.
	Canary    bool      `json:"canary,omitempty"`
	Hold      bool      `json:"hold,omitempty"`
	CreatedAt time.Time `json:"createdAt"`
	UpdatedAt time.Time `json:"updatedAt"`
	// ArchivedAt is set for an archived tenant, which runs nothing until it
	// is restored.
	ArchivedAt time.Time         `json:"archivedAt,omitzero"`
	Status     *reconcile.Status `json:"status,omitempty"`
}

// Archived reports whether the tenant is archived.
func (t Tenant) Archived() bool { return !t.ArchivedAt.IsZero() }

// TenantRequest creates a tenant (with Name) or reconfigures one.
type TenantRequest struct {
	Name      string         `json:"name,omitempty"`
	App       string         `json:"app"`
	Overrides spec.Overrides `json:"overrides"`
	// Node, when creating, runs the tenant: a node's name, "auto" for the
	// one running the fewest tenants, or this node if empty.
	Node string `json:"node,omitempty"`
	// Generation, when reconfiguring, is the generation the client read; a
	// tenant changed since is refused with 409 instead of overwritten.
	Generation int64 `json:"generation,omitempty"`
}

// Applied is the result of creating or reconfiguring a tenant.
type Applied struct {
	Tenant Tenant `json:"tenant"`
	// Changed is false when the request matched what was stored.
	Changed bool `json:"changed"`
	// Note explains how an image was pinned when it fell back to a local
	// image.
	Note string `json:"note,omitempty"`
}

// App is what the API shows about an app.
type App struct {
	Name string      `json:"name"`
	Spec spec.Tenant `json:"spec"`
	// Tag is the tag the image was given as and pinned from; empty when it
	// was given pinned.
	Tag string `json:"tag,omitempty"`
	// Revision numbers the app's spec; 0 for one stored before revisions.
	Revision int64 `json:"revision,omitempty"`
	// Latest is the tag's newest image in its registry, while the app runs
	// another one: an update. LatestAt is when it was found.
	Latest   string    `json:"latest,omitempty"`
	LatestAt time.Time `json:"latestAt,omitzero"`
	// Rollout is the app's newest rollout, if it has had one.
	Rollout *store.Rollout `json:"rollout,omitempty"`
	// Tenants are the live tenants running the app.
	Tenants   []string  `json:"tenants"`
	CreatedAt time.Time `json:"createdAt"`
	UpdatedAt time.Time `json:"updatedAt"`
}

// AppApplied is the result of storing an app.
type AppApplied struct {
	App     App  `json:"app"`
	Created bool `json:"created"`
	Changed bool `json:"changed"`
	// Rollout is set when the tenants move in waves.
	Rollout bool `json:"rollout,omitempty"`
	// Tenants are the live tenants the change rolls out to.
	Tenants []Tenant `json:"tenants"`
	Note    string   `json:"note,omitempty"`
}

// AppCheck is what an app's registry holds under its tag now.
type AppCheck struct {
	App App `json:"app"`
	// Image is the tag's image in the registry, pinned.
	Image string `json:"image"`
	// Newer is set when it is not the image the app runs.
	Newer bool `json:"newer"`
}

// RolloutRestore names the tenants of an app's rollout to take back with
// their data; none takes back every tenant it moved.
type RolloutRestore struct {
	Tenants []string `json:"tenants,omitempty"`
}

// RolloutRestored is an app after some of its rollout's tenants were taken
// back with their data, and those tenants.
type RolloutRestored struct {
	App     App      `json:"app"`
	Tenants []string `json:"tenants"`
}

// TenantFlags makes a tenant a canary or held, or not; nil leaves one as
// it is.
type TenantFlags struct {
	Canary *bool `json:"canary,omitempty"`
	Hold   *bool `json:"hold,omitempty"`
}

// SecretValue carries one secret to an operator.
type SecretValue struct {
	Value string `json:"value"`
}

// ErrorBody is how the agent answers a request it refused.
type ErrorBody struct {
	Error string `json:"error"`
}

// ActorHeader lets a trusted client, such as the panel, name the person it
// acts for. It is recorded next to the socket user, never instead of it:
// anyone who can use the socket controls every tenant anyway.
const ActorHeader = "X-Murder-Actor"
