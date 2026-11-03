package reconcile

import "github.com/PowerPenguini/murder/internal/store"

// State is what the reconciler reads and records of the desired state:
// *store.SQLite in the agent.
type State interface {
	// Node names this node; the reconciler runs only its tenants.
	Node() string
	// List returns every tenant, archived ones too; Get one of them.
	List() []store.Record
	Get(name string) (store.Record, error)
	// Changed is signalled after every change of the desired state.
	Changed() <-chan struct{}
	// Frozen says why changes are refused, while an upgrade runs.
	Frozen() string

	// Observe records the image that now owns a tenant's data; the one
	// before becomes its PreviousImage.
	Observe(name, dataImage string) error
	// Images lists every image a tenant was given or ran; ForgetImage
	// drops one that is gone from the node.
	Images() []string
	ForgetImage(ref string) error
	// ReleaseHold lets requests for a tenant that moved here through, once
	// it serves.
	ReleaseHold(name string) error
	// Forget deletes an archived tenant for good, once its runtime and data
	// are gone.
	Forget(name, actor string) error
	// Restored records that a tenant's data went back to image, as its
	// Restore asked.
	Restored(name, image string) error
	// Served records that a tenant a rollout moved passed a health
	// check on its new revision, at generation.
	Served(name string, generation int64) error
}

// The agent passes its store.
var (
	_ State = (*store.SQLite)(nil)
)
