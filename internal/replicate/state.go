package replicate

import "github.com/PowerPenguini/murder/internal/store"

// State is what the replicator reads and records of the desired state:
// *store.SQLite in the agent.
type State interface {
	// Node names this node; Nodes lists the cluster's.
	Node() string
	Nodes() []store.Node
	// List returns every tenant, archived ones too; Get one of them.
	List() []store.Record
	Get(name string) (store.Record, error)
	// Frozen says why changes are refused, while an upgrade runs.
	Frozen() string

	// PlaceReplicas gives every tenant without a copy on another node one,
	// and evens out the copies the nodes keep (ReplicaNext); it reports how
	// many it changed.
	PlaceReplicas(actor string) (int, error)
	// ReplicaReady records that the copy on replica holds the whole data.
	ReplicaReady(name, replica, owner string, generation int64) error
	// SwitchReplica makes next, the tenant's ReplicaNext, its replica once
	// next holds the whole data; it reports whether it switched.
	SwitchReplica(name, next string) (bool, error)
	// GiveUpCopy gives up the tenant's copy on node, for why; it reports
	// whether it changed anything.
	GiveUpCopy(name, node, why string) (bool, error)
}

// Placements is what a node receiving copies reads of the desired state:
// where a tenant runs, which node keeps its copy or is to, and whether it
// moves here, so a copy is taken only from the tenant's node and only by
// the node meant to keep it.
type Placements interface {
	Node() string
	Get(name string) (store.Record, error)
}

// The agent passes its store.
var (
	_ State      = (*store.SQLite)(nil)
	_ Placements = (*store.SQLite)(nil)
)
