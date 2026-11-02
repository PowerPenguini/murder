package failover

import "github.com/PowerPenguini/murder/internal/store"

// State is what failover reads and records of the desired state:
// *store.SQLite in the agent.
type State interface {
	// Node names this node; Nodes lists the cluster's.
	Node() string
	Nodes() []store.Node
	// List returns every tenant, archived ones too; Get one of them.
	List() []store.Record
	Get(name string) (store.Record, error)
	// Frozen says why changes are refused, while an upgrade runs: no node
	// fails over meanwhile.
	Frozen() string

	// FailOver gives every tenant of node, a node confirmed powered off,
	// to its replica's node, and marks node failed until NodeBack, once it
	// answers again. MarkFailed marks node failed without moving its
	// tenants, so the copies it kept go elsewhere; it reports whether it
	// changed anything.
	FailOver(node, actor string) (store.FailedOver, error)
	NodeBack(node, actor string) error
	MarkFailed(node, actor string) (bool, error)
}
