package move

import "github.com/PowerPenguini/murder/internal/store"

// State is what a move reads and records of the desired state:
// *store.SQLite in the agent.
type State interface {
	// Node names this node, which moves its own tenants away.
	Node() string
	// List returns every tenant, archived ones too; Get one of them.
	List() []store.Record
	Get(name string) (store.Record, error)

	// StartMove marks a tenant of this node, live or archived, as moving
	// to another node;
	// HoldMove has requests wait while it runs nowhere; FinishMove gives
	// it to the target, requests still held until it serves there
	// (ReleaseHold); CancelMove leaves it here, and reports whether there
	// was a move to cancel. Cancelling and releasing work while changes
	// are frozen: a frozen state must not strand a tenant.
	// Both take the fence of the operation the move is made for, if any
	// (see store.Fence).
	StartMoveFenced(name, target, actor string, fence store.Fence) (store.Record, error)
	HoldMove(name string) error
	FinishMoveFenced(name, actor string, fence store.Fence) (store.Record, error)
	ReleaseHold(name string) error
	CancelMove(name, actor string) (bool, error)
}

// The agent passes its store.
var (
	_ State = (*store.SQLite)(nil)
)
