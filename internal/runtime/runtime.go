// Package runtime runs tenant containers. Runtime is the narrow seam the
// reconciler depends on; Docker implements it through the Engine API.
package runtime

import (
	"context"
	"errors"
	"time"
)

// ErrNotFound reports a missing container, network or image.
var ErrNotFound = errors.New("not found")

// ErrNotRunning reports a container that has no running process.
var ErrNotRunning = errors.New("container is not running")

// ErrNotOwned reports an object with the expected name that this node does
// not own; Corral leaves it alone.
var ErrNotOwned = errors.New("not owned by this node")

// Labels Corral puts on everything it creates. LabelNode marks which node's
// agent owns an object; an agent never touches another node's objects.
const (
	LabelManaged = "io.corral.managed"
	LabelNode    = "io.corral.node"
	LabelTenant  = "io.corral.tenant"
	LabelHash    = "io.corral.spec-hash"
	LabelImage   = "io.corral.image"
)

// Runtime is what the reconciler needs from a container engine.
type Runtime interface {
	// HasImage reports whether ref is present locally; PullImage fetches it.
	HasImage(ctx context.Context, ref string) (bool, error)
	PullImage(ctx context.Context, ref string) error
	// Resolve pins an image reference: a digest reference or image ID comes
	// back unchanged; a tag is pulled and replaced by its registry digest,
	// or by the local image ID if it has none or cannot be pulled. note
	// explains a fallback to the local image.
	Resolve(ctx context.Context, ref string) (pinned, note string, err error)
	// EnsureNetwork creates the isolated network with the given IPv4 subnet
	// if it is missing. An existing network with another subnet, or one this
	// node does not own, is an error. RemoveNetwork leaves alone a network
	// this node does not own and reports it with ErrNotOwned.
	EnsureNetwork(ctx context.Context, name, subnet string, labels map[string]string) error
	RemoveNetwork(ctx context.Context, name string) error
	// ListNetworks and List return this node's managed networks and
	// containers.
	ListNetworks(ctx context.Context) ([]Network, error)
	List(ctx context.Context) ([]Container, error)
	Create(ctx context.Context, spec ContainerSpec) (string, error)
	Start(ctx context.Context, id string) error
	Stop(ctx context.Context, id string, timeout time.Duration) error
	Remove(ctx context.Context, id string) error
	// Logs returns the last lines of a container's output.
	Logs(ctx context.Context, id string, tail int) ([]byte, error)
	// Pid returns the host PID of a running container's main process.
	Pid(ctx context.Context, id string) (int, error)
}

// Container is the observed state of one managed container.
type Container struct {
	ID      string
	Name    string
	Running bool
	State   string
	Labels  map[string]string
}

// Network is the observed state of one managed network.
type Network struct {
	Name   string
	Labels map[string]string
}

// Mount binds a host directory into the container.
type Mount struct {
	Source, Target string
}

// ContainerSpec is everything needed to create one tenant container.
type ContainerSpec struct {
	Name          string
	Image         string
	Env           []string
	User          string
	Labels        map[string]string
	Network       string
	ContainerPort int
	// HostPort is published on 127.0.0.1 only.
	HostPort     int
	Mounts       []Mount
	MemoryBytes  int64
	NanoCPUs     int64
	PIDs         int64
	ReadOnlyRoot bool
	Tmpfs        map[string]string
}
