// Package runtime runs tenant containers. Runtime is the narrow seam the
// reconciler depends on; Docker implements it through the Engine API.
package runtime

import (
	"context"
	"errors"
	"fmt"
	"net/netip"
	"time"
)

// ErrNotFound reports a missing container, network or image.
var ErrNotFound = errors.New("not found")

// ErrNotRunning reports a container that has no running process.
var ErrNotRunning = errors.New("container is not running")

// ErrNotOwned reports an object with the expected name that this node does
// not own; Murder leaves it alone.
var ErrNotOwned = errors.New("not owned by this node")

// ErrInUse reports an image a container still uses.
var ErrInUse = errors.New("in use")

// ErrNetworkMismatch reports a network that exists with another subnet.
var ErrNetworkMismatch = errors.New("network has another subnet")

// Addresses returns the two addresses a tenant network uses: its gateway,
// which is the host (the edge proxy and health checks connect from it), and
// the tenant container's own fixed address.
func Addresses(subnet string) (gateway, container netip.Addr, err error) {
	prefix, err := netip.ParsePrefix(subnet)
	if err != nil || !prefix.Addr().Is4() || prefix.Bits() > 29 || prefix != prefix.Masked() {
		return netip.Addr{}, netip.Addr{}, fmt.Errorf("tenant subnet %q is not an IPv4 network of /29 or larger", subnet)
	}
	gateway = prefix.Addr().Next()
	return gateway, gateway.Next(), nil
}

// Labels Murder puts on everything it creates. LabelNode marks which node's
// agent owns an object; an agent never touches another node's objects.
const (
	LabelManaged = "io.murder.managed"
	LabelNode    = "io.murder.node"
	LabelTenant  = "io.murder.tenant"
	LabelHash    = "io.murder.spec-hash"
	LabelImage   = "io.murder.image"
)

// Runtime is what the reconciler needs from a container engine.
type Runtime interface {
	// HasImage reports whether ref is present locally; PullImage fetches it.
	HasImage(ctx context.Context, ref string) (bool, error)
	PullImage(ctx context.Context, ref string) error
	// ImageID returns the ID of the image ref names, or "" if it is not
	// present.
	ImageID(ctx context.Context, ref string) (string, error)
	// RemoveImage deletes the image with this ID and its names. One a
	// container uses, stopped or not, or one named in several repositories,
	// is kept and reported with ErrInUse; a missing one is no error.
	RemoveImage(ctx context.Context, id string) error
	// Resolve pins an image reference: a digest reference or image ID comes
	// back unchanged; a tag is pulled and replaced by its registry digest,
	// or by the local image ID if it has none or cannot be pulled. note
	// explains a fallback to the local image.
	Resolve(ctx context.Context, ref string) (pinned, note string, err error)
	// EnsureNetwork creates the isolated network with the given IPv4 subnet
	// (its gateway the first address) if it is missing. An existing network
	// with another subnet is reported with ErrNetworkMismatch, and one this
	// node does not own is an error. RemoveNetwork leaves alone a network
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
	Name    string
	Image   string
	Env     []string
	User    string
	Labels  map[string]string
	Network string
	// Address is the container's fixed IPv4 address in Network. Nothing is
	// published on the host: the edge proxy connects to Address directly.
	Address      string
	Mounts       []Mount
	MemoryBytes  int64
	NanoCPUs     int64
	PIDs         int64
	ReadOnlyRoot bool
	Tmpfs        map[string]string
}
