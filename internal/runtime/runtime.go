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

// Labels Corral puts on everything it creates.
const (
	LabelManaged = "io.corral.managed"
	LabelTenant  = "io.corral.tenant"
	LabelHash    = "io.corral.spec-hash"
	LabelImage   = "io.corral.image"
)

// Runtime is what the reconciler needs from a container engine.
type Runtime interface {
	// HasImage reports whether ref is present locally; PullImage fetches it.
	HasImage(ctx context.Context, ref string) (bool, error)
	PullImage(ctx context.Context, ref string) error
	// EnsureNetwork creates the isolated network if needed and returns its
	// subnet in CIDR notation.
	EnsureNetwork(ctx context.Context, name string, labels map[string]string) (string, error)
	RemoveNetwork(ctx context.Context, name string) error
	// List returns every container labelled as managed by Corral.
	List(ctx context.Context) ([]Container, error)
	Create(ctx context.Context, spec ContainerSpec) (string, error)
	Start(ctx context.Context, id string) error
	Stop(ctx context.Context, id string, timeout time.Duration) error
	Remove(ctx context.Context, id string) error
	// Logs returns the last lines of a container's output.
	Logs(ctx context.Context, id string, tail int) ([]byte, error)
}

// Container is the observed state of one managed container.
type Container struct {
	ID      string
	Name    string
	Running bool
	State   string
	Labels  map[string]string
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
