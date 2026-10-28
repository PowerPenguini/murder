// Package config loads the agent's configuration file.
package config

import (
	"bytes"
	"errors"
	"fmt"
	"io"
	"net/netip"
	"os"
	"slices"
	"strings"
	"time"

	"gopkg.in/yaml.v3"

	"github.com/PowerPenguini/corral/internal/edge"
	"github.com/PowerPenguini/corral/internal/spec"
)

// Agent configures one node's agent.
type Agent struct {
	// Node names this node; the agent only manages Docker objects carrying
	// its node label. "corral init" stores it in the state (the host name if
	// unset here); afterwards the database decides and a different value
	// here is an error, so renaming the host changes nothing.
	Node string `yaml:"node"`
	// BaseDomain is the domain tenants live below: <tenant>.<BaseDomain>.
	BaseDomain string `yaml:"baseDomain"`
	// Database holds the node's apps, tenants, secrets and audit log.
	Database      string `yaml:"database"`
	MasterKeyFile string `yaml:"masterKeyFile"`
	// Socket is the local API the CLI talks to. SocketGroup, if set, may use
	// it besides root.
	Socket       string `yaml:"socket"`
	SocketGroup  string `yaml:"socketGroup"`
	DockerSocket string `yaml:"dockerSocket"`
	// RegistryAuthFile is a Docker config.json with credentials for private
	// registries (as written by "docker login" without a credential helper).
	RegistryAuthFile string `yaml:"registryAuthFile"`
	// NetworkPool is split into one /28 per tenant network. It must not
	// overlap networks the host or Docker already use.
	NetworkPool string  `yaml:"networkPool"`
	Volumes     Volumes `yaml:"volumes"`
	Edge        Edge    `yaml:"edge"`
	// Defaults fill in tenant limits left at zero.
	// RetiredPorts catches the "ports" setting of earlier releases, so its
	// error says what to do instead of naming an unknown field.
	RetiredPorts any `yaml:"ports"`

	Defaults          spec.Limits `yaml:"defaults"`
	ReconcileInterval Duration    `yaml:"reconcileInterval"`
}

// Volumes selects the data volume driver.
type Volumes struct {
	// Driver is "dir" (Root/<tenant>) or "zfs" (Parent/<tenant> mounted at
	// MountRoot/<tenant>). Root and MountRoot default to /srv/corral/tenants.
	Driver    string `yaml:"driver"`
	Root      string `yaml:"root"`
	Parent    string `yaml:"parent"`
	MountRoot string `yaml:"mountRoot"`
}

// Edge configures the public listeners.
type Edge struct {
	HTTP  string         `yaml:"http"`
	HTTPS string         `yaml:"https"`
	TLS   edge.TLSConfig `yaml:"tls"`
}

// Duration reads Go duration strings such as "10s".
type Duration struct{ time.Duration }

func (d *Duration) UnmarshalYAML(node *yaml.Node) error {
	parsed, err := time.ParseDuration(node.Value)
	if err != nil {
		return fmt.Errorf("duration %q: %w", node.Value, err)
	}
	d.Duration = parsed
	return nil
}

// Load reads the file at path and fills in defaults.
func Load(path string) (Agent, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return Agent{}, err
	}
	return Parse(data)
}

// Parse reads a configuration and fills in defaults.
func Parse(data []byte) (Agent, error) {
	agent := Agent{
		Database:          "/var/lib/corral/corral.db",
		MasterKeyFile:     "/etc/corral/master.key",
		Socket:            "/run/corral/agent.sock",
		DockerSocket:      "/var/run/docker.sock",
		Volumes:           Volumes{Driver: "dir", Root: "/srv/corral/tenants", MountRoot: "/srv/corral/tenants"},
		NetworkPool:       "10.240.0.0/16",
		Edge:              Edge{HTTP: ":80", HTTPS: ":443", TLS: edge.TLSConfig{Mode: edge.TLSOff, Storage: "/var/lib/corral/certs"}},
		Defaults:          spec.Limits{MemoryMB: 512, CPUs: 1, PIDs: 512},
		ReconcileInterval: Duration{10 * time.Second},
	}
	decoder := yaml.NewDecoder(bytes.NewReader(data))
	decoder.KnownFields(true)
	// An empty document keeps every default; Validate then reports the
	// missing base domain.
	if err := decoder.Decode(&agent); err != nil && !errors.Is(err, io.EOF) {
		return Agent{}, fmt.Errorf("parse config: %w", err)
	}
	return agent, agent.Validate()
}

// Validate reports settings that cannot work.
func (a Agent) Validate() error {
	var problems []string
	if a.RetiredPorts != nil {
		problems = append(problems, "ports is no longer used (tenants are reached at their own address, nothing is published on the host); remove it")
	}
	if a.BaseDomain == "" || strings.HasPrefix(a.BaseDomain, ".") || strings.Contains(a.BaseDomain, "/") {
		problems = append(problems, "baseDomain must be a domain name")
	}
	switch a.Volumes.Driver {
	case "dir":
		if !strings.HasPrefix(a.Volumes.Root, "/") {
			problems = append(problems, "volumes.root must be an absolute path")
		}
	case "zfs":
		if a.Volumes.Parent == "" || !strings.HasPrefix(a.Volumes.MountRoot, "/") {
			problems = append(problems, "zfs volumes need parent and an absolute mountRoot")
		}
	default:
		problems = append(problems, fmt.Sprintf("unknown volume driver %q", a.Volumes.Driver))
	}
	if pool, err := netip.ParsePrefix(a.NetworkPool); err != nil || !pool.Addr().Is4() || pool.Bits() > 24 {
		problems = append(problems, "networkPool must be an IPv4 prefix of /24 or larger")
	}
	if strings.ContainsAny(a.Node, " \t\n=") {
		problems = append(problems, "node must be a name without spaces or '='")
	}
	switch a.Edge.TLS.Mode {
	case edge.TLSOff, edge.TLSInternal, edge.TLSACME:
	default:
		problems = append(problems, fmt.Sprintf("unknown edge.tls.mode %q", a.Edge.TLS.Mode))
	}
	if a.Edge.TLS.Mode != edge.TLSOff && !strings.HasPrefix(a.Edge.TLS.Storage, "/") {
		problems = append(problems, "edge.tls.storage must be an absolute path")
	}
	if a.Edge.TLS.Mode == edge.TLSACME {
		if a.Edge.TLS.Email == "" || a.Edge.TLS.DNSTokenFile == "" {
			problems = append(problems, "acme TLS needs email and dnsTokenFile")
		}
		if !slices.Contains(edge.DNSProviders, a.Edge.TLS.DNSProvider) {
			problems = append(problems, fmt.Sprintf("edge.tls.dnsProvider must be one of %v", edge.DNSProviders))
		}
	}
	if a.ReconcileInterval.Duration < time.Second {
		problems = append(problems, "reconcileInterval must be at least 1s")
	}
	if len(problems) > 0 {
		return fmt.Errorf("invalid config: %s", strings.Join(problems, "; "))
	}
	return nil
}

// Scheme is the public URL scheme tenants are served with.
func (a Agent) Scheme() string {
	if a.Edge.TLS.Mode == edge.TLSOff {
		return "http"
	}
	return "https"
}
