// Package config loads the agent's configuration file.
package config

import (
	"errors"
	"fmt"
	"io"
	"net"
	"net/netip"
	"os"
	"slices"
	"strconv"
	"strings"
	"time"

	"gopkg.in/yaml.v3"

	"github.com/PowerPenguini/murder/internal/edge"
	"github.com/PowerPenguini/murder/internal/spec"
	"github.com/PowerPenguini/murder/internal/strictyaml"
)

// Agent configures one node's agent.
type Agent struct {
	// Node names this node; the agent only manages Docker objects carrying
	// its node label. "murder init" stores it in the state (the host name if
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
	NetworkPool string `yaml:"networkPool"`
	// Firewall is "nftables" (the default) or "off". With nftables the agent
	// keeps tenants from opening connections to the host, other than to
	// the edge ports, and to private and link-local addresses, which
	// include the cloud's metadata service and other servers.
	Firewall string  `yaml:"firewall"`
	Volumes  Volumes `yaml:"volumes"`
	Edge     Edge    `yaml:"edge"`
	// Metrics, if set, is where the agent serves /metrics for Prometheus
	// or VictoriaMetrics: tenant names and their traffic, so a loopback or
	// private address. Tenants cannot reach it while the firewall is on.
	Metrics string `yaml:"metrics"`

	// Cluster, if set, makes the node part of a cluster: its state is
	// replicated between the nodes with Raft.
	Cluster *Cluster `yaml:"cluster"`

	// Defaults fill in tenant limits left at zero. Disk use is unlimited
	// unless diskGB is set (with the zfs driver).
	Defaults          spec.Limits `yaml:"defaults"`
	ReconcileInterval Duration    `yaml:"reconcileInterval"`
}

// Cluster sets up the node's part of a cluster.
type Cluster struct {
	// Address is where the node listens for other nodes and where they
	// reach it: its IP address on the private network and a port.
	Address string `yaml:"address"`
	// Dir holds the cluster's log and snapshots.
	Dir string `yaml:"dir"`
}

// Volumes selects the data volume driver.
type Volumes struct {
	// Driver is "dir" (Root/<tenant>) or "zfs" (Parent/<tenant> mounted at
	// MountRoot/<tenant>). Root and MountRoot default to /srv/murder/tenants.
	Driver    string `yaml:"driver"`
	Root      string `yaml:"root"`
	Parent    string `yaml:"parent"`
	MountRoot string `yaml:"mountRoot"`
}

// Edge configures the public listeners, which "murder edge" serves.
type Edge struct {
	HTTP  string         `yaml:"http"`
	HTTPS string         `yaml:"https"`
	TLS   edge.TLSConfig `yaml:"tls"`
	// Metrics, if set, is where the edge serves /metrics: its requests by
	// tenant, its certificate and its routes. A loopback or private
	// address, as for the agent's.
	Metrics string `yaml:"metrics"`
	// Routes keeps the last routes the edge got from the agent, so an edge
	// started while the agent is down serves all the same.
	Routes string `yaml:"routes"`
	// PIDFile names the edge's process; an upgrade replaces the process,
	// and systemd follows it there.
	PIDFile string `yaml:"pidFile"`
	// Drain is how long an edge that was replaced or stopped finishes the
	// requests it has, such as event streams, before it closes them.
	Drain Duration `yaml:"drain"`
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
		Database:      "/var/lib/murder/murder.db",
		MasterKeyFile: "/etc/murder/master.key",
		Socket:        "/run/murder/agent.sock",
		DockerSocket:  "/var/run/docker.sock",
		Volumes:       Volumes{Driver: "dir", Root: "/srv/murder/tenants", MountRoot: "/srv/murder/tenants"},
		NetworkPool:   "10.240.0.0/16",
		Firewall:      FirewallNftables,
		Edge: Edge{
			HTTP: ":80", HTTPS: ":443", TLS: edge.TLSConfig{Mode: edge.TLSOff, Storage: "/var/lib/murder/certs"},
			Routes: "/var/lib/murder/edge-routes.json", PIDFile: "/run/murder-edge/edge.pid", Drain: Duration{30 * time.Second},
		},
		Defaults:          spec.Limits{MemoryMB: 512, CPUs: 1, PIDs: 512},
		ReconcileInterval: Duration{10 * time.Second},
	}
	// An empty document keeps every default; Validate then reports the
	// missing base domain.
	if err := strictyaml.Decode(data, &agent); err != nil && !errors.Is(err, io.EOF) {
		return Agent{}, fmt.Errorf("parse config: %w", err)
	}
	if agent.Cluster != nil {
		if agent.Cluster.Dir == "" {
			agent.Cluster.Dir = "/var/lib/murder/cluster"
		}
	}
	return agent, agent.Validate()
}

// Validate reports settings that cannot work.
func (a Agent) Validate() error {
	var problems []string
	if a.BaseDomain == "" || strings.HasPrefix(a.BaseDomain, ".") || strings.Contains(a.BaseDomain, "/") {
		problems = append(problems, "baseDomain must be a domain name")
	}
	if a.Cluster != nil {
		problems = append(problems, a.Cluster.problems()...)
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
	if a.Firewall != FirewallNftables && a.Firewall != FirewallOff {
		problems = append(problems, fmt.Sprintf("firewall must be %q or %q", FirewallNftables, FirewallOff))
	}
	if a.Firewall == FirewallNftables {
		if _, err := a.EdgePorts(); err != nil {
			problems = append(problems, err.Error())
		}
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
	if a.Metrics != "" {
		if host, _, err := net.SplitHostPort(a.Metrics); err != nil || host == "" {
			problems = append(problems, "metrics must be a host and port, such as 127.0.0.1:9464")
		}
	}
	if a.Edge.Metrics != "" {
		if host, _, err := net.SplitHostPort(a.Edge.Metrics); err != nil || host == "" {
			problems = append(problems, "edge.metrics must be a host and port, such as 127.0.0.1:9465")
		}
	}
	if !strings.HasPrefix(a.Edge.Routes, "/") || !strings.HasPrefix(a.Edge.PIDFile, "/") {
		problems = append(problems, "edge.routes and edge.pidFile must be absolute paths")
	}
	if a.Edge.Drain.Duration < 0 {
		problems = append(problems, "edge.drain must not be negative")
	}
	for _, problem := range a.Defaults.Problems() {
		problems = append(problems, "defaults: "+problem)
	}
	if a.ReconcileInterval.Duration < time.Second {
		problems = append(problems, "reconcileInterval must be at least 1s")
	}
	if len(problems) > 0 {
		return fmt.Errorf("invalid config: %s", strings.Join(problems, "; "))
	}
	return nil
}

// Firewall settings.
const (
	FirewallNftables = "nftables"
	FirewallOff      = "off"
)

// EdgePorts are the ports the edge listens on, which tenants may reach on
// the host: their own public address resolves to it.
func (a Agent) EdgePorts() ([]int, error) {
	addresses := []string{a.Edge.HTTP}
	if a.Edge.TLS.Mode != edge.TLSOff {
		addresses = append(addresses, a.Edge.HTTPS)
	}
	var ports []int
	for _, address := range addresses {
		_, service, err := net.SplitHostPort(address)
		if err == nil {
			var port int
			if port, err = net.LookupPort("tcp", service); err == nil && port > 0 {
				ports = append(ports, port)
				continue
			}
		}
		return nil, fmt.Errorf("edge address %q needs a port the firewall can open", address)
	}
	return ports, nil
}

// Scheme is the public URL scheme tenants are served with.
func (a Agent) Scheme() string {
	if a.Edge.TLS.Mode == edge.TLSOff {
		return "http"
	}
	return "https"
}

// sharedAddressSpace (RFC 6598) is private too: Tailscale and some
// WireGuard setups use it.
var sharedAddressSpace = netip.MustParsePrefix("100.64.0.0/10")

func (c Cluster) problems() []string {
	var problems []string
	host, port, err := net.SplitHostPort(c.Address)
	address, parseErr := netip.ParseAddr(host)
	number, portErr := strconv.Atoi(port)
	if err != nil || parseErr != nil || address.IsUnspecified() || portErr != nil || number < 1 || number > 65535 {
		problems = append(problems, "cluster.address must be the node's IP address on the private network and a port, such as 10.0.0.2:7400")
	} else if !address.IsPrivate() && !address.IsLoopback() && !sharedAddressSpace.Contains(address) {
		problems = append(problems, "cluster.address must be on a private network: other nodes reach the cluster's state there")
	}
	if !strings.HasPrefix(c.Dir, "/") {
		problems = append(problems, "cluster.dir must be an absolute path")
	}
	return problems
}
