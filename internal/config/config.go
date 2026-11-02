// Package config loads the agent's configuration file.
package config

import (
	"errors"
	"fmt"
	"io"
	"net"
	"net/netip"
	"net/url"
	"os"
	"slices"
	"strconv"
	"strings"
	"time"

	"gopkg.in/yaml.v3"

	"github.com/PowerPenguini/murder/internal/edge"
	"github.com/PowerPenguini/murder/internal/records"
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

	// Backups, if set, keeps the node's tenants and the cluster's state in
	// an S3 bucket.
	Backups *Backups `yaml:"backups"`

	// Defaults fill in tenant limits left at zero. Disk use is unlimited
	// unless diskGB is set (with the zfs driver).
	Defaults          spec.Limits `yaml:"defaults"`
	ReconcileInterval Duration    `yaml:"reconcileInterval"`
}

// Backups keeps tenants' data and the cluster's state in an S3 bucket,
// such as Hetzner Object Storage: every backup is the whole data of a
// tenant (its database, receipts, everything), from a snapshot, compressed
// and encrypted for a recipient whose key is kept apart. Mode test keeps no
// lock, so the bucket can be deleted at once; mode production locks
// objects (compliance: no one deletes them before the lock ends), and needs
// a bucket made with object lock.
type Backups struct {
	Mode          string `yaml:"mode"`
	Endpoint      string `yaml:"endpoint"` // https://hel1.your-objectstorage.com
	Region        string `yaml:"region"`
	Bucket        string `yaml:"bucket"`
	AccessKeyFile string `yaml:"accessKeyFile"`
	SecretKeyFile string `yaml:"secretKeyFile"`
	// Recipient is the age public key (age1…) backups are encrypted for.
	Recipient string `yaml:"recipient"`
	// IdentityFile, if set, holds the matching private key on this node,
	// which then verifies its tenants' backups by restoring them; without
	// it, verification runs from the CLI with the key brought along.
	IdentityFile string   `yaml:"identityFile"`
	Every        Duration `yaml:"every"`
	// Backups are thinned out as they age: every one for keepHourly, the
	// first of each day for keepDaily, of each month for keepMonthly.
	KeepHourly  Duration `yaml:"keepHourly"`
	KeepDaily   Duration `yaml:"keepDaily"`
	KeepMonthly Duration `yaml:"keepMonthly"`
	// LockFor locks, in production, the first backup of each day; the
	// others are locked while they are kept.
	LockFor     Duration `yaml:"lockFor"`
	VerifyEvery Duration `yaml:"verifyEvery"`
	KeepLocal   int      `yaml:"keepLocal"`
}

func (b Backups) problems(driver string) []string {
	var problems []string
	if driver != "zfs" {
		problems = append(problems, "backups need the zfs volume driver")
	}
	if b.Mode != "test" && b.Mode != "production" {
		problems = append(problems, "backups.mode must be test or production")
	}
	// HTTPS only: the backups are encrypted anyway, but the requests sign
	// with the bucket's keys.
	if endpoint, err := url.Parse(b.Endpoint); err != nil || endpoint.Scheme != "https" || endpoint.Host == "" || strings.Trim(endpoint.Path, "/") != "" || endpoint.RawQuery != "" || endpoint.User != nil {
		problems = append(problems, "backups.endpoint must be an https URL without a path, such as https://hel1.your-objectstorage.com")
	}
	if b.Bucket == "" || b.AccessKeyFile == "" || b.SecretKeyFile == "" {
		problems = append(problems, "backups need bucket, accessKeyFile and secretKeyFile")
	}
	if !strings.HasPrefix(b.Recipient, "age1") {
		problems = append(problems, "backups.recipient must be an age public key (age1…)")
	}
	switch {
	case b.Mode == "test" && b.LockFor.Duration != 0:
		problems = append(problems, "test backups are not locked: leave backups.lockFor out")
	case b.Mode == "production" && b.LockFor.Duration <= 0:
		problems = append(problems, "production backups need backups.lockFor")
	case b.LockFor.Duration > b.KeepDaily.Duration:
		problems = append(problems, "backups.lockFor must not outlast backups.keepDaily")
	}
	if b.VerifyEvery.Duration < b.Every.Duration {
		problems = append(problems, "backups.verifyEvery must be at least backups.every")
	}
	if b.Every.Duration < time.Minute || b.KeepHourly.Duration < b.Every.Duration || b.KeepDaily.Duration < b.KeepHourly.Duration || b.KeepMonthly.Duration < b.KeepDaily.Duration || b.KeepLocal < 1 {
		problems = append(problems, "backups: every at least 1m, keepHourly at least every, keepDaily at least keepHourly, keepMonthly at least keepDaily, keepLocal at least 1")
	}
	return problems
}

// Cluster sets up the node's part of a cluster.
type Cluster struct {
	// Address is where the node listens for other nodes and where they
	// reach it: its IP address on the private network and a port.
	Address string `yaml:"address"`
	// Dir holds the cluster's log and snapshots.
	Dir string `yaml:"dir"`
	// Failover, if set, has the leader power a dead node off through its
	// provider and run its tenants from their replicas.
	Failover *Failover `yaml:"failover"`
	// PublicAddresses are the node's public IP addresses, where clients
	// reach its edge; the DNS records name them while the node serves.
	PublicAddresses []string `yaml:"publicAddresses"`
	// DNS, if set, has the leader keep the base domain's wildcard records
	// naming the public addresses of the nodes that serve.
	DNS *DNS `yaml:"dns"`
}

// DNS reaches the provider of the base domain's zone.
type DNS struct {
	// Provider is "hetzner" (Hetzner Cloud DNS) or "namecheap".
	Provider string `yaml:"provider"`
	// TokenFile holds the provider's API token; for Namecheap it holds
	// "user:apikey".
	TokenFile string `yaml:"tokenFile"`
	// Zone is the zone the base domain is in; the base domain if empty.
	Zone string `yaml:"zone"`
	// TTL is how long resolvers keep the records; 60 s by default.
	TTL Duration `yaml:"ttl"`
	// Endpoint is the Hetzner API's base URL, for tests.
	Endpoint string `yaml:"endpoint"`
}

// DNSProviders are the providers cluster.dns supports.
var DNSProviders = []string{"hetzner", "namecheap"}

// Failover runs a dead node's tenants from their replicas.
type Failover struct {
	// After is how long a node goes unanswered before it counts as dead;
	// 30 s by default.
	After Duration `yaml:"after"`
	// Hetzner powers nodes off through the Hetzner Cloud API.
	Hetzner *HetznerFence `yaml:"hetzner"`
}

// HetznerFence reaches the Hetzner Cloud project of the nodes.
type HetznerFence struct {
	// TokenFile holds a read and write API token of the project.
	TokenFile string `yaml:"tokenFile"`
	// Servers names each node's server, where it is not the node's name.
	Servers map[string]string `yaml:"servers"`
	// Endpoint is the API's base URL, for tests; Hetzner's if empty.
	Endpoint string `yaml:"endpoint"`
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
	// RelayPort is where, in a cluster, the edge takes requests other
	// nodes' edges pass on to the tenants this node runs: on the node's
	// cluster address. The same on every node.
	RelayPort int `yaml:"relayPort"`
}

// RelayAddress is where the edge takes requests other nodes pass on; empty
// outside a cluster.
func (a Agent) RelayAddress() string {
	if a.Cluster == nil {
		return ""
	}
	host, _, _ := net.SplitHostPort(a.Cluster.Address)
	return net.JoinHostPort(host, strconv.Itoa(a.Edge.RelayPort))
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
			Routes: "/var/lib/murder/edge-routes.json", PIDFile: "/run/murder-edge/edge.pid", Drain: Duration{30 * time.Second}, RelayPort: 7402,
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
		if f := agent.Cluster.Failover; f != nil && f.After.Duration == 0 {
			f.After.Duration = 30 * time.Second
		}
		if d := agent.Cluster.DNS; d != nil {
			if d.Zone == "" {
				d.Zone = agent.BaseDomain
			}
			if d.TTL.Duration == 0 {
				d.TTL.Duration = time.Minute
			}
		}
	}
	if b := agent.Backups; b != nil {
		day := 24 * time.Hour
		for _, d := range []struct {
			field *Duration
			value time.Duration
		}{{&b.Every, time.Hour}, {&b.KeepHourly, 2 * day}, {&b.KeepDaily, 30 * day}, {&b.KeepMonthly, 365 * day}, {&b.VerifyEvery, 7 * day}} {
			if d.field.Duration == 0 {
				d.field.Duration = d.value
			}
		}
		// The default lock never outlasts the daily backups it locks.
		if b.Mode == "production" && b.LockFor.Duration == 0 {
			b.LockFor.Duration = min(30*day, b.KeepDaily.Duration)
		}
		if b.KeepLocal == 0 {
			b.KeepLocal = 24
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
	if a.Backups != nil {
		problems = append(problems, a.Backups.problems(a.Volumes.Driver)...)
	}
	if d := a.Cluster; d != nil && d.DNS != nil && a.BaseDomain != "" {
		if _, err := records.Wildcard(a.BaseDomain, d.DNS.Zone); err != nil {
			problems = append(problems, "cluster.dns.zone: "+err.Error())
		}
	}
	if a.Cluster != nil && a.Cluster.Failover != nil && a.Volumes.Driver != "zfs" {
		problems = append(problems, "cluster.failover needs the zfs volume driver: only ZFS nodes keep copies of tenants to run them from")
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
	if a.Edge.RelayPort < 1 || a.Edge.RelayPort > 65535 {
		problems = append(problems, "edge.relayPort must be a port")
	} else if a.Cluster != nil && a.RelayAddress() == a.Cluster.Address {
		problems = append(problems, "edge.relayPort must differ from the cluster address's port")
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
	if f := c.Failover; f != nil {
		if f.After.Duration < 10*time.Second {
			problems = append(problems, "cluster.failover.after must be at least 10s: shorter, a busy node would be powered off")
		}
		if f.Hetzner == nil {
			problems = append(problems, "cluster.failover needs a provider to power nodes off (hetzner); without one, fail a node over by hand: murder cluster failover NODE -powered-off")
		} else if !strings.HasPrefix(f.Hetzner.TokenFile, "/") {
			problems = append(problems, "cluster.failover.hetzner.tokenFile must be an absolute path")
		}
	}
	for _, value := range c.PublicAddresses {
		if address, err := netip.ParseAddr(value); err != nil || address.Zone() != "" {
			problems = append(problems, fmt.Sprintf("cluster.publicAddresses: %q is not an IP address", value))
		} else if address.IsPrivate() || address.IsLoopback() || address.IsUnspecified() || address.IsLinkLocalUnicast() || address.IsMulticast() {
			problems = append(problems, fmt.Sprintf("cluster.publicAddresses: %s is not a public address", value))
		}
	}
	if d := c.DNS; d != nil {
		if !slices.Contains(DNSProviders, d.Provider) {
			problems = append(problems, fmt.Sprintf("cluster.dns.provider must be one of %v", DNSProviders))
		}
		if !strings.HasPrefix(d.TokenFile, "/") {
			problems = append(problems, "cluster.dns.tokenFile must be an absolute path")
		}
		if d.TTL.Duration < time.Minute || d.TTL.Duration > 24*time.Hour {
			problems = append(problems, "cluster.dns.ttl must be from 60s, which providers require, to 24h")
		} else if d.Provider == "namecheap" && d.TTL.Duration > records.MaxNamecheapTTL {
			problems = append(problems, "cluster.dns.ttl must be at most 60000s with namecheap")
		}
		if len(c.PublicAddresses) == 0 {
			problems = append(problems, "cluster.dns needs cluster.publicAddresses: without them the records never name this node")
		}
	}
	return problems
}
