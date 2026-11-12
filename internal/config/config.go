// Package config loads the agent's configuration file.
package config

import (
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"net"
	"net/netip"
	"net/url"
	"regexp"
	"slices"
	"strconv"
	"strings"
	"time"

	"gopkg.in/yaml.v3"

	"github.com/PowerPenguini/murder/internal/edge"
	"github.com/PowerPenguini/murder/internal/firewall"
	"github.com/PowerPenguini/murder/internal/hostprobe"
	"github.com/PowerPenguini/murder/internal/records"
	"github.com/PowerPenguini/murder/internal/spec"
	"github.com/PowerPenguini/murder/internal/strictyaml"
)

// Agent configures one node's agent.
type Agent struct {
	// Node names this node; the agent only manages Docker objects carrying
	// its node label. "murder init" stores it in the state (the host name's
	// first label if unset here); afterwards the database decides and a
	// different value set here is an error, so renaming the host changes
	// nothing.
	Node string `yaml:"node"`
	// BaseDomain is the domain tenants live below: <tenant>.<BaseDomain>.
	BaseDomain string `yaml:"baseDomain"`
	// PublicAddresses are a node alone's public IP addresses, where clients
	// reach its edge: a tenant's own domain must point at them before its
	// certificate is ordered. A node of a cluster has cluster.publicAddresses.
	// Left out, they are the public addresses of the host's interfaces.
	PublicAddresses []string `yaml:"publicAddresses"`
	// Database holds the node's apps, tenants, secrets and audit log.
	Database      string `yaml:"database"`
	MasterKeyFile string `yaml:"masterKeyFile"`
	// Socket is the local API the CLI talks to. SocketGroup, if set, may use
	// it besides root.
	Socket       string `yaml:"socket"`
	SocketGroup  string `yaml:"socketGroup"`
	DockerSocket string `yaml:"dockerSocket"`
	// Registries are the private registries the node pulls from and asks
	// about tags, each signed in to with a user and a token kept in a file:
	// GitHub's container registry (ghcr.io) with a token that may only read
	// packages, or any other. They come before RegistryAuthFile.
	Registries []Registry `yaml:"registries"`
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
	Firewall string `yaml:"firewall"`
	// HostFirewall, if set, has the same firewall drop what reaches the
	// host from anywhere other than a tenant network, unless it goes to
	// SSH, the edge, the panel's tunnel or a port listed here; the cluster
	// and relay ports, and metrics not on loopback, stay open to private
	// networks only. Off without the key, as it would close ports the host
	// serves besides Murder; murder setup writes it, with what listened.
	HostFirewall *HostFirewall `yaml:"hostFirewall"`
	Volumes      Volumes       `yaml:"volumes"`
	Edge         Edge          `yaml:"edge"`
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

	// Panel configures "murder panel", the administration panel.
	Panel Panel `yaml:"panel"`

	// Defaults fill in tenant limits left at zero. Disk use is unlimited
	// unless diskGB is set (with the zfs driver).
	Defaults          spec.Limits `yaml:"defaults"`
	ReconcileInterval Duration    `yaml:"reconcileInterval"`
	// ImageChecks is how often the cluster's leader asks apps' registries
	// which image each app's tag names now (5m); 0 asks never.
	ImageChecks Duration `yaml:"imageChecks"`

	// Detected are the node's own settings the config left out, as found
	// on the host (see Detect).
	Detected []Detected `yaml:"-"`
}

// Registry is how the node signs in to one private registry.
type Registry struct {
	// Host is the registry's host name, with its port if it has one, as
	// image names begin with it: ghcr.io. docker.io is Docker Hub.
	Host     string `yaml:"host"`
	Username string `yaml:"username"`
	// TokenFile holds the password or access token. It is read at every
	// pull, so a token replaced in it is used at once.
	TokenFile string `yaml:"tokenFile"`
}

// registryHost is a host name with an optional port, as an image name
// begins with it.
var registryHost = regexp.MustCompile(`^(localhost|[A-Za-z0-9]([A-Za-z0-9-]*[A-Za-z0-9])?(\.[A-Za-z0-9]([A-Za-z0-9-]*[A-Za-z0-9])?)+)(:[0-9]{1,5})?$`)

// sameRegistry names a registry as pulls see it: Docker Hub's host names
// are all docker.io.
func sameRegistry(host string) string {
	host = strings.ToLower(host)
	switch host {
	case "index.docker.io", "registry-1.docker.io":
		return "docker.io"
	}
	return host
}

func registriesProblems(registries []Registry) []string {
	var problems []string
	seen := map[string]bool{}
	for i, registry := range registries {
		where := fmt.Sprintf("registries[%d]", i)
		switch {
		case !registryHost.MatchString(registry.Host):
			problems = append(problems, where+".host must be a registry's host name, such as ghcr.io, without https:// or a path")
		case seen[sameRegistry(registry.Host)]:
			problems = append(problems, where+".host "+registry.Host+" is given twice")
		}
		seen[sameRegistry(registry.Host)] = true
		if registry.Username == "" {
			problems = append(problems, where+".username must be set")
		}
		if !strings.HasPrefix(registry.TokenFile, "/") {
			problems = append(problems, where+".tokenFile must be an absolute path")
		}
	}
	return problems
}

// HostFirewall lists what reaches the host besides Murder's own ports.
type HostFirewall struct {
	// SSH are the ports sshd listens on, open to everyone. Left out, they
	// are found from sshd at every start, so a port sshd moved to is open
	// once the agent starts again; a port missing here locks SSH out
	// (connections already open stay).
	SSH []int `yaml:"ssh"`
	// Open are more ports open to everyone, and Private more ports open
	// to private networks only, as "9100/tcp" or "53/udp".
	Open    []string `yaml:"open"`
	Private []string `yaml:"private"`
}

// HostRules is what the host firewall lets in; nil when hostFirewall is
// unset. The rules name ports, and for the private ones the private
// source networks, never the node's own addresses: what is open is open
// on every address the host has. So public addresses found on the host,
// which may differ from one start to the next (an interface up late at
// boot, a new DHCP lease), change nothing here; only the ports do.
func (a Agent) HostRules() (*firewall.Host, error) {
	h := a.HostFirewall
	if h == nil {
		return nil, nil
	}
	var problems []string
	rules := &firewall.Host{}
	for _, port := range h.SSH {
		if port < 1 || port > 65535 {
			problems = append(problems, fmt.Sprintf("hostFirewall.ssh: %d is not a port", port))
		}
		rules.PublicTCP = append(rules.PublicTCP, port)
	}
	if h.SSH != nil && len(h.SSH) == 0 {
		problems = append(problems, "hostFirewall.ssh must name the SSH ports, or the firewall locks SSH out")
	}
	edgePorts, err := a.EdgePorts()
	if err != nil {
		problems = append(problems, err.Error())
	}
	rules.PublicTCP = append(rules.PublicTCP, edgePorts...)
	if t := a.Panel.Tunnel; t != nil {
		rules.PublicUDP = append(rules.PublicUDP, t.Port)
	}
	if a.Cluster != nil {
		if port := a.Cluster.Port(); port > 0 {
			rules.PrivateTCP = append(rules.PrivateTCP, port)
		}
		rules.PrivateTCP = append(rules.PrivateTCP, a.Edge.RelayPort)
	}
	// Metrics on loopback need no rule; on another address they are for a
	// scraper on the private network.
	for _, address := range []string{a.Metrics, a.Edge.Metrics} {
		host, port, err := net.SplitHostPort(address)
		if err != nil {
			continue
		}
		if ip, err := netip.ParseAddr(host); err == nil && ip.IsLoopback() || host == "localhost" {
			continue
		}
		if number, err := strconv.Atoi(port); err == nil {
			rules.PrivateTCP = append(rules.PrivateTCP, number)
		}
	}
	for _, list := range []struct {
		name     string
		entries  []string
		tcp, udp *[]int
	}{
		{"open", h.Open, &rules.PublicTCP, &rules.PublicUDP},
		{"private", h.Private, &rules.PrivateTCP, &rules.PrivateUDP},
	} {
		for _, entry := range list.entries {
			port, protocol, _ := strings.Cut(entry, "/")
			number, err := strconv.Atoi(port)
			switch {
			case err != nil || number < 1 || number > 65535:
				problems = append(problems, fmt.Sprintf("hostFirewall.%s: %q needs a port, such as 9100/tcp", list.name, entry))
			case protocol == "tcp":
				*list.tcp = append(*list.tcp, number)
			case protocol == "udp":
				*list.udp = append(*list.udp, number)
			default:
				problems = append(problems, fmt.Sprintf("hostFirewall.%s: %q needs /tcp or /udp", list.name, entry))
			}
		}
	}
	if len(problems) > 0 {
		return nil, errors.New(strings.Join(problems, "; "))
	}
	return rules, nil
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
	// ForgetPurgedAfter is how long after a tenant's purge every backup of
	// it is deleted, keepDaily by default: by then the locks of its
	// backups have ended, and a purge done by mistake had as long to be
	// undone as a daily backup is kept.
	ForgetPurgedAfter Duration `yaml:"forgetPurgedAfter"`
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
	if b.ForgetPurgedAfter.Duration < 0 {
		problems = append(problems, "backups.forgetPurgedAfter must not be negative")
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
	// reach it: its IP address on the private network and a port. Left
	// out, it is the host's only private address, at port 7400; a host
	// with several names it in node.yaml.
	Address string `yaml:"address"`
	// Dir holds the cluster's log and snapshots.
	Dir string `yaml:"dir"`
	// Failover, if set, has the leader power a dead node off through its
	// provider and run its tenants from their replicas.
	Failover *Failover `yaml:"failover"`
	// PublicAddresses are the node's public IP addresses, where clients
	// reach its edge; the DNS records name them while the node serves.
	// Left out, they are the public addresses of the host's interfaces;
	// [] says it has none.
	PublicAddresses []string `yaml:"publicAddresses"`
	// DNS, if set, has the leader keep the base domain's wildcard records
	// naming the public addresses of the nodes that serve.
	DNS *DNS `yaml:"dns"`
}

// Panel is where the administration panel serves.
type Panel struct {
	// Socket is the unix socket the panel serves on, readable by root
	// only, reached through an SSH tunnel.
	Socket string `yaml:"socket"`
	// Name is the panel's host name inside the tunnel; panel.<baseDomain>
	// by default, which the wildcard certificate covers.
	Name string `yaml:"name"`
	// Tunnel, if set, serves the panel inside a WireGuard tunnel that
	// only the devices added with "murder admin device add" reach.
	Tunnel *Tunnel `yaml:"tunnel"`
}

// Tunnel is this node's end of the panel's WireGuard tunnel.
type Tunnel struct {
	// Address is this node's address in the tunnel, different on every
	// node and below the network's 100th address, where devices begin.
	// Left out, the panel takes the one the cluster's state has for this
	// node, or else the lowest one no node has, and records it there.
	Address string `yaml:"address"`
	// Network is the tunnel's network, the same on every node;
	// 10.77.0.0/24 by default.
	Network string `yaml:"network"`
	// Port is the UDP port, open on the public addresses; 51820 by default.
	Port int `yaml:"port"`
	// KeyFile holds this node's WireGuard private key, made at first start.
	KeyFile string `yaml:"keyFile"`
}

// hostNumber is address's place in network: 1 for its first host.
func hostNumber(network netip.Prefix, address netip.Addr) uint32 {
	base, host := network.Masked().Addr().As4(), address.As4()
	return binary.BigEndian.Uint32(host[:]) - binary.BigEndian.Uint32(base[:])
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
	// MountRoot/<tenant>). Root and MountRoot default to /srv/murder/tenants;
	// Parent to <pool>/murder/tenants on a host with one ZFS pool.
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
	// RoutesHold is how long an edge started with those routes, as after a
	// reboot, holds requests for tenants until the agent confirms routes,
	// which may have changed while the node was down: past it, the saved
	// routes serve. Zero serves them at once.
	RoutesHold Duration `yaml:"routesHold"`
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

// Parse reads a configuration and fills in defaults. It checks what is
// set, and lets the node's own settings be left out, as in a config the
// nodes of a cluster share: Detect finds those, then Validate checks the
// whole. Load does both.
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
			Routes: "/var/lib/murder/edge-routes.json", PIDFile: "/run/murder-edge/edge.pid", Drain: Duration{30 * time.Second}, RoutesHold: Duration{15 * time.Second}, RelayPort: 7402,
		},
		Defaults:          spec.Limits{MemoryMB: 512, CPUs: 1, PIDs: 512},
		ReconcileInterval: Duration{10 * time.Second},
		ImageChecks:       Duration{5 * time.Minute},
	}
	// An empty document keeps every default; Validate then reports the
	// missing base domain.
	if err := strictyaml.Decode(data, &agent); err != nil && !errors.Is(err, io.EOF) {
		return Agent{}, fmt.Errorf("parse config: %w", err)
	}
	if agent.Panel.Socket == "" {
		agent.Panel.Socket = "/run/murder/panel.sock"
	}
	if agent.Panel.Name == "" && agent.BaseDomain != "" {
		agent.Panel.Name = "panel." + agent.BaseDomain
	}
	if t := agent.Panel.Tunnel; t != nil {
		if t.Network == "" {
			t.Network = "10.77.0.0/24"
		}
		if t.Port == 0 {
			t.Port = 51820
		}
		if t.KeyFile == "" {
			t.KeyFile = "/var/lib/murder/panel/tunnel.key"
		}
	}
	// A bare "hostFirewall:" is null, which leaves the pointer nil: it asks
	// for the firewall with every default all the same.
	if agent.HostFirewall == nil && bareKey(data, "hostFirewall") {
		agent.HostFirewall = &HostFirewall{}
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
		if b.ForgetPurgedAfter.Duration == 0 {
			b.ForgetPurgedAfter.Duration = b.KeepDaily.Duration
		}
	}
	return agent, agent.validate(true)
}

// Validate reports settings that cannot work, the node's own ones
// included.
func (a Agent) Validate() error { return a.validate(false) }

// validate reports settings that cannot work; shared lets the node's own
// settings be left out, for Detect to find.
func (a Agent) validate(shared bool) error {
	var problems []string
	if a.BaseDomain == "" || strings.HasPrefix(a.BaseDomain, ".") || strings.Contains(a.BaseDomain, "/") {
		problems = append(problems, "baseDomain must be a domain name")
	}
	problems = append(problems, registriesProblems(a.Registries)...)
	if a.Cluster != nil {
		problems = append(problems, a.Cluster.problems(shared)...)
		if len(a.PublicAddresses) > 0 {
			problems = append(problems, "publicAddresses is for a node alone: a node of a cluster has them under cluster.publicAddresses")
		}
	}
	problems = append(problems, publicProblems("publicAddresses", a.PublicAddresses)...)
	if a.Backups != nil {
		problems = append(problems, a.Backups.problems(a.Volumes.Driver)...)
	}
	if !strings.HasPrefix(a.Panel.Socket, "/") {
		problems = append(problems, "panel.socket must be an absolute path")
	}
	if label, rest, _ := strings.Cut(a.Panel.Name, "."); a.BaseDomain != "" && (rest != a.BaseDomain || label == "" || label == "*") {
		problems = append(problems, "panel.name must be a name right under the base domain, such as panel."+a.BaseDomain+", which the wildcard certificate covers")
	}
	if t := a.Panel.Tunnel; t != nil {
		network, err := netip.ParsePrefix(t.Network)
		address, addressErr := netip.ParseAddr(t.Address)
		switch {
		case err != nil || !network.Addr().Is4() || network.Bits() > 24:
			problems = append(problems, "panel.tunnel.network must be an IPv4 network, a /24 or larger, such as 10.77.0.0/24")
		case t.Address == "":
			// The panel takes one from the cluster's state.
		case addressErr != nil || !network.Contains(address) || address == network.Masked().Addr():
			problems = append(problems, "panel.tunnel.address must be this node's address in panel.tunnel.network")
		case hostNumber(network, address) >= 100:
			problems = append(problems, "panel.tunnel.address must be below the network's 100th address, where devices begin")
		}
		if a.Cluster == nil {
			problems = append(problems, "panel.tunnel needs cluster: the devices are kept in the cluster's state")
		}
		if t.Port < 1 || t.Port > 65535 {
			problems = append(problems, "panel.tunnel.port must be a UDP port")
		}
		if !strings.HasPrefix(t.KeyFile, "/") {
			problems = append(problems, "panel.tunnel.keyFile must be an absolute path")
		}
		if a.Edge.TLS.Mode != edge.TLSInternal && a.Edge.TLS.Mode != edge.TLSACME {
			problems = append(problems, "panel.tunnel needs edge.tls (internal or acme): passkeys need HTTPS, with the wildcard certificate")
		}
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
		if a.Volumes.Parent == "" && !shared || !strings.HasPrefix(a.Volumes.MountRoot, "/") {
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
	if a.HostFirewall != nil {
		if a.Firewall != FirewallNftables {
			problems = append(problems, "hostFirewall needs firewall: nftables, whose table it is part of")
		} else if _, err := a.HostRules(); err != nil {
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
	} else if a.Cluster != nil && a.Cluster.Port() == a.Edge.RelayPort {
		problems = append(problems, "edge.relayPort must differ from the cluster address's port")
	}
	if a.Edge.Drain.Duration < 0 {
		problems = append(problems, "edge.drain must not be negative")
	}
	if a.Edge.RoutesHold.Duration < 0 || a.Edge.RoutesHold.Duration > time.Minute {
		problems = append(problems, "edge.routesHold must be 0-60s")
	}
	for _, problem := range a.Defaults.Problems() {
		problems = append(problems, "defaults: "+problem)
	}
	if a.ReconcileInterval.Duration < time.Second {
		problems = append(problems, "reconcileInterval must be at least 1s")
	}
	if a.ImageChecks.Duration != 0 && a.ImageChecks.Duration < time.Minute {
		problems = append(problems, "imageChecks must be at least 1m, or 0 to ask registries never")
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

// Port is the cluster address's port: ClusterPort when the address is
// left out, to be found on the host; 0 if it is not an address.
func (c Cluster) Port() int {
	if c.Address == "" {
		return ClusterPort
	}
	_, port, err := net.SplitHostPort(c.Address)
	number, portErr := strconv.Atoi(port)
	if err != nil || portErr != nil {
		return 0
	}
	return number
}

func (c Cluster) problems(shared bool) []string {
	var problems []string
	host, port, err := net.SplitHostPort(c.Address)
	address, parseErr := netip.ParseAddr(host)
	number, portErr := strconv.Atoi(port)
	if c.Address == "" && shared {
		// Detect finds it.
	} else if err != nil || parseErr != nil || address.IsUnspecified() || portErr != nil || number < 1 || number > 65535 {
		problems = append(problems, "cluster.address must be the node's IP address on the private network and a port, such as 10.0.0.2:7400")
	} else if !address.IsPrivate() && !address.IsLoopback() && !hostprobe.CGNAT.Contains(address) {
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
	problems = append(problems, publicProblems("cluster.publicAddresses", c.PublicAddresses)...)
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
		if len(c.PublicAddresses) == 0 && !(shared && c.PublicAddresses == nil) {
			problems = append(problems, "cluster.dns needs cluster.publicAddresses: without them the records never name this node")
		}
	}
	return problems
}

// publicProblems are what is wrong with the public addresses values, the
// setting named key.
func publicProblems(key string, values []string) []string {
	var problems []string
	for _, value := range values {
		if address, err := netip.ParseAddr(value); err != nil || address.Zone() != "" {
			problems = append(problems, fmt.Sprintf("%s: %q is not an IP address", key, value))
		} else if address.IsPrivate() || address.IsLoopback() || address.IsUnspecified() || address.IsLinkLocalUnicast() || address.IsMulticast() {
			problems = append(problems, fmt.Sprintf("%s: %s is not a public address", key, value))
		}
	}
	return problems
}

// bareKey reports whether the document's top level has key with no value.
func bareKey(data []byte, key string) bool {
	var top map[string]yaml.Node
	if yaml.Unmarshal(data, &top) != nil {
		return false
	}
	node, ok := top[key]
	return ok && node.ShortTag() == "!!null"
}
