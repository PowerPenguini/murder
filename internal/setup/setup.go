// Package setup installs Murder on a host: it looks at what the host has,
// settles what it was not told, writes the config and the install files,
// and starts the services. It never installs system packages: what is
// missing it names, with the commands that install it.
package setup

import (
	"context"
	"errors"
	"fmt"
	"io/fs"
	"net/netip"
	"regexp"
	"strconv"
	"strings"
)

// Paths a node is installed at.
const (
	Binary     = "/usr/local/bin/murder"
	ConfigDir  = "/etc/murder"
	ConfigFile = "/etc/murder/agent.yaml"
	UnitDir    = "/etc/systemd/system"
	SysctlFile = "/etc/sysctl.d/90-murder.conf"
	// HardeningFile keeps the kernel from taking redirects and source
	// routes, and its addresses, log and BPF from users.
	HardeningFile = "/etc/sysctl.d/91-murder-hardening.conf"
	DockerDropIn  = "/etc/systemd/system/docker.service.d/murder.conf"
	// DNSTokenFile and HetznerTokenFile hold the tokens setup is given.
	DNSTokenFile     = "/etc/murder/dns.token"
	HetznerTokenFile = "/etc/murder/hetzner.token"
)

// ClusterPort is where nodes reach each other on the private network.
const ClusterPort = 7400

// minDockerAPI is the oldest Engine API the agent speaks (Docker 25).
const minDockerAPI = "1.44"

// Host is what setup does on a host, so it can be tried on a fake one.
type Host interface {
	// Run runs a command and returns its standard output.
	Run(ctx context.Context, name string, args ...string) (string, error)
	ReadFile(path string) ([]byte, error)
	WriteFile(path string, data []byte, perm fs.FileMode) error
	Remove(path string) error
	MkdirAll(path string, perm fs.FileMode) error
	Exists(path string) bool
	// Executable is the running binary's path.
	Executable() (string, error)
	Hostname() (string, error)
	// Addresses are the host's unicast IP addresses.
	Addresses() ([]netip.Addr, error)
	Root() bool
	// Invoker is the account that runs setup: the one sudo was run from,
	// or root.
	Invoker() string
	// Connection is the invoker's SSH_CONNECTION ("client port server
	// port"); empty when it did not log in over SSH.
	Connection() string
}

// Facts are what the host has.
type Facts struct {
	Root      bool
	Systemd   bool
	Docker    string // the engine's API version; empty without one
	Nftables  bool
	ZFS       bool
	Pools     []string
	CgroupV2  bool
	Hostname  string
	Private   []netip.Addr
	Public    []netip.Addr
	Installed bool // a config exists already
	// HasKey and HasDatabase report a master key and a database at their
	// default places: a node's, or one that was.
	HasKey, HasDatabase bool
	// Busy are the edge's ports something else listens on.
	Busy []string
	// LiveRestore reports whether Docker keeps containers running while
	// it restarts; Swarm whether it is in swarm mode, which forbids that.
	LiveRestore, Swarm bool
	// SSH is what sshd lets in, and SSHPorts the ports it listens on.
	SSH      SSH
	SSHPorts []int
	// Listeners are the ports the host firewall would close: what
	// listens beyond loopback, other than Murder and sshd.
	Listeners []Listener
}

// DaemonConfig is Docker's own config file.
const DaemonConfig = "/etc/docker/daemon.json"

// Default places of the master key and the database.
const (
	KeyFile      = "/etc/murder/master.key"
	DatabaseFile = "/var/lib/murder/murder.db"
)

// Inspect looks at the host.
func Inspect(ctx context.Context, host Host) Facts {
	facts := Facts{Root: host.Root()}
	_, err := host.Run(ctx, "systemctl", "--version")
	facts.Systemd = err == nil
	if out, err := host.Run(ctx, "docker", "version", "--format", "{{.Server.APIVersion}}"); err == nil {
		facts.Docker = strings.TrimSpace(out)
		if out, err := host.Run(ctx, "docker", "info", "--format", "{{.LiveRestoreEnabled}} {{.Swarm.LocalNodeState}}"); err == nil {
			fields := strings.Fields(out)
			facts.LiveRestore = len(fields) > 0 && fields[0] == "true"
			facts.Swarm = len(fields) > 1 && fields[1] != "inactive" && fields[1] != ""
		}
	}
	_, err = host.Run(ctx, "nft", "--version")
	facts.Nftables = err == nil
	if out, err := host.Run(ctx, "zpool", "list", "-H", "-o", "name"); err == nil {
		facts.ZFS = true
		facts.Pools = strings.Fields(out)
	}
	facts.CgroupV2 = host.Exists("/sys/fs/cgroup/cgroup.controllers")
	facts.Hostname, _ = host.Hostname()
	addresses, _ := host.Addresses()
	for _, address := range addresses {
		switch {
		case !address.Is4() || address.IsLoopback() || address.IsLinkLocalUnicast():
		case address.IsPrivate() || cgnat.Contains(address):
			facts.Private = append(facts.Private, address)
		case address.IsGlobalUnicast():
			facts.Public = append(facts.Public, address)
		}
	}
	facts.SSH = inspectSSH(ctx, host)
	facts.SSHPorts = sshPorts(ctx, host)
	facts.Listeners = listeners(ctx, host, facts.SSHPorts)
	facts.Installed = host.Exists(ConfigFile)
	facts.HasKey, facts.HasDatabase = host.Exists(KeyFile), host.Exists(DatabaseFile)
	// The edge's ports, unless Murder's own edge holds them.
	if out, err := host.Run(ctx, "ss", "-H", "-ltnp", "( sport = :80 or sport = :443 )"); err == nil {
		for _, line := range strings.Split(strings.TrimSpace(out), "\n") {
			if fields := strings.Fields(line); len(fields) >= 4 && !strings.Contains(line, `"murder"`) {
				facts.Busy = append(facts.Busy, fields[3])
			}
		}
	}
	return facts
}

// cgnat is shared address space, private to a provider's network.
var cgnat = netip.MustParsePrefix("100.64.0.0/10")

// Problems names what the host lacks for Murder, with how to get it; none
// means it is ready.
func (f Facts) Problems() []string {
	var problems []string
	if !f.Root {
		problems = append(problems, "run setup as root (sudo murder setup …)")
	}
	if !f.Systemd {
		problems = append(problems, "systemd is needed to run Murder's services")
	}
	switch {
	case f.Docker == "":
		problems = append(problems, "Docker is not running: install Docker Engine 25 or newer (https://docs.docker.com/engine/install/), then `systemctl enable --now docker`")
	case !atLeast(f.Docker, minDockerAPI):
		problems = append(problems, fmt.Sprintf("Docker's API is %s; Murder needs %s (Docker 25) or newer", f.Docker, minDockerAPI))
	}
	if !f.Nftables {
		problems = append(problems, "nftables is missing: `apt install nftables` (the tenant firewall)")
	}
	if len(f.Busy) > 0 {
		problems = append(problems, fmt.Sprintf("something else listens on %s: the edge serves ports 80 and 443 (`ss -ltnp` names it)", strings.Join(f.Busy, ", ")))
	}
	return problems
}

// WantsLiveRestore reports whether setup should turn Docker's
// live-restore on: it is off, and swarm mode does not forbid it.
func (f Facts) WantsLiveRestore() bool { return f.Docker != "" && !f.LiveRestore && !f.Swarm }

// Warnings are what Murder runs without, but lesser.
func (f Facts) Warnings() []string {
	var warnings []string
	if !f.CgroupV2 {
		warnings = append(warnings, "the host does not use cgroup v2: tenants run, but what they use is not measured")
	}
	switch s := f.SSH; {
	case !s.Passwords || s.Harden():
	case s.Leave != "":
		warnings = append(warnings, "SSH takes passwords, which bots guess around the clock; setup leaves it as it is, as "+s.Leave)
	default:
		warnings = append(warnings, "SSH takes passwords, which bots guess around the clock, and "+s.Account+" has no key to log in with instead: setup leaves SSH as it is (add a key to ~"+s.Account+"/.ssh/authorized_keys then murder doctor -fix, or set PasswordAuthentication no in "+SSHDropIn+")")
	}
	if f.Docker != "" && !f.LiveRestore && f.Swarm {
		warnings = append(warnings, "Docker is in swarm mode, which cannot keep containers running while Docker restarts: restarting Docker stops every tenant until the agent starts them again")
	}
	return warnings
}

// atLeast compares two "major.minor" versions.
func atLeast(version, minimum string) bool {
	parse := func(v string) (int, int) {
		major, minor, _ := strings.Cut(v, ".")
		a, _ := strconv.Atoi(major)
		b, _ := strconv.Atoi(minor)
		return a, b
	}
	a, b := parse(version)
	x, y := parse(minimum)
	return a > x || (a == x && b >= y)
}

// Options are what the operator said; empty ones are settled from the
// facts or asked.
type Options struct {
	Domain string
	Node   string
	// TLS is acme, internal or off.
	TLS   string
	Email string
	// DNSProvider is hetzner or namecheap; DNSToken its token, which for
	// Namecheap is "user:apikey".
	DNSProvider string
	DNSToken    string
	// ZFSPool is the pool tenants' datasets go in; "none" keeps them in
	// plain directories.
	ZFSPool string
	// ClusterAddress is this node's address on the private network;
	// "none" sets up no cluster.
	ClusterAddress string
	// Public are the node's public addresses.
	Public []string
	// HetznerToken powers dead nodes off; with the hetzner DNS provider
	// it is the DNS token too.
	HetznerToken string
	// ManageDNS has the leader keep the wildcard records on the nodes
	// that serve.
	ManageDNS bool
	// Force writes a new config over an existing one.
	Force bool
	// KeepSSH leaves sshd's settings as they are.
	KeepSSH bool
	// NoHostFirewall leaves the host's own ports unfiltered.
	NoHostFirewall bool
}

// Settings are what setup installs: every choice made.
type Settings struct {
	Domain, Node   string
	TLS, Email     string
	DNSProvider    string
	DNSToken       string
	ZFSPool        string // "" for plain directories
	ClusterAddress string // "" without a cluster
	Public         []string
	HetznerToken   string
	Failover       bool
	ManageDNS      bool
	Panel          bool
	TunnelAddress  string
	// HostFirewall drops what reaches the host besides SSH on SSHPorts,
	// Murder's ports and Open (to everyone) and Private (to private
	// networks), which list what listened when setup ran.
	HostFirewall  bool
	SSHPorts      []int
	Open, Private []Listener
}

var (
	domainPattern = regexp.MustCompile(`^([a-z0-9]([a-z0-9-]{0,61}[a-z0-9])?\.)+[a-z]{2,63}$`)
	nodePattern   = regexp.MustCompile(`^[A-Za-z0-9][A-Za-z0-9.-]{0,62}$`)
)

// Settle makes every choice: from the options, then from the facts. It
// returns what it could not settle.
func Settle(opts Options, facts Facts) (Settings, []string) {
	var problems []string
	s := Settings{Domain: strings.ToLower(strings.TrimSpace(opts.Domain)), Email: opts.Email, ManageDNS: opts.ManageDNS}
	if !domainPattern.MatchString(s.Domain) {
		problems = append(problems, fmt.Sprintf("-domain %q is not a domain name: tenants live at <tenant>.<domain>", opts.Domain))
	}
	s.Node = opts.Node
	if s.Node == "" {
		s.Node, _, _ = strings.Cut(facts.Hostname, ".")
	}
	if !nodePattern.MatchString(s.Node) {
		problems = append(problems, fmt.Sprintf("node name %q: give one with -node (letters, digits, dots, hyphens)", s.Node))
	}

	pool, problem := choosePool(opts.ZFSPool, facts)
	s.ZFSPool = pool
	if problem != "" {
		problems = append(problems, problem)
	}

	switch address := opts.ClusterAddress; {
	case address == "none":
	case address != "":
		host, port, err := splitAddress(address)
		if err != nil {
			problems = append(problems, fmt.Sprintf("-cluster-address %q: %v", address, err))
		} else {
			s.ClusterAddress = netip.AddrPortFrom(host, port).String()
		}
	case len(facts.Private) == 1:
		s.ClusterAddress = netip.AddrPortFrom(facts.Private[0], ClusterPort).String()
	case len(facts.Private) > 1:
		problems = append(problems, fmt.Sprintf("the host has several private addresses (%s): name the cluster's with -cluster-address, or -cluster-address none", joinAddrs(facts.Private)))
	}

	s.Public = opts.Public
	if len(s.Public) == 0 {
		for _, address := range facts.Public {
			s.Public = append(s.Public, address.String())
		}
	}
	for _, public := range s.Public {
		if _, err := netip.ParseAddr(public); err != nil {
			problems = append(problems, fmt.Sprintf("public address %q is not an IP address", public))
		}
	}

	s.TLS = opts.TLS
	s.DNSProvider, s.DNSToken = opts.DNSProvider, strings.TrimSpace(opts.DNSToken)
	s.HetznerToken = strings.TrimSpace(opts.HetznerToken)
	if s.DNSProvider == "hetzner" && s.DNSToken == "" {
		s.DNSToken = s.HetznerToken
	}
	if s.TLS == "" {
		s.TLS = "internal"
		if s.DNSProvider != "" && s.DNSToken != "" {
			s.TLS = "acme"
		}
	}
	switch s.TLS {
	case "acme":
		if s.DNSProvider != "hetzner" && s.DNSProvider != "namecheap" {
			problems = append(problems, "-tls acme needs -dns-provider hetzner or namecheap: the certificate is a wildcard, proven through DNS")
		}
		if s.DNSToken == "" {
			problems = append(problems, "-tls acme needs the DNS provider's token (-dns-token-file)")
		}
		if !strings.Contains(s.Email, "@") {
			problems = append(problems, "-tls acme needs -email, for the certificate authority's notices")
		}
	case "internal", "off":
	default:
		problems = append(problems, fmt.Sprintf("-tls %q: acme, internal or off", s.TLS))
	}

	cluster := s.ClusterAddress != ""
	s.Failover = cluster && s.HetznerToken != "" && s.ZFSPool != ""
	if s.ManageDNS {
		switch {
		case !cluster:
			problems = append(problems, "-manage-dns needs a cluster: its leader keeps the records")
		case s.DNSProvider == "" || s.DNSToken == "":
			problems = append(problems, "-manage-dns needs -dns-provider and its token")
		case len(s.Public) == 0:
			problems = append(problems, "-manage-dns needs the node's public address (-public)")
		}
	}
	// The panel serves HTTPS inside its tunnel, whose devices are kept in
	// the cluster's state, and whose endpoint is a public address.
	s.Panel = cluster && s.TLS != "off" && len(s.Public) > 0
	if !opts.NoHostFirewall {
		s.HostFirewall = true
		s.SSHPorts = facts.SSHPorts
		if len(s.SSHPorts) == 0 {
			s.SSHPorts = []int{22}
		}
		for _, l := range facts.Listeners {
			if l.Private {
				s.Private = append(s.Private, l)
			} else {
				s.Open = append(s.Open, l)
			}
		}
	}
	if s.Panel {
		s.TunnelAddress = "10.77.0.1"
	}
	return s, problems
}

func splitAddress(text string) (netip.Addr, uint16, error) {
	if addrPort, err := netip.ParseAddrPort(text); err == nil {
		return addrPort.Addr(), addrPort.Port(), nil
	}
	address, err := netip.ParseAddr(text)
	if err != nil {
		return netip.Addr{}, 0, errors.New("an IP address, with a port if not 7400")
	}
	return address, ClusterPort, nil
}

func listOr(items []string, none string) string {
	if len(items) == 0 {
		return none
	}
	return strings.Join(items, ", ")
}

func joinAddrs(addresses []netip.Addr) string {
	var texts []string
	for _, address := range addresses {
		texts = append(texts, address.String())
	}
	return strings.Join(texts, ", ")
}
