package config

import (
	"bytes"
	"errors"
	"fmt"
	"io"
	"net/netip"
	"os"
	"path/filepath"
	"slices"
	"strconv"
	"strings"

	"gopkg.in/yaml.v3"

	"github.com/PowerPenguini/murder/internal/hostprobe"
	"github.com/PowerPenguini/murder/internal/strictyaml"
)

// A node's own settings: its name, its cluster address, its public
// addresses, the ZFS pool its tenants' data is in, its place in the
// panel's tunnel and its SSH ports. Each may be left out of the config,
// and is then found at every start: so the nodes of a cluster can share
// one agent.yaml, byte for byte, and a node set up again or moved to
// another host finds its own. What cannot be found safely, such as which
// of several private addresses the other nodes reach, goes in node.yaml
// beside the config, which is this node's alone and is read over it.

// NodeFileName is the file beside the config with the node's own
// settings.
const NodeFileName = "node.yaml"

// NodeFile is the node's own file for the config at path.
func NodeFile(path string) string { return filepath.Join(filepath.Dir(path), NodeFileName) }

// ClusterPort is the port of a cluster address found on the host.
const ClusterPort = 7400

// Host is what the node's own settings are found from.
type Host interface {
	// Hostname is the host's name; the node's is its first label.
	Hostname() (string, error)
	// Addresses are the unicast addresses of the host's own interfaces.
	Addresses() ([]netip.Addr, error)
	// Pools are the host's ZFS pools; an error without ZFS.
	Pools() ([]string, error)
	// SSHPorts are the ports sshd listens on.
	SSHPorts() []int
}

// Detected is a setting the config left out, found on the host.
type Detected struct {
	// Key names the setting as the config would: cluster.address.
	Key string
	// Value is what was found, as the config would write it; empty for
	// what is only found later.
	Value string
	// From says where.
	From string
}

// Load reads the config at path, and node.yaml beside it over it, and
// finds on this host what both leave out of the node's own settings.
func Load(path string) (Agent, error) { return LoadOn(path, hostprobe.System{}) }

// LoadOn is Load, finding what is left out on host.
func LoadOn(path string, host Host) (Agent, error) {
	shared, err := os.ReadFile(path)
	if err != nil {
		return Agent{}, err
	}
	own, err := os.ReadFile(NodeFile(path))
	if err != nil && !errors.Is(err, os.ErrNotExist) {
		return Agent{}, err
	}
	return ParseOn(shared, own, host)
}

// ParseOn reads the shared config with the node's own (node.yaml, nil
// without one) over it, finds on host what both leave out, and checks
// the whole.
func ParseOn(shared, own []byte, host Host) (Agent, error) {
	merged, err := Merge(shared, own)
	if err != nil {
		return Agent{}, err
	}
	agent, err := Parse(merged)
	if err != nil {
		return Agent{}, err
	}
	if err := agent.Detect(host); err != nil {
		return Agent{}, err
	}
	return agent, agent.Validate()
}

// Merge is the shared config with own over it: a mapping in both is
// merged key by key, anything else of own's, lists too, replaces the
// shared one's. Comments of the shared config stay.
func Merge(shared, own []byte) ([]byte, error) {
	if len(bytes.TrimSpace(own)) == 0 {
		return shared, nil
	}
	// Mistakes in node.yaml are named as its own, not by a line of the
	// merged document.
	var check Agent
	if err := strictyaml.Decode(own, &check); err != nil && !errors.Is(err, io.EOF) {
		return nil, fmt.Errorf("parse %s: %w", NodeFileName, err)
	}
	var base, over yaml.Node
	if err := yaml.Unmarshal(shared, &base); err != nil {
		return nil, fmt.Errorf("parse config: %w", err)
	}
	if err := yaml.Unmarshal(own, &over); err != nil {
		return nil, fmt.Errorf("parse %s: %w", NodeFileName, err)
	}
	if len(over.Content) == 0 || over.Content[0].ShortTag() == "!!null" {
		return shared, nil
	}
	if over.Content[0].Kind != yaml.MappingNode {
		return nil, fmt.Errorf("%s must be a mapping of settings", NodeFileName)
	}
	if len(base.Content) == 0 || base.Content[0].ShortTag() == "!!null" {
		// An empty shared config: node.yaml is the whole.
		return own, nil
	}
	if base.Content[0].Kind != yaml.MappingNode {
		return nil, errors.New("parse config: the config must be a mapping of settings")
	}
	mergeMappings(base.Content[0], over.Content[0])
	var out bytes.Buffer
	encoder := yaml.NewEncoder(&out)
	encoder.SetIndent(2)
	if err := encoder.Encode(&base); err != nil {
		return nil, err
	}
	return out.Bytes(), nil
}

func mergeMappings(base, over *yaml.Node) {
	for i := 0; i+1 < len(over.Content); i += 2 {
		key, value := over.Content[i], over.Content[i+1]
		found := false
		for j := 0; j+1 < len(base.Content); j += 2 {
			if base.Content[j].Value != key.Value {
				continue
			}
			found = true
			if base.Content[j+1].Kind == yaml.MappingNode && value.Kind == yaml.MappingNode {
				mergeMappings(base.Content[j+1], value)
			} else {
				base.Content[j+1] = value
			}
			break
		}
		if !found {
			base.Content = append(base.Content, key, value)
		}
	}
}

// Detect fills in, from host, the node's own settings the config left out,
// and lists them in Detected. What it cannot find it names, with where to
// set it instead; it fills in the rest all the same.
func (a *Agent) Detect(host Host) error {
	var problems []string
	note := func(key, value, from string) { a.Detected = append(a.Detected, Detected{key, value, from}) }
	if a.Node == "" {
		name, err := host.Hostname()
		short, _, _ := strings.Cut(name, ".")
		if err != nil || short == "" {
			problems = append(problems, fmt.Sprintf("node: the host's name is unknown (%v): name the node in %s", err, NodeFileName))
		} else {
			a.Node = short
			note("node", short, "the host name")
		}
	}
	// The addresses are read once, and only if a setting needs them.
	var private, public []netip.Addr
	var addressErr error
	read := false
	addresses := func() error {
		if !read {
			var all []netip.Addr
			all, addressErr = host.Addresses()
			private, public = hostprobe.Classify(all)
			read = true
		}
		return addressErr
	}
	if c := a.Cluster; c != nil && c.Address == "" {
		switch err := addresses(); {
		case err != nil:
			problems = append(problems, fmt.Sprintf("cluster.address: the host's addresses are unknown (%v): name it in %s", err, NodeFileName))
		case len(private) == 1:
			c.Address = netip.AddrPortFrom(private[0], ClusterPort).String()
			note("cluster.address", c.Address, "the host's only private address")
		case len(private) == 0:
			problems = append(problems, "cluster.address: the host has no private address: name the address other nodes reach it at in "+NodeFileName)
		default:
			problems = append(problems, fmt.Sprintf("cluster.address: the host has several private addresses (%s): name the one other nodes reach it at in %s, such as cluster: {address: %s}", joinAddresses(private), NodeFileName, netip.AddrPortFrom(private[0], ClusterPort)))
		}
	}
	// Left out, and not set empty: "publicAddresses: []" says the node
	// has none clients reach, as behind a load balancer.
	key, target := "publicAddresses", &a.PublicAddresses
	if a.Cluster != nil {
		key, target = "cluster.publicAddresses", &a.Cluster.PublicAddresses
	}
	if *target == nil {
		if err := addresses(); err != nil {
			problems = append(problems, fmt.Sprintf("%s: the host's addresses are unknown (%v): name them in %s", key, err, NodeFileName))
		} else {
			found := []string{}
			for _, address := range public {
				found = append(found, address.String())
			}
			*target = found
			note(key, strings.Join(found, ", "), "the host's public addresses")
		}
	}
	if v := &a.Volumes; v.Driver == "zfs" && v.Parent == "" {
		switch pools, err := host.Pools(); {
		case err != nil:
			problems = append(problems, fmt.Sprintf("volumes.parent: %v: name the dataset in %s, or use driver: dir", err, NodeFileName))
		case len(pools) == 1:
			v.Parent = pools[0] + "/murder/tenants"
			note("volumes.parent", v.Parent, "the host's only ZFS pool")
		case len(pools) == 0:
			problems = append(problems, "volumes.parent: the host has no ZFS pool: create one, or name the dataset in "+NodeFileName)
		default:
			problems = append(problems, fmt.Sprintf("volumes.parent: the host has several ZFS pools (%s): name the dataset in %s, such as volumes: {parent: %s/murder/tenants}", strings.Join(pools, ", "), NodeFileName, pools[0]))
		}
	}
	if h := a.HostFirewall; h != nil && h.SSH == nil {
		h.SSH = host.SSHPorts()
		note("hostFirewall.ssh", joinPorts(h.SSH), "the ports sshd listens on")
	}
	if t := a.Panel.Tunnel; t != nil && t.Address == "" {
		note("panel.tunnel.address", "", "the cluster's state, as the panel starts: the address it has there, or the lowest one free")
	}
	if len(problems) > 0 {
		return fmt.Errorf("invalid config: %s", strings.Join(problems, "; "))
	}
	return nil
}

// IsDetected reports whether the setting key was left out and found.
func (a Agent) IsDetected(key string) bool {
	return slices.ContainsFunc(a.Detected, func(d Detected) bool { return d.Key == key })
}

// Derivable names the node's own settings the config sets that host would
// give the same if they were left out: a config every node shares can do
// without them.
func (a Agent) Derivable(host Host) []string {
	var keys []string
	try := func(key string, clear func(*Agent), same func(Agent) bool) {
		if a.IsDetected(key) {
			return
		}
		probe := a.clone()
		probe.Detected = nil
		clear(&probe)
		if probe.Detect(host) == nil && probe.IsDetected(key) && same(probe) {
			keys = append(keys, key)
		}
	}
	if a.Node != "" {
		try("node", func(p *Agent) { p.Node = "" }, func(p Agent) bool { return p.Node == a.Node })
	}
	if c := a.Cluster; c != nil {
		try("cluster.address", func(p *Agent) { p.Cluster.Address = "" }, func(p Agent) bool { return p.Cluster.Address == c.Address })
		try("cluster.publicAddresses", func(p *Agent) { p.Cluster.PublicAddresses = nil }, func(p Agent) bool {
			return slices.Equal(p.Cluster.PublicAddresses, c.PublicAddresses)
		})
	} else if a.PublicAddresses != nil {
		try("publicAddresses", func(p *Agent) { p.PublicAddresses = nil }, func(p Agent) bool { return slices.Equal(p.PublicAddresses, a.PublicAddresses) })
	}
	if a.Volumes.Driver == "zfs" {
		try("volumes.parent", func(p *Agent) { p.Volumes.Parent = "" }, func(p Agent) bool { return p.Volumes.Parent == a.Volumes.Parent })
	}
	if h := a.HostFirewall; h != nil {
		try("hostFirewall.ssh", func(p *Agent) { p.HostFirewall.SSH = nil }, func(p Agent) bool {
			return slices.Equal(p.HostFirewall.SSH, h.SSH)
		})
	}
	return keys
}

// clone copies a, with its own copies of what Detect changes.
func (a Agent) clone() Agent {
	if a.Cluster != nil {
		c := *a.Cluster
		a.Cluster = &c
	}
	if a.HostFirewall != nil {
		h := *a.HostFirewall
		a.HostFirewall = &h
	}
	if a.Panel.Tunnel != nil {
		t := *a.Panel.Tunnel
		a.Panel.Tunnel = &t
	}
	a.Detected = slices.Clone(a.Detected)
	return a
}

func joinAddresses(addresses []netip.Addr) string {
	var texts []string
	for _, address := range addresses {
		texts = append(texts, address.String())
	}
	return strings.Join(texts, ", ")
}

func joinPorts(ports []int) string {
	var texts []string
	for _, port := range ports {
		texts = append(texts, strconv.Itoa(port))
	}
	return strings.Join(texts, ", ")
}
