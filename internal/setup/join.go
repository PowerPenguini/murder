package setup

import (
	"bytes"
	"crypto/ed25519"
	"fmt"
	"net/netip"
	"path"
	"slices"
	"strconv"
	"strings"

	"gopkg.in/yaml.v3"

	"github.com/PowerPenguini/murder/internal/cluster"
	"github.com/PowerPenguini/murder/internal/config"
)

// Own are a joining host's own settings; everything else it takes from
// the cluster.
type Own struct {
	Node           string
	ZFSPool        string // "" for plain directories
	ClusterAddress string
	Public         []string
	// Identity is the host's identity key as a node, made before it
	// redeems the invitation, which admits its public half.
	Identity ed25519.PrivateKey
	// SSHPorts are the ports this host's sshd listens on, and Open and
	// Private what else listened on it: a host firewall the cluster has
	// opens them here too, so joining locks SSH out on no port and shuts
	// off nothing that served.
	SSHPorts      []int
	Open, Private []Listener
	// Host answers what the agent will find on the host by itself, which
	// node.yaml need not name; nil finds nothing.
	Host config.Host
}

// SettleOwn makes a joining host's own choices: from the options, then
// from the facts. peer is the node the invitation names; the host's
// cluster address is on its network.
func SettleOwn(opts Options, facts Facts, peer netip.AddrPort) (Own, []string) {
	var problems []string
	own := Own{Node: opts.Node, Public: opts.Public}
	if own.Node == "" {
		own.Node, _, _ = strings.Cut(facts.Hostname, ".")
	}
	if !nodePattern.MatchString(own.Node) {
		problems = append(problems, fmt.Sprintf("node name %q: give one with -node (letters, digits, dots, hyphens)", own.Node))
	}
	pool, problem := choosePool(opts.ZFSPool, facts)
	own.ZFSPool = pool
	if problem != "" {
		problems = append(problems, problem)
	}
	switch address := opts.ClusterAddress; {
	case address == "none":
		problems = append(problems, "a node of a cluster needs a cluster address")
	case address != "":
		host, port, err := splitAddress(address)
		if err != nil {
			problems = append(problems, fmt.Sprintf("-cluster-address %q: %v", address, err))
		} else {
			own.ClusterAddress = netip.AddrPortFrom(host, port).String()
		}
	default:
		if host, ok := nearest(facts.Private, peer.Addr()); ok {
			own.ClusterAddress = netip.AddrPortFrom(host, peer.Port()).String()
		} else {
			problems = append(problems, fmt.Sprintf("the host has no private address on %s's network: name its cluster address with -cluster-address", peer.Addr()))
		}
	}
	if len(own.Public) == 0 {
		for _, address := range facts.Public {
			own.Public = append(own.Public, address.String())
		}
	}
	own.SSHPorts = facts.SSHPorts
	own.Open, own.Private = splitListeners(facts.Listeners)
	own.Host = factsHost{facts}
	return own, problems
}

// nearest is the address of addresses sharing the longest prefix with
// peer, at least a /16.
func nearest(addresses []netip.Addr, peer netip.Addr) (netip.Addr, bool) {
	var best netip.Addr
	bestBits := 15
	for _, address := range addresses {
		bits := 0
		for bits < 32 {
			prefix, _ := address.Prefix(bits + 1)
			if !prefix.Contains(peer) {
				break
			}
			bits++
		}
		if bits > bestBits {
			best, bestBits = address, bits
		}
	}
	return best, best.IsValid()
}

// choosePool picks the ZFS pool for tenants' data, or none.
func choosePool(asked string, facts Facts) (string, string) {
	switch {
	case asked == "none":
		return "", ""
	case asked != "":
		for _, pool := range facts.Pools {
			if pool == asked {
				return asked, ""
			}
		}
		return "", fmt.Sprintf("there is no ZFS pool %q (pools: %s)", asked, listOr(facts.Pools, "none"))
	case len(facts.Pools) == 1:
		return facts.Pools[0], ""
	case len(facts.Pools) > 1:
		return "", fmt.Sprintf("there are several ZFS pools (%s): name one with -zfs-pool", strings.Join(facts.Pools, ", "))
	case !facts.ZFS:
		return "", "ZFS is missing: `apt install zfsutils-linux`, then create a pool, say `zpool create tank /dev/sdb`; or use -zfs-pool none for plain directories, without disk limits or copies on other nodes"
	default:
		return "", "there is no ZFS pool: create one, say `zpool create tank /dev/sdb`; or use -zfs-pool none for plain directories, without disk limits or copies on other nodes"
	}
}

// Adopt makes the cluster's config, from the node that invited the host,
// the host's own. The config itself it keeps byte for byte, so every node
// of the cluster has the same; what is the host's own (its name,
// addresses, data, place in the panel's tunnel, and the listeners and
// ports the cluster's config binds or opens on the inviting node alone)
// goes in node.yaml, where the agent would not find it by itself. It
// returns what to install.
func Adopt(bundle cluster.Bundle, own Own) (Install, config.Agent, error) {
	var doc, work yaml.Node
	if err := yaml.Unmarshal(bundle.Config, &doc); err != nil || doc.Kind != yaml.DocumentNode || len(doc.Content) != 1 || doc.Content[0].Kind != yaml.MappingNode {
		return Install{}, config.Agent{}, fmt.Errorf("the cluster's config is not a YAML mapping: %v", err)
	}
	if err := yaml.Unmarshal(bundle.Config, &work); err != nil {
		return Install{}, config.Agent{}, err
	}
	root, changed := doc.Content[0], work.Content[0]
	theirs, err := config.Parse(bundle.Config)
	if err != nil {
		return Install{}, config.Agent{}, fmt.Errorf("the cluster's config: %w", err)
	}
	// Listeners the inviting node bound to an address of its own bind to
	// this host's same address. A cluster's config that leaves the
	// addresses out has the inviting node's in its node.yaml, not here.
	moved := map[string]string{}
	if theirs.Cluster != nil {
		if address, err := netip.ParseAddrPort(theirs.Cluster.Address); err == nil {
			if ours, err := netip.ParseAddrPort(own.ClusterAddress); err == nil {
				moved[address.Addr().String()] = ours.Addr().String()
			}
		}
		for i, address := range theirs.Cluster.PublicAddresses {
			if i < len(own.Public) {
				moved[address] = own.Public[i]
			}
		}
	}
	listeners := [][]string{{"edge", "http"}, {"edge", "https"}, {"edge", "metrics"}, {"metrics"}}
	for _, keys := range listeners {
		value := get(changed, keys...)
		if value == nil || value.Kind != yaml.ScalarNode {
			continue
		}
		host, port, ok := strings.Cut(value.Value, ":")
		if ours, mine := moved[host]; ok && mine {
			set(changed, scalar(ours+":"+port), keys...)
		}
	}
	keepOwnPorts(changed, own)
	over := &yaml.Node{Kind: yaml.MappingNode}
	for _, keys := range append(listeners, []string{"hostFirewall", "ssh"}, []string{"hostFirewall", "open"}, []string{"hostFirewall", "private"}) {
		if value := get(changed, keys...); value != nil && !sameNode(value, get(root, keys...)) {
			set(over, value, keys...)
		}
	}
	values := ownValues{Node: own.Node, ClusterAddress: own.ClusterAddress, Public: own.Public, ZFSPool: own.ZFSPool}
	// The address the inviting node handed out, where the cluster's config
	// names the inviting node's; else the panel takes one from the state.
	if t := theirs.Panel.Tunnel; t != nil && t.Address != "" {
		values.TunnelAddress = bundle.TunnelAddress
	}
	host := own.Host
	if host == nil {
		host = nowhere{}
	}
	nodeData, agent, err := nodeConfig(bundle.Config, over, values, host)
	if err != nil {
		return Install{}, config.Agent{}, fmt.Errorf("the cluster's config, made this host's: %w", err)
	}
	if agent.Cluster == nil {
		return Install{}, config.Agent{}, fmt.Errorf("the cluster's config has no cluster section")
	}
	in := Install{Config: bundle.Config, NodeConfig: nodeData, Secrets: map[string][]byte{}, ZFSPool: own.ZFSPool, Panel: agent.Panel.Tunnel != nil}
	// Files land where the cluster keeps them, beside the config only.
	for file, data := range bundle.Files {
		if path.Dir(file) != ConfigDir {
			return Install{}, config.Agent{}, fmt.Errorf("the cluster sent %s, which is not in %s", file, ConfigDir)
		}
		in.Secrets[file] = data
	}
	if path.Dir(agent.MasterKeyFile) != ConfigDir || len(bundle.Key) == 0 {
		return Install{}, config.Agent{}, fmt.Errorf("the cluster's master key %s is not in %s", agent.MasterKeyFile, ConfigDir)
	}
	in.Secrets[agent.MasterKeyFile] = bundle.Key
	// The identity key the invitation admitted, where the agent looks for
	// it: without it, the node would make another, which no node accepts.
	if len(own.Identity) != ed25519.PrivateKeySize || !path.IsAbs(agent.Cluster.Dir) {
		return Install{}, config.Agent{}, fmt.Errorf("the host has no identity key, or the cluster's config no absolute cluster.dir (%q)", agent.Cluster.Dir)
	}
	identity, err := cluster.EncodeIdentity(own.Identity)
	if err != nil {
		return Install{}, config.Agent{}, err
	}
	in.Secrets[path.Join(agent.Cluster.Dir, cluster.IdentityFile)] = identity
	return in, agent, nil
}

// keepOwnPorts opens, in the cluster's host firewall, this host's SSH
// ports and what else listened on it, beside the cluster's own entries:
// the inviting node's sshd may listen elsewhere, and what the cluster's
// config opens was found on that node, not on this host. A cluster
// without a host firewall gets none.
func keepOwnPorts(root *yaml.Node, own Own) {
	firewall := get(root, "hostFirewall")
	switch {
	case firewall == nil:
		return
	case firewall.Kind == yaml.ScalarNode && firewall.ShortTag() == "!!null":
		// A bare "hostFirewall:" turns it on with every default; it
		// becomes a mapping once there is something to add.
		bare := &yaml.Node{Kind: yaml.MappingNode}
		defer func() {
			if len(bare.Content) > 0 {
				set(root, bare, "hostFirewall")
			}
		}()
		firewall = bare
	case firewall.Kind != yaml.MappingNode:
		return // config.Parse says what is wrong with it
	}
	// SSH: left out, the agent finds this host's ports at every start.
	// Named, the host's ports go beside the cluster's, which keep their
	// layout and comments.
	if list := get(firewall, "ssh"); list != nil && list.Kind == yaml.SequenceNode {
		var ssh []int
		for _, item := range list.Content {
			var port int
			if item.Decode(&port) == nil { // 22, but also 0x16 or 2_2, as config.Parse reads them
				ssh = append(ssh, port)
			}
		}
		for _, port := range own.SSHPorts {
			if !slices.Contains(ssh, port) {
				list.Content = append(list.Content, scalar(strconv.Itoa(port)))
				ssh = append(ssh, port)
			}
		}
	}
	entries := func(key string) []string {
		var out []string
		if list := get(firewall, key); list != nil && list.Kind == yaml.SequenceNode {
			for _, item := range list.Content {
				out = append(out, item.Value)
			}
		}
		return out
	}
	add := func(key string, listeners []Listener, covered []string) {
		list := get(firewall, key)
		for _, l := range listeners {
			if slices.Contains(covered, l.Entry()) {
				continue
			}
			if list == nil || list.Kind != yaml.SequenceNode {
				list = &yaml.Node{Kind: yaml.SequenceNode}
				set(firewall, list, key)
			}
			item := scalar(l.Entry())
			if l.Process != "" {
				item.LineComment = "# " + l.Process + " on " + own.Node
			}
			list.Content = append(list.Content, item)
			covered = append(covered, l.Entry())
		}
	}
	add("open", own.Open, entries("open"))
	// What is open to everyone needs no opening to private networks.
	add("private", own.Private, append(entries("open"), entries("private")...))
}

// sameNode reports whether a and b are the same YAML, comments aside.
func sameNode(a, b *yaml.Node) bool {
	if a == nil || b == nil {
		return a == b
	}
	encode := func(n *yaml.Node) string {
		var out bytes.Buffer
		encoder := yaml.NewEncoder(&out)
		_ = encoder.Encode(n)
		return out.String()
	}
	return encode(a) == encode(b)
}

func scalar(value string) *yaml.Node { return &yaml.Node{Kind: yaml.ScalarNode, Value: value} }

func sequence(values []string) *yaml.Node {
	node := &yaml.Node{Kind: yaml.SequenceNode, Style: yaml.FlowStyle}
	for _, value := range values {
		node.Content = append(node.Content, scalar(value))
	}
	return node
}

// set puts value at keys in mapping, making the mappings on the way and
// replacing what is not one.
func set(mapping *yaml.Node, value *yaml.Node, keys ...string) {
	for i, key := range keys {
		index := -1
		for j := 0; j+1 < len(mapping.Content); j += 2 {
			if mapping.Content[j].Value == key {
				index = j + 1
				break
			}
		}
		if i == len(keys)-1 {
			if index >= 0 {
				value.LineComment = mapping.Content[index].LineComment
				mapping.Content[index] = value
			} else {
				mapping.Content = append(mapping.Content, scalar(key), value)
			}
			return
		}
		if index >= 0 && mapping.Content[index].Kind == yaml.MappingNode {
			mapping = mapping.Content[index]
			continue
		}
		next := &yaml.Node{Kind: yaml.MappingNode}
		if index >= 0 {
			mapping.Content[index] = next
		} else {
			mapping.Content = append(mapping.Content, scalar(key), next)
		}
		mapping = next
	}
}

// get is the value at keys in mapping, or nil.
func get(mapping *yaml.Node, keys ...string) *yaml.Node {
	for _, key := range keys {
		var next *yaml.Node
		for j := 0; mapping != nil && mapping.Kind == yaml.MappingNode && j+1 < len(mapping.Content); j += 2 {
			if mapping.Content[j].Value == key {
				next = mapping.Content[j+1]
				break
			}
		}
		if next == nil {
			return nil
		}
		mapping = next
	}
	return mapping
}

// remove deletes keys from mapping, if they are there.
func remove(mapping *yaml.Node, keys ...string) {
	for i, key := range keys {
		for j := 0; j+1 < len(mapping.Content); j += 2 {
			if mapping.Content[j].Value != key {
				continue
			}
			if i == len(keys)-1 {
				mapping.Content = append(mapping.Content[:j], mapping.Content[j+2:]...)
				return
			}
			mapping = mapping.Content[j+1]
			break
		}
	}
}
