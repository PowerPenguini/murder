package setup

import (
	"bytes"
	"fmt"
	"net/netip"
	"path"
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
// the host's own: its name, addresses, data and place in the panel's
// tunnel; everything else stays as the cluster has it. It returns what
// to install.
func Adopt(bundle cluster.Bundle, own Own) (Install, config.Agent, error) {
	var doc yaml.Node
	if err := yaml.Unmarshal(bundle.Config, &doc); err != nil || doc.Kind != yaml.DocumentNode || len(doc.Content) != 1 || doc.Content[0].Kind != yaml.MappingNode {
		return Install{}, config.Agent{}, fmt.Errorf("the cluster's config is not a YAML mapping: %v", err)
	}
	root := doc.Content[0]
	set(root, scalar(own.Node), "node")
	set(root, scalar(own.ClusterAddress), "cluster", "address")
	if len(own.Public) > 0 {
		set(root, sequence(own.Public), "cluster", "publicAddresses")
	} else {
		remove(root, "cluster", "publicAddresses")
	}
	volumes := &yaml.Node{Kind: yaml.MappingNode}
	if own.ZFSPool != "" {
		set(volumes, scalar("zfs"), "driver")
		set(volumes, scalar(own.ZFSPool+"/murder/tenants"), "parent")
	} else {
		set(volumes, scalar("dir"), "driver")
	}
	set(root, volumes, "volumes")
	if bundle.TunnelAddress != "" {
		set(root, scalar(bundle.TunnelAddress), "panel", "tunnel", "address")
	}
	var out bytes.Buffer
	encoder := yaml.NewEncoder(&out)
	encoder.SetIndent(2)
	if err := encoder.Encode(&doc); err != nil {
		return Install{}, config.Agent{}, err
	}
	agent, err := config.Parse(out.Bytes())
	if err != nil {
		return Install{}, config.Agent{}, fmt.Errorf("the cluster's config, made this host's: %w", err)
	}
	if agent.Cluster == nil {
		return Install{}, config.Agent{}, fmt.Errorf("the cluster's config has no cluster section")
	}
	in := Install{Config: out.Bytes(), Secrets: map[string][]byte{}, ZFSPool: own.ZFSPool, Panel: agent.Panel.Tunnel != nil}
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
	return in, agent, nil
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
