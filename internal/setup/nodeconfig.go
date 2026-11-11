package setup

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"net/netip"
	"slices"
	"strings"

	"gopkg.in/yaml.v3"

	"github.com/PowerPenguini/murder/internal/config"
	"github.com/PowerPenguini/murder/internal/hostprobe"
)

// ownValues are a node's own settings, as setup chose them.
type ownValues struct {
	Node           string
	ClusterAddress string // "" without a cluster
	Public         []string
	ZFSPool        string // "" for plain directories
	// TunnelAddress is the node's address in the panel's tunnel; "" to
	// have the panel take one from the cluster's state.
	TunnelAddress string
}

// nodeConfig is node.yaml for a node whose shared config is shared: over,
// the settings that must differ from the shared ones whatever the host
// has (nil for none), and then each of own's that the agent would not find
// on host by itself. It returns nil for node.yaml when nothing is left for
// it, and the config the agent will load.
//
// What is written is only what detection would get wrong, so a node whose
// host is plain (one private address, one ZFS pool) needs no node.yaml,
// and one that has one names only what sets it apart.
func nodeConfig(shared []byte, over *yaml.Node, own ownValues, host config.Host) ([]byte, config.Agent, error) {
	node := over
	if node == nil {
		node = &yaml.Node{Kind: yaml.MappingNode}
	}
	encode := func() ([]byte, error) {
		if len(node.Content) == 0 {
			return nil, nil
		}
		var out bytes.Buffer
		out.WriteString("# " + NodeFile + ": this node's own settings, read over " + ConfigFile + ",\n# which every node of the cluster may share. Only what the agent would\n# not find on the host by itself is here.\n")
		encoder := yaml.NewEncoder(&out)
		encoder.SetIndent(2)
		if err := encoder.Encode(node); err != nil {
			return nil, err
		}
		return out.Bytes(), nil
	}
	first, err := encode()
	if err != nil {
		return nil, config.Agent{}, err
	}
	merged, err := config.Merge(shared, first)
	if err != nil {
		return nil, config.Agent{}, err
	}
	agent, err := config.Parse(merged)
	if err != nil {
		return nil, config.Agent{}, err
	}
	// What detection cannot find it leaves empty, which then differs from
	// what was chosen: that goes in node.yaml.
	_ = agent.Detect(host)

	if agent.Node != own.Node {
		set(node, scalar(own.Node), "node")
	}
	if own.ClusterAddress != "" && (agent.Cluster == nil || agent.Cluster.Address != own.ClusterAddress) {
		set(node, scalar(own.ClusterAddress), "cluster", "address")
	}
	public, publicKeys := agent.PublicAddresses, []string{"publicAddresses"}
	if agent.Cluster != nil {
		public, publicKeys = agent.Cluster.PublicAddresses, []string{"cluster", "publicAddresses"}
	}
	if public == nil || !slices.Equal(public, own.Public) {
		set(node, sequence(own.Public), publicKeys...)
	}
	driver, parent := "dir", ""
	if own.ZFSPool != "" {
		driver, parent = "zfs", own.ZFSPool+"/murder/tenants"
	}
	if agent.Volumes.Driver != driver {
		set(node, scalar(driver), "volumes", "driver")
	}
	if parent != "" && agent.Volumes.Parent != parent {
		set(node, scalar(parent), "volumes", "parent")
	}
	if t := agent.Panel.Tunnel; t != nil && t.Address != own.TunnelAddress {
		set(node, scalar(own.TunnelAddress), "panel", "tunnel", "address")
	}

	data, err := encode()
	if err != nil {
		return nil, config.Agent{}, err
	}
	agent, err = config.ParseOn(shared, data, host)
	if err != nil {
		return nil, config.Agent{}, fmt.Errorf("the config, with this node's own settings: %w", err)
	}
	var wrong []string
	if agent.Node != own.Node {
		wrong = append(wrong, "node "+agent.Node)
	}
	if own.ClusterAddress != "" && (agent.Cluster == nil || agent.Cluster.Address != own.ClusterAddress) {
		wrong = append(wrong, "its cluster address")
	}
	if agent.Volumes.Driver != driver || parent != "" && agent.Volumes.Parent != parent {
		wrong = append(wrong, "its volumes")
	}
	if len(wrong) > 0 {
		return nil, config.Agent{}, fmt.Errorf("the config would give this node %s, not what setup chose: write %s by hand", strings.Join(wrong, ", "), NodeFile)
	}
	return data, agent, nil
}

// factsHost answers detection from what Inspect found: as the agent will
// find it on this host.
type factsHost struct{ facts Facts }

func (h factsHost) Hostname() (string, error) {
	if h.facts.Hostname == "" {
		return "", errors.New("no host name")
	}
	return h.facts.Hostname, nil
}

func (h factsHost) Addresses() ([]netip.Addr, error) {
	return append(slices.Clone(h.facts.Private), h.facts.Public...), nil
}

func (h factsHost) Pools() ([]string, error) {
	if !h.facts.ZFS {
		return nil, hostprobe.ErrNoZFS
	}
	return h.facts.Pools, nil
}

func (h factsHost) SSHPorts() []int { return h.facts.SSHPorts }

// Probe answers detection from host, as the agent will on it.
func Probe(ctx context.Context, host Host) config.Host { return probe{ctx, host} }

type probe struct {
	ctx  context.Context
	host Host
}

func (p probe) Hostname() (string, error)        { return p.host.Hostname() }
func (p probe) Addresses() ([]netip.Addr, error) { return p.host.Addresses() }
func (p probe) Pools() ([]string, error)         { return hostprobe.Pools(p.ctx, p.host) }
func (p probe) SSHPorts() []int                  { return hostprobe.SSHPorts(p.ctx, p.host) }

// nowhere finds nothing: every own setting goes in node.yaml.
type nowhere struct{}

func (nowhere) Hostname() (string, error)        { return "", errors.New("unknown") }
func (nowhere) Addresses() ([]netip.Addr, error) { return nil, errors.New("unknown") }
func (nowhere) Pools() ([]string, error)         { return nil, hostprobe.ErrNoZFS }
func (nowhere) SSHPorts() []int                  { return []int{22} }
