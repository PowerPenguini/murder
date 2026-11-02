package tunnel

import (
	"bufio"
	"fmt"
	"net/netip"
	"strconv"
	"strings"
)

// Node is a node's end, as a device's configuration names it.
type Node struct {
	Name string
	Key  string
	// Address is the node's address in the tunnel.
	Address netip.Addr
	// Endpoint is its public address and UDP port.
	Endpoint netip.AddrPort
}

// DeviceConfig is a device's WireGuard configuration: its private key and
// address, and every node, each reached at its own public address.
type DeviceConfig struct {
	PrivateKey string
	Address    netip.Addr
	// Panel is the panel's URL, as a comment for the person.
	Panel string
	Nodes []Node
}

// keepalive keeps a device's way through NAT open, in seconds.
const keepalive = 25

// String writes the configuration as the WireGuard apps read it.
func (c DeviceConfig) String() string {
	var b strings.Builder
	b.WriteString("# Murder panel")
	if c.Panel != "" {
		b.WriteString(": " + c.Panel)
	}
	b.WriteString("\n[Interface]\n")
	fmt.Fprintf(&b, "PrivateKey = %s\nAddress = %s/32\n", c.PrivateKey, c.Address)
	for _, node := range c.Nodes {
		fmt.Fprintf(&b, "\n# %s\n[Peer]\nPublicKey = %s\nAllowedIPs = %s/32\nEndpoint = %s\nPersistentKeepalive = %d\n",
			node.Name, node.Key, node.Address, node.Endpoint, keepalive)
	}
	return b.String()
}

// ParseDeviceConfig reads what String writes.
func ParseDeviceConfig(text string) (DeviceConfig, error) {
	var c DeviceConfig
	var node *Node
	section := ""
	scanner := bufio.NewScanner(strings.NewReader(text))
	for line := 1; scanner.Scan(); line++ {
		row := strings.TrimSpace(scanner.Text())
		switch {
		case row == "" || strings.HasPrefix(row, "#"):
			if panel, ok := strings.CutPrefix(row, "# Murder panel: "); ok {
				c.Panel = panel
			}
			continue
		case row == "[Interface]":
			section = "interface"
			continue
		case row == "[Peer]":
			section = "peer"
			c.Nodes = append(c.Nodes, Node{})
			node = &c.Nodes[len(c.Nodes)-1]
			continue
		}
		key, value, ok := strings.Cut(row, "=")
		if !ok {
			return c, fmt.Errorf("line %d: %q is not key = value", line, row)
		}
		key, value = strings.TrimSpace(key), strings.TrimSpace(value)
		var err error
		switch section + "." + key {
		case "interface.PrivateKey":
			c.PrivateKey = value
		case "interface.Address":
			var prefix netip.Prefix
			prefix, err = netip.ParsePrefix(value)
			c.Address = prefix.Addr()
		case "peer.PublicKey":
			node.Key = value
		case "peer.AllowedIPs":
			var prefix netip.Prefix
			prefix, err = netip.ParsePrefix(value)
			node.Address = prefix.Addr()
		case "peer.Endpoint":
			node.Endpoint, err = netip.ParseAddrPort(value)
		case "peer.PersistentKeepalive":
			_, err = strconv.Atoi(value)
		default:
			// Settings this end does not need, such as DNS.
		}
		if err != nil {
			return c, fmt.Errorf("line %d: %s: %w", line, key, err)
		}
	}
	if c.PrivateKey == "" || !c.Address.IsValid() || len(c.Nodes) == 0 {
		return c, fmt.Errorf("a device's configuration needs its private key, its address and a node")
	}
	return c, nil
}

// Peers are the nodes, as a device's end of the tunnel talks to them.
func (c DeviceConfig) Peers() []Peer {
	peers := make([]Peer, len(c.Nodes))
	for i, node := range c.Nodes {
		peers[i] = Peer{Key: node.Key, Allowed: netip.PrefixFrom(node.Address, 32), Endpoint: node.Endpoint, Keepalive: keepalive}
	}
	return peers
}
