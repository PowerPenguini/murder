package config

import (
	"errors"
	"net/netip"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"testing"
)

// fakeHost is what detection finds on a host.
type fakeHost struct {
	name      string
	addresses []string
	pools     []string
	noZFS     bool
	ssh       []int
}

func (h fakeHost) Hostname() (string, error) {
	if h.name == "" {
		return "", errors.New("no name")
	}
	return h.name, nil
}

func (h fakeHost) Addresses() ([]netip.Addr, error) {
	var out []netip.Addr
	for _, a := range h.addresses {
		out = append(out, netip.MustParseAddr(a))
	}
	return out, nil
}

func (h fakeHost) Pools() ([]string, error) {
	if h.noZFS {
		return nil, errors.New("ZFS is missing")
	}
	return h.pools, nil
}

func (h fakeHost) SSHPorts() []int { return h.ssh }

// shared names nothing of a node's own: every node of a cluster can have
// it.
const shared = `baseDomain: example.com
volumes: {driver: zfs}
edge: {tls: {mode: internal}}
hostFirewall:
cluster: {}
panel: {tunnel: {}}
`

func TestASharedConfigIsCompletedFromTheHost(t *testing.T) {
	// Parse alone takes it, leaving the node's own settings out.
	agent, err := Parse([]byte(shared))
	if err != nil {
		t.Fatal(err)
	}
	if agent.Node != "" || agent.Cluster.Address != "" || agent.Volumes.Parent != "" || agent.HostFirewall.SSH != nil || agent.Validate() == nil {
		t.Fatalf("parsed %+v", agent)
	}
	host := fakeHost{name: "fsn-1.example.com", addresses: []string{"10.0.0.2", "203.0.113.10", "2001:db8::1", "127.0.0.1", "fe80::1"}, pools: []string{"tank"}, ssh: []int{22, 2222}}
	agent, err = ParseOn([]byte(shared), nil, host)
	if err != nil {
		t.Fatal(err)
	}
	if agent.Node != "fsn-1" || agent.Cluster.Address != "10.0.0.2:7400" || !slices.Equal(agent.Cluster.PublicAddresses, []string{"203.0.113.10"}) ||
		agent.Volumes.Parent != "tank/murder/tenants" || !slices.Equal(agent.HostFirewall.SSH, []int{22, 2222}) || agent.Panel.Tunnel.Address != "" {
		t.Fatalf("detected %+v", agent)
	}
	var keys []string
	for _, d := range agent.Detected {
		keys = append(keys, d.Key)
	}
	if want := []string{"node", "cluster.address", "cluster.publicAddresses", "volumes.parent", "hostFirewall.ssh", "panel.tunnel.address"}; !slices.Equal(keys, want) {
		t.Fatalf("detected %v, want %v", keys, want)
	}
	if !agent.IsDetected("node") || agent.RelayAddress() != "10.0.0.2:7402" {
		t.Fatalf("relay %s", agent.RelayAddress())
	}
	rules, err := agent.HostRules()
	if err != nil || !slices.Contains(rules.PublicTCP, 2222) || !slices.Contains(rules.PrivateTCP, 7400) {
		t.Fatalf("rules %+v, %v", rules, err)
	}
}

// What is set wins over what the host has, and a node alone finds its
// public addresses at the top level.
func TestWhatIsSetIsKept(t *testing.T) {
	host := fakeHost{name: "fsn-1", addresses: []string{"10.0.0.2", "203.0.113.10"}, pools: []string{"tank"}, ssh: []int{22}}
	agent, err := ParseOn([]byte(`baseDomain: example.com
node: n7
volumes: {driver: zfs, parent: tank/elsewhere}
hostFirewall: {ssh: [2200]}
cluster: {address: 10.0.0.9:7500, publicAddresses: []}
`), nil, host)
	if err != nil {
		t.Fatal(err)
	}
	if agent.Node != "n7" || agent.Cluster.Address != "10.0.0.9:7500" || len(agent.Cluster.PublicAddresses) != 0 || agent.Volumes.Parent != "tank/elsewhere" || !slices.Equal(agent.HostFirewall.SSH, []int{2200}) || len(agent.Detected) != 0 {
		t.Fatalf("agent %+v, detected %+v", agent, agent.Detected)
	}
	alone, err := ParseOn([]byte("baseDomain: example.com\n"), nil, host)
	if err != nil || !slices.Equal(alone.PublicAddresses, []string{"203.0.113.10"}) || alone.Cluster != nil {
		t.Fatalf("alone %+v, %v", alone, err)
	}
}

// What the host cannot tell is refused, with where to say it.
func TestWhatTheHostCannotTellIsNamed(t *testing.T) {
	for name, c := range map[string]struct {
		host fakeHost
		want string
	}{
		"several private": {fakeHost{name: "n", addresses: []string{"10.0.0.2", "192.168.1.2"}, pools: []string{"tank"}}, "several private addresses (10.0.0.2, 192.168.1.2)"},
		"no private":      {fakeHost{name: "n", addresses: []string{"203.0.113.10"}, pools: []string{"tank"}}, "no private address"},
		"several pools":   {fakeHost{name: "n", addresses: []string{"10.0.0.2"}, pools: []string{"tank", "fast"}}, "several ZFS pools (tank, fast)"},
		"no pool":         {fakeHost{name: "n", addresses: []string{"10.0.0.2"}}, "no ZFS pool"},
		"no zfs":          {fakeHost{name: "n", addresses: []string{"10.0.0.2"}, noZFS: true}, "ZFS is missing"},
		"no name":         {fakeHost{addresses: []string{"10.0.0.2"}, pools: []string{"tank"}}, "node:"},
	} {
		_, err := ParseOn([]byte(shared), nil, c.host)
		if err == nil || !strings.Contains(err.Error(), c.want) || !strings.Contains(err.Error(), NodeFileName) {
			t.Errorf("%s: %v", name, err)
		}
	}
}

// node.yaml beside the config is read over it: a mapping is merged key by
// key, anything else replaced.
func TestNodeFileIsReadOverTheConfig(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "agent.yaml")
	if err := os.WriteFile(path, []byte("# shared\n"+shared+"metrics: 127.0.0.1:9464\n"), 0o644); err != nil {
		t.Fatal(err)
	}
	host := fakeHost{name: "fsn-1", addresses: []string{"10.0.0.2", "192.168.1.2"}, pools: []string{"tank", "fast"}, ssh: []int{22}}
	if _, err := LoadOn(path, host); err == nil {
		t.Fatal("a host with two private addresses and two pools was taken without node.yaml")
	}
	own := "cluster: {address: 192.168.1.2:7400}\nvolumes: {parent: fast/murder/tenants}\nhostFirewall: {open: [9100/tcp]}\n"
	if err := os.WriteFile(NodeFile(path), []byte(own), 0o644); err != nil {
		t.Fatal(err)
	}
	agent, err := LoadOn(path, host)
	if err != nil {
		t.Fatal(err)
	}
	if agent.Cluster.Address != "192.168.1.2:7400" || agent.Volumes.Driver != "zfs" || agent.Volumes.Parent != "fast/murder/tenants" || agent.Metrics != "127.0.0.1:9464" ||
		!slices.Equal(agent.HostFirewall.Open, []string{"9100/tcp"}) || !slices.Equal(agent.HostFirewall.SSH, []int{22}) || agent.Node != "fsn-1" {
		t.Fatalf("agent %+v", agent)
	}
	// Its mistakes are named as its own.
	if err := os.WriteFile(NodeFile(path), []byte("clster: {address: 192.168.1.2:7400}\n"), 0o644); err != nil {
		t.Fatal(err)
	}
	if _, err := LoadOn(path, host); err == nil || !strings.Contains(err.Error(), NodeFileName) {
		t.Fatalf("a typo in node.yaml: %v", err)
	}
	// Lists are replaced, not added to.
	merged, err := Merge([]byte("publicAddresses: [203.0.113.1, 203.0.113.2]\nedge: {http: \":80\", https: \":443\"}\n"), []byte("publicAddresses: [203.0.113.3]\nedge: {https: \"10.0.0.2:443\"}\n"))
	if err != nil || string(merged) != "publicAddresses: [203.0.113.3]\nedge: {http: \":80\", https: \"10.0.0.2:443\"}\n" {
		t.Fatalf("merged %q, %v", merged, err)
	}
}

// Derivable names what a config sets that the host gives the same.
func TestDerivableSettings(t *testing.T) {
	host := fakeHost{name: "fsn-1", addresses: []string{"10.0.0.2", "203.0.113.10"}, pools: []string{"tank"}, ssh: []int{22}}
	agent, err := ParseOn([]byte(`baseDomain: example.com
node: fsn-1
volumes: {driver: zfs, parent: tank/murder/tenants}
hostFirewall: {ssh: [22, 2222]}
cluster: {address: 10.0.0.2:7500, publicAddresses: [203.0.113.10]}
`), nil, host)
	if err != nil {
		t.Fatal(err)
	}
	if got, want := agent.Derivable(host), []string{"node", "cluster.publicAddresses", "volumes.parent"}; !slices.Equal(got, want) {
		t.Fatalf("derivable %v, want %v", got, want)
	}
}
