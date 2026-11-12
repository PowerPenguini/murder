package config

import (
	"errors"
	"fmt"
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
	sshErr    error
	// record is the record of SSH ports, shared by copies; nil keeps none.
	record *sshRecord
	// open is what the running host firewall opens; openErr when it
	// cannot be read.
	open    []int
	openErr error
	// public is the record of public addresses; nil keeps none.
	public *publicRecord
}

// sshRecord holds ports; none (nil) is nothing recorded, as a missing,
// unreadable, corrupt or empty file.
type sshRecord struct {
	ports  []int
	writes int
	fail   error
}

type publicRecord struct {
	addresses []string
	writes    int
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

func (h fakeHost) SSHPorts() ([]int, error) { return h.ssh, h.sshErr }

func (h fakeHost) RecordedSSHPorts() ([]int, error) {
	if h.record == nil || h.record.ports == nil {
		return nil, errors.New("nothing recorded: corrupt")
	}
	return h.record.ports, nil
}

func (h fakeHost) OpenTCPPorts() ([]int, error) {
	if h.open == nil && h.openErr == nil {
		return nil, errors.New("nft: not found")
	}
	return h.open, h.openErr
}

func (h fakeHost) RecordedPublicAddresses() ([]string, error) {
	if h.public == nil || h.public.addresses == nil {
		return nil, errors.New("nothing recorded")
	}
	return h.public.addresses, nil
}

func (h fakeHost) RecordPublicAddresses(addresses []string) error {
	if h.public != nil {
		h.public.addresses, h.public.writes = slices.Clone(addresses), h.public.writes+1
	}
	return nil
}

func (h fakeHost) RecordSSHPorts(ports []int) error {
	if h.record == nil {
		return nil
	}
	if h.record.fail != nil {
		return h.record.fail
	}
	h.record.ports, h.record.writes = slices.Clone(ports), h.record.writes+1
	return nil
}

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

// The SSH ports the host firewall opens never shrink on a detection that
// failed or found only some: a port closed by mistake locks the operator
// out.
func TestSSHPortsNeverShrinkOnAFailedDetection(t *testing.T) {
	config := []byte("baseDomain: example.com\nhostFirewall:\n")
	record := &sshRecord{}
	host := fakeHost{name: "n1", addresses: []string{"203.0.113.10"}, ssh: []int{22, 2222}, record: record}
	last := func(agent Agent) Detected { return agent.Detected[len(agent.Detected)-1] }
	agent, err := ParseOn(config, nil, host)
	if err != nil {
		t.Fatal(err)
	}
	// A complete detection is recorded, and is no problem.
	if !slices.Equal(agent.HostFirewall.SSH, []int{22, 2222}) || !slices.Equal(record.ports, []int{22, 2222}) || last(agent).Problem != "" {
		t.Fatalf("ssh %v, record %v, detected %+v", agent.HostFirewall.SSH, record.ports, agent.Detected)
	}
	// At the next start ss is missing and sshd -T fails: nothing found,
	// the recorded ports stay open, and the record stays as it was.
	host.ssh, host.sshErr = nil, errors.New("sshd -T: exit status 255; ss: not found")
	agent, err = ParseOn(config, nil, host)
	if err != nil {
		t.Fatal(err)
	}
	if d := last(agent); !slices.Equal(agent.HostFirewall.SSH, []int{22, 2222}) || !strings.Contains(d.Problem, "ss: not found") || record.writes != 1 {
		t.Fatalf("ssh %v, detected %+v, writes %d", agent.HostFirewall.SSH, d, record.writes)
	}
	if rules, err := agent.HostRules(); err != nil || !slices.Contains(rules.PublicTCP, 2222) {
		t.Fatalf("rules %+v, %v", rules, err)
	}
	// Partial: sshd -T answers 22 but a socket unit cannot be asked; 2222
	// stays open.
	host.ssh, host.sshErr = []int{22}, errors.New("systemctl show ssh.socket: exit status 1")
	if agent, err = ParseOn(config, nil, host); err != nil || !slices.Equal(agent.HostFirewall.SSH, []int{22, 2222}) || record.writes != 1 {
		t.Fatalf("partial: ssh %v, %v, writes %d", agent.HostFirewall.SSH, err, record.writes)
	}
	// Nothing recorded (missing, unreadable, corrupt or empty) and the
	// detection fails: what was found, 22, and what the running firewall
	// opens to everyone, which held SSH's port until now.
	host.record, host.ssh = &sshRecord{}, []int{22}
	host.open = []int{22, 80, 443, 2222}
	if agent, err = ParseOn(config, nil, host); err != nil || !slices.Equal(agent.HostFirewall.SSH, []int{22, 80, 443, 2222}) || !strings.Contains(last(agent).Problem, "running host firewall opens (22, 80, 443, 2222)") {
		t.Fatalf("no record: ssh %v, %+v, %v", agent.HostFirewall.SSH, last(agent), err)
	}
	// And the running firewall cannot be read either: 22 beside what was
	// found, said plainly.
	host.ssh, host.open, host.openErr = nil, nil, errors.New("nft: operation not permitted")
	if agent, err = ParseOn(config, nil, host); err != nil || !slices.Equal(agent.HostFirewall.SSH, []int{22}) || !strings.Contains(last(agent).Problem, "may close") {
		t.Fatalf("nothing to go by: ssh %v, %+v, %v", agent.HostFirewall.SSH, last(agent), err)
	}
	// A complete detection that finds sshd moved off a port is believed:
	// the record follows it.
	host.record, host.ssh, host.sshErr = record, []int{2222}, nil
	if agent, err = ParseOn(config, nil, host); err != nil || !slices.Equal(agent.HostFirewall.SSH, []int{2222}) || !slices.Equal(record.ports, []int{2222}) {
		t.Fatalf("moved: ssh %v, record %v, %v", agent.HostFirewall.SSH, record.ports, err)
	}
	// A record that cannot be written is said.
	record.fail = errors.New("read-only file system")
	host.ssh = []int{22}
	if agent, err = ParseOn(config, nil, host); err != nil || !strings.Contains(last(agent).Problem, "read-only") {
		t.Fatalf("unwritable record: %+v, %v", agent.Detected, err)
	}
	// Derivable tries detection without recording anything.
	record.fail, record.writes = nil, 0
	explicit, err := ParseOn([]byte("baseDomain: example.com\nhostFirewall: {ssh: [22]}\n"), nil, host)
	if err != nil || !slices.Contains(explicit.Derivable(host), "hostFirewall.ssh") || record.writes != 0 {
		t.Fatalf("derivable %v, writes %d, %v", explicit.Derivable(host), record.writes, err)
	}
}

// Public addresses found on the host never drop one recorded before: a
// node up before its public interface would otherwise vanish from the
// cluster's DNS records. Only an explicit publicAddresses removes one.
func TestPublicAddressesNeverShrinkOnAStart(t *testing.T) {
	config := []byte("baseDomain: example.com\ncluster: {}\n")
	record := &publicRecord{}
	host := fakeHost{name: "n1", addresses: []string{"10.0.0.2", "203.0.113.11", "203.0.113.10"}, public: record}
	last := func(agent Agent) Detected {
		for _, d := range agent.Detected {
			if d.Key == "cluster.publicAddresses" {
				return d
			}
		}
		t.Fatalf("not detected: %+v", agent.Detected)
		return Detected{}
	}
	agent, err := ParseOn(config, nil, host)
	if err != nil {
		t.Fatal(err)
	}
	// Found, in order, recorded, no problem.
	if want := []string{"203.0.113.10", "203.0.113.11"}; !slices.Equal(agent.Cluster.PublicAddresses, want) || !slices.Equal(record.addresses, want) || last(agent).Problem != "" {
		t.Fatalf("public %v, record %v, %+v", agent.Cluster.PublicAddresses, record.addresses, last(agent))
	}
	// Up before its public interface: none found, the recorded ones kept.
	host.addresses = []string{"10.0.0.2"}
	agent, err = ParseOn(config, nil, host)
	if err != nil || !slices.Equal(agent.Cluster.PublicAddresses, []string{"203.0.113.10", "203.0.113.11"}) || !strings.Contains(last(agent).Problem, "no public address was found") || record.writes != 1 {
		t.Fatalf("none found: %v, %+v, %v", agent.Cluster.PublicAddresses, last(agent), err)
	}
	// One of two found: both kept, with a warning; the record unchanged.
	host.addresses = []string{"10.0.0.2", "203.0.113.11"}
	agent, err = ParseOn(config, nil, host)
	if err != nil || !slices.Equal(agent.Cluster.PublicAddresses, []string{"203.0.113.10", "203.0.113.11"}) || !strings.Contains(last(agent).Problem, "found only 203.0.113.11") || record.writes != 1 {
		t.Fatalf("subset: %v, %+v, %v", agent.Cluster.PublicAddresses, last(agent), err)
	}
	// A new address is believed and recorded.
	host.addresses = []string{"10.0.0.2", "203.0.113.10", "203.0.113.11", "198.51.100.7"}
	agent, err = ParseOn(config, nil, host)
	if want := []string{"198.51.100.7", "203.0.113.10", "203.0.113.11"}; err != nil || !slices.Equal(agent.Cluster.PublicAddresses, want) || !slices.Equal(record.addresses, want) || last(agent).Problem != "" {
		t.Fatalf("added: %v, record %v, %v", agent.Cluster.PublicAddresses, record.addresses, err)
	}
	// An address dropped for good is dropped by naming the others.
	agent, err = ParseOn([]byte("baseDomain: example.com\ncluster: {publicAddresses: [203.0.113.10]}\n"), nil, host)
	if err != nil || !slices.Equal(agent.Cluster.PublicAddresses, []string{"203.0.113.10"}) || agent.IsDetected("cluster.publicAddresses") {
		t.Fatalf("explicit: %v, %v", agent.Cluster.PublicAddresses, err)
	}
	// Nothing recorded and none found: none, as before.
	host.addresses, host.public = []string{"10.0.0.2"}, &publicRecord{}
	if agent, err = ParseOn(config, nil, host); err != nil || len(agent.Cluster.PublicAddresses) != 0 || last(agent).Problem != "" {
		t.Fatalf("none at all: %v, %+v, %v", agent.Cluster.PublicAddresses, last(agent), err)
	}
}

// The host firewall's rules do not follow the public addresses found on
// the host: it opens ports on every address, so an interface up late at
// boot or a new DHCP lease changes no rule.
func TestHostRulesDoNotFollowPublicAddresses(t *testing.T) {
	config := []byte(shared)
	rules := func(addresses ...string) string {
		t.Helper()
		host := fakeHost{name: "n1", addresses: append([]string{"10.0.0.2"}, addresses...), pools: []string{"tank"}, ssh: []int{22}}
		agent, err := ParseOn(config, nil, host)
		if err != nil {
			t.Fatal(err)
		}
		r, err := agent.HostRules()
		if err != nil {
			t.Fatal(err)
		}
		return fmt.Sprintf("%+v", *r)
	}
	none, one, two := rules(), rules("203.0.113.10"), rules("203.0.113.10", "198.51.100.7")
	if none != one || one != two {
		t.Fatalf("rules changed with the public addresses:\n%s\n%s\n%s", none, one, two)
	}
}
