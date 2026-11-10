package setup

import (
	"context"
	"net/netip"
	"slices"
	"strings"
	"testing"

	"github.com/PowerPenguini/murder/internal/cluster"
	"github.com/PowerPenguini/murder/internal/config"
)

const clusterConfig = `# written by murder setup
node: n1
baseDomain: example.com  # tenants live here
metrics: 10.0.0.2:9464
volumes:
  driver: zfs
  parent: tank/murder/tenants
  mountRoot: /srv/tenants
edge:
  http: ":80"
  https: 203.0.113.10:443
  tls:
    mode: acme
    email: ops@example.com
    dnsProvider: hetzner
    dnsTokenFile: /etc/murder/hetzner.token
registries:
  - {host: ghcr.io, username: bot, tokenFile: /etc/murder/ghcr.token}
cluster:
  address: 10.0.0.2:7400  # this node on the private network
  publicAddresses: [203.0.113.10]
  failover:
    hetzner:
      tokenFile: /etc/murder/hetzner.token
panel:
  tunnel:
    address: 10.77.0.1
`

func TestAHostMakesTheClustersConfigItsOwn(t *testing.T) {
	bundle := cluster.Bundle{Cluster: "acme", Key: []byte("a2V5\n"), Config: []byte(clusterConfig), TunnelAddress: "10.77.0.2",
		Files: map[string][]byte{"/etc/murder/hetzner.token": []byte("hcloud\n"), "/etc/murder/ghcr.token": []byte("ghp\n")}}
	identity, err := cluster.NewIdentity()
	if err != nil {
		t.Fatal(err)
	}
	in, agent, err := Adopt(bundle, Own{Node: "n2", ZFSPool: "data", ClusterAddress: "10.0.0.3:7400", Public: []string{"203.0.113.11"}, Identity: identity})
	if err != nil {
		t.Fatal(err)
	}
	if agent.Node != "n2" || agent.Cluster.Address != "10.0.0.3:7400" || agent.Cluster.PublicAddresses[0] != "203.0.113.11" || agent.Volumes.Parent != "data/murder/tenants" || agent.Panel.Tunnel.Address != "10.77.0.2" {
		t.Fatalf("agent %+v", agent)
	}
	// The cluster's own settings stay as they are.
	if agent.BaseDomain != "example.com" || agent.Edge.TLS.DNSProvider != "hetzner" || agent.Cluster.Failover == nil || len(agent.Registries) != 1 {
		t.Fatalf("agent %+v", agent)
	}
	// Listeners on the inviting node's own addresses move to this host's.
	if agent.Metrics != "10.0.0.3:9464" || agent.Edge.HTTPS != "203.0.113.11:443" || agent.Edge.HTTP != ":80" || agent.Volumes.MountRoot != "/srv/tenants" {
		t.Fatalf("metrics %s, https %s, http %s, mountRoot %s", agent.Metrics, agent.Edge.HTTPS, agent.Edge.HTTP, agent.Volumes.MountRoot)
	}
	if !strings.Contains(string(in.Config), "this node on the private network") {
		t.Fatalf("comments lost:\n%s", in.Config)
	}
	if string(in.Secrets["/etc/murder/master.key"]) != "a2V5\n" || string(in.Secrets["/etc/murder/ghcr.token"]) != "ghp\n" || len(in.Secrets) != 4 || !in.Panel || in.ZFSPool != "data" {
		t.Fatalf("install %+v", in)
	}
	// The identity key the invitation admitted goes where the agent reads
	// it.
	if written, _ := cluster.EncodeIdentity(identity); string(in.Secrets["/var/lib/murder/cluster/node.key"]) != string(written) {
		t.Fatalf("identity key at %v", in.Secrets)
	}
	if _, _, err := Adopt(bundle, Own{Node: "n2", ZFSPool: "data", ClusterAddress: "10.0.0.3:7400"}); err == nil || !strings.Contains(err.Error(), "identity key") {
		t.Fatalf("a host without an identity key: %v", err)
	}
	// A cluster that keeps copies refuses a node without ZFS.
	if _, _, err := Adopt(bundle, Own{Node: "n2", ClusterAddress: "10.0.0.3:7400", Identity: identity}); err == nil || !strings.Contains(err.Error(), "zfs") {
		t.Fatalf("plain directories in a failover cluster: %v", err)
	}
	// Files outside the config's directory are refused.
	bundle.Files["/root/.ssh/authorized_keys"] = []byte("x")
	if _, _, err := Adopt(bundle, Own{Node: "n2", ZFSPool: "data", ClusterAddress: "10.0.0.3:7400", Identity: identity}); err == nil || !strings.Contains(err.Error(), "not in /etc/murder") {
		t.Fatalf("a file elsewhere: %v", err)
	}
}

func TestAJoiningHostPicksItsAddressOnThePeersNetwork(t *testing.T) {
	host := newFakeHost()
	host.addrs = []string{"203.0.113.11", "10.0.0.3", "192.168.1.5"}
	facts := Inspect(context.Background(), host)
	own, problems := SettleOwn(Options{}, facts, netip.MustParseAddrPort("10.0.0.2:7400"))
	if len(problems) > 0 || own.Node != "fsn-1" || own.ClusterAddress != "10.0.0.3:7400" || own.ZFSPool != "tank" || own.Public[0] != "203.0.113.11" || len(own.SSHPorts) == 0 || !slices.Equal(own.SSHPorts, facts.SSHPorts) {
		t.Fatalf("own %+v, problems %v", own, problems)
	}
	_, problems = SettleOwn(Options{}, facts, netip.MustParseAddrPort("172.20.0.2:7400"))
	if !strings.Contains(strings.Join(problems, "\n"), "no private address on 172.20.0.2's network") {
		t.Fatalf("problems %v", problems)
	}
	own, problems = SettleOwn(Options{ClusterAddress: "192.168.1.5", Node: "n9"}, facts, netip.MustParseAddrPort("172.20.0.2:7400"))
	if len(problems) > 0 || own.ClusterAddress != "192.168.1.5:7400" || own.Node != "n9" {
		t.Fatalf("own %+v, problems %v", own, problems)
	}
}

// A joining host keeps its own SSH ports and what listened on it open in
// the cluster's host firewall: the inviting node's sshd may listen on
// other ports, and the entries the cluster's config opens were found on
// that node, not on this host.
func TestAJoiningHostKeepsItsOwnPortsOpen(t *testing.T) {
	identity, err := cluster.NewIdentity()
	if err != nil {
		t.Fatal(err)
	}
	own := Own{Node: "n2", ZFSPool: "data", ClusterAddress: "10.0.0.3:7400", Identity: identity, SSHPorts: []int{22, 2222},
		Open:    []Listener{{Protocol: "tcp", Port: 9300, Process: "python3"}, {Protocol: "tcp", Port: 9100, Process: "node_exporter"}},
		Private: []Listener{{Protocol: "tcp", Port: 9200, Process: "app"}, {Protocol: "udp", Port: 9400, Process: "dns"}, {Protocol: "tcp", Port: 9100}}}
	adopt := func(firewall string) (config.Agent, string) {
		t.Helper()
		bundle := cluster.Bundle{Cluster: "acme", Key: []byte("a2V5\n"), Config: []byte(clusterConfig + firewall), TunnelAddress: "10.77.0.2",
			Files: map[string][]byte{"/etc/murder/hetzner.token": []byte("hcloud\n"), "/etc/murder/ghcr.token": []byte("ghp\n")}}
		in, agent, err := Adopt(bundle, own)
		if err != nil {
			t.Fatal(err)
		}
		return agent, string(in.Config)
	}

	agent, text := adopt("hostFirewall:  # drops the rest\n  ssh: [22]\n  open:\n    - 9100/tcp  # python3\n  private:\n    - 9200/tcp\n")
	h := agent.HostFirewall
	if h == nil || !slices.Equal(h.SSH, []int{22, 2222}) || !slices.Equal(h.Open, []string{"9100/tcp", "9300/tcp"}) || !slices.Equal(h.Private, []string{"9200/tcp", "9400/udp"}) {
		t.Fatalf("host firewall %+v in\n%s", h, text)
	}
	if !strings.Contains(text, "9300/tcp # python3 on n2") || !strings.Contains(text, "# drops the rest") {
		t.Fatalf("comments lost:\n%s", text)
	}
	// "hostFirewall:" alone opens 22 by default, which stays.
	if agent, text := adopt("hostFirewall:\n"); agent.HostFirewall == nil || !slices.Equal(agent.HostFirewall.SSH, []int{22, 2222}) {
		t.Fatalf("bare host firewall %+v in\n%s", agent.HostFirewall, text)
	}
	// The cluster's SSH ports keep their layout and comments, in any form
	// YAML writes an int.
	agent, text = adopt("hostFirewall:\n  ssh:\n    - 0x16  # main\n    - 2_000  # alt\n")
	if !slices.Equal(agent.HostFirewall.SSH, []int{22, 2000, 2222}) || !strings.Contains(text, "- 2_000 # alt") {
		t.Fatalf("ssh %v in\n%s", agent.HostFirewall.SSH, text)
	}
	// A cluster without a host firewall gets none.
	if agent, text := adopt(""); agent.HostFirewall != nil || strings.Contains(text, "hostFirewall") {
		t.Fatalf("host firewall %+v in\n%s", agent.HostFirewall, text)
	}
	// Nothing to add leaves the cluster's entries as they are.
	own.SSHPorts, own.Open, own.Private = []int{22}, nil, nil
	if agent, _ := adopt("hostFirewall:\n  ssh: [22]\n  open: [\"9100/tcp\"]\n"); !slices.Equal(agent.HostFirewall.SSH, []int{22}) || !slices.Equal(agent.HostFirewall.Open, []string{"9100/tcp"}) || agent.HostFirewall.Private != nil {
		t.Fatalf("host firewall %+v", agent.HostFirewall)
	}
}
