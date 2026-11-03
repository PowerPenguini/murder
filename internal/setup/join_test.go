package setup

import (
	"context"
	"net/netip"
	"strings"
	"testing"

	"github.com/PowerPenguini/murder/internal/cluster"
)

const clusterConfig = `# written by murder setup
node: n1
baseDomain: example.com  # tenants live here
volumes:
  driver: zfs
  parent: tank/murder/tenants
edge:
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
	in, agent, err := Adopt(bundle, Own{Node: "n2", ZFSPool: "data", ClusterAddress: "10.0.0.3:7400", Public: []string{"203.0.113.11"}})
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
	if !strings.Contains(string(in.Config), "this node on the private network") {
		t.Fatalf("comments lost:\n%s", in.Config)
	}
	if string(in.Secrets["/etc/murder/master.key"]) != "a2V5\n" || string(in.Secrets["/etc/murder/ghcr.token"]) != "ghp\n" || len(in.Secrets) != 3 || !in.Panel || in.ZFSPool != "data" {
		t.Fatalf("install %+v", in)
	}
	// A cluster that keeps copies refuses a node without ZFS.
	if _, _, err := Adopt(bundle, Own{Node: "n2", ClusterAddress: "10.0.0.3:7400"}); err == nil || !strings.Contains(err.Error(), "zfs") {
		t.Fatalf("plain directories in a failover cluster: %v", err)
	}
	// Files outside the config's directory are refused.
	bundle.Files["/root/.ssh/authorized_keys"] = []byte("x")
	if _, _, err := Adopt(bundle, Own{Node: "n2", ZFSPool: "data", ClusterAddress: "10.0.0.3:7400"}); err == nil || !strings.Contains(err.Error(), "not in /etc/murder") {
		t.Fatalf("a file elsewhere: %v", err)
	}
}

func TestAJoiningHostPicksItsAddressOnThePeersNetwork(t *testing.T) {
	host := newFakeHost()
	host.addrs = []string{"203.0.113.11", "10.0.0.3", "192.168.1.5"}
	facts := Inspect(context.Background(), host)
	own, problems := SettleOwn(Options{}, facts, netip.MustParseAddrPort("10.0.0.2:7400"))
	if len(problems) > 0 || own.Node != "fsn-1" || own.ClusterAddress != "10.0.0.3:7400" || own.ZFSPool != "tank" || own.Public[0] != "203.0.113.11" {
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
