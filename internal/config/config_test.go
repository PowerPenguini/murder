package config

import (
	"slices"
	"strings"
	"testing"
	"time"
)

func TestParseFillsDefaults(t *testing.T) {
	agent, err := Parse([]byte("baseDomain: example.com\n"))
	if err != nil {
		t.Fatal(err)
	}
	if agent.Volumes.Driver != "dir" || agent.Edge.TLS.Mode != "off" || agent.ReconcileInterval.Duration != 10*time.Second || agent.Defaults.MemoryMB != 512 {
		t.Fatalf("defaults = %+v", agent)
	}
	if agent.Scheme() != "http" {
		t.Fatalf("scheme with TLS off = %q", agent.Scheme())
	}
	if agent.Node != "" || agent.NetworkPool != "10.240.0.0/16" {
		t.Fatalf("node = %q, pool = %q", agent.Node, agent.NetworkPool)
	}
	if ports, err := agent.EdgePorts(); agent.Firewall != FirewallNftables || err != nil || !slices.Equal(ports, []int{80}) {
		t.Fatalf("firewall = %q, edge ports = %v, %v", agent.Firewall, ports, err)
	}
}

func TestParseFullConfig(t *testing.T) {
	agent, err := Parse([]byte(`
baseDomain: example.com
volumes: {driver: zfs, parent: tank/murder/tenants, mountRoot: /srv/murder/tenants}
edge:
  tls: {mode: acme, email: ops@example.com, dnsProvider: hetzner, dnsTokenFile: /etc/murder/dns.token}
reconcileInterval: 5s
defaults: {memoryMB: 768}
`))
	if err != nil {
		t.Fatal(err)
	}
	if agent.Volumes.Parent != "tank/murder/tenants" || agent.ReconcileInterval.Duration != 5*time.Second || agent.Scheme() != "https" || agent.Defaults.MemoryMB != 768 {
		t.Fatalf("config = %+v", agent)
	}
	if ports, err := agent.EdgePorts(); err != nil || !slices.Equal(ports, []int{80, 443}) {
		t.Fatalf("edge ports with TLS = %v, %v", ports, err)
	}
	// A service name is a port too; without the firewall nothing is checked.
	named, err := Parse([]byte("baseDomain: example.com\nedge: {http: \":http\"}\n"))
	if ports, _ := named.EdgePorts(); err != nil || !slices.Equal(ports, []int{80}) {
		t.Fatalf("edge port by name = %v, %v", ports, err)
	}
	if _, err := Parse([]byte("baseDomain: example.com\nfirewall: \"off\"\nedge: {http: \"localhost\"}\n")); err != nil {
		t.Fatalf("edge address without the firewall = %v", err)
	}
}

func TestParseRejectsBadConfig(t *testing.T) {
	for name, document := range map[string]string{
		"empty":         "",
		"unknown field": "baseDomain: example.com\nbaseDomian: typo.com\n",
		"two documents": "baseDomain: example.com\n---\nnode: other\n",
		"defaults":      "baseDomain: example.com\ndefaults: {diskGB: 9000000000}\n",
		"firewall":      "baseDomain: example.com\nfirewall: iptables\n",
		"edge port":     "baseDomain: example.com\nedge: {http: \"localhost\"}\n",
		"bad driver":    "baseDomain: example.com\nvolumes: {driver: nfs}\n",
		"zfs no parent": "baseDomain: example.com\nvolumes: {driver: zfs, mountRoot: /srv}\n",
		"acme partial":  "baseDomain: example.com\nedge: {tls: {mode: acme}}\n",
		"interval":      "baseDomain: example.com\nreconcileInterval: 10ms\n",
		"duration":      "baseDomain: example.com\nreconcileInterval: soon\n",
		"pool":          "baseDomain: example.com\nnetworkPool: 10.0.0.0/28\n",
		"node":          "baseDomain: example.com\nnode: \"a b\"\n",
		"dns provider":  "baseDomain: example.com\nedge: {tls: {mode: acme, email: a@b.c, dnsTokenFile: /t, dnsProvider: cloudflare}}\n",
		"tls storage":   "baseDomain: example.com\nedge: {tls: {mode: internal, storage: \"\"}}\n",
		"metrics":       "baseDomain: example.com\nmetrics: \":9464\"\n",
	} {
		if _, err := Parse([]byte(document)); err == nil {
			t.Errorf("%s: accepted", name)
		} else if name == "empty" && !strings.Contains(err.Error(), "baseDomain") {
			t.Errorf("empty config error = %v", err)
		}
	}
}

func TestExampleConfigParses(t *testing.T) {
	agent, err := Load("../../deploy/agent.example.yaml")
	if err != nil {
		t.Fatalf("deploy/agent.example.yaml: %v", err)
	}
	if agent.Volumes.Driver != "zfs" || agent.Edge.TLS.Mode != "acme" || agent.Metrics != "127.0.0.1:9464" {
		t.Fatalf("example config = %+v", agent)
	}
}

func TestParseCluster(t *testing.T) {
	agent, err := Parse([]byte("baseDomain: example.com\ncluster: {address: 10.0.0.2:7400}\n"))
	if err != nil {
		t.Fatal(err)
	}
	if *agent.Cluster != (Cluster{Address: "10.0.0.2:7400", Role: RoleWorker, Dir: "/var/lib/murder/cluster"}) || agent.Witness() {
		t.Fatalf("cluster = %+v", agent.Cluster)
	}
	// A witness serves no tenants and needs no domain.
	agent, err = Parse([]byte("cluster: {address: 10.0.0.3:7400, role: witness, dir: /srv/cluster}\n"))
	if err != nil || !agent.Witness() {
		t.Fatalf("witness: %+v, %v", agent.Cluster, err)
	}
	for _, bad := range []string{
		"cluster: {address: ':7400'}",
		"cluster: {address: 0.0.0.0:7400}",
		"cluster: {address: node-1.internal:7400}",
		"cluster: {address: 203.0.113.7:7400}",
		"cluster: {address: 10.0.0.2}",
		"cluster: {address: 10.0.0.2:0}",
		"cluster: {address: 10.0.0.2:7400, role: boss}",
		"cluster: {address: 10.0.0.2:7400, dir: relative}",
	} {
		if _, err := Parse([]byte("baseDomain: example.com\n" + bad + "\n")); err == nil || !strings.Contains(err.Error(), "cluster.") {
			t.Errorf("%s: %v", bad, err)
		}
	}
	if _, err := Parse([]byte("cluster: {address: 10.0.0.2:7400}\n")); err == nil || !strings.Contains(err.Error(), "baseDomain") {
		t.Errorf("a worker without a domain: %v", err)
	}
}
