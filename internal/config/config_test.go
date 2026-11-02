package config

import (
	"reflect"
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
	if !reflect.DeepEqual(*agent.Cluster, Cluster{Address: "10.0.0.2:7400", Dir: "/var/lib/murder/cluster"}) {
		t.Fatalf("cluster = %+v", agent.Cluster)
	}
	if _, err := Parse([]byte("baseDomain: example.com\ncluster: {address: 100.101.102.103:7400}\n")); err != nil {
		t.Fatalf("a Tailscale address: %v", err)
	}
	for _, bad := range []string{
		"cluster: {address: ':7400'}",
		"cluster: {address: 0.0.0.0:7400}",
		"cluster: {address: node-1.internal:7400}",
		"cluster: {address: 203.0.113.7:7400}",
		"cluster: {address: 10.0.0.2}",
		"cluster: {address: 10.0.0.2:0}",
		"cluster: {address: 10.0.0.2:7400, dir: relative}",
	} {
		if _, err := Parse([]byte("baseDomain: example.com\n" + bad + "\n")); err == nil || !strings.Contains(err.Error(), "cluster.") {
			t.Errorf("%s: %v", bad, err)
		}
	}
	if _, err := Parse([]byte("baseDomain: example.com\ncluster: {address: 10.0.0.2:7400, role: witness}\n")); err == nil {
		t.Error("a role was accepted: every node runs tenants")
	}
}

// Backups fit their mode: a test bucket keeps no lock, a production one
// needs one, which fills in, and never longer than every backup is kept.
func TestBackupsFitTheirMode(t *testing.T) {
	base := "baseDomain: example.com\nvolumes: {driver: zfs, parent: tank/murder/tenants}\nbackups:\n  endpoint: https://hel1.your-objectstorage.com\n  bucket: b\n  accessKeyFile: /etc/murder/s3.access\n  secretKeyFile: /etc/murder/s3.secret\n  recipient: age1qqq\n"
	test, err := Parse([]byte(base + "  mode: test\n"))
	if err != nil {
		t.Fatal(err)
	}
	if test.Backups.LockFor.Duration != 0 || test.Backups.Every.Duration != time.Hour || test.Backups.KeepHourly.Duration != 48*time.Hour || test.Backups.KeepLocal != 24 {
		t.Fatalf("test backups = %+v", test.Backups)
	}
	production, err := Parse([]byte(base + "  mode: production\n"))
	if err != nil {
		t.Fatal(err)
	}
	if production.Backups.LockFor.Duration != 30*24*time.Hour || production.Backups.KeepMonthly.Duration != 365*24*time.Hour {
		t.Fatalf("production backups = %+v", production.Backups)
	}
	week, err := Parse([]byte(base + "  mode: production\n  keepDaily: 168h\n"))
	if err != nil || week.Backups.LockFor.Duration != 168*time.Hour {
		t.Fatalf("the default lock with daily backups kept a week: %+v, %v", week.Backups, err)
	}
	endpoint := func(value string) string {
		return strings.Replace(base, "https://hel1.your-objectstorage.com", value, 1) + "  mode: test\n"
	}
	for name, c := range map[string]struct{ config, problem string }{
		"test with a lock":        {base + "  mode: test\n  lockFor: 24h\n", "not locked"},
		"lock beyond keepDaily":   {base + "  mode: production\n  lockFor: 1000h\n  keepDaily: 100h\n", "lockFor must not outlast"},
		"no mode":                 {base, "mode must be"},
		"another mode":            {base + "  mode: maybe\n", "mode must be"},
		"negative verifyEvery":    {base + "  mode: test\n  verifyEvery: -1h\n", "verifyEvery"},
		"verifyEvery below every": {base + "  mode: test\n  every: 2h\n  keepHourly: 48h\n  verifyEvery: 1h\n", "verifyEvery"},
		"plain http":              {endpoint("http://hel1.your-objectstorage.com"), "endpoint"},
		"no host":                 {endpoint("https://"), "endpoint"},
		"a path":                  {endpoint("https://hel1.your-objectstorage.com/bucket"), "endpoint"},
	} {
		if _, err := Parse([]byte(c.config)); err == nil || !strings.Contains(err.Error(), c.problem) {
			t.Errorf("%s: %v", name, err)
		}
	}
	if _, err := Parse([]byte(strings.Replace(base, "driver: zfs, parent: tank/murder/tenants", "driver: dir", 1) + "  mode: test\n")); err == nil {
		t.Error("backups with the dir driver: accepted")
	}
}

// Failover needs a provider to power nodes off, and a delay long enough
// that a busy node is not powered off; it waits 30 s by default.
func TestFailoverNeedsAProvider(t *testing.T) {
	base := "baseDomain: example.com\nvolumes: {driver: zfs, parent: tank/murder/tenants}\ncluster:\n  address: 10.0.0.2:7400\n  failover:\n"
	agent, err := Parse([]byte(base + "    hetzner: {tokenFile: /etc/murder/hcloud.token, servers: {n1: murder-n1}}\n"))
	if err != nil {
		t.Fatal(err)
	}
	if f := agent.Cluster.Failover; f.After.Duration != 30*time.Second || f.Hetzner.Servers["n1"] != "murder-n1" {
		t.Fatalf("failover = %+v", f)
	}
	for name, extra := range map[string]string{
		"no provider":      "    after: 30s\n",
		"a relative token": "    hetzner: {tokenFile: hcloud.token}\n",
		"too soon":         "    after: 5s\n    hetzner: {tokenFile: /etc/murder/hcloud.token}\n",
	} {
		if _, err := Parse([]byte(base + extra)); err == nil || !strings.Contains(err.Error(), "failover") {
			t.Errorf("%s: %v", name, err)
		}
	}
	dir := strings.Replace(base, "{driver: zfs, parent: tank/murder/tenants}", "{driver: dir}", 1)
	if _, err := Parse([]byte(dir + "    hetzner: {tokenFile: /etc/murder/hcloud.token}\n")); err == nil || !strings.Contains(err.Error(), "zfs") {
		t.Errorf("failover with the dir driver: %v", err)
	}
}

// The DNS records need the node's public addresses, a known provider and a
// zone the base domain is in, the base domain by default.
func TestDNSRecords(t *testing.T) {
	base := "baseDomain: apps.example.com\ncluster:\n  address: 10.0.0.2:7400\n"
	agent, err := Parse([]byte(base + "  publicAddresses: [203.0.113.7, \"2001:db8::7\"]\n  dns: {provider: hetzner, tokenFile: /etc/murder/dns.token}\n"))
	if err != nil {
		t.Fatal(err)
	}
	if d := agent.Cluster.DNS; d.Zone != "apps.example.com" || d.TTL.Duration != time.Minute {
		t.Fatalf("dns = %+v", d)
	}
	if _, err := Parse([]byte(base + "  publicAddresses: [203.0.113.7]\n  dns: {provider: namecheap, tokenFile: /t, zone: example.com, ttl: 5m}\n")); err != nil {
		t.Fatalf("a zone above the base domain: %v", err)
	}
	for name, extra := range map[string]string{
		"no addresses":     "  dns: {provider: hetzner, tokenFile: /t}\n",
		"a private one":    "  publicAddresses: [10.0.0.2]\n",
		"not an address":   "  publicAddresses: [example.com]\n",
		"no provider":      "  publicAddresses: [203.0.113.7]\n  dns: {tokenFile: /t}\n",
		"a relative token": "  publicAddresses: [203.0.113.7]\n  dns: {provider: hetzner, tokenFile: t}\n",
		"another zone":     "  publicAddresses: [203.0.113.7]\n  dns: {provider: hetzner, tokenFile: /t, zone: other.com}\n",
		"a short ttl":      "  publicAddresses: [203.0.113.7]\n  dns: {provider: hetzner, tokenFile: /t, ttl: 10s}\n",
		"a long ttl":       "  publicAddresses: [203.0.113.7]\n  dns: {provider: hetzner, tokenFile: /t, ttl: 48h}\n",
		"namecheap's max":  "  publicAddresses: [203.0.113.7]\n  dns: {provider: namecheap, tokenFile: /t, ttl: 20h}\n",
	} {
		if _, err := Parse([]byte(base + extra)); err == nil || !strings.Contains(err.Error(), "cluster.") {
			t.Errorf("%s: %v", name, err)
		}
	}
}
