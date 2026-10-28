package config

import (
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
}

func TestParseFullConfig(t *testing.T) {
	agent, err := Parse([]byte(`
baseDomain: example.com
volumes: {driver: zfs, parent: tank/corral/tenants, mountRoot: /srv/corral/tenants}
edge:
  tls: {mode: acme, email: ops@example.com, dnsProvider: hetzner, dnsTokenFile: /etc/corral/dns.token}
reconcileInterval: 5s
defaults: {memoryMB: 768}
`))
	if err != nil {
		t.Fatal(err)
	}
	if agent.Volumes.Parent != "tank/corral/tenants" || agent.ReconcileInterval.Duration != 5*time.Second || agent.Scheme() != "https" || agent.Defaults.MemoryMB != 768 {
		t.Fatalf("config = %+v", agent)
	}
}

func TestParseRejectsBadConfig(t *testing.T) {
	for name, document := range map[string]string{
		"empty":          "",
		"unknown field":  "baseDomain: example.com\nbaseDomian: typo.com\n",
		"bad driver":     "baseDomain: example.com\nvolumes: {driver: nfs}\n",
		"zfs no parent":  "baseDomain: example.com\nvolumes: {driver: zfs, mountRoot: /srv}\n",
		"no longer used": "baseDomain: example.com\nports: {from: 20000, to: 29999}\n",
		"acme partial":   "baseDomain: example.com\nedge: {tls: {mode: acme}}\n",
		"interval":       "baseDomain: example.com\nreconcileInterval: 10ms\n",
		"duration":       "baseDomain: example.com\nreconcileInterval: soon\n",
		"pool":           "baseDomain: example.com\nnetworkPool: 10.0.0.0/28\n",
		"node":           "baseDomain: example.com\nnode: \"a b\"\n",
		"dns provider":   "baseDomain: example.com\nedge: {tls: {mode: acme, email: a@b.c, dnsTokenFile: /t, dnsProvider: cloudflare}}\n",
		"tls storage":    "baseDomain: example.com\nedge: {tls: {mode: internal, storage: \"\"}}\n",
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
	if agent.Volumes.Driver != "zfs" || agent.Edge.TLS.Mode != "acme" {
		t.Fatalf("example config = %+v", agent)
	}
}
