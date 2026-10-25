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
	if agent.Volumes.Driver != "dir" || agent.Ports.From != 20000 || agent.Edge.TLS.Mode != "off" || agent.ReconcileInterval.Duration != 10*time.Second || agent.Defaults.MemoryMB != 512 {
		t.Fatalf("defaults = %+v", agent)
	}
	if agent.Scheme() != "http" {
		t.Fatalf("scheme with TLS off = %q", agent.Scheme())
	}
}

func TestParseFullConfig(t *testing.T) {
	agent, err := Parse([]byte(`
baseDomain: example.com
volumes: {driver: zfs, parent: tank/corral/tenants, mountRoot: /srv/corral/tenants}
ports: {from: 21000, to: 21999}
edge:
  tls: {mode: acme, email: ops@example.com, dnsProvider: hetzner, dnsTokenFile: /etc/corral/dns.token}
reconcileInterval: 5s
defaults: {memoryMB: 768}
`))
	if err != nil {
		t.Fatal(err)
	}
	if agent.Volumes.Parent != "tank/corral/tenants" || agent.Ports.To != 21999 || agent.ReconcileInterval.Duration != 5*time.Second || agent.Scheme() != "https" || agent.Defaults.MemoryMB != 768 {
		t.Fatalf("config = %+v", agent)
	}
}

func TestParseRejectsBadConfig(t *testing.T) {
	for name, document := range map[string]string{
		"empty":         "",
		"unknown field": "baseDomain: example.com\nbaseDomian: typo.com\n",
		"bad driver":    "baseDomain: example.com\nvolumes: {driver: nfs}\n",
		"zfs no parent": "baseDomain: example.com\nvolumes: {driver: zfs, mountRoot: /srv}\n",
		"ports":         "baseDomain: example.com\nports: {from: 80, to: 90}\n",
		"acme partial":  "baseDomain: example.com\nedge: {tls: {mode: acme}}\n",
		"interval":      "baseDomain: example.com\nreconcileInterval: 10ms\n",
		"duration":      "baseDomain: example.com\nreconcileInterval: soon\n",
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
