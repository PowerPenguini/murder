package firewall

import (
	"net/netip"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
)

func TestRules(t *testing.T) {
	rules := Rules([]netip.Prefix{netip.MustParsePrefix("10.240.0.0/16")}, []int{443, 80, 80}, []netip.Addr{netip.MustParseAddr("10.0.2.3"), netip.MustParseAddr("10.0.2.3")})
	for _, want := range []string{
		"table inet murder\ndelete table inet murder\ntable inet murder {",
		"ip saddr { 10.240.0.0/16 } ct state established,related accept",
		"ip saddr { 10.240.0.0/16 } tcp dport { 80, 443 } accept",
		"ip saddr { 10.240.0.0/16 } counter drop",
		"ip saddr { 10.240.0.0/16 } ip daddr { 10.0.2.3 } meta l4proto { tcp, udp } th dport 53 accept\n\t\tip saddr { 10.240.0.0/16 } counter drop",
		"ip saddr { 10.240.0.0/16 } ip daddr { 10.0.2.3 } meta l4proto { tcp, udp } th dport 53 accept\n\t\tip saddr { 10.240.0.0/16 } ip daddr {",
		"ip saddr { 10.240.0.0/16 } ip daddr { 0.0.0.0/8, 10.0.0.0/8, 100.64.0.0/10, 127.0.0.0/8, 169.254.0.0/16, 172.16.0.0/12, 192.0.0.0/24, 192.168.0.0/16, 198.18.0.0/15, 224.0.0.0/3 } counter drop",
	} {
		if !strings.Contains(rules, want) {
			t.Errorf("rules lack %q:\n%s", want, rules)
		}
	}
	// The established rule comes first, so replies to the edge pass.
	if strings.Index(rules, "established") > strings.Index(rules, "counter drop") {
		t.Fatalf("rules drop before accepting replies:\n%s", rules)
	}
	if without := Rules([]netip.Prefix{netip.MustParsePrefix("10.240.0.0/16")}, nil, nil); strings.Contains(without, "dport") {
		t.Fatalf("rules without edge ports:\n%s", without)
	}
}

// nft checks the script's syntax without loading it (it needs no root).
func TestRulesParse(t *testing.T) {
	if _, err := exec.LookPath("nft"); err != nil {
		t.Skip("nft is not installed")
	}
	command := exec.Command("nft", "--check", "-f", "-")
	command.Stdin = strings.NewReader(Rules([]netip.Prefix{netip.MustParsePrefix("10.240.0.0/16")}, []int{80, 443}, []netip.Addr{netip.MustParseAddr("10.0.2.3")}))
	output, err := command.CombinedOutput()
	if err != nil && strings.Contains(string(output), "Operation not permitted") {
		t.Skip("nft --check needs privileges here")
	}
	if err != nil {
		t.Fatalf("nft --check: %v\n%s", err, output)
	}
}

// Resolvers reads both resolv.conf files and keeps IPv4 servers that are
// not loopback.
func TestResolvers(t *testing.T) {
	dir := t.TempDir()
	classic, upstream := filepath.Join(dir, "resolv.conf"), filepath.Join(dir, "upstream.conf")
	if err := os.WriteFile(classic, []byte("# stub\nnameserver 127.0.0.53\noptions edns0\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(upstream, []byte("nameserver 10.0.2.3\nnameserver 2001:db8::53\nnameserver 185.12.64.1\nsearch example\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	resolvConfs = []string{classic, upstream, filepath.Join(dir, "missing")}
	t.Cleanup(func() { resolvConfs = []string{"/etc/resolv.conf", "/run/systemd/resolve/resolv.conf"} })
	got := Resolvers()
	if len(got) != 2 || got[0].String() != "10.0.2.3" || got[1].String() != "185.12.64.1" {
		t.Fatalf("resolvers = %v", got)
	}
}

// Tenant subnets left outside the pool by an earlier pool are covered too.
func TestRulesCoverEverySource(t *testing.T) {
	rules := Rules([]netip.Prefix{netip.MustParsePrefix("10.250.0.0/16"), netip.MustParsePrefix("10.240.0.16/28"), netip.MustParsePrefix("10.250.0.0/16")}, nil, nil)
	if !strings.Contains(rules, "ip saddr { 10.240.0.16/28, 10.250.0.0/16 } counter drop") {
		t.Fatalf("rules:\n%s", rules)
	}
}
