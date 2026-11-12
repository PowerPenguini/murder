package firewall

import (
	"net/netip"
	"os"
	"os/exec"
	"path/filepath"
	"slices"
	"strings"
	"testing"
)

func TestRules(t *testing.T) {
	rules := Rules([]netip.Prefix{netip.MustParsePrefix("10.240.0.0/16")}, []int{443, 80, 80}, []netip.Addr{netip.MustParseAddr("10.0.2.3"), netip.MustParseAddr("10.0.2.3")}, nil)
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
	if without := Rules([]netip.Prefix{netip.MustParsePrefix("10.240.0.0/16")}, nil, nil, nil); strings.Contains(without, "dport") {
		t.Fatalf("rules without edge ports:\n%s", without)
	}
}

// nft checks the script's syntax without loading it (it needs no root).
func TestRulesParse(t *testing.T) {
	if _, err := exec.LookPath("nft"); err != nil {
		t.Skip("nft is not installed")
	}
	command := exec.Command("nft", "--check", "-f", "-")
	command.Stdin = strings.NewReader(Rules([]netip.Prefix{netip.MustParsePrefix("10.240.0.0/16")}, []int{80, 443}, []netip.Addr{netip.MustParseAddr("10.0.2.3")}, testHost))
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
	rules := Rules([]netip.Prefix{netip.MustParsePrefix("10.250.0.0/16"), netip.MustParsePrefix("10.240.0.16/28"), netip.MustParsePrefix("10.250.0.0/16")}, nil, nil, nil)
	if !strings.Contains(rules, "ip saddr { 10.240.0.16/28, 10.250.0.0/16 } counter drop") {
		t.Fatalf("rules:\n%s", rules)
	}
}

var testHost = &Host{PublicTCP: []int{443, 22, 80}, PublicUDP: []int{51820}, PrivateTCP: []int{7402, 7400}}

// With a host, what is not a tenant's is let in by the list and dropped
// otherwise, after the tenant rules; without one, the input chain is as
// before.
func TestHostRules(t *testing.T) {
	sources := []netip.Prefix{netip.MustParsePrefix("10.240.0.0/16")}
	rules := Rules(sources, []int{80, 443}, nil, testHost)
	tenantDrop := strings.Index(rules, "ip saddr { 10.240.0.0/16 } counter drop")
	for _, want := range []string{
		"\t\tiif lo accept\n\t\tct state established,related accept\n\t\tct state invalid drop\n",
		"meta l4proto { icmp, ipv6-icmp } accept",
		"udp sport 67 udp dport 68 accept",
		"ip6 saddr fe80::/10 udp sport 547 udp dport 546 accept",
		"\t\ttcp dport { 22, 80, 443 } accept\n",
		"\t\tudp dport { 51820 } accept\n",
		"ip saddr { 10.0.0.0/8, 100.64.0.0/10, 172.16.0.0/12, 192.168.0.0/16 } tcp dport { 7400, 7402 } accept",
		"ip6 saddr { fc00::/7, fe80::/10 } tcp dport { 7400, 7402 } accept",
		"\t\tcounter drop\n\t}\n\tchain forward",
	} {
		index := strings.Index(rules, want)
		if index < 0 {
			t.Errorf("rules lack %q:\n%s", want, rules)
		} else if index < tenantDrop {
			t.Errorf("%q comes before the tenant rules:\n%s", want, rules)
		}
	}
	if strings.Contains(rules, "udp dport { } ") || strings.Contains(rules, "{  }") {
		t.Errorf("rules hold an empty set:\n%s", rules)
	}
	if without := Rules(sources, []int{80, 443}, nil, nil); strings.Contains(without, "iif lo") || !strings.Contains(without, "ip saddr { 10.240.0.0/16 } counter drop\n\t}\n\tchain forward") {
		t.Errorf("rules without a host:\n%s", without)
	}
}

// OpenTCP reads back, from nft's listing of the table, the TCP ports the
// host rules open to everyone.
func TestOpenTCP(t *testing.T) {
	listing := `table inet murder {
	chain input {
		type filter hook input priority filter; policy accept;
		ip saddr { 10.240.0.0/16 } tcp dport { 80, 443 } accept
		tcp dport { 22, 80, 443, 2222 } accept
		udp dport 51820 accept
		ip saddr { 10.0.0.0/8 } tcp dport { 7400, 7402 } accept
		counter packets 0 bytes 0 drop
	}
}
`
	if ports := OpenTCP(listing); !slices.Equal(ports, []int{22, 80, 443, 2222}) {
		t.Fatalf("ports %v", ports)
	}
	if ports := OpenTCP("\t\ttcp dport 2222 accept\n"); !slices.Equal(ports, []int{2222}) {
		t.Fatalf("one port %v", ports)
	}
	// What Rules writes is what OpenTCP reads.
	script := Rules([]netip.Prefix{netip.MustParsePrefix("10.240.0.0/16")}, []int{80, 443}, nil, &Host{PublicTCP: []int{2222, 22, 80, 443}, PrivateTCP: []int{7400}})
	if ports := OpenTCP(script); !slices.Equal(ports, []int{22, 80, 443, 2222}) {
		t.Fatalf("from Rules %v", ports)
	}
}
