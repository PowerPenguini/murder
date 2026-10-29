package firewall

import (
	"net/netip"
	"os/exec"
	"strings"
	"testing"
)

func TestRules(t *testing.T) {
	rules := Rules(netip.MustParsePrefix("10.240.0.0/16"), []int{443, 80, 80})
	for _, want := range []string{
		"table inet murder\ndelete table inet murder\ntable inet murder {",
		"ip saddr 10.240.0.0/16 ct state established,related accept",
		"ip saddr 10.240.0.0/16 tcp dport { 80, 443 } accept",
		"ip saddr 10.240.0.0/16 counter drop",
		"ip saddr 10.240.0.0/16 ip daddr { 0.0.0.0/8, 10.0.0.0/8, 100.64.0.0/10, 127.0.0.0/8, 169.254.0.0/16, 172.16.0.0/12, 192.0.0.0/24, 192.168.0.0/16, 198.18.0.0/15, 224.0.0.0/3 } counter drop",
	} {
		if !strings.Contains(rules, want) {
			t.Errorf("rules lack %q:\n%s", want, rules)
		}
	}
	// The established rule comes first, so replies to the edge pass.
	if strings.Index(rules, "established") > strings.Index(rules, "counter drop") {
		t.Fatalf("rules drop before accepting replies:\n%s", rules)
	}
	if without := Rules(netip.MustParsePrefix("10.240.0.0/16"), nil); strings.Contains(without, "dport") {
		t.Fatalf("rules without edge ports:\n%s", without)
	}
}

// nft checks the script's syntax without loading it (it needs no root).
func TestRulesParse(t *testing.T) {
	if _, err := exec.LookPath("nft"); err != nil {
		t.Skip("nft is not installed")
	}
	command := exec.Command("nft", "--check", "-f", "-")
	command.Stdin = strings.NewReader(Rules(netip.MustParsePrefix("10.240.0.0/16"), []int{80, 443}))
	output, err := command.CombinedOutput()
	if err != nil && strings.Contains(string(output), "Operation not permitted") {
		t.Skip("nft --check needs privileges here")
	}
	if err != nil {
		t.Fatalf("nft --check: %v\n%s", err, output)
	}
}
