// Package firewall keeps tenants away from the host and from private
// networks. Docker already isolates tenant networks from each other, but a
// container can still open connections to every service the host listens
// on, to other servers on a private network and to the cloud's metadata
// service. The rules here allow a tenant only the internet, the edge ports
// and replies to the connections the host opens to it.
package firewall

import (
	"context"
	"fmt"
	"net/netip"
	"os/exec"
	"slices"
	"strconv"
	"strings"
	"time"
)

// Table is the nftables table Murder owns; nothing else is touched.
const Table = "murder"

// private are the destinations a tenant may not reach through the host:
// private, shared, link-local (the metadata service), loopback, benchmark,
// multicast and reserved ranges.
var private = []string{
	"0.0.0.0/8", "10.0.0.0/8", "100.64.0.0/10", "127.0.0.0/8", "169.254.0.0/16",
	"172.16.0.0/12", "192.0.0.0/24", "192.168.0.0/16", "198.18.0.0/15", "224.0.0.0/3",
}

// Rules returns the nftables script that replaces Murder's table: traffic
// from pool to the host is dropped unless it answers a connection the host
// opened (the edge proxy and health checks) or goes to an edge port, so a
// tenant can still reach its own public address; traffic from pool to a
// private destination is dropped. The script is one transaction.
func Rules(pool netip.Prefix, edgePorts []int) string {
	ports := slices.Clone(edgePorts)
	slices.Sort(ports)
	ports = slices.Compact(ports)
	source := "ip saddr " + pool.Masked().String()
	var script strings.Builder
	fmt.Fprintf(&script, "table inet %s\ndelete table inet %s\ntable inet %s {\n", Table, Table, Table)
	script.WriteString("\tchain input {\n\t\ttype filter hook input priority filter; policy accept;\n")
	fmt.Fprintf(&script, "\t\t%s ct state established,related accept\n", source)
	if len(ports) > 0 {
		names := make([]string, len(ports))
		for i, port := range ports {
			names[i] = strconv.Itoa(port)
		}
		fmt.Fprintf(&script, "\t\t%s tcp dport { %s } accept\n", source, strings.Join(names, ", "))
	}
	fmt.Fprintf(&script, "\t\t%s counter drop\n\t}\n", source)
	script.WriteString("\tchain forward {\n\t\ttype filter hook forward priority filter; policy accept;\n")
	fmt.Fprintf(&script, "\t\t%s ip daddr { %s } counter drop\n\t}\n}\n", source, strings.Join(private, ", "))
	return script.String()
}

// Apply loads the rules with nft, replacing Murder's table at once.
func Apply(ctx context.Context, pool netip.Prefix, edgePorts []int) error {
	ctx, cancel := context.WithTimeout(ctx, 30*time.Second)
	defer cancel()
	command := exec.CommandContext(ctx, "nft", "-f", "-")
	command.Stdin = strings.NewReader(Rules(pool, edgePorts))
	if output, err := command.CombinedOutput(); err != nil {
		return fmt.Errorf("load the tenant firewall with nft: %w: %s", err, strings.TrimSpace(string(output)))
	}
	return nil
}
