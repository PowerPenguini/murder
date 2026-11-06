// Package firewall keeps tenants away from the host and from private
// networks. Docker already isolates tenant networks from each other, but a
// container can still open connections to every service the host listens
// on, to other servers on a private network and to the cloud's metadata
// service. The rules here allow a tenant only the internet, the edge ports,
// the host's DNS resolvers and replies to the connections the host opens
// to it.
//
// With a Host, the rules also keep the rest of the world to the ports the
// host serves: anything else that reaches the host is dropped.
package firewall

import (
	"bufio"
	"bytes"
	"context"
	"fmt"
	"net/netip"
	"os"
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

// privateSources are the networks a Host's private ports are open to: the
// private ranges cluster addresses are taken from, for IPv4 and IPv6.
var privateSources = [2][]string{
	{"10.0.0.0/8", "100.64.0.0/10", "172.16.0.0/12", "192.168.0.0/16"},
	{"fc00::/7", "fe80::/10"},
}

// Host lists what may reach the host itself, from anywhere but a tenant
// network; the rest is dropped. Replies to connections the host opened,
// loopback, ICMP (path MTU discovery and, for IPv6, neighbour discovery)
// and DHCP replies always pass.
type Host struct {
	// PublicTCP and PublicUDP are open to every address: SSH, the edge,
	// the panel's tunnel.
	PublicTCP, PublicUDP []int
	// PrivateTCP and PrivateUDP are open to private networks only: other
	// nodes on the cluster port, a scraper on a metrics port.
	PrivateTCP, PrivateUDP []int
}

// Rules returns the nftables script that replaces Murder's table. From
// the tenant networks (sources: the pool and any tenant subnet outside it,
// left from an earlier pool), traffic to the host is dropped unless it answers a connection the
// host opened (the edge proxy and health checks) or goes to an edge port,
// so a tenant can still reach its own public address; traffic to a private
// destination is dropped unless it is DNS to one of resolvers, which
// Docker's embedded DNS server forwards to from the tenant's network. With
// a host (nil leaves the host's own traffic alone), what comes from
// elsewhere is then dropped unless host lets it in. The script is one
// transaction, so a script nft refuses leaves the rules loaded before.
func Rules(sources []netip.Prefix, edgePorts []int, resolvers []netip.Addr, host *Host) string {
	masked := make([]netip.Prefix, len(sources))
	for i, prefix := range sources {
		masked[i] = prefix.Masked()
	}
	source := "ip saddr { " + strings.Join(sortedText(masked, netip.Prefix.String, comparePrefixes), ", ") + " }"
	var script strings.Builder
	fmt.Fprintf(&script, "table inet %s\ndelete table inet %s\ntable inet %s {\n", Table, Table, Table)
	script.WriteString("\tchain input {\n\t\ttype filter hook input priority filter; policy accept;\n")
	fmt.Fprintf(&script, "\t\t%s ct state established,related accept\n", source)
	if ports := portList(edgePorts); ports != "" {
		fmt.Fprintf(&script, "\t\t%s tcp dport { %s } accept\n", source, ports)
	}
	if dns := dnsRule(source, resolvers); dns != "" {
		script.WriteString(dns)
	}
	fmt.Fprintf(&script, "\t\t%s counter drop\n", source)
	if host != nil {
		script.WriteString(host.rules())
	}
	script.WriteString("\t}\n")
	script.WriteString("\tchain forward {\n\t\ttype filter hook forward priority filter; policy accept;\n")
	if dns := dnsRule(source, resolvers); dns != "" {
		script.WriteString(dns)
	}
	fmt.Fprintf(&script, "\t\t%s ip daddr { %s } counter drop\n\t}\n}\n", source, strings.Join(private, ", "))
	return script.String()
}

// rules lets in what h allows and drops the rest. They follow the tenant
// rules, which have already decided every packet from a tenant network.
func (h *Host) rules() string {
	var b strings.Builder
	b.WriteString("\t\tiif lo accept\n")
	b.WriteString("\t\tct state established,related accept\n")
	b.WriteString("\t\tct state invalid drop\n")
	b.WriteString("\t\tmeta l4proto { icmp, ipv6-icmp } accept\n")
	// A DHCP client's lease renewals; IPv6 ones come from a link-local
	// server.
	b.WriteString("\t\tudp sport 67 udp dport 68 accept\n")
	b.WriteString("\t\tip6 saddr fe80::/10 udp sport 547 udp dport 546 accept\n")
	for _, open := range []struct {
		protocol string
		ports    []int
	}{{"tcp", h.PublicTCP}, {"udp", h.PublicUDP}} {
		if list := portList(open.ports); list != "" {
			fmt.Fprintf(&b, "\t\t%s dport { %s } accept\n", open.protocol, list)
		}
	}
	for family, networks := range privateSources {
		match := [2]string{"ip", "ip6"}[family]
		for _, open := range []struct {
			protocol string
			ports    []int
		}{{"tcp", h.PrivateTCP}, {"udp", h.PrivateUDP}} {
			if list := portList(open.ports); list != "" {
				fmt.Fprintf(&b, "\t\t%s saddr { %s } %s dport { %s } accept\n", match, strings.Join(networks, ", "), open.protocol, list)
			}
		}
	}
	b.WriteString("\t\tcounter drop\n")
	return b.String()
}

// portList is ports for an nft set, sorted and without repeats; empty when
// there are none.
func portList(ports []int) string {
	return strings.Join(sortedText(ports, strconv.Itoa, func(a, b int) int { return a - b }), ", ")
}

func dnsRule(source string, resolvers []netip.Addr) string {
	addresses := sortedText(resolvers, netip.Addr.String, netip.Addr.Compare)
	if len(addresses) == 0 {
		return ""
	}
	return fmt.Sprintf("\t\t%s ip daddr { %s } meta l4proto { tcp, udp } th dport 53 accept\n", source, strings.Join(addresses, ", "))
}

func comparePrefixes(a, b netip.Prefix) int {
	if c := a.Addr().Compare(b.Addr()); c != 0 {
		return c
	}
	return a.Bits() - b.Bits()
}

// sortedText returns values sorted and without repeats, as text.
func sortedText[T comparable](values []T, text func(T) string, compare func(T, T) int) []string {
	values = slices.Clone(values)
	slices.SortFunc(values, compare)
	values = slices.Compact(values)
	out := make([]string, len(values))
	for i, value := range values {
		out[i] = text(value)
	}
	return out
}

// resolvConfs are where the host's DNS servers are listed: the classic file
// and, behind systemd-resolved's 127.0.0.53 stub, the upstream list Docker
// then uses.
var resolvConfs = []string{"/etc/resolv.conf", "/run/systemd/resolve/resolv.conf"}

// Resolvers returns the host's IPv4 DNS servers that a tenant's queries are
// forwarded to. Loopback ones are left out: Docker queries those from the
// host, not from the tenant's network.
func Resolvers() []netip.Addr {
	var resolvers []netip.Addr
	for _, path := range resolvConfs {
		data, err := os.ReadFile(path)
		if err != nil {
			continue
		}
		scanner := bufio.NewScanner(bytes.NewReader(data))
		for scanner.Scan() {
			fields := strings.Fields(scanner.Text())
			if len(fields) < 2 || fields[0] != "nameserver" {
				continue
			}
			if address, err := netip.ParseAddr(fields[1]); err == nil && address.Is4() && !address.IsLoopback() {
				resolvers = append(resolvers, address)
			}
		}
	}
	return resolvers
}

// Apply loads the rules with nft, replacing Murder's table at once.
func Apply(ctx context.Context, sources []netip.Prefix, edgePorts []int, resolvers []netip.Addr, host *Host) error {
	ctx, cancel := context.WithTimeout(ctx, 30*time.Second)
	defer cancel()
	command := exec.CommandContext(ctx, "nft", "-f", "-")
	command.Stdin = strings.NewReader(Rules(sources, edgePorts, resolvers, host))
	if output, err := command.CombinedOutput(); err != nil {
		return fmt.Errorf("load the tenant firewall with nft: %w: %s", err, strings.TrimSpace(string(output)))
	}
	return nil
}
