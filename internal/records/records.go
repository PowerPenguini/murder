// Package records keeps the base domain's wildcard DNS records naming the
// public addresses of the nodes that serve. The cluster's leader decides
// which nodes serve; this package only writes the records, one address
// family at a time and never leaving one empty, through the DNS provider's
// API.
package records

import (
	"context"
	"fmt"
	"net/netip"
	"slices"
	"strings"
	"time"
)

// Publisher writes the records.
type Publisher interface {
	// Publish makes the records name addresses: the A records the IPv4
	// ones, the AAAA records the IPv6 ones. A family without addresses is
	// left as it is, never emptied.
	Publish(ctx context.Context, addresses []netip.Addr) error
}

// Wildcard is the name, relative to zone, of the records every tenant's
// host name falls under: "*" when the base domain is the zone, "*.<sub>"
// when it lies within it.
func Wildcard(baseDomain, zone string) (string, error) {
	baseDomain = strings.ToLower(strings.TrimSuffix(baseDomain, "."))
	zone = strings.ToLower(strings.TrimSuffix(zone, "."))
	switch {
	case zone == "":
		return "", fmt.Errorf("no DNS zone")
	case baseDomain == zone:
		return "*", nil
	case strings.HasSuffix(baseDomain, "."+zone):
		return "*." + strings.TrimSuffix(baseDomain, "."+zone), nil
	}
	return "", fmt.Errorf("the base domain %s is not in the zone %s", baseDomain, zone)
}

// Relative is name relative to zone: "panel" for panel.example.com in
// example.com.
func Relative(name, zone string) (string, error) {
	name = strings.ToLower(strings.TrimSuffix(name, "."))
	zone = strings.ToLower(strings.TrimSuffix(zone, "."))
	relative, ok := strings.CutSuffix(name, "."+zone)
	if zone == "" || !ok || relative == "" {
		return "", fmt.Errorf("the name %s is not in the zone %s", name, zone)
	}
	return relative, nil
}

// families splits addresses into the values of the A and of the AAAA
// records, each sorted and without repeats.
func families(addresses []netip.Addr) (v4, v6 []string) {
	sorted := slices.Clone(addresses)
	slices.SortFunc(sorted, func(a, b netip.Addr) int { return a.Compare(b) })
	for _, address := range slices.Compact(sorted) {
		address = address.Unmap()
		switch {
		case !address.IsValid():
		case address.Is4():
			v4 = append(v4, address.String())
		default:
			v6 = append(v6, address.String())
		}
	}
	return v4, v6
}

// TTL is how long resolvers keep the records unless configured otherwise:
// short, so a dead node's address leaves caches soon.
const TTL = time.Minute
