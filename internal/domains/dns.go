package domains

import (
	"context"
	"errors"
	"fmt"
	"net"
	"net/netip"
	"slices"
	"strings"
	"time"
)

// netResolver is the system's resolver.
var netResolver = net.DefaultResolver

// Resolver looks names up in DNS; *net.Resolver is one.
type Resolver interface {
	LookupCNAME(ctx context.Context, host string) (string, error)
	LookupNetIP(ctx context.Context, network, host string) ([]netip.Addr, error)
}

// NewResolver is a resolver that asks the DNS server at address
// (host:port) only, over UDP and TCP: say a test's.
func NewResolver(address string) *net.Resolver {
	return &net.Resolver{PreferGo: true, Dial: func(ctx context.Context, network, _ string) (net.Conn, error) {
		var dialer net.Dialer
		return dialer.DialContext(ctx, network, address)
	}}
}

// Target is what a domain must lead to in DNS to point at the cluster.
type Target struct {
	// Names are the names a CNAME may lead it to: the tenant's own name
	// below the base domain, and the base domain.
	Names []string
	// Addresses are the nodes' public addresses: every address the name
	// has must be one of them.
	Addresses []netip.Addr
}

// Result is what a check of a domain's DNS found.
type Result struct {
	// OK is set when the name points at the cluster.
	OK bool
	// Said is what its DNS shows, in words.
	Said string
}

// checkTimeout bounds one check.
const checkTimeout = 10 * time.Second

// Check looks name up and says whether it points at the cluster: a CNAME
// that leads it to one of target's names, or addresses that are all the
// nodes'. A CA asks any of its addresses, so one that is not the
// cluster's fails it.
func Check(ctx context.Context, resolver Resolver, name string, target Target) Result {
	ctx, cancel := context.WithTimeout(ctx, checkTimeout)
	defer cancel()
	canonical, err := resolver.LookupCNAME(ctx, name)
	if err != nil {
		return Result{Said: lookupFailed(name, err)}
	}
	canonical = strings.ToLower(strings.TrimSuffix(canonical, "."))
	via := ""
	if canonical != "" && canonical != name {
		for _, wanted := range target.Names {
			if canonical == strings.ToLower(strings.TrimSuffix(wanted, ".")) {
				return Result{OK: true, Said: "a CNAME leads it to " + canonical}
			}
		}
		via = " through a CNAME to " + canonical
	}
	addresses, err := resolver.LookupNetIP(ctx, "ip", name)
	if err != nil {
		return Result{Said: lookupFailed(name, err)}
	}
	if len(addresses) == 0 {
		return Result{Said: name + " has no A or AAAA records" + via}
	}
	ours := map[netip.Addr]bool{}
	for _, address := range target.Addresses {
		ours[address.Unmap()] = true
	}
	var foreign []string
	for _, address := range addresses {
		if !ours[address.Unmap()] {
			foreign = append(foreign, address.Unmap().String())
		}
	}
	slices.Sort(foreign)
	foreign = slices.Compact(foreign)
	switch {
	case len(foreign) == 0:
		return Result{OK: true, Said: "its addresses are the cluster's" + via}
	case len(target.Addresses) == 0:
		return Result{Said: fmt.Sprintf("it points at %s%s, and the nodes' public addresses are not known (cluster.publicAddresses): only a CNAME to %s is checked", strings.Join(foreign, ", "), via, strings.Join(target.Names, " or "))}
	default:
		return Result{Said: fmt.Sprintf("it points at %s%s, which no node of the cluster has", strings.Join(foreign, ", "), via)}
	}
}

// lookupFailed says why a name could not be looked up.
func lookupFailed(name string, err error) string {
	var dnsErr *net.DNSError
	if errors.As(err, &dnsErr) && dnsErr.IsNotFound {
		return name + " has no DNS records yet"
	}
	if errors.As(err, &dnsErr) && dnsErr.IsTimeout {
		return "its DNS did not answer in time"
	}
	return "its DNS could not be read: " + err.Error()
}
