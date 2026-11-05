package domains

import (
	"context"
	"net"
	"net/netip"
	"strings"
	"testing"
)

// fakeResolver answers from maps: a name's CNAME chain's end, and its
// addresses; a name in neither does not exist.
type fakeResolver struct {
	cnames    map[string]string
	addresses map[string][]string
	failing   error
}

func (f fakeResolver) LookupCNAME(_ context.Context, host string) (string, error) {
	if f.failing != nil {
		return "", f.failing
	}
	if target, ok := f.cnames[host]; ok {
		return target + ".", nil
	}
	if _, ok := f.addresses[host]; ok {
		return host + ".", nil
	}
	return "", &net.DNSError{Err: "no such host", Name: host, IsNotFound: true}
}

func (f fakeResolver) LookupNetIP(_ context.Context, _, host string) ([]netip.Addr, error) {
	if target, ok := f.cnames[host]; ok {
		host = target
	}
	texts, ok := f.addresses[host]
	if !ok {
		return nil, &net.DNSError{Err: "no such host", Name: host, IsNotFound: true}
	}
	var out []netip.Addr
	for _, text := range texts {
		out = append(out, netip.MustParseAddr(text))
	}
	return out, nil
}

func TestCheck(t *testing.T) {
	target := Target{
		Names:     []string{"acme.murder.example.net", "murder.example.net"},
		Addresses: []netip.Addr{netip.MustParseAddr("203.0.113.10"), netip.MustParseAddr("203.0.113.11"), netip.MustParseAddr("2001:db8::10")},
	}
	resolver := fakeResolver{
		cnames: map[string]string{
			"cname.customer.com":  "acme.murder.example.net",
			"base.customer.com":   "murder.example.net",
			"other.customer.com":  "beta.murder.example.net",
			"elsewhere.customer":  "host.parking.example",
			"node-cname.customer": "fsn-1.ops.example",
		},
		addresses: map[string][]string{
			"acme.murder.example.net": {"203.0.113.10"},
			"murder.example.net":      {"203.0.113.10"},
			"beta.murder.example.net": {"192.0.2.99"},
			"host.parking.example":    {"192.0.2.80"},
			"fsn-1.ops.example":       {"203.0.113.11", "2001:db8::10"},
			"ours.customer.com":       {"203.0.113.10", "::ffff:203.0.113.11", "2001:db8::10"},
			"mixed.customer.com":      {"203.0.113.10", "192.0.2.80"},
			"empty.customer.com":      {},
		},
	}
	for name, want := range map[string]Result{
		"cname.customer.com":  {OK: true, Said: "a CNAME leads it to acme.murder.example.net"},
		"base.customer.com":   {OK: true, Said: "a CNAME leads it to murder.example.net"},
		"node-cname.customer": {OK: true, Said: "its addresses are the cluster's through a CNAME to fsn-1.ops.example"},
		"ours.customer.com":   {OK: true, Said: "its addresses are the cluster's"},
		"other.customer.com":  {Said: "it points at 192.0.2.99 through a CNAME to beta.murder.example.net, which no node of the cluster has"},
		"elsewhere.customer":  {Said: "it points at 192.0.2.80 through a CNAME to host.parking.example, which no node of the cluster has"},
		"mixed.customer.com":  {Said: "it points at 192.0.2.80, which no node of the cluster has"},
		"empty.customer.com":  {Said: "empty.customer.com has no A or AAAA records"},
		"none.customer.com":   {Said: "none.customer.com has no DNS records yet"},
	} {
		if got := Check(t.Context(), resolver, name, target); got != want {
			t.Errorf("%s: %+v; want %+v", name, got, want)
		}
	}
	// Without the nodes' addresses, the tenant's own name's are the
	// cluster's.
	unknown := Target{Names: target.Names}
	if got := Check(t.Context(), resolver, "ours.customer.com", unknown); got.OK || !strings.Contains(got.Said, "which acme.murder.example.net does not") {
		t.Errorf("without the nodes' addresses, other addresses: %+v", got)
	}
	resolver.addresses["same.customer.com"] = []string{"203.0.113.10"}
	if got := Check(t.Context(), resolver, "same.customer.com", unknown); !got.OK {
		t.Errorf("without the nodes' addresses, the tenant's name's: %+v", got)
	}
	if got := Check(t.Context(), resolver, "cname.customer.com", unknown); !got.OK {
		t.Errorf("a CNAME without the nodes' addresses: %+v", got)
	}
	failing := fakeResolver{failing: &net.DNSError{Err: "i/o timeout", IsTimeout: true}}
	if got := Check(t.Context(), failing, "x.customer.com", target); got.OK || got.Said != "its DNS did not answer in time" {
		t.Errorf("a resolver that does not answer: %+v", got)
	}
}
