// Package domains gives tenants host names of their own, outside the base
// domain, such as app.customer.com. Names are normalised and checked here;
// the cluster's leader checks that a name points at the cluster before it
// orders a certificate for it over ACME (HTTP-01), keeps the certificate
// in the replicated state, its private key sealed with the master key, and
// renews it. Every edge routes the name to its tenant and serves the
// certificate.
package domains

import (
	"errors"
	"fmt"
	"net/netip"
	"strings"

	"golang.org/x/net/idna"
	"golang.org/x/net/publicsuffix"
)

// ErrName refuses a name that cannot be a tenant's domain.
var ErrName = errors.New("not a domain a tenant can have")

// profile maps a name as a browser's address bar would (case, width,
// Unicode normalisation) and checks it as IDNA 2008 allows, so a name is
// stored once, in its ASCII form, however it was typed.
var profile = idna.New(idna.MapForLookup(), idna.Transitional(false), idna.BidiRule(), idna.ValidateLabels(true), idna.StrictDomainName(true), idna.VerifyDNSLength(true))

// Normalize returns name as a tenant's domain is kept: lowercase ASCII,
// IDNA's A-labels, no trailing dot. It refuses what cannot be one: a URL,
// a port, a wildcard, an address, a single label, or a name that is
// baseDomain or below it, which the wildcard name serves already.
func Normalize(name, baseDomain string) (string, error) {
	given := name
	name = strings.TrimSpace(name)
	refuse := func(why string) (string, error) {
		return "", fmt.Errorf("%w: %q %s", ErrName, given, why)
	}
	switch {
	case name == "":
		return refuse("is empty")
	case strings.Contains(name, "://") || strings.ContainsAny(name, "/?#@"):
		return refuse("is not a host name: give the name alone, such as app.example.com, without https:// or a path")
	case strings.Contains(name, "*"):
		return refuse("has a wildcard: give each host name of its own")
	}
	if _, err := netip.ParseAddr(strings.Trim(name, "[]")); err == nil {
		return refuse("is an address: give a host name that points at the cluster")
	}
	if strings.Contains(name, ":") {
		return refuse("has a port: the edge serves ports 80 and 443")
	}
	name = strings.TrimSuffix(name, ".")
	ascii, err := profile.ToASCII(name)
	if err != nil {
		return refuse("is not a valid host name: " + err.Error())
	}
	ascii = strings.ToLower(ascii)
	if err := checkLabels(ascii); err != nil {
		return refuse(err.Error())
	}
	base := strings.ToLower(strings.TrimSuffix(baseDomain, "."))
	if base != "" && (ascii == base || strings.HasSuffix(ascii, "."+base)) {
		return refuse("is the base domain or below it: its tenants have their names there already")
	}
	if suffix, _ := publicsuffix.PublicSuffix(ascii); suffix == ascii {
		return refuse("is a public suffix, which no one owns alone")
	}
	return ascii, nil
}

// checkLabels checks a name in ASCII: two labels at least, each of 1 to 63
// letters, digits and hyphens, not beginning or ending with a hyphen, and
// a last label that is not all digits (no address).
func checkLabels(name string) error {
	if len(name) > 253 {
		return errors.New("is longer than 253 characters")
	}
	labels := strings.Split(name, ".")
	if len(labels) < 2 {
		return errors.New("has a single label: give a whole name, such as app.example.com")
	}
	for _, label := range labels {
		if label == "" || len(label) > 63 {
			return errors.New("has a label that is empty or longer than 63 characters")
		}
		if label[0] == '-' || label[len(label)-1] == '-' {
			return fmt.Errorf("has a label %q that begins or ends with a hyphen", label)
		}
		for _, c := range label {
			if !(c >= 'a' && c <= 'z' || c >= '0' && c <= '9' || c == '-') {
				return fmt.Errorf("has a label %q with %q in it", label, c)
			}
		}
	}
	last := labels[len(labels)-1]
	if strings.Trim(last, "0123456789") == "" {
		return errors.New("ends in a number: give a host name, not an address")
	}
	return nil
}

// Display is name as people read it: Unicode where it was given so, such
// as bücher.example for xn--bcher-kva.example.
func Display(name string) string {
	if unicode, err := idna.Display.ToUnicode(name); err == nil {
		return unicode
	}
	return name
}

// Apex reports whether name is a registrable domain itself, such as
// example.com or example.co.uk, rather than a name below one: its zone's
// apex, where DNS allows no CNAME.
func Apex(name string) bool {
	registrable, err := publicsuffix.EffectiveTLDPlusOne(name)
	return err == nil && registrable == name
}

// Record is a DNS record to create for a domain.
type Record struct {
	Type  string `json:"type"`
	Name  string `json:"name"`
	Value string `json:"value"`
}

// Records are the DNS records that point a domain at the cluster: a CNAME
// to the tenant's own name, or, for a zone's apex, which may have no
// CNAME, A and AAAA records with the nodes' public addresses.
func Records(name, tenantHost string, public []netip.Addr) []Record {
	if !Apex(name) {
		return []Record{{Type: "CNAME", Name: name, Value: tenantHost}}
	}
	var records []Record
	for _, address := range public {
		kind := "A"
		if address.Unmap().Is6() {
			kind = "AAAA"
		}
		records = append(records, Record{Type: kind, Name: name, Value: address.Unmap().String()})
	}
	if len(records) == 0 {
		// The nodes' addresses are not known: the name of the tenant
		// leads to them, through a provider's ALIAS or ANAME record.
		records = append(records, Record{Type: "ALIAS", Name: name, Value: tenantHost})
	}
	return records
}
