package domains

import (
	"errors"
	"net/netip"
	"reflect"
	"strings"
	"testing"
)

func TestNormalize(t *testing.T) {
	for given, want := range map[string]string{
		"app.customer.com":      "app.customer.com",
		"  App.Customer.COM.  ": "app.customer.com",
		"bücher.example":        "xn--bcher-kva.example",
		"XN--BCHER-KVA.example": "xn--bcher-kva.example",
		"shop.example.co.uk":    "shop.example.co.uk",
		"example.com":           "example.com",
		"a-b.c-d.example":       "a-b.c-d.example",
		"ｆｕｌｌｗｉｄｔｈ.example.com": "fullwidth.example.com",
	} {
		got, err := Normalize(given, "murder.example.net")
		if err != nil || got != want {
			t.Errorf("Normalize(%q) = %q, %v; want %q", given, got, err, want)
		}
	}
	for given, why := range map[string]string{
		"":                                 "empty",
		"https://app.customer.com":         "without https://",
		"app.customer.com/login":           "without https://",
		"*.customer.com":                   "wildcard",
		"app.customer.com:8443":            "port",
		"203.0.113.10":                     "address",
		"[2001:db8::1]":                    "address",
		"localhost":                        "single label",
		"customer":                         "single label",
		"murder.example.net":               "base domain",
		"acme.murder.example.net":          "base domain",
		"deep.acme.MURDER.example.net.":    "base domain",
		"co.uk":                            "public suffix",
		"-app.customer.com":                "not a valid host name",
		"app..customer.com":                "not a valid host name",
		"app.customer.123":                 "number",
		"app_1.customer.com":               "not a valid host name",
		strings.Repeat("a", 64) + ".com":   "not a valid host name",
		strings.Repeat("abcd.", 51) + "io": "not a valid host name",
	} {
		_, err := Normalize(given, "murder.example.net")
		if !errors.Is(err, ErrName) || !strings.Contains(err.Error(), why) {
			t.Errorf("Normalize(%q) = %v; want a refusal saying %q", given, err, why)
		}
	}
}

func TestDisplayAndApex(t *testing.T) {
	if got := Display("xn--bcher-kva.example"); got != "bücher.example" {
		t.Errorf("Display = %q", got)
	}
	if got := Display("app.example.com"); got != "app.example.com" {
		t.Errorf("Display = %q", got)
	}
	for name, apex := range map[string]bool{"example.com": true, "example.co.uk": true, "app.example.com": false, "a.b.example.co.uk": false} {
		if Apex(name) != apex {
			t.Errorf("Apex(%q) = %v", name, !apex)
		}
	}
}

func TestRecords(t *testing.T) {
	public := []netip.Addr{netip.MustParseAddr("203.0.113.10"), netip.MustParseAddr("2001:db8::10")}
	if got := Records("app.customer.com", "acme.murder.example.net", public); !reflect.DeepEqual(got, []Record{{"CNAME", "app.customer.com", "acme.murder.example.net"}}) {
		t.Errorf("below the apex: %+v", got)
	}
	want := []Record{{"A", "customer.com", "203.0.113.10"}, {"AAAA", "customer.com", "2001:db8::10"}}
	if got := Records("customer.com", "acme.murder.example.net", public); !reflect.DeepEqual(got, want) {
		t.Errorf("the apex: %+v", got)
	}
	if got := Records("customer.com", "acme.murder.example.net", nil); !reflect.DeepEqual(got, []Record{{"ALIAS", "customer.com", "acme.murder.example.net"}}) {
		t.Errorf("the apex without addresses: %+v", got)
	}
}
