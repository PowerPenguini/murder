package main

import (
	"net/url"
	"testing"
)

func TestTransactionsFollowHEREsPricing(t *testing.T) {
	cases := []struct {
		host  string
		query url.Values
		body  string
		want  int64
	}{
		{"geocode.search.hereapi.com", nil, "", 1},
		{"matrix.router.hereapi.com", nil, `{"origins":[{},{},{}],"destinations":[{},{}]}`, 6},
		{"matrix.router.hereapi.com", nil, `{"origins":[{},{},{},{}]}`, 16},
		{"matrix.router.hereapi.com", nil, `{"origins":[{},{},{},{},{}],"destinations":[{},{},{},{},{},{},{},{},{},{}]}`, 50},
		{"isoline.router.hereapi.com", url.Values{"range[values]": {"300,600,900"}}, "", 3},
		{"multi-revgeocode.search.hereapi.com", nil, "id=1&at=1,2\nid=2&at=3,4\n", 2},
		{"wps.hereapi.com", url.Values{"start": {"a"}, "destination1": {"b"}, "end": {"c"}}, "", 1},
	}
	for _, c := range cases {
		if got := transactions(c.host, c.query, []byte(c.body)); got != c.want {
			t.Errorf("%s %v %s: %d, not %d", c.host, c.query, c.body, got, c.want)
		}
	}
}

func TestAWildcardAllowsNamesBelowTheDomainOnly(t *testing.T) {
	hosts := []string{"*.hereapi.com", "account.api.here.com"}
	for host, want := range map[string]string{
		"geocode.search.hereapi.com": "*.hereapi.com",
		"hereapi.com":                "",
		"evilhereapi.com":            "",
		"x.hereapi.com.evil.com":     "",
		"account.api.here.com":       "account.api.here.com",
		"api.here.com":               "",
		"a_b.hereapi.com":            "",
		".hereapi.com":               "",
	} {
		if got := hostRule(hosts, host); got != want {
			t.Errorf("%s: %q, not %q", host, got, want)
		}
	}
	if service("geocode.search.hereapi.com") != "geocode.search" || service("*.hereapi.com") != "*.hereapi.com" {
		t.Error("services")
	}
}
