// Command here is Murder's plugin for HERE's location services: the key
// goes in the query, and requests and transactions, as HERE bills them,
// are counted by service. Murder's releases publish it as
// ghcr.io/powerpenguini/murder-here (see plugins/Dockerfile and
// docs/plugins.md).
//
// A tenant asks for /<host>/<path>, as
// /geocode.search.hereapi.com/v1/geocode?q=Berlin: it goes to
// https://<host>/<path>, a host the plugin allows. Its spec's hosts name
// other services in place of the default ones.
package main

import (
	"bytes"
	"encoding/json"
	"net/url"
	"regexp"
	"strings"

	"github.com/PowerPenguini/murder/pluginsdk"
)

func main() {
	pluginsdk.Register(manifest, forward)
	pluginsdk.Run()
}

// metricTransactions counts HERE's billing units.
const metricTransactions = "transactions"

var manifest = pluginsdk.Manifest{
	Name:    "here",
	Version: "1.0.0",
	Hosts:   hosts,
	Key:     pluginsdk.Key{Query: "apiKey"},
	Metrics: []string{metricTransactions},
	// A matrix and a multi reverse geocode are billed by what their body
	// holds.
	RequestBody: 16 << 20,
	UI: &pluginsdk.UI{
		Title:       "HERE",
		Description: "HERE's location services: requests, and transactions as HERE bills them, by service.",
		Detail:      "service",
		Metrics: []pluginsdk.MetricUI{
			{Name: metricTransactions, Label: "Transactions", Unit: "transactions"},
			{Name: "requests", Label: "Requests", Unit: "requests"},
		},
	},
}

// hosts are the HERE services a tenant reaches by default: search,
// routing, matrices, isolines, waypoint sequencing, map tiles and traffic,
// which keep nothing of a request. Those that keep data under the account
// (batch geocoding, geofencing, fleet and tour planning, Data Hub) are
// left out: every tenant would reach every other's.
var hosts = []string{
	"geocode.search.hereapi.com", "revgeocode.search.hereapi.com", "multi-revgeocode.search.hereapi.com",
	"autosuggest.search.hereapi.com", "autocomplete.search.hereapi.com", "discover.search.hereapi.com",
	"browse.search.hereapi.com", "lookup.search.hereapi.com",
	"router.hereapi.com", "matrix.router.hereapi.com", "isoline.router.hereapi.com", "intermodal.router.hereapi.com", "transit.router.hereapi.com",
	"wps.hereapi.com",
	"maps.hereapi.com", "traffic.maps.hereapi.com", "vector.hereapi.com", "traffic.vector.hereapi.com", "data.traffic.hereapi.com",
}

func forward(r *pluginsdk.Request) (*pluginsdk.Forward, error) {
	host, path, _ := strings.Cut(strings.TrimPrefix(r.Path, "/"), "/")
	host = strings.ToLower(host)
	rule := hostRule(r.Hosts, host)
	if rule == "" {
		return nil, pluginsdk.Refuse(400, "the path must begin with a HERE host the plugin allows, such as /geocode.search.hereapi.com/v1/geocode; %s is not one", host)
	}
	// The query is passed on as it was written: parsed and encoded again,
	// parameters with a ; would be lost. The edge took the tenant's key out
	// of it, and puts the plugin's in.
	to := &pluginsdk.Forward{URL: "https://" + host + "/" + path, Detail: service(rule)}
	if r.Query != "" {
		to.URL += "?" + r.Query
	}
	query, _ := url.ParseQuery(r.Query)
	to.Meter = &meter{service: to.Detail, transactions: transactions(host, query, r.Body)}
	return to, nil
}

var hostLabel = regexp.MustCompile(`^[a-z0-9]([a-z0-9-]{0,61}[a-z0-9])?$`)

// hostRule is the entry of hosts that allows host: the host itself, or a
// wildcard; empty for none.
func hostRule(hosts []string, host string) string {
	for _, allowed := range hosts {
		if domain, wildcard := strings.CutPrefix(allowed, "*."); wildcard {
			if below, ok := strings.CutSuffix(host, "."+domain); ok && below != "" {
				labels := strings.Split(below, ".")
				valid := true
				for _, label := range labels {
					valid = valid && hostLabel.MatchString(label)
				}
				if valid {
					return allowed
				}
			}
		} else if host == allowed {
			return allowed
		}
	}
	return ""
}

// service names the service of a host's rule: geocode.search for
// geocode.search.hereapi.com, or for a host a wildcard allows, the
// wildcard, so tenants cannot make up services without end.
func service(rule string) string {
	if strings.HasPrefix(rule, "*.") {
		return rule
	}
	if name, ok := strings.CutSuffix(rule, ".hereapi.com"); ok {
		return name
	}
	return rule
}

// transactions counts a request as HERE bills it (its supplemental
// pricing, April 2026): one a request, except a matrix (origins times
// destinations, or five times the larger when both are five or more), an
// isoline request (one an isoline), a multi reverse geocode (one a
// coordinate) and waypoint sequencing (one per 100 waypoints). Other
// exceptions, such as tour planning and map attributes, count one a
// request.
func transactions(host string, query url.Values, body []byte) int64 {
	switch {
	case strings.HasPrefix(host, "matrix.router."):
		var matrix struct {
			Origins      []json.RawMessage `json:"origins"`
			Destinations []json.RawMessage `json:"destinations"`
		}
		if json.Unmarshal(body, &matrix) != nil || len(matrix.Origins) == 0 {
			return 1
		}
		origins, destinations := int64(len(matrix.Origins)), int64(len(matrix.Destinations))
		if destinations == 0 {
			destinations = origins // a square matrix of the origins
		}
		if origins < 5 || destinations < 5 {
			return origins * destinations
		}
		return 5 * max(origins, destinations)
	case strings.HasPrefix(host, "isoline.router."):
		count := int64(0)
		for value := range strings.SplitSeq(query.Get("range[values]"), ",") {
			if strings.TrimSpace(value) != "" {
				count++
			}
		}
		return max(count, 1)
	case strings.HasPrefix(host, "multi-revgeocode."):
		return max(int64(bytes.Count(body, []byte("at="))), 1)
	case strings.HasPrefix(host, "wps."):
		waypoints := int64(0)
		for name := range query {
			if strings.HasPrefix(name, "destination") || name == "start" || name == "end" {
				waypoints++
			}
		}
		return max((waypoints+99)/100, 1)
	}
	return 1
}

// meter counts the transactions of a request HERE answered with success:
// those it bills.
type meter struct {
	service      string
	transactions int64
}

func (m *meter) Response(r *pluginsdk.Response, c *pluginsdk.Counter) bool {
	if r.Status >= 200 && r.Status < 300 {
		c.Count(metricTransactions, m.service, m.transactions)
	}
	return false
}

func (m *meter) Body([]byte, *pluginsdk.Counter) bool { return false }

func (m *meter) End(bool, *pluginsdk.Counter) {}
