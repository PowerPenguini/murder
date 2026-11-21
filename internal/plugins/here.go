package plugins

import (
	"bytes"
	"encoding/json"
	"net/http"
	"net/url"
	"strings"
)

// hereHandler passes a request to HERE on, and what it asked: the
// transactions HERE bills it as, once answered.
type hereHandler struct {
	x            *exchange
	transactions int64
}

// hereKey is the query parameter HERE takes the key in.
const hereKey = "apiKey"

// request takes a request for /<host>/<path>: it goes to
// https://<host>/<path>, a host the plugin allows, with the tenant's own
// key taken out of its query.
func (h *hereHandler) request(r *http.Request, rest string) (*url.URL, error) {
	x := h.x
	host, path, _ := strings.Cut(rest, "/")
	host = strings.ToLower(host)
	if !x.spec.AllowsHost(host) {
		return nil, statusError{code: http.StatusBadRequest, message: "the path must begin with a HERE host the plugin allows, such as /geocode.search.hereapi.com/v1/geocode; " + host + " is not one"}
	}
	unescaped, err := url.PathUnescape(path)
	if err != nil || strings.Contains("/"+unescaped+"/", "/../") {
		return nil, statusError{code: http.StatusBadRequest, message: "the path is not one to pass on"}
	}
	// The query is passed on as it was written, but for the tenant's key:
	// parsed and encoded again, parameters with a ; would be lost.
	target := &url.URL{Scheme: "https", Host: host, Path: "/" + unescaped, RawPath: "/" + path, RawQuery: withoutKey(r.URL.RawQuery, hereKey)}
	x.detail = hereService(x.spec, host)
	var body []byte
	if r.Body != nil && r.Body != http.NoBody && serviceCountsBody(host) {
		if body, err = x.readBody(r); err != nil {
			return nil, err
		}
	}
	query, _ := url.ParseQuery(r.URL.RawQuery)
	h.transactions = hereTransactions(host, query, body)
	return target, nil
}

// hereService names a host's service: geocode.search for
// geocode.search.hereapi.com, or for a host a wildcard allows, the
// wildcard, so tenants cannot make up services without end.
func hereService(spec Spec, host string) string {
	rule := spec.hostRule(host)
	if strings.HasPrefix(rule, "*.") {
		return rule
	}
	if service, ok := strings.CutSuffix(rule, ".hereapi.com"); ok {
		return service
	}
	return rule
}

// serviceCountsBody reports whether HERE bills a request to host by what
// its body holds.
func serviceCountsBody(host string) bool {
	return strings.HasPrefix(host, "matrix.router.") || strings.HasPrefix(host, "multi-revgeocode.")
}

// hereTransactions counts a request as HERE bills it (its supplemental
// pricing, April 2026): one a request, except a matrix (origins times
// destinations, or five times the larger when both are five or more), an
// isoline request (one an isoline), a multi reverse geocode (one a
// coordinate) and waypoint sequencing (one per 100 waypoints). Other
// exceptions, such as tour planning and map attributes, count one a
// request.
func hereTransactions(host string, query url.Values, body []byte) int64 {
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
		ranges := strings.Split(query.Get("range[values]"), ",")
		count := int64(0)
		for _, value := range ranges {
			if strings.TrimSpace(value) != "" {
				count++
			}
		}
		return max(count, 1)
	case strings.HasPrefix(host, "multi-revgeocode."):
		count := int64(bytes.Count(body, []byte("at=")))
		return max(count, 1)
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

// authorize puts the key in the query.
func (h *hereHandler) authorize(out *http.Request) {
	// Errors are read uncompressed, to take the key out of them.
	out.Header.Set("Accept-Encoding", "identity")
	out.URL.RawQuery = withKey(out.URL.RawQuery, hereKey, h.x.key)
}

func (h *hereHandler) close() {}

// response counts the transactions of an answered request: HERE bills
// those it answers with success.
func (h *hereHandler) response(response *http.Response) {
	if response.StatusCode >= 200 && response.StatusCode < 300 {
		h.x.count(MetricTransactions, h.x.detail, h.transactions)
	}
	// An error may say what was asked, key and all.
	scrubErrorBody(response, h.x.key)
}
