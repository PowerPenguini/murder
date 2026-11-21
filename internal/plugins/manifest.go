package plugins

import (
	"errors"
	"fmt"
	"net/textproto"
	"regexp"
	"slices"
	"strings"
)

// ABI is the version of the interface between the edge and a plugin's
// module (see docs/plugins.md): what the module exports and the JSON it
// takes and gives. A module says the version it was built for in its
// manifest; an edge runs only versions it knows.
const ABI = 1

// The bounds of a plugin's module: its size, the body of a request it
// reads, and the memory an instance of it has.
const (
	MaxModule     = 16 << 20
	MaxModuleBody = 4 << 20
	// moduleMemory is the memory an instance may grow to, in pages of
	// 64 KB: a body of 16 MB, read as JSON by a module built with Go,
	// takes about 40 MB.
	moduleMemory = 2048
)

// Manifest is what a plugin's module says of itself: where its requests
// go, where the key goes on them, and what it counts. The agent reads it
// once, as the module is installed, and the edge holds the module to it.
type Manifest struct {
	// ABI is the interface the module was built for (see ABI).
	ABI int `json:"abi"`
	// Name and Version say what the module is, as anthropic 1.2.0.
	Name    string `json:"name"`
	Version string `json:"version,omitempty"`
	// Hosts are the hosts its requests go to, over HTTPS: a name, or "*."
	// and a domain for every name below it. The key goes to no other. A
	// plugin's own hosts, in its spec, replace them.
	Hosts []string `json:"hosts"`
	// Key is where the key goes on each request.
	Key KeyPlace `json:"key"`
	// Metrics are what it counts, beyond the requests Murder counts: the
	// metrics a plugin's limits may name.
	Metrics []string `json:"metrics,omitempty"`
	// RequestBody is the most bytes of a request's body it reads, at most
	// MaxModuleBody; 0 reads none, and bodies pass on as they come.
	RequestBody int64 `json:"requestBody,omitempty"`
	// UI says how the panel shows the plugin: none says only the
	// module's name and its metrics' names.
	UI *UI `json:"ui,omitempty"`
}

// KeyPlace is where a key goes on a request: in a header, after a prefix,
// as in Authorization: Bearer KEY, or in a parameter of the query.
type KeyPlace struct {
	Header string `json:"header,omitempty"`
	Prefix string `json:"prefix,omitempty"`
	Query  string `json:"query,omitempty"`
}

// String says where the key goes, as plugin show says it.
func (k KeyPlace) String() string {
	if k.Query != "" {
		return "query " + k.Query
	}
	return "header " + textproto.CanonicalMIMEHeaderKey(k.Header) + ": " + k.Prefix + "KEY"
}

var (
	moduleName  = regexp.MustCompile(`^[a-z0-9][a-z0-9._-]{0,63}$`)
	metricName  = regexp.MustCompile(`^[a-z][a-z0-9_]*(\.[a-z0-9_]+)*$`)
	queryName   = regexp.MustCompile(`^[A-Za-z0-9._-]{1,64}$`)
	headerToken = regexp.MustCompile("^[!#$%&'*+.^_`|~0-9A-Za-z-]{1,64}$")
)

// reservedHeaders are headers a module may neither set nor have the key
// put in: they say how the request is carried, or whose it is.
var reservedHeaders = []string{
	"Host", "Content-Length", "Transfer-Encoding", "Connection", "Upgrade", "Te", "Trailer",
	"Keep-Alive", "Proxy-Connection", "Proxy-Authorization", "Cookie", "Forwarded", "X-Forwarded-For",
	"X-Forwarded-Host", "X-Forwarded-Proto", "Accept-Encoding",
}

func reservedHeader(name string) bool {
	return slices.Contains(reservedHeaders, textproto.CanonicalMIMEHeaderKey(name))
}

// Validate says what is wrong with a manifest.
func (m Manifest) Validate() error {
	var problems []string
	if m.ABI != ABI {
		problems = append(problems, fmt.Sprintf("the module was built for version %d of the plugin interface; this release runs version %d", m.ABI, ABI))
	}
	if !moduleName.MatchString(m.Name) {
		problems = append(problems, fmt.Sprintf("name %q must be 1 to 64 lowercase letters, digits and ._-", m.Name))
	}
	if len(m.Version) > 64 || strings.ContainsFunc(m.Version, func(r rune) bool { return r < ' ' || r > '~' }) {
		problems = append(problems, "the version must be at most 64 printable characters")
	}
	if len(m.Hosts) == 0 || len(m.Hosts) > 64 {
		problems = append(problems, "a module names from 1 to 64 hosts its requests go to")
	}
	for _, host := range m.Hosts {
		if err := validHost(host); err != nil {
			problems = append(problems, err.Error())
		}
	}
	switch key := m.Key; {
	case key.Header != "" && key.Query != "", key.Header == "" && key.Query == "":
		problems = append(problems, "the key goes either in a header or in the query")
	case key.Header != "" && (!headerToken.MatchString(key.Header) || reservedHeader(key.Header)):
		problems = append(problems, fmt.Sprintf("header %q cannot carry the key", key.Header))
	case key.Query != "" && !queryName.MatchString(key.Query):
		problems = append(problems, fmt.Sprintf("query parameter %q must be 1 to 64 letters, digits and ._-", key.Query))
	case key.Query != "" && key.Prefix != "":
		problems = append(problems, "a prefix is for a key in a header")
	case len(key.Prefix) > 32 || strings.ContainsFunc(key.Prefix, func(r rune) bool { return r < ' ' || r > '~' }):
		problems = append(problems, "the key's prefix must be at most 32 printable characters")
	}
	if len(m.Metrics) > 32 {
		problems = append(problems, "a module counts at most 32 metrics")
	}
	for _, metric := range m.Metrics {
		if len(metric) > 64 || !metricName.MatchString(metric) {
			problems = append(problems, fmt.Sprintf("metric %q must be lowercase words of letters, digits and _, joined by dots, at most 64 characters", metric))
		}
	}
	if m.UI != nil {
		if err := m.UI.validate(m.Metrics); err != nil {
			problems = append(problems, err.Error())
		}
	}
	if m.RequestBody < 0 || m.RequestBody > MaxModuleBody {
		problems = append(problems, fmt.Sprintf("a module reads at most %d MB of a request's body", MaxModuleBody>>20))
	}
	if len(problems) > 0 {
		return errors.New(strings.Join(problems, "; "))
	}
	return nil
}

// MetricsOf are the metrics a plugin counts, which its limits may name:
// its kind's, or for a module, the requests and the module's.
func MetricsOf(spec Spec, manifest *Manifest) []string {
	if spec.Kind != KindWasm {
		return Metrics[spec.Kind]
	}
	metrics := []string{MetricRequests}
	if manifest != nil {
		for _, metric := range manifest.Metrics {
			if !slices.Contains(metrics, metric) {
				metrics = append(metrics, metric)
			}
		}
	}
	return metrics
}

// hostsOf are the hosts a module's requests may go to: the plugin's own,
// or else the module's.
func hostsOf(spec Spec, manifest Manifest) []string {
	if len(spec.Hosts) > 0 {
		return spec.Hosts
	}
	return manifest.Hosts
}
