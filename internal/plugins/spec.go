// Package plugins passes tenants' requests on to the APIs of providers
// such as OpenAI and HERE, with a key the tenant never sees, and counts
// and limits what each tenant uses of them. A plugin is configuration: a
// kind Murder knows how to meter, the provider's address, and the limits
// its tenants get. The edge serves it, on a port only tenants reach; the
// agent keeps what was used in the replicated state.
package plugins

import (
	"encoding/json"
	"errors"
	"fmt"
	"maps"
	"net/url"
	"regexp"
	"slices"
	"strconv"
	"strings"

	"github.com/PowerPenguini/murder/internal/spec"
	"github.com/PowerPenguini/murder/internal/strictyaml"
)

// The kinds of plugin: how requests are passed on and what is counted.
const (
	// KindOpenAI is OpenAI's API, or one that speaks it: a bearer key,
	// tokens counted from each response's usage.
	KindOpenAI = "openai"
	// KindHERE is HERE's location services: a key in the query, requests
	// and transactions counted by service.
	KindHERE = "here"
	// KindContainer is a plugin of one's own, run in a container on each
	// node: its code says where requests go and counts what they used
	// (see Manifest and docs/plugins.md).
	KindContainer = "container"
)

// Kinds are the kinds Murder meters.
var Kinds = []string{KindOpenAI, KindHERE, KindContainer}

// Spec is a plugin as its file says it: what `murder plugin apply` reads.
// The plugin is named after its file.
type Spec struct {
	// Kind is how its requests are passed on and metered (see Kinds).
	Kind string `json:"kind" yaml:"kind"`
	// Upstream is the provider's address, scheme and host, for a kind
	// that has one: OpenAI's is https://api.openai.com.
	Upstream string `json:"upstream,omitempty" yaml:"upstream,omitempty"`
	// Hosts are the host names requests may go to, for a kind whose
	// services each have their own, such as HERE's: a name, or "*." and a
	// domain for every name below it. HERE's are HEREHosts.
	Hosts []string `json:"hosts,omitempty" yaml:"hosts,omitempty"`
	// Allow are endpoints requests may go to beyond the kind's own, for a
	// kind that limits them, such as openai: a method and a path, such as
	// "POST /v1/batches", the path ending in /* for any one segment more.
	// What they use is counted as the kind reads it, which may be only
	// requests; and every tenant reaches what they keep with the same key.
	Allow []string `json:"allow,omitempty" yaml:"allow,omitempty"`
	// Image is the image of a plugin of kind container: in a plugin's
	// file, a reference such as ghcr.io/acme/plugin:1.2; installed, pinned
	// to its content, repository@sha256:<digest> (or a local sha256:<id>,
	// which only a node that has it runs).
	Image string `json:"image,omitempty" yaml:"image,omitempty"`
	// Config is what a plugin of kind container is told with each request,
	// as JSON: whatever its code takes.
	Config map[string]any `json:"config,omitempty" yaml:"config,omitempty"`
	// Limits are what each tenant may use, unless it has limits of its
	// own, or a key of its own (then only its own limits hold).
	Limits []Limit `json:"limits,omitempty" yaml:"limits,omitempty"`
}

// OpenAIEndpoints are the endpoints of OpenAI's API a tenant reaches: those
// whose answers say what they used, and the models. Those that keep or
// list what was sent (files, batches, stored responses and completions,
// fine-tuning, vector stores) are left out: every tenant would reach every
// other's, under the one key. So are those that spend without saying
// what (speech, the Realtime API's WebSockets).
var OpenAIEndpoints = []string{
	"POST /v1/chat/completions",
	"POST /v1/responses",
	"POST /v1/completions",
	"POST /v1/embeddings",
	"POST /v1/moderations",
	"POST /v1/images/generations",
	"POST /v1/images/edits",
	"POST /v1/images/variations",
	"POST /v1/audio/transcriptions",
	"POST /v1/audio/translations",
	"GET /v1/models",
	"GET /v1/models/*",
}

// HEREHosts are the HERE services a tenant reaches by default: search,
// routing, matrices, isolines, waypoint sequencing, map tiles and traffic,
// which keep nothing of a request. Those that keep data under the account
// (batch geocoding, geofencing, fleet and tour planning, Data Hub) are
// left out: every tenant would reach every other's.
var HEREHosts = []string{
	"geocode.search.hereapi.com", "revgeocode.search.hereapi.com", "multi-revgeocode.search.hereapi.com",
	"autosuggest.search.hereapi.com", "autocomplete.search.hereapi.com", "discover.search.hereapi.com",
	"browse.search.hereapi.com", "lookup.search.hereapi.com",
	"router.hereapi.com", "matrix.router.hereapi.com", "isoline.router.hereapi.com", "intermodal.router.hereapi.com", "transit.router.hereapi.com",
	"wps.hereapi.com",
	"maps.hereapi.com", "traffic.maps.hereapi.com", "vector.hereapi.com", "traffic.vector.hereapi.com", "data.traffic.hereapi.com",
}

// Limit caps what a tenant uses of a plugin's metric in a day or a
// calendar month, in UTC.
type Limit struct {
	// Metric is what is counted, such as tokens or requests (see
	// Metrics).
	Metric string `json:"metric" yaml:"metric"`
	// Per is the period: day or month.
	Per string `json:"per" yaml:"per"`
	// Amount is the most a tenant uses in the period.
	Amount int64 `json:"amount" yaml:"amount"`
	// Mode is what happens past it: hard refuses the tenant's next
	// requests (429) until the period ends or the limit is raised; soft
	// only says so, in the audit log and the metrics. Hard if left out.
	Mode string `json:"mode,omitempty" yaml:"mode,omitempty"`
}

// The periods and modes of a limit.
const (
	PerDay    = "day"
	PerMonth  = "month"
	ModeHard  = "hard"
	ModeSoft  = "soft"
	maxAmount = 1 << 53
)

// Hard reports whether going past the limit refuses requests.
func (l Limit) Hard() bool { return l.Mode != ModeSoft }

// String says a limit as the command line takes it: tokens=5000000/month:hard.
func (l Limit) String() string {
	mode := l.Mode
	if mode == "" {
		mode = ModeHard
	}
	return fmt.Sprintf("%s=%d/%s:%s", l.Metric, l.Amount, l.Per, mode)
}

// The metrics plugins count. A plugin's usage also says what each count
// was for, its detail: the model for OpenAI, the service for HERE.
const (
	// MetricRequests counts the requests passed on, answered or not.
	MetricRequests = "requests"
	// MetricTokens counts input and output tokens together.
	MetricTokens       = "tokens"
	MetricInputTokens  = "tokens.input"
	MetricOutputTokens = "tokens.output"
	// MetricCachedTokens counts the input tokens the provider read from
	// its cache, among the input tokens.
	MetricCachedTokens = "tokens.cached"
	// MetricEstimatedTokens counts tokens estimated from the bytes of a
	// response that ended without its usage, such as one the app stopped
	// reading: they are among the tokens counted.
	MetricEstimatedTokens = "tokens.estimated"
	// MetricTransactions counts HERE's billing units.
	MetricTransactions = "transactions"
)

// Metrics are the metrics each kind counts, which its limits may name.
var Metrics = map[string][]string{
	KindOpenAI: {MetricRequests, MetricTokens, MetricInputTokens, MetricOutputTokens, MetricCachedTokens, MetricEstimatedTokens},
	KindHERE:   {MetricRequests, MetricTransactions},
}

// defaults are each kind's upstream and hosts.
var defaults = map[string]Spec{
	KindOpenAI: {Upstream: "https://api.openai.com"},
	KindHERE:   {Hosts: HEREHosts},
}

// name is a plugin's name: a DNS label, as it is a path segment of its
// address too.
var name = regexp.MustCompile(`^[a-z0-9]([a-z0-9-]{0,30}[a-z0-9])?$`)

// ValidName checks a plugin's name.
func ValidName(plugin string) error {
	if !name.MatchString(plugin) {
		return fmt.Errorf("plugin name %q must be 1 to 32 lowercase letters, digits and hyphens, not starting or ending with a hyphen", plugin)
	}
	return nil
}

// Parse reads a plugin's file, unknown fields refused, and checks it. A
// file of kind container may name its image by tag: Decode reads it, the
// agent pins it, and Validate checks it then.
func Parse(data []byte) (Spec, error) {
	spec, err := Decode(data)
	if err != nil {
		return Spec{}, err
	}
	return spec, spec.Validate()
}

// Decode reads a plugin's file, unknown fields refused, filled in by
// WithDefaults but not checked.
func Decode(data []byte) (Spec, error) {
	var spec Spec
	if err := strictyaml.Decode(data, &spec); err != nil {
		return Spec{}, err
	}
	return spec.WithDefaults(), nil
}

// pinnedImage is how an installed plugin names its image: by registry
// digest, or by local image ID.
var pinnedImage = regexp.MustCompile(`^([a-z0-9][a-z0-9._/:-]*@)?sha256:[0-9a-f]{64}$`)

// PinnedImage reports whether image names exact content, as an installed
// plugin's does.
func PinnedImage(image string) bool { return pinnedImage.MatchString(image) && len(image) <= 512 }

// maxConfig is the most a plugin's config takes, as JSON.
const maxConfig = 64 << 10

// WithDefaults fills in the kind's upstream and hosts, and the limits'
// mode.
func (s Spec) WithDefaults() Spec {
	d := defaults[s.Kind]
	if s.Upstream == "" {
		s.Upstream = d.Upstream
	}
	if len(s.Hosts) == 0 {
		s.Hosts = slices.Clone(d.Hosts)
	}
	s.Allow = slices.Clone(s.Allow)
	s.Config = maps.Clone(s.Config)
	s.Limits = slices.Clone(s.Limits)
	for i := range s.Limits {
		if s.Limits[i].Mode == "" {
			s.Limits[i].Mode = ModeHard
		}
	}
	return s
}

// Validate says what is wrong with a spec filled in by WithDefaults.
func (s Spec) Validate() error { return s.validate(true) }

// ValidateUnpinned is Validate for a plugin's file, whose image may be a
// tag the agent pins.
func (s Spec) ValidateUnpinned() error { return s.validate(false) }

func (s Spec) validate(pinned bool) error {
	var problems []string
	if !slices.Contains(Kinds, s.Kind) {
		problems = append(problems, fmt.Sprintf("kind %q is not one of %s", s.Kind, strings.Join(Kinds, ", ")))
	}
	if s.Kind != KindContainer && (s.Image != "" || s.Config != nil) {
		problems = append(problems, "image and config are for a plugin of kind container")
	}
	switch s.Kind {
	case KindContainer:
		switch {
		case s.Image == "":
			problems = append(problems, "a plugin of kind container names its image")
		case pinned && !PinnedImage(s.Image):
			problems = append(problems, fmt.Sprintf("image %q must be pinned, repository@sha256:<digest>; murder plugin apply pins a tag", s.Image))
		case !pinned:
			if err := spec.ValidReference(s.Image); err != nil {
				problems = append(problems, err.Error())
			}
		}
		if s.Upstream != "" || len(s.Allow) > 0 {
			problems = append(problems, "upstream and allow are for openai; a plugin's manifest says where its requests go, and hosts in the spec replaces its hosts")
		}
		for _, host := range s.Hosts {
			if err := validHost(host); err != nil {
				problems = append(problems, err.Error())
			}
		}
		if encoded, err := json.Marshal(s.Config); err != nil {
			problems = append(problems, fmt.Sprintf("config cannot be said as JSON: %v", err))
		} else if len(encoded) > maxConfig {
			problems = append(problems, fmt.Sprintf("config is at most %d KB as JSON", maxConfig>>10))
		}
	case KindOpenAI:
		if err := validUpstream(s.Upstream); err != nil {
			problems = append(problems, err.Error())
		}
		if len(s.Hosts) > 0 {
			problems = append(problems, "hosts are for a kind whose services have host names of their own, such as here; openai takes upstream")
		}
		for _, endpoint := range s.Allow {
			if !validEndpoint(endpoint) {
				problems = append(problems, fmt.Sprintf("allow %q must be a method and a path, such as \"POST /v1/batches\" or \"GET /v1/batches/*\"", endpoint))
			}
		}
	case KindHERE:
		if s.Upstream != "" {
			problems = append(problems, "upstream is for a kind with one address, such as openai; here takes hosts")
		}
		if len(s.Allow) > 0 {
			problems = append(problems, "allow is for openai; here takes hosts")
		}
		for _, host := range s.Hosts {
			if err := validHost(host); err != nil {
				problems = append(problems, err.Error())
			}
		}
	}
	// A container's metrics are known from its manifest: ValidateWith
	// checks them.
	var metrics []string
	if s.Kind != KindContainer {
		metrics = MetricsOf(s, nil)
	}
	if err := ValidLimits(metrics, s.Limits); err != nil {
		problems = append(problems, err.Error())
	}
	if len(problems) > 0 {
		return errors.New(strings.Join(problems, "; "))
	}
	return nil
}

// ValidateWith checks a spec and, for a container, its limits against the
// metrics its manifest says it counts.
func (s Spec) ValidateWith(manifest *Manifest) error {
	if err := s.Validate(); err != nil {
		return err
	}
	if s.Kind == KindContainer {
		if manifest == nil {
			return fmt.Errorf("image %s has no plugin's manifest", s.Image)
		}
		return ValidLimits(MetricsOf(s, manifest), s.Limits)
	}
	return nil
}

// ValidLimits checks limits of a plugin that counts metrics: each of a
// metric it counts (or, for metrics nil, a metric's name), a period and a
// mode, at most one per metric and period.
func ValidLimits(metrics []string, limits []Limit) error {
	var problems []string
	seen := map[[2]string]bool{}
	for _, limit := range limits {
		switch {
		case metrics == nil && !metricName.MatchString(limit.Metric):
			problems = append(problems, fmt.Sprintf("limit of %q: that is not a metric's name", limit.Metric))
		case metrics != nil && !slices.Contains(metrics, limit.Metric):
			problems = append(problems, fmt.Sprintf("limit of %q: the plugin counts %s", limit.Metric, strings.Join(metrics, ", ")))
		}
		if limit.Per != PerDay && limit.Per != PerMonth {
			problems = append(problems, fmt.Sprintf("limit of %s per %q: a limit is per day or per month", limit.Metric, limit.Per))
		}
		if limit.Mode != "" && limit.Mode != ModeHard && limit.Mode != ModeSoft {
			problems = append(problems, fmt.Sprintf("limit of %s: mode %q is hard or soft", limit.Metric, limit.Mode))
		}
		if limit.Amount < 0 || limit.Amount > maxAmount {
			problems = append(problems, fmt.Sprintf("limit of %s: %d is not an amount from 0 to 2^53", limit.Metric, limit.Amount))
		}
		key := [2]string{limit.Metric, limit.Per}
		if seen[key] {
			problems = append(problems, fmt.Sprintf("%s has two limits per %s", limit.Metric, limit.Per))
		}
		seen[key] = true
	}
	if len(problems) > 0 {
		return errors.New(strings.Join(problems, "; "))
	}
	return nil
}

// ParseLimit reads a limit as the command line gives it:
// METRIC=AMOUNT/PER[:MODE], AMOUNT with an optional k, M or G (thousands,
// millions, billions), as in tokens=5M/month or requests=1000/day:soft.
func ParseLimit(text string) (Limit, error) {
	metric, rest, ok := strings.Cut(text, "=")
	if !ok {
		return Limit{}, fmt.Errorf("limit %q is not METRIC=AMOUNT/PER[:MODE], such as tokens=5M/month:hard", text)
	}
	amount, period, ok := strings.Cut(rest, "/")
	if !ok {
		return Limit{}, fmt.Errorf("limit %q names no period: add /day or /month", text)
	}
	per, mode, _ := strings.Cut(period, ":")
	value, err := parseAmount(amount)
	if err != nil {
		return Limit{}, fmt.Errorf("limit %q: %w", text, err)
	}
	if mode == "" {
		mode = ModeHard
	}
	return Limit{Metric: strings.TrimSpace(metric), Per: per, Amount: value, Mode: mode}, nil
}

func parseAmount(text string) (int64, error) {
	multiplier := int64(1)
	switch {
	case strings.HasSuffix(text, "k"):
		multiplier, text = 1_000, strings.TrimSuffix(text, "k")
	case strings.HasSuffix(text, "M"):
		multiplier, text = 1_000_000, strings.TrimSuffix(text, "M")
	case strings.HasSuffix(text, "G"):
		multiplier, text = 1_000_000_000, strings.TrimSuffix(text, "G")
	}
	value, err := strconv.ParseInt(text, 10, 64)
	if err != nil || value < 0 || value > maxAmount/multiplier {
		return 0, fmt.Errorf("%q is not an amount", text)
	}
	return value * multiplier, nil
}

func validUpstream(upstream string) error {
	parsed, err := url.Parse(upstream)
	if err != nil || parsed.Scheme != "https" && parsed.Scheme != "http" || parsed.Host == "" || parsed.User != nil || parsed.RawQuery != "" || parsed.Fragment != "" || strings.Trim(parsed.Path, "/") != "" {
		return fmt.Errorf("upstream %q must be a scheme and a host, such as https://api.openai.com", upstream)
	}
	return nil
}

var hostLabel = regexp.MustCompile(`^[a-z0-9]([a-z0-9-]{0,61}[a-z0-9])?$`)

func validHost(host string) error {
	labels := strings.Split(strings.TrimPrefix(host, "*."), ".")
	ok := len(labels) >= 2
	for _, label := range labels {
		ok = ok && hostLabel.MatchString(label)
	}
	if !ok {
		return fmt.Errorf("host %q must be a lowercase host name, or *. and a domain", host)
	}
	return nil
}

var endpoint = regexp.MustCompile(`^(GET|POST|PUT|PATCH|DELETE) (/[A-Za-z0-9._~-]+)+(/\*)?$`)

func validEndpoint(text string) bool { return endpoint.MatchString(text) }

// AllowsEndpoint reports whether a request of method for path may go on:
// one of the kind's endpoints, or of those the spec allows.
func (s Spec) AllowsEndpoint(method, path string) bool {
	for _, allowed := range append(slices.Clip(OpenAIEndpoints), s.Allow...) {
		want, pattern, _ := strings.Cut(allowed, " ")
		if want != method {
			continue
		}
		if prefix, oneMore := strings.CutSuffix(pattern, "/*"); oneMore {
			rest, ok := strings.CutPrefix(path, prefix+"/")
			if ok && rest != "" && !strings.Contains(rest, "/") {
				return true
			}
		} else if path == pattern {
			return true
		}
	}
	return false
}

// hostRule is the entry of the spec's hosts that allows host: the host
// itself, or a wildcard; empty for none.
func (s Spec) hostRule(host string) string { return hostRuleIn(s.Hosts, host) }

// hostRuleIn is the entry of hosts that allows host: the host itself, or a
// wildcard; empty for none.
func hostRuleIn(hosts []string, host string) string {
	for _, allowed := range hosts {
		if domain, wildcard := strings.CutPrefix(allowed, "*."); wildcard {
			if strings.HasSuffix(host, "."+domain) && validHost(host) == nil {
				return allowed
			}
		} else if host == allowed {
			return allowed
		}
	}
	return ""
}

// AllowsHost reports whether a request may go to host.
func (s Spec) AllowsHost(host string) bool { return s.hostRule(host) != "" }

// SortLimits orders limits by metric and period, as they are shown.
func SortLimits(limits []Limit) []Limit {
	sorted := slices.Clone(limits)
	slices.SortFunc(sorted, func(a, b Limit) int {
		return strings.Compare(a.Metric+"\x00"+a.Per, b.Metric+"\x00"+b.Per)
	})
	return sorted
}
