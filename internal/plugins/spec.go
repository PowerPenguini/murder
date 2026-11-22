// Package plugins passes tenants' requests on to the APIs of providers
// such as OpenAI and HERE, with a key the tenant never sees, and counts
// and limits what each tenant uses of them. A plugin is an image, run in
// a container on each node, that says where each request goes and counts
// what it used; its spec names the image and the limits its tenants get.
// The edge serves it, on a port only tenants reach, and holds it to its
// manifest; the agent keeps what was used in the replicated state.
package plugins

import (
	"encoding/json"
	"errors"
	"fmt"
	"maps"
	"regexp"
	"slices"
	"strconv"
	"strings"

	"github.com/PowerPenguini/murder/internal/spec"
	"github.com/PowerPenguini/murder/internal/strictyaml"
	"gopkg.in/yaml.v3"
)

// KindContainer is the kind of every plugin: run in a container on each
// node, its code says where requests go and counts what they used (see
// Manifest and docs/plugins.md). Murder's own plugins for OpenAI and HERE
// are images too, built from plugins/ in its repository.
const KindContainer = "container"

// Kinds are the kinds this release runs.
var Kinds = []string{KindContainer}

// Spec is a plugin as its file says it: what `murder plugin apply` reads.
// The plugin is named after its file.
type Spec struct {
	// Kind is container (see Kinds).
	Kind string `json:"kind" yaml:"kind"`
	// Image is the plugin's image: in a plugin's file, a reference such
	// as ghcr.io/acme/plugin:1.2; installed, pinned to its content,
	// repository@sha256:<digest> (or a local sha256:<id>, which only a
	// node that has it runs).
	Image string `json:"image" yaml:"image"`
	// Hosts are the host names requests may go to, in place of those the
	// plugin's manifest names: a name, or "*." and a domain for every
	// name below it.
	Hosts []string `json:"hosts,omitempty" yaml:"hosts,omitempty"`
	// Config is what the plugin is told with each request, as JSON:
	// whatever its code takes.
	Config map[string]any `json:"config,omitempty" yaml:"config,omitempty"`
	// Limits are what each tenant may use, unless it has limits of its
	// own, or a key of its own (then only its own limits hold).
	Limits []Limit `json:"limits,omitempty" yaml:"limits,omitempty"`
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

// The metrics Murder counts of every plugin; the others are the
// plugin's, which its manifest names. A count also says what it was for,
// its detail, such as a model or a service.
const (
	// MetricRequests counts the requests passed on, answered or not.
	MetricRequests = "requests"
)

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
		// A file of a kind Murder once ran itself, of the fields it took,
		// is told what to write in its place.
		var old struct {
			Kind string `yaml:"kind"`
		}
		if yaml.Unmarshal(data, &old) == nil && formerKind(old.Kind) != "" {
			return Spec{}, errors.New(formerKind(old.Kind))
		}
		return Spec{}, err
	}
	return spec.WithDefaults(), nil
}

// formerKind says what a plugin of a kind Murder once ran itself is now;
// empty for another kind.
func formerKind(kind string) string {
	switch kind {
	case "openai":
		return "kind openai is now an image of Murder's: kind container, image ghcr.io/powerpenguini/murder-openai, its upstream and allow under config (docs/plugins.md)"
	case "here":
		return "kind here is now an image of Murder's: kind container, image ghcr.io/powerpenguini/murder-here (docs/plugins.md)"
	}
	return ""
}

// pinnedImage is how an installed plugin names its image: by registry
// digest, or by local image ID.
var pinnedImage = regexp.MustCompile(`^([a-z0-9][a-z0-9._/:-]*@)?sha256:[0-9a-f]{64}$`)

// PinnedImage reports whether image names exact content, as an installed
// plugin's does.
func PinnedImage(image string) bool { return pinnedImage.MatchString(image) && len(image) <= 512 }

// maxConfig is the most a plugin's config takes, as JSON.
const maxConfig = 64 << 10

// WithDefaults fills in the limits' mode.
func (s Spec) WithDefaults() Spec {
	s.Hosts = slices.Clone(s.Hosts)
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
	switch s.Kind {
	case KindContainer:
	case "openai", "here":
		problems = append(problems, formerKind(s.Kind))
	default:
		problems = append(problems, fmt.Sprintf("kind %q is not one of %s", s.Kind, strings.Join(Kinds, ", ")))
	}
	switch {
	case s.Image == "":
		problems = append(problems, "a plugin names its image")
	case pinned && !PinnedImage(s.Image):
		problems = append(problems, fmt.Sprintf("image %q must be pinned, repository@sha256:<digest>; murder plugin apply pins a tag", s.Image))
	case !pinned:
		if err := spec.ValidReference(s.Image); err != nil {
			problems = append(problems, err.Error())
		}
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
	// The metrics are known from the manifest: ValidateWith checks them.
	if err := ValidLimits(nil, s.Limits); err != nil {
		problems = append(problems, err.Error())
	}
	if len(problems) > 0 {
		return errors.New(strings.Join(problems, "; "))
	}
	return nil
}

// ValidateWith checks a spec and its limits against the metrics its
// manifest says it counts.
func (s Spec) ValidateWith(manifest *Manifest) error {
	if err := s.Validate(); err != nil {
		return err
	}
	if manifest == nil {
		return fmt.Errorf("image %s has no plugin's manifest", s.Image)
	}
	if manifest.UI != nil && len(manifest.UI.Config) > 0 {
		if err := checkConfig(s.Config, manifest.UI.Config); err != nil {
			return err
		}
	}
	return ValidLimits(MetricsOf(s, manifest), s.Limits)
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

// SortLimits orders limits by metric and period, as they are shown.
func SortLimits(limits []Limit) []Limit {
	sorted := slices.Clone(limits)
	slices.SortFunc(sorted, func(a, b Limit) int {
		return strings.Compare(a.Metric+"\x00"+a.Per, b.Metric+"\x00"+b.Per)
	})
	return sorted
}
