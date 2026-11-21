package plugins

import (
	"errors"
	"fmt"
	"slices"
	"strings"
)

// UI is how the panel shows a plugin: what it is, how its metrics read,
// the first of them charted, what its counts are kept by, and what its
// config takes. A module says it in its manifest; Murder says it for its
// own kinds.
type UI struct {
	Title       string `json:"title,omitempty"`
	Description string `json:"description,omitempty"`
	// Detail names what counts are kept by, as model or service.
	Detail string `json:"detail,omitempty"`
	// Metrics say how metrics read, the first the one charted; those
	// left out read as their names.
	Metrics []MetricUI `json:"metrics,omitempty"`
	// Config says the fields of the plugin's config.
	Config []ConfigUI `json:"config,omitempty"`
}

// MetricUI is how a metric reads: a label, as Input tokens, and the unit
// of its amounts, as tokens.
type MetricUI struct {
	Name  string `json:"name"`
	Label string `json:"label,omitempty"`
	Unit  string `json:"unit,omitempty"`
}

// ConfigUI is a field of a plugin's config: its name, a label and what it
// is for.
type ConfigUI struct {
	Name  string `json:"name"`
	Label string `json:"label,omitempty"`
	Help  string `json:"help,omitempty"`
}

// builtinUI is how Murder shows its own kinds.
var builtinUI = map[string]UI{
	KindOpenAI: {
		Title:       "OpenAI",
		Description: "OpenAI's API, or one that speaks it: tokens counted from each answer's usage, by the model that answered.",
		Detail:      "model",
		Metrics: []MetricUI{
			{Name: MetricTokens, Label: "Tokens", Unit: "tokens"},
			{Name: MetricInputTokens, Label: "Input tokens", Unit: "tokens"},
			{Name: MetricOutputTokens, Label: "Output tokens", Unit: "tokens"},
			{Name: MetricCachedTokens, Label: "Cached input tokens", Unit: "tokens"},
			{Name: MetricEstimatedTokens, Label: "Estimated tokens", Unit: "tokens"},
			{Name: MetricRequests, Label: "Requests", Unit: "requests"},
		},
	},
	KindHERE: {
		Title:       "HERE",
		Description: "HERE's location services: requests, and transactions as HERE bills them, by service.",
		Detail:      "service",
		Metrics: []MetricUI{
			{Name: MetricTransactions, Label: "Transactions", Unit: "transactions"},
			{Name: MetricRequests, Label: "Requests", Unit: "requests"},
		},
	},
}

// UIOf is how the panel shows a plugin: its kind's, or its module's, every
// metric it counts said, those the module left out by their names.
func UIOf(spec Spec, manifest *Manifest) UI {
	ui := builtinUI[spec.Kind]
	if spec.Kind == KindWasm && manifest != nil {
		if manifest.UI != nil {
			ui = *manifest.UI
		}
		if ui.Title == "" {
			ui.Title = manifest.Name
		}
	}
	ui.Metrics = slices.Clone(ui.Metrics)
	ui.Config = slices.Clone(ui.Config)
	metrics := MetricsOf(spec, manifest)
	// Only metrics it counts, each once, then those it left out.
	var said []MetricUI
	for _, m := range ui.Metrics {
		if slices.Contains(metrics, m.Name) && !slices.ContainsFunc(said, func(s MetricUI) bool { return s.Name == m.Name }) {
			said = append(said, m)
		}
	}
	for _, name := range metrics {
		if !slices.ContainsFunc(said, func(s MetricUI) bool { return s.Name == name }) {
			said = append(said, MetricUI{Name: name})
		}
	}
	for i := range said {
		if said[i].Label == "" {
			said[i].Label = said[i].Name
		}
		if said[i].Unit == "" && said[i].Name == MetricRequests {
			said[i].Unit = "requests"
		}
	}
	ui.Metrics = said
	if ui.Detail == "" {
		ui.Detail = "detail"
	}
	return ui
}

// Metric is how one of the plugin's metrics reads.
func (u UI) Metric(name string) MetricUI {
	for _, m := range u.Metrics {
		if m.Name == name {
			return m
		}
	}
	return MetricUI{Name: name, Label: name}
}

// printable reports whether text is at most max characters, none of them
// a control character.
func printable(text string, max int) bool {
	return len([]rune(text)) <= max && !strings.ContainsFunc(text, func(r rune) bool { return r < ' ' || r == 0x7f })
}

// validate says what is wrong with a module's UI, of a module that counts
// metrics (and requests).
func (u UI) validate(metrics []string) error {
	var problems []string
	if !printable(u.Title, 64) || !printable(u.Description, 500) || !printable(u.Detail, 32) {
		problems = append(problems, "ui: a title is at most 64 characters, a description 500 and a detail 32, none a control character")
	}
	if len(u.Metrics) > 33 || len(u.Config) > 32 {
		problems = append(problems, "ui: at most 33 metrics and 32 fields of config")
	}
	for _, m := range u.Metrics {
		switch {
		case m.Name != MetricRequests && !slices.Contains(metrics, m.Name):
			problems = append(problems, fmt.Sprintf("ui: metric %q is not one the module counts", m.Name))
		case !printable(m.Label, 48) || !printable(m.Unit, 24):
			problems = append(problems, fmt.Sprintf("ui: metric %s: a label is at most 48 characters and a unit 24", m.Name))
		}
	}
	for _, c := range u.Config {
		if c.Name == "" || !printable(c.Name, 64) || !printable(c.Label, 48) || !printable(c.Help, 300) {
			problems = append(problems, fmt.Sprintf("ui: config field %q: a name of at most 64 characters, a label of 48 and help of 300", c.Name))
		}
	}
	if len(problems) > 0 {
		return errors.New(strings.Join(problems, "; "))
	}
	return nil
}
