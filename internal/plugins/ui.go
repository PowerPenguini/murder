package plugins

import (
	"errors"
	"fmt"
	"maps"
	"regexp"
	"slices"
	"strings"
)

// UI is how the panel shows a plugin: what it is, how its metrics read,
// the first of them charted, what its counts are kept by, and what its
// config takes, as the plugin's manifest says it.
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
// is for, and what it takes: text, or a list of texts, each matching
// Pattern if it names one. A manifest that names its config's fields has
// a spec's config checked against them as the plugin is applied.
type ConfigUI struct {
	Name    string `json:"name"`
	Label   string `json:"label,omitempty"`
	Help    string `json:"help,omitempty"`
	Type    string `json:"type,omitempty"`
	Pattern string `json:"pattern,omitempty"`
}

// The types of a field of config: text, the default, or a list of texts.
const (
	ConfigText = "text"
	ConfigList = "list"
)

// checkConfig says what is wrong with a config, of fields a manifest
// names: none it does not name, and each of its type and pattern.
func checkConfig(config map[string]any, fields []ConfigUI) error {
	var problems []string
	for _, name := range slices.Sorted(maps.Keys(config)) {
		i := slices.IndexFunc(fields, func(f ConfigUI) bool { return f.Name == name })
		if i < 0 {
			names := make([]string, len(fields))
			for j, f := range fields {
				names[j] = f.Name
			}
			problems = append(problems, fmt.Sprintf("config: the plugin takes no %s (it takes %s)", name, strings.Join(names, ", ")))
			continue
		}
		field := fields[i]
		pattern, _ := regexp.Compile(field.Pattern)
		var texts []any
		if field.Type == ConfigList {
			list, ok := config[name].([]any)
			if !ok {
				problems = append(problems, fmt.Sprintf("config: %s is a list", name))
				continue
			}
			texts = list
		} else {
			texts = []any{config[name]}
		}
		for _, value := range texts {
			text, ok := value.(string)
			switch {
			case !ok:
				problems = append(problems, fmt.Sprintf("config: %s takes text, not %v", name, value))
			case field.Pattern != "" && !pattern.MatchString(text):
				problems = append(problems, fmt.Sprintf("config: %s: %q is not as the plugin takes (%s)", name, text, field.Pattern))
			}
		}
	}
	if len(problems) > 0 {
		return errors.New(strings.Join(problems, "; "))
	}
	return nil
}

// UIOf is how the panel shows a plugin: as its manifest says, every
// metric it counts said, those the manifest left out by their names.
func UIOf(spec Spec, manifest *Manifest) UI {
	var ui UI
	if manifest != nil {
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

// validate says what is wrong with a manifest's UI, of a plugin that counts
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
			problems = append(problems, fmt.Sprintf("ui: metric %q is not one the plugin counts", m.Name))
		case !printable(m.Label, 48) || !printable(m.Unit, 24):
			problems = append(problems, fmt.Sprintf("ui: metric %s: a label is at most 48 characters and a unit 24", m.Name))
		}
	}
	for _, c := range u.Config {
		if c.Name == "" || !printable(c.Name, 64) || !printable(c.Label, 48) || !printable(c.Help, 300) {
			problems = append(problems, fmt.Sprintf("ui: config field %q: a name of at most 64 characters, a label of 48 and help of 300", c.Name))
		}
		if c.Type != "" && c.Type != ConfigText && c.Type != ConfigList {
			problems = append(problems, fmt.Sprintf("ui: config field %s: type %q is %s or %s", c.Name, c.Type, ConfigText, ConfigList))
		}
		if _, err := regexp.Compile(c.Pattern); err != nil || len(c.Pattern) > 256 {
			problems = append(problems, fmt.Sprintf("ui: config field %s: a pattern is a regular expression of at most 256 characters", c.Name))
		}
	}
	if len(problems) > 0 {
		return errors.New(strings.Join(problems, "; "))
	}
	return nil
}
