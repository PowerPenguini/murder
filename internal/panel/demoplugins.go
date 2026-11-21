package panel

import (
	"context"
	"maps"
	"slices"
	"strings"
	"sync"
	"time"

	"github.com/PowerPenguini/murder/internal/api"
	"github.com/PowerPenguini/murder/internal/plugins"
	"github.com/PowerPenguini/murder/internal/store"
)

// demoPlugins are the demo's plugins: Murder's OpenAI and HERE, and
// Anthropic's API through the example's image, with a month of what the
// tenants used of them, one tenant past a hard limit and two near one.
type demoPlugins struct {
	mu      sync.Mutex
	plugins map[string]api.Plugin
	// own are tenants' own settings: a key, limits of their own.
	own    map[[2]string]demoOwn
	counts []plugins.Count
	// grants are the plugins each tenant's use is shown of.
	grants map[string][]string
}

type demoOwn struct {
	key       bool
	hasLimits bool
	limits    []plugins.Limit
}

// demoAnthropic is the manifest of examples/plugins/anthropic.
var demoAnthropic = plugins.Manifest{ABI: plugins.ABI, Name: "anthropic", Version: "1.0.0", Hosts: []string{"api.anthropic.com"},
	Key: plugins.KeyPlace{Header: "x-api-key"}, Metrics: []string{"tokens", "tokens.input", "tokens.output", "tokens.cache_read", "tokens.cache_write"},
	UI: &plugins.UI{Title: "Anthropic", Description: "Claude through Anthropic's Messages API: tokens read from each answer's usage, streamed or not, by the model that answered.", Detail: "model",
		Metrics: []plugins.MetricUI{{Name: "tokens", Label: "Tokens", Unit: "tokens"}, {Name: "tokens.input", Label: "Input tokens", Unit: "tokens"}, {Name: "tokens.output", Label: "Output tokens", Unit: "tokens"},
			{Name: "tokens.cache_read", Label: "Read from the cache", Unit: "tokens"}, {Name: "tokens.cache_write", Label: "Written to the cache", Unit: "tokens"}, {Name: "requests", Label: "Requests", Unit: "requests"}},
		Config: []plugins.ConfigUI{{Name: "allow", Label: "More endpoints", Help: "Endpoints passed on beyond messages, token counting and models: what they keep, every tenant reaches under the one key."}}}}

// demoOpenAI and demoHERE are the manifests of plugins/openai and
// plugins/here.
var (
	demoOpenAI = plugins.Manifest{ABI: plugins.ABI, Name: "openai", Version: "1.0.0", Hosts: []string{"api.openai.com"},
		Key: plugins.KeyPlace{Header: "Authorization", Prefix: "Bearer "}, Metrics: []string{"tokens", "tokens.input", "tokens.output", "tokens.cached", "tokens.estimated"},
		RequestBody: plugins.MaxRequestBody, RequestBodyTypes: []string{"application/json"},
		UI: &plugins.UI{Title: "OpenAI", Description: "OpenAI's API, or one that speaks it: tokens counted from each answer's usage, by the model that answered.", Detail: "model",
			Metrics: []plugins.MetricUI{{Name: "tokens", Label: "Tokens", Unit: "tokens"}, {Name: "tokens.input", Label: "Input tokens", Unit: "tokens"}, {Name: "tokens.output", Label: "Output tokens", Unit: "tokens"},
				{Name: "tokens.cached", Label: "Cached input tokens", Unit: "tokens"}, {Name: "tokens.estimated", Label: "Estimated tokens", Unit: "tokens"}, {Name: "requests", Label: "Requests", Unit: "requests"}},
			Config: []plugins.ConfigUI{{Name: "upstream", Label: "Upstream", Help: "Where the API is, as https://api.groq.com/openai: one of the plugin's hosts, and a path. The first of its hosts if left out."},
				{Name: "allow", Label: "More endpoints", Help: "Endpoints passed on beyond those whose use is counted, as POST /v1/batches: what they keep, every tenant reaches under the one key."}}}}
	demoHERE = plugins.Manifest{ABI: plugins.ABI, Name: "here", Version: "1.0.0",
		Hosts: []string{"geocode.search.hereapi.com", "revgeocode.search.hereapi.com", "multi-revgeocode.search.hereapi.com",
			"autosuggest.search.hereapi.com", "autocomplete.search.hereapi.com", "discover.search.hereapi.com",
			"browse.search.hereapi.com", "lookup.search.hereapi.com",
			"router.hereapi.com", "matrix.router.hereapi.com", "isoline.router.hereapi.com", "intermodal.router.hereapi.com", "transit.router.hereapi.com",
			"wps.hereapi.com",
			"maps.hereapi.com", "traffic.maps.hereapi.com", "vector.hereapi.com", "traffic.vector.hereapi.com", "data.traffic.hereapi.com"},
		Key: plugins.KeyPlace{Query: "apiKey"}, Metrics: []string{"transactions"}, RequestBody: plugins.MaxRequestBody,
		UI: &plugins.UI{Title: "HERE", Description: "HERE's location services: requests, and transactions as HERE bills them, by service.", Detail: "service",
			Metrics: []plugins.MetricUI{{Name: "transactions", Label: "Transactions", Unit: "transactions"}, {Name: "requests", Label: "Requests", Unit: "requests"}}}}
)

func newDemoPlugins(now time.Time) *demoPlugins {
	created := now.Add(-25 * 24 * time.Hour)
	p := &demoPlugins{plugins: map[string]api.Plugin{}, own: map[[2]string]demoOwn{}, grants: map[string][]string{}}
	add := func(name string, spec plugins.Spec, manifest *plugins.Manifest, key bool, apps ...string) {
		p.plugins[name] = api.Plugin{Name: name, Spec: spec.WithDefaults(), Manifest: manifest, HasKey: key, Apps: apps, CreatedAt: created, UpdatedAt: created.Add(3 * 24 * time.Hour)}
	}
	add("openai", plugins.Spec{Kind: plugins.KindContainer, Image: "ghcr.io/powerpenguini/murder-openai@sha256:0c4a7e1f9b3d6a2c8e5f1b7d4a9c3e6f0b2d8a5c1e7f4b9d3a6c0e2f8b5d1a7c",
		Limits: []plugins.Limit{{Metric: "tokens", Per: plugins.PerMonth, Amount: 5_000_000}, {Metric: "requests", Per: plugins.PerDay, Amount: 20_000, Mode: plugins.ModeSoft}}}, &demoOpenAI, true, "web")
	add("here", plugins.Spec{Kind: plugins.KindContainer, Image: "ghcr.io/powerpenguini/murder-here@sha256:9e2b6d0a4c8f1e5b3d7a9c2e6f0b4d8a1c5e9f3b7d2a6c0e4f8b1d5a9c3e7f2b",
		Limits: []plugins.Limit{{Metric: "transactions", Per: plugins.PerMonth, Amount: 250_000}}}, &demoHERE, true, "web")
	add("anthropic", plugins.Spec{Kind: plugins.KindContainer, Image: "ghcr.io/acme/anthropic-plugin@sha256:5f1e4c0a8d2b9e7f3a6c1d4b8e0f2a7c9d3b6e1f4a8c2d5b9e0f3a7c1d4b6e8f", Config: map[string]any{"allow": []any{"POST /v1/messages/batches"}},
		Limits: []plugins.Limit{{Metric: "tokens", Per: plugins.PerMonth, Amount: 2_000_000}}}, &demoAnthropic, false, "web")
	p.own[[2]string{"acme-logistics", "openai"}] = demoOwn{hasLimits: true, limits: []plugins.Limit{{Metric: "tokens", Per: plugins.PerMonth, Amount: 1_000_000, Mode: plugins.ModeHard}}}
	p.own[[2]string{"nordwind", "openai"}] = demoOwn{key: true}
	// Each tenant's month so far of each plugin it uses, of its main
	// metric.
	uses := []struct {
		tenant, plugin string
		month          int64
	}{
		{"atlas", "openai", 2_400_000}, {"atlas", "anthropic", 380_000},
		{"acme-logistics", "openai", 1_040_000}, {"acme-logistics", "here", 61_000},
		{"baltic-freight", "here", 214_000}, {"kowalski-trans", "here", 92_000},
		{"nordwind", "openai", 3_100_000}, {"vistula-cargo", "anthropic", 1_710_000},
	}
	seed := uint32(11)
	next := func() float64 {
		seed = seed*1664525 + 1013904223
		return float64(seed>>8) / float64(1<<24)
	}
	now = now.UTC()
	today := time.Date(now.Year(), now.Month(), now.Day(), 0, 0, 0, 0, time.UTC)
	for _, u := range uses {
		p.grants[u.tenant] = append(p.grants[u.tenant], u.plugin)
		// Weights for the last 30 days, quieter at weekends, scaled so the
		// month so far is its month.
		weights := make([]float64, pluginDays)
		var month float64
		for i := range weights {
			day := today.AddDate(0, 0, i-(pluginDays-1))
			weights[i] = 0.55 + 0.9*next()
			if day.Weekday() == time.Saturday || day.Weekday() == time.Sunday {
				weights[i] *= 0.35
			}
			if day.Month() == today.Month() {
				month += weights[i]
			}
		}
		for i, w := range weights {
			day := plugins.Day(today.AddDate(0, 0, i-(pluginDays-1)))
			amount := int64(float64(u.month) * w / month)
			p.counts = append(p.counts, demoCounts(u.tenant, u.plugin, day, amount)...)
		}
	}
	return p
}

// demoCounts are one day's counts of a tenant's use of a plugin, of
// amount of its main metric.
func demoCounts(tenant, plugin, day string, amount int64) []plugins.Count {
	var out []plugins.Count
	count := func(metric, detail string, n int64) {
		if n > 0 {
			out = append(out, plugins.Count{Tenant: tenant, Plugin: plugin, Day: day, Metric: metric, Detail: detail, Amount: n})
		}
	}
	switch plugin {
	case "openai":
		for _, model := range []struct {
			name  string
			share float64
		}{{"gpt-5.1", 0.62}, {"gpt-5-mini", 0.33}, {"text-embedding-3-small", 0.05}} {
			n := int64(float64(amount) * model.share)
			input := n * 7 / 10
			count("tokens", model.name, n)
			count("tokens.input", model.name, input)
			count("tokens.output", model.name, n-input)
			count("tokens.cached", model.name, input/5)
		}
		count("requests", "", amount/1800)
	case "here":
		for _, service := range []struct {
			name  string
			share float64
		}{{"geocode.search", 0.46}, {"router", 0.31}, {"matrix.router", 0.18}, {"autosuggest.search", 0.05}} {
			n := int64(float64(amount) * service.share)
			count("transactions", service.name, n)
			requests := n
			if service.name == "matrix.router" {
				requests = n / 25
			}
			count("requests", service.name, requests)
		}
	case "anthropic":
		for _, model := range []struct {
			name  string
			share float64
		}{{"claude-opus-5-5", 0.4}, {"claude-sonnet-5-5", 0.45}, {"claude-haiku-5-5", 0.15}} {
			n := int64(float64(amount) * model.share)
			count("tokens", model.name, n)
			count("tokens.input", model.name, n*3/10)
			count("tokens.cache_read", model.name, n*4/10)
			count("tokens.cache_write", model.name, n/10)
			count("tokens.output", model.name, n-n*3/10-n*4/10-n/10)
		}
		count("requests", "", amount/4200)
	}
	return out
}

// AsPlugins is the demo's plugins, for the person given.
func (d *DemoCluster) AsPlugins(actor string) PluginAgent {
	return &demoPluginActor{d: d, actor: actor}
}

type demoPluginActor struct {
	d     *DemoCluster
	actor string
}

func (a *demoPluginActor) state() *demoPlugins {
	a.d.mu.Lock()
	defer a.d.mu.Unlock()
	if a.d.pluginState == nil {
		a.d.pluginState = newDemoPlugins(a.d.now())
	}
	return a.d.pluginState
}

func (a *demoPluginActor) Plugins(context.Context) ([]api.Plugin, error) {
	p := a.state()
	p.mu.Lock()
	defer p.mu.Unlock()
	var out []api.Plugin
	for _, name := range slices.Sorted(maps.Keys(p.plugins)) {
		out = append(out, p.plugins[name])
	}
	return out, nil
}

func (a *demoPluginActor) PluginUses(context.Context) ([]api.PluginUse, error) {
	p := a.state()
	p.mu.Lock()
	defer p.mu.Unlock()
	now := a.d.now()
	day, month := plugins.Day(now), plugins.Month(now)
	var out []api.PluginUse
	for _, tenant := range slices.Sorted(maps.Keys(p.grants)) {
		for _, name := range p.grants[tenant] {
			plugin := p.plugins[name]
			own := p.own[[2]string{tenant, name}]
			use := api.PluginUse{Tenant: tenant, Plugin: name, Granted: true, OwnKey: own.key, OwnLimits: own.hasLimits}
			limits := plugin.Spec.Limits
			switch {
			case own.hasLimits:
				limits = own.limits
			case own.key:
				limits = nil
			}
			today, sofar := map[string]int64{}, map[string]int64{}
			for _, c := range p.counts {
				if c.Tenant != tenant || c.Plugin != name {
					continue
				}
				if c.Day == day {
					today[c.Metric] += c.Amount
				}
				if strings.HasPrefix(c.Day, month) {
					sofar[c.Metric] += c.Amount
				}
			}
			for _, limit := range limits {
				used := sofar[limit.Metric]
				if limit.Per == plugins.PerDay {
					used = today[limit.Metric]
				}
				use.Limits = append(use.Limits, api.LimitUse{Limit: limit, Period: plugins.PeriodOf(limit.Per, now), Used: used, Reached: used >= limit.Amount})
			}
			for _, metric := range plugins.MetricsOf(plugin.Spec, plugin.Manifest) {
				if n := today[metric]; n > 0 {
					use.Today = append(use.Today, api.MetricUsed{Metric: metric, Amount: n})
				}
				if n := sofar[metric]; n > 0 {
					use.Month = append(use.Month, api.MetricUsed{Metric: metric, Amount: n})
				}
			}
			out = append(out, use)
		}
	}
	return out, nil
}

func (a *demoPluginActor) PluginUsage(_ context.Context, tenant, plugin, from, to string) (api.PluginUsage, error) {
	p := a.state()
	p.mu.Lock()
	defer p.mu.Unlock()
	now := a.d.now()
	if from == "" {
		from = plugins.Month(now) + "-01"
	}
	if to == "" {
		to = plugins.Day(now)
	}
	usage := api.PluginUsage{From: from, To: to, Counts: []plugins.Count{}}
	for _, c := range p.counts {
		if (tenant == "" || c.Tenant == tenant) && (plugin == "" || c.Plugin == plugin) && c.Day >= from && c.Day <= to {
			usage.Counts = append(usage.Counts, c)
		}
	}
	return usage, nil
}

func (a *demoPluginActor) SetPluginKey(_ context.Context, name, tenant, key string) error {
	p := a.state()
	p.mu.Lock()
	plugin, ok := p.plugins[name]
	if !ok {
		p.mu.Unlock()
		return &api.Error{Status: 404, Message: "plugin " + name + ": not found"}
	}
	action := "plugin.key-set"
	if tenant == "" {
		plugin.HasKey = key != ""
		p.plugins[name] = plugin
		if key == "" {
			action = "plugin.key-removed"
		}
	} else {
		own := p.own[[2]string{tenant, name}]
		own.key = key != ""
		p.own[[2]string{tenant, name}] = own
		action = map[bool]string{true: "plugin.tenant-key-set", false: "plugin.tenant-key-removed"}[key != ""]
	}
	p.mu.Unlock()
	a.d.recordEvent(a.actor, action, tenant, name)
	return nil
}

func (a *demoPluginActor) SetPluginLimits(_ context.Context, name, tenant string, limits []plugins.Limit, set bool) error {
	p := a.state()
	p.mu.Lock()
	plugin, ok := p.plugins[name]
	if !ok {
		p.mu.Unlock()
		return &api.Error{Status: 404, Message: "plugin " + name + ": not found"}
	}
	if err := plugins.ValidLimits(plugins.MetricsOf(plugin.Spec, plugin.Manifest), limits); err != nil {
		p.mu.Unlock()
		return &api.Error{Status: 400, Message: err.Error()}
	}
	for i := range limits {
		if limits[i].Mode == "" {
			limits[i].Mode = plugins.ModeHard
		}
	}
	own := p.own[[2]string{tenant, name}]
	own.hasLimits, own.limits = set, plugins.SortLimits(limits)
	p.own[[2]string{tenant, name}] = own
	p.mu.Unlock()
	detail := name + ": the plugin's limits"
	if set {
		var said []string
		for _, l := range limits {
			said = append(said, l.String())
		}
		detail = name + ": " + strings.Join(said, " ")
		if len(limits) == 0 {
			detail = name + ": no limits"
		}
	}
	a.d.recordEvent(a.actor, "plugin.tenant-limits-set", tenant, detail)
	return nil
}

// recordEvent adds an event of the demo's, as the agent's audit log would.
func (d *DemoCluster) recordEvent(actor, action, tenant, detail string) {
	d.mu.Lock()
	defer d.mu.Unlock()
	var id int64
	if len(d.events) > 0 {
		id = d.events[0].ID
	}
	app := ""
	if t, ok := d.tenants[tenant]; ok {
		app = t.App
	}
	d.events = append([]store.Event{{ID: id + 1, At: d.now(), Actor: actor + " via murder-panel", Action: action, Tenant: tenant, App: app, Detail: detail}}, d.events...)
}
