package panel

// The Plugins pages show what tenants use of the plugins (see
// docs/plugins.md): the plugins, each with what its module says of
// itself, its tenants against their limits, and its last 30 days by day.
// A plugin's page sets its key, and a tenant's own limits or key of it; a
// key is never shown, only whether there is one. A tenant's page has a
// Plugins box, and the overview says a tenant that reached a limit, or
// nears one.

import (
	"context"
	"encoding/csv"
	"fmt"
	"maps"
	"net/http"
	"net/url"
	"slices"
	"strconv"
	"strings"
	"time"

	"github.com/PowerPenguini/murder/internal/api"
	"github.com/PowerPenguini/murder/internal/plugins"
)

// PluginAgent is what the Plugins pages ask of the agent, for the person
// signed in.
type PluginAgent interface {
	Plugins(ctx context.Context) ([]api.Plugin, error)
	PluginUses(ctx context.Context) ([]api.PluginUse, error)
	PluginUsage(ctx context.Context, tenant, plugin, from, to string) (api.PluginUsage, error)
	SetPluginKey(ctx context.Context, name, tenant, key string) error
	SetPluginLimits(ctx context.Context, name, tenant string, limits []plugins.Limit, set bool) error
}

// pluginDays is how many days a plugin's page charts.
const pluginDays = 30

// nearShare is the share of a limit a tenant uses before it is said.
const nearShare = 0.8

// amountText says an amount short: 950, 12.5k, 3.2M, 1.1G.
func amountText(n int64) string {
	switch {
	case n < 10_000:
		return strconv.FormatInt(n, 10)
	case n < 1_000_000:
		return trimZero(float64(n)/1e3) + "k"
	case n < 1_000_000_000:
		return trimZero(float64(n)/1e6) + "M"
	}
	return trimZero(float64(n)/1e9) + "G"
}

func trimZero(v float64) string {
	if v >= 100 {
		return strconv.FormatFloat(v, 'f', 0, 64)
	}
	return strings.TrimSuffix(strconv.FormatFloat(v, 'f', 1, 64), ".0")
}

// limitView is a limit and how far a tenant is into it.
type limitView struct {
	plugins.Limit
	// Said is the limit in words: "2M tokens a month".
	Said string
	// Used is what was used of it, said; Meter as a bar.
	Used  string
	Meter meter
	// Reached says it was used whole; Near that it is past nearShare.
	Reached, Near bool
	// Until is when its period ends, as a hard one reached refuses
	// requests until then.
	Until time.Time
}

func limitViewOf(limit api.LimitUse, ui plugins.UI, now time.Time) limitView {
	m := ui.Metric(limit.Metric)
	unit := m.Unit
	if unit == "" {
		unit = m.Label
	}
	per := "a day"
	if limit.Per == plugins.PerMonth {
		per = "a month"
	}
	v := limitView{Limit: limit.Limit, Said: amountText(limit.Amount) + " " + unit + " " + per, Used: amountText(limit.Used), Reached: limit.Reached, Until: plugins.PeriodEnd(limit.Per, now)}
	if limit.Amount > 0 {
		v.Meter = meterOf(int(min(limit.Used, limit.Amount)*1000/limit.Amount), 1000)
		v.Near = !limit.Reached && float64(limit.Used) >= nearShare*float64(limit.Amount)
	} else {
		v.Meter = meter{Width: 100}
	}
	return v
}

// pluginUseView is a tenant's use of a plugin, as its rows say it.
type pluginUseView struct {
	api.PluginUse
	Limits []limitView
	// Month is what it used of the plugin's main metric this month.
	Month string
	// Reached and Near say a limit of it is reached, or near.
	Reached, Near bool
}

func pluginUseViewOf(use api.PluginUse, ui plugins.UI, main string, now time.Time) pluginUseView {
	v := pluginUseView{PluginUse: use, Month: "0"}
	for _, limit := range use.Limits {
		l := limitViewOf(limit, ui, now)
		v.Reached = v.Reached || l.Reached
		v.Near = v.Near || l.Near
		v.Limits = append(v.Limits, l)
	}
	for _, used := range use.Month {
		if used.Metric == main {
			v.Month = amountText(used.Amount)
		}
	}
	return v
}

// pluginRow is a plugin on the Plugins page.
type pluginRow struct {
	api.Plugin
	UI plugins.UI
	// Kind says the kind, and for a module its name and version.
	Kind   string
	Limits []string
	// Main is the metric charted, MonthText what its tenants used of it
	// this month.
	Main      plugins.MetricUI
	MonthText string
	Tenants   int
	// Reached and Near count its tenants at a limit, or near one.
	Reached, Near int
}

func kindText(p api.Plugin) string {
	if p.Manifest == nil {
		return p.Spec.Kind
	}
	return strings.TrimSpace(p.Spec.Kind + " · " + p.Manifest.Name + " " + p.Manifest.Version)
}

func limitsText(limits []plugins.Limit, ui plugins.UI) []string {
	var said []string
	for _, limit := range plugins.SortLimits(limits) {
		l := limitViewOf(api.LimitUse{Limit: limit}, ui, time.Time{})
		if !limit.Hard() {
			l.Said += ", soft"
		}
		said = append(said, l.Said)
	}
	return said
}

type pluginsData struct {
	Plugins []pluginRow
	// Attention are the tenants at a limit, or near one.
	Attention []pluginNotice
}

// pluginNotice is a tenant at a limit of a plugin, or near one.
type pluginNotice struct {
	Tenant, Plugin string
	Limit          limitView
}

func (s *Server) pluginsPage(w http.ResponseWriter, r *http.Request) {
	ctx, cancel := context.WithTimeout(r.Context(), 10*time.Second)
	defer cancel()
	agent := s.Plugins(s.actor(userOf(r)))
	var list []api.Plugin
	var uses []api.PluginUse
	if err := all(
		func() (err error) { list, err = agent.Plugins(ctx); return },
		func() (err error) { uses, err = agent.PluginUses(ctx); return },
	); err != nil {
		s.fail(w, r, err)
		return
	}
	now := s.now()
	data := pluginsData{}
	for _, p := range list {
		ui := plugins.UIOf(p.Spec, p.Manifest)
		row := pluginRow{Plugin: p, UI: ui, Kind: kindText(p), Limits: limitsText(p.Spec.Limits, ui), Main: ui.Metrics[0]}
		var month int64
		for _, use := range uses {
			if use.Plugin != p.Name {
				continue
			}
			row.Tenants++
			v := pluginUseViewOf(use, ui, row.Main.Name, now)
			if v.Reached {
				row.Reached++
			} else if v.Near {
				row.Near++
			}
			for _, used := range use.Month {
				if used.Metric == row.Main.Name {
					month += used.Amount
				}
			}
		}
		row.MonthText = amountText(month)
		data.Plugins = append(data.Plugins, row)
	}
	data.Attention = pluginNoticesOf(list, uses, now)
	s.render(w, r, "plugins", "Plugins", "plugins", data)
}

// pluginNoticesOf are the tenants at a limit, or near one, the reached
// and hard first.
func pluginNoticesOf(list []api.Plugin, uses []api.PluginUse, now time.Time) []pluginNotice {
	byName := map[string]api.Plugin{}
	for _, p := range list {
		byName[p.Name] = p
	}
	var out []pluginNotice
	for _, use := range uses {
		p, ok := byName[use.Plugin]
		if !ok || !use.Granted {
			continue
		}
		ui := plugins.UIOf(p.Spec, p.Manifest)
		for _, limit := range use.Limits {
			if l := limitViewOf(limit, ui, now); l.Reached || l.Near {
				out = append(out, pluginNotice{Tenant: use.Tenant, Plugin: use.Plugin, Limit: l})
			}
		}
	}
	rank := func(n pluginNotice) int {
		switch {
		case n.Limit.Reached && n.Limit.Hard():
			return 0
		case n.Limit.Reached:
			return 1
		}
		return 2
	}
	slices.SortStableFunc(out, func(a, b pluginNotice) int {
		if x, y := rank(a), rank(b); x != y {
			return x - y
		}
		return strings.Compare(a.Tenant+a.Plugin, b.Tenant+b.Plugin)
	})
	return out
}

// noticesOfPlugins says, for the overview, the tenants that reached a
// limit of a plugin, or near one: a hard one reached is broken, as the
// tenant's requests are refused.
func noticesOfPlugins(found []pluginNotice) []notice {
	var out []notice
	for _, n := range found {
		l := n.Limit
		text := ""
		switch {
		case l.Reached && l.Hard():
			text = fmt.Sprintf(" used all its %s of plugin %s: its requests to it are refused until %s UTC", l.Said, n.Plugin, l.Until.UTC().Format("2 Jan 15:04"))
		case l.Reached:
			text = fmt.Sprintf(" used all its %s of plugin %s, a soft limit: its requests still pass", l.Said, n.Plugin)
		default:
			text = fmt.Sprintf(" used %s of its %s of plugin %s", l.Used, l.Said, n.Plugin)
		}
		out = append(out, notice{Bad: l.Reached && l.Hard(), Subject: n.Tenant, Href: "/tenants/" + n.Tenant + "#plugins", Text: text,
			Reasons: []string{"plugin " + n.Plugin + " " + map[bool]string{true: "limit reached", false: "limit near"}[l.Reached]},
			Fixes:   []fix{{Label: "Its limits of " + n.Plugin, Href: "/plugins/" + n.Plugin + "?tenant=" + url.QueryEscape(n.Tenant) + "#edit"}}})
	}
	return out
}

// pluginsNotices are the plugins' notices of the cluster now, or none
// without the Plugins pages.
func (s *Server) pluginsNotices(ctx context.Context, actor string) []notice {
	if s.Plugins == nil {
		return nil
	}
	agent := s.Plugins(actor)
	var list []api.Plugin
	var uses []api.PluginUse
	if err := all(
		func() (err error) { list, err = agent.Plugins(ctx); return },
		func() (err error) { uses, err = agent.PluginUses(ctx); return },
	); err != nil {
		return nil
	}
	return noticesOfPlugins(pluginNoticesOf(list, uses, s.now()))
}

// pluginData is a plugin's page.
type pluginData struct {
	Plugin api.Plugin
	UI     plugins.UI
	Kind   string
	Limits []string
	// Hosts are where its requests go; KeyPlace where the key goes on
	// them.
	Hosts    []string
	KeyPlace string
	// Config is its config, field by field, as the module's UI says them.
	Config []configView
	// Metric is the metric shown; Main the first, charted by default.
	Metric plugins.MetricUI
	Chart  usageChart
	// Tenants are those that may use it, or have settings of it.
	Tenants []pluginUseView
	// Details are what the metric was used for this month, the most
	// first.
	Details []detailRow
	// Edit is the tenant whose settings are being changed, if one is.
	Edit *pluginEdit
	// From and To are this month's days so far, for the export.
	From, To string
}

type configView struct {
	Name, Label, Help, Value string
}

type detailRow struct {
	Detail string
	Amount string
	Meter  meter
}

// pluginEdit is the form of a tenant's settings of the plugin.
type pluginEdit struct {
	Tenant string
	Use    pluginUseView
	// Mode is plugin (the plugin's limits), own or none; Text its own
	// limits, as murder plugin limit takes them.
	Mode, Text string
	Problem    string
}

// usageChart is a metric's last days, by day.
type usageChart struct {
	activity
	Title, MaxText, TotalText string
}

func (s *Server) pluginPage(w http.ResponseWriter, r *http.Request) {
	s.pluginPageWith(w, r, nil, "", http.StatusOK)
}

func (s *Server) pluginPageWith(w http.ResponseWriter, r *http.Request, edit *pluginEdit, problem string, status int) {
	ctx, cancel := context.WithTimeout(r.Context(), 10*time.Second)
	defer cancel()
	name := r.PathValue("name")
	agent := s.Plugins(s.actor(userOf(r)))
	now := s.now()
	from := now.AddDate(0, 0, -(pluginDays - 1))
	var list []api.Plugin
	var uses []api.PluginUse
	var usage api.PluginUsage
	if err := all(
		func() (err error) { list, err = agent.Plugins(ctx); return },
		func() (err error) { uses, err = agent.PluginUses(ctx); return },
		func() (err error) {
			usage, err = agent.PluginUsage(ctx, "", name, plugins.Day(from), plugins.Day(now))
			return
		},
	); err != nil {
		s.fail(w, r, err)
		return
	}
	i := slices.IndexFunc(list, func(p api.Plugin) bool { return p.Name == name })
	if i < 0 {
		s.fail(w, r, &api.Error{Status: http.StatusNotFound, Message: "plugin " + name + ": not found"})
		return
	}
	p := list[i]
	ui := plugins.UIOf(p.Spec, p.Manifest)
	data := pluginData{Plugin: p, UI: ui, Kind: kindText(p), Limits: limitsText(p.Spec.Limits, ui), Metric: ui.Metrics[0], From: plugins.Month(now) + "-01", To: plugins.Day(now)}
	data.Hosts, data.KeyPlace = whereTo(p)
	if asked := r.URL.Query().Get("metric"); asked != "" {
		for _, m := range ui.Metrics {
			if m.Name == asked {
				data.Metric = m
			}
		}
	}
	for _, field := range ui.Config {
		value := "not set"
		if v, ok := p.Spec.Config[field.Name]; ok {
			value = configValue(v)
		}
		data.Config = append(data.Config, configView{Name: field.Name, Label: field.Label, Help: field.Help, Value: value})
	}
	// Fields the module's UI does not say, as they are.
	for _, key := range slices.Sorted(maps.Keys(p.Spec.Config)) {
		if !slices.ContainsFunc(ui.Config, func(c plugins.ConfigUI) bool { return c.Name == key }) {
			data.Config = append(data.Config, configView{Name: key, Label: key, Value: configValue(p.Spec.Config[key])})
		}
	}
	for _, use := range uses {
		if use.Plugin == name {
			data.Tenants = append(data.Tenants, pluginUseViewOf(use, ui, data.Metric.Name, now))
		}
	}
	slices.SortFunc(data.Tenants, func(a, b pluginUseView) int { return strings.Compare(a.Tenant, b.Tenant) })
	byDay := map[string]int64{}
	byDetail := map[string]int64{}
	month := plugins.Month(now)
	for _, c := range usage.Counts {
		if c.Metric != data.Metric.Name {
			continue
		}
		byDay[c.Day] += c.Amount
		if strings.HasPrefix(c.Day, month) {
			detail := c.Detail
			if detail == "" {
				detail = "none said"
			}
			byDetail[detail] += c.Amount
		}
	}
	data.Chart = usageChartOf(byDay, data.Metric, now)
	var most int64
	for _, amount := range byDetail {
		most = max(most, amount)
	}
	for _, detail := range slices.SortedFunc(maps.Keys(byDetail), func(a, b string) int {
		if byDetail[a] != byDetail[b] {
			if byDetail[a] > byDetail[b] {
				return -1
			}
			return 1
		}
		return strings.Compare(a, b)
	}) {
		data.Details = append(data.Details, detailRow{Detail: detail, Amount: amountText(byDetail[detail]), Meter: meterOf(int(byDetail[detail]*1000/max(most, 1)), 1000)})
	}
	if edit == nil {
		if tenant := r.URL.Query().Get("tenant"); tenant != "" {
			edit = &pluginEdit{Tenant: tenant}
		}
	}
	if edit != nil {
		for _, t := range data.Tenants {
			if t.Tenant == edit.Tenant {
				edit.Use = t
			}
		}
		if edit.Mode == "" {
			edit.Mode = "plugin"
			if edit.Use.OwnLimits {
				edit.Mode = "own"
				if len(edit.Use.Limits) == 0 {
					edit.Mode = "none"
				}
			}
			var said []string
			for _, l := range edit.Use.Limits {
				said = append(said, l.Limit.String())
			}
			edit.Text = strings.Join(said, "\n")
		}
		data.Edit = edit
	}
	s.renderWith(w, r, "plugin", p.Name, "plugins", data, problem, status)
}

// whereTo says where a plugin's requests go, and where the key goes on
// them.
func whereTo(p api.Plugin) ([]string, string) {
	switch {
	case p.Spec.Kind == plugins.KindOpenAI:
		return []string{p.Spec.Upstream}, plugins.KeyPlace{Header: "Authorization", Prefix: "Bearer "}.String()
	case p.Spec.Kind == plugins.KindHERE:
		return p.Spec.Hosts, plugins.KeyPlace{Query: "apiKey"}.String()
	case p.Manifest != nil:
		hosts := p.Spec.Hosts
		if len(hosts) == 0 {
			hosts = p.Manifest.Hosts
		}
		return hosts, p.Manifest.Key.String()
	}
	return nil, ""
}

// configValue says a value of a config as JSON says it, strings without
// their quotes.
func configValue(v any) string {
	if text, ok := v.(string); ok {
		return text
	}
	return jsonOf(v)
}

// usageChartOf charts a metric's last pluginDays days, today last.
func usageChartOf(byDay map[string]int64, m plugins.MetricUI, now time.Time) usageChart {
	unit := m.Unit
	if unit == "" {
		unit = m.Label
	}
	now = now.UTC()
	start := time.Date(now.Year(), now.Month(), now.Day(), 0, 0, 0, 0, time.UTC).AddDate(0, 0, -(pluginDays - 1))
	c := usageChart{Title: m.Label + ", by day"}
	var most, total int64
	for i := range pluginDays {
		amount := byDay[plugins.Day(start.AddDate(0, 0, i))]
		most, total = max(most, amount), total+amount
	}
	slot := chartWidth / float64(pluginDays)
	w := min(columnMaxWidth, slot*0.62)
	var points [][]any
	for i := range pluginDays {
		day := start.AddDate(0, 0, i)
		amount := byDay[plugins.Day(day)]
		col := column{X: float64(i)*slot + (slot-w)/2, W: w, N: int(min(amount, 1<<31)), Now: i == pluginDays-1}
		if amount > 0 && most > 0 {
			col.H = max(2, plotHeight*float64(amount)/float64(most))
		}
		col.Y = plotHeight - col.H
		what := amountText(amount) + " " + unit
		col.Title = dayLabel(day) + ": " + what
		points = append(points, []any{round1(float64(i)*slot + slot/2), dayLabel(day), what})
		c.Columns = append(c.Columns, col)
	}
	c.From, c.Mid, c.To = dayLabel(start), dayLabel(start.AddDate(0, 0, pluginDays/2)), "today"
	c.Points = jsonOf(points)
	c.MaxText, c.TotalText = amountText(most), amountText(total)+" "+unit
	c.Summary = fmt.Sprintf("%s %s in the last %d days, since %s UTC; the most in a day, %s", amountText(total), unit, pluginDays, dayLabel(start), amountText(most))
	return c
}

// pluginAction sets the plugin's key, or a tenant's own limits or key.
func (s *Server) pluginAction(w http.ResponseWriter, r *http.Request) {
	if !readForm(w, r) {
		return
	}
	name := r.PathValue("name")
	agent := s.Plugins(s.actor(userOf(r)))
	ctx, cancel := context.WithTimeout(r.Context(), 30*time.Second)
	defer cancel()
	back := "/plugins/" + name
	tenant := strings.TrimSpace(r.FormValue("tenant"))
	done := func(message string, err error) {
		if err != nil {
			flash(w, r, "problem", err.Error())
		} else {
			flash(w, r, "done", message)
		}
		http.Redirect(w, r, back, http.StatusSeeOther)
	}
	switch r.FormValue("do") {
	case "key":
		key := strings.TrimSpace(r.FormValue("key"))
		if key == "" || strings.ContainsAny(key, "\r\n") {
			done("", fmt.Errorf("a key is one line, not empty"))
			return
		}
		whose := "plugin " + name + "'s key is set: every tenant without one of its own uses it"
		if tenant != "" {
			whose = tenant + " has a key of its own of " + name + ": its use is counted, and only its own limits hold"
			back += "#tenants"
		}
		done(whose, agent.SetPluginKey(ctx, name, tenant, key))
	case "key-remove":
		if tenant == "" && !typed(r.FormValue("confirm"), name) {
			done("", fmt.Errorf("type %s to remove its key", name))
			return
		}
		whose := "plugin " + name + "'s key is removed: its tenants without one of their own are answered 503"
		if tenant != "" {
			whose = tenant + "'s own key of " + name + " is removed: it uses the plugin's again"
			back += "#tenants"
		}
		done(whose, agent.SetPluginKey(ctx, name, tenant, ""))
	case "limits":
		edit := &pluginEdit{Tenant: tenant, Mode: r.FormValue("mode"), Text: r.FormValue("limits")}
		var limits []plugins.Limit
		set := true
		switch edit.Mode {
		case "plugin":
			set = false
		case "none":
		case "own":
			for _, field := range strings.Fields(strings.ReplaceAll(edit.Text, ",", " ")) {
				limit, err := plugins.ParseLimit(field)
				if err != nil {
					s.pluginPageWith(w, r, edit, err.Error(), http.StatusUnprocessableEntity)
					return
				}
				limits = append(limits, limit)
			}
			if len(limits) == 0 {
				s.pluginPageWith(w, r, edit, "no limit given: write one a line, as tokens=5M/month, or choose no limits", http.StatusUnprocessableEntity)
				return
			}
		default:
			s.pluginPageWith(w, r, edit, "choose the plugin's limits, limits of its own, or none", http.StatusUnprocessableEntity)
			return
		}
		if err := agent.SetPluginLimits(ctx, name, tenant, limits, set); err != nil {
			s.pluginPageWith(w, r, edit, err.Error(), http.StatusUnprocessableEntity)
			return
		}
		said := map[string]string{"plugin": " has the plugin's limits of ", "none": " has no limits of ", "own": " has limits of its own of "}[edit.Mode]
		back += "#tenants"
		done(tenant+said+name, nil)
	default:
		done("", fmt.Errorf("nothing to do"))
	}
}

// pluginUsageCSV exports what tenants used of a plugin, a row a day,
// tenant, metric and detail, from and to the days the address says (this
// month so far by default).
func (s *Server) pluginUsageCSV(w http.ResponseWriter, r *http.Request) {
	ctx, cancel := context.WithTimeout(r.Context(), 30*time.Second)
	defer cancel()
	name := r.PathValue("name")
	usage, err := s.Plugins(s.actor(userOf(r))).PluginUsage(ctx, r.URL.Query().Get("tenant"), name, r.URL.Query().Get("from"), r.URL.Query().Get("to"))
	if err != nil {
		s.fail(w, r, err)
		return
	}
	w.Header().Set("Content-Type", "text/csv; charset=utf-8")
	w.Header().Set("Content-Disposition", fmt.Sprintf("attachment; filename=%q", name+"-"+usage.From+"-"+usage.To+".csv"))
	out := csv.NewWriter(w)
	_ = out.Write([]string{"tenant", "plugin", "day", "metric", "detail", "amount"})
	for _, c := range usage.Counts {
		_ = out.Write([]string{c.Tenant, c.Plugin, c.Day, c.Metric, c.Detail, strconv.FormatInt(c.Amount, 10)})
	}
	out.Flush()
}

// tenantPlugins is a tenant's Plugins box: its use of each plugin, or nil
// when it uses none, or the pages are not served.
func (s *Server) tenantPlugins(ctx context.Context, actor, tenant string) []tenantPluginView {
	if s.Plugins == nil {
		return nil
	}
	agent := s.Plugins(actor)
	var list []api.Plugin
	var uses []api.PluginUse
	if err := all(
		func() (err error) { list, err = agent.Plugins(ctx); return },
		func() (err error) { uses, err = agent.PluginUses(ctx); return },
	); err != nil {
		return nil
	}
	now := s.now()
	var out []tenantPluginView
	for _, use := range uses {
		if use.Tenant != tenant {
			continue
		}
		i := slices.IndexFunc(list, func(p api.Plugin) bool { return p.Name == use.Plugin })
		if i < 0 {
			continue
		}
		ui := plugins.UIOf(list[i].Spec, list[i].Manifest)
		v := tenantPluginView{UI: ui, Use: pluginUseViewOf(use, ui, ui.Metrics[0].Name, now)}
		// In the order the plugin says its metrics, the main first.
		for _, m := range ui.Metrics {
			for _, used := range use.Month {
				if used.Metric == m.Name {
					v.Month = append(v.Month, usedView{Label: m.Label, Amount: amountText(used.Amount)})
				}
			}
		}
		out = append(out, v)
	}
	return out
}

// tenantPluginView is a tenant's use of one plugin, on its page.
type tenantPluginView struct {
	UI    plugins.UI
	Use   pluginUseView
	Month []usedView
}

type usedView struct{ Label, Amount string }
