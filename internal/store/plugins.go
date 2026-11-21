package store

import (
	"crypto/sha256"
	"database/sql"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"maps"
	"regexp"
	"slices"
	"strings"
	"time"

	"github.com/PowerPenguini/murder/internal/plugins"
)

// PluginsSchema is the schema version that keeps plugins, tenants' own
// settings of them, and what tenants used of them: an agent counts its
// edge's reports only once every node runs a release of this version or
// later, which applies those writes. AddPluginUsage writes its batches as
// of this version, frozen or not, as an upgrade replaces the edge while
// changes are frozen: a migration must keep plugin_usage, plugin_reports
// and events as this version runs them.
const PluginsSchema = 21

// Plugin is a plugin installed in the cluster (see internal/plugins): its
// spec and the operator's key, sealed with the master key under
// plugins.KeyContext; empty when it has none.
type Plugin struct {
	Name string       `json:"name"`
	Spec plugins.Spec `json:"spec"`
	// Manifest is what the module of a plugin of kind wasm says of
	// itself.
	Manifest  *plugins.Manifest `json:"manifest,omitempty"`
	Key       string            `json:"-"`
	CreatedAt time.Time         `json:"createdAt"`
	UpdatedAt time.Time         `json:"updatedAt"`
}

// Metrics are what the plugin counts, which its limits may name.
func (p Plugin) Metrics() []string { return plugins.MetricsOf(p.Spec, p.Manifest) }

// PluginModule is a plugin's module as it is installed: its digest, the
// manifest read from it, and the module itself.
type PluginModule struct {
	Digest   string
	Manifest plugins.Manifest
	Wasm     []byte
}

// PluginTenant is a tenant's own settings of a plugin: a key of its own,
// sealed under plugins.TenantKeyContext, and limits in place of the
// plugin's (when HasLimits; none at all for an empty list).
type PluginTenant struct {
	Tenant    string          `json:"tenant"`
	Plugin    string          `json:"plugin"`
	Key       string          `json:"-"`
	HasLimits bool            `json:"hasLimits,omitempty"`
	Limits    []plugins.Limit `json:"limits,omitempty"`
	UpdatedAt time.Time       `json:"updatedAt"`
}

// pluginReport is the last report of an edge process a node's agent
// counted.
type pluginReport struct {
	Seq int64
	At  time.Time
}

// reportsKept is how long the last report of an edge process is kept: an
// edge sends one again within seconds, or never.
const reportsKept = 48 * time.Hour

// pluginTables are the tables schema 21 added, as one script.
const pluginTables = `-- Plugins (see internal/plugins): their spec as JSON, and the operator's
-- key, sealed with the master key; empty for none.
CREATE TABLE plugins (
	name       TEXT PRIMARY KEY,
	spec       TEXT NOT NULL,
	key        TEXT NOT NULL DEFAULT '',
	created_at TEXT NOT NULL,
	updated_at TEXT NOT NULL
) STRICT;

-- The modules of plugins of kind wasm, by digest: the manifest read from
-- each as it was installed, as JSON, and the module, in base64. A module
-- goes with the last plugin that runs it.
CREATE TABLE plugin_modules (
	digest     TEXT PRIMARY KEY,
	manifest   TEXT NOT NULL,
	wasm       TEXT NOT NULL,
	size       INTEGER NOT NULL,
	created_at TEXT NOT NULL
) STRICT;

-- A tenant's own settings of a plugin: a key of its own, sealed, and
-- limits in place of the plugin's, as JSON; NULL for the plugin's.
CREATE TABLE plugin_tenants (
	tenant     TEXT NOT NULL REFERENCES tenants (name) ON DELETE CASCADE,
	plugin     TEXT NOT NULL REFERENCES plugins (name) ON DELETE CASCADE,
	key        TEXT NOT NULL DEFAULT '',
	limits     TEXT,
	updated_at TEXT NOT NULL,
	PRIMARY KEY (tenant, plugin)
) STRICT;

-- What tenants used of plugins, a day (UTC) at a time: an amount of a
-- metric, for a detail (a model, a service; empty for none). Kept when
-- the plugin goes, until the tenant is purged.
CREATE TABLE plugin_usage (
	tenant TEXT NOT NULL,
	plugin TEXT NOT NULL,
	day    TEXT NOT NULL,
	metric TEXT NOT NULL,
	detail TEXT NOT NULL,
	amount INTEGER NOT NULL,
	PRIMARY KEY (tenant, plugin, day, metric, detail)
) STRICT;

-- The last report of each edge process that a node's agent counted, so
-- one the edge sends again is not counted twice.
CREATE TABLE plugin_reports (
	node TEXT NOT NULL,
	edge TEXT NOT NULL,
	seq  INTEGER NOT NULL,
	at   TEXT NOT NULL,
	PRIMARY KEY (node, edge)
) STRICT;`

// loadPlugins reads the plugins, tenants' settings and the edges' last
// reports. What tenants used is read when asked for, not kept in memory.
func loadPlugins(q querier, state *snapshot) error {
	state.plugins = map[string]Plugin{}
	state.pluginTenants = map[[2]string]PluginTenant{}
	state.pluginReports = map[[2]string]pluginReport{}
	state.pluginManifests = map[string]plugins.Manifest{}
	// The modules themselves are read when an edge asks for one.
	rows, err := q.Query(`SELECT digest, manifest FROM plugin_modules`)
	if err != nil {
		return err
	}
	for rows.Next() {
		var digest, manifestText string
		var manifest plugins.Manifest
		if err := rows.Scan(&digest, &manifestText); err != nil {
			rows.Close()
			return err
		}
		if err := json.Unmarshal([]byte(manifestText), &manifest); err != nil {
			rows.Close()
			return fmt.Errorf("module %s: %w", digest, err)
		}
		state.pluginManifests[digest] = manifest
	}
	if err := closeRows(rows); err != nil {
		return err
	}
	rows, err = q.Query(`SELECT name, spec, key, created_at, updated_at FROM plugins`)
	if err != nil {
		return err
	}
	for rows.Next() {
		var p Plugin
		var specText, created, updated string
		if err := rows.Scan(&p.Name, &specText, &p.Key, &created, &updated); err != nil {
			rows.Close()
			return err
		}
		if err := json.Unmarshal([]byte(specText), &p.Spec); err != nil {
			rows.Close()
			return fmt.Errorf("plugin %s: %w", p.Name, err)
		}
		p.CreatedAt, p.UpdatedAt = parseTime(created), parseTime(updated)
		if manifest, ok := state.pluginManifests[p.Spec.Module]; ok && p.Spec.Kind == plugins.KindWasm {
			p.Manifest = &manifest
		}
		state.plugins[p.Name] = p
	}
	if err := closeRows(rows); err != nil {
		return err
	}
	rows, err = q.Query(`SELECT tenant, plugin, key, limits, updated_at FROM plugin_tenants`)
	if err != nil {
		return err
	}
	for rows.Next() {
		var t PluginTenant
		var limits sql.NullString
		var updated string
		if err := rows.Scan(&t.Tenant, &t.Plugin, &t.Key, &limits, &updated); err != nil {
			rows.Close()
			return err
		}
		if limits.Valid {
			t.HasLimits = true
			if err := json.Unmarshal([]byte(limits.String), &t.Limits); err != nil {
				rows.Close()
				return fmt.Errorf("tenant %s, plugin %s: %w", t.Tenant, t.Plugin, err)
			}
		}
		t.UpdatedAt = parseTime(updated)
		state.pluginTenants[[2]string{t.Tenant, t.Plugin}] = t
	}
	if err := closeRows(rows); err != nil {
		return err
	}
	rows, err = q.Query(`SELECT node, edge, seq, at FROM plugin_reports`)
	if err != nil {
		return err
	}
	for rows.Next() {
		var node, edge, at string
		var seq int64
		if err := rows.Scan(&node, &edge, &seq, &at); err != nil {
			rows.Close()
			return err
		}
		state.pluginReports[[2]string{node, edge}] = pluginReport{Seq: seq, At: parseTime(at)}
	}
	return closeRows(rows)
}

// Plugins lists the plugins, by name.
func (s *SQLite) Plugins() []Plugin {
	state := s.state.Load()
	list := make([]Plugin, 0, len(state.plugins))
	for _, name := range slices.Sorted(maps.Keys(state.plugins)) {
		list = append(list, clonePlugin(state.plugins[name]))
	}
	return list
}

// Plugin returns one plugin.
func (s *SQLite) Plugin(name string) (Plugin, error) {
	plugin, ok := s.state.Load().plugins[name]
	if !ok {
		return Plugin{}, fmt.Errorf("plugin %s: %w", name, ErrNotFound)
	}
	return clonePlugin(plugin), nil
}

func clonePlugin(p Plugin) Plugin {
	p.Spec.Hosts = slices.Clone(p.Spec.Hosts)
	p.Spec.Allow = slices.Clone(p.Spec.Allow)
	p.Spec.Limits = slices.Clone(p.Spec.Limits)
	p.Spec.Config = maps.Clone(p.Spec.Config)
	if p.Manifest != nil {
		manifest := *p.Manifest
		manifest.Hosts = slices.Clone(manifest.Hosts)
		manifest.Metrics = slices.Clone(manifest.Metrics)
		p.Manifest = &manifest
	}
	return p
}

// PluginModule returns the module of a digest, as an edge fetches it.
func (s *SQLite) PluginModule(digest string) ([]byte, error) {
	var encoded string
	err := s.db.QueryRow(`SELECT wasm FROM plugin_modules WHERE digest = ?`, digest).Scan(&encoded)
	if errors.Is(err, sql.ErrNoRows) {
		return nil, fmt.Errorf("module %s: %w", digest, ErrNotFound)
	}
	if err != nil {
		return nil, err
	}
	return base64.StdEncoding.DecodeString(encoded)
}

// dropModules removes the modules no plugin runs, but for keep.
func dropModules(tx *recorder, current *snapshot, gone, keep string) error {
	runs := map[string]bool{keep: true}
	for name, plugin := range current.plugins {
		if name != gone && plugin.Spec.Kind == plugins.KindWasm {
			runs[plugin.Spec.Module] = true
		}
	}
	for _, digest := range slices.Sorted(maps.Keys(current.pluginManifests)) {
		if !runs[digest] {
			if err := tx.Exec(`DELETE FROM plugin_modules WHERE digest = ?`, digest); err != nil {
				return err
			}
		}
	}
	return nil
}

// PluginTenants lists tenants' own settings of plugins: of one tenant, of
// one plugin, or (both empty) all, by tenant and plugin.
func (s *SQLite) PluginTenants(tenant, plugin string) []PluginTenant {
	state := s.state.Load()
	var list []PluginTenant
	for _, key := range slices.SortedFunc(maps.Keys(state.pluginTenants), func(a, b [2]string) int {
		return strings.Compare(a[0]+"\x00"+a[1], b[0]+"\x00"+b[1])
	}) {
		if (tenant == "" || key[0] == tenant) && (plugin == "" || key[1] == plugin) {
			settings := state.pluginTenants[key]
			settings.Limits = slices.Clone(settings.Limits)
			list = append(list, settings)
		}
	}
	return list
}

// pluginTenantsOf lists a state's tenants' settings of plugins, by tenant
// and plugin.
func (s *SQLite) pluginTenantsOf(current *snapshot) []PluginTenant {
	var list []PluginTenant
	for _, key := range slices.SortedFunc(maps.Keys(current.pluginTenants), func(a, b [2]string) int {
		return strings.Compare(a[0]+"\x00"+a[1], b[0]+"\x00"+b[1])
	}) {
		list = append(list, current.pluginTenants[key])
	}
	return list
}

// PluginLimits are the limits that hold for a tenant's use of a plugin:
// its own, if it has any, or else the plugin's, unless it has a key of its
// own.
func PluginLimits(plugin Plugin, settings PluginTenant) []plugins.Limit {
	if settings.HasLimits {
		return slices.Clone(settings.Limits)
	}
	if settings.Key != "" {
		return nil
	}
	return slices.Clone(plugin.Spec.Limits)
}

// PutPlugin installs a plugin, or changes its spec; changed is false for
// the same spec. The spec is checked first. A plugin of kind wasm runs a
// module installed already, or module, of the spec's digest, its manifest
// read by the caller: a module is never run as it is written.
func (s *SQLite) PutPlugin(name string, spec plugins.Spec, module *PluginModule, actor string) (Plugin, bool, error) {
	if err := plugins.ValidName(name); err != nil {
		return Plugin{}, false, fmt.Errorf("%w: %v", ErrInvalid, err)
	}
	spec = spec.WithDefaults()
	if err := spec.Validate(); err != nil {
		return Plugin{}, false, fmt.Errorf("%w: plugin %s: %v", ErrInvalid, name, err)
	}
	if module != nil {
		switch {
		case spec.Kind != plugins.KindWasm || module.Digest != spec.Module:
			return Plugin{}, false, fmt.Errorf("%w: plugin %s: the module sent is not the one its spec names", ErrInvalid, name)
		case plugins.ModuleDigest(sha256.Sum256(module.Wasm)) != module.Digest:
			return Plugin{}, false, fmt.Errorf("%w: plugin %s: the module sent is not of its digest", ErrInvalid, name)
		case len(module.Wasm) > plugins.MaxModule:
			return Plugin{}, false, fmt.Errorf("%w: plugin %s: a module is at most %d MB", ErrInvalid, name, plugins.MaxModule>>20)
		}
		if err := module.Manifest.Validate(); err != nil {
			return Plugin{}, false, fmt.Errorf("%w: plugin %s: %v", ErrInvalid, name, err)
		}
	}
	encoded, err := json.Marshal(spec)
	if err != nil {
		return Plugin{}, false, err
	}
	next, err := s.write(false, func(tx *recorder, current *snapshot, now time.Time) error {
		var manifest *plugins.Manifest
		if spec.Kind == plugins.KindWasm {
			if installed, ok := current.pluginManifests[spec.Module]; ok {
				manifest = &installed
			} else if module != nil {
				manifest = &module.Manifest
				manifestText, err := json.Marshal(module.Manifest)
				if err != nil {
					return err
				}
				if err := tx.Exec(`INSERT INTO plugin_modules (digest, manifest, wasm, size, created_at) VALUES (?, ?, ?, ?, ?)`,
					module.Digest, string(manifestText), base64.StdEncoding.EncodeToString(module.Wasm), int64(len(module.Wasm)), formatTime(now)); err != nil {
					return err
				}
			} else {
				return fmt.Errorf("%w: plugin %s: module %s is not installed: murder plugin apply sends it", ErrInvalid, name, spec.Module)
			}
			if err := spec.ValidateWith(manifest); err != nil {
				return fmt.Errorf("%w: plugin %s: %v", ErrInvalid, name, err)
			}
		}
		existing, found := current.plugins[name]
		if found {
			previous, _ := json.Marshal(existing.Spec)
			if string(previous) == string(encoded) {
				return errUnchanged
			}
			if existing.Spec.Kind != spec.Kind {
				return fmt.Errorf("%w: plugin %s is of kind %s; delete it to install one of kind %s", ErrConflict, name, existing.Spec.Kind, spec.Kind)
			}
		}
		// Tenants' own limits must still be of metrics the plugin counts.
		for key, settings := range current.pluginTenants {
			if key[1] == name && settings.HasLimits {
				if err := plugins.ValidLimits(plugins.MetricsOf(spec, manifest), settings.Limits); err != nil {
					return fmt.Errorf("%w: tenant %s's limits of plugin %s: %v", ErrConflict, key[0], name, err)
				}
			}
		}
		stamp := formatTime(now)
		if err := tx.Exec(`INSERT INTO plugins (name, spec, created_at, updated_at) VALUES (?, ?, ?, ?)
			ON CONFLICT (name) DO UPDATE SET spec = excluded.spec, updated_at = excluded.updated_at`, name, string(encoded), stamp, stamp); err != nil {
			return err
		}
		// The module the plugin ran before goes, unless another runs it.
		if err := dropModules(tx, current, name, spec.Module); err != nil {
			return err
		}
		action := "plugin.installed"
		if found {
			action = "plugin.changed"
		}
		return event(tx, now, actor, action, "", "", name+" ("+spec.Kind+")")
	})
	if errors.Is(err, errUnchanged) {
		plugin, err := s.Plugin(name)
		return plugin, false, err
	}
	if err != nil {
		return Plugin{}, false, err
	}
	return clonePlugin(next.plugins[name]), true, nil
}

// DeletePlugin removes a plugin, its key and tenants' settings of it; what
// tenants used of it is kept. An app that lists it keeps it.
func (s *SQLite) DeletePlugin(name, actor string) error {
	_, err := s.write(false, func(tx *recorder, current *snapshot, now time.Time) error {
		if _, ok := current.plugins[name]; !ok {
			return fmt.Errorf("plugin %s: %w", name, ErrNotFound)
		}
		if apps := appsUsing(current, name); len(apps) > 0 {
			return fmt.Errorf("%w: app %s lists plugin %s under plugins; take it out first", ErrConflict, strings.Join(apps, ", "), name)
		}
		if err := tx.Exec(`DELETE FROM plugins WHERE name = ?`, name); err != nil {
			return err
		}
		if err := dropModules(tx, current, name, ""); err != nil {
			return err
		}
		return event(tx, now, actor, "plugin.deleted", "", "", name)
	})
	return err
}

// appsUsing names the apps whose newest spec lists a plugin. A tenant
// that still runs an older revision listing it is refused the plugin
// once it is gone.
func appsUsing(current *snapshot, plugin string) []string {
	var names []string
	for _, name := range slices.Sorted(maps.Keys(current.apps)) {
		if slices.Contains(current.apps[name].Spec.Plugins, plugin) {
			names = append(names, name)
		}
	}
	return names
}

// SetPluginKey keeps the key of a plugin, sealed by the caller: the
// operator's, or with tenant a tenant's own; an empty one removes it.
func (s *SQLite) SetPluginKey(plugin, tenant, sealed, actor string) error {
	_, err := s.write(false, func(tx *recorder, current *snapshot, now time.Time) error {
		if _, ok := current.plugins[plugin]; !ok {
			return fmt.Errorf("plugin %s: %w", plugin, ErrNotFound)
		}
		stamp := formatTime(now)
		if tenant == "" {
			if err := tx.Exec(`UPDATE plugins SET key = ?, updated_at = ? WHERE name = ?`, sealed, stamp, plugin); err != nil {
				return err
			}
			action := "plugin.key-set"
			if sealed == "" {
				action = "plugin.key-removed"
			}
			return event(tx, now, actor, action, "", "", plugin)
		}
		record, ok := current.tenants[tenant]
		if !ok {
			return fmt.Errorf("tenant %s: %w", tenant, ErrNotFound)
		}
		if err := putPluginTenant(tx, current, tenant, plugin, func(t *PluginTenant) { t.Key = sealed }, stamp); err != nil {
			return err
		}
		action := "plugin.tenant-key-set"
		if sealed == "" {
			action = "plugin.tenant-key-removed"
		}
		return event(tx, now, actor, action, tenant, record.App, plugin)
	})
	return err
}

// SetPluginLimits gives a tenant limits of a plugin of its own, in place
// of the plugin's (none at all for an empty list), or, with set false,
// takes them away: the plugin's hold again.
func (s *SQLite) SetPluginLimits(plugin, tenant string, limits []plugins.Limit, set bool, actor string) error {
	limits = slices.Clone(limits)
	for i := range limits {
		if limits[i].Mode == "" {
			limits[i].Mode = plugins.ModeHard
		}
	}
	limits = plugins.SortLimits(limits)
	_, err := s.write(false, func(tx *recorder, current *snapshot, now time.Time) error {
		p, ok := current.plugins[plugin]
		if !ok {
			return fmt.Errorf("plugin %s: %w", plugin, ErrNotFound)
		}
		if err := plugins.ValidLimits(p.Metrics(), limits); err != nil {
			return fmt.Errorf("%w: %v", ErrInvalid, err)
		}
		record, ok := current.tenants[tenant]
		if !ok {
			return fmt.Errorf("tenant %s: %w", tenant, ErrNotFound)
		}
		if err := putPluginTenant(tx, current, tenant, plugin, func(t *PluginTenant) { t.HasLimits, t.Limits = set, limits }, formatTime(now)); err != nil {
			return err
		}
		detail := plugin + ": the plugin's limits"
		if set {
			said := make([]string, len(limits))
			for i, limit := range limits {
				said[i] = limit.String()
			}
			detail = plugin + ": " + strings.Join(said, " ")
			if len(limits) == 0 {
				detail = plugin + ": no limits"
			}
		}
		return event(tx, now, actor, "plugin.tenant-limits-set", tenant, record.App, detail)
	})
	return err
}

// putPluginTenant changes a tenant's settings of a plugin, dropping the
// row once it holds nothing of its own.
func putPluginTenant(tx *recorder, current *snapshot, tenant, plugin string, change func(*PluginTenant), stamp string) error {
	settings, ok := current.pluginTenants[[2]string{tenant, plugin}]
	if !ok {
		settings = PluginTenant{Tenant: tenant, Plugin: plugin}
	}
	change(&settings)
	if settings.Key == "" && !settings.HasLimits {
		return tx.Exec(`DELETE FROM plugin_tenants WHERE tenant = ? AND plugin = ?`, tenant, plugin)
	}
	var limits any
	if settings.HasLimits {
		encoded, err := json.Marshal(settings.Limits)
		if err != nil {
			return err
		}
		if settings.Limits == nil {
			encoded = []byte("[]")
		}
		limits = string(encoded)
	}
	return tx.Exec(`INSERT INTO plugin_tenants (tenant, plugin, key, limits, updated_at) VALUES (?, ?, ?, ?, ?)
		ON CONFLICT (tenant, plugin) DO UPDATE SET key = excluded.key, limits = excluded.limits, updated_at = excluded.updated_at`,
		tenant, plugin, settings.Key, limits, stamp)
}

// usageDay is a day as plugin usage is kept: 2006-01-02.
var usageDay = regexp.MustCompile(`^\d{4}-\d{2}-\d{2}$`)

// maxDetail is the longest detail kept, such as a model's name.
const maxDetail = 128

// maxReportAmount bounds one count of a report, far above any real one.
const maxReportAmount = plugins.MaxCount

// validCount reports whether a count is one to keep: of a day, a metric
// and a detail as the edge names them, and an amount.
func validCount(count plugins.Count) bool {
	return usageDay.MatchString(count.Day) && count.Amount >= 1 && count.Amount <= maxReportAmount &&
		len(count.Detail) <= maxDetail && count.Metric != "" && len(count.Metric) <= 64 && count.Tenant != "" && count.Plugin != ""
}

// UsageKept is how long what tenants used of plugins is kept: past it,
// the days are pruned.
const UsageKept = 400 * 24 * time.Hour

// AddPluginUsage counts a report of the edge on node, unless it counted it
// already (counted is then false): reports of an edge process are counted
// in order, each once. Counts of tenants no longer in the state, and
// counts not of a form kept, are left out. A limit a count goes past, or
// comes within a fifth of, is said in the audit log. Once a day, it also
// prunes days older than UsageKept.
func (s *SQLite) AddPluginUsage(node string, report plugins.Report) (counted bool, err error) {
	if report.Edge == "" || report.Seq < 1 || len(report.Edge) > 128 {
		return false, fmt.Errorf("%w: a report names its edge and its number", ErrInvalid)
	}
	prune := ""
	// Frozen or not: the edge an upgrade replaces tells its last counts
	// while changes are frozen. Its statements are of PluginsSchema.
	_, err = s.writeAs(PluginsSchema, false, false, func(tx *recorder, current *snapshot, now time.Time) error {
		key := [2]string{node, report.Edge}
		if last, ok := current.pluginReports[key]; ok && report.Seq <= last.Seq {
			return errUnchanged
		}
		// Which limits each count may cross, and what was used before.
		type crossing struct {
			tenant, plugin string
			limit          plugins.Limit
			period         string
			before, added  int64
		}
		var crossings []*crossing
		watched := map[[3]string][]*crossing{}
		for _, count := range report.Counts {
			if _, ok := current.tenants[count.Tenant]; !ok || !validCount(count) {
				continue
			}
			if err := tx.Exec(`INSERT INTO plugin_usage (tenant, plugin, day, metric, detail, amount) VALUES (?, ?, ?, ?, ?, ?)
				ON CONFLICT (tenant, plugin, day, metric, detail) DO UPDATE SET amount = amount + excluded.amount`,
				count.Tenant, count.Plugin, count.Day, count.Metric, count.Detail, count.Amount); err != nil {
				return err
			}
			plugin, ok := current.plugins[count.Plugin]
			if !ok {
				continue
			}
			k := [3]string{count.Tenant, count.Plugin, count.Metric}
			if _, seen := watched[k]; !seen {
				for _, limit := range PluginLimits(plugin, current.pluginTenants[[2]string{count.Tenant, count.Plugin}]) {
					if limit.Metric == count.Metric {
						c := &crossing{tenant: count.Tenant, plugin: count.Plugin, limit: limit, period: plugins.PeriodOf(limit.Per, now)}
						before, err := s.pluginUsed(count.Tenant, count.Plugin, count.Metric, c.period)
						if err != nil {
							return err
						}
						c.before = before
						watched[k] = append(watched[k], c)
						crossings = append(crossings, c)
					}
				}
				if watched[k] == nil {
					watched[k] = []*crossing{}
				}
			}
			for _, c := range watched[k] {
				if strings.HasPrefix(count.Day, c.period) {
					c.added += count.Amount
				}
			}
		}
		for _, c := range crossings {
			after := c.before + c.added
			near := c.limit.Amount - c.limit.Amount/5
			action := ""
			switch {
			case c.before < c.limit.Amount && after >= c.limit.Amount:
				action = "plugin.limit-reached"
			case c.before < near && after >= near:
				action = "plugin.limit-near"
			default:
				continue
			}
			detail := fmt.Sprintf("%s: %d of %s this %s", c.plugin, after, c.limit, c.limit.Per)
			if err := event(tx, now, "murder", action, c.tenant, current.tenants[c.tenant].App, detail); err != nil {
				return err
			}
		}
		if err := tx.Exec(`INSERT INTO plugin_reports (node, edge, seq, at) VALUES (?, ?, ?, ?)
			ON CONFLICT (node, edge) DO UPDATE SET seq = excluded.seq, at = excluded.at`, node, report.Edge, report.Seq, formatTime(now)); err != nil {
			return err
		}
		prune = ""
		if today := plugins.Day(now); s.usagePruned.Load() != today {
			prune = today
			if err := tx.Exec(`DELETE FROM plugin_usage WHERE day < ?`, plugins.Day(now.Add(-UsageKept))); err != nil {
				return err
			}
		}
		for _, other := range slices.SortedFunc(maps.Keys(current.pluginReports), func(a, b [2]string) int {
			return strings.Compare(a[0]+"\x00"+a[1], b[0]+"\x00"+b[1])
		}) {
			if other != key && now.Sub(current.pluginReports[other].At) > reportsKept {
				if err := tx.Exec(`DELETE FROM plugin_reports WHERE node = ? AND edge = ?`, other[0], other[1]); err != nil {
					return err
				}
			}
		}
		return nil
	})
	if errors.Is(err, errUnchanged) {
		return false, nil
	}
	if err == nil && prune != "" {
		s.usagePruned.Store(prune)
	}
	return err == nil, err
}

// pluginUsed is what a tenant used of a plugin's metric in a period: a day
// (2006-01-02) or a month (2006-01).
func (s *SQLite) pluginUsed(tenant, plugin, metric, period string) (int64, error) {
	var used int64
	from, to := daysOf(period)
	err := s.db.QueryRow(`SELECT COALESCE(SUM(amount), 0) FROM plugin_usage WHERE tenant = ? AND plugin = ? AND metric = ? AND day >= ? AND day <= ?`,
		tenant, plugin, metric, from, to).Scan(&used)
	return used, err
}

// daysOf are the first and last day a period may hold: itself for a day,
// the 1st to the 31st for a month.
func daysOf(period string) (string, string) {
	if len(period) == len("2006-01") {
		return period + "-01", period + "-31"
	}
	return period, period
}

// PluginReported is the last report of an edge process on node counted:
// its number, 0 for none.
func (s *SQLite) PluginReported(node, edge string) int64 {
	return s.state.Load().pluginReports[[2]string{node, edge}].Seq
}

// PluginTotals are what tenants used, by plugin and metric, today and this
// month (UTC), summed over details.
func (s *SQLite) PluginTotals(tenants []string, now time.Time) ([]plugins.Total, error) {
	var totals []plugins.Total
	today, month := plugins.Day(now), plugins.Month(now)
	for _, tenant := range tenants {
		from, to := daysOf(month)
		rows, err := s.db.Query(`SELECT plugin, metric, day = ?, SUM(amount) FROM plugin_usage WHERE tenant = ? AND day >= ? AND day <= ? GROUP BY plugin, metric, day = ?`,
			today, tenant, from, to, today)
		if err != nil {
			return nil, err
		}
		byMonth := map[[2]string]int64{}
		for rows.Next() {
			var plugin, metric string
			var isToday bool
			var amount int64
			if err := rows.Scan(&plugin, &metric, &isToday, &amount); err != nil {
				rows.Close()
				return nil, err
			}
			byMonth[[2]string{plugin, metric}] += amount
			if isToday {
				totals = append(totals, plugins.Total{Tenant: tenant, Plugin: plugin, Period: today, Metric: metric, Amount: amount})
			}
		}
		if err := closeRows(rows); err != nil {
			return nil, err
		}
		for _, k := range slices.SortedFunc(maps.Keys(byMonth), func(a, b [2]string) int { return strings.Compare(a[0]+"\x00"+a[1], b[0]+"\x00"+b[1]) }) {
			totals = append(totals, plugins.Total{Tenant: tenant, Plugin: k[0], Period: month, Metric: k[1], Amount: byMonth[k]})
		}
	}
	return totals, nil
}

// UsageQuery picks what PluginUsage returns: a tenant's or every
// tenant's, a plugin's or every plugin's, from day From to day To, both
// included (2006-01-02).
type UsageQuery struct {
	Tenant, Plugin string
	From, To       string
}

// PluginUsage lists what tenants used, a day, metric and detail at a time,
// by tenant, plugin, day, metric and detail.
func (s *SQLite) PluginUsage(query UsageQuery) ([]plugins.Count, error) {
	if !usageDay.MatchString(query.From) || !usageDay.MatchString(query.To) {
		return nil, fmt.Errorf("%w: days are 2006-01-02", ErrInvalid)
	}
	conditions, args := []string{"day >= ?", "day <= ?"}, []any{query.From, query.To}
	if query.Tenant != "" {
		conditions, args = append(conditions, "tenant = ?"), append(args, query.Tenant)
	}
	if query.Plugin != "" {
		conditions, args = append(conditions, "plugin = ?"), append(args, query.Plugin)
	}
	rows, err := s.db.Query(`SELECT tenant, plugin, day, metric, detail, amount FROM plugin_usage WHERE `+strings.Join(conditions, " AND ")+` ORDER BY tenant, plugin, day, metric, detail`, args...)
	if err != nil {
		return nil, err
	}
	var counts []plugins.Count
	for rows.Next() {
		var c plugins.Count
		if err := rows.Scan(&c.Tenant, &c.Plugin, &c.Day, &c.Metric, &c.Detail, &c.Amount); err != nil {
			rows.Close()
			return nil, err
		}
		counts = append(counts, c)
	}
	return counts, closeRows(rows)
}
