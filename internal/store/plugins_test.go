package store

import (
	"database/sql"
	"encoding/json"
	"errors"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/PowerPenguini/murder/internal/plugins"
)

// openAISpec is a plugin of Murder's openai, its image pinned, with
// limits.
func openAISpec(limits ...plugins.Limit) plugins.Spec {
	return plugins.Spec{Kind: plugins.KindContainer, Image: "ghcr.io/powerpenguini/murder-openai@sha256:" + strings.Repeat("0", 64), Limits: limits}
}

// openAIManifest is the manifest of Murder's openai, in short: the store
// never runs it.
func openAIManifest() *plugins.Manifest {
	return &plugins.Manifest{ABI: plugins.ABI, Name: "openai", Hosts: []string{"api.openai.com"}, Key: plugins.KeyPlace{Header: "Authorization", Prefix: "Bearer "}, Metrics: []string{"tokens", "tokens.input", "tokens.output"}}
}

// withPlugin opens a store with plugin openai, a limit of 1000 tokens a
// month, and app web listing it, with tenants of the names given.
func withPlugin(t *testing.T, names ...string) *SQLite {
	t.Helper()
	st, _ := openNew(t, options())
	if _, _, err := st.PutPlugin("openai", openAISpec(plugins.Limit{Metric: "tokens", Per: plugins.PerMonth, Amount: 1000}), openAIManifest(), "alice"); err != nil {
		t.Fatal(err)
	}
	app := appSpec()
	app.Plugins = []string{"openai"}
	if _, err := st.PutApp(App{Name: "web", Spec: app}, "test"); err != nil {
		t.Fatal(err)
	}
	for _, name := range names {
		if _, err := st.Create(NewTenant{Name: name, App: "web"}, "test"); err != nil {
			t.Fatal(err)
		}
	}
	return st
}

func TestAPluginIsInstalledChangedAndDeletedOnceNoAppListsIt(t *testing.T) {
	st, _ := openNew(t, options())
	app := appSpec()
	app.Plugins = []string{"openai"}
	if _, err := st.PutApp(App{Name: "web", Spec: app}, "test"); !errors.Is(err, ErrInvalid) || !strings.Contains(err.Error(), "not installed") {
		t.Fatalf("an app listing a plugin not installed: %v", err)
	}
	plugin, changed, err := st.PutPlugin("openai", openAISpec(), openAIManifest(), "alice")
	if err != nil || !changed || plugin.Manifest.Name != "openai" {
		t.Fatalf("install: %+v %v %v", plugin, changed, err)
	}
	version := st.state.Load().version
	if _, changed, err := st.PutPlugin("openai", openAISpec(), openAIManifest(), "alice"); err != nil || changed || st.state.Load().version != version {
		t.Fatalf("the same again: changed %v, %v", changed, err)
	}
	if _, _, err := st.PutPlugin("openai", plugins.Spec{Kind: "here"}, openAIManifest(), "alice"); !errors.Is(err, ErrInvalid) || !strings.Contains(err.Error(), "murder-here") {
		t.Fatalf("a kind of Murder's own, as before: %v", err)
	}
	for _, bad := range []string{"Open", "-x", strings.Repeat("a", 33)} {
		if _, _, err := st.PutPlugin(bad, openAISpec(), openAIManifest(), "alice"); !errors.Is(err, ErrInvalid) {
			t.Errorf("name %q: %v", bad, err)
		}
	}
	if _, err := st.PutApp(App{Name: "web", Spec: app}, "test"); err != nil {
		t.Fatal(err)
	}
	if err := st.DeletePlugin("openai", "alice"); !errors.Is(err, ErrConflict) || !strings.Contains(err.Error(), "app web") {
		t.Fatalf("delete while web lists it: %v", err)
	}
	if _, err := st.PutApp(App{Name: "web", Spec: appSpec()}, "test"); err != nil {
		t.Fatal(err)
	}
	if err := st.DeletePlugin("openai", "alice"); err != nil {
		t.Fatal(err)
	}
	if len(st.Plugins()) != 0 {
		t.Fatal("the plugin stayed")
	}
}

func TestATenantsOwnKeyLiftsThePluginsLimitsButNotItsOwn(t *testing.T) {
	st := withPlugin(t, "acme")
	plugin, _ := st.Plugin("openai")
	own := func() PluginTenant {
		list := st.PluginTenants("acme", "openai")
		if len(list) == 0 {
			return PluginTenant{}
		}
		return list[0]
	}
	if limits := PluginLimits(plugin, own()); len(limits) != 1 || limits[0].Amount != 1000 {
		t.Fatalf("the plugin's limits: %v", limits)
	}
	if err := st.SetPluginKey("openai", "acme", "sealed", "alice"); err != nil {
		t.Fatal(err)
	}
	if limits := PluginLimits(plugin, own()); len(limits) != 0 {
		t.Fatalf("limits with a key of its own: %v", limits)
	}
	if err := st.SetPluginLimits("openai", "acme", []plugins.Limit{{Metric: plugins.MetricRequests, Per: plugins.PerDay, Amount: 5}}, true, "alice"); err != nil {
		t.Fatal(err)
	}
	if limits := PluginLimits(plugin, own()); len(limits) != 1 || limits[0].Metric != plugins.MetricRequests || limits[0].Mode != plugins.ModeHard {
		t.Fatalf("its own limits: %v", limits)
	}
	if err := st.SetPluginLimits("openai", "acme", []plugins.Limit{{Metric: "transactions", Per: plugins.PerDay, Amount: 5}}, true, "alice"); !errors.Is(err, ErrInvalid) {
		t.Fatalf("a metric openai does not count: %v", err)
	}
	// Taking key and limits away drops the row.
	if err := st.SetPluginKey("openai", "acme", "", "alice"); err != nil {
		t.Fatal(err)
	}
	if err := st.SetPluginLimits("openai", "acme", nil, false, "alice"); err != nil {
		t.Fatal(err)
	}
	if len(st.PluginTenants("", "")) != 0 {
		t.Fatalf("settings left: %v", st.PluginTenants("", ""))
	}
	// No limits at all is a setting of its own.
	if err := st.SetPluginLimits("openai", "acme", nil, true, "alice"); err != nil {
		t.Fatal(err)
	}
	if limits := PluginLimits(plugin, own()); len(limits) != 0 || !own().HasLimits {
		t.Fatalf("no limits: %v %+v", limits, own())
	}
}

func TestAReportIsCountedOnceAndSaysTheLimitsItCrosses(t *testing.T) {
	st := withPlugin(t, "acme")
	now := time.Now().UTC()
	day := plugins.Day(now)
	report := func(seq, amount int64) plugins.Report {
		return plugins.Report{Edge: "edge-1", Seq: seq, Counts: []plugins.Count{
			{Tenant: "acme", Plugin: "openai", Day: day, Metric: "tokens", Detail: "gpt-5", Amount: amount},
			{Tenant: "acme", Plugin: "openai", Day: day, Metric: plugins.MetricRequests, Amount: 1},
			{Tenant: "gone", Plugin: "openai", Day: day, Metric: plugins.MetricRequests, Amount: 1},
		}}
	}
	if counted, err := st.AddPluginUsage("node-1", report(1, 700)); err != nil || !counted {
		t.Fatalf("first: %v %v", counted, err)
	}
	// Sent again, as after an answer lost: not counted again.
	if counted, err := st.AddPluginUsage("node-1", report(1, 700)); err != nil || counted {
		t.Fatalf("again: %v %v", counted, err)
	}
	if st.PluginReported("node-1", "edge-1") != 1 {
		t.Fatal("the report is not recorded")
	}
	used := func(metric, period string) int64 {
		totals, err := st.PluginTotals([]string{"acme", "gone"}, now)
		if err != nil {
			t.Fatal(err)
		}
		for _, total := range totals {
			if total.Tenant == "gone" {
				t.Fatal("a tenant not in the state was counted")
			}
			if total.Metric == metric && total.Period == period && total.Plugin == "openai" {
				return total.Amount
			}
		}
		return 0
	}
	if used("tokens", day) != 700 || used("tokens", plugins.Month(now)) != 700 {
		t.Fatalf("tokens %d today, %d this month", used("tokens", day), used("tokens", plugins.Month(now)))
	}
	events, _ := st.Events(EventQuery{Tenant: "acme", Limit: 5})
	if len(events) > 0 && strings.HasPrefix(events[0].Action, "plugin.limit") {
		t.Fatalf("700 of 1000 is no news: %+v", events[0])
	}
	// 850: past four fifths. 1100: past the limit.
	if _, err := st.AddPluginUsage("node-1", report(2, 150)); err != nil {
		t.Fatal(err)
	}
	events, _ = st.Events(EventQuery{Tenant: "acme", Limit: 1})
	if events[0].Action != "plugin.limit-near" || !strings.Contains(events[0].Detail, "850 of tokens=1000/month:hard") {
		t.Fatalf("near: %+v", events[0])
	}
	if _, err := st.AddPluginUsage("node-1", report(3, 250)); err != nil {
		t.Fatal(err)
	}
	events, _ = st.Events(EventQuery{Tenant: "acme", Limit: 1})
	if events[0].Action != "plugin.limit-reached" || !strings.Contains(events[0].Detail, "1100 of") {
		t.Fatalf("reached: %+v", events[0])
	}
	// While an upgrade has changes frozen, reports are still counted:
	// the edge it replaces tells its last.
	if err := st.Freeze("upgrade", "root"); err != nil {
		t.Fatal(err)
	}
	if counted, err := st.AddPluginUsage("node-1", plugins.Report{Edge: "edge-0", Seq: 1, Counts: []plugins.Count{{Tenant: "acme", Plugin: "openai", Day: day, Metric: plugins.MetricRequests, Amount: 1}}}); err != nil || !counted {
		t.Fatalf("frozen: %v %v", counted, err)
	}
	if err := st.SetPluginKey("openai", "", "x", "alice"); !errors.Is(err, ErrFrozen) {
		t.Fatalf("a change of a plugin while frozen: %v", err)
	}
	if err := st.Thaw("root"); err != nil {
		t.Fatal(err)
	}
	// Another edge process of the node numbers its own reports.
	if counted, err := st.AddPluginUsage("node-1", plugins.Report{Edge: "edge-2", Seq: 1, Counts: report(1, 1).Counts}); err != nil || !counted {
		t.Fatalf("another edge: %v %v", counted, err)
	}
	counts, err := st.PluginUsage(UsageQuery{Tenant: "acme", From: day, To: day})
	if err != nil {
		t.Fatal(err)
	}
	if len(counts) != 2 || counts[0].Metric != plugins.MetricRequests || counts[0].Amount != 5 || counts[1].Detail != "gpt-5" || counts[1].Amount != 1101 {
		t.Fatalf("counts %+v", counts)
	}
	for _, bad := range []plugins.Report{{Edge: "", Seq: 1}, {Edge: "e", Seq: 0}} {
		if _, err := st.AddPluginUsage("node-1", bad); !errors.Is(err, ErrInvalid) {
			t.Errorf("%+v: %v", bad, err)
		}
	}
	// Counts not of a form kept are left out; the rest of the report is
	// counted, and it is not refused, to be sent again for ever.
	mixed := plugins.Report{Edge: "e", Seq: 1, Counts: []plugins.Count{
		{Tenant: "acme", Plugin: "openai", Day: "2026-1-1", Metric: "tokens", Amount: 1},
		{Tenant: "acme", Plugin: "openai", Day: day, Metric: "tokens", Amount: -1},
		{Tenant: "acme", Plugin: "openai", Day: day, Metric: "tokens", Detail: strings.Repeat("m", 200), Amount: 1},
		{Tenant: "acme", Plugin: "openai", Day: day, Metric: plugins.MetricRequests, Amount: 1},
	}}
	if counted, err := st.AddPluginUsage("node-1", mixed); err != nil || !counted {
		t.Fatalf("a report with counts left out: %v %v", counted, err)
	}
	if counts, _ := st.PluginUsage(UsageQuery{Tenant: "acme", From: day, To: day}); counts[0].Amount != 6 || len(counts) != 2 {
		t.Fatalf("counts %+v", counts)
	}
}

func TestUsageOlderThanItIsKeptIsPruned(t *testing.T) {
	st := withPlugin(t, "acme")
	now := time.Now().UTC()
	old := plugins.Day(now.Add(-UsageKept - 48*time.Hour))
	count := func(day string) []plugins.Count {
		return []plugins.Count{{Tenant: "acme", Plugin: "openai", Day: day, Metric: plugins.MetricRequests, Amount: 1}}
	}
	// A day long past, as an edge whose clock was wrong would say.
	st.usagePruned.Store(plugins.Day(now))
	if _, err := st.AddPluginUsage("node-1", plugins.Report{Edge: "e", Seq: 1, Counts: count(old)}); err != nil {
		t.Fatal(err)
	}
	st.usagePruned.Store("")
	if _, err := st.AddPluginUsage("node-1", plugins.Report{Edge: "e", Seq: 2, Counts: count(plugins.Day(now))}); err != nil {
		t.Fatal(err)
	}
	counts, _ := st.PluginUsage(UsageQuery{From: "2000-01-01", To: plugins.Day(now)})
	if len(counts) != 1 || counts[0].Day != plugins.Day(now) {
		t.Fatalf("counts %+v", counts)
	}
}

func TestAnEdgesLastReportIsForgottenAfterTwoDays(t *testing.T) {
	st := withPlugin(t, "acme")
	clock := time.Now().UTC()
	st.now = func() time.Time { return clock }
	count := []plugins.Count{{Tenant: "acme", Plugin: "openai", Day: plugins.Day(clock), Metric: plugins.MetricRequests, Amount: 1}}
	if _, err := st.AddPluginUsage("node-1", plugins.Report{Edge: "old", Seq: 4, Counts: count}); err != nil {
		t.Fatal(err)
	}
	clock = clock.Add(reportsKept + time.Minute)
	if _, err := st.AddPluginUsage("node-1", plugins.Report{Edge: "new", Seq: 1, Counts: count}); err != nil {
		t.Fatal(err)
	}
	if st.PluginReported("node-1", "old") != 0 || st.PluginReported("node-1", "new") != 1 {
		t.Fatalf("old %d, new %d", st.PluginReported("node-1", "old"), st.PluginReported("node-1", "new"))
	}
}

func TestAPurgedTenantTakesWhatItUsedAndItsSettingsAlong(t *testing.T) {
	st := withPlugin(t, "acme")
	if err := st.SetPluginKey("openai", "acme", "sealed", "alice"); err != nil {
		t.Fatal(err)
	}
	day := plugins.Day(time.Now())
	if _, err := st.AddPluginUsage("node-1", plugins.Report{Edge: "e", Seq: 1, Counts: []plugins.Count{{Tenant: "acme", Plugin: "openai", Day: day, Metric: plugins.MetricRequests, Amount: 3}}}); err != nil {
		t.Fatal(err)
	}
	if err := st.Archive("acme", "alice"); err != nil {
		t.Fatal(err)
	}
	if counts, _ := st.PluginUsage(UsageQuery{From: day, To: day}); len(counts) != 1 || len(st.PluginTenants("acme", "")) != 1 {
		t.Fatal("archiving dropped what it used or its settings")
	}
	if err := st.Forget("acme", "alice"); err != nil {
		t.Fatal(err)
	}
	if counts, _ := st.PluginUsage(UsageQuery{From: day, To: day}); len(counts) != 0 || len(st.PluginTenants("", "")) != 0 {
		t.Fatalf("after the purge: %v, %v", counts, st.PluginTenants("", ""))
	}
}

func TestPluginKeysAreSealedAgainWithTheSecrets(t *testing.T) {
	st := withPlugin(t, "acme")
	if err := st.SetPluginKey("openai", "", "old:operator", "alice"); err != nil {
		t.Fatal(err)
	}
	if err := st.SetPluginKey("openai", "acme", "old:acme", "alice"); err != nil {
		t.Fatal(err)
	}
	var contexts []string
	_, err := st.ResealSecrets(func(context, sealed string) (string, bool, error) {
		if !strings.HasPrefix(sealed, "old:") {
			return sealed, false, nil
		}
		contexts = append(contexts, context)
		return "new:" + strings.TrimPrefix(sealed, "old:"), true, nil
	}, "root")
	if err != nil {
		t.Fatal(err)
	}
	plugin, _ := st.Plugin("openai")
	if plugin.Key != "new:operator" || st.PluginTenants("acme", "openai")[0].Key != "new:acme" {
		t.Fatalf("keys %q, %q", plugin.Key, st.PluginTenants("acme", "openai")[0].Key)
	}
	if strings.Join(contexts, ",") != plugins.KeyContext("openai")+","+plugins.TenantKeyContext("openai", "acme") {
		t.Fatalf("sealed again under %v", contexts)
	}
}

// A database of schema 20 gets the plugin tables, and takes the batches
// of the release before.
func TestOpenUpgradesSchemaVersion20(t *testing.T) {
	path := filepath.Join(t.TempDir(), "murder.db")
	db, err := sql.Open("sqlite", initDSN(path))
	if err != nil {
		t.Fatal(err)
	}
	for _, statement := range []string{
		schema,
		`DROP TABLE plugin_reports`, `DROP TABLE plugin_usage`, `DROP TABLE plugin_tenants`, `DROP TABLE plugins`,
		`INSERT INTO meta (key, value) VALUES ('node', 'n1'), ('version', '3'), ('applied', '0')`,
		`INSERT INTO apps (name, spec, created_at, updated_at, revision) VALUES ('web', '` + marshal(normalizeApp(appSpec())) + `', '2026-10-01T00:00:00Z', '2026-10-01T00:00:00Z', 1)`,
		`PRAGMA user_version = 20`,
	} {
		if _, err := db.Exec(statement); err != nil {
			t.Fatal(err)
		}
	}
	if err := db.Close(); err != nil {
		t.Fatal(err)
	}
	st, err := Open(path, options())
	if err != nil {
		t.Fatal(err)
	}
	defer st.Close()
	if len(st.Plugins()) != 0 {
		t.Fatal("plugins from nowhere")
	}
	if _, _, err := st.PutPlugin("mine", containerSpec(), testManifest(), "alice"); err != nil {
		t.Fatal(err)
	}
	recorded := &recorder{}
	if err := event(recorded, time.Now(), "murder", "note", "", "web", "old release"); err != nil {
		t.Fatal(err)
	}
	if err := st.Apply(Batch{Schema: 20, Version: st.state.Load().version, Statements: recorded.statements}); err != nil {
		t.Fatal(err)
	}
}

// A database of schema 21, which kept WebAssembly modules, keeps plugins
// of kind container instead: a plugin of kind wasm goes, the others stay.
func TestOpenUpgradesSchemaVersion21(t *testing.T) {
	path := filepath.Join(t.TempDir(), "murder.db")
	db, err := sql.Open("sqlite", initDSN(path))
	if err != nil {
		t.Fatal(err)
	}
	for _, statement := range []string{
		schema,
		`ALTER TABLE plugins DROP COLUMN manifest`,
		`CREATE TABLE plugin_modules (digest TEXT PRIMARY KEY, manifest TEXT NOT NULL, wasm TEXT NOT NULL, size INTEGER NOT NULL, created_at TEXT NOT NULL) STRICT`,
		`INSERT INTO meta (key, value) VALUES ('node', 'n1'), ('version', '3'), ('applied', '0')`,
		`INSERT INTO plugins (name, spec, created_at, updated_at) VALUES ('openai', '{"kind":"openai","upstream":"https://api.openai.com"}', '2026-11-01T00:00:00Z', '2026-11-01T00:00:00Z')`,
		`INSERT INTO plugins (name, spec, created_at, updated_at) VALUES ('mine', '{"kind":"wasm","module":"sha256:` + strings.Repeat("a", 64) + `"}', '2026-11-01T00:00:00Z', '2026-11-01T00:00:00Z')`,
		`INSERT INTO plugin_modules VALUES ('sha256:` + strings.Repeat("a", 64) + `', '{}', 'AAAA', 3, '2026-11-01T00:00:00Z')`,
		`PRAGMA user_version = 21`,
	} {
		if _, err := db.Exec(statement); err != nil {
			t.Fatal(statement, err)
		}
	}
	if err := db.Close(); err != nil {
		t.Fatal(err)
	}
	st, err := Open(path, options())
	if err != nil {
		t.Fatal(err)
	}
	defer st.Close()
	// Neither the module's plugin nor openai, a kind of Murder's own then,
	// stays (TestOpenUpgradesSchemaVersion22).
	if list := st.Plugins(); len(list) != 0 {
		t.Fatalf("plugins %+v", list)
	}
	if _, _, err := st.PutPlugin("mine", containerSpec(), testManifest(), "alice"); err != nil {
		t.Fatal(err)
	}
	if plugin, _ := st.Plugin("mine"); plugin.Manifest == nil || plugin.Manifest.Name != "widgets" {
		t.Fatalf("mine %+v", plugin)
	}
}

// A database of schema 22, which kept plugins of kind openai and here as
// kinds of Murder's own, keeps only those of kind container, and tenants'
// own settings of them.
func TestOpenUpgradesSchemaVersion22(t *testing.T) {
	path := filepath.Join(t.TempDir(), "murder.db")
	db, err := sql.Open("sqlite", initDSN(path))
	if err != nil {
		t.Fatal(err)
	}
	manifest, _ := json.Marshal(testManifest())
	spec, _ := json.Marshal(containerSpec())
	for _, statement := range []string{
		schema,
		`INSERT INTO meta (key, value) VALUES ('node', 'n1'), ('version', '3'), ('applied', '0')`,
		// Tenant acme is of no use but to its settings.
		`PRAGMA foreign_keys = OFF`,
		`INSERT INTO plugins (name, spec, created_at, updated_at) VALUES ('openai', '{"kind":"openai","upstream":"https://api.openai.com"}', '2026-11-01T00:00:00Z', '2026-11-01T00:00:00Z')`,
		`INSERT INTO plugins (name, spec, created_at, updated_at) VALUES ('here', '{"kind":"here","hosts":["router.hereapi.com"]}', '2026-11-01T00:00:00Z', '2026-11-01T00:00:00Z')`,
		`INSERT INTO plugins (name, spec, created_at, updated_at, manifest) VALUES ('mine', '` + string(spec) + `', '2026-11-01T00:00:00Z', '2026-11-01T00:00:00Z', '` + string(manifest) + `')`,
		`INSERT INTO plugin_tenants (tenant, plugin, key, limits, updated_at) VALUES ('acme', 'openai', 'sealed', NULL, '2026-11-01T00:00:00Z'), ('acme', 'mine', 'sealed', NULL, '2026-11-01T00:00:00Z')`,
		`PRAGMA user_version = 22`,
	} {
		if _, err := db.Exec(statement); err != nil {
			t.Fatal(statement, err)
		}
	}
	if err := db.Close(); err != nil {
		t.Fatal(err)
	}
	st, err := Open(path, options())
	if err != nil {
		t.Fatal(err)
	}
	defer st.Close()
	if list := st.Plugins(); len(list) != 1 || list[0].Name != "mine" || list[0].Manifest == nil {
		t.Fatalf("plugins %+v", list)
	}
	if settings := st.PluginTenants("acme", ""); len(settings) != 1 || settings[0].Plugin != "mine" {
		t.Fatalf("tenants' settings %+v", settings)
	}
}

// An app that lists a plugin of kind openai keeps it once the plugin went
// in the upgrade: the store opens, and the plugin is applied again from
// its image under its name.
func TestAnAppListingAPluginTheUpgradeTookStaysUntilItIsAppliedAgain(t *testing.T) {
	path := filepath.Join(t.TempDir(), "murder.db")
	if err := Init(path, "n1"); err != nil {
		t.Fatal(err)
	}
	st, err := Open(path, options())
	if err != nil {
		t.Fatal(err)
	}
	if _, _, err := st.PutPlugin("openai", openAISpec(), openAIManifest(), "alice"); err != nil {
		t.Fatal(err)
	}
	app := appSpec()
	app.Plugins = []string{"openai"}
	if _, err := st.PutApp(App{Name: "web", Spec: app}, "test"); err != nil {
		t.Fatal(err)
	}
	if err := st.Close(); err != nil {
		t.Fatal(err)
	}
	db, err := sql.Open("sqlite", initDSN(path))
	if err != nil {
		t.Fatal(err)
	}
	for _, statement := range []string{
		`UPDATE plugins SET spec = '{"kind":"openai","upstream":"https://api.openai.com"}', manifest = NULL WHERE name = 'openai'`,
		`PRAGMA user_version = 22`,
	} {
		if _, err := db.Exec(statement); err != nil {
			t.Fatal(statement, err)
		}
	}
	if err := db.Close(); err != nil {
		t.Fatal(err)
	}
	st, err = Open(path, options())
	if err != nil {
		t.Fatalf("open with an app listing a plugin gone: %v", err)
	}
	defer st.Close()
	if len(st.Plugins()) != 0 {
		t.Fatalf("plugins %+v", st.Plugins())
	}
	if _, _, err := st.PutPlugin("openai", openAISpec(), openAIManifest(), "alice"); err != nil {
		t.Fatal(err)
	}
	if _, err := st.Create(NewTenant{Name: "acme", App: "web"}, "test"); err != nil {
		t.Fatal(err)
	}
}

// Plugin writes are batches every node applies alike: checked as the
// leader checks them, applied on a second node from the log, and carried
// by a snapshot to a third.
func TestPluginWritesReplicate(t *testing.T) {
	leader, follower := openNamed(t, "node-1"), openNamed(t, "node-2")
	var entries []Batch
	leader.Replicate(func(batch Batch) (uint64, error) {
		if err := batch.Check(); err != nil {
			return 0, err
		}
		entries = append(entries, batch)
		index := uint64(len(entries))
		if err := leader.ApplyEntry(index, batch); err != nil {
			return 0, err
		}
		return index, follower.ApplyEntry(index, batch)
	}, func() error { return nil })
	if err := leader.PutNode(Node{Name: "node-1", Address: "10.0.0.1:7400"}, "alice"); err != nil {
		t.Fatal(err)
	}
	if _, _, err := leader.PutPlugin("openai", openAISpec(plugins.Limit{Metric: "tokens", Per: plugins.PerDay, Amount: 10}), openAIManifest(), "alice"); err != nil {
		t.Fatal(err)
	}
	app := appSpec()
	app.Plugins = []string{"openai"}
	if _, err := leader.PutApp(App{Name: "web", Spec: app}, "alice"); err != nil {
		t.Fatal(err)
	}
	if _, err := leader.Create(NewTenant{Name: "acme", App: "web"}, "alice"); err != nil {
		t.Fatal(err)
	}
	if err := leader.SetPluginKey("openai", "", "sealed", "alice"); err != nil {
		t.Fatal(err)
	}
	if err := leader.SetPluginKey("openai", "acme", "own", "alice"); err != nil {
		t.Fatal(err)
	}
	if err := leader.SetPluginLimits("openai", "acme", []plugins.Limit{{Metric: plugins.MetricRequests, Per: plugins.PerMonth, Amount: 1}}, true, "alice"); err != nil {
		t.Fatal(err)
	}
	day := plugins.Day(time.Now())
	for seq := int64(1); seq <= 2; seq++ {
		if _, err := leader.AddPluginUsage("node-1", plugins.Report{Edge: "e", Seq: seq, Counts: []plugins.Count{{Tenant: "acme", Plugin: "openai", Day: day, Metric: plugins.MetricRequests, Amount: 1}}}); err != nil {
			t.Fatal(err)
		}
	}
	// A plugin of kind container goes with its manifest.
	if _, _, err := leader.PutPlugin("mine", containerSpec(), testManifest(), "alice"); err != nil {
		t.Fatal(err)
	}
	same := func(st *SQLite) {
		t.Helper()
		if mine, err := st.Plugin("mine"); err != nil || mine.Manifest == nil || mine.Manifest.Name != "widgets" {
			t.Fatalf("%s's plugin of kind container: %+v, %v", st.Node(), mine, err)
		}
		if !reflect.DeepEqual(st.Plugins(), leader.Plugins()) || !reflect.DeepEqual(st.PluginTenants("", ""), leader.PluginTenants("", "")) || st.PluginReported("node-1", "e") != 2 {
			t.Fatalf("%s differs: %+v %+v", st.Node(), st.Plugins(), st.PluginTenants("", ""))
		}
		counts, _ := st.PluginUsage(UsageQuery{From: day, To: day})
		want, _ := leader.PluginUsage(UsageQuery{From: day, To: day})
		if !reflect.DeepEqual(counts, want) || len(counts) != 1 || counts[0].Amount != 2 {
			t.Fatalf("%s's usage %+v, the leader's %+v", st.Node(), counts, want)
		}
		events, _ := st.Events(EventQuery{Tenant: "acme", Limit: 1})
		if events[0].Action != "plugin.limit-reached" {
			t.Fatalf("%s's last event %+v", st.Node(), events[0])
		}
	}
	same(follower)
	snapshot, err := leader.SnapshotFile()
	if err != nil {
		t.Fatal(err)
	}
	defer removeDatabase(snapshot)
	third := openNamed(t, "node-3")
	file, err := os.Open(snapshot)
	if err != nil {
		t.Fatal(err)
	}
	defer file.Close()
	if installed, err := third.InstallSnapshot(file); err != nil || !installed {
		t.Fatalf("install: %v %v", installed, err)
	}
	same(third)
}

// testManifest is the manifest of an image that counts widgets: the
// store never runs it.
func testManifest() *plugins.Manifest {
	return &plugins.Manifest{ABI: plugins.ABI, Name: "widgets", Hosts: []string{"api.example.com"}, Key: plugins.KeyPlace{Header: "X-Key"}, Metrics: []string{"widgets"}}
}

func containerSpec() plugins.Spec {
	return plugins.Spec{Kind: plugins.KindContainer, Image: "ghcr.io/acme/widgets@sha256:" + strings.Repeat("1", 64)}
}

func TestAPluginOfKindContainerKeepsItsImagesManifest(t *testing.T) {
	st, _ := openNew(t, options())
	spec := containerSpec()
	spec.Limits = []plugins.Limit{{Metric: "widgets", Per: plugins.PerDay, Amount: 5}}
	// Without its manifest, or its image pinned, it is not installed.
	if _, _, err := st.PutPlugin("a", spec, nil, "alice"); !errors.Is(err, ErrInvalid) {
		t.Fatalf("without its manifest: %v", err)
	}
	tagged := spec
	tagged.Image = "ghcr.io/acme/widgets:1"
	if _, _, err := st.PutPlugin("a", tagged, testManifest(), "alice"); !errors.Is(err, ErrInvalid) {
		t.Fatalf("a tag: %v", err)
	}
	// A limit of a metric the manifest does not count is refused.
	wrong := spec
	wrong.Limits = []plugins.Limit{{Metric: "tokens", Per: plugins.PerDay, Amount: 5}}
	if _, _, err := st.PutPlugin("a", wrong, testManifest(), "alice"); !errors.Is(err, ErrInvalid) {
		t.Fatalf("a limit of tokens: %v", err)
	}
	plugin, changed, err := st.PutPlugin("a", spec, testManifest(), "alice")
	if err != nil || !changed || plugin.Manifest == nil || plugin.Manifest.Name != "widgets" {
		t.Fatalf("%+v %v %v", plugin, changed, err)
	}
	if metrics := plugin.Metrics(); strings.Join(metrics, " ") != "requests widgets" {
		t.Fatalf("metrics %v", metrics)
	}
	if _, changed, err := st.PutPlugin("a", spec, testManifest(), "alice"); err != nil || changed {
		t.Fatalf("the same again: changed %v, %v", changed, err)
	}
	app := appSpec()
	app.Plugins = []string{"a"}
	if _, err := st.PutApp(App{Name: "web", Spec: app}, "alice"); err != nil {
		t.Fatal(err)
	}
	if _, err := st.Create(NewTenant{Name: "acme", App: "web"}, "alice"); err != nil {
		t.Fatal(err)
	}
	if err := st.SetPluginLimits("a", "acme", []plugins.Limit{{Metric: "tokens", Per: plugins.PerDay, Amount: 1}}, true, "alice"); !errors.Is(err, ErrInvalid) {
		t.Fatalf("a tenant's limit of tokens: %v", err)
	}
	if err := st.SetPluginLimits("a", "acme", []plugins.Limit{{Metric: "widgets", Per: plugins.PerDay, Amount: 1}}, true, "alice"); err != nil {
		t.Fatal(err)
	}
	// An image that does not count what a tenant's limit names is refused.
	other := containerSpec()
	other.Image = "ghcr.io/acme/widgets@sha256:" + strings.Repeat("2", 64)
	gadgets := testManifest()
	gadgets.Metrics = []string{"gadgets"}
	if _, _, err := st.PutPlugin("a", other, gadgets, "alice"); !errors.Is(err, ErrConflict) {
		t.Fatalf("an image without the tenant's metric: %v", err)
	}
	if err := st.SetPluginLimits("a", "acme", nil, false, "alice"); err != nil {
		t.Fatal(err)
	}
	if plugin, changed, err := st.PutPlugin("a", other, gadgets, "alice"); err != nil || !changed || plugin.Manifest.Metrics[0] != "gadgets" || plugin.Spec.Image != other.Image {
		t.Fatalf("another image: %+v %v %v", plugin, changed, err)
	}
}

// What a container counts is kept, totalled and held to limits as any other
// metric is.
func TestAContainersMetricsAreCountedAndHeld(t *testing.T) {
	st, _ := openNew(t, options())
	manifest := testManifest()
	manifest.Metrics = []string{"tokens.cache_read"}
	spec := containerSpec()
	spec.Limits = []plugins.Limit{{Metric: "tokens.cache_read", Per: plugins.PerDay, Amount: 10, Mode: plugins.ModeHard}}
	if _, _, err := st.PutPlugin("m", spec, manifest, "alice"); err != nil {
		t.Fatal(err)
	}
	app := appSpec()
	app.Plugins = []string{"m"}
	if _, err := st.PutApp(App{Name: "web", Spec: app}, "alice"); err != nil {
		t.Fatal(err)
	}
	if _, err := st.Create(NewTenant{Name: "acme", App: "web"}, "alice"); err != nil {
		t.Fatal(err)
	}
	now := time.Now()
	report := plugins.Report{Edge: "e", Seq: 1, Counts: []plugins.Count{{Tenant: "acme", Plugin: "m", Day: plugins.Day(now), Metric: "tokens.cache_read", Detail: "claude-opus-5-5", Amount: 12}}}
	if counted, err := st.AddPluginUsage(st.Node(), report); err != nil || !counted {
		t.Fatalf("%v %v", counted, err)
	}
	totals, err := st.PluginTotals([]string{"acme"}, now)
	if err != nil || len(totals) != 2 || totals[0].Metric != "tokens.cache_read" || totals[0].Amount != 12 {
		t.Fatalf("totals %+v %v", totals, err)
	}
	events, _ := st.Events(EventQuery{Tenant: "acme", Limit: 1})
	if len(events) == 0 || events[0].Action != "plugin.limit-reached" {
		t.Fatalf("events %+v", events)
	}
}

// A plugin read is a copy: what is done to its manifest is not done to the
// state.
func TestAPluginReadIsACopyOfItsManifest(t *testing.T) {
	st, _ := openNew(t, options())
	manifest := openAIManifest()
	manifest.RequestBodyTypes = []string{"application/json"}
	manifest.UI = &plugins.UI{Title: "OpenAI", Metrics: []plugins.MetricUI{{Name: "tokens"}}, Config: []plugins.ConfigUI{{Name: "allow"}}}
	if _, _, err := st.PutPlugin("openai", openAISpec(), manifest, "alice"); err != nil {
		t.Fatal(err)
	}
	read, _ := st.Plugin("openai")
	read.Manifest.RequestBodyTypes[0] = "text/plain"
	read.Manifest.UI.Title = "changed"
	read.Manifest.UI.Metrics[0].Name = "changed"
	read.Manifest.UI.Config[0].Name = "changed"
	again, _ := st.Plugin("openai")
	if again.Manifest.RequestBodyTypes[0] != "application/json" || again.Manifest.UI.Title != "OpenAI" || again.Manifest.UI.Metrics[0].Name != "tokens" || again.Manifest.UI.Config[0].Name != "allow" {
		t.Fatalf("the state changed: %+v %+v", again.Manifest.RequestBodyTypes, *again.Manifest.UI)
	}
}
