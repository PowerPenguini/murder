package store

import (
	"bytes"
	"crypto/sha256"
	"database/sql"
	"errors"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/PowerPenguini/murder/internal/plugins"
)

// withPlugin opens a store with plugin openai, a limit of 1000 tokens a
// month, and app web listing it, with tenants of the names given.
func withPlugin(t *testing.T, names ...string) *SQLite {
	t.Helper()
	st, _ := openNew(t, options())
	if _, _, err := st.PutPlugin("openai", plugins.Spec{Kind: plugins.KindOpenAI, Limits: []plugins.Limit{{Metric: plugins.MetricTokens, Per: plugins.PerMonth, Amount: 1000}}}, nil, "alice"); err != nil {
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
	plugin, changed, err := st.PutPlugin("openai", plugins.Spec{Kind: plugins.KindOpenAI}, nil, "alice")
	if err != nil || !changed || plugin.Spec.Upstream != "https://api.openai.com" {
		t.Fatalf("install: %+v %v %v", plugin, changed, err)
	}
	version := st.state.Load().version
	if _, changed, err := st.PutPlugin("openai", plugins.Spec{Kind: plugins.KindOpenAI}, nil, "alice"); err != nil || changed || st.state.Load().version != version {
		t.Fatalf("the same again: changed %v, %v", changed, err)
	}
	if _, _, err := st.PutPlugin("openai", plugins.Spec{Kind: plugins.KindHERE}, nil, "alice"); !errors.Is(err, ErrConflict) {
		t.Fatalf("another kind: %v", err)
	}
	for _, bad := range []string{"Open", "-x", strings.Repeat("a", 33)} {
		if _, _, err := st.PutPlugin(bad, plugins.Spec{Kind: plugins.KindOpenAI}, nil, "alice"); !errors.Is(err, ErrInvalid) {
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
	if err := st.SetPluginLimits("openai", "acme", []plugins.Limit{{Metric: plugins.MetricTransactions, Per: plugins.PerDay, Amount: 5}}, true, "alice"); !errors.Is(err, ErrInvalid) {
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
			{Tenant: "acme", Plugin: "openai", Day: day, Metric: plugins.MetricTokens, Detail: "gpt-5", Amount: amount},
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
	if used(plugins.MetricTokens, day) != 700 || used(plugins.MetricTokens, plugins.Month(now)) != 700 {
		t.Fatalf("tokens %d today, %d this month", used(plugins.MetricTokens, day), used(plugins.MetricTokens, plugins.Month(now)))
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
		`DROP TABLE plugin_modules`, `DROP TABLE plugin_reports`, `DROP TABLE plugin_usage`, `DROP TABLE plugin_tenants`, `DROP TABLE plugins`,
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
	if _, _, err := st.PutPlugin("here", plugins.Spec{Kind: plugins.KindHERE}, nil, "alice"); err != nil {
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
	if _, _, err := leader.PutPlugin("openai", plugins.Spec{Kind: plugins.KindOpenAI, Limits: []plugins.Limit{{Metric: plugins.MetricTokens, Per: plugins.PerDay, Amount: 10}}}, nil, "alice"); err != nil {
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
	// A module of the largest size goes through the log and the snapshot.
	module := wasmModule(plugins.MaxModule)
	if _, _, err := leader.PutPlugin("mine", plugins.Spec{Kind: plugins.KindWasm, Module: module.Digest}, module, "alice"); err != nil {
		t.Fatal(err)
	}
	same := func(st *SQLite) {
		t.Helper()
		if wasm, err := st.PluginModule(module.Digest); err != nil || !bytes.Equal(wasm, module.Wasm) {
			t.Fatalf("%s's module: %d bytes, %v", st.Node(), len(wasm), err)
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

// wasmModule is a module of size bytes, with a manifest that counts
// widgets: the store never runs it.
func wasmModule(size int) *PluginModule {
	wasm := bytes.Repeat([]byte{0x2a}, size)
	return &PluginModule{
		Digest:   plugins.ModuleDigest(sha256.Sum256(wasm)),
		Manifest: plugins.Manifest{ABI: plugins.ABI, Name: "widgets", Hosts: []string{"api.example.com"}, Key: plugins.KeyPlace{Header: "X-Key"}, Metrics: []string{"widgets"}},
		Wasm:     wasm,
	}
}

func TestAPluginRunsAModuleOfItsDigestKeptAsLongAsOneRunsIt(t *testing.T) {
	st, _ := openNew(t, options())
	module := wasmModule(1024)
	spec := plugins.Spec{Kind: plugins.KindWasm, Module: module.Digest, Limits: []plugins.Limit{{Metric: "widgets", Per: plugins.PerDay, Amount: 5}}}
	// Without its module, or with another, it is not installed.
	if _, _, err := st.PutPlugin("a", spec, nil, "alice"); !errors.Is(err, ErrInvalid) {
		t.Fatalf("without its module: %v", err)
	}
	other := wasmModule(10)
	if _, _, err := st.PutPlugin("a", spec, other, "alice"); !errors.Is(err, ErrInvalid) {
		t.Fatalf("with another module: %v", err)
	}
	forged := *module
	forged.Wasm = []byte("forged")
	if _, _, err := st.PutPlugin("a", spec, &forged, "alice"); !errors.Is(err, ErrInvalid) {
		t.Fatalf("with a module not of its digest: %v", err)
	}
	// A limit of a metric the module does not count is refused.
	wrong := spec
	wrong.Limits = []plugins.Limit{{Metric: "tokens", Per: plugins.PerDay, Amount: 5}}
	if _, _, err := st.PutPlugin("a", wrong, module, "alice"); !errors.Is(err, ErrInvalid) {
		t.Fatalf("a limit of tokens: %v", err)
	}
	plugin, changed, err := st.PutPlugin("a", spec, module, "alice")
	if err != nil || !changed || plugin.Manifest == nil || plugin.Manifest.Name != "widgets" {
		t.Fatalf("%+v %v %v", plugin, changed, err)
	}
	if metrics := plugin.Metrics(); strings.Join(metrics, " ") != "requests widgets" {
		t.Fatalf("metrics %v", metrics)
	}
	// Another plugin runs the same module without sending it again.
	if _, _, err := st.PutPlugin("b", plugins.Spec{Kind: plugins.KindWasm, Module: module.Digest}, nil, "alice"); err != nil {
		t.Fatal(err)
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
	// A module that does not count what a tenant's limit names is refused.
	other.Manifest.Metrics = []string{"gadgets"}
	if _, _, err := st.PutPlugin("a", plugins.Spec{Kind: plugins.KindWasm, Module: other.Digest}, other, "alice"); !errors.Is(err, ErrConflict) {
		t.Fatalf("a module without the tenant's metric: %v", err)
	}
	if err := st.SetPluginLimits("a", "acme", nil, false, "alice"); err != nil {
		t.Fatal(err)
	}
	// Plugin a moves to the other module: b still runs the first.
	if _, _, err := st.PutPlugin("a", plugins.Spec{Kind: plugins.KindWasm, Module: other.Digest}, other, "alice"); err != nil {
		t.Fatal(err)
	}
	if _, err := st.PluginModule(module.Digest); err != nil {
		t.Fatalf("the module b runs went: %v", err)
	}
	if err := st.DeletePlugin("b", "alice"); err != nil {
		t.Fatal(err)
	}
	if _, err := st.PluginModule(module.Digest); !errors.Is(err, ErrNotFound) {
		t.Fatalf("a module no plugin runs is kept: %v", err)
	}
	if wasm, err := st.PluginModule(other.Digest); err != nil || !bytes.Equal(wasm, other.Wasm) {
		t.Fatalf("a's module: %v", err)
	}
}

// What a module counts is kept, totalled and held to limits as any other
// metric is.
func TestAModulesMetricsAreCountedAndHeld(t *testing.T) {
	st, _ := openNew(t, options())
	module := wasmModule(64)
	module.Manifest.Metrics = []string{"tokens.cache_read"}
	if _, _, err := st.PutPlugin("m", plugins.Spec{Kind: plugins.KindWasm, Module: module.Digest, Limits: []plugins.Limit{{Metric: "tokens.cache_read", Per: plugins.PerDay, Amount: 10, Mode: plugins.ModeHard}}}, module, "alice"); err != nil {
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
