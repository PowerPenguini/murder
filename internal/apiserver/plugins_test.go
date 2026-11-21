package apiserver

import (
	"context"
	"os/exec"
	"path/filepath"
	"runtime"
	"testing"
	"time"

	"github.com/PowerPenguini/murder/internal/plugins"
)

// A plugin is installed, given a key sealed for it, listed by an app, and
// the edge's routes then let the node's tenant reach it; what the edge
// reports is counted once and comes back as the tenant's totals.
func TestPluginsThroughTheAPI(t *testing.T) {
	f := newAPIFixture(t)
	ctx := context.Background()
	change, err := f.client.PutPlugin(ctx, "openai", plugins.Spec{Kind: plugins.KindOpenAI, Limits: []plugins.Limit{{Metric: plugins.MetricTokens, Per: plugins.PerMonth, Amount: 100}}})
	if err != nil || !change.Changed || change.Plugin.HasKey {
		t.Fatalf("install: %+v %v", change, err)
	}
	_, err = f.client.PutPlugin(ctx, "gemini", plugins.Spec{Kind: "gemini"})
	checkStatus(t, "an unknown kind", err, 400)
	if err := f.client.SetPluginKey(ctx, "openai", "", "sk-operator"); err != nil {
		t.Fatal(err)
	}
	app := appSpec()
	app.Plugins = []string{"openai"}
	app.Env = map[string]string{"OPENAI_BASE_URL": "{{plugin.openai.url}}/v1"}
	if _, err := f.client.PutApp(ctx, "web", app); err != nil {
		t.Fatal(err)
	}
	if _, err := f.client.Create(ctx, tenantOf("acme")); err != nil {
		t.Fatal(err)
	}
	listed, err := f.client.Plugins(ctx)
	if err != nil || len(listed) != 1 || !listed[0].HasKey || len(listed[0].Apps) != 1 {
		t.Fatalf("list: %+v %v", listed, err)
	}
	stored, _ := f.store.Plugin("openai")
	if opened, err := f.server.key.Open(plugins.KeyContext("openai"), stored.Key); err != nil || opened != "sk-operator" {
		t.Fatalf("the key is not sealed for the plugin: %q %v", opened, err)
	}
	table := Routes{Store: f.store}.Table()
	served := table.Plugins
	if served.Plugins["openai"].Key != stored.Key || served.Tenants["acme"].Address == "" || len(served.Tenants["acme"].Grants["openai"].Limits) != 1 {
		t.Fatalf("the edge is given %+v", served)
	}
	// A tenant's own limits change the routes' version.
	if err := f.client.SetPluginLimits(ctx, "openai", "acme", []plugins.Limit{{Metric: plugins.MetricTokens, Per: plugins.PerDay, Amount: 50}}, true); err != nil {
		t.Fatal(err)
	}
	if again := (Routes{Store: f.store}).Table(); again.Version == table.Version || again.Plugins.Tenants["acme"].Grants["openai"].Limits[0].Amount != 50 {
		t.Fatalf("own limits: %+v", again.Plugins.Tenants["acme"])
	}
	day := plugins.Day(time.Now())
	report := plugins.Report{Edge: "edge-1", Seq: 1, Counts: []plugins.Count{{Tenant: "acme", Plugin: "openai", Day: day, Metric: plugins.MetricTokens, Detail: "gpt-5", Amount: 60}}}
	for range 2 {
		totals, err := f.client.EdgePluginUsage(ctx, report)
		if err != nil || totals.Seq != 1 || len(totals.Totals) != 2 || totals.Totals[0].Amount != 60 {
			t.Fatalf("report: %+v %v", totals, err)
		}
	}
	uses, err := f.client.TenantPlugins(ctx, "acme")
	if err != nil || len(uses) != 1 || !uses[0].Granted || !uses[0].OwnLimits || !uses[0].Limits[0].Reached || uses[0].Limits[0].Used != 60 {
		t.Fatalf("the tenant's plugins: %+v %v", uses, err)
	}
	every, err := f.client.PluginUses(ctx)
	if err != nil || len(every) != 1 || every[0].Tenant != "acme" || every[0].Limits[0].Used != 60 {
		t.Fatalf("every tenant's plugins: %+v %v", every, err)
	}
	usage, err := f.client.PluginUsage(ctx, "acme", "", "", "")
	if err != nil || len(usage.Counts) != 1 || usage.Counts[0].Amount != 60 {
		t.Fatalf("usage: %+v %v", usage, err)
	}
	checkStatus(t, "delete while an app lists it", f.client.DeletePlugin(ctx, "openai"), 409)
}

// A plugin of kind container names its image by tag: the agent pins it,
// reads its manifest from its label without running it, and gives the
// edge both.
func TestAPluginsImageThroughTheAPI(t *testing.T) {
	f := newAPIFixture(t)
	ctx := context.Background()
	printed, err := exec.Command(filepath.Join(runtime.GOROOT(), "bin", "go"), "run", "../../examples/plugins/anthropic", "-manifest").Output()
	if err != nil {
		t.Fatalf("the example's manifest: %v", err)
	}
	tag := "ghcr.io/acme/anthropic-plugin:1.0.0"
	f.runtime.setLabels(pinnedTag(tag), map[string]string{plugins.ManifestLabel: string(printed)})
	f.runtime.setLabels(pinnedTag("ghcr.io/acme/broken:1"), map[string]string{plugins.ManifestLabel: `{"abi":2,"name":"x","run":"rm -rf /"}`})
	spec := plugins.Spec{Kind: plugins.KindContainer, Image: tag, Limits: []plugins.Limit{{Metric: "tokens", Per: plugins.PerMonth, Amount: 100}}}
	for name, image := range map[string]string{
		"an image without the label": "ghcr.io/acme/plain:1",
		"a label not a manifest":     "ghcr.io/acme/broken:1",
		"no image reference":         "Not An Image!",
	} {
		_, err := f.client.PutPlugin(ctx, "anthropic", plugins.Spec{Kind: plugins.KindContainer, Image: image})
		checkStatus(t, name, err, 400)
	}
	wrong := spec
	wrong.Limits = []plugins.Limit{{Metric: "widgets", Per: plugins.PerDay, Amount: 1}}
	_, err = f.client.PutPlugin(ctx, "anthropic", wrong)
	checkStatus(t, "a limit of a metric the image does not count", err, 400)
	change, err := f.client.PutPlugin(ctx, "anthropic", spec)
	if err != nil || !change.Changed || change.Plugin.Manifest == nil || change.Plugin.Manifest.Name != "anthropic" || change.Plugin.Spec.Image != pinnedTag(tag) {
		t.Fatalf("install: %+v %v", change, err)
	}
	// Applied again, it is the same.
	if change, err := f.client.PutPlugin(ctx, "anthropic", spec); err != nil || change.Changed {
		t.Fatalf("again: %+v %v", change, err)
	}
	if served := (Routes{Store: f.store}).Table().Plugins.Plugins["anthropic"]; served.Manifest == nil || served.Spec.Image != pinnedTag(tag) {
		t.Fatalf("the edge is given %+v", served)
	}
}
