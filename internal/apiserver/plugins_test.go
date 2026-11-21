package apiserver

import (
	"bytes"
	"context"
	"crypto/sha256"
	"os"
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
	change, err := f.client.PutPlugin(ctx, "openai", plugins.Spec{Kind: plugins.KindOpenAI, Limits: []plugins.Limit{{Metric: plugins.MetricTokens, Per: plugins.PerMonth, Amount: 100}}}, nil)
	if err != nil || !change.Changed || change.Plugin.HasKey {
		t.Fatalf("install: %+v %v", change, err)
	}
	_, err = f.client.PutPlugin(ctx, "gemini", plugins.Spec{Kind: "gemini"}, nil)
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

// A plugin of kind wasm comes with its module: the agent reads its
// manifest, keeps both, and gives the module to the edge by its digest.
func TestAPluginsModuleThroughTheAPI(t *testing.T) {
	f := newAPIFixture(t)
	ctx := context.Background()
	out := filepath.Join(t.TempDir(), "anthropic.wasm")
	build := exec.Command(filepath.Join(runtime.GOROOT(), "bin", "go"), "build", "-buildmode=c-shared", "-o", out, "../../examples/plugins/anthropic")
	build.Env = append(os.Environ(), "GOOS=wasip1", "GOARCH=wasm", "CGO_ENABLED=0", "GOFLAGS=")
	if output, err := build.CombinedOutput(); err != nil {
		t.Fatalf("build the example: %v\n%s", err, output)
	}
	wasm, err := os.ReadFile(out)
	if err != nil {
		t.Fatal(err)
	}
	digest := plugins.ModuleDigest(sha256.Sum256(wasm))
	spec := plugins.Spec{Kind: plugins.KindWasm, Module: digest, Limits: []plugins.Limit{{Metric: "tokens", Per: plugins.PerMonth, Amount: 100}}}
	// Not WebAssembly, or not the module the spec names: refused.
	junk := []byte("not wasm")
	_, err = f.client.PutPlugin(ctx, "anthropic", plugins.Spec{Kind: plugins.KindWasm, Module: plugins.ModuleDigest(sha256.Sum256(junk))}, junk)
	checkStatus(t, "a module that is no WebAssembly", err, 400)
	_, err = f.client.PutPlugin(ctx, "anthropic", plugins.Spec{Kind: plugins.KindWasm, Module: plugins.ModuleDigest(sha256.Sum256(junk))}, wasm)
	checkStatus(t, "a module the spec does not name", err, 400)
	_, err = f.client.PutPlugin(ctx, "anthropic", spec, nil)
	checkStatus(t, "a module not sent", err, 400)
	change, err := f.client.PutPlugin(ctx, "anthropic", spec, wasm)
	if err != nil || !change.Changed || change.Plugin.Manifest == nil || change.Plugin.Manifest.Name != "anthropic" {
		t.Fatalf("install: %+v %v", change, err)
	}
	// Sent again, it is the same.
	if change, err := f.client.PutPlugin(ctx, "anthropic", spec, wasm); err != nil || change.Changed {
		t.Fatalf("again: %+v %v", change, err)
	}
	fetched, err := f.client.EdgePluginModule(ctx, digest)
	if err != nil || !bytes.Equal(fetched, wasm) {
		t.Fatalf("the edge fetched %d bytes: %v", len(fetched), err)
	}
	_, err = f.client.EdgePluginModule(ctx, plugins.ModuleDigest(sha256.Sum256(junk)))
	checkStatus(t, "a module not installed", err, 404)
	if served := (Routes{Store: f.store}).Table().Plugins.Plugins["anthropic"]; served.Manifest == nil || served.Spec.Module != digest {
		t.Fatalf("the edge is given %+v", served)
	}
}
