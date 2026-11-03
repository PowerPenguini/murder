package apiserver

import (
	"context"
	"fmt"
	"net/http/httptest"
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/PowerPenguini/murder/internal/api"
	"github.com/PowerPenguini/murder/internal/store"
)

func TestAnAppKeepsItsImageUntilItIsUpdated(t *testing.T) {
	f := withApp(t)
	ctx := context.Background()
	const tag = "ghcr.io/example/app:1.4"
	if _, err := f.client.Create(ctx, tenantOf("acme")); err != nil {
		t.Fatal(err)
	}
	app, err := f.client.App(ctx, "web")
	if err != nil || app.Tag != tag || app.Revision != 1 || app.Spec.Image != pinnedTag(tag) {
		t.Fatalf("app %+v, %v", app, err)
	}
	// The tag moves in the registry.
	moved := "ghcr.io/example/app@sha256:" + strings.Repeat("9", 64)
	f.runtime.setLatest(tag, moved)
	check, err := f.client.CheckApp(ctx, "web")
	if err != nil || !check.Newer || check.Image != moved || check.App.Latest != moved || check.App.LatestAt.IsZero() {
		t.Fatalf("check %+v, %v", check, err)
	}
	// A change of the spec keeps the image, and says a newer one is there.
	next := appSpec()
	next.Env = map[string]string{"TZ": "UTC"}
	applied, err := f.client.PutApp(ctx, "web", next)
	if err != nil || !applied.Changed || applied.App.Spec.Image != pinnedTag(tag) || !strings.Contains(applied.Note, "murder app update web") || applied.App.Latest != moved {
		t.Fatalf("a change of env %+v, %v", applied, err)
	}
	// An update made on an outdated read is refused.
	_, err = f.client.AtOnce().UpdateApp(ctx, "web", app.UpdatedAt)
	checkStatus(t, "an update over a change made meanwhile", err, 409)
	updated, err := f.client.AtOnce().UpdateApp(ctx, "web", time.Time{})
	if err != nil || !updated.Changed || updated.App.Spec.Image != moved || updated.App.Latest != "" || updated.App.Spec.Env["TZ"] != "UTC" || updated.App.Revision != 3 {
		t.Fatalf("update %+v, %v", updated, err)
	}
	if len(updated.Tenants) != 1 || updated.Tenants[0].Spec.Image != moved {
		t.Fatalf("the update rolls out to %+v", updated.Tenants)
	}
	if check, err := f.client.CheckApp(ctx, "web"); err != nil || check.Newer || check.App.Latest != "" {
		t.Fatalf("check after the update %+v, %v", check, err)
	}
	// Apply with update takes the newest at once.
	newest := "ghcr.io/example/app@sha256:" + strings.Repeat("8", 64)
	f.runtime.setLatest(tag, newest)
	taken, err := f.client.AtOnce().PutAppNewest(ctx, "web", next)
	if err != nil || taken.App.Spec.Image != newest {
		t.Fatalf("apply with update %+v, %v", taken, err)
	}
	// Back to an earlier revision.
	revisions, err := f.client.AppRevisions(ctx, "web")
	if err != nil || len(revisions) != 4 || revisions[0].Revision != 4 || revisions[3].Spec.Image != pinnedTag(tag) {
		t.Fatalf("revisions %+v, %v", revisions, err)
	}
	_, err = f.client.AtOnce().RestoreRevision(ctx, "web", 1, app.UpdatedAt)
	checkStatus(t, "a restore over a change made meanwhile", err, 409)
	back, err := f.client.AtOnce().RestoreRevision(ctx, "web", 1, taken.App.UpdatedAt)
	if err != nil || back.App.Spec.Image != pinnedTag(tag) || back.App.Revision != 5 || len(back.Tenants) != 1 || back.App.Spec.Env["TZ"] != "" {
		t.Fatalf("back to revision 1: %+v, %v", back, err)
	}
	_, err = f.client.AtOnce().RestoreRevision(ctx, "web", 99, time.Time{})
	checkStatus(t, "an unknown revision", err, 404)
	_, err = f.client.AppRevisions(ctx, "nope")
	checkStatus(t, "revisions of no app", err, 404)
	// A registry that does not answer.
	f.runtime.setLatest(tag, "")
	_, err = f.client.CheckApp(ctx, "web")
	checkStatus(t, "a registry that does not answer", err, 502)
}

func TestAnAppGivenPinnedHasNoTagToAskAbout(t *testing.T) {
	f := newAPIFixture(t)
	ctx := context.Background()
	pinned := appSpec()
	pinned.Image = "ghcr.io/example/app@sha256:" + strings.Repeat("7", 64)
	applied, err := f.client.PutApp(ctx, "web", pinned)
	if err != nil || applied.App.Tag != "" {
		t.Fatalf("pinned app %+v, %v", applied, err)
	}
	_, err = f.client.CheckApp(ctx, "web")
	checkStatus(t, "check a pinned app", err, 422)
	_, err = f.client.AtOnce().UpdateApp(ctx, "web", time.Time{})
	checkStatus(t, "update a pinned app", err, 422)
	_, err = f.client.CheckApp(ctx, "nope")
	checkStatus(t, "check no app", err, 404)
	// Given a tag again, it is pinned anew and asked about.
	tagged, err := f.client.PutApp(ctx, "web", appSpec())
	if err != nil || tagged.App.Tag != "ghcr.io/example/app:1.4" || tagged.App.Spec.Image != pinnedTag("ghcr.io/example/app:1.4") {
		t.Fatalf("tagged again %+v, %v", tagged, err)
	}
}

func TestAnUpdateTakesOnlyTheRegistrysAnswer(t *testing.T) {
	f := withApp(t)
	ctx := context.Background()
	const tag = "ghcr.io/example/app:1.4"
	before, _ := f.client.App(ctx, "web")
	// The registry does not answer: an update changes nothing, rather than
	// falling back to whatever this node has.
	f.runtime.setLatest(tag, "")
	_, err := f.client.AtOnce().UpdateApp(ctx, "web", time.Time{})
	checkStatus(t, "an update while the registry does not answer", err, 502)
	if after, _ := f.client.App(ctx, "web"); after.Spec.Image != before.Spec.Image || after.Revision != before.Revision {
		t.Fatalf("a refused update changed the app: %+v", after)
	}
	// An image found earlier is forgotten once a newer one is taken.
	d2 := "ghcr.io/example/app@sha256:" + strings.Repeat("2", 64)
	d3 := "ghcr.io/example/app@sha256:" + strings.Repeat("3", 64)
	f.runtime.setLatest(tag, d2)
	if _, err := f.client.CheckApp(ctx, "web"); err != nil {
		t.Fatal(err)
	}
	f.runtime.setLatest(tag, d3)
	updated, err := f.client.AtOnce().UpdateApp(ctx, "web", time.Time{})
	if err != nil || updated.App.Spec.Image != d3 || updated.App.Latest != "" {
		t.Fatalf("update %+v, %v", updated.App, err)
	}
	// Applying the same tag again asks the registry, and says what it holds.
	d4 := "ghcr.io/example/app@sha256:" + strings.Repeat("4", 64)
	f.runtime.setLatest(tag, d4)
	same, err := f.client.PutApp(ctx, "web", appSpec())
	if err != nil || same.Changed || same.App.Spec.Image != d3 || same.App.Latest != d4 || !strings.Contains(same.Note, "murder app update web") {
		t.Fatalf("the same tag again %+v, %v", same, err)
	}
	if _, err := f.client.AtOnce().PutAppNewest(ctx, "web", appSpec()); err != nil {
		t.Fatal(err)
	}
	if app, _ := f.client.App(ctx, "web"); app.Spec.Image != d4 || app.Latest != "" {
		t.Fatalf("after apply -update %+v", app)
	}
}

// Only an update of the tag the app follows insists on the registry's
// answer: apply -update with a new tag pins it as a plain apply would.
func TestApplyWithUpdateTakesANewTagAsAPlainApplyWould(t *testing.T) {
	f := withApp(t)
	ctx := context.Background()
	next := appSpec()
	next.Image = "ghcr.io/example/app:dev"
	f.runtime.setLatest(next.Image, "")
	applied, err := f.client.AtOnce().PutAppNewest(ctx, "web", next)
	if err != nil || applied.App.Spec.Image != pinnedTag(next.Image) || applied.App.Tag != next.Image {
		t.Fatalf("a new tag with -update: %+v, %v", applied.App, err)
	}
	// The same tag again, the registry still silent: refused, unchanged.
	_, err = f.client.AtOnce().PutAppNewest(ctx, "web", next)
	checkStatus(t, "an update of the followed tag without the registry", err, 502)
}

func TestAnAppChangeRollsOutInWavesThroughTheAPI(t *testing.T) {
	f := withApp(t)
	ctx := context.Background()
	for _, name := range []string{"acme", "beta", "gamma"} {
		if _, err := f.client.Create(ctx, tenantOf(name)); err != nil {
			t.Fatal(err)
		}
	}
	yes := true
	if tenant, err := f.client.SetFlags(ctx, "gamma", api.TenantFlags{Canary: &yes}); err != nil || !tenant.Canary {
		t.Fatalf("flags %+v, %v", tenant, err)
	}
	next := appSpec()
	next.Image = "ghcr.io/example/app:1.5"
	applied, err := f.client.PutApp(ctx, "web", next)
	if err != nil || !applied.Rollout || len(applied.Tenants) != 3 || applied.App.Rollout == nil || applied.App.Rollout.State != store.RolloutRunning {
		t.Fatalf("applied %+v, %v", applied, err)
	}
	// Nothing moved: every tenant runs the revision it ran.
	for _, tenant := range applied.Tenants {
		if tenant.AppRevision != 1 || tenant.Spec.Image != pinnedTag("ghcr.io/example/app:1.4") {
			t.Fatalf("%s moved: %+v", tenant.Spec.Name, tenant)
		}
	}
	app, err := f.client.RolloutAction(ctx, "web", "pause")
	if err != nil || app.Rollout.State != store.RolloutPaused {
		t.Fatalf("pause %+v, %v", app.Rollout, err)
	}
	_, err = f.client.RolloutAction(ctx, "web", "promote")
	checkStatus(t, "promote a paused rollout", err, 409)
	if app, err = f.client.RolloutAction(ctx, "web", "resume"); err != nil || app.Rollout.State != store.RolloutRunning {
		t.Fatalf("resume %+v, %v", app.Rollout, err)
	}
	// Promote moves every tenant waiting, at once.
	if _, err := f.client.RolloutAction(ctx, "web", "promote"); err != nil {
		t.Fatal(err)
	}
	for _, name := range []string{"acme", "beta", "gamma"} {
		if tenant, _ := f.client.Get(ctx, name); tenant.AppRevision != 0 || tenant.Spec.Image != pinnedTag("ghcr.io/example/app:1.5") {
			t.Fatalf("%s after promote: %+v", name, tenant)
		}
	}
	// Restore takes beta back with its data, as asked: the rollout pauses,
	// and beta takes no request until its data is back.
	routes := Routes{Store: f.store}
	restored, err := f.client.RestoreRollout(ctx, "web", []string{"beta"})
	if err != nil || !slices.Equal(restored.Tenants, []string{"beta"}) || restored.App.Rollout.State != store.RolloutPaused {
		t.Fatalf("restore %+v, %v", restored, err)
	}
	if tenant, _ := f.client.Get(ctx, "beta"); tenant.AppRevision != 1 || tenant.Spec.Image != pinnedTag("ghcr.io/example/app:1.4") {
		t.Fatalf("beta after restore: %+v", tenant)
	}
	if route, _ := routes.Route("beta"); !route.Moving {
		t.Fatalf("beta's requests are not held while its data goes back: %+v", route)
	}
	if route, _ := routes.Route("acme"); route.Moving || route.Backend == "" {
		t.Fatalf("acme is held: %+v", route)
	}
	// beta went back already; one not of the app is refused, nothing
	// changed.
	for _, names := range [][]string{{"beta"}, {"acme", "nobody"}} {
		_, err = f.client.RestoreRollout(ctx, "web", names)
		checkStatus(t, fmt.Sprintf("restore %v", names), err, 409)
	}
	if tenant, _ := f.client.Get(ctx, "acme"); tenant.AppRevision != 0 {
		t.Fatalf("a refused restore moved acme: %+v", tenant)
	}
	// Abort takes the others back, their data as it is, and beta's data
	// stays as it is too.
	if app, err = f.client.RolloutAction(ctx, "web", "abort"); err != nil || app.Rollout.State != store.RolloutAborted {
		t.Fatalf("abort %+v, %v", app.Rollout, err)
	}
	if tenant, _ := f.client.Get(ctx, "acme"); tenant.AppRevision != 1 || tenant.Spec.Image != pinnedTag("ghcr.io/example/app:1.4") {
		t.Fatalf("acme after abort: %+v", tenant)
	}
	if route, _ := routes.Route("beta"); route.Moving {
		t.Fatalf("beta still held after the abort: %+v", route)
	}
	// A restore without a body names every tenant; nothing is left.
	w := httptest.NewRecorder()
	f.handler.ServeHTTP(w, httptest.NewRequest("POST", "/v1/apps/web/rollout/restore", nil))
	if w.Code != 409 {
		t.Fatalf("a restore of an aborted rollout: %d %s", w.Code, w.Body)
	}
	_, err = f.client.RolloutAction(ctx, "web", "dance")
	checkStatus(t, "an unknown action", err, 400)
	_, err = f.client.RolloutAction(ctx, "nope", "pause")
	checkStatus(t, "no such app", err, 404)
}
