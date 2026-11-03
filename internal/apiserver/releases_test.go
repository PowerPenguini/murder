package apiserver

import (
	"context"
	"strings"
	"testing"
	"time"
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
	_, err = f.client.UpdateApp(ctx, "web", app.UpdatedAt)
	checkStatus(t, "an update over a change made meanwhile", err, 409)
	updated, err := f.client.UpdateApp(ctx, "web", time.Time{})
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
	taken, err := f.client.PutAppNewest(ctx, "web", next)
	if err != nil || taken.App.Spec.Image != newest {
		t.Fatalf("apply with update %+v, %v", taken, err)
	}
	// Back to an earlier revision.
	revisions, err := f.client.AppRevisions(ctx, "web")
	if err != nil || len(revisions) != 4 || revisions[0].Revision != 4 || revisions[3].Spec.Image != pinnedTag(tag) {
		t.Fatalf("revisions %+v, %v", revisions, err)
	}
	_, err = f.client.RestoreRevision(ctx, "web", 1, app.UpdatedAt)
	checkStatus(t, "a restore over a change made meanwhile", err, 409)
	back, err := f.client.RestoreRevision(ctx, "web", 1, taken.App.UpdatedAt)
	if err != nil || back.App.Spec.Image != pinnedTag(tag) || back.App.Revision != 5 || len(back.Tenants) != 1 || back.App.Spec.Env["TZ"] != "" {
		t.Fatalf("back to revision 1: %+v, %v", back, err)
	}
	_, err = f.client.RestoreRevision(ctx, "web", 99, time.Time{})
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
	_, err = f.client.UpdateApp(ctx, "web", time.Time{})
	checkStatus(t, "update a pinned app", err, 422)
	_, err = f.client.CheckApp(ctx, "nope")
	checkStatus(t, "check no app", err, 404)
	// Given a tag again, it is pinned anew and asked about.
	tagged, err := f.client.PutApp(ctx, "web", appSpec())
	if err != nil || tagged.App.Tag != "ghcr.io/example/app:1.4" || tagged.App.Spec.Image != pinnedTag("ghcr.io/example/app:1.4") {
		t.Fatalf("tagged again %+v, %v", tagged, err)
	}
}
