package store

import (
	"database/sql"
	"errors"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

const (
	pinnedV1 = "ghcr.io/example/web@sha256:1111111111111111111111111111111111111111111111111111111111111111"
	pinnedV2 = "ghcr.io/example/web@sha256:2222222222222222222222222222222222222222222222222222222222222222"
	pinnedV3 = "ghcr.io/example/web@sha256:3333333333333333333333333333333333333333333333333333333333333333"
)

func taggedApp(image, tag string) App {
	app := App{Name: "web", Spec: appSpec(), Tag: tag}
	app.Spec.Image = image
	return app
}

func TestEveryChangeOfAnAppIsARevision(t *testing.T) {
	st, _ := openNew(t, options())
	created, err := st.PutApp(taggedApp(pinnedV1, "ghcr.io/example/web:stable"), "alice")
	if err != nil || created.App.Revision != 1 || created.App.Tag != "ghcr.io/example/web:stable" {
		t.Fatalf("created %+v, %v", created.App, err)
	}
	if again, err := st.PutApp(taggedApp(pinnedV1, "ghcr.io/example/web:stable"), "alice"); err != nil || again.Changed || again.App.Revision != 1 {
		t.Fatalf("the same app again: %+v, %v", again, err)
	}
	if _, err := st.Create(NewTenant{Name: "acme", App: "web"}, "alice"); err != nil {
		t.Fatal(err)
	}
	// Another tag for the same image changes no tenant, but is a revision.
	retagged, err := st.PutApp(taggedApp(pinnedV1, "ghcr.io/example/web:1.0"), "bob")
	if err != nil || !retagged.Changed || retagged.App.Revision != 2 || len(retagged.Tenants) != 0 {
		t.Fatalf("retagged %+v, %v", retagged, err)
	}
	updated, err := st.PutApp(taggedApp(pinnedV2, "ghcr.io/example/web:1.0"), "carol")
	if err != nil || updated.App.Revision != 3 || len(updated.Tenants) != 1 {
		t.Fatalf("updated %+v, %v", updated, err)
	}
	revisions, err := st.Revisions("web")
	if err != nil || len(revisions) != 3 {
		t.Fatalf("revisions %+v, %v", revisions, err)
	}
	if newest := revisions[0]; newest.Revision != 3 || newest.Spec.Image != pinnedV2 || newest.Actor != "carol" || newest.Tag != "ghcr.io/example/web:1.0" {
		t.Fatalf("newest revision %+v", newest)
	}
	if oldest := revisions[2]; oldest.Revision != 1 || oldest.Spec.Image != pinnedV1 || oldest.Tag != "ghcr.io/example/web:stable" || oldest.Actor != "alice" {
		t.Fatalf("oldest revision %+v", oldest)
	}
	events, _ := st.Events(EventQuery{App: "web", Limit: 1})
	if len(events) != 1 || !strings.Contains(events[0].Detail, "of ghcr.io/example/web:1.0") {
		t.Fatalf("events %+v", events)
	}
	if _, err := st.Revisions("nope"); !errors.Is(err, ErrNotFound) {
		t.Fatalf("revisions of no app: %v", err)
	}
}

func TestOnlyRecentRevisionsAreKept(t *testing.T) {
	st, _ := openNew(t, options())
	for i := range revisionsKept + 5 {
		app := taggedApp(pinnedV1, "")
		app.Spec.Env = map[string]string{"N": strings.Repeat("x", i+1)}
		if _, err := st.PutApp(app, "alice"); err != nil {
			t.Fatal(err)
		}
	}
	revisions, _ := st.Revisions("web")
	if len(revisions) != revisionsKept || revisions[0].Revision != revisionsKept+5 || revisions[len(revisions)-1].Revision != 6 {
		t.Fatalf("kept %d revisions, %d to %d", len(revisions), revisions[len(revisions)-1].Revision, revisions[0].Revision)
	}
}

func TestARevisionCanBeStoredAgain(t *testing.T) {
	st, _ := openNew(t, options())
	if _, err := st.PutApp(taggedApp(pinnedV1, "ghcr.io/example/web:stable"), "alice"); err != nil {
		t.Fatal(err)
	}
	if _, err := st.Create(NewTenant{Name: "acme", App: "web"}, "alice"); err != nil {
		t.Fatal(err)
	}
	second, err := st.PutApp(taggedApp(pinnedV2, "ghcr.io/example/web:stable"), "alice")
	if err != nil {
		t.Fatal(err)
	}
	if _, err := st.RestoreRevision("web", 1, AppIf{UpdatedAt: second.App.CreatedAt.Add(-time.Hour)}, true, "bob"); !errors.Is(err, ErrConflict) {
		t.Fatalf("a restore over a change made meanwhile: %v", err)
	}
	back, err := st.RestoreRevision("web", 1, AppIf{UpdatedAt: second.App.UpdatedAt}, true, "bob")
	if err != nil || back.App.Revision != 3 || back.App.Spec.Image != pinnedV1 || back.App.Tag != "ghcr.io/example/web:stable" || len(back.Tenants) != 1 {
		t.Fatalf("back to revision 1: %+v, %v", back, err)
	}
	if record, _ := st.Get("acme"); record.Spec.Image != pinnedV1 {
		t.Fatalf("tenant runs %s", record.Spec.Image)
	}
	events, _ := st.Events(EventQuery{App: "web", Limit: 1})
	if len(events) != 1 || events[0].Actor != "bob" || !strings.HasPrefix(events[0].Detail, "back to revision 1;") {
		t.Fatalf("events %+v", events)
	}
	if _, err := st.RestoreRevision("web", 9, AppIf{}, true, "bob"); !errors.Is(err, ErrNotFound) {
		t.Fatalf("an unknown revision: %v", err)
	}
	// The revision the app already runs changes nothing.
	if same, err := st.RestoreRevision("web", 3, AppIf{}, true, "bob"); err != nil || same.Changed {
		t.Fatalf("the newest revision again: %+v, %v", same, err)
	}
}

func TestTheRegistrysNewestImageIsNewsUntilTheAppRunsIt(t *testing.T) {
	st, _ := openNew(t, options())
	const tag = "ghcr.io/example/web:stable"
	if _, err := st.PutApp(taggedApp(pinnedV1, tag), "alice"); err != nil {
		t.Fatal(err)
	}
	drain(st)
	if changed, err := setLatest(st, "web", tag, pinnedV1); err != nil || changed {
		t.Fatalf("the image the app runs: %v %v", changed, err)
	}
	if changed, err := setLatest(st, "web", "ghcr.io/example/web:other", pinnedV2); err != nil || changed {
		t.Fatalf("a check of another tag: %v %v", changed, err)
	}
	if _, err := setLatest(st, "nope", tag, pinnedV2); !errors.Is(err, ErrNotFound) {
		t.Fatalf("no such app: %v", err)
	}
	changed, err := setLatest(st, "web", tag, pinnedV2)
	if err != nil || !changed {
		t.Fatalf("a newer image: %v %v", changed, err)
	}
	app, _ := st.App("web")
	if app.Latest != pinnedV2 || app.LatestAt.IsZero() {
		t.Fatalf("app %+v", app)
	}
	if drain(st) {
		t.Fatal("finding an image woke the reconciler")
	}
	if changed, _ := setLatest(st, "web", tag, pinnedV2); changed {
		t.Fatal("the same image found twice changed the app twice")
	}
	events, _ := st.Events(EventQuery{App: "web", Limit: 1})
	if len(events) != 1 || events[0].Action != "app.image-found" || events[0].Detail != "image "+pinnedV2+" of "+tag {
		t.Fatalf("events %+v", events)
	}
	// A conditional store of the app is not refused for it.
	if _, err := st.PutAppIf(withEnv(taggedApp(pinnedV1, tag), "A", "1"), AppIf{UpdatedAt: app.UpdatedAt}, "alice"); err != nil {
		t.Fatal(err)
	}
	if app, _ := st.App("web"); app.Latest != pinnedV2 {
		t.Fatalf("a change of env forgot the image found: %+v", app)
	}
	// Running a newer one still keeps it news.
	if _, err := setLatest(st, "web", tag, pinnedV3); err != nil {
		t.Fatal(err)
	}
	if _, err := st.PutApp(withEnv(taggedApp(pinnedV2, tag), "A", "1"), "alice"); err != nil {
		t.Fatal(err)
	}
	if app, _ := st.App("web"); app.Latest != pinnedV3 {
		t.Fatalf("latest %q after taking an older image", app.Latest)
	}
	// Taking it ends the news.
	if _, err := st.PutApp(withEnv(taggedApp(pinnedV3, tag), "A", "1"), "alice"); err != nil {
		t.Fatal(err)
	}
	if app, _ := st.App("web"); app.Latest != "" || !app.LatestAt.IsZero() {
		t.Fatalf("latest %q at %v once the app runs it", app.Latest, app.LatestAt)
	}
	// So does another tag.
	if _, err := setLatest(st, "web", tag, pinnedV1); err != nil {
		t.Fatal(err)
	}
	if _, err := st.PutApp(withEnv(taggedApp(pinnedV3, "ghcr.io/example/web:3"), "A", "1"), "alice"); err != nil {
		t.Fatal(err)
	}
	if app, _ := st.App("web"); app.Latest != "" {
		t.Fatalf("latest %q of the old tag", app.Latest)
	}
	// And the registry going back to what the app runs.
	if _, err := setLatest(st, "web", "ghcr.io/example/web:3", pinnedV1); err != nil {
		t.Fatal(err)
	}
	if changed, err := setLatest(st, "web", "ghcr.io/example/web:3", pinnedV3); err != nil || !changed {
		t.Fatalf("the registry back to the app's image: %v %v", changed, err)
	}
	if app, _ := st.App("web"); app.Latest != "" {
		t.Fatalf("latest %q", app.Latest)
	}
	// An app given pinned asks the registry nothing.
	if _, err := st.PutApp(withEnv(taggedApp(pinnedV3, ""), "A", "1"), "alice"); err != nil {
		t.Fatal(err)
	}
	if changed, _ := setLatest(st, "web", "", pinnedV1); changed {
		t.Fatal("an app without a tag took an image found")
	}
}

// setLatest records what the registry holds as asked about the app as it
// is now.
func setLatest(st *SQLite, name, tag, latest string) (bool, error) {
	app, _ := st.App(name)
	return st.SetLatest(name, tag, latest, app.Revision, "murder")
}

func TestAnAnswerOlderThanAChangeIsDropped(t *testing.T) {
	st, _ := openNew(t, options())
	const tag = "ghcr.io/example/web:stable"
	if _, err := st.PutApp(taggedApp(pinnedV1, tag), "alice"); err != nil {
		t.Fatal(err)
	}
	asked, _ := st.App("web")
	// The app takes v3 while the question about revision 1 is out; the
	// registry's older answer, v2, is no newer image.
	if _, err := st.PutApp(taggedApp(pinnedV3, tag), "alice"); err != nil {
		t.Fatal(err)
	}
	if changed, err := st.SetLatest("web", tag, pinnedV2, asked.Revision, "murder"); err != nil || changed {
		t.Fatalf("an answer about revision %d: %v %v", asked.Revision, changed, err)
	}
	if app, _ := st.App("web"); app.Latest != "" {
		t.Fatalf("latest %q", app.Latest)
	}
}

func TestWhatTheCallerFoundReplacesWhatWasFound(t *testing.T) {
	st, _ := openNew(t, options())
	const tag = "ghcr.io/example/web:stable"
	if _, err := st.PutApp(taggedApp(pinnedV1, tag), "alice"); err != nil {
		t.Fatal(err)
	}
	if _, err := setLatest(st, "web", tag, pinnedV2); err != nil {
		t.Fatal(err)
	}
	// Pinned to the registry's answer just now: v3, newer than v2 found.
	update := taggedApp(pinnedV3, tag)
	update.Latest = pinnedV3
	if _, err := st.PutApp(update, "alice"); err != nil {
		t.Fatal(err)
	}
	if app, _ := st.App("web"); app.Latest != "" {
		t.Fatalf("latest %q after taking the registry's image", app.Latest)
	}
	// Kept, while the registry now names v4: found with the change.
	kept := withEnv(taggedApp(pinnedV3, tag), "A", "1")
	const pinnedV4 = "ghcr.io/example/web@sha256:4444444444444444444444444444444444444444444444444444444444444444"
	kept.Latest = pinnedV4
	if _, err := st.PutApp(kept, "alice"); err != nil {
		t.Fatal(err)
	}
	app, _ := st.App("web")
	if app.Latest != pinnedV4 || app.LatestAt.IsZero() || app.Spec.Image != pinnedV3 {
		t.Fatalf("app %+v", app)
	}
	events, _ := st.Events(EventQuery{App: "web", Limit: 2})
	if len(events) != 2 || events[1].Action != "app.image-found" || events[0].Action != "app.updated" {
		t.Fatalf("events %+v", events)
	}
}

func withEnv(app App, key, value string) App {
	app.Spec.Env = map[string]string{key: value}
	return app
}

func TestDeletingAnAppDeletesItsRevisions(t *testing.T) {
	st, _ := openNew(t, options())
	for _, image := range []string{pinnedV1, pinnedV2} {
		if _, err := st.PutApp(taggedApp(image, ""), "alice"); err != nil {
			t.Fatal(err)
		}
	}
	if err := st.DeleteApp("web", "alice"); err != nil {
		t.Fatal(err)
	}
	again, err := st.PutApp(taggedApp(pinnedV3, ""), "alice")
	if err != nil || again.App.Revision != 1 {
		t.Fatalf("the app again: %+v, %v", again.App, err)
	}
	if revisions, _ := st.Revisions("web"); len(revisions) != 1 {
		t.Fatalf("revisions %+v", revisions)
	}
}

func TestAnAppFromBeforeRevisionsKeepsItsSpecAsTheFirst(t *testing.T) {
	path := filepath.Join(t.TempDir(), "murder.db")
	db, err := sql.Open("sqlite", initDSN(path))
	if err != nil {
		t.Fatal(err)
	}
	for _, statement := range []string{
		schema,
		`ALTER TABLE tenants DROP COLUMN app_revision`,
		`ALTER TABLE tenants DROP COLUMN before_revision`,
		`ALTER TABLE tenants DROP COLUMN canary`,
		`ALTER TABLE tenants DROP COLUMN hold`,
		`ALTER TABLE tenants DROP COLUMN restore`, `ALTER TABLE tenants DROP COLUMN moved_on`,
		`DROP TABLE admissions`, `DROP TABLE departed_keys`, `DROP TRIGGER events_id_bounded`, `DROP TRIGGER events_id_kept_bounded`, `DROP TRIGGER nodes_key_kept`, `DROP TRIGGER nodes_key_not_replaced`, `ALTER TABLE nodes DROP COLUMN key`, `DROP TABLE acme_challenges`, `DROP TABLE acme_accounts`, `DROP TABLE domains`, `DROP TABLE operation_lease`, `ALTER TABLE tenants DROP COLUMN canonical`, `DROP TABLE acme_revocations`, `DROP TABLE rollouts`,
		`DROP TABLE app_revisions`,
		`ALTER TABLE apps DROP COLUMN tag`,
		`ALTER TABLE apps DROP COLUMN revision`,
		`ALTER TABLE apps DROP COLUMN latest`,
		`ALTER TABLE apps DROP COLUMN latest_at`,
		`INSERT INTO meta (key, value) VALUES ('node', 'n1'), ('version', '3'), ('applied', '0')`,
		`INSERT INTO apps (name, spec, created_at, updated_at) VALUES ('web', '` + marshal(normalizeApp(taggedApp(pinnedV1, "").Spec)) + `', '2026-10-01T00:00:00Z', '2026-10-02T00:00:00Z')`,
		`PRAGMA user_version = 13`,
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
	app, _ := st.App("web")
	if app.Revision != 0 || app.Tag != "" {
		t.Fatalf("upgraded app %+v", app)
	}
	if revisions, _ := st.Revisions("web"); len(revisions) != 0 {
		t.Fatalf("revisions before a change: %+v", revisions)
	}
	if _, err := st.PutApp(taggedApp(pinnedV2, "ghcr.io/example/web:2"), "alice"); err != nil {
		t.Fatal(err)
	}
	revisions, _ := st.Revisions("web")
	if len(revisions) != 2 || revisions[0].Revision != 2 || revisions[1].Revision != 1 || revisions[1].Spec.Image != pinnedV1 || revisions[1].Actor != "murder" || !revisions[1].CreatedAt.Equal(time.Date(2026, 10, 2, 0, 0, 0, 0, time.UTC)) {
		t.Fatalf("revisions %+v", revisions)
	}
	if app, _ := st.App("web"); app.Revision != 2 {
		t.Fatalf("app revision %d", app.Revision)
	}
	// A batch of a version 13 node still applies here.
	recorded := &recorder{}
	_ = recorded.Exec(`INSERT INTO apps (name, spec, created_at, updated_at) VALUES (?, ?, ?, ?)
			ON CONFLICT (name) DO UPDATE SET spec = excluded.spec, updated_at = excluded.updated_at`, "other", marshal(normalizeApp(appSpec())), "2026-10-03T00:00:00Z", "2026-10-03T00:00:00Z")
	if err := st.Apply(Batch{Schema: 13, Version: st.state.Load().version, Statements: recorded.statements}); err != nil {
		t.Fatal(err)
	}
	if other, err := st.App("other"); err != nil || other.Revision != 0 {
		t.Fatalf("app of a version 13 batch: %+v, %v", other, err)
	}
}
