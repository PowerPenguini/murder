package panel

import (
	"context"
	"html"
	"io"
	"log/slog"
	"net/http"
	"net/http/cookiejar"
	"net/http/httptest"
	"net/url"
	"reflect"
	"regexp"
	"strconv"
	"strings"
	"testing"

	"github.com/PowerPenguini/murder/internal/api"
	"github.com/PowerPenguini/murder/internal/spec"
	"github.com/PowerPenguini/murder/internal/store"
)

// A panel editing the demo cluster's apps and settings.
func newAppsPanel(t *testing.T) (*DemoCluster, *httptest.Server, *http.Client) {
	t.Helper()
	cluster := NewDemoCluster()
	s, err := New(cluster, slog.New(slog.NewTextHandler(io.Discard, nil)))
	if err != nil {
		t.Fatal(err)
	}
	s.Actions, s.Apps = cluster.As, cluster.AsApps
	server := httptest.NewServer(s.Handler())
	t.Cleanup(server.Close)
	jar, _ := cookiejar.New(nil)
	return cluster, server, &http.Client{Jar: jar}
}

const testAppFile = "image: ghcr.io/example/web:1.0\nuser: \"10001:10001\"\nenv:\n  PUBLIC_URL: \"{{url}}\"\nsecrets:\n  SESSION_SECRET: hex\n"

func TestCreatingAnApp(t *testing.T) {
	cluster, server, client := newAppsPanel(t)
	ctx := context.Background()
	if _, page := get(t, server.URL+"/apps"); !strings.Contains(page, `href="/apps/web"`) || !strings.Contains(page, "Create the app") {
		t.Fatalf("the apps page:\n%s", page)
	}

	// Refused, the form comes back as it was typed, saying why.
	response, body := submit(t, client, server, "/apps", url.Values{"name": {"shop"}, "file": {"image: x\nuser: root\nbogus: 1\n"}}, server.URL)
	if response.StatusCode != http.StatusUnprocessableEntity || !strings.Contains(body, "bogus") || !strings.Contains(body, "user: root") {
		t.Fatalf("a bad spec: %d\n%s", response.StatusCode, body)
	}
	_, body = submit(t, client, server, "/apps", url.Values{"name": {"Not A Label"}, "file": {testAppFile}}, server.URL)
	if !strings.Contains(body, "the app&#39;s name") {
		t.Fatalf("a bad name:\n%s", body)
	}
	_, body = submit(t, client, server, "/apps", url.Values{"name": {"web"}, "file": {testAppFile}}, server.URL)
	if !strings.Contains(body, "app web exists already") {
		t.Fatalf("an app of the name stored already was replaced:\n%s", body)
	}

	response, body = submit(t, client, server, "/apps", url.Values{"name": {"shop"}, "file": {testAppFile}}, server.URL)
	if response.Request.URL.Path != "/apps/shop" || !strings.Contains(body, "app shop is created") || !strings.Contains(body, "SESSION_SECRET: hex") {
		t.Fatalf("after creating shop (%s):\n%s", response.Request.URL, body)
	}
	if events, _ := cluster.Events(ctx, store.EventQuery{App: "shop", Limit: 1}); len(events) != 1 || events[0].Action != "app.created" || who(events[0].Actor) != "the local panel" {
		t.Fatalf("the audit log: %+v", events)
	}
	if _, page := get(t, server.URL+"/events?app=shop"); !strings.Contains(page, "app.created") || strings.Contains(page, "tenant.suspended") {
		t.Fatalf("the events of app shop:\n%s", page)
	}
}

func TestEditingAnApp(t *testing.T) {
	cluster, server, client := newAppsPanel(t)
	ctx := context.Background()
	_, page := get(t, server.URL+"/apps/web")
	updated := between(page, `name="updated" value="`, `"`)
	file := between(page, `aria-label="the app's spec">`, "</textarea>")
	if updated == "" || !strings.Contains(file, "TZ: Europe/Warsaw") || !strings.Contains(file, "image: ghcr.io/example/web:stable\n") {
		t.Fatalf("the app's page:\n%s", page)
	}
	before, _ := cluster.AsApps("test").App(ctx, "web")
	changed := strings.Replace(file, "TZ: Europe/Warsaw", "TZ: Europe/Paris", 1)

	// A save over a change made since the page was read is refused, the
	// text kept.
	_, body := submit(t, client, server, "/apps/web", url.Values{"do": {"save"}, "updated": {"2001-01-01T00:00:00Z"}, "file": {changed}}, server.URL)
	if !strings.Contains(body, "someone changed the app since") || !strings.Contains(body, "Europe/Paris") || !strings.Contains(body, `href="/tenants/old-demo"`) || strings.Contains(body, `value="delete"`) {
		t.Fatalf("a stale save:\n%s", body)
	}
	_, body = submit(t, client, server, "/apps/web", url.Values{"do": {"save"}, "updated": {updated}, "file": {changed}}, server.URL)
	// atlas keeps its own TZ, test-staging is held, old-demo archived: the
	// change rolls out to the five whose spec it changes, in waves.
	if !strings.Contains(body, "app web is changed; it rolls out in waves to 5 tenants") {
		t.Fatalf("after saving:\n%s", body)
	}
	if tenant, _ := cluster.Get(ctx, "acme-logistics"); tenant.Spec.Env["TZ"] != "Europe/Paris" || tenant.Spec.Image != before.Spec.Image {
		t.Fatalf("acme-logistics runs %s with %v", tenant.Spec.Image, tenant.Spec.Env)
	}
	// A tenant's own settings stay its own.
	if tenant, _ := cluster.Get(ctx, "atlas"); tenant.Spec.Env["TZ"] != "Europe/Berlin" {
		t.Fatalf("atlas runs %v", tenant.Spec.Env)
	}

	// An app tenants run is not deleted; one without is, its name typed.
	_, body = submit(t, client, server, "/apps/web", url.Values{"do": {"delete"}, "confirm": {"web"}}, server.URL)
	if !strings.Contains(body, "still has tenant") {
		t.Fatalf("deleted an app tenants run:\n%s", body)
	}
	_, body = submit(t, client, server, "/apps/testapp", url.Values{"do": {"delete"}, "confirm": {"test"}}, server.URL)
	if !strings.Contains(body, "type the app&#39;s name to delete it") {
		t.Fatalf("deleted without the name typed:\n%s", body)
	}
	response, body := submit(t, client, server, "/apps/testapp", url.Values{"do": {"delete"}, "confirm": {"testapp"}}, server.URL)
	if response.Request.URL.Path != "/apps" || !strings.Contains(body, "app testapp is deleted") || strings.Contains(body, `href="/apps/testapp"`) {
		t.Fatalf("after deleting testapp:\n%s", body)
	}
}

func TestATenantsSettings(t *testing.T) {
	cluster, server, client := newAppsPanel(t)
	ctx := context.Background()
	_, page := get(t, server.URL+"/tenants/atlas/settings")
	for _, want := range []string{`name="envown" value="FEATURE_PLANNER" class="own-switch" data-key="FEATURE_PLANNER" checked aria-label=`, `name="secretown" value="ADMIN_PASSWORD" class="own-switch" data-key="ADMIN_PASSWORD" checked aria-label=`, `value="768"`, "SESSION_KEY", "Inherited from web"} {
		if !strings.Contains(page, want) {
			t.Fatalf("the settings page lacks %q:\n%s", want, page)
		}
	}
	before, _ := cluster.Get(ctx, "atlas")
	form := url.Values{"do": {"save"}, "app": {"web"}, "generation": {strconv.FormatInt(before.Generation, 10)},
		"env": {"TZ=Europe/Warsaw\nbroken"}, "secrets": {"ADMIN_PASSWORD password 20\nAPI_TOKEN base64 48"}, "memoryMB": {"1024"}, "cpus": {"1.5"}}

	// A form that does not read is shown again, as typed.
	_, body := submit(t, client, server, "/tenants/atlas/settings", form, server.URL)
	if !strings.Contains(body, "env line 2: NAME=value") || !strings.Contains(body, `name="secretown" value="API_TOKEN" class="own-switch" data-key="API_TOKEN" checked aria-label=`) || !strings.Contains(body, `value="48"`) {
		t.Fatalf("a bad env line:\n%s", body)
	}
	form.Set("env", "TZ=Europe/Warsaw\n\n# the planner is on for everyone now")
	stale := url.Values{}
	for key, values := range form {
		stale[key] = values
	}
	stale.Set("generation", "1")
	if _, body = submit(t, client, server, "/tenants/atlas/settings", stale, server.URL); !strings.Contains(body, "someone changed atlas since") {
		t.Fatalf("a stale save:\n%s", body)
	}

	// A save is only of what was reviewed.
	if _, body = submit(t, client, server, "/tenants/atlas/settings", form, server.URL); !strings.Contains(body, "these are not the values you reviewed") {
		t.Fatalf("saved without a review:\n%s", body)
	}
	form = reviewed(t, client, server, "/tenants/atlas/settings", form)
	_, body = submit(t, client, server, "/tenants/atlas/settings", form, server.URL)
	if !strings.Contains(body, "atlas&#39;s settings are changed") {
		t.Fatalf("after saving:\n%s", body)
	}
	after, _ := cluster.Get(ctx, "atlas")
	want := spec.Overrides{Env: map[string]string{"TZ": "Europe/Warsaw"}, Secrets: spec.Secrets{"ADMIN_PASSWORD": {Kind: "password", Bytes: 20}, "API_TOKEN": {Kind: "base64", Bytes: 48}},
		Limits: spec.Limits{MemoryMB: 1024, CPUs: 1.5}}
	if !after.Overrides.Equal(want) || after.Spec.Env["FEATURE_PLANNER"] != "" || after.Spec.Limits.MemoryMB != 1024 || after.Generation != before.Generation+1 {
		t.Fatalf("atlas after saving: %+v, env %v, generation %d", after.Overrides, after.Spec.Env, after.Generation)
	}

	// A secret gets a new value with its name typed; its value is never
	// on the page.
	_, body = submit(t, client, server, "/tenants/atlas/settings", url.Values{"do": {"rotate"}, "key": {"API_TOKEN"}, "confirm": {"api"}}, server.URL)
	if !strings.Contains(body, "type the secret&#39;s name") {
		t.Fatalf("rotated without the name typed:\n%s", body)
	}
	_, body = submit(t, client, server, "/tenants/atlas/settings", url.Values{"do": {"rotate"}, "key": {"API_TOKEN"}, "confirm": {"API_TOKEN"}}, server.URL)
	if !strings.Contains(body, "API_TOKEN has a new value") {
		t.Fatalf("after rotating:\n%s", body)
	}
	if events, _ := cluster.Events(ctx, store.EventQuery{Tenant: "atlas", Limit: 1}); events[0].Action != "secret.rotated" || events[0].Detail != "API_TOKEN" {
		t.Fatalf("the audit log: %+v", events[0])
	}
}

func TestReadingSettings(t *testing.T) {
	form := settingsForm{Env: "A=1\r\nB = two=2\n#C=3\n\nD=\" padded \"\nE=\"line\\nbreak\"", Secrets: "X\nY password\nZ base64 16", MemoryMB: "512", CPUs: "0.5", PIDs: "", DiskGB: "20"}
	got, err := form.overrides()
	want := spec.Overrides{Env: map[string]string{"A": "1", "B": "two=2", "D": " padded ", "E": "line\nbreak"}, Secrets: spec.Secrets{"X": {Kind: "hex"}, "Y": {Kind: "password"}, "Z": {Kind: "base64", Bytes: 16}},
		Limits: spec.Limits{MemoryMB: 512, CPUs: 0.5, DiskGB: 20}}
	if err != nil || !reflect.DeepEqual(got, want) {
		t.Fatalf("overrides = %+v, %v", got, err)
	}
	// Read back, the form says the same, whatever the values hold.
	want.Env["CERT"] = "-----BEGIN-----\nAAA=\nBBB=\n-----END-----\n"
	want.Env["QUOTED"] = `"as typed"`
	want.Env["CR"] = "value\r"
	if back, err := formOf(apiTenant(want)).overrides(); err != nil || !reflect.DeepEqual(back, want) {
		t.Fatalf("read back: %+v, %v\nfrom:\n%s", back, err, formOf(apiTenant(want)).Env)
	}
	for _, bad := range []settingsForm{
		{Env: "A=1\nA=2"}, {Env: "=1"}, {Secrets: "X hex 0"}, {Secrets: "X hex 1 2"}, {Secrets: "X\nX"},
		{MemoryMB: "lots"}, {CPUs: "-1"}, {CPUs: "NaN"}, {CPUs: "+Inf"}, {PIDs: "1.5"}, {Env: `A="unclosed`},
	} {
		if _, err := bad.overrides(); err == nil {
			t.Errorf("%+v was read", bad)
		}
	}
}

// between is the text of page between before and after.
// keptWhole is the markup that keeps a name or a figure whole on a line.
var keptWhole = regexp.MustCompile(`<span class="nowrap">([^<]*)</span>`)

// said is a page as its words read: without what keeps a name ("fsn-1")
// or a figure ("1.4 GB") whole on its line.
func said(page string) string {
	return strings.ReplaceAll(keptWhole.ReplaceAllString(page, "$1"), "\u00a0", " ")
}

func between(page, before, after string) string {
	_, rest, ok := strings.Cut(page, before)
	if !ok {
		return ""
	}
	text, _, _ := strings.Cut(rest, after)
	return html.UnescapeString(text)
}

func apiTenant(overrides spec.Overrides) api.Tenant { return api.Tenant{Overrides: overrides} }

// An archived tenant's secrets are kept as they are, and an app that one
// runs is not offered for deletion.
func TestArchivedTenantsInSettings(t *testing.T) {
	_, server, client := newAppsPanel(t)
	if _, page := get(t, server.URL+"/tenants/old-demo/settings"); strings.Contains(page, `value="rotate"`) || !strings.Contains(page, "once it is restored") {
		t.Fatalf("the settings of an archived tenant:\n%s", page)
	}
	if _, page := get(t, server.URL+"/apps/web"); !strings.Contains(page, `href="/tenants/old-demo"`) || strings.Contains(page, `value="delete"`) {
		t.Fatalf("the page of an app an archived tenant runs:\n%s", page)
	}
	// Too large a form is refused as such.
	response, _ := submit(t, client, server, "/apps", url.Values{"name": {"big"}, "file": {strings.Repeat("#", formLimit)}}, server.URL)
	if response.StatusCode != http.StatusRequestEntityTooLarge {
		t.Fatalf("too large a form: %d", response.StatusCode)
	}
}

func TestUpdatingAnAppAndGoingBack(t *testing.T) {
	cluster, server, client := newAppsPanel(t)
	ctx := context.Background()
	agent := cluster.AsApps("test")
	_, page := get(t, server.URL+"/apps/web")
	updated := between(page, `name="updated" value="`, `"`)
	if !strings.Contains(page, "A newer image of") || !strings.Contains(page, "Review the update") || !strings.Contains(page, "Review going back") {
		t.Fatalf("the app's page:\n%s", page)
	}
	_, body := submit(t, client, server, "/apps/web", url.Values{"do": {"check"}}, server.URL)
	if !strings.Contains(body, "a newer image of ghcr.io/example/web:stable is in its registry: sha256:5e5e5e5e5e5e") {
		t.Fatalf("after checking:\n%s", body)
	}
	before, _ := agent.App(ctx, "web")
	// An update over a page read before a change is refused.
	_, body = submit(t, client, server, "/apps/web", url.Values{"do": {"update"}, "updated": {"2001-01-01T00:00:00Z"}}, server.URL)
	if !strings.Contains(body, "someone changed the app since you opened it") {
		t.Fatalf("a stale update:\n%s", body)
	}
	_, body = submit(t, client, server, "/apps/web", url.Values{"do": {"update"}, "updated": {updated}}, server.URL)
	if !strings.Contains(body, "app web is changed; it rolls out in waves to 6 tenants") || strings.Contains(body, "A newer image of") {
		t.Fatalf("after updating:\n%s", body)
	}
	after, _ := agent.App(ctx, "web")
	if after.Spec.Image != before.Latest || after.Latest != "" || after.Revision != 4 {
		t.Fatalf("updated app %+v", after)
	}
	if tenant, _ := cluster.Get(ctx, "acme-logistics"); tenant.Spec.Image != before.Latest {
		t.Fatalf("acme-logistics runs %s", tenant.Spec.Image)
	}
	// Back to revision 2.
	updated = between(body, `name="updated" value="`, `"`)
	// At once, as the review may choose: every tenant moves now.
	_, body = submit(t, client, server, "/apps/web", url.Values{"do": {"restore"}, "revision": {"2"}, "updated": {updated}, "pace": {"now"}}, server.URL)
	if !strings.Contains(body, "app web is changed; it rolls out to 6 tenants") {
		t.Fatalf("after going back:\n%s", body)
	}
	if tenant, _ := cluster.Get(ctx, "baltic-freight"); tenant.AppRevision != 0 || !strings.HasSuffix(tenant.Spec.Image, strings.Repeat("a1", 32)) {
		t.Fatalf("baltic-freight after going back at once: revision %d, %s", tenant.AppRevision, tenant.Spec.Image)
	}
	if back, _ := agent.App(ctx, "web"); back.Revision != 5 || !strings.HasSuffix(back.Spec.Image, strings.Repeat("a1", 32)) {
		t.Fatalf("app after going back %+v", back)
	}
	// An app given pinned has nothing to check.
	_, page = get(t, server.URL+"/apps/testapp")
	if strings.Contains(page, `value="check"`) || !strings.Contains(page, "no tag: the image was given pinned") {
		t.Fatalf("a pinned app's page:\n%s", page)
	}
	_, body = submit(t, client, server, "/apps/testapp", url.Values{"do": {"check"}}, server.URL)
	if !strings.Contains(body, "nothing to ask its registry about") {
		t.Fatalf("checking a pinned app:\n%s", body)
	}
}

func TestARolloutFromThePanel(t *testing.T) {
	cluster, server, client := newAppsPanel(t)
	ctx := context.Background()
	agent := cluster.AsApps("test")
	_, page := get(t, server.URL+"/apps/web")
	if !strings.Contains(page, "Rollout of revision 3") || !strings.Contains(page, "Blocked:</span> baltic-freight moved in wave 2 (a tenth)") || !strings.Contains(page, `value="pause"`) || !strings.Contains(page, `value="promote"`) {
		t.Fatalf("the app's page:\n%s", page)
	}
	_, body := submit(t, client, server, "/apps/web", url.Values{"do": {"pause"}}, server.URL)
	if !strings.Contains(body, "the rollout is paused") || !strings.Contains(body, `value="resume"`) {
		t.Fatalf("after pausing:\n%s", body)
	}
	// Restore warns of what it loses, and needs the name too.
	if !strings.Contains(body, `value="restore-data"`) || !strings.Contains(body, "what it wrote since is lost") {
		t.Fatalf("no restore on the paused rollout:\n%s", body)
	}
	_, body = submit(t, client, server, "/apps/web", url.Values{"do": {"restore-data"}, "confirm": {"we"}}, server.URL)
	if !strings.Contains(body, "type the app&#39;s name to take its tenants back with their data") {
		t.Fatalf("restored without the name:\n%s", body)
	}
	_, body = submit(t, client, server, "/apps/web", url.Values{"do": {"restore-data"}, "confirm": {"web"}}, server.URL)
	if app, _ := agent.App(ctx, "web"); app.Rollout.State != "paused" || !strings.Contains(body, "go back to the revision before with their data from before they moved") {
		t.Fatalf("after restoring: %+v\n%s", app.Rollout, body)
	}
	_, body = submit(t, client, server, "/apps/web", url.Values{"do": {"abort"}, "confirm": {"we"}}, server.URL)
	if !strings.Contains(body, "type the app&#39;s name") {
		t.Fatalf("aborted without the name:\n%s", body)
	}
	_, body = submit(t, client, server, "/apps/web", url.Values{"do": {"abort"}, "confirm": {"web"}}, server.URL)
	if app, _ := agent.App(ctx, "web"); app.Rollout.State != "aborted" || strings.Contains(body, `value="pause"`) {
		t.Fatalf("after aborting: %+v\n%s", app.Rollout, body)
	}
	// A tenant's flags.
	_, body = submit(t, client, server, "/tenants/atlas/settings", url.Values{"do": {"flags"}, "canary": {"on"}}, server.URL)
	if !strings.Contains(body, "atlas takes rollouts first: it is a canary") {
		t.Fatalf("after the flags:\n%s", body)
	}
	if tenant, _ := agent.Get(ctx, "atlas"); !tenant.Canary || tenant.Hold {
		t.Fatalf("atlas %+v", tenant)
	}
}

// A tenant still waiting for its data once its app's rollout is done is
// stopped: the app's page still offers to start it as it is.
func TestATenantWaitingForItsDataCanBeStartedAsItIs(t *testing.T) {
	cluster, server, _ := newAppsPanel(t)
	cluster.mu.Lock()
	tenant := cluster.tenants["atlas"]
	tenant.Restoring = true
	cluster.tenants["atlas"] = tenant
	app := cluster.apps["web"]
	done := *app.Rollout
	done.State = store.RolloutDone
	app.Rollout = &done
	cluster.apps["web"] = app
	cluster.mu.Unlock()
	_, page := get(t, server.URL+"/apps/web")
	if !strings.Contains(page, "atlas waits for their data from before the rollout") || !strings.Contains(page, `value="abort"`) || strings.Contains(page, `value="pause"`) {
		t.Fatalf("the app's page:\n%s", page)
	}
}

// reviewed reviews a settings form and returns it as its review's Save
// sends it: with the token that binds it to the values reviewed.
func reviewed(t *testing.T, client *http.Client, server *httptest.Server, path string, form url.Values) url.Values {
	t.Helper()
	review := url.Values{}
	for key, values := range form {
		review[key] = values
	}
	review.Set("do", "review")
	_, body := submit(t, client, server, path, review, server.URL)
	token := between(body, `name="reviewed" value="`, `"`)
	if token == "" {
		t.Fatalf("no review:\n%s", body)
	}
	saved := url.Values{}
	for key, values := range form {
		saved[key] = values
	}
	saved.Set("do", "save")
	saved.Set("reviewed", token)
	return saved
}
