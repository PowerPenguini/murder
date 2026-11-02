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
	if _, page := get(t, server.URL+"/apps"); !strings.Contains(page, `href="/apps/addhoc"`) || !strings.Contains(page, "create the app") {
		t.Fatalf("the apps page:\n%s", page)
	}

	// Refused, the form comes back as it was typed, saying why.
	response, body := submit(t, client, server, "/apps", url.Values{"name": {"web"}, "file": {"image: x\nuser: root\nbogus: 1\n"}}, server.URL)
	if response.StatusCode != http.StatusUnprocessableEntity || !strings.Contains(body, "bogus") || !strings.Contains(body, "user: root") {
		t.Fatalf("a bad spec: %d\n%s", response.StatusCode, body)
	}
	_, body = submit(t, client, server, "/apps", url.Values{"name": {"Not A Label"}, "file": {testAppFile}}, server.URL)
	if !strings.Contains(body, "the app&#39;s name") {
		t.Fatalf("a bad name:\n%s", body)
	}
	_, body = submit(t, client, server, "/apps", url.Values{"name": {"addhoc"}, "file": {testAppFile}}, server.URL)
	if !strings.Contains(body, "app addhoc exists already") {
		t.Fatalf("an app of the name stored already was replaced:\n%s", body)
	}

	response, body = submit(t, client, server, "/apps", url.Values{"name": {"web"}, "file": {testAppFile}}, server.URL)
	if response.Request.URL.Path != "/apps/web" || !strings.Contains(body, "app web is created") || !strings.Contains(body, "SESSION_SECRET: hex") {
		t.Fatalf("after creating web (%s):\n%s", response.Request.URL, body)
	}
	if events, _ := cluster.Events(ctx, store.EventQuery{App: "web", Limit: 1}); len(events) != 1 || events[0].Action != "app.created" || events[0].Actor != "murder-panel socket" {
		t.Fatalf("the audit log: %+v", events)
	}
	if _, page := get(t, server.URL+"/events?app=web"); !strings.Contains(page, "app.created") || strings.Contains(page, "tenant.suspended") {
		t.Fatalf("the events of app web:\n%s", page)
	}
}

func TestEditingAnApp(t *testing.T) {
	cluster, server, client := newAppsPanel(t)
	ctx := context.Background()
	_, page := get(t, server.URL+"/apps/addhoc")
	updated := between(page, `name="updated" value="`, `"`)
	file := between(page, `aria-label="the app's spec">`, "</textarea>")
	if updated == "" || !strings.Contains(file, "TZ: Europe/Warsaw") {
		t.Fatalf("the app's page:\n%s", page)
	}
	changed := strings.Replace(file, "TZ: Europe/Warsaw", "TZ: Europe/Paris", 1)

	// A save over a change made since the page was read is refused, the
	// text kept.
	_, body := submit(t, client, server, "/apps/addhoc", url.Values{"do": {"save"}, "updated": {"2001-01-01T00:00:00Z"}, "file": {changed}}, server.URL)
	if !strings.Contains(body, "someone changed the app since") || !strings.Contains(body, "Europe/Paris") {
		t.Fatalf("a stale save:\n%s", body)
	}
	_, body = submit(t, client, server, "/apps/addhoc", url.Values{"do": {"save"}, "updated": {updated}, "file": {changed}}, server.URL)
	if !strings.Contains(body, "app addhoc is changed; it rolls out to 7 tenants") {
		t.Fatalf("after saving:\n%s", body)
	}
	if tenant, _ := cluster.Get(ctx, "acme-logistics"); tenant.Spec.Env["TZ"] != "Europe/Paris" {
		t.Fatalf("acme-logistics runs %v", tenant.Spec.Env)
	}
	// A tenant's own settings stay its own.
	if tenant, _ := cluster.Get(ctx, "iqtms"); tenant.Spec.Env["TZ"] != "Europe/Berlin" {
		t.Fatalf("iqtms runs %v", tenant.Spec.Env)
	}

	// An app tenants run is not deleted; one without is, its name typed.
	_, body = submit(t, client, server, "/apps/addhoc", url.Values{"do": {"delete"}, "confirm": {"addhoc"}}, server.URL)
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
	_, page := get(t, server.URL+"/tenants/iqtms/settings")
	for _, want := range []string{"FEATURE_PLANNER=on", "ADMIN_PASSWORD password 20", `value="768"`, "SESSION_KEY", "app addhoc"} {
		if !strings.Contains(page, want) {
			t.Fatalf("the settings page lacks %q:\n%s", want, page)
		}
	}
	before, _ := cluster.Get(ctx, "iqtms")
	form := url.Values{"do": {"save"}, "app": {"addhoc"}, "generation": {strconv.FormatInt(before.Generation, 10)},
		"env": {"TZ=Europe/Warsaw\nbroken"}, "secrets": {"ADMIN_PASSWORD password 20\nAPI_TOKEN base64 48"}, "memoryMB": {"1024"}, "cpus": {"1.5"}}

	// A form that does not read is shown again, as typed.
	_, body := submit(t, client, server, "/tenants/iqtms/settings", form, server.URL)
	if !strings.Contains(body, "env line 2: NAME=value") || !strings.Contains(body, "API_TOKEN base64 48") {
		t.Fatalf("a bad env line:\n%s", body)
	}
	form.Set("env", "TZ=Europe/Warsaw\n\n# the planner is on for everyone now")
	stale := url.Values{}
	for key, values := range form {
		stale[key] = values
	}
	stale.Set("generation", "1")
	if _, body = submit(t, client, server, "/tenants/iqtms/settings", stale, server.URL); !strings.Contains(body, "someone changed iqtms since") {
		t.Fatalf("a stale save:\n%s", body)
	}

	_, body = submit(t, client, server, "/tenants/iqtms/settings", form, server.URL)
	if !strings.Contains(body, "iqtms&#39;s settings are changed") {
		t.Fatalf("after saving:\n%s", body)
	}
	after, _ := cluster.Get(ctx, "iqtms")
	want := spec.Overrides{Env: map[string]string{"TZ": "Europe/Warsaw"}, Secrets: spec.Secrets{"ADMIN_PASSWORD": {Kind: "password", Bytes: 20}, "API_TOKEN": {Kind: "base64", Bytes: 48}},
		Limits: spec.Limits{MemoryMB: 1024, CPUs: 1.5}}
	if !after.Overrides.Equal(want) || after.Spec.Env["FEATURE_PLANNER"] != "" || after.Spec.Limits.MemoryMB != 1024 || after.Generation != before.Generation+1 {
		t.Fatalf("iqtms after saving: %+v, env %v, generation %d", after.Overrides, after.Spec.Env, after.Generation)
	}

	// A secret gets a new value with its name typed; its value is never
	// on the page.
	_, body = submit(t, client, server, "/tenants/iqtms/settings", url.Values{"do": {"rotate"}, "key": {"API_TOKEN"}, "confirm": {"api"}}, server.URL)
	if !strings.Contains(body, "type the secret&#39;s name") {
		t.Fatalf("rotated without the name typed:\n%s", body)
	}
	_, body = submit(t, client, server, "/tenants/iqtms/settings", url.Values{"do": {"rotate"}, "key": {"API_TOKEN"}, "confirm": {"API_TOKEN"}}, server.URL)
	if !strings.Contains(body, "API_TOKEN has a new value") {
		t.Fatalf("after rotating:\n%s", body)
	}
	if events, _ := cluster.Events(ctx, store.EventQuery{Tenant: "iqtms", Limit: 1}); events[0].Action != "tenant.secret-rotated" || events[0].Detail != "API_TOKEN" {
		t.Fatalf("the audit log: %+v", events[0])
	}
}

func TestReadingSettings(t *testing.T) {
	form := settingsForm{Env: "A=1\r\nB = two=2\n#C=3\n\n", Secrets: "X\nY password\nZ base64 16", MemoryMB: "512", CPUs: "0.5", PIDs: "", DiskGB: "20"}
	got, err := form.overrides()
	want := spec.Overrides{Env: map[string]string{"A": "1", "B": " two=2"}, Secrets: spec.Secrets{"X": {Kind: "hex"}, "Y": {Kind: "password"}, "Z": {Kind: "base64", Bytes: 16}},
		Limits: spec.Limits{MemoryMB: 512, CPUs: 0.5, DiskGB: 20}}
	if err != nil || !reflect.DeepEqual(got, want) {
		t.Fatalf("overrides = %+v, %v", got, err)
	}
	// Read back, the form says the same.
	if back, err := formOf(apiTenant(want)).overrides(); err != nil || !back.Equal(want) {
		t.Fatalf("read back: %+v, %v", back, err)
	}
	for _, bad := range []settingsForm{
		{Env: "A=1\nA=2"}, {Env: "=1"}, {Secrets: "X hex 0"}, {Secrets: "X hex 1 2"}, {Secrets: "X\nX"},
		{MemoryMB: "lots"}, {CPUs: "-1"}, {PIDs: "1.5"},
	} {
		if _, err := bad.overrides(); err == nil {
			t.Errorf("%+v was read", bad)
		}
	}
}

// between is the text of page between before and after.
func between(page, before, after string) string {
	_, rest, ok := strings.Cut(page, before)
	if !ok {
		return ""
	}
	text, _, _ := strings.Cut(rest, after)
	return html.UnescapeString(text)
}

func apiTenant(overrides spec.Overrides) api.Tenant { return api.Tenant{Overrides: overrides} }
