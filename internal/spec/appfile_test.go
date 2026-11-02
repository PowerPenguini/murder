package spec

import (
	"reflect"
	"strings"
	"testing"

	"github.com/PowerPenguini/murder/internal/strictyaml"
)

// An app's file reads back as the same app, and names no tenant.
func TestAnAppFileReadsBack(t *testing.T) {
	app := Tenant{
		Name: "ignored", Image: "ghcr.io/example/web@sha256:" + strings.Repeat("a", 64), User: "10001:10001", Port: 8080,
		HealthPath: "/api/health", Env: map[string]string{"PUBLIC_URL": "{{url}}", "TZ": "Europe/Berlin"},
		Secrets: Secrets{"SESSION_SECRET": {Kind: "hex"}, "ADMIN_PASSWORD": {Kind: "password", Bytes: 24}},
		Limits:  Limits{MemoryMB: 512, CPUs: 1.5}, Tmpfs: map[string]string{"/tmp": "size=64m"}, Suspended: true,
	}
	file, err := AppFile(app)
	if err != nil {
		t.Fatal(err)
	}
	text := string(file)
	if strings.Contains(text, "name:") || strings.Contains(text, "suspended") || !strings.Contains(text, "SESSION_SECRET: hex") {
		t.Fatalf("the file:\n%s", text)
	}
	// With defaults filled in, as the agent stores apps, it says the same.
	if defaults, err := AppFile(app.WithDefaults()); err != nil || !strings.Contains(string(defaults), "SESSION_SECRET: hex\n") {
		t.Fatalf("with defaults: %v\n%s", err, defaults)
	}
	var back Tenant
	if err := strictyaml.Decode(file, &back); err != nil {
		t.Fatalf("%v\n%s", err, text)
	}
	app.Name, app.Suspended = "", false
	if !reflect.DeepEqual(back, app) {
		t.Fatalf("read back %+v\nwant %+v\nfrom:\n%s", back, app, text)
	}
}

// What the agent stores reads back from its file as the same app, so a
// save from the panel left as it was changes nothing.
func TestAStoredAppReadsBackUnchanged(t *testing.T) {
	image := "ghcr.io/example/web@sha256:" + strings.Repeat("b", 64)
	for _, app := range []Tenant{
		{Image: image, User: "1000:1000"},
		{Image: image, User: "1000:1000", Tmpfs: map[string]string{}},
		{Image: image, User: "1000:1000", WritableRoot: true, Tmpfs: map[string]string{"/cache": "size=1g"}},
		{Image: image, User: "1000:1000", Env: map[string]string{"CERT": "-----BEGIN-----\nAAA=\n-----END-----", "FLAG": "yes", "EMPTY": ""},
			Secrets: Secrets{"A": {Kind: "base64"}, "B": {Kind: "password", Bytes: 40}}, Limits: Limits{CPUs: 0.25}},
	} {
		stored := app.WithDefaults()
		file, err := AppFile(stored)
		if err != nil {
			t.Fatal(err)
		}
		var back Tenant
		if err := strictyaml.Decode(file, &back); err != nil {
			t.Fatalf("%v\n%s", err, file)
		}
		if !back.WithDefaults().Equal(stored) {
			t.Errorf("read back %+v\nstored %+v\nfrom:\n%s", back.WithDefaults(), stored, file)
		}
	}
}
