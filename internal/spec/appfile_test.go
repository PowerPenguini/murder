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
