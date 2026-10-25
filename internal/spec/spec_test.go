package spec

import (
	"errors"
	"strings"
	"testing"
)

const digest = "sha256:0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef"

func validTenant() Tenant {
	return Tenant{
		Name:         "acme",
		Image:        "ghcr.io/example/app@" + digest,
		Port:         8080,
		HealthPath:   "/api/health",
		DataPath:     "/data",
		DataUID:      10001,
		DataGID:      10001,
		User:         "10001:10001",
		Env:          map[string]string{"PUBLIC_URL": "{{url}}", "TRUSTED_PROXIES": "{{network}}"},
		Secrets:      map[string]Secret{"SESSION_SECRET": {Kind: "hex", Bytes: 32}},
		ReadOnlyRoot: true,
		Tmpfs:        map[string]string{"/tmp": "rw,noexec,nosuid,size=256m"},
	}
}

func TestValidTenantPasses(t *testing.T) {
	if err := validTenant().Validate(); err != nil {
		t.Fatal(err)
	}
	local := validTenant()
	local.Image = digest
	if err := local.Validate(); err != nil {
		t.Fatalf("a local image ID must be accepted: %v", err)
	}
}

func TestValidateReportsEveryProblem(t *testing.T) {
	tenant := validTenant()
	tenant.Name = "Acme_1"
	tenant.Image = "ghcr.io/example/app:latest"
	tenant.Port = 0
	tenant.HealthPath = "health"
	tenant.DataPath = "/data/../etc"
	tenant.Env["BAD-NAME"] = "x"
	tenant.Env["URL"] = "{{nope}}"
	tenant.Secrets["PUBLIC_URL"] = Secret{Kind: "hex", Bytes: 32}
	tenant.Secrets["SHORT"] = Secret{Kind: "rot13", Bytes: 4}
	tenant.Tmpfs["/data/../etc"] = ""
	err := tenant.Validate()
	var validation *ValidationError
	if !errors.As(err, &validation) {
		t.Fatalf("Validate() = %v, want a ValidationError", err)
	}
	for _, want := range []string{
		`tenant name "Acme_1"`, "must be pinned", "port 0", `health path "health"`, `data path "/data/../etc"`,
		`env name "BAD-NAME"`, "unknown placeholder {{nope}}", "PUBLIC_URL is both env and secret",
		`unknown kind "rot13"`, "needs 16-128 bytes", `tmpfs path "/data/../etc"`,
	} {
		if !strings.Contains(err.Error(), want) {
			t.Errorf("error lacks %q:\n%v", want, err)
		}
	}
}

func TestUserMustBeNumericAndNotRoot(t *testing.T) {
	for _, user := range []string{"10001:10001", "1:1", "4294967:65534"} {
		tenant := validTenant()
		tenant.User = user
		if err := tenant.Validate(); err != nil {
			t.Errorf("user %q refused: %v", user, err)
		}
	}
	for _, user := range []string{"", "0:0", "10001:0", "0:10001", "00:1", "+0:1", "1:00", "root", "app:app", "10001", " 1:1", "1:1 "} {
		tenant := validTenant()
		tenant.User = user
		if tenant.Validate() == nil {
			t.Errorf("user %q accepted", user)
		}
	}
}

func TestNames(t *testing.T) {
	for _, name := range []string{"a", "acme", "acme-2", strings.Repeat("a", 40)} {
		if err := ValidName(name); err != nil {
			t.Errorf("ValidName(%q) = %v", name, err)
		}
	}
	for _, name := range []string{"", "-a", "a-", "a.b", "A", "a_b", strings.Repeat("a", 41), "www", "corral"} {
		if ValidName(name) == nil {
			t.Errorf("ValidName(%q) accepted", name)
		}
	}
	if err := ValidLabel("www"); err != nil {
		t.Errorf("reserved names are still valid labels: %v", err)
	}
}

func TestRenderReplacesPlaceholders(t *testing.T) {
	tenant := validTenant()
	tenant.Env["MIXED"] = "{{name}} at {{host}}"
	env := tenant.Render(Values{Host: "acme.example.com", URL: "https://acme.example.com", Network: "172.20.0.0/16"})
	want := map[string]string{"PUBLIC_URL": "https://acme.example.com", "TRUSTED_PROXIES": "172.20.0.0/16", "MIXED": "acme at acme.example.com"}
	for key, value := range want {
		if env[key] != value {
			t.Errorf("%s = %q, want %q", key, env[key], value)
		}
	}
	if tenant.Env["PUBLIC_URL"] != "{{url}}" {
		t.Error("Render changed the spec")
	}
}
