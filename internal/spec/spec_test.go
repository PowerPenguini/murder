package spec

import (
	"errors"
	"strings"
	"testing"

	"gopkg.in/yaml.v3"
)

const digest = "sha256:0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef"

func validTenant() Tenant {
	return Tenant{
		Name:    "acme",
		Image:   "ghcr.io/example/app@" + digest,
		User:    "10001:10002",
		Env:     map[string]string{"PUBLIC_URL": "{{url}}", "TRUSTED_PROXIES": "{{network}}"},
		Secrets: Secrets{"SESSION_SECRET": {}},
	}.WithDefaults()
}

func TestDefaultsFillTheGaps(t *testing.T) {
	tenant := validTenant()
	if err := tenant.Validate(); err != nil {
		t.Fatal(err)
	}
	if tenant.Port != 8080 || tenant.HealthPath != "/" || tenant.DataPath != "/data" || tenant.WritableRoot || tenant.Tmpfs["/tmp"] != DefaultTmpfs {
		t.Fatalf("defaults = %+v", tenant)
	}
	if secret := tenant.Secrets["SESSION_SECRET"]; secret.Kind != "hex" || secret.Bytes != 32 {
		t.Fatalf("default secret = %+v", secret)
	}
	if uid, gid := tenant.Owner(); uid != 10001 || gid != 10002 {
		t.Fatalf("owner = %d:%d", uid, gid)
	}
	explicit := Tenant{Port: 9000, HealthPath: "/h", DataPath: "/srv", Tmpfs: map[string]string{}, Secrets: Secrets{"P": {Kind: "password"}}}.WithDefaults()
	if explicit.Port != 9000 || explicit.HealthPath != "/h" || explicit.DataPath != "/srv" || len(explicit.Tmpfs) != 0 || explicit.Secrets["P"].Bytes != 20 {
		t.Fatalf("explicit values were overridden: %+v", explicit)
	}
	if again := tenant.WithDefaults(); !again.Equal(tenant) {
		t.Fatal("WithDefaults is not idempotent")
	}
}

func TestSecretsInYAML(t *testing.T) {
	var app struct {
		Secrets Secrets `yaml:"secrets"`
	}
	for document, want := range map[string]Secrets{
		"secrets: [A, B]":                      {"A": {Kind: "hex"}, "B": {Kind: "hex"}},
		"secrets: {A: password, B: base64}":    {"A": {Kind: "password"}, "B": {Kind: "base64"}},
		"secrets: {A: {kind: hex, bytes: 64}}": {"A": {Kind: "hex", Bytes: 64}},
	} {
		app.Secrets = nil
		if err := yaml.Unmarshal([]byte(document), &app); err != nil {
			t.Fatalf("%s: %v", document, err)
		}
		if len(app.Secrets) != len(want) {
			t.Fatalf("%s: %v", document, app.Secrets)
		}
		for name, secret := range want {
			if app.Secrets[name] != secret {
				t.Errorf("%s: %s = %+v, want %+v", document, name, app.Secrets[name], secret)
			}
		}
	}
	for _, document := range []string{"secrets: [A, A]", "secrets: A", "secrets: {A: {kind: hex, size: 3}}"} {
		app.Secrets = nil
		decoder := yaml.NewDecoder(strings.NewReader(document))
		decoder.KnownFields(true)
		if err := decoder.Decode(&app); err == nil {
			t.Errorf("%q accepted", document)
		}
	}
}

func TestValidateReportsEveryProblem(t *testing.T) {
	tenant := validTenant()
	tenant.Name = "Acme_1"
	tenant.Image = "ghcr.io/example/app:latest"
	tenant.Port = 70000
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
		`tenant name "Acme_1"`, "must be pinned", "port 70000", `health path "health"`, `data path "/data/../etc"`,
		`env name "BAD-NAME"`, "unknown placeholder {{nope}}", "PUBLIC_URL is both env and secret",
		`unknown kind "rot13"`, "needs 16-128 bytes", `tmpfs path "/data/../etc"`,
	} {
		if !strings.Contains(err.Error(), want) {
			t.Errorf("error lacks %q:\n%v", want, err)
		}
	}
}

func TestUserMustBeNumericAndNotRoot(t *testing.T) {
	for _, user := range []string{"10001:10001", "1:1", "4294967:65534", "2147483647:2147483647"} {
		tenant := validTenant()
		tenant.User = user
		if err := tenant.Validate(); err != nil {
			t.Errorf("user %q refused: %v", user, err)
		}
	}
	for _, user := range []string{"", "0:0", "10001:0", "0:10001", "00:1", "+0:1", "1:00", "root", "app:app", "10001", " 1:1", "1:1 ", "4294967295:10001", "10001:2147483648"} {
		tenant := validTenant()
		tenant.User = user
		if tenant.Validate() == nil {
			t.Errorf("user %q accepted", user)
		}
	}
}

func TestImageReferences(t *testing.T) {
	for _, ref := range []string{"nginx", "nginx:1.27", "ghcr.io/org/app:v1.4.2", "registry.local:5000/team/app", "ghcr.io/org/app@" + digest, digest} {
		if err := ValidReference(ref); err != nil {
			t.Errorf("ValidReference(%q) = %v", ref, err)
		}
	}
	for _, ref := range []string{"", "Nginx", "nginx:", "nginx:-bad", "a b", "ghcr.io/org/app@sha256:short"} {
		if ValidReference(ref) == nil {
			t.Errorf("ValidReference(%q) accepted", ref)
		}
	}
	if Pinned("nginx:1.27") || !Pinned(digest) || !Pinned("nginx@"+digest) {
		t.Fatal("Pinned misjudges references")
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
	env := tenant.Render(Values{Host: "acme.example.com", URL: "https://acme.example.com", Network: "10.240.0.0/28"})
	want := map[string]string{"PUBLIC_URL": "https://acme.example.com", "TRUSTED_PROXIES": "10.240.0.0/28", "MIXED": "acme at acme.example.com"}
	for key, value := range want {
		if env[key] != value {
			t.Errorf("%s = %q, want %q", key, env[key], value)
		}
	}
	if tenant.Env["PUBLIC_URL"] != "{{url}}" {
		t.Error("Render changed the spec")
	}
}
