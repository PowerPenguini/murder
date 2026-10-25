package fleet

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func write(t *testing.T, dir, name, content string) string {
	t.Helper()
	path := filepath.Join(dir, name)
	if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(path, []byte(content), 0o600); err != nil {
		t.Fatal(err)
	}
	return path
}

func TestLoadComposesTenantsFromAppsAndOverrides(t *testing.T) {
	dir := t.TempDir()
	write(t, dir, "apps/web.yaml", `
image: ghcr.io/example/web:1.4
user: "10001:10001"
env: {MODE: prod, TZ: UTC}
secrets: [SESSION_SECRET]
limits: {memoryMB: 512, cpus: 1}
`)
	path := write(t, dir, "fleet.yaml", `
apps:
  web: apps/web.yaml
  demo: {image: "nginx:1.27", user: "101:101", port: 80}
tenants:
  beta: {app: web, env: {TZ: Europe/Berlin}, secrets: [EXTRA], limits: {memoryMB: 768}}
  acme: {app: web}
  canary: {app: web, image: "ghcr.io/example/web:1.5"}
  shop: {app: demo}
`)
	tenants, err := Load(path)
	if err != nil {
		t.Fatal(err)
	}
	names := []string{}
	for _, tenant := range tenants {
		names = append(names, tenant.Name)
	}
	if strings.Join(names, ",") != "acme,beta,canary,shop" {
		t.Fatalf("tenants = %v", names)
	}
	acme, beta, canary, shop := tenants[0], tenants[1], tenants[2], tenants[3]
	if acme.Image != "ghcr.io/example/web:1.4" || acme.Env["TZ"] != "UTC" || acme.Limits.MemoryMB != 512 {
		t.Errorf("acme = %+v", acme)
	}
	if beta.Env["TZ"] != "Europe/Berlin" || beta.Env["MODE"] != "prod" || beta.Limits.MemoryMB != 768 || beta.Limits.CPUs != 1 {
		t.Errorf("beta overrides = %+v", beta)
	}
	if _, ok := beta.Secrets["EXTRA"]; !ok || len(beta.Secrets) != 2 {
		t.Errorf("beta secrets = %v", beta.Secrets)
	}
	if len(acme.Secrets) != 1 || acme.Env["TZ"] != "UTC" {
		t.Error("an override leaked into another tenant of the same app")
	}
	if canary.Image != "ghcr.io/example/web:1.5" {
		t.Errorf("canary image = %q", canary.Image)
	}
	if shop.Port != 80 || shop.User != "101:101" {
		t.Errorf("inline app = %+v", shop)
	}
}

func TestLoadRejectsMistakes(t *testing.T) {
	dir := t.TempDir()
	write(t, dir, "named.yaml", "name: acme\nimage: nginx:1\nuser: \"1:1\"\n")
	write(t, dir, "typo.yaml", "image: nginx:1\nuser: \"1:1\"\nhelthPath: /x\n")
	for name, document := range map[string]string{
		"unknown app":      "apps: {}\ntenants: {acme: {app: web}}\n",
		"bad tenant name":  "apps: {web: {image: nginx, user: \"1:1\"}}\ntenants: {Acme: {app: web}}\n",
		"reserved name":    "apps: {web: {image: nginx, user: \"1:1\"}}\ntenants: {www: {app: web}}\n",
		"unknown field":    "apps: {web: {image: nginx, user: \"1:1\"}}\ntenants: {acme: {app: web, memory: 1}}\n",
		"inline typo":      "apps: {web: {image: nginx, user: \"1:1\", prot: 80}}\ntenants: {acme: {app: web}}\n",
		"named app":        "apps: {web: named.yaml}\ntenants: {acme: {app: web}}\n",
		"app file typo":    "apps: {web: typo.yaml}\ntenants: {acme: {app: web}}\n",
		"missing app file": "apps: {web: missing.yaml}\ntenants: {acme: {app: web}}\n",
		"top-level typo":   "apps: {}\ntenant: {}\n",
	} {
		if _, err := Load(write(t, dir, "fleet.yaml", document)); err == nil {
			t.Errorf("%s: accepted", name)
		}
	}
}

func TestExampleFleetLoads(t *testing.T) {
	tenants, err := Load(filepath.Join("..", "..", "examples", "fleet.yaml"))
	if err != nil {
		t.Fatalf("examples/fleet.yaml: %v", err)
	}
	if len(tenants) != 3 {
		t.Fatalf("example tenants = %d", len(tenants))
	}
	for _, tenant := range tenants {
		if err := tenant.WithDefaults().Validate(); err != nil && !strings.Contains(err.Error(), "must be pinned") {
			t.Errorf("%s: %v", tenant.Name, err)
		}
	}
}
