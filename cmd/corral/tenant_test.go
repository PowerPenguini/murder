package main

import (
	"flag"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"testing"
)

const appYAML = `image: ghcr.io/example/app@sha256:0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef
port: 8080
healthPath: /api/health
dataPath: /data
user: "10001:10001"
dataUID: 10001
dataGID: 10001
env:
  PUBLIC_URL: "{{url}}"
secrets:
  SESSION_SECRET: {kind: hex, bytes: 32}
readOnlyRoot: true
tmpfs:
  /tmp: rw,noexec,nosuid,size=256m
limits: {memoryMB: 512}
`

func TestLoadSpecNamesTheApp(t *testing.T) {
	path := filepath.Join(t.TempDir(), "app.yaml")
	if err := os.WriteFile(path, []byte(appYAML), 0o600); err != nil {
		t.Fatal(err)
	}
	tenant, err := loadSpec(path, "acme", "")
	if err != nil {
		t.Fatal(err)
	}
	if tenant.Name != "acme" || tenant.Port != 8080 || tenant.Secrets["SESSION_SECRET"].Bytes != 32 || tenant.Limits.MemoryMB != 512 || tenant.Tmpfs["/tmp"] == "" {
		t.Fatalf("spec = %+v", tenant)
	}
	override := "sha256:fedcba9876543210fedcba9876543210fedcba9876543210fedcba9876543210"
	if tenant, err := loadSpec(path, "beta", override); err != nil || tenant.Image != override {
		t.Fatalf("image override = %+v, %v", tenant, err)
	}
	named := filepath.Join(t.TempDir(), "named.yaml")
	_ = os.WriteFile(named, []byte("name: other\n"+appYAML), 0o600)
	if _, err := loadSpec(named, "acme", ""); err == nil || !strings.Contains(err.Error(), `"other"`) {
		t.Fatalf("mismatched name = %v", err)
	}
	typo := filepath.Join(t.TempDir(), "typo.yaml")
	_ = os.WriteFile(typo, []byte(appYAML+"helthPath: /x\n"), 0o600)
	if _, err := loadSpec(typo, "acme", ""); err == nil {
		t.Fatal("an unknown field was accepted")
	}
	if _, err := loadSpec("", "acme", ""); err == nil {
		t.Fatal("missing -file accepted")
	}
}

func TestFlagsMayFollowArguments(t *testing.T) {
	flags := flag.NewFlagSet("t", flag.ContinueOnError)
	file := flags.String("file", "", "")
	yes := flags.Bool("yes", false, "")
	positional, err := parseInterspersed(flags, []string{"acme", "-file", "app.yaml", "extra", "-yes"})
	if err != nil {
		t.Fatal(err)
	}
	if !slices.Equal(positional, []string{"acme", "extra"}) || *file != "app.yaml" || !*yes {
		t.Fatalf("positional=%v file=%q yes=%v", positional, *file, *yes)
	}
}

func TestExampleAppSpecLoads(t *testing.T) {
	if _, err := loadSpec(filepath.Join("..", "..", "examples", "app.example.yaml"), "acme", ""); err != nil {
		t.Fatalf("examples/app.example.yaml: %v", err)
	}
}
