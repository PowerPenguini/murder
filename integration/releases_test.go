//go:build docker

package integration

import (
	"context"
	"io"
	"log/slog"
	"net/http"
	"os"
	"os/exec"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/PowerPenguini/murder/internal/api"
	"github.com/PowerPenguini/murder/internal/releases"
	"github.com/PowerPenguini/murder/internal/spec"
)

// startRegistry runs a registry on a free local port and returns its
// address.
func startRegistry(t *testing.T) string {
	t.Helper()
	output, err := exec.Command("docker", "run", "-d", "--rm", "-p", "127.0.0.1::5000", "registry:2").Output()
	if err != nil {
		t.Fatalf("start a registry: %v\n%s", err, output)
	}
	id := strings.TrimSpace(string(output))
	t.Cleanup(func() { _ = exec.Command("docker", "rm", "-f", id).Run() })
	output, err = exec.Command("docker", "port", id, "5000/tcp").CombinedOutput()
	if err != nil {
		t.Fatalf("the registry's port: %v\n%s", err, output)
	}
	address := strings.TrimSpace(strings.Split(string(output), "\n")[0])
	address = strings.Replace(address, "127.0.0.1", "localhost", 1)
	deadline := time.Now().Add(30 * time.Second)
	for {
		response, err := http.Get("http://" + address + "/v2/")
		if err == nil {
			response.Body.Close()
			return address
		}
		if time.Now().After(deadline) {
			t.Fatalf("the registry does not answer: %v", err)
		}
		time.Sleep(200 * time.Millisecond)
	}
}

func push(t *testing.T, ref string) {
	t.Helper()
	if output, err := exec.Command("docker", "push", "-q", ref).CombinedOutput(); err != nil {
		t.Fatalf("push %s: %v\n%s", ref, err, output)
	}
}

// An app given a tag keeps its image until someone takes the newer one the
// registry holds; an earlier revision brings the old image back.
func TestAnAppIsUpdatedToItsTagsNewestImageOnDocker(t *testing.T) {
	const tenant = "it-release"
	cleanupTenant(t, tenant)
	registry := startRegistry(t)
	tag := registry + "/murder-it/web:stable"
	buildTestImage(t, "v1", tag)
	push(t, tag)
	n := newNode(t, tenant)
	ctx := context.Background()

	uid, gid := strconv.Itoa(os.Getuid()), strconv.Itoa(os.Getgid())
	app := spec.Tenant{Image: tag, HealthPath: "/health", User: uid + ":" + gid, Env: map[string]string{"DATA_DIR": "/data"}, Limits: spec.Limits{MemoryMB: 64, CPUs: 0.5, PIDs: 64}}
	stored, err := n.client.PutApp(ctx, "it-release", app)
	if err != nil {
		t.Fatal(err)
	}
	v1 := stored.App.Spec.Image
	if stored.App.Tag != tag || !strings.HasPrefix(v1, registry+"/murder-it/web@sha256:") || stored.Note != "" {
		t.Fatalf("stored %+v", stored)
	}
	if _, err := n.client.Create(ctx, api.TenantRequest{Name: tenant, App: "it-release"}); err != nil {
		t.Fatal(err)
	}
	n.waitHealthy(t, n.reconciler, tenant)
	variant := func() string {
		_, body := n.request(t, http.MethodGet, tenant, "/info", "")
		return body
	}
	if !strings.Contains(variant(), `"variant":"v1"`) {
		t.Fatalf("the app runs %s", variant())
	}

	// A new image is pushed under the same tag; the leader's check finds it.
	buildTestImage(t, "v2", tag)
	push(t, tag)
	watcher := &releases.Watcher{Store: n.store, Registry: n.docker, Every: time.Minute, Log: slog.New(slog.NewTextHandler(io.Discard, nil))}
	watcher.Check(ctx)
	found, err := n.client.App(ctx, "it-release")
	if err != nil || found.Latest == "" || found.Latest == v1 || len(watcher.Failing()) != 0 {
		t.Fatalf("after the check: %+v, %v, failing %v", found, err, watcher.Failing())
	}
	// Changing the spec does not take it by the way.
	app.Env["TZ"] = "UTC"
	changed, err := n.client.PutApp(ctx, "it-release", app)
	if err != nil || changed.App.Spec.Image != v1 || !strings.Contains(changed.Note, "murder app update") {
		t.Fatalf("a change of env %+v, %v", changed, err)
	}
	if check, err := n.client.CheckApp(ctx, "it-release"); err != nil || !check.Newer || check.Image != found.Latest {
		t.Fatalf("check %+v, %v", check, err)
	}
	updated, err := n.client.UpdateApp(ctx, "it-release", time.Time{})
	if err != nil || updated.App.Spec.Image != found.Latest || updated.App.Latest != "" || len(updated.Tenants) != 1 {
		t.Fatalf("update %+v, %v", updated, err)
	}
	n.waitHealthy(t, n.reconciler, tenant)
	if !strings.Contains(variant(), `"variant":"v2"`) {
		t.Fatalf("after the update the app runs %s", variant())
	}

	// Back to the first revision: v1, by its digest.
	back, err := n.client.RestoreRevision(ctx, "it-release", 1, updated.App.UpdatedAt)
	if err != nil || back.App.Spec.Image != v1 || back.App.Tag != tag {
		t.Fatalf("back %+v, %v", back, err)
	}
	n.waitHealthy(t, n.reconciler, tenant)
	if !strings.Contains(variant(), `"variant":"v1"`) {
		t.Fatalf("after going back the app runs %s", variant())
	}
	// The registry still holds v2: the next check says so again.
	watcher.Check(ctx)
	if again, _ := n.client.App(ctx, "it-release"); again.Latest != found.Latest {
		t.Fatalf("after going back the newer image is %q", again.Latest)
	}
	if err := n.client.Archive(ctx, tenant); err != nil {
		t.Fatal(err)
	}
}
