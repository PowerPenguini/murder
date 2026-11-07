package apiserver

import (
	"context"
	"encoding/json"
	"errors"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/PowerPenguini/murder/internal/api"
	"github.com/PowerPenguini/murder/internal/store"
)

// The last lines of a tenant's previous container are served read-only
// by the node that runs it: none until a container went, those of the
// suspended one after a suspension. Once the app exited and its container
// was started again in place, the current lines are those after the exit.
func TestPreviousContainersLines(t *testing.T) {
	f := withApp(t)
	ctx := context.Background()
	if _, err := f.client.Create(ctx, tenantOf("acme")); err != nil {
		t.Fatal(err)
	}
	_ = f.client.Reconcile(ctx)
	var refused *api.Error
	if _, err := f.client.PreviousLogs(ctx, "acme", recorded); !errors.As(err, &refused) || refused.Status != 404 || !strings.Contains(err.Error(), "no previous container's lines are kept on node test-node") {
		t.Fatalf("before any container went: %v", err)
	}
	if _, err := f.client.Suspend(ctx, "acme", true); err != nil {
		t.Fatal(err)
	}
	_ = f.client.Reconcile(ctx)
	if _, err := f.client.Suspend(ctx, "acme", false); err != nil {
		t.Fatal(err)
	}
	_ = f.client.Reconcile(ctx)
	previous, err := f.client.PreviousLogs(ctx, "acme", recorded)
	if err != nil || previous.Why != "stopped" || previous.Container != "c-acme" || previous.Lines != "logs of c-acme\n" || previous.At.IsZero() {
		t.Fatalf("after a suspension: %+v, %v", previous, err)
	}
	if logs, err := f.client.Logs(ctx, "acme", 10, recorded); err != nil || string(logs) != "logs of c-acme\n" {
		t.Fatalf("a new container's lines: %q, %v", logs, err)
	}

	// Its app exited and the same container started again: kept as the
	// reconciler keeps it.
	exited := time.Date(2026, 11, 4, 15, 20, 0, 0, time.UTC)
	data, _ := json.Marshal(map[string]any{"tenant": "acme", "container": "c-acme", "why": "exited", "exitedAt": exited,
		"exitKnown": true, "exitCode": 137, "oomKilled": true, "memoryMB": 256, "at": exited, "lines": []byte("ERROR cannot allocate memory\n")})
	if err := os.WriteFile(filepath.Join(f.previousDir, "acme.json"), data, 0o600); err != nil {
		t.Fatal(err)
	}
	previous, err = f.client.PreviousLogs(ctx, "acme", recorded)
	if err != nil || previous.Why != "exited" || previous.ExitCode != 137 || !previous.OOMKilled || !previous.ExitedAt.Equal(exited) || previous.Lines != "ERROR cannot allocate memory\n" {
		t.Fatalf("after an exit: %+v, %v", previous, err)
	}
	if logs, err := f.client.Logs(ctx, "acme", 10, recorded); err != nil || string(logs) != "logs of c-acme since 2026-11-04T15:20:00Z\n" {
		t.Fatalf("the current run's lines: %q, %v", logs, err)
	}

	// Another node's tenant: read there.
	if err := f.store.PutNode(store.Node{Name: "other-node", Address: "10.0.0.2:7400"}, "test"); err != nil {
		t.Fatal(err)
	}
	if _, err := f.store.Create(store.NewTenant{Name: "away", App: "web", Node: "other-node"}, "test"); err != nil {
		t.Fatal(err)
	}
	if _, err := f.client.PreviousLogs(ctx, "away", recorded); err == nil || !strings.Contains(err.Error(), "runs on node other-node") {
		t.Fatalf("the previous lines of a tenant elsewhere: %v", err)
	}
}

// An app change in waves can name tenants to move at once, first, through
// the API: ?first=a,b.
func TestAnAppChangeMovesSomeFirst(t *testing.T) {
	f := withApp(t)
	ctx := context.Background()
	for _, name := range []string{"a", "b"} {
		if _, err := f.client.Create(ctx, tenantOf(name)); err != nil {
			t.Fatal(err)
		}
	}
	next := appSpec()
	next.Limits.MemoryMB = 512
	applied, err := f.client.First([]string{"b"}).PutApp(ctx, "web", next)
	if err != nil || !applied.Rollout {
		t.Fatalf("applied %+v, %v", applied, err)
	}
	b, _ := f.store.Get("b")
	a, _ := f.store.Get("a")
	if b.AppRevision != 0 || b.BeforeRevision == 0 || a.AppRevision == 0 {
		t.Fatalf("a %+v / b %+v", a, b)
	}
}
