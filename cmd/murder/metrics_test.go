package main

import (
	"io"
	"log/slog"
	"net/http/httptest"
	"net/netip"
	"path/filepath"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/PowerPenguini/murder/internal/api"
	"github.com/PowerPenguini/murder/internal/metrics"
	"github.com/PowerPenguini/murder/internal/reconcile"
	"github.com/PowerPenguini/murder/internal/runtime"
	"github.com/PowerPenguini/murder/internal/secrets"
	"github.com/PowerPenguini/murder/internal/spec"
	"github.com/PowerPenguini/murder/internal/store"
	"github.com/PowerPenguini/murder/internal/volume"
)

// unusedRuntime is a runtime no test calls.
type unusedRuntime struct{ runtime.Runtime }

func TestMetricsDescribeTheNode(t *testing.T) {
	dir := t.TempDir()
	keyPath := filepath.Join(dir, "master.key")
	if err := secrets.CreateKey(keyPath); err != nil {
		t.Fatal(err)
	}
	key, _ := secrets.LoadKey(keyPath)
	database := filepath.Join(dir, "murder.db")
	if err := store.Init(database, "node-1"); err != nil {
		t.Fatal(err)
	}
	st, err := store.Open(database, store.Options{NetworkPool: netip.MustParsePrefix("10.240.0.0/16"), Sealer: key.NewSealed})
	if err != nil {
		t.Fatal(err)
	}
	defer st.Close()
	app := spec.Tenant{Image: "sha256:" + strings.Repeat("a", 64), HealthPath: "/health", User: "1000:1000"}
	if _, err := st.PutApp(store.App{Name: "web", Spec: app}, "test"); err != nil {
		t.Fatal(err)
	}
	for _, name := range []string{"acme", "gone"} {
		if _, err := st.Create(store.NewTenant{Name: name, App: "web"}, "test"); err != nil {
			t.Fatal(err)
		}
	}
	if err := st.Archive("gone", "test"); err != nil {
		t.Fatal(err)
	}
	log := slog.New(slog.NewTextHandler(io.Discard, nil))
	reconciler := reconcile.New(st, unusedRuntime{}, volume.Dir{Root: dir}, key, reconcile.Config{BaseDomain: "example.test"}, log)
	edge := metrics.NewEdge()
	edge.Request("acme", 200, 30*time.Millisecond)
	edge.Request("gone", 200, time.Millisecond)   // archived: kept for its return
	edge.Request("purged", 200, time.Millisecond) // forgotten
	firewall := new(atomic.Int64)
	firewall.Store(time.Date(2026, 10, 29, 12, 0, 0, 0, time.UTC).UnixNano())
	handler := &agentMetrics{store: st, reconciler: reconciler, api: api.NewServer(st, reconciler, nil, key, log), edge: edge, firewall: firewall}

	recorder := httptest.NewRecorder()
	handler.ServeHTTP(recorder, httptest.NewRequest("GET", "/metrics", nil))
	text := recorder.Body.String()
	for _, want := range []string{
		`murder_edge_requests_total{tenant="acme",code="2xx"} 1`,
		`murder_edge_response_seconds_bucket{tenant="acme",le="0.05"} 1`,
		`murder_tenant_phase{tenant="acme",phase="pending"} 1`,
		`murder_tenant_phase{tenant="acme",phase="running"} 0`,
		`murder_tenant_healthy{tenant="acme"} 0`,
		`murder_firewall_loaded_timestamp_seconds 1.7932752e+09`,
		`murder_edge_requests_total{tenant="gone",code="2xx"} 1`,
	} {
		if !strings.Contains(text, want+"\n") {
			t.Errorf("missing %s", want)
		}
	}
	// An archived tenant runs nothing; nothing before a first pass, backup
	// or certificate is reported as if it happened.
	for _, absent := range []string{`tenant="purged"`, `murder_tenant_phase{tenant="gone"`, "murder_reconcile_", "murder_backup_", "murder_tls_"} {
		if strings.Contains(text, absent) {
			t.Errorf("unexpected %s", absent)
		}
	}
	if t.Failed() {
		t.Logf("metrics:\n%s", text)
	}
	if recorder.Header().Get("Content-Type") != "text/plain; version=0.0.4; charset=utf-8" {
		t.Errorf("content type = %q", recorder.Header().Get("Content-Type"))
	}
}
