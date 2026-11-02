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
	"github.com/PowerPenguini/murder/internal/edge"
	"github.com/PowerPenguini/murder/internal/metrics"
	"github.com/PowerPenguini/murder/internal/reconcile"
	"github.com/PowerPenguini/murder/internal/runtime"
	"github.com/PowerPenguini/murder/internal/secrets"
	"github.com/PowerPenguini/murder/internal/spec"
	"github.com/PowerPenguini/murder/internal/store"
	measure "github.com/PowerPenguini/murder/internal/usage"
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
	if err := st.PutNode(store.Node{Name: "node-2", Address: "10.0.0.2:7400"}, "test"); err != nil {
		t.Fatal(err)
	}
	if _, err := st.Create(store.NewTenant{Name: "away", App: "web", Node: "node-2"}, "test"); err != nil {
		t.Fatal(err)
	}
	firewall := new(atomic.Int64)
	firewall.Store(time.Date(2026, 10, 29, 12, 0, 0, 0, time.UTC).UnixNano())
	handler := &agentMetrics{store: st, reconciler: reconciler, api: api.NewServer(st, reconciler, nil, key, log), firewall: firewall}

	recorder := httptest.NewRecorder()
	handler.ServeHTTP(recorder, httptest.NewRequest("GET", "/metrics", nil))
	text := recorder.Body.String()
	for _, want := range []string{
		`murder_tenant_phase{tenant="acme",phase="pending"} 1`,
		`murder_tenant_phase{tenant="acme",phase="running"} 0`,
		`murder_tenant_healthy{tenant="acme"} 0`,
		`murder_firewall_loaded_timestamp_seconds 1.7932752e+09`,
	} {
		if !strings.Contains(text, want+"\n") {
			t.Errorf("missing %s", want)
		}
	}
	// An archived tenant runs nothing, another node reports its own, and
	// nothing before a first pass or backup is reported as if it happened.
	// The edge serves its own metrics.
	for _, absent := range []string{`tenant="gone"`, `tenant="away"`, "murder_reconcile_", "murder_backup_", "murder_edge_", "murder_tls_"} {
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

func TestEdgeMetricsDescribeTheEdge(t *testing.T) {
	stats := metrics.NewEdge()
	stats.Request("acme", 200, 30*time.Millisecond)
	stats.Request("purged", 200, time.Millisecond) // no longer routed: forgotten
	table := &edge.LiveTable{}
	table.Set(edge.NewTable(map[string]edge.Route{"acme": {Backend: "10.240.0.2:8080"}}))
	handler := &edgeMetrics{edge: stats, table: table}
	recorder := httptest.NewRecorder()
	handler.ServeHTTP(recorder, httptest.NewRequest("GET", "/metrics", nil))
	text := recorder.Body.String()
	for _, want := range []string{
		`murder_edge_requests_total{tenant="acme",code="2xx"} 1`,
		`murder_edge_response_seconds_bucket{tenant="acme",le="0.05"} 1`,
		`murder_edge_routes 1`,
		`murder_edge_routes_timestamp_seconds `,
	} {
		if !strings.Contains(text, want) {
			t.Errorf("missing %s", want)
		}
	}
	for _, absent := range []string{`tenant="purged"`, "murder_tls_"} {
		if strings.Contains(text, absent) {
			t.Errorf("unexpected %s", absent)
		}
	}
	if t.Failed() {
		t.Logf("metrics:\n%s", text)
	}
}

// What tenants use is written as Prometheus reads it: their containers'
// counters as counters, memory and data as gauges, and nothing not known.
func TestUsageMetrics(t *testing.T) {
	var out strings.Builder
	writeUsage(metrics.NewWriter(&out), measure.Report{Tenants: map[string]measure.Now{
		"acme": {Memory: 64 << 20, MemoryLimit: 512 << 20, DiskUsed: 1 << 30, Counters: measure.Raw{CPU: 2_500_000, NetRx: 100, NetTx: 200, DiskRead: 300, DiskWrite: 400}},
		"beta": {Memory: 1, DiskUsed: -1},
	}})
	text := out.String()
	for _, want := range []string{
		"# TYPE murder_tenant_cpu_seconds_total counter",
		`murder_tenant_cpu_seconds_total{tenant="acme"} 2.5`,
		`murder_tenant_memory_bytes{tenant="acme"} 6.7108864e+07`,
		`murder_tenant_memory_limit_bytes{tenant="acme"} 5.36870912e+08`,
		`murder_tenant_network_receive_bytes_total{tenant="acme"} 100`,
		`murder_tenant_network_transmit_bytes_total{tenant="acme"} 200`,
		`murder_tenant_disk_read_bytes_total{tenant="acme"} 300`,
		`murder_tenant_disk_written_bytes_total{tenant="acme"} 400`,
		`murder_tenant_data_bytes{tenant="acme"} 1.073741824e+09`,
	} {
		if !strings.Contains(text, want+"\n") {
			t.Errorf("missing %s in:\n%s", want, text)
		}
	}
	if strings.Contains(text, `murder_tenant_memory_limit_bytes{tenant="beta"}`) || strings.Contains(text, `murder_tenant_data_bytes{tenant="beta"}`) {
		t.Errorf("an unknown limit or size was written:\n%s", text)
	}
}
