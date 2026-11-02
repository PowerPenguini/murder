package apiserver

import (
	"context"
	"errors"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/PowerPenguini/murder/internal/api"
	"github.com/PowerPenguini/murder/internal/metrics"
	"github.com/PowerPenguini/murder/internal/store"
	"github.com/PowerPenguini/murder/internal/usage"
)

// usageCluster answers for other nodes' use as they would; a node missing
// from reports does not answer.
type fakeUsageCluster struct {
	statusCluster
	reports map[string]usage.Report
	series  map[string]usage.Series
}

func (c fakeUsageCluster) UsageOn(_ context.Context, node string, out any) error {
	report, ok := c.reports[node]
	if !ok {
		return errors.New("node " + node + " does not answer")
	}
	*out.(*usage.Report) = report
	return nil
}

func (c fakeUsageCluster) TenantUsageOn(_ context.Context, node, tenant string, step time.Duration, out any) error {
	series, ok := c.series[node+"/"+tenant]
	if !ok {
		return &api.Error{Status: http.StatusNotFound, Message: "not measured"}
	}
	series.Step = int(step / time.Second)
	*out.(*usage.Series) = series
	return nil
}

// The edge's counts reach the recorder; the node's use and a tenant's day
// are served, a tenant of another node's asked of that node.
func TestUsage(t *testing.T) {
	f := newAPIFixture(t)
	ctx := context.Background()
	recorder := usage.NewRecorder(f.store.Node())
	f.server.WithUsage(recorder)
	if _, err := f.client.PutApp(ctx, "web", appSpec()); err != nil {
		t.Fatal(err)
	}
	if _, err := f.client.Create(ctx, tenantOf("acme")); err != nil {
		t.Fatal(err)
	}

	// Two minutes ago the container read, and the edge counted 7
	// requests since.
	ago := time.Now().Add(-2 * time.Minute).Truncate(time.Minute)
	recorder.Sample("acme", usage.Raw{At: ago, Container: "c", CPU: 0, Memory: 64 << 20}, 1)
	recorder.Sample("acme", usage.Raw{At: ago.Add(30 * time.Second), Container: "c", CPU: 15_000_000, Memory: 80 << 20}, 1)
	recorder.Edge("acme", metrics.Counts{Waits: make([]uint64, len(metrics.Buckets)+1)}, ago)
	if err := f.client.EdgeUsage(ctx, map[string]metrics.Counts{"acme": {ByClass: [5]uint64{0, 6, 0, 0, 1}, Waits: []uint64{7, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0}, Count: 7}}); err != nil {
		t.Fatal(err)
	}

	report, err := f.client.Usage(ctx)
	if err != nil || len(report.Nodes) != 1 || report.Nodes[0].Node != "test-node" {
		t.Fatalf("report = %+v, %v", report, err)
	}
	if acme := report.Nodes[0].Tenants["acme"]; acme.Memory != 80<<20 || acme.CPU != 0.5 || acme.CPULimit != 1 {
		t.Fatalf("acme now: %+v", acme)
	}
	series, err := f.client.TenantUsage(ctx, "acme", time.Minute)
	if err != nil || series.Step != 60 || len(series.Points) == 0 || series.Points[0].Memory != 80<<20 {
		t.Fatalf("acme's day = %+v, %v", series, err)
	}
	// The counts the edge gave just now are in the minute running: shown
	// once it closes, so no minute is shown half counted.
	closed, _ := recorder.Series("acme", time.Minute, time.Now().Add(time.Minute))
	var requests, errs int64
	for _, p := range closed.Points {
		requests, errs = requests+p.Requests, errs+p.Errors
	}
	if requests != 7 || errs != 1 {
		t.Fatalf("requests %d, errors %d in %+v", requests, errs, closed.Points)
	}

	recorder.Keep(func(string) bool { return false })
	if _, err := f.client.TenantUsage(ctx, "acme", time.Minute); !api.IsNotFound(err) {
		t.Fatalf("a tenant not measured: %v", err)
	}
	if _, err := f.client.TenantUsage(ctx, "nobody", time.Minute); !api.IsNotFound(err) {
		t.Fatalf("no such tenant: %v", err)
	}
	recorder.Sample("acme", usage.Raw{At: ago, Container: "c"}, 0)
	request := httptest.NewRequest(http.MethodGet, "/v1/tenants/acme/usage?step=5", nil)
	recorded := httptest.NewRecorder()
	f.handler.ServeHTTP(recorded, request)
	if recorded.Code != http.StatusBadRequest || !strings.Contains(recorded.Body.String(), "step") {
		t.Fatalf("a step too short: %d %s", recorded.Code, recorded.Body)
	}

	// In a cluster: other nodes' use is asked of them, and a node that
	// does not answer is said to be missing.
	for _, node := range []string{"n2", "n3"} {
		if err := f.store.PutNode(store.Node{Name: node, Address: node + ":7400"}, "test"); err != nil {
			t.Fatal(err)
		}
	}
	if _, err := f.store.Create(store.NewTenant{Name: "away", App: "web", Node: "n2"}, "test"); err != nil {
		t.Fatal(err)
	}
	f.server.WithCluster(fakeUsageCluster{
		reports: map[string]usage.Report{"n2": {Node: "n2", Host: usage.Host{CPUs: 4}, Tenants: map[string]usage.Now{"away": {Memory: 1 << 20}}}},
		series:  map[string]usage.Series{"n2/away": {Tenant: "away", Node: "n2", Points: []usage.Point{{Memory: 1 << 20}}}},
	})
	report, err = f.client.Usage(ctx)
	if err != nil || len(report.Nodes) != 2 || report.Nodes[0].Node != "n2" || report.Nodes[1].Node != "test-node" || strings.Join(report.Missing, ",") != "n3" {
		t.Fatalf("the cluster's use = %+v, %v", report, err)
	}
	if away, err := f.client.TenantUsage(ctx, "away", 10*time.Minute); err != nil || away.Node != "n2" || away.Step != 600 || len(away.Points) != 1 {
		t.Fatalf("away's day = %+v, %v", away, err)
	}
}
