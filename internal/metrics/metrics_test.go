package metrics

import (
	"strings"
	"testing"
	"time"
)

func TestWriterEscapesLabelValues(t *testing.T) {
	var out strings.Builder
	w := NewWriter(&out)
	w.Family("m", "gauge", "Help.")
	w.Sample("m", 1.5, "a", `x"y\z`+"\n", "b", "")
	w.Sample("m", 2)
	want := "# HELP m Help.\n# TYPE m gauge\n" + `m{a="x\"y\\z\n",b=""} 1.5` + "\nm 2\n"
	if out.String() != want || w.Err() != nil {
		t.Fatalf("got:\n%s\nwant:\n%s", out.String(), want)
	}
}

// A response time on a bucket's bound falls in that bucket; buckets are
// cumulative and one past the last bound counts only in +Inf.
func TestResponseTimeBuckets(t *testing.T) {
	edge := NewEdge()
	edge.Request("acme", 200, 5*time.Millisecond)
	edge.Request("acme", 404, 7*time.Millisecond)
	edge.Request("acme", 503, time.Minute)
	edge.Request("acme", 0, time.Second) // no response: not counted
	var out strings.Builder
	edge.Write(NewWriter(&out))
	for _, want := range []string{
		`murder_edge_response_seconds_bucket{tenant="acme",le="0.005"} 1`,
		`murder_edge_response_seconds_bucket{tenant="acme",le="0.01"} 2`,
		`murder_edge_response_seconds_bucket{tenant="acme",le="30"} 2`,
		`murder_edge_response_seconds_bucket{tenant="acme",le="+Inf"} 3`,
		`murder_edge_response_seconds_sum{tenant="acme"} 60.012`,
		`murder_edge_requests_total{tenant="acme",code="4xx"} 1`,
	} {
		if !strings.Contains(out.String(), want+"\n") {
			t.Errorf("missing %s in:\n%s", want, out.String())
		}
	}
}

func TestKeepForgetsTenantsThatAreGone(t *testing.T) {
	edge := NewEdge()
	edge.Request("acme", 200, time.Millisecond)
	edge.BackendError("purged")
	edge.Keep(func(tenant string) bool { return tenant == "acme" })
	var out strings.Builder
	edge.Write(NewWriter(&out))
	if !strings.Contains(out.String(), `tenant="acme"`) || strings.Contains(out.String(), `tenant="purged"`) {
		t.Fatalf("metrics:\n%s", out.String())
	}
}
