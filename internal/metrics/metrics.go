// Package metrics writes what the agent observes in the Prometheus text
// format, for VictoriaMetrics or Prometheus to scrape, and counts the edge
// proxy's requests per tenant.
package metrics

import (
	"fmt"
	"io"
	"maps"
	"math"
	"slices"
	"strconv"
	"strings"
	"sync"
	"time"
)

// Writer writes metric families in the text exposition format; the first
// write error sticks and is returned by Err.
type Writer struct {
	w   io.Writer
	err error
}

// NewWriter writes to w.
func NewWriter(w io.Writer) *Writer { return &Writer{w: w} }

// Family starts a family: kind is "counter", "gauge" or "histogram".
func (w *Writer) Family(name, kind, help string) {
	w.printf("# HELP %s %s\n# TYPE %s %s\n", name, help, name, kind)
}

// Sample writes one sample; labels are name, value pairs.
func (w *Writer) Sample(name string, value float64, labels ...string) {
	var text strings.Builder
	text.WriteString(name)
	if len(labels) > 0 {
		text.WriteByte('{')
		for i := 0; i+1 < len(labels); i += 2 {
			if i > 0 {
				text.WriteByte(',')
			}
			text.WriteString(labels[i] + `="` + escape(labels[i+1]) + `"`)
		}
		text.WriteByte('}')
	}
	w.printf("%s %s\n", text.String(), format(value))
}

// Err returns the first write error.
func (w *Writer) Err() error { return w.err }

func (w *Writer) printf(pattern string, args ...any) {
	if w.err == nil {
		_, w.err = fmt.Fprintf(w.w, pattern, args...)
	}
}

func escape(value string) string {
	return strings.NewReplacer(`\`, `\\`, `"`, `\"`, "\n", `\n`).Replace(value)
}

func format(value float64) string {
	if math.IsInf(value, 1) {
		return "+Inf"
	}
	return strconv.FormatFloat(value, 'g', -1, 64)
}

// Bool is 1 for true and 0 for false.
func Bool(b bool) float64 {
	if b {
		return 1
	}
	return 0
}

// Timestamp is t in Unix seconds.
func Timestamp(t time.Time) float64 { return float64(t.UnixNano()) / 1e9 }

// buckets are the upper bounds, in seconds, of the response time
// histogram: from a cached page to a slow report.
var buckets = [...]float64{0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1, 2.5, 5, 10, 30}

// Edge counts the edge proxy's requests.
type Edge struct {
	mu       sync.Mutex
	tenants  map[string]*tenantRequests
	unrouted uint64
}

type tenantRequests struct {
	// byClass counts responses by status class: 1xx (a protocol upgrade)
	// to 5xx.
	byClass       [5]uint64
	backendErrors uint64
	// waits is the response time histogram: counts per bucket (not
	// cumulative), their sum and count.
	waits [len(buckets)]uint64
	sum   float64
	count uint64
}

// NewEdge returns empty counters.
func NewEdge() *Edge { return &Edge{tenants: map[string]*tenantRequests{}} }

// Request counts a response the proxy gave for tenant, with its status and
// how long the response headers took.
func (e *Edge) Request(tenant string, status int, wait time.Duration) {
	class := status/100 - 1
	if class < 0 || class > 4 {
		return
	}
	e.mu.Lock()
	defer e.mu.Unlock()
	counts := e.tenant(tenant)
	counts.byClass[class]++
	seconds := wait.Seconds()
	for i, bound := range buckets {
		if seconds <= bound {
			counts.waits[i]++
			break
		}
	}
	counts.sum += seconds
	counts.count++
}

// BackendError counts a request the proxy could not pass to tenant.
func (e *Edge) BackendError(tenant string) {
	e.mu.Lock()
	defer e.mu.Unlock()
	e.tenant(tenant).backendErrors++
}

// Unrouted counts a request for no known tenant.
func (e *Edge) Unrouted() {
	e.mu.Lock()
	defer e.mu.Unlock()
	e.unrouted++
}

func (e *Edge) tenant(name string) *tenantRequests {
	counts := e.tenants[name]
	if counts == nil {
		counts = &tenantRequests{}
		e.tenants[name] = counts
	}
	return counts
}

// Write writes the edge's families.
func (e *Edge) Write(w *Writer) {
	e.mu.Lock()
	tenants := make(map[string]tenantRequests, len(e.tenants))
	for name, counts := range e.tenants {
		tenants[name] = *counts
	}
	unrouted := e.unrouted
	e.mu.Unlock()
	names := slices.Sorted(maps.Keys(tenants))

	w.Family("murder_edge_requests_total", "counter", "Responses the edge proxy gave, by tenant and status class.")
	for _, name := range names {
		for class, count := range tenants[name].byClass {
			w.Sample("murder_edge_requests_total", float64(count), "tenant", name, "code", strconv.Itoa(class+1)+"xx")
		}
	}
	w.Family("murder_edge_response_seconds", "histogram", "Time until the response headers, by tenant.")
	for _, name := range names {
		counts := tenants[name]
		var cumulative uint64
		for i, bound := range buckets {
			cumulative += counts.waits[i]
			w.Sample("murder_edge_response_seconds_bucket", float64(cumulative), "tenant", name, "le", format(bound))
		}
		w.Sample("murder_edge_response_seconds_bucket", float64(counts.count), "tenant", name, "le", "+Inf")
		w.Sample("murder_edge_response_seconds_sum", counts.sum, "tenant", name)
		w.Sample("murder_edge_response_seconds_count", float64(counts.count), "tenant", name)
	}
	w.Family("murder_edge_backend_errors_total", "counter", "Requests the edge proxy could not pass to the tenant's container.")
	for _, name := range names {
		w.Sample("murder_edge_backend_errors_total", float64(tenants[name].backendErrors), "tenant", name)
	}
	w.Family("murder_edge_unrouted_requests_total", "counter", "Requests for a host that is no tenant's.")
	w.Sample("murder_edge_unrouted_requests_total", float64(unrouted))
}
