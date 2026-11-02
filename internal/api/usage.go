package api

import (
	"context"
	"fmt"
	"net/http"
	"net/url"
	"slices"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/PowerPenguini/murder/internal/metrics"
	"github.com/PowerPenguini/murder/internal/usage"
)

// Usage is what this node measured of its tenants and itself.
type Usage interface {
	Edge(tenant string, counts metrics.Counts, at time.Time)
	Report(at time.Time) usage.Report
	Series(tenant string, step time.Duration, at time.Time) (usage.Series, bool)
}

// usageCluster asks other nodes what they measured.
type usageCluster interface {
	UsageOn(ctx context.Context, node string, out any) error
	TenantUsageOn(ctx context.Context, node, tenant string, step time.Duration, out any) error
}

// WithUsage serves what usage measured.
func (s *Server) WithUsage(u Usage) *Server {
	s.usage = u
	return s
}

// UsageReport is the cluster's use now: each node's that answered, and the
// nodes that did not.
type UsageReport struct {
	Nodes   []usage.Report `json:"nodes"`
	Missing []string       `json:"missing,omitempty"`
}

// remoteUsageTimeout bounds how long the report waits for a node.
const remoteUsageTimeout = 2 * time.Second

func (s *Server) usageRoutes(mux *http.ServeMux) {
	mux.HandleFunc("POST /v1/edge/usage", s.edgeUsage)
	mux.HandleFunc("GET /v1/usage", s.usageReport)
	mux.HandleFunc("GET /v1/tenants/{name}/usage", s.tenantUsage)
}

// edgeUsage takes the node's edge's counts of the requests it passed to
// this node's containers.
func (s *Server) edgeUsage(w http.ResponseWriter, r *http.Request) {
	var counts map[string]metrics.Counts
	if !readJSON(w, r, &counts) {
		return
	}
	if s.usage != nil {
		now := time.Now()
		for tenant, c := range counts {
			s.usage.Edge(tenant, c, now)
		}
	}
	w.WriteHeader(http.StatusNoContent)
}

func (s *Server) usageReport(w http.ResponseWriter, r *http.Request) {
	report := UsageReport{Nodes: []usage.Report{}}
	if s.usage != nil {
		report.Nodes = append(report.Nodes, s.usage.Report(time.Now()))
	}
	remote, ok := s.cluster.(usageCluster)
	if s.cluster != nil && ok {
		ctx, cancel := context.WithTimeout(r.Context(), remoteUsageTimeout)
		defer cancel()
		var mu sync.Mutex
		var group sync.WaitGroup
		for _, node := range s.store.Nodes() {
			if node.Name == s.store.Node() || node.Failed() {
				continue
			}
			group.Go(func() {
				var got usage.Report
				err := remote.UsageOn(ctx, node.Name, &got)
				mu.Lock()
				defer mu.Unlock()
				if err != nil {
					report.Missing = append(report.Missing, node.Name)
					return
				}
				report.Nodes = append(report.Nodes, got)
			})
		}
		group.Wait()
	}
	slices.SortFunc(report.Nodes, func(a, b usage.Report) int { return strings.Compare(a.Node, b.Node) })
	slices.Sort(report.Missing)
	writeJSON(w, http.StatusOK, report)
}

// tenantUsage is a tenant's day, asked of the node it runs on.
func (s *Server) tenantUsage(w http.ResponseWriter, r *http.Request) {
	name := r.PathValue("name")
	step := 5 * time.Minute
	if value := r.URL.Query().Get("step"); value != "" {
		seconds, err := strconv.Atoi(value)
		if err != nil || seconds < 60 || seconds > 3600 {
			writeError(w, badRequest(fmt.Errorf("step must be 60-3600 seconds")))
			return
		}
		step = time.Duration(seconds) * time.Second
	}
	record, err := s.store.Live(name)
	if err != nil {
		writeError(w, err)
		return
	}
	if record.Node != s.store.Node() {
		remote, ok := s.cluster.(usageCluster)
		if s.cluster == nil || !ok {
			writeError(w, notFound(fmt.Errorf("tenant %s runs on node %s, which this node cannot ask", name, record.Node)))
			return
		}
		var series usage.Series
		if err := remote.TenantUsageOn(r.Context(), record.Node, name, step, &series); err != nil {
			writeError(w, err)
			return
		}
		writeJSON(w, http.StatusOK, series)
		return
	}
	if s.usage == nil {
		writeError(w, notFound(fmt.Errorf("this node measures no use")))
		return
	}
	series, ok := s.usage.Series(name, step, time.Now())
	if !ok {
		writeError(w, notFound(fmt.Errorf("tenant %s has not been measured yet", name)))
		return
	}
	writeJSON(w, http.StatusOK, series)
}

// EdgeUsage gives the agent the edge's counts of the requests it passed to
// this node's containers.
func (c *Client) EdgeUsage(ctx context.Context, counts map[string]metrics.Counts) error {
	return c.do(ctx, http.MethodPost, "/v1/edge/usage", counts, nil)
}

// Usage returns the cluster's use now.
func (c *Client) Usage(ctx context.Context) (UsageReport, error) {
	var report UsageReport
	return report, c.do(ctx, http.MethodGet, "/v1/usage", nil, &report)
}

// TenantUsage returns a tenant's last day, in steps of step.
func (c *Client) TenantUsage(ctx context.Context, name string, step time.Duration) (usage.Series, error) {
	var series usage.Series
	err := c.do(ctx, http.MethodGet, tenantPath(name)+"/usage?step="+url.QueryEscape(strconv.Itoa(int(step/time.Second))), nil, &series)
	return series, err
}
