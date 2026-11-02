package api

import (
	"context"
	"net/http"
	"net/url"
	"strconv"
	"time"

	"github.com/PowerPenguini/murder/internal/metrics"
	"github.com/PowerPenguini/murder/internal/usage"
)

// UsageReport is the cluster's use now: each node's that answered, and the
// nodes that did not.
type UsageReport struct {
	Nodes   []usage.Report `json:"nodes"`
	Missing []string       `json:"missing,omitempty"`
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
