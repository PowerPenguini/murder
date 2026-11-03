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

// EdgeUsage gives the agent the counts of the requests the edge from
// passed to this node's containers. Each edge process says who it is: while
// one hands over to the next, both serve and tell their counts.
func (c *Client) EdgeUsage(ctx context.Context, from usage.EdgeInstance, counts map[string]metrics.Counts) error {
	query := url.Values{"edge": {from.ID}, "started": {from.Started.UTC().Format(time.RFC3339Nano)}}
	return c.do(ctx, http.MethodPost, "/v1/edge/usage?"+query.Encode(), counts, nil)
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
