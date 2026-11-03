package api

import (
	"context"
	"net/http"
)

// BackupResult is one tenant's backup, or the cluster's state's (State),
// or why it failed.
type BackupResult struct {
	Tenant string `json:"tenant"`
	State  bool   `json:"state,omitempty"`
	Key    string `json:"key,omitempty"`
	Size   int64  `json:"size,omitempty"`
	Error  string `json:"error,omitempty"`
}

// Label names what the result is the backup of.
func (r BackupResult) Label() string {
	if r.State {
		return "the cluster's state"
	}
	return r.Tenant
}

// ReplaceRequest asks for a tenant's data to be replaced by a copy.
type ReplaceRequest struct {
	Tenant string `json:"tenant"`
	Copy   string `json:"copy"`
}

// ReplaceResult is a replacement: Aside is the dataset what the tenant had
// is kept in. With Error set, the replacement failed: it was undone if
// Aside is empty, and otherwise Aside is where the tenant's data is.
type ReplaceResult struct {
	Aside string `json:"aside,omitempty"`
	Error string `json:"error,omitempty"`
}

// RunBackups backs this node's tenants up now.
func (c *Client) RunBackups(ctx context.Context) ([]BackupResult, error) {
	var results []BackupResult
	err := c.do(ctx, http.MethodPost, "/v1/backups/run", nil, &results)
	return results, err
}

// ReplaceTenant makes copy the data of an archived tenant of this node.
func (c *Client) ReplaceTenant(ctx context.Context, tenant, copy string) (ReplaceResult, error) {
	var result ReplaceResult
	err := c.do(ctx, http.MethodPost, "/v1/backups/replace", ReplaceRequest{Tenant: tenant, Copy: copy}, &result)
	return result, err
}
