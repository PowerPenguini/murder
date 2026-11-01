package api

import (
	"context"
	"errors"
	"net/http"
)

// Backups runs this node's backups.
type Backups interface {
	// RunNow backs this node's tenants up now, between the agent's own
	// runs, never beside one, and reports what it made.
	RunNow(ctx context.Context) ([]BackupResult, error)
	// ReplaceTenant makes copy, a restore filled from a backup, the data of
	// an archived tenant of this node, while no backup run, reconcile pass
	// or purge runs. ctx ends the wait for those, never the replacement
	// once begun. An error refuses it, with nothing changed; a replacement
	// that fails reports so in the result.
	ReplaceTenant(ctx context.Context, tenant, copy string) (ReplaceResult, error)
}

// BackupResult is one tenant's backup, or why it failed.
type BackupResult struct {
	Tenant string `json:"tenant"`
	Key    string `json:"key,omitempty"`
	Size   int64  `json:"size,omitempty"`
	Error  string `json:"error,omitempty"`
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

// WithBackups backs tenants up on request.
func (s *Server) WithBackups(b Backups) *Server {
	s.backups = b
	return s
}

var errNoBackups = statusError{http.StatusConflict, errors.New("this node has no backups set up (backups: in its config)")}

func (s *Server) runBackups(w http.ResponseWriter, r *http.Request) {
	if s.backups == nil {
		writeError(w, errNoBackups)
		return
	}
	results, err := s.backups.RunNow(r.Context())
	if err != nil {
		writeError(w, err)
		return
	}
	writeJSON(w, http.StatusOK, results)
}

func (s *Server) replaceTenant(w http.ResponseWriter, r *http.Request) {
	if s.backups == nil {
		writeError(w, errNoBackups)
		return
	}
	var request ReplaceRequest
	if !readJSON(w, r, &request) {
		return
	}
	result, err := s.backups.ReplaceTenant(r.Context(), request.Tenant, request.Copy)
	if err != nil {
		writeError(w, err)
		return
	}
	writeJSON(w, http.StatusOK, result)
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
