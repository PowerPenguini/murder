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
}

// BackupResult is one tenant's backup, or why it failed.
type BackupResult struct {
	Tenant string `json:"tenant"`
	Key    string `json:"key,omitempty"`
	Size   int64  `json:"size,omitempty"`
	Error  string `json:"error,omitempty"`
}

// WithBackups backs tenants up on request.
func (s *Server) WithBackups(b Backups) *Server {
	s.backups = b
	return s
}

func (s *Server) runBackups(w http.ResponseWriter, r *http.Request) {
	if s.backups == nil {
		writeError(w, statusError{http.StatusConflict, errors.New("this node has no backups set up (backups: in its config)")})
		return
	}
	results, err := s.backups.RunNow(r.Context())
	if err != nil {
		writeError(w, err)
		return
	}
	writeJSON(w, http.StatusOK, results)
}

// RunBackups backs this node's tenants up now.
func (c *Client) RunBackups(ctx context.Context) ([]BackupResult, error) {
	var results []BackupResult
	err := c.do(ctx, http.MethodPost, "/v1/backups/run", nil, &results)
	return results, err
}
