package apiserver

import (
	"context"
	"errors"
	"fmt"
	"net/http"

	"github.com/PowerPenguini/murder/internal/access"
	"github.com/PowerPenguini/murder/internal/api"
)

// Backups runs this node's backups.
type Backups interface {
	// RunNow backs this node's tenants up now, between the agent's own
	// runs, never beside one, and reports what it made.
	RunNow(ctx context.Context) ([]api.BackupResult, error)
	// ReplaceTenant makes copy, a restore filled from a backup, the data of
	// an archived tenant of this node, while no backup run, reconcile pass
	// or purge runs. ctx ends the wait for those, never the replacement
	// once begun. An error refuses it, with nothing changed; a replacement
	// that fails reports so in the result.
	ReplaceTenant(ctx context.Context, tenant, copy string) (api.ReplaceResult, error)
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
	var request api.ReplaceRequest
	if !readJSON(w, r, &request) {
		return
	}
	// Putting a backup back follows reading it, which the restore
	// recorded first: it names that entry.
	if request.Access == 0 {
		writeError(w, statusError{http.StatusForbidden, fmt.Errorf("%w: a restore names its entry in the access register, recorded before its backup was read", access.ErrNoGrant)})
		return
	}
	if !s.proven(w, r, access.Proof{Grant: request.Access}, access.BackupRestore, request.Tenant, "") {
		return
	}
	result, err := s.backups.ReplaceTenant(r.Context(), request.Tenant, request.Copy)
	if err != nil {
		writeError(w, err)
		return
	}
	writeJSON(w, http.StatusOK, result)
}
