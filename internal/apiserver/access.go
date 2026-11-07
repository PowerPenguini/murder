package apiserver

import (
	"errors"
	"fmt"
	"net/http"
	"time"

	"github.com/PowerPenguini/murder/internal/access"
	"github.com/PowerPenguini/murder/internal/store"
)

func (s *Server) accessRoutes(mux *http.ServeMux) {
	mux.HandleFunc("POST /v1/access", s.recordAccess)
	mux.HandleFunc("GET /v1/access", s.accesses)
}

// now is the server's clock, which tests set.
func (s *Server) now() time.Time {
	if s.clock != nil {
		return s.clock()
	}
	return time.Now()
}

// recordAccess records an access in the register: one a client makes
// itself, such as of a backup in the bucket, before it makes it; one made
// outside Murder, after; or one recorded offline first. It returns the
// entry, whose ID serves the same person's reads of the same thing for a
// while (access.GrantFor).
func (s *Server) recordAccess(w http.ResponseWriter, r *http.Request) {
	var request access.Request
	if !readJSON(w, r, &request) {
		return
	}
	if err := request.Normalize(); err != nil {
		writeError(w, badRequest(err))
		return
	}
	// What only root does: record what was written offline first, and an
	// access made before, other than one made by hand outside Murder.
	if (request.Offline != "" || !request.AccessedAt.IsZero() && request.Kind != access.Manual) && !root(r) {
		writeError(w, statusError{http.StatusForbidden, errors.New("only root (the agent's user) records an access made before, other than one made by hand")})
		return
	}
	if !request.AccessedAt.IsZero() && request.AccessedAt.After(s.now().Add(time.Minute)) {
		writeError(w, badRequest(errors.New("an access recorded after the fact was not in the future")))
		return
	}
	entry, err := s.record(r, request)
	if err != nil {
		writeError(w, err)
		return
	}
	writeJSON(w, http.StatusCreated, entry)
}

// accesses lists the register, oldest first: ?tenant= for a tenant's
// (with those of every tenant), ?since= (RFC 3339) for those recorded
// since.
func (s *Server) accesses(w http.ResponseWriter, r *http.Request) {
	var since time.Time
	if value := r.URL.Query().Get("since"); value != "" {
		parsed, err := time.Parse(time.RFC3339, value)
		if err != nil {
			writeError(w, badRequest(fmt.Errorf("since: %w", err)))
			return
		}
		since = parsed
	}
	events, err := s.store.Accesses(r.URL.Query().Get("tenant"), since)
	if err != nil {
		writeError(w, err)
		return
	}
	entries := make([]access.Entry, 0, len(events))
	for _, event := range events {
		if entry, ok := access.FromEvent(event); ok {
			entries = append(entries, entry)
		}
	}
	writeJSON(w, http.StatusOK, entries)
}

// record writes request in the register, as the requester's.
func (s *Server) record(r *http.Request, request access.Request) (access.Entry, error) {
	// An offline record is recorded once, however many syncs send it.
	event, err := s.store.Access(request.Tenant, request.Kind.Action(), request.Detail(), actor(r), request.Offline != "")
	if err != nil {
		return access.Entry{}, fmt.Errorf("the access register could not record this access, so it is refused: %w", err)
	}
	entry, _ := access.FromEvent(event)
	s.log.Info("access recorded", "id", entry.ID, "tenant", entry.Tenant, "kind", entry.Kind, "what", entry.What, "ticket", entry.Ticket, "actor", entry.Actor)
	return entry, nil
}

// recorded says whether a read of kind, of tenant's what, is recorded in
// the register, recording it if the request gives its purpose and ticket.
// When it is not, it has written why, and nothing must be read: a read is
// refused when the register cannot record it. Call it last, once nothing
// but the read itself is left to fail.
func (s *Server) recorded(w http.ResponseWriter, r *http.Request, kind access.Kind, tenant, what string) bool {
	proof, err := access.ProofFrom(r.URL.Query())
	if err != nil {
		writeError(w, badRequest(err))
		return false
	}
	return s.proven(w, r, proof, kind, tenant, what)
}

// proven is recorded with the proof given apart from the query.
func (s *Server) proven(w http.ResponseWriter, r *http.Request, proof access.Proof, kind access.Kind, tenant, what string) bool {
	if proof.Grant > 0 {
		if err := s.granted(r, proof.Grant, kind, tenant, what); err != nil {
			writeError(w, err)
			return false
		}
		return true
	}
	request := access.Request{Tenant: tenant, Kind: kind, What: what, Purpose: proof.Purpose, Ticket: proof.Ticket}
	if err := request.Normalize(); err != nil {
		writeError(w, statusError{http.StatusForbidden, err})
		return false
	}
	if _, err := s.record(r, request); err != nil {
		writeError(w, err)
		return false
	}
	return true
}

// granted checks that the access of ID id, recorded before, covers this
// read by this requester.
func (s *Server) granted(r *http.Request, id int64, kind access.Kind, tenant, what string) error {
	event, err := s.store.Event(id)
	if errors.Is(err, store.ErrNotFound) {
		return statusError{http.StatusForbidden, fmt.Errorf("%w: no access %d", access.ErrNoGrant, id)}
	}
	if err != nil {
		return err
	}
	entry, ok := access.FromEvent(event)
	if !ok {
		return statusError{http.StatusForbidden, fmt.Errorf("%w: event %d is not an access", access.ErrNoGrant, id)}
	}
	if err := entry.Grants(kind, tenant, what, actor(r), s.now()); err != nil {
		return statusError{http.StatusForbidden, err}
	}
	return nil
}
