package panel

import (
	"context"
	"errors"
	"net/http"
	"net/url"
	"sync"
	"time"

	"github.com/PowerPenguini/murder/internal/access"
	"github.com/PowerPenguini/murder/internal/api"
)

// accessGrants are the accesses to tenants' logs the panel recorded in
// the register for the people signed in, by person and tenant: an access
// serves its person's reads of those logs for a while (access.GrantFor),
// so a page that follows new lines records one entry, not one a read.
// The agent checks each read against its entry again; these only spare
// asking twice.
type accessGrants struct {
	mu     sync.Mutex
	grants map[grantKey]access.Entry
}

type grantKey struct{ user, tenant string }

// grantMargin ends a grant here before the agent ends it, so a page never
// reads with one the agent refuses a moment later.
const grantMargin = time.Minute

func (g *accessGrants) get(user, tenant string, now time.Time) (access.Entry, bool) {
	g.mu.Lock()
	defer g.mu.Unlock()
	entry, ok := g.grants[grantKey{user, tenant}]
	if !ok || now.Sub(entry.At) > access.GrantFor-grantMargin {
		delete(g.grants, grantKey{user, tenant})
		return access.Entry{}, false
	}
	return entry, true
}

func (g *accessGrants) put(user, tenant string, entry access.Entry, now time.Time) {
	g.mu.Lock()
	defer g.mu.Unlock()
	if g.grants == nil {
		g.grants = map[grantKey]access.Entry{}
	}
	for key, kept := range g.grants {
		if now.Sub(kept.At) > access.GrantFor {
			delete(g.grants, key)
		}
	}
	g.grants[grantKey{user, tenant}] = entry
}

func (g *accessGrants) drop(user, tenant string) {
	g.mu.Lock()
	defer g.mu.Unlock()
	delete(g.grants, grantKey{user, tenant})
}

// refusedAccess says the agent refused a read as not in the register.
func refusedAccess(err error) bool {
	var refused *api.Error
	return errors.As(err, &refused) && refused.Status == http.StatusForbidden
}

// askAccess shows, in place of a tenant's logs, the form that records
// why they are read; problem says why it is asked again, if it is.
func (s *Server) askAccess(w http.ResponseWriter, r *http.Request, name, problem, purpose, ticket string, status int) {
	ctx, cancel := context.WithTimeout(r.Context(), 30*time.Second)
	defer cancel()
	var t api.Tenant
	var nodes api.Nodes
	if err := all(
		func() (err error) { t, err = s.backend.Get(ctx, name); return },
		func() (err error) { nodes, err = s.backend.Nodes(ctx); return },
	); err != nil {
		s.fail(w, r, err)
		return
	}
	head := s.withApps(ctx, r, s.headOf(t, nodes, "logs"))
	head.Back = r.URL.RequestURI()
	data := logsData{Tenant: name, Head: head, Ask: true, Purpose: purpose, Ticket: ticket, ReadAt: s.now()}
	p := s.pageFor(w, r, name+" · logs", "tenants", nodes, data)
	if problem != "" {
		p.Problem = problem
	}
	s.write(w, "logs", p, status)
}

// recordLogsAccess records, for the person signed in, why they read a
// tenant's logs, then shows the logs as the address asked for them.
func (s *Server) recordLogsAccess(w http.ResponseWriter, r *http.Request) {
	name := r.PathValue("name")
	purpose, ticket := r.FormValue("purpose"), r.FormValue("ticket")
	ctx, cancel := context.WithTimeout(r.Context(), 30*time.Second)
	defer cancel()
	user := userOf(r)
	entry, err := s.Actions(s.actor(user)).RecordAccess(ctx, access.Request{Tenant: name, Kind: access.Logs, Purpose: purpose, Ticket: ticket})
	if err != nil {
		var refused *api.Error
		if errors.As(err, &refused) && refused.Status < 500 {
			s.askAccess(w, r, name, refused.Message, purpose, ticket, http.StatusUnprocessableEntity)
			return
		}
		s.askAccess(w, r, name, "The access register could not record this, so the logs stay closed: "+err.Error(), purpose, ticket, http.StatusBadGateway)
		return
	}
	s.grants.put(user, name, entry, s.now())
	target := "/tenants/" + url.PathEscape(name) + "/logs"
	if query, err := url.ParseQuery(r.URL.RawQuery); err == nil && len(query) > 0 {
		target += "?" + query.Encode()
	}
	http.Redirect(w, r, target, http.StatusSeeOther)
}
