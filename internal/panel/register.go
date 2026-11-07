package panel

import (
	"context"
	"crypto/rand"
	"errors"
	"net/http"
	"net/url"
	"strings"
	"sync"
	"time"
	"unicode"

	"github.com/PowerPenguini/murder/internal/access"
	"github.com/PowerPenguini/murder/internal/api"
)

// accessGrants are the accesses to tenants' logs the panel recorded in
// the register, by reader and tenant: an access serves its person's reads
// of those logs for a while (access.GrantFor), so a page that follows new
// lines records one entry, not one a read. The agent checks each read
// against its entry again; these only spare asking twice.
type accessGrants struct {
	mu     sync.Mutex
	grants map[grantKey]grant
}

// grantKey is a reader and a tenant. A reader is the person signed in;
// on the socket's panel, where no one signs in, one browser (readerCookie).
type grantKey struct{ reader, tenant string }

// grant is an entry and the actor it was recorded as, whom the reads it
// serves name too.
type grant struct {
	access.Entry
	actor string
}

// grantMargin ends a grant here before the agent ends it, so a page never
// reads with one the agent refuses a moment later.
const grantMargin = time.Minute

func (g *accessGrants) get(reader, tenant string, now time.Time) (grant, bool) {
	g.mu.Lock()
	defer g.mu.Unlock()
	kept, ok := g.grants[grantKey{reader, tenant}]
	if !ok || now.Sub(kept.At) > access.GrantFor-grantMargin {
		delete(g.grants, grantKey{reader, tenant})
		return grant{}, false
	}
	return kept, true
}

func (g *accessGrants) put(reader, tenant string, given grant, now time.Time) {
	g.mu.Lock()
	defer g.mu.Unlock()
	if g.grants == nil {
		g.grants = map[grantKey]grant{}
	}
	for key, kept := range g.grants {
		if now.Sub(kept.At) > access.GrantFor {
			delete(g.grants, key)
		}
	}
	g.grants[grantKey{reader, tenant}] = given
}

func (g *accessGrants) drop(reader, tenant string) {
	g.mu.Lock()
	defer g.mu.Unlock()
	delete(g.grants, grantKey{reader, tenant})
}

// readerCookie tells browsers of the socket's panel apart, for their
// grants: whoever reaches that panel names themselves, per browser.
const readerCookie = "__Host-murder_reader"

// reader is who reads, for the grants: the person signed in, or on the
// socket's panel this browser; "" when it has no cookie yet.
func (s *Server) reader(r *http.Request) string {
	if s.Auth != nil {
		return "user:" + userOf(r)
	}
	cookie, err := r.Cookie(readerCookie)
	if err != nil || len(cookie.Value) < 20 || len(cookie.Value) > 64 {
		return ""
	}
	return "browser:" + cookie.Value
}

// readerName is how the socket's panel names whoever says their name, in
// the audit log ("jan (panel socket)"); "" for a name that is no name.
func readerName(given string) string {
	name := strings.Map(func(r rune) rune {
		if unicode.IsPrint(r) {
			return r
		}
		return -1
	}, strings.TrimSpace(given))
	if name == "" || len([]rune(name)) > 40 {
		return ""
	}
	return name + " (panel socket)"
}

// refusedAccess says the agent refused a read as not in the register.
func refusedAccess(err error) bool {
	var refused *api.Error
	return errors.As(err, &refused) && refused.Status == http.StatusForbidden
}

// askAccess shows, in place of a tenant's logs, the form that records
// why they are read; problem says why it is asked again, if it is.
func (s *Server) askAccess(w http.ResponseWriter, r *http.Request, name, problem string, given logsData, status int) {
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
	data := logsData{Tenant: name, Head: head, Ask: true, AskName: s.Auth == nil, Reader: given.Reader, Purpose: given.Purpose, Ticket: given.Ticket, ReadAt: s.now()}
	p := s.pageFor(w, r, name+" · logs", "tenants", nodes, data)
	if problem != "" {
		p.Problem = problem
	}
	s.write(w, "logs", p, status)
}

// recordLogsAccess records, for whoever reads, why they read a tenant's
// logs, then shows the logs as the address asked for them.
func (s *Server) recordLogsAccess(w http.ResponseWriter, r *http.Request) {
	name := r.PathValue("name")
	given := logsData{Reader: r.FormValue("reader"), Purpose: r.FormValue("purpose"), Ticket: r.FormValue("ticket")}
	actor, reader := s.actor(userOf(r)), s.reader(r)
	if s.Auth == nil {
		// No one signed in: the person says who they are, and their
		// browser keeps its grants apart from other browsers'.
		if actor = readerName(given.Reader); actor == "" {
			s.askAccess(w, r, name, "Say who you are: the access register records who reads (40 characters at most).", given, http.StatusUnprocessableEntity)
			return
		}
		if reader == "" {
			value := rand.Text()
			http.SetCookie(w, &http.Cookie{Name: readerCookie, Value: value, Path: "/", HttpOnly: true, Secure: true, SameSite: http.SameSiteStrictMode})
			reader = "browser:" + value
		}
	}
	ctx, cancel := context.WithTimeout(r.Context(), 30*time.Second)
	defer cancel()
	entry, err := s.Actions(actor).RecordAccess(ctx, access.Request{Tenant: name, Kind: access.Logs, Purpose: given.Purpose, Ticket: given.Ticket})
	if err != nil {
		var refused *api.Error
		if errors.As(err, &refused) && refused.Status < 500 {
			s.askAccess(w, r, name, refused.Message, given, http.StatusUnprocessableEntity)
			return
		}
		s.askAccess(w, r, name, "The access register could not record this, so the logs stay closed: "+err.Error(), given, http.StatusBadGateway)
		return
	}
	s.grants.put(reader, name, grant{Entry: entry, actor: actor}, s.now())
	target := "/tenants/" + url.PathEscape(name) + "/logs"
	if query, err := url.ParseQuery(r.URL.RawQuery); err == nil && len(query) > 0 {
		target += "?" + query.Encode()
	}
	http.Redirect(w, r, target, http.StatusSeeOther)
}
