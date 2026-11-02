package panel

import (
	"context"
	"errors"
	"net/http"
	"net/url"
	"regexp"
	"strconv"
	"strings"
	"time"

	"github.com/PowerPenguini/murder/internal/api"
)

// ActionAgent is what acting on tenants asks of the agent, in the name of
// the person signed in.
type ActionAgent interface {
	Apps(ctx context.Context) ([]api.App, error)
	Create(ctx context.Context, request api.TenantRequest) (api.Applied, error)
	Suspend(ctx context.Context, name string, suspended bool) (api.Tenant, error)
	Roll(ctx context.Context, name string) (api.Tenant, error)
	Move(ctx context.Context, tenant, to string) (api.Tenant, error)
	Archive(ctx context.Context, name string) error
	Restore(ctx context.Context, name string) (api.Tenant, error)
	Purge(ctx context.Context, name string) error
	Logs(ctx context.Context, name string, tail int) ([]byte, error)
}

const flashCookie = "murder_flash"

// flash keeps what an action did for the page it sends to, once: a
// reload does not do it again, and nothing of it is in the address.
func flash(w http.ResponseWriter, r *http.Request, kind, message string) {
	http.SetCookie(w, &http.Cookie{Name: flashCookie, Value: url.QueryEscape(kind + ":" + message), Path: "/", MaxAge: 60,
		HttpOnly: true, Secure: r.TLS != nil, SameSite: http.SameSiteStrictMode})
}

// takeFlash is what the last action did, if anything, and forgets it.
func takeFlash(w http.ResponseWriter, r *http.Request) (done, problem string) {
	cookie, err := r.Cookie(flashCookie)
	if err != nil {
		return "", ""
	}
	http.SetCookie(w, &http.Cookie{Name: flashCookie, Value: "", Path: "/", MaxAge: -1, HttpOnly: true, Secure: r.TLS != nil, SameSite: http.SameSiteStrictMode})
	value, _ := url.QueryUnescape(cookie.Value)
	kind, message, _ := strings.Cut(value, ":")
	if kind == "problem" {
		return "", message
	}
	return message, ""
}

// tenantAction does one thing to a tenant, and sends back to its page.
func (s *Server) tenantAction(w http.ResponseWriter, r *http.Request) {
	name := r.PathValue("name")
	agent := s.Actions(s.actor(userOf(r)))
	// A move waits for the tenant to run on its new node.
	ctx, cancel := context.WithTimeout(r.Context(), 15*time.Minute)
	defer cancel()
	back := "/tenants/" + url.PathEscape(name)
	confirmed := strings.TrimSpace(r.FormValue("confirm")) == name
	var err error
	var done string
	switch r.FormValue("do") {
	case "suspend":
		_, err = agent.Suspend(ctx, name, true)
		done = name + " is suspended: its container stopped, its data kept"
	case "resume":
		_, err = agent.Suspend(ctx, name, false)
		done = name + " runs again"
	case "roll":
		_, err = agent.Roll(ctx, name)
		done = name + " gets a new container"
	case "move":
		to := r.FormValue("to")
		_, err = agent.Move(ctx, name, to)
		done = name + " runs on " + to + " now"
	case "archive":
		if !confirmed {
			err = errors.New("type the tenant's name to archive it")
			break
		}
		err = agent.Archive(ctx, name)
		done = name + " is archived: it runs nowhere, its data kept until purged"
	case "restore":
		_, err = agent.Restore(ctx, name)
		done = name + " is restored"
	case "purge":
		if !confirmed {
			err = errors.New("type the tenant's name to purge it")
			break
		}
		if err = agent.Purge(ctx, name); err == nil {
			flash(w, r, "done", name+" is purged: its data is gone")
			http.Redirect(w, r, "/", http.StatusSeeOther)
			return
		}
	default:
		err = errors.New("unknown action")
	}
	if err != nil {
		flash(w, r, "problem", err.Error())
	} else {
		flash(w, r, "done", done)
	}
	http.Redirect(w, r, back, http.StatusSeeOther)
}

var tenantName = regexp.MustCompile(`^[a-z0-9]([a-z0-9-]{0,61}[a-z0-9])?$`)

// createTenant makes a tenant of an app, on the node the panel is on.
func (s *Server) createTenant(w http.ResponseWriter, r *http.Request) {
	agent := s.Actions(s.actor(userOf(r)))
	ctx, cancel := context.WithTimeout(r.Context(), time.Minute)
	defer cancel()
	name := strings.TrimSpace(r.FormValue("name"))
	if !tenantName.MatchString(name) {
		flash(w, r, "problem", "a tenant's name is lowercase letters, digits and dashes")
		http.Redirect(w, r, "/", http.StatusSeeOther)
		return
	}
	applied, err := agent.Create(ctx, api.TenantRequest{Name: name, App: r.FormValue("app")})
	if err != nil {
		flash(w, r, "problem", err.Error())
		http.Redirect(w, r, "/", http.StatusSeeOther)
		return
	}
	message := name + " is created on " + applied.Tenant.Node + ", at " + applied.Tenant.URL
	if applied.Note != "" {
		message += " (" + applied.Note + ")"
	}
	flash(w, r, "done", message)
	http.Redirect(w, r, "/tenants/"+url.PathEscape(name), http.StatusSeeOther)
}

type logsData struct {
	Tenant string
	Lines  string
	Tail   int
}

// ansi matches terminal escapes, which a page shows as noise.
var ansi = regexp.MustCompile("\x1b\\[[0-9;?]*[A-Za-z]")

func (s *Server) logs(w http.ResponseWriter, r *http.Request) {
	name := r.PathValue("name")
	tail, err := strconv.Atoi(r.URL.Query().Get("tail"))
	if err != nil || tail < 1 || tail > 5000 {
		tail = 300
	}
	ctx, cancel := context.WithTimeout(r.Context(), 30*time.Second)
	defer cancel()
	data, err := s.Actions(s.actor(userOf(r))).Logs(ctx, name, tail)
	if err != nil {
		s.fail(w, r, err)
		return
	}
	s.render(w, r, "logs", name+" logs", "overview", logsData{Tenant: name, Lines: ansi.ReplaceAllString(string(data), ""), Tail: tail})
}
