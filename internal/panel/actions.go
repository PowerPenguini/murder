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
	"github.com/PowerPenguini/murder/internal/store"
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

// flashCookie is __Host-, so only the panel's own name sets it, not a
// tenant's site under the same base domain (browsers take Secure cookies
// on localhost too, for the socket's panel).
const flashCookie = "__Host-murder_flash"

// flashLength bounds what a cookie carries of a message.
const flashLength = 400

// flash keeps what an action did for the page it sends to, once: a
// reload does not do it again, and nothing of it is in the address.
func flash(w http.ResponseWriter, r *http.Request, kind, message string) {
	if len(message) > flashLength {
		message = message[:flashLength] + "…"
	}
	http.SetCookie(w, &http.Cookie{Name: flashCookie, Value: url.QueryEscape(kind + ":" + message), Path: "/", MaxAge: 60,
		HttpOnly: true, Secure: true, SameSite: http.SameSiteStrictMode})
}

// takeFlash is what the last action did, if anything, and forgets it.
func takeFlash(w http.ResponseWriter, r *http.Request) (done, problem string) {
	cookie, err := r.Cookie(flashCookie)
	if err != nil {
		return "", ""
	}
	http.SetCookie(w, &http.Cookie{Name: flashCookie, Value: "", Path: "/", MaxAge: -1, HttpOnly: true, Secure: true, SameSite: http.SameSiteStrictMode})
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
		done = name + " is restarting in a new container, its data kept: its state shows when it is healthy"
	case "move":
		// A move outlives the request: a browser that stops waiting, or
		// a panel that restarts, would otherwise have it undone.
		to := r.FormValue("to")
		actor := s.actor(userOf(r))
		go func() {
			move, cancel := context.WithTimeout(context.WithoutCancel(r.Context()), 30*time.Minute)
			defer cancel()
			if _, err := agent.Move(move, name, to); err != nil {
				s.log.Warn("a move from the panel failed", "tenant", name, "to", to, "by", actor, "error", err)
			}
		}()
		done = name + " moves to " + to + ": the page follows it, the audit log says when it is done"
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
			http.Redirect(w, r, "/tenants", http.StatusSeeOther)
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

// createTenant makes a tenant of an app, on the node chosen: the least
// loaded unless one is named.
func (s *Server) createTenant(w http.ResponseWriter, r *http.Request) {
	agent := s.Actions(s.actor(userOf(r)))
	ctx, cancel := context.WithTimeout(r.Context(), time.Minute)
	defer cancel()
	name := strings.TrimSpace(r.FormValue("name"))
	if !tenantName.MatchString(name) {
		flash(w, r, "problem", "a tenant's name is lowercase letters, digits and dashes")
		http.Redirect(w, r, "/tenants", http.StatusSeeOther)
		return
	}
	node := r.FormValue("node")
	if node == "" {
		node = store.Auto
	}
	applied, err := agent.Create(ctx, api.TenantRequest{Name: name, App: r.FormValue("app"), Node: node})
	if err != nil {
		flash(w, r, "problem", err.Error())
		http.Redirect(w, r, "/tenants", http.StatusSeeOther)
		return
	}
	message := name + " is created on " + applied.Tenant.Node + ", at " + applied.Tenant.URL
	if applied.Note != "" {
		message += " (" + applied.Note + ")"
	}
	flash(w, r, "done", message)
	http.Redirect(w, r, "/tenants/"+url.PathEscape(name), http.StatusSeeOther)
}

// logBytes bounds the logs a page shows.
const logBytes = 2 << 20

type logsData struct {
	Tenant string
	Tail   int
	Head   tenantHead
	// Lines are those the filter lets through, of All read; Warnings and
	// Errors count those read.
	Lines            []logLine
	All              int
	Warnings, Errors int
	// Level is all, warn (warnings and errors) or error; Query the text
	// asked for; Follow says the page follows new lines.
	Level, Query string
	Follow       bool
	// ReadAt is when they were read.
	ReadAt time.Time
	// Restarted says its container restarted: the lines of the one before
	// are gone.
	Restarted bool
	LastExit  string
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
	var data []byte
	var t api.Tenant
	var nodes api.Nodes
	if err := all(
		func() (err error) { data, err = s.Actions(s.actor(userOf(r))).Logs(ctx, name, tail); return },
		func() (err error) { t, err = s.backend.Get(ctx, name); return },
		func() (err error) { nodes, err = s.backend.Nodes(ctx); return },
	); err != nil {
		s.fail(w, r, err)
		return
	}
	// The newest lines, should a few be very long.
	if len(data) > logBytes {
		data = data[len(data)-logBytes:]
	}
	q := r.URL.Query()
	logs := logsData{Tenant: name, Tail: tail, Head: s.headOf(t, nodes, "logs"), Level: q.Get("level"), Query: strings.TrimSpace(q.Get("q")),
		Follow: q.Get("follow") != "", ReadAt: s.now()}
	if logs.Level != "warn" && logs.Level != "error" {
		logs.Level = "all"
	}
	if t.Status != nil && t.Status.Restarts > 0 {
		logs.Restarted, logs.LastExit = true, t.Status.LastExit
	}
	query := strings.ToLower(logs.Query)
	for _, line := range logLines(ansi.ReplaceAllString(string(data), "")) {
		logs.All++
		switch line.Level {
		case "warn":
			logs.Warnings++
		case "error":
			logs.Errors++
		}
		if (logs.Level == "warn" && line.Level == "") || (logs.Level == "error" && line.Level != "error") {
			continue
		}
		if query != "" && !strings.Contains(strings.ToLower(line.Stamp+" "+line.Text), query) {
			continue
		}
		logs.Lines = append(logs.Lines, line)
	}
	s.write(w, "logs", s.pageFor(w, r, name+" · logs", "tenants", nodes, logs), http.StatusOK)
}
