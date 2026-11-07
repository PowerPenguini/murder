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
	"unicode"

	"github.com/PowerPenguini/murder/internal/access"
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
	// Logs and PreviousLogs read under an entry of the access register;
	// RecordAccess records one. PreviousLogs are the last lines its node
	// kept of the tenant's previous container; a 404 when it kept none.
	Logs(ctx context.Context, name string, tail int, proof access.Proof) ([]byte, error)
	PreviousLogs(ctx context.Context, name string, proof access.Proof) (api.PreviousLogs, error)
	RecordAccess(ctx context.Context, request access.Request) (access.Entry, error)
}

// typed says the name typed to confirm an action is the one asked for:
// spaces around it and the case of its letters aside, as a phone's
// keyboard may capitalise the first letter or add a space. The names asked
// for are lowercase by rule, or a secret's, which no two differ by case.
func typed(given, name string) bool {
	return name != "" && strings.EqualFold(strings.TrimSpace(given), name)
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

// tenantBack is where a form posted from one of tenant name's pages
// returns: the page its back field names, when that is the tenant's
// Overview, Logs or Settings, else fallback. Nothing else is taken: no
// other site, no other page of the panel. Its Overview and Logs keep
// their address's choices (a range, a filter); its Settings not, as those
// propose or review a change.
func tenantBack(r *http.Request, name, fallback string) string {
	home := "/tenants/" + url.PathEscape(name)
	back := r.FormValue("back")
	if back == "" || len(back) > 2048 || !strings.HasPrefix(back, "/") || strings.ContainsAny(back, "\\#") ||
		strings.IndexFunc(back, unicode.IsControl) >= 0 {
		return fallback
	}
	u, err := url.Parse(back)
	if err != nil || u.Scheme != "" || u.Host != "" || u.User != nil || u.Opaque != "" || u.RawPath != "" {
		return fallback
	}
	switch u.Path {
	case home + "/settings":
		return u.Path
	case home, home + "/logs":
	default:
		return fallback
	}
	query, err := url.ParseQuery(u.RawQuery)
	if err != nil || len(query) == 0 {
		return u.Path
	}
	return u.Path + "?" + query.Encode()
}

// tenantAction does one thing to a tenant, and sends back to the page of
// it the form was on.
func (s *Server) tenantAction(w http.ResponseWriter, r *http.Request) {
	if strings.HasPrefix(r.FormValue("do"), "domain-") {
		s.domainAction(w, r)
		return
	}
	name := r.PathValue("name")
	agent := s.Actions(s.actor(userOf(r)))
	// A move waits for the tenant to run on its new node.
	ctx, cancel := context.WithTimeout(r.Context(), 15*time.Minute)
	defer cancel()
	home := "/tenants/" + url.PathEscape(name)
	back := tenantBack(r, name, home)
	confirmed := typed(r.FormValue("confirm"), name)
	var err error
	var done string
	switch r.FormValue("do") {
	case "suspend":
		// It stops serving: the name is typed, as for archiving.
		if !confirmed {
			err = errors.New("type the tenant's name to suspend it")
			break
		}
		_, err = agent.Suspend(ctx, name, true)
		done = name + " is suspended: its container stopped, its data kept"
	case "resume":
		_, err = agent.Suspend(ctx, name, false)
		done = name + " runs again"
	case "roll":
		// It stops serving for a moment: the name is typed.
		if !confirmed {
			err = errors.New("type the tenant's name to restart it")
			break
		}
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
		// Suspended or archived, it has no container whose lines its Logs
		// could read: to its Overview.
		if do := r.FormValue("do"); (do == "suspend" || do == "archive") && strings.HasPrefix(back, home+"/logs") {
			back = home
		}
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
	// Restarted says its container restarted; LastExit how it last exited.
	Restarted bool
	LastExit  string
	// Previous is what its node kept of its previous container, if
	// anything; Showing says the page shows those lines, not the current
	// container's. PreviousProblem says why they could not be read.
	Previous        *api.PreviousLogs
	Showing         bool
	PreviousProblem string
	// Ask says the page asks why the logs are read, for the access
	// register, before it shows them, and AskName who reads, on the
	// socket's panel; Reader, Purpose and Ticket are what was given, if
	// anything. Grant is the entry the page reads under.
	Ask, AskName            bool
	Reader, Purpose, Ticket string
	Grant                   access.Entry
}

// Link is this page's address with some of its choices changed: pairs of
// a name ("level", "q", "follow", "tail", "container") and its value, an
// empty value dropping it.
func (d logsData) Link(pairs ...string) string {
	values := url.Values{}
	set := func(name, value string) {
		if value == "" {
			values.Del(name)
		} else {
			values.Set(name, value)
		}
	}
	if d.Level != "all" {
		set("level", d.Level)
	}
	set("q", d.Query)
	if d.Follow {
		set("follow", "1")
	}
	if d.Tail != 300 {
		set("tail", strconv.Itoa(d.Tail))
	}
	if d.Showing {
		set("container", "previous")
	}
	for i := 0; i+1 < len(pairs); i += 2 {
		set(pairs[i], pairs[i+1])
	}
	if values.Get("level") == "all" {
		values.Del("level")
	}
	if len(values) == 0 {
		return "?"
	}
	return "?" + values.Encode()
}

// PreviousSaid is how a previous container went: "exit 137, 4m ago".
func (d logsData) PreviousSaid() string {
	p := d.Previous
	if p == nil {
		return ""
	}
	switch {
	case p.Why == "exited" || p.ExitKnown:
		return "exit " + strconv.Itoa(p.ExitCode) + ", " + ago(d.ReadAt, p.ExitedAt) + " ago"
	case p.Why == "stopped":
		return "stopped " + ago(d.ReadAt, p.At) + " ago"
	}
	return "replaced " + ago(d.ReadAt, p.At) + " ago"
}

// PreviousShort says it in a phone's switch: "137, 4m".
func (d logsData) PreviousShort() string {
	p := d.Previous
	if p == nil {
		return ""
	}
	switch {
	case p.Why == "exited" || p.ExitKnown:
		return strconv.Itoa(p.ExitCode) + ", " + ago(d.ReadAt, p.ExitedAt)
	case p.Why == "stopped":
		return "stopped, " + ago(d.ReadAt, p.At)
	}
	return "replaced, " + ago(d.ReadAt, p.At)
}

// PreviousWhy says in a sentence how the previous container ended.
func (d logsData) PreviousWhy() string {
	p := d.Previous
	if p == nil {
		return ""
	}
	at := p.At
	if !p.ExitedAt.IsZero() {
		at = p.ExitedAt
	}
	when := ago(d.ReadAt, at) + " ago (" + at.UTC().Format("2006-01-02 15:04:05") + " UTC)"
	switch {
	case p.ExitKnown && (p.OOMKilled || p.ExitCode == 137) && p.MemoryMB > 0:
		how := "was killed"
		if !p.OOMKilled {
			how = "was killed (exit 137), most likely"
		}
		return "Its app " + how + " for using more than its " + memory(p.MemoryMB) + " of memory " + when + "."
	case p.ExitKnown:
		return "Its app exited with code " + strconv.Itoa(p.ExitCode) + " " + when + "."
	case p.Why == "stopped":
		return "It was removed as the tenant was suspended, " + when + "."
	}
	return "A new container took its place " + when + "."
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
	var previous api.PreviousLogs
	var previousErr error
	// A tenant's logs are its customer's data: read under an entry of the
	// access register, which the page asks for first.
	reader := s.reader(r)
	grant, granted := s.grants.get(reader, name, s.now())
	if reader == "" || !granted {
		s.askAccess(w, r, name, "", logsData{}, http.StatusOK)
		return
	}
	proof := access.Proof{Grant: grant.ID}
	agent := s.Actions(grant.actor)
	if err := all(
		func() (err error) { data, err = agent.Logs(ctx, name, tail, proof); return },
		func() error { previous, previousErr = agent.PreviousLogs(ctx, name, proof); return nil },
		func() (err error) { t, err = s.backend.Get(ctx, name); return },
		func() (err error) { nodes, err = s.backend.Nodes(ctx); return },
	); err != nil {
		if refusedAccess(err) {
			s.grants.drop(reader, name)
			s.askAccess(w, r, name, err.Error(), logsData{Reader: strings.TrimSuffix(grant.actor, " (panel socket)"), Purpose: grant.Purpose, Ticket: grant.Ticket}, http.StatusOK)
			return
		}
		s.fail(w, r, err)
		return
	}
	// The newest lines, should a few be very long.
	if len(data) > logBytes {
		data = data[len(data)-logBytes:]
	}
	q := r.URL.Query()
	head := s.withApps(ctx, r, s.headOf(t, nodes, "logs"))
	head.Back = r.URL.RequestURI()
	// Its state as its other tabs say it: one that cannot write says so.
	if u, ok := s.usageNow(ctx).Tenants[t.Spec.Name]; ok {
		head.State = writable(head.State, &u)
	}
	logs := logsData{Tenant: name, Tail: tail, Head: head, Level: q.Get("level"), Query: strings.TrimSpace(q.Get("q")),
		Follow: q.Get("follow") != "", ReadAt: s.now(), Grant: grant.Entry}
	if logs.Level != "warn" && logs.Level != "error" {
		logs.Level = "all"
	}
	if t.Status != nil && t.Status.Restarts > 0 {
		logs.Restarted, logs.LastExit = true, t.Status.LastExit
	}
	var missing *api.Error
	switch {
	case previousErr == nil:
		logs.Previous = &previous
		if q.Get("container") == "previous" {
			// What was kept does not change: nothing to follow.
			logs.Showing, logs.Follow = true, false
			data = []byte(previous.Lines)
		}
	case errors.As(previousErr, &missing) && missing.Status == http.StatusNotFound:
	default:
		logs.PreviousProblem = previousErr.Error()
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
