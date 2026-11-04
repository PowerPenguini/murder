// Package panel serves Murder's administration panel: the cluster, its
// nodes and tenants, and the audit log, as the agent's API reports them.
// It holds no state of its own; every page asks the agent.
package panel

import (
	"context"
	"crypto/sha256"
	"embed"
	"encoding/hex"
	"errors"
	"fmt"
	"html/template"
	"io"
	"io/fs"
	"log/slog"
	"net"
	"net/http"
	"net/netip"
	"net/url"
	"strconv"
	"strings"
	"time"

	"github.com/PowerPenguini/murder/internal/api"
	"github.com/PowerPenguini/murder/internal/store"
)

// Backend is what the panel asks of the agent: its API client.
type Backend interface {
	Nodes(ctx context.Context) (api.Nodes, error)
	List(ctx context.Context) ([]api.Tenant, error)
	Get(ctx context.Context, name string) (api.Tenant, error)
	Events(ctx context.Context, query store.EventQuery) ([]store.Event, error)
}

//go:embed templates/*.html
var templateFiles embed.FS

//go:embed static
var staticFiles embed.FS

// staticVersion changes with the static files, so that a browser never
// keeps one from an earlier release: their addresses carry it.
var staticVersion = func() string {
	hash := sha256.New()
	_ = fs.WalkDir(staticFiles, "static", func(path string, entry fs.DirEntry, err error) error {
		if err != nil || entry.IsDir() {
			return err
		}
		data, err := staticFiles.ReadFile(path)
		hash.Write([]byte(path))
		hash.Write(data)
		return err
	})
	return hex.EncodeToString(hash.Sum(nil))[:12]
}()

// staticURL is where the page asks for a static file.
func staticURL(name string) string { return "/static/" + name + "?v=" + staticVersion }

// Server serves the panel's pages.
type Server struct {
	// Name is the panel's host name in its tunnel; requests are answered
	// only as it or as a loopback name.
	Name string
	// Auth, if set, signs people in: every page but signing in needs it.
	// The socket's panel, which root reaches through SSH, has none.
	Auth *Auth
	// Access, if set, serves the people and devices page, acting for the
	// person signed in; TunnelNetwork is where devices get addresses.
	Access        func(actor string) AccessAgent
	TunnelNetwork netip.Prefix
	// Actions, if set, acts on tenants for the person signed in; Cluster
	// on the cluster.
	Actions func(actor string) ActionAgent
	Cluster func(actor string) ClusterAgent
	// Apps, when set, edits apps and tenants' settings.
	Apps func(actor string) AppAgent
	// Jobs are the panel's background work; panels of one process share
	// them.
	Jobs *Jobs
	// UploadDir keeps uploaded binaries until they are installed; the
	// system's temporary directory if empty.
	UploadDir string
	// bodyTime is how long a body may take to arrive; bodyTime if zero.
	bodyTime time.Duration
	backend  Backend
	log      *slog.Logger
	now      func() time.Time
	pages    map[string]*template.Template
}

// New returns a panel asking backend.
func New(backend Backend, log *slog.Logger) (*Server, error) {
	s := &Server{backend: backend, log: log, now: time.Now, pages: map[string]*template.Template{}, Jobs: &Jobs{}}
	for _, page := range []string{"overview", "tenants", "tenant", "events", "error", "login", "enroll", "access", "logs", "cluster", "job", "apps", "app", "settings", "node", "review"} {
		t, err := template.New("layout.html").Funcs(s.funcs()).ParseFS(templateFiles, "templates/layout.html", "templates/partials.html", "templates/"+page+".html")
		if err != nil {
			return nil, fmt.Errorf("panel template %s: %w", page, err)
		}
		s.pages[page] = t
	}
	return s, nil
}

// Handler serves the panel.
func (s *Server) Handler() http.Handler {
	mux := http.NewServeMux()
	static, _ := fs.Sub(staticFiles, "static")
	files := http.FileServerFS(static)
	mux.Handle("GET /static/", http.StripPrefix("/static/", noListing(cacheFor(24*time.Hour, files))))
	pages := http.NewServeMux()
	pages.HandleFunc("GET /{$}", s.overview)
	pages.HandleFunc("GET /tenants", s.tenants)
	pages.HandleFunc("GET /tenants/{name}", s.tenant)
	pages.HandleFunc("GET /events", s.events)
	pages.HandleFunc("GET /nodes/{name}", s.node)
	if s.Access != nil {
		pages.HandleFunc("GET /access", s.access)
		pages.HandleFunc("POST /access", s.access)
	}
	if s.Actions != nil {
		pages.HandleFunc("POST /tenants", s.createTenant)
		pages.HandleFunc("POST /tenants/{name}", s.tenantAction)
		pages.HandleFunc("GET /tenants/{name}/logs", s.logs)
	}
	if s.Apps != nil {
		pages.HandleFunc("GET /apps", s.apps)
		pages.HandleFunc("POST /apps", s.createApp)
		pages.HandleFunc("GET /apps/{name}", s.app)
		pages.HandleFunc("POST /apps/{name}", s.appAction)
		pages.HandleFunc("GET /tenants/{name}/settings", s.settings)
		pages.HandleFunc("POST /tenants/{name}/settings", s.settingsAction)
	}
	if s.Cluster != nil {
		pages.HandleFunc("GET /cluster", s.cluster)
		pages.HandleFunc("POST /cluster", s.clusterAction)
		pages.HandleFunc("POST /cluster/upgrade", s.upgradeAction)
		pages.HandleFunc("GET /jobs/{id}", s.jobPage)
	}
	if s.Auth != nil {
		mux.HandleFunc("GET /login", s.gate("login", "Sign in"))
		mux.HandleFunc("GET /enroll", s.gate("enroll", "Add a passkey"))
		mux.HandleFunc("POST /auth/login/begin", s.Auth.loginBegin)
		mux.HandleFunc("POST /auth/login/finish", s.Auth.loginFinish)
		mux.HandleFunc("POST /auth/enroll/begin", s.Auth.enrollBegin)
		mux.HandleFunc("POST /auth/enroll/finish", s.Auth.enrollFinish)
		mux.HandleFunc("POST /auth/logout", s.Auth.logout)
	}
	mux.Handle("/", s.signedIn(pages))
	// Bounded outermost: a request refused before its body is read still
	// has its body held to a time, which the server's draining of it
	// keeps to.
	return s.bounded(s.ownNames(secure(s.sameOrigin(mux))))
}

// Bounds of what a request sends: a sign-in's credential is small, a form
// (an app's spec) larger; an upgrade's binary has its own bound. Each must
// arrive within its time, so a slow sender holds nothing for long.
const (
	authBody       = 64 << 10
	formBody       = 2 << 20
	bodyTime       = 30 * time.Second
	uploadBodyTime = 15 * time.Minute
)

// IdleTimeout is how long the panel's servers keep a connection between
// requests.
const IdleTimeout = 2 * time.Minute

// bounded holds every request's body to its bound and time, before anyone,
// signed in or not, reads it, or refuses the request unread: what is left
// of a body is drained after the answer, and only within its time. Once
// the body is read to its end the time no longer runs: on HTTP/1 the
// connection is then watched for the client leaving, and a deadline there
// would end the request's context. Requests without a body set none, for
// that reason too.
func (s *Server) bounded(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		limit, within := int64(formBody), bodyTime
		if s.bodyTime > 0 {
			within = s.bodyTime
		}
		switch {
		case strings.HasPrefix(r.URL.Path, "/auth/"):
			limit = authBody
		case r.URL.Path == "/cluster/upgrade":
			limit, within = -1, uploadBodyTime
		}
		// No body for certain only when the server says so: an HTTP/2
		// stream with Content-Length 0 may still not have ended.
		if r.Body == nil || r.Body == http.NoBody {
			next.ServeHTTP(w, r)
			return
		}
		if limit > 0 {
			r.Body = http.MaxBytesReader(w, r.Body, limit)
		}
		control := http.NewResponseController(w)
		// Servers that cannot set deadlines (tests' recorders) go without.
		if control.SetReadDeadline(time.Now().Add(within)) == nil {
			r.Body = &untimedAtEnd{ReadCloser: r.Body, control: control}
		}
		next.ServeHTTP(w, r)
	})
}

// untimedAtEnd lifts the read deadline once its body has been read to its
// end.
type untimedAtEnd struct {
	io.ReadCloser
	control *http.ResponseController
	done    bool
}

func (u *untimedAtEnd) Read(p []byte) (int, error) {
	n, err := u.ReadCloser.Read(p)
	if errors.Is(err, io.EOF) && !u.done {
		u.done = true
		_ = u.control.SetReadDeadline(time.Time{})
	}
	return n, err
}

// gate serves a page of signing in, which needs no session.
func (s *Server) gate(name, title string) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		s.write(w, name, page{Title: title, Now: s.now(), Gate: true}, http.StatusOK)
	}
}

type userKey struct{}

// signedIn lets through those signed in, sending the rest to sign in.
func (s *Server) signedIn(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if s.Auth == nil {
			next.ServeHTTP(w, r)
			return
		}
		user, ok := s.Auth.User(r)
		if !ok {
			if r.Method == http.MethodGet {
				// Signed in, the person comes back here.
				http.Redirect(w, r, "/login?next="+url.QueryEscape(r.URL.RequestURI()), http.StatusSeeOther)
				return
			}
			http.Error(w, "sign in first", http.StatusUnauthorized)
			return
		}
		next.ServeHTTP(w, r.WithContext(context.WithValue(r.Context(), userKey{}, user)))
	})
}

// sameOrigin refuses changes asked from pages elsewhere (CSRF): with the
// session cookie SameSite=Strict, and the browser's word where they come
// from.
func (s *Server) sameOrigin(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method == http.MethodGet || r.Method == http.MethodHead {
			next.ServeHTTP(w, r)
			return
		}
		// The socket's panel, reached as localhost through SSH, takes
		// changes from its own pages only too: a page elsewhere could
		// post to the forwarded port.
		ok := sameOrigin(r, []string{"http://" + r.Host, "https://" + r.Host})
		if s.Auth != nil {
			ok = s.Auth.sameOrigin(r)
		}
		if !ok {
			http.Error(w, "the panel takes changes from its own pages only", http.StatusForbidden)
			return
		}
		next.ServeHTTP(w, r)
	})
}

// userOf is the person r is signed in as; "" on the socket's panel.
func userOf(r *http.Request) string {
	user, _ := r.Context().Value(userKey{}).(string)
	return user
}

// ownNames refuses requests addressed to another name than the panel's, or
// a loopback one through an SSH tunnel: a page elsewhere that points its
// own name at the panel's address (DNS rebinding) reads nothing.
func (s *Server) ownNames(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		host := r.Host
		if h, _, err := net.SplitHostPort(host); err == nil {
			host = h
		}
		host = strings.ToLower(strings.TrimSuffix(strings.Trim(host, "[]"), "."))
		ok := host == "localhost" || (s.Name != "" && host == strings.ToLower(s.Name))
		if address, err := netip.ParseAddr(host); err == nil && address.IsLoopback() {
			ok = true
		}
		if !ok {
			http.Error(w, "the panel answers only as its own name", http.StatusMisdirectedRequest)
			return
		}
		next.ServeHTTP(w, r)
	})
}

// noListing serves files, never a directory's listing.
func noListing(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "" || strings.HasSuffix(r.URL.Path, "/") {
			http.NotFound(w, r)
			return
		}
		next.ServeHTTP(w, r)
	})
}

// secure sets the headers every answer carries: nothing is framed, sniffed
// or loaded from elsewhere.
func secure(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		h := w.Header()
		h.Set("Content-Security-Policy", "default-src 'none'; style-src 'self'; img-src 'self' data:; script-src 'self'; font-src 'self'; connect-src 'self'; form-action 'self'; frame-ancestors 'none'; base-uri 'none'")
		h.Set("X-Content-Type-Options", "nosniff")
		// Not no-referrer: browsers then send forms with "Origin: null",
		// and the panel could not tell its own from others'.
		h.Set("Referrer-Policy", "same-origin")
		h.Set("Cache-Control", "no-store")
		next.ServeHTTP(w, r)
	})
}

func cacheFor(d time.Duration, next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		next.ServeHTTP(&cacheWriter{ResponseWriter: w, value: "public, max-age=" + strconv.Itoa(int(d.Seconds()))}, r)
	})
}

// cacheWriter replaces secure's no-store for static files.
type cacheWriter struct {
	http.ResponseWriter
	value string
	done  bool
}

func (c *cacheWriter) WriteHeader(status int) {
	if !c.done {
		// Only what was found is kept: not a 404.
		if status == http.StatusOK || status == http.StatusPartialContent || status == http.StatusNotModified {
			c.Header().Set("Cache-Control", c.value)
		}
		c.done = true
	}
	c.ResponseWriter.WriteHeader(status)
}

func (c *cacheWriter) Write(data []byte) (int, error) {
	if !c.done {
		c.WriteHeader(http.StatusOK)
	}
	return c.ResponseWriter.Write(data)
}

// page is what every page's layout shows.
type page struct {
	Title   string
	Section string
	Cluster api.Nodes
	Now     time.Time
	// User is the person signed in; "" on a page of signing in, or on
	// the socket's panel.
	User string
	// Gate marks a page of signing in: no navigation.
	Gate bool
	// Acts says the person may act, not only look; Done and Problem are
	// what their last action did.
	Acts          bool
	Done, Problem string
	// Edits says apps and tenants' settings may be edited.
	Edits bool
	// Alerts count what is broken, for the side bar.
	Alerts navAlerts
	Data   any
}

func (s *Server) render(w http.ResponseWriter, r *http.Request, name, title, section string, data any) {
	ctx, cancel := context.WithTimeout(r.Context(), 10*time.Second)
	defer cancel()
	nodes, err := s.backend.Nodes(ctx)
	if err != nil {
		s.fail(w, r, err)
		return
	}
	s.write(w, name, s.pageFor(w, r, title, section, nodes, data), http.StatusOK)
}

// renderWith renders a page saying problem, such as a form refused and
// shown again with what was typed.
func (s *Server) renderWith(w http.ResponseWriter, r *http.Request, name, title, section string, data any, problem string, status int) {
	ctx, cancel := context.WithTimeout(r.Context(), 10*time.Second)
	defer cancel()
	nodes, err := s.backend.Nodes(ctx)
	if err != nil {
		s.fail(w, r, err)
		return
	}
	p := s.pageFor(w, r, title, section, nodes, data)
	if problem != "" {
		p.Problem = problem
	}
	s.write(w, name, p, status)
}

// pageFor is a page's layout data: who looks, whether they may act, and
// what their last action did.
func (s *Server) pageFor(w http.ResponseWriter, r *http.Request, title, section string, nodes api.Nodes, data any) page {
	p := page{Title: title, Section: section, Cluster: nodes, Now: s.now(), User: userOf(r), Acts: s.Actions != nil, Edits: s.Apps != nil, Data: data}
	p.Alerts = s.alertsFor(r, nodes, data)
	// A page following the cluster leaves what an action did to the page
	// it was meant for.
	if r.Header.Get("X-Murder-Live") == "" {
		p.Done, p.Problem = takeFlash(w, r)
	}
	return p
}

// alertsFor counts what is broken in the cluster, for the side bar: from
// the tenants a page read already, or asked for.
func (s *Server) alertsFor(r *http.Request, nodes api.Nodes, data any) navAlerts {
	if d, ok := data.(overviewData); ok {
		return alertsOf(d.Attention, d.FailingBackups)
	}
	ctx, cancel := context.WithTimeout(r.Context(), 3*time.Second)
	defer cancel()
	var tenants []api.Tenant
	var use clusterUse
	_ = all(
		func() (err error) { tenants, err = s.backend.List(ctx); return },
		func() error { use = s.usageNow(ctx); return nil },
	)
	notices, failing := s.triage(nodes, tenants, use)
	return alertsOf(notices, failing)
}

func (s *Server) write(w http.ResponseWriter, name string, p page, status int) {
	w.Header().Set("Content-Type", "text/html; charset=utf-8")
	w.WriteHeader(status)
	if err := s.pages[name].ExecuteTemplate(w, "layout.html", p); err != nil {
		s.log.Error("render a panel page", "page", name, "error", err)
	}
}

// fail shows what went wrong asking the agent.
func (s *Server) fail(w http.ResponseWriter, r *http.Request, err error) {
	status := http.StatusBadGateway
	if api.IsNotFound(err) {
		status = http.StatusNotFound
	}
	if errors.Is(err, context.Canceled) {
		return
	}
	s.log.Warn("a panel page failed", "path", r.URL.Path, "error", err)
	fault := fault{Detail: err.Error(), Title: "The agent did not answer as expected", Hint: "It may be restarting: try again in a moment. If it lasts, see its log: journalctl -u murder-agent."}
	var apiErr *api.Error
	switch {
	case status == http.StatusNotFound:
		fault.Title, fault.Hint = "Not found", "It may have been purged, renamed or never made; the overview lists what there is."
	case errors.As(err, &apiErr) && apiErr.Status < 500:
		fault.Title, fault.Hint = "The agent refused this", ""
		status = apiErr.Status
	case errors.Is(err, context.DeadlineExceeded):
		fault.Title, fault.Hint = "The agent took too long to answer", "A node it asked may be down. Try again in a moment."
		status = http.StatusGatewayTimeout
	}
	p := page{Title: fault.Title, Now: s.now(), User: userOf(r), Acts: s.Actions != nil, Edits: s.Apps != nil, Data: fault}
	s.write(w, "error", p, status)
}

// fault is what the error page says: what happened, the agent's words, and
// what to do.
type fault struct{ Title, Detail, Hint string }
