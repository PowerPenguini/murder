// Package panel serves Murder's administration panel: the cluster, its
// nodes and tenants, and the audit log, as the agent's API reports them.
// It holds no state of its own; every page asks the agent.
package panel

import (
	"context"
	"embed"
	"errors"
	"fmt"
	"html/template"
	"io/fs"
	"log/slog"
	"net/http"
	"strconv"
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

// Server serves the panel's pages.
type Server struct {
	backend Backend
	log     *slog.Logger
	now     func() time.Time
	pages   map[string]*template.Template
}

// New returns a panel asking backend.
func New(backend Backend, log *slog.Logger) (*Server, error) {
	s := &Server{backend: backend, log: log, now: time.Now, pages: map[string]*template.Template{}}
	for _, page := range []string{"overview", "tenant", "events", "error"} {
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
	mux.Handle("GET /static/", http.StripPrefix("/static/", cacheFor(time.Hour, files)))
	mux.HandleFunc("GET /{$}", s.overview)
	mux.HandleFunc("GET /tenants/{name}", s.tenant)
	mux.HandleFunc("GET /events", s.events)
	return secure(mux)
}

// secure sets the headers every answer carries: nothing is framed, sniffed
// or loaded from elsewhere.
func secure(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		h := w.Header()
		h.Set("Content-Security-Policy", "default-src 'none'; style-src 'self'; img-src 'self' data:; script-src 'self'; connect-src 'self'; form-action 'self'; frame-ancestors 'none'; base-uri 'none'")
		h.Set("X-Content-Type-Options", "nosniff")
		h.Set("Referrer-Policy", "no-referrer")
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
		c.Header().Set("Cache-Control", c.value)
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
	Data    any
}

func (s *Server) render(w http.ResponseWriter, r *http.Request, name, title, section string, data any) {
	ctx, cancel := context.WithTimeout(r.Context(), 10*time.Second)
	defer cancel()
	nodes, err := s.backend.Nodes(ctx)
	if err != nil {
		s.fail(w, r, err)
		return
	}
	s.write(w, name, page{Title: title, Section: section, Cluster: nodes, Now: s.now(), Data: data}, http.StatusOK)
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
	s.write(w, "error", page{Title: "Error", Now: s.now(), Data: err.Error()}, status)
}
