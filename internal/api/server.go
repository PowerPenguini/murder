// Package api is the agent's local HTTP API on a unix socket, used by the CLI
// and, later, by the panel.
package api

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net"
	"net/http"
	"strconv"
	"sync"
	"sync/atomic"
	"time"

	"github.com/PowerPenguini/murder/internal/cluster"
	"github.com/PowerPenguini/murder/internal/edge"
	"github.com/PowerPenguini/murder/internal/reconcile"
	"github.com/PowerPenguini/murder/internal/runtime"
	"github.com/PowerPenguini/murder/internal/secrets"
	"github.com/PowerPenguini/murder/internal/spec"
	"github.com/PowerPenguini/murder/internal/store"
)

// Tenant is what the API shows about a tenant. Secret values never appear.
type Tenant struct {
	// Spec is what the tenant runs: its app's spec with its overrides.
	Spec      spec.Tenant    `json:"spec"`
	App       string         `json:"app"`
	Overrides spec.Overrides `json:"overrides"`
	// Node runs the tenant. Address is reachable from there only; Status
	// is asked of it when another node answers, and left out if it does
	// not answer in time.
	Node string `json:"node"`
	// Elsewhere is set when another node than this one runs the tenant.
	Elsewhere bool   `json:"elsewhere,omitempty"`
	Host      string `json:"host"`
	URL       string `json:"url"`
	// Address is where the tenant listens, reachable from this node only.
	Address    string    `json:"address,omitempty"`
	Generation int64     `json:"generation"`
	CreatedAt  time.Time `json:"createdAt"`
	UpdatedAt  time.Time `json:"updatedAt"`
	// ArchivedAt is set for an archived tenant, which runs nothing until it
	// is restored.
	ArchivedAt time.Time         `json:"archivedAt,omitzero"`
	Status     *reconcile.Status `json:"status,omitempty"`
}

// Archived reports whether the tenant is archived.
func (t Tenant) Archived() bool { return !t.ArchivedAt.IsZero() }

// TenantRequest creates a tenant (with Name) or reconfigures one.
type TenantRequest struct {
	Name      string         `json:"name,omitempty"`
	App       string         `json:"app"`
	Overrides spec.Overrides `json:"overrides"`
	// Generation, when reconfiguring, is the generation the client read; a
	// tenant changed since is refused with 409 instead of overwritten.
	Generation int64 `json:"generation,omitempty"`
}

// Applied is the result of creating or reconfiguring a tenant.
type Applied struct {
	Tenant Tenant `json:"tenant"`
	// Changed is false when the request matched what was stored.
	Changed bool `json:"changed"`
	// Note explains how an image was pinned when it fell back to a local
	// image.
	Note string `json:"note,omitempty"`
}

// App is what the API shows about an app.
type App struct {
	Name string      `json:"name"`
	Spec spec.Tenant `json:"spec"`
	// Tenants are the live tenants running the app.
	Tenants   []string  `json:"tenants"`
	CreatedAt time.Time `json:"createdAt"`
	UpdatedAt time.Time `json:"updatedAt"`
}

// AppApplied is the result of storing an app.
type AppApplied struct {
	App     App  `json:"app"`
	Created bool `json:"created"`
	Changed bool `json:"changed"`
	// Tenants are the live tenants the change rolls out to.
	Tenants []Tenant `json:"tenants"`
	Note    string   `json:"note,omitempty"`
}

// SecretValue carries one secret to an operator.
type SecretValue struct {
	Value string `json:"value"`
}

// Server serves the API.
type Server struct {
	store      store.Store
	reconciler *reconcile.Reconciler
	runtime    runtime.Runtime
	key        *secrets.Key
	log        *slog.Logger
	// cluster is this node's part of a cluster; nil outside one.
	cluster Cluster
	// stopping is closed when the agent stops, so requests that wait, such
	// as the edge's for new routes, end at once.
	stopping chan struct{}
	stopOnce sync.Once
	// upgrader installs new binaries on this node; nil if it takes none.
	upgrader Upgrader
	// relayPort is where the edges of a cluster pass requests on.
	relayPort int
	// moves moves tenants between nodes; nil if this node moves none.
	moves Moves
	// lastBackup is when a backup was last sent whole, in Unix nanoseconds.
	lastBackup atomic.Int64
}

// NewServer returns the API server.
func NewServer(st store.Store, reconciler *reconcile.Reconciler, rt runtime.Runtime, key *secrets.Key, log *slog.Logger) *Server {
	return &Server{store: st, reconciler: reconciler, runtime: rt, key: key, log: log, stopping: make(chan struct{})}
}

// WithRelayPort routes to tenants of other nodes through their edges'
// relay port.
func (s *Server) WithRelayPort(port int) *Server {
	s.relayPort = port
	return s
}

// Stop ends the requests that wait, before the server shuts down.
func (s *Server) Stop() { s.stopOnce.Do(func() { close(s.stopping) }) }

// Handler routes API requests.
func (s *Server) Handler() http.Handler {
	mux := http.NewServeMux()
	s.clusterRoutes(mux)
	s.upgradeRoutes(mux)
	mux.HandleFunc("GET /v1/routes", s.routes)
	mux.HandleFunc("GET /v1/apps", s.listApps)
	mux.HandleFunc("GET /v1/apps/{name}", s.getApp)
	mux.HandleFunc("PUT /v1/apps/{name}", s.putApp)
	mux.HandleFunc("DELETE /v1/apps/{name}", s.deleteApp)
	mux.HandleFunc("GET /v1/tenants", s.list)
	mux.HandleFunc("POST /v1/tenants", s.create)
	mux.HandleFunc("GET /v1/tenants/{name}", s.get)
	mux.HandleFunc("PUT /v1/tenants/{name}", s.configure)
	mux.HandleFunc("POST /v1/tenants/{name}/suspend", s.suspend(true))
	mux.HandleFunc("POST /v1/tenants/{name}/resume", s.suspend(false))
	mux.HandleFunc("POST /v1/tenants/{name}/archive", s.archive)
	mux.HandleFunc("POST /v1/tenants/{name}/restore", s.restore)
	mux.HandleFunc("DELETE /v1/tenants/{name}", s.purge)
	mux.HandleFunc("GET /v1/tenants/{name}/secrets/{key}", s.secret)
	mux.HandleFunc("POST /v1/tenants/{name}/secrets/{key}/rotate", s.rotate)
	mux.HandleFunc("POST /v1/tenants/{name}/roll", s.roll)
	mux.HandleFunc("POST /v1/tenants/{name}/move", s.move)
	mux.HandleFunc("GET /v1/tenants/{name}/logs", s.logs)
	mux.HandleFunc("GET /v1/events", s.events)
	mux.HandleFunc("POST /v1/reconcile", s.reconcileNow)
	mux.HandleFunc("GET /v1/backup", s.backup)
	return mux
}

func (s *Server) view(record store.Record) Tenant {
	view := Tenant{
		Spec: record.Spec, App: record.App, Overrides: record.Overrides, Node: record.Node,
		Host: s.reconciler.Host(record.Spec.Name), URL: s.reconciler.URL(record.Spec.Name),
		Generation: record.Generation, CreatedAt: record.CreatedAt, UpdatedAt: record.UpdatedAt, ArchivedAt: record.ArchivedAt,
	}
	view.Elsewhere = record.Node != s.store.Node()
	if record.Archived() || view.Elsewhere {
		return view
	}
	if address, err := reconcile.Backend(record); err == nil {
		view.Address = address
	}
	if status, ok := s.reconciler.Status(record.Spec.Name); ok {
		view.Status = &status
	}
	return view
}

func (s *Server) appView(app store.App) App {
	view := App{Name: app.Name, Spec: app.Spec, Tenants: []string{}, CreatedAt: app.CreatedAt, UpdatedAt: app.UpdatedAt}
	for _, record := range s.store.List() {
		if record.App == app.Name && !record.Archived() {
			view.Tenants = append(view.Tenants, record.Spec.Name)
		}
	}
	return view
}

func (s *Server) listApps(w http.ResponseWriter, _ *http.Request) {
	apps := s.store.Apps()
	views := make([]App, 0, len(apps))
	for _, app := range apps {
		views = append(views, s.appView(app))
	}
	writeJSON(w, http.StatusOK, views)
}

func (s *Server) getApp(w http.ResponseWriter, r *http.Request) {
	app, err := s.store.App(r.PathValue("name"))
	if err != nil {
		writeError(w, err)
		return
	}
	writeJSON(w, http.StatusOK, s.appView(app))
}

// putApp stores an app and rolls it out to its tenants. Everything but the
// image pin is checked first, so a broken spec never waits for a pull; the
// pull runs on the request's context, so a client that gives up cancels it.
func (s *Server) putApp(w http.ResponseWriter, r *http.Request) {
	name := r.PathValue("name")
	var app spec.Tenant
	if !readJSON(w, r, &app) {
		return
	}
	if err := s.store.CheckApp(store.App{Name: name, Spec: app}); err != nil {
		writeError(w, err)
		return
	}
	pinned, note, err := s.resolve(r.Context(), app.Image)
	if err != nil {
		writeError(w, err)
		return
	}
	app.Image = pinned
	if err := r.Context().Err(); err != nil {
		writeError(w, clientGone(fmt.Errorf("the client gave up while waiting; nothing was changed: %w", err)))
		return
	}
	change, err := s.store.PutApp(store.App{Name: name, Spec: app}, actor(r))
	if err != nil {
		writeError(w, err)
		return
	}
	result := AppApplied{App: s.appView(change.App), Created: change.Created, Changed: change.Changed, Tenants: []Tenant{}, Note: note}
	for _, tenant := range change.Tenants {
		if record, err := s.store.Get(tenant); err == nil {
			result.Tenants = append(result.Tenants, s.view(record))
		}
	}
	if change.Changed {
		s.log.Info("app stored", "app", name, "image", app.Image, "tenants", change.Tenants)
	}
	status := http.StatusOK
	if change.Created {
		status = http.StatusCreated
	}
	writeJSON(w, status, result)
}

func (s *Server) deleteApp(w http.ResponseWriter, r *http.Request) {
	name := r.PathValue("name")
	if err := s.store.DeleteApp(name, actor(r)); err != nil {
		writeError(w, err)
		return
	}
	s.log.Info("app deleted", "app", name)
	w.WriteHeader(http.StatusNoContent)
}

func (s *Server) list(w http.ResponseWriter, r *http.Request) {
	records := s.store.List()
	views := make([]Tenant, 0, len(records))
	for _, record := range records {
		views = append(views, s.view(record))
	}
	s.remoteStatus(r.Context(), views)
	writeJSON(w, http.StatusOK, views)
}

func (s *Server) get(w http.ResponseWriter, r *http.Request) {
	record, err := s.store.Get(r.PathValue("name"))
	if err != nil {
		writeError(w, err)
		return
	}
	views := []Tenant{s.view(record)}
	s.remoteStatus(r.Context(), views)
	writeJSON(w, http.StatusOK, views[0])
}

// remoteStatusTimeout bounds asking other nodes for their tenants' status:
// a node that is down leaves its tenants' status unknown instead of
// holding the answer.
const remoteStatusTimeout = 2 * time.Second

// remoteStatus fills in the status of tenants other nodes run, asking for
// all of them side by side, so that waiting for a tenant, or listing them,
// works from any node.
func (s *Server) remoteStatus(ctx context.Context, views []Tenant) {
	if s.cluster == nil {
		return
	}
	ctx, cancel := context.WithTimeout(ctx, remoteStatusTimeout)
	defer cancel()
	asking := make(chan struct{}, 16)
	var group sync.WaitGroup
	for i := range views {
		if !views[i].Elsewhere || views[i].Archived() {
			continue
		}
		group.Go(func() {
			asking <- struct{}{}
			defer func() { <-asking }()
			var status reconcile.Status
			if err := s.cluster.StatusOn(ctx, views[i].Node, views[i].Spec.Name, &status); err == nil {
				views[i].Status = &status
			}
		})
	}
	group.Wait()
}

func (s *Server) resolve(ctx context.Context, image string) (string, string, error) {
	pinned, note, err := s.runtime.Resolve(ctx, image)
	if err != nil {
		return "", "", statusError{http.StatusUnprocessableEntity, fmt.Errorf("pin image %s: %w", image, err)}
	}
	return pinned, note, nil
}

// prepareOverrides checks what a tenant of app with overrides would run and
// pins an image override, before anything is stored.
func (s *Server) prepareOverrides(r *http.Request, name, appName string, overrides spec.Overrides) (spec.Overrides, string, error) {
	app, err := s.store.App(appName)
	if err != nil {
		return spec.Overrides{}, "", err
	}
	if overrides.Image != "" {
		if err := spec.ValidReference(overrides.Image); err != nil {
			return spec.Overrides{}, "", badRequest(err)
		}
	}
	if err := spec.Compose(app.Spec, name, overrides).ValidateUnpinned(); err != nil {
		return spec.Overrides{}, "", badRequest(err)
	}
	note := ""
	if overrides.Image != "" {
		overrides.Image, note, err = s.resolve(r.Context(), overrides.Image)
		if err != nil {
			return spec.Overrides{}, "", err
		}
	}
	return overrides, note, nil
}

func (s *Server) create(w http.ResponseWriter, r *http.Request) {
	var request TenantRequest
	if !readJSON(w, r, &request) {
		return
	}
	if err := spec.ValidName(request.Name); err != nil {
		writeError(w, badRequest(err))
		return
	}
	overrides, note, err := s.prepareOverrides(r, request.Name, request.App, request.Overrides)
	if err != nil {
		writeError(w, err)
		return
	}
	ctx, cancel := detached(r)
	defer cancel()
	var record store.Record
	// Under the reconciler's lock, so a concurrent purge cannot interleave
	// with the check for leftover data.
	err = s.reconciler.Exclusive(func() error {
		if err := r.Context().Err(); err != nil {
			return clientGone(fmt.Errorf("the client gave up while waiting; nothing was created: %w", err))
		}
		if existing, err := s.store.Get(request.Name); err == nil && existing.Archived() {
			return store.ErrArchived
		} else if err == nil {
			return store.ErrExists
		}
		exists, err := s.reconciler.DataExists(ctx, request.Name)
		if err != nil {
			return err
		}
		if exists {
			return statusError{http.StatusConflict, fmt.Errorf("data of an earlier tenant %s is on this node but not in the archive; purge it first", request.Name)}
		}
		record, err = s.store.Create(store.NewTenant{Name: request.Name, App: request.App, Overrides: overrides}, actor(r))
		return err
	})
	if err != nil {
		writeError(w, err)
		return
	}
	s.log.Info("tenant created", "tenant", request.Name, "app", request.App, "image", record.Spec.Image)
	writeJSON(w, http.StatusCreated, Applied{Tenant: s.view(record), Changed: true, Note: note})
}

// configure gives a tenant, live or archived, another app or other
// overrides. The request replaces both; omitted overrides are dropped. With
// a generation it is refused if the tenant changed since the client read
// it. Suspension is operational state and stays as it is. A request equal
// to what is stored changes nothing, not even the generation.
func (s *Server) configure(w http.ResponseWriter, r *http.Request) {
	name := r.PathValue("name")
	var request TenantRequest
	if !readJSON(w, r, &request) {
		return
	}
	if request.Name != "" && request.Name != name {
		writeError(w, badRequest(fmt.Errorf("request names tenant %q, not %q", request.Name, name)))
		return
	}
	if _, err := s.store.Get(name); err != nil {
		writeError(w, err)
		return
	}
	// Checked against what exists now; the store checks again when it
	// writes.
	overrides, note, err := s.prepareOverrides(r, name, request.App, request.Overrides)
	if err != nil {
		writeError(w, err)
		return
	}
	if err := r.Context().Err(); err != nil {
		writeError(w, clientGone(fmt.Errorf("the client gave up while waiting; nothing was changed: %w", err)))
		return
	}
	record, changed, err := s.store.Configure(name, request.App, overrides, request.Generation, actor(r))
	if err != nil {
		writeError(w, err)
		return
	}
	if changed {
		s.log.Info("tenant updated", "tenant", name, "app", request.App, "generation", record.Generation, "image", record.Spec.Image)
	}
	writeJSON(w, http.StatusOK, Applied{Tenant: s.view(record), Changed: changed, Note: note})
}

func (s *Server) suspend(suspended bool) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		record, err := s.store.SetSuspended(r.PathValue("name"), suspended, actor(r))
		if err != nil {
			writeError(w, err)
			return
		}
		s.log.Info("tenant suspension changed", "tenant", record.Spec.Name, "suspended", suspended)
		writeJSON(w, http.StatusOK, s.view(record))
	}
}

// archive takes a tenant out of service and keeps everything needed to
// restore it. The edge stops routing to it at once; the next reconcile
// pass, which the change starts, removes its container and network.
func (s *Server) archive(w http.ResponseWriter, r *http.Request) {
	name := r.PathValue("name")
	if err := s.store.Archive(name, actor(r)); err != nil {
		writeError(w, err)
		return
	}
	s.log.Info("tenant archived", "tenant", name)
	w.WriteHeader(http.StatusNoContent)
}

func (s *Server) restore(w http.ResponseWriter, r *http.Request) {
	name := r.PathValue("name")
	var record store.Record
	err := s.reconciler.Exclusive(func() error {
		var err error
		record, err = s.store.Restore(name, actor(r))
		return err
	})
	if err != nil {
		writeError(w, err)
		return
	}
	s.log.Info("tenant restored", "tenant", name)
	writeJSON(w, http.StatusOK, s.view(record))
}

// purge deletes an archived tenant, or data no archive entry accounts for.
func (s *Server) purge(w http.ResponseWriter, r *http.Request) {
	name := r.PathValue("name")
	if err := spec.ValidLabel(name); err != nil {
		writeError(w, badRequest(err))
		return
	}
	ctx, cancel := detached(r)
	defer cancel()
	if err := s.reconciler.Purge(ctx, name, actor(r)); err != nil {
		writeError(w, err)
		return
	}
	s.log.Info("tenant purged", "tenant", name)
	w.WriteHeader(http.StatusNoContent)
}

// detached gives work that changes the runtime its own deadline, so a client
// that disconnects cannot leave it half done.
func detached(r *http.Request) (context.Context, context.CancelFunc) {
	return context.WithTimeout(context.WithoutCancel(r.Context()), 10*time.Minute)
}

func (s *Server) secret(w http.ResponseWriter, r *http.Request) {
	name, key := r.PathValue("name"), r.PathValue("key")
	record, err := s.store.Live(name)
	if err != nil {
		writeError(w, err)
		return
	}
	sealed, ok := record.Sealed[key]
	if !ok {
		writeError(w, notFound(fmt.Errorf("tenant %s has no secret %s", name, key)))
		return
	}
	value, err := s.key.Open(secrets.Context(name, key), sealed)
	if err != nil {
		writeError(w, err)
		return
	}
	s.log.Info("secret read", "tenant", name, "secret", key, "actor", actor(r))
	writeJSON(w, http.StatusOK, SecretValue{Value: value})
}

// rotate gives one secret of the spec a new value; the container is
// replaced with it. Values are otherwise generated once and kept, even when
// a secret leaves the spec and comes back.
// roll replaces a tenant's container as it is, made as this release makes
// containers.
func (s *Server) roll(w http.ResponseWriter, r *http.Request) {
	record, err := s.store.Roll(r.PathValue("name"), actor(r))
	if err != nil {
		writeError(w, err)
		return
	}
	writeJSON(w, http.StatusOK, s.view(record))
}

func (s *Server) rotate(w http.ResponseWriter, r *http.Request) {
	name, key := r.PathValue("name"), r.PathValue("key")
	record, err := s.store.Rotate(name, key, actor(r))
	if err != nil {
		writeError(w, err)
		return
	}
	s.log.Info("secret rotated", "tenant", name, "secret", key, "generation", record.Generation)
	writeJSON(w, http.StatusOK, s.view(record))
}

func (s *Server) logs(w http.ResponseWriter, r *http.Request) {
	name := r.PathValue("name")
	record, err := s.store.Live(name)
	if err != nil {
		writeError(w, err)
		return
	}
	if record.Node != s.store.Node() {
		writeError(w, fmt.Errorf("%w: tenant %s runs on node %s: read its logs there", reconcile.ErrElsewhere, name, record.Node))
		return
	}
	tail := 200
	if value := r.URL.Query().Get("tail"); value != "" {
		parsed, err := strconv.Atoi(value)
		if err != nil || parsed < 1 || parsed > 10000 {
			writeError(w, badRequest(errors.New("tail must be 1-10000")))
			return
		}
		tail = parsed
	}
	status, ok := s.reconciler.Status(name)
	if !ok || status.ContainerID == "" {
		writeError(w, notFound(fmt.Errorf("tenant %s has no container", name)))
		return
	}
	output, err := s.runtime.Logs(r.Context(), status.ContainerID, tail)
	if err != nil {
		writeError(w, err)
		return
	}
	w.Header().Set("Content-Type", "text/plain; charset=utf-8")
	_, _ = w.Write(output)
}

// events returns the audit log, newest first: ?tenant=, ?app=, ?before=ID
// for the next page and ?limit= (at most 1000).
func (s *Server) events(w http.ResponseWriter, r *http.Request) {
	query := store.EventQuery{Tenant: r.URL.Query().Get("tenant"), App: r.URL.Query().Get("app")}
	if value := r.URL.Query().Get("before"); value != "" {
		parsed, err := strconv.ParseInt(value, 10, 64)
		if err != nil || parsed < 1 {
			writeError(w, badRequest(errors.New("before must be a positive event ID")))
			return
		}
		query.Before = parsed
	}
	if value := r.URL.Query().Get("limit"); value != "" {
		parsed, err := strconv.Atoi(value)
		if err != nil || parsed < 1 || parsed > 1000 {
			writeError(w, badRequest(errors.New("limit must be 1-1000")))
			return
		}
		query.Limit = parsed
	}
	events, err := s.store.Events(query)
	if err != nil {
		writeError(w, err)
		return
	}
	writeJSON(w, http.StatusOK, events)
}

// reconcileNow runs a pass at once, retrying failed pulls without waiting
// for their delay and waiting for the pulls it starts.
func (s *Server) reconcileNow(w http.ResponseWriter, r *http.Request) {
	ctx, cancel := detached(r)
	defer cancel()
	if err := s.reconciler.ReconcileNow(ctx); err != nil {
		writeError(w, err)
		return
	}
	w.WriteHeader(http.StatusNoContent)
}

// backup streams a copy of the database. A failure before the first byte is
// an error response; one after it cuts the connection, so the client never
// takes a partial copy for a whole one.
func (s *Server) backup(w http.ResponseWriter, _ *http.Request) {
	w.Header().Set("Content-Type", "application/vnd.sqlite3")
	out := &countingWriter{w: w}
	if err := s.store.Backup(out); err != nil {
		if out.n == 0 {
			w.Header().Del("Content-Type")
			writeError(w, err)
			return
		}
		s.log.Error("back up the database", "error", err)
		panic(http.ErrAbortHandler)
	}
	s.lastBackup.Store(time.Now().UnixNano())
}

// LastBackup returns when a backup was last sent whole since the agent
// started; zero if none was.
func (s *Server) LastBackup() time.Time {
	if at := s.lastBackup.Load(); at != 0 {
		return time.Unix(0, at)
	}
	return time.Time{}
}

type countingWriter struct {
	w io.Writer
	n int64
}

func (c *countingWriter) Write(p []byte) (int, error) {
	n, err := c.w.Write(p)
	c.n += int64(n)
	return n, err
}

// Routes adapts the store for the edge proxy.
type Routes struct {
	Store store.Store
	// RelayPort is where every node's edge takes requests other nodes pass
	// on; 0 outside a cluster.
	RelayPort int
	// Replacing reports a tenant of this node whose container is being
	// replaced; nil if none ever is.
	Replacing func(tenant string) bool
}

func (r Routes) Route(tenant string) (edge.Route, bool) {
	record, err := r.Store.Live(tenant)
	if err != nil {
		return edge.Route{}, false
	}
	// Moving, it runs nowhere for a moment: requests wait for it.
	if record.MoveHeld {
		return edge.Route{Moving: true, Suspended: record.Spec.Suspended}, true
	}
	if record.Node != r.Store.Node() {
		return r.relay(record)
	}
	// So does a tenant whose container is being replaced. Other nodes' edges
	// pass its requests on to this one, which holds them.
	if r.Replacing != nil && r.Replacing(tenant) {
		return edge.Route{Moving: true, Suspended: record.Spec.Suspended}, true
	}
	backend, err := reconcile.Backend(record)
	if err != nil {
		return edge.Route{}, false
	}
	return edge.Route{Backend: backend, Suspended: record.Spec.Suspended}, true
}

// relay routes to the edge of the node that runs the tenant, on the private
// network, at the host of its cluster address.
func (r Routes) relay(record store.Record) (edge.Route, bool) {
	if r.RelayPort == 0 {
		return edge.Route{}, false
	}
	for _, node := range r.Store.Nodes() {
		if node.Name != record.Node {
			continue
		}
		host, _, err := net.SplitHostPort(node.Address)
		if err != nil {
			return edge.Route{}, false
		}
		return edge.Route{Relay: net.JoinHostPort(host, strconv.Itoa(r.RelayPort)), Suspended: record.Spec.Suspended}, true
	}
	return edge.Route{}, false
}

type statusError struct {
	status int
	err    error
}

func (e statusError) Error() string { return e.err.Error() }
func (e statusError) Unwrap() error { return e.err }

func badRequest(err error) error { return statusError{http.StatusBadRequest, err} }
func notFound(err error) error   { return statusError{http.StatusNotFound, err} }

// clientGone marks work refused because the client disconnected: nginx's 499,
// not a server failure.
func clientGone(err error) error { return statusError{499, err} }

type errorBody struct {
	Error string `json:"error"`
}

func writeError(w http.ResponseWriter, err error) {
	status := http.StatusInternalServerError
	var typed statusError
	var peer *cluster.PeerError
	switch {
	case errors.As(err, &typed):
		status = typed.status
	case errors.As(err, &peer) && peer.Status >= 400 && peer.Status < 500:
		status = peer.Status
	case errors.Is(err, store.ErrNotFound):
		status = http.StatusNotFound
	case errors.Is(err, store.ErrExists), errors.Is(err, store.ErrArchived), errors.Is(err, store.ErrNotArchived), errors.Is(err, store.ErrInUse), errors.Is(err, store.ErrConflict):
		status = http.StatusConflict
	case errors.As(err, new(*spec.ValidationError)):
		status = http.StatusBadRequest
	case errors.Is(err, reconcile.ErrElsewhere):
		status = http.StatusConflict
	case errors.Is(err, reconcile.ErrStopping), errors.Is(err, cluster.ErrNoLeader), errors.Is(err, cluster.ErrNotLeader):
		status = http.StatusServiceUnavailable
	case errors.Is(err, store.ErrStale), errors.Is(err, store.ErrFrozen), errors.Is(err, cluster.ErrNotJoined), errors.Is(err, cluster.ErrMember):
		status = http.StatusConflict
	case errors.Is(err, context.Canceled):
		status = 499
	}
	writeJSON(w, status, errorBody{Error: err.Error()})
}

func readJSON(w http.ResponseWriter, r *http.Request, out any) bool {
	decoder := json.NewDecoder(io.LimitReader(r.Body, 1<<20))
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(out); err != nil {
		writeError(w, badRequest(fmt.Errorf("request body: %w", err)))
		return false
	}
	return true
}

func writeJSON(w http.ResponseWriter, status int, value any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(value)
}
