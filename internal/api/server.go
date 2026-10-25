// Package api is the agent's local HTTP API on a unix socket, used by the CLI.
package api

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"maps"
	"net/http"
	"strconv"
	"time"

	"github.com/PowerPenguini/corral/internal/edge"
	"github.com/PowerPenguini/corral/internal/reconcile"
	"github.com/PowerPenguini/corral/internal/runtime"
	"github.com/PowerPenguini/corral/internal/secrets"
	"github.com/PowerPenguini/corral/internal/spec"
	"github.com/PowerPenguini/corral/internal/store"
)

// Tenant is what the API shows about a tenant. Secret values never appear.
type Tenant struct {
	Spec       spec.Tenant       `json:"spec"`
	Host       string            `json:"host"`
	URL        string            `json:"url"`
	HostPort   int               `json:"hostPort"`
	Generation int64             `json:"generation"`
	CreatedAt  time.Time         `json:"createdAt"`
	UpdatedAt  time.Time         `json:"updatedAt"`
	Status     *reconcile.Status `json:"status,omitempty"`
}

// Applied is the result of creating or updating a tenant.
type Applied struct {
	Tenant Tenant `json:"tenant"`
	// Changed is false when the update matched what was stored.
	Changed bool `json:"changed"`
	// Note explains how the image was pinned when it fell back to a local
	// image.
	Note string `json:"note,omitempty"`
}

// ArchivedTenant is what the API shows about an archived tenant.
type ArchivedTenant struct {
	Name       string    `json:"name"`
	Image      string    `json:"image"`
	Suspended  bool      `json:"suspended"`
	ArchivedAt time.Time `json:"archivedAt"`
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
}

// NewServer returns the API server.
func NewServer(st store.Store, reconciler *reconcile.Reconciler, rt runtime.Runtime, key *secrets.Key, log *slog.Logger) *Server {
	return &Server{store: st, reconciler: reconciler, runtime: rt, key: key, log: log}
}

// Handler routes API requests.
func (s *Server) Handler() http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("GET /v1/tenants", s.list)
	mux.HandleFunc("POST /v1/tenants", s.create)
	mux.HandleFunc("GET /v1/tenants/{name}", s.get)
	mux.HandleFunc("PUT /v1/tenants/{name}", s.replace)
	mux.HandleFunc("POST /v1/tenants/{name}/suspend", s.suspend(true))
	mux.HandleFunc("POST /v1/tenants/{name}/resume", s.suspend(false))
	mux.HandleFunc("POST /v1/tenants/{name}/archive", s.archive)
	mux.HandleFunc("GET /v1/tenants/{name}/secrets/{key}", s.secret)
	mux.HandleFunc("GET /v1/tenants/{name}/logs", s.logs)
	mux.HandleFunc("GET /v1/archive", s.listArchived)
	mux.HandleFunc("POST /v1/archive/{name}/restore", s.restore)
	mux.HandleFunc("DELETE /v1/archive/{name}", s.purge)
	mux.HandleFunc("POST /v1/images/resolve", s.resolveImage)
	mux.HandleFunc("POST /v1/reconcile", s.reconcileNow)
	return mux
}

func (s *Server) view(record store.Record) Tenant {
	view := Tenant{
		Spec: record.Spec, Host: s.reconciler.Host(record.Spec.Name), URL: s.reconciler.URL(record.Spec.Name),
		HostPort: record.HostPort, Generation: record.Generation, CreatedAt: record.CreatedAt, UpdatedAt: record.UpdatedAt,
	}
	if status, ok := s.reconciler.Status(record.Spec.Name); ok {
		view.Status = &status
	}
	return view
}

func (s *Server) list(w http.ResponseWriter, _ *http.Request) {
	records := s.store.List()
	views := make([]Tenant, 0, len(records))
	for _, record := range records {
		views = append(views, s.view(record))
	}
	writeJSON(w, http.StatusOK, views)
}

func (s *Server) get(w http.ResponseWriter, r *http.Request) {
	record, err := s.store.Get(r.PathValue("name"))
	if err != nil {
		writeError(w, err)
		return
	}
	writeJSON(w, http.StatusOK, s.view(record))
}

// prepare fills in defaults, validates and pins the image: what every spec
// goes through before it is stored. Everything but the pin is checked first,
// so a broken spec never waits for a pull. ctx is the request's own: a
// client that gives up cancels the pull.
func (s *Server) prepare(ctx context.Context, tenant spec.Tenant) (spec.Tenant, string, error) {
	tenant = tenant.WithDefaults()
	tenant.Suspended = false
	if err := spec.ValidReference(tenant.Image); err != nil {
		return spec.Tenant{}, "", badRequest(err)
	}
	if err := tenant.ValidateUnpinned(); err != nil {
		return spec.Tenant{}, "", badRequest(err)
	}
	pinned, note, err := s.resolve(ctx, tenant.Image)
	if err != nil {
		return spec.Tenant{}, "", err
	}
	tenant.Image = pinned
	if err := tenant.Validate(); err != nil {
		return spec.Tenant{}, "", badRequest(err)
	}
	return tenant, note, nil
}

func (s *Server) resolve(ctx context.Context, image string) (string, string, error) {
	pinned, note, err := s.runtime.Resolve(ctx, image)
	if err != nil {
		return "", "", statusError{http.StatusUnprocessableEntity, fmt.Errorf("pin image %s: %w", image, err)}
	}
	return pinned, note, nil
}

// Resolved is an image reference pinned to a digest.
type Resolved struct {
	Image string `json:"image"`
	// Note explains a fallback to a local image.
	Note string `json:"note,omitempty"`
}

// resolveImage pins one reference, so a client applying many tenants of one
// app pulls once and pins them all to the same digest.
func (s *Server) resolveImage(w http.ResponseWriter, r *http.Request) {
	var request Resolved
	if !readJSON(w, r, &request) {
		return
	}
	if err := spec.ValidReference(request.Image); err != nil {
		writeError(w, badRequest(err))
		return
	}
	pinned, note, err := s.resolve(r.Context(), request.Image)
	if err != nil {
		writeError(w, err)
		return
	}
	writeJSON(w, http.StatusOK, Resolved{Image: pinned, Note: note})
}

func (s *Server) create(w http.ResponseWriter, r *http.Request) {
	var tenant spec.Tenant
	if !readJSON(w, r, &tenant) {
		return
	}
	if err := spec.ValidName(tenant.Name); err != nil {
		writeError(w, badRequest(err))
		return
	}
	tenant, note, err := s.prepare(r.Context(), tenant)
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
			return fmt.Errorf("the client gave up while waiting; nothing was created: %w", err)
		}
		if _, err := s.store.Get(tenant.Name); err == nil {
			return store.ErrExists
		}
		if _, archived := s.store.ArchivedTenant(tenant.Name); archived {
			return store.ErrArchived
		}
		exists, err := s.reconciler.DataExists(ctx, tenant.Name)
		if err != nil {
			return err
		}
		if exists {
			return statusError{http.StatusConflict, fmt.Errorf("data of an earlier tenant %s is on this node but not in the archive; purge it first", tenant.Name)}
		}
		sealed, err := s.sealMissing(tenant, nil)
		if err != nil {
			return err
		}
		record, err = s.store.Create(store.Record{Spec: tenant, Sealed: sealed})
		return err
	})
	if err != nil {
		writeError(w, err)
		return
	}
	s.log.Info("tenant created", "tenant", tenant.Name, "image", tenant.Image)
	writeJSON(w, http.StatusCreated, Applied{Tenant: s.view(record), Changed: true, Note: note})
}

// replace swaps the spec. Secrets keep their values; new secret names get
// generated values, and the values of removed names are kept too, so a
// mistaken edit to an app spec cannot destroy them (only a purge does).
// Suspension is operational
// state, not part of an app spec, so it is kept as it was. A spec equal to
// the stored one changes nothing, not even the generation.
func (s *Server) replace(w http.ResponseWriter, r *http.Request) {
	name := r.PathValue("name")
	var tenant spec.Tenant
	if !readJSON(w, r, &tenant) {
		return
	}
	if tenant.Name != name {
		writeError(w, badRequest(fmt.Errorf("spec name %q does not match tenant %q", tenant.Name, name)))
		return
	}
	tenant, note, err := s.prepare(r.Context(), tenant)
	if err != nil {
		writeError(w, err)
		return
	}
	current, err := s.store.Get(name)
	if err != nil {
		writeError(w, err)
		return
	}
	tenant.Suspended = current.Spec.Suspended
	if tenant.Equal(current.Spec) {
		writeJSON(w, http.StatusOK, Applied{Tenant: s.view(current), Changed: false, Note: note})
		return
	}
	record, err := s.store.Update(name, func(record *store.Record) error {
		if err := r.Context().Err(); err != nil {
			return fmt.Errorf("the client gave up while waiting; nothing was changed: %w", err)
		}
		sealed, err := s.sealMissing(tenant, record.Sealed)
		if err != nil {
			return err
		}
		tenant.Suspended = record.Spec.Suspended
		record.Spec, record.Sealed = tenant, sealed
		return nil
	})
	if err != nil {
		writeError(w, err)
		return
	}
	s.log.Info("tenant updated", "tenant", name, "generation", record.Generation, "image", tenant.Image)
	writeJSON(w, http.StatusOK, Applied{Tenant: s.view(record), Changed: true, Note: note})
}

func (s *Server) sealMissing(tenant spec.Tenant, existing map[string]string) (map[string]string, error) {
	sealed := maps.Clone(existing)
	if sealed == nil {
		sealed = map[string]string{}
	}
	for name, secret := range tenant.Secrets {
		if _, ok := sealed[name]; ok {
			continue
		}
		plain, err := secrets.Generate(secret)
		if err != nil {
			return nil, err
		}
		if sealed[name], err = s.key.Seal(secrets.Context(tenant.Name, name), plain); err != nil {
			return nil, err
		}
	}
	return sealed, nil
}

func (s *Server) suspend(suspended bool) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		record, err := s.store.SetSuspended(r.PathValue("name"), suspended)
		if err != nil {
			writeError(w, err)
			return
		}
		s.log.Info("tenant suspension changed", "tenant", record.Spec.Name, "suspended", suspended)
		writeJSON(w, http.StatusOK, s.view(record))
	}
}

// archive takes a tenant out of service and keeps everything needed to
// restore it. A failed archive leaves the tenant suspended; repeat it.
func (s *Server) archive(w http.ResponseWriter, r *http.Request) {
	name := r.PathValue("name")
	ctx, cancel := detached(r)
	defer cancel()
	if err := s.reconciler.Archive(ctx, name); err != nil {
		writeError(w, err)
		return
	}
	s.log.Info("tenant archived", "tenant", name)
	w.WriteHeader(http.StatusNoContent)
}

func (s *Server) listArchived(w http.ResponseWriter, _ *http.Request) {
	archived := s.store.ListArchived()
	views := make([]ArchivedTenant, 0, len(archived))
	for _, entry := range archived {
		views = append(views, ArchivedTenant{Name: entry.Record.Spec.Name, Image: entry.Record.Spec.Image, Suspended: entry.Record.Spec.Suspended, ArchivedAt: entry.ArchivedAt})
	}
	writeJSON(w, http.StatusOK, views)
}

func (s *Server) restore(w http.ResponseWriter, r *http.Request) {
	name := r.PathValue("name")
	var record store.Record
	err := s.reconciler.Exclusive(func() error {
		var err error
		record, err = s.store.Restore(name)
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
	if err := s.reconciler.Purge(ctx, name); err != nil {
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
	record, err := s.store.Get(name)
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
	s.log.Info("secret read", "tenant", name, "secret", key)
	writeJSON(w, http.StatusOK, SecretValue{Value: value})
}

func (s *Server) logs(w http.ResponseWriter, r *http.Request) {
	name := r.PathValue("name")
	if _, err := s.store.Get(name); err != nil {
		writeError(w, err)
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

func (s *Server) reconcileNow(w http.ResponseWriter, r *http.Request) {
	ctx, cancel := detached(r)
	defer cancel()
	if err := s.reconciler.Reconcile(ctx); err != nil {
		writeError(w, err)
		return
	}
	w.WriteHeader(http.StatusNoContent)
}

// Routes adapts the store for the edge proxy.
type Routes struct {
	Store store.Store
}

func (r Routes) Route(tenant string) (edge.Route, bool) {
	record, err := r.Store.Get(tenant)
	if err != nil {
		return edge.Route{}, false
	}
	return edge.Route{Backend: "127.0.0.1:" + strconv.Itoa(record.HostPort), Suspended: record.Spec.Suspended}, true
}

type statusError struct {
	status int
	err    error
}

func (e statusError) Error() string { return e.err.Error() }
func (e statusError) Unwrap() error { return e.err }

func badRequest(err error) error { return statusError{http.StatusBadRequest, err} }
func notFound(err error) error   { return statusError{http.StatusNotFound, err} }

type errorBody struct {
	Error string `json:"error"`
}

func writeError(w http.ResponseWriter, err error) {
	status := http.StatusInternalServerError
	var typed statusError
	switch {
	case errors.As(err, &typed):
		status = typed.status
	case errors.Is(err, store.ErrNotFound):
		status = http.StatusNotFound
	case errors.Is(err, store.ErrExists), errors.Is(err, store.ErrArchived), errors.Is(err, store.ErrNotArchived):
		status = http.StatusConflict
	case errors.As(err, new(*spec.ValidationError)):
		status = http.StatusBadRequest
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
