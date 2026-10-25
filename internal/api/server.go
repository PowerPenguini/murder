// Package api is the agent's local HTTP API on a unix socket, used by the CLI.
package api

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
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

// ImageChange asks for a new image.
type ImageChange struct {
	Image string `json:"image"`
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
	mux.HandleFunc("POST /v1/tenants/{name}/image", s.setImage)
	mux.HandleFunc("POST /v1/tenants/{name}/suspend", s.suspend(true))
	mux.HandleFunc("POST /v1/tenants/{name}/resume", s.suspend(false))
	mux.HandleFunc("DELETE /v1/tenants/{name}", s.delete)
	mux.HandleFunc("GET /v1/tenants/{name}/secrets/{key}", s.secret)
	mux.HandleFunc("GET /v1/tenants/{name}/logs", s.logs)
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

func (s *Server) create(w http.ResponseWriter, r *http.Request) {
	var tenant spec.Tenant
	if !readJSON(w, r, &tenant) {
		return
	}
	if err := spec.ValidName(tenant.Name); err != nil {
		writeError(w, badRequest(err))
		return
	}
	if err := tenant.Validate(); err != nil {
		writeError(w, badRequest(err))
		return
	}
	sealed, err := s.sealMissing(tenant, nil)
	if err != nil {
		writeError(w, err)
		return
	}
	record, err := s.store.Create(store.Record{Spec: tenant, Sealed: sealed})
	if err != nil {
		writeError(w, err)
		return
	}
	s.log.Info("tenant created", "tenant", tenant.Name, "image", tenant.Image)
	writeJSON(w, http.StatusCreated, s.view(record))
}

// replace swaps the whole spec. Secrets keep their values; new secret names
// get generated values and removed ones are forgotten.
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
	record, err := s.store.Update(name, func(record *store.Record) error {
		sealed, err := s.sealMissing(tenant, record.Sealed)
		if err != nil {
			return err
		}
		record.Spec, record.Sealed = tenant, sealed
		return nil
	})
	if err != nil {
		writeError(w, err)
		return
	}
	s.log.Info("tenant updated", "tenant", name, "generation", record.Generation)
	writeJSON(w, http.StatusOK, s.view(record))
}

func (s *Server) sealMissing(tenant spec.Tenant, existing map[string]string) (map[string]string, error) {
	sealed := map[string]string{}
	for name, secret := range tenant.Secrets {
		if value, ok := existing[name]; ok {
			sealed[name] = value
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

func (s *Server) setImage(w http.ResponseWriter, r *http.Request) {
	var change ImageChange
	if !readJSON(w, r, &change) {
		return
	}
	if err := spec.ValidImage(change.Image); err != nil {
		writeError(w, badRequest(err))
		return
	}
	record, err := s.store.Update(r.PathValue("name"), func(record *store.Record) error {
		record.Spec.Image = change.Image
		return nil
	})
	if err != nil {
		writeError(w, err)
		return
	}
	s.log.Info("tenant image changed", "tenant", record.Spec.Name, "image", change.Image)
	writeJSON(w, http.StatusOK, s.view(record))
}

func (s *Server) suspend(suspended bool) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		record, err := s.store.Update(r.PathValue("name"), func(record *store.Record) error {
			record.Spec.Suspended = suspended
			return nil
		})
		if err != nil {
			writeError(w, err)
			return
		}
		s.log.Info("tenant suspension changed", "tenant", record.Spec.Name, "suspended", suspended)
		writeJSON(w, http.StatusOK, s.view(record))
	}
}

// delete removes the tenant and its containers. Its data stays unless
// purge=true, which deletes the data volume too.
func (s *Server) delete(w http.ResponseWriter, r *http.Request) {
	name := r.PathValue("name")
	if err := s.store.Delete(name); err != nil {
		writeError(w, err)
		return
	}
	s.log.Info("tenant deleted", "tenant", name)
	if r.URL.Query().Get("purge") == "true" {
		if err := s.reconciler.Purge(r.Context(), name); err != nil {
			writeError(w, fmt.Errorf("tenant deleted but its data was not purged: %w", err))
			return
		}
		s.log.Info("tenant data purged", "tenant", name)
	}
	w.WriteHeader(http.StatusNoContent)
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
	if err := s.reconciler.Reconcile(r.Context()); err != nil {
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
	case errors.Is(err, store.ErrExists):
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
