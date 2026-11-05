package apiserver

import (
	"net/http"
	"strings"

	"github.com/PowerPenguini/murder/internal/api"
	"github.com/PowerPenguini/murder/internal/domains"
	"github.com/PowerPenguini/murder/internal/store"
)

// domainRoutes serve tenants' own domains.
func (s *Server) domainRoutes(mux *http.ServeMux) {
	mux.HandleFunc("GET /v1/tenants/{name}/domains", s.listDomains)
	mux.HandleFunc("POST /v1/tenants/{name}/domains", s.addDomain)
	mux.HandleFunc("DELETE /v1/tenants/{name}/domains/{domain}", s.removeDomain)
	mux.HandleFunc("POST /v1/tenants/{name}/domains/{domain}/check", s.checkDomain)
}

// domainView is what the API shows of a domain: its state, and the DNS
// records that point it at the cluster.
func (s *Server) domainView(domain store.Domain) api.Domain {
	view := api.Domain{
		Name: domain.Name, Tenant: domain.Tenant, State: domain.State, Detail: domain.Detail, Attempts: domain.Attempts, NextAt: domain.NextAt,
		CheckedAt: domain.CheckedAt, VerifiedAt: domain.VerifiedAt, IssuedAt: domain.IssuedAt, NotAfter: domain.NotAfter,
		Records:   domains.Records(domain.Name, s.reconciler.Host(domain.Tenant), domains.PublicAddresses(s.store.Nodes())),
		CreatedAt: domain.CreatedAt, CreatedBy: domain.CreatedBy,
	}
	if display := domains.Display(domain.Name); display != domain.Name {
		view.Display = display
	}
	return view
}

func (s *Server) domainViews(tenant string) []api.Domain {
	views := []api.Domain{}
	for _, domain := range s.store.TenantDomains(tenant) {
		views = append(views, s.domainView(domain))
	}
	return views
}

func (s *Server) listDomains(w http.ResponseWriter, r *http.Request) {
	name := r.PathValue("name")
	if _, err := s.store.Get(name); err != nil {
		writeError(w, err)
		return
	}
	writeJSON(w, http.StatusOK, s.domainViews(name))
}

func (s *Server) addDomain(w http.ResponseWriter, r *http.Request) {
	var request api.DomainRequest
	if !readJSON(w, r, &request) {
		return
	}
	name, err := domains.Normalize(request.Name, s.reconciler.BaseDomain())
	if err != nil {
		writeError(w, badRequest(err))
		return
	}
	domain, added, err := s.store.AddDomain(r.PathValue("name"), name, actor(r))
	if err != nil {
		writeError(w, err)
		return
	}
	status := http.StatusOK
	if added {
		status = http.StatusCreated
	}
	writeJSON(w, status, api.DomainAdded{Domain: s.domainView(domain), Added: added})
}

// domainNamed is the stored name of a domain a request names, as it was
// given or normalised.
func domainNamed(given string) string {
	if name, err := domains.Normalize(given, ""); err == nil {
		return name
	}
	return strings.TrimSuffix(strings.ToLower(strings.TrimSpace(given)), ".")
}

func (s *Server) removeDomain(w http.ResponseWriter, r *http.Request) {
	if err := s.store.RemoveDomain(r.PathValue("name"), domainNamed(r.PathValue("domain")), actor(r)); err != nil {
		writeError(w, err)
		return
	}
	w.WriteHeader(http.StatusNoContent)
}

func (s *Server) checkDomain(w http.ResponseWriter, r *http.Request) {
	if err := s.store.CheckDomainNow(r.PathValue("name"), domainNamed(r.PathValue("domain")), actor(r)); err != nil {
		writeError(w, err)
		return
	}
	w.WriteHeader(http.StatusNoContent)
}
