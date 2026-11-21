package apiserver

import (
	"errors"
	"net/http"
	"time"

	"github.com/PowerPenguini/murder/internal/edge"
	"github.com/PowerPenguini/murder/internal/plugins"
	"github.com/PowerPenguini/murder/internal/runtime"
	"github.com/PowerPenguini/murder/internal/store"
)

// routesWait is how long a request for routes newer than the client's waits
// for a change before it answers with the same ones.
var routesWait = 25 * time.Second

// Table is every route this node serves, with its version: the tenants,
// their own domains and the ACME challenges the edges answer.
func (r Routes) Table() edge.Table {
	routes := map[string]edge.Route{}
	for _, record := range r.Store.List() {
		if route, ok := r.Route(record.Spec.Name); ok {
			routes[record.Spec.Name] = route
		}
	}
	domains := map[string]edge.DomainRoute{}
	for _, domain := range r.Store.Domains() {
		if _, routed := routes[domain.Tenant]; routed {
			if route, ok := domainRoute(domain, time.Now()); ok {
				domains[domain.Name] = route
			}
		}
	}
	challenges := map[string]edge.ChallengeRoute{}
	for _, challenge := range r.Store.Challenges() {
		if challenge.ExpiresAt.After(time.Now()) {
			challenges[challenge.Token] = edge.ChallengeRoute{Domain: challenge.Domain, KeyAuth: challenge.KeyAuth}
		}
	}
	return edge.NewTableWithPlugins(routes, domains, challenges, r.Plugins(routes))
}

// Plugins are what the edge serves of plugins: those installed, and the
// tenants of this node that may use them (see routes).
func (r Routes) Plugins(routes map[string]edge.Route) plugins.Config {
	installed := r.Store.Plugins()
	if len(installed) == 0 {
		return plugins.Config{}
	}
	config := plugins.Config{Plugins: map[string]plugins.Plugin{}, Tenants: map[string]plugins.Tenant{}}
	byName := map[string]store.Plugin{}
	for _, plugin := range installed {
		config.Plugins[plugin.Name] = plugins.Plugin{Spec: plugin.Spec, Key: plugin.Key, Manifest: plugin.Manifest}
		byName[plugin.Name] = plugin
	}
	settings := map[[2]string]store.PluginTenant{}
	for _, s := range r.Store.PluginTenants("", "") {
		settings[[2]string{s.Tenant, s.Plugin}] = s
	}
	for _, record := range r.Store.List() {
		route, ok := routes[record.Spec.Name]
		if !ok || route.Backend == "" || len(record.Spec.Plugins) == 0 {
			continue
		}
		_, address, err := runtime.Addresses(record.Subnet)
		if err != nil {
			continue
		}
		tenant := plugins.Tenant{Address: address.String(), Grants: map[string]plugins.Grant{}}
		for _, name := range record.Spec.Plugins {
			plugin, ok := byName[name]
			if !ok {
				continue
			}
			own := settings[[2]string{record.Spec.Name, name}]
			tenant.Grants[name] = plugins.Grant{Key: own.Key, Limits: store.PluginLimits(plugin, own)}
		}
		config.Tenants[record.Spec.Name] = tenant
	}
	return config
}

// domainRoute is how the edges route a tenant's domain: once it was seen
// to point at the cluster, with its certificate until that expires.
func domainRoute(domain store.Domain, now time.Time) (edge.DomainRoute, bool) {
	if !domain.Verified() {
		return edge.DomainRoute{}, false
	}
	route := edge.DomainRoute{Tenant: domain.Tenant}
	if domain.Certificate != "" && domain.Key != "" && now.Before(domain.NotAfter) {
		route.Certificate, route.Key = domain.Certificate, domain.Key
	}
	return route, true
}

// Domain routes a tenant's own domain, as the edge's table would.
func (r Routes) Domain(host string) (edge.DomainRoute, bool) {
	for _, domain := range r.Store.Domains() {
		if domain.Name != host {
			continue
		}
		if _, ok := r.Route(domain.Tenant); !ok {
			return edge.DomainRoute{}, false
		}
		return domainRoute(domain, time.Now())
	}
	return edge.DomainRoute{}, false
}

// Challenge finds an ACME challenge the edges answer.
func (r Routes) Challenge(token string) (edge.ChallengeRoute, bool) {
	for _, challenge := range r.Store.Challenges() {
		if challenge.Token == token && challenge.ExpiresAt.After(time.Now()) {
			return edge.ChallengeRoute{Domain: challenge.Domain, KeyAuth: challenge.KeyAuth}, true
		}
	}
	return edge.ChallengeRoute{}, false
}

// routes answers the edge: the route table, or, with after naming the
// version the edge has, the next one once the routes change, or the same
// one after routesWait.
func (s *Server) routes(w http.ResponseWriter, r *http.Request) {
	// A node set up for a cluster has the cluster's tenants only once it
	// is part of it.
	if s.cluster != nil && s.store.Cluster() == "" {
		writeError(w, statusError{http.StatusServiceUnavailable, errors.New("this node is not part of a cluster yet: it has no routes")})
		return
	}
	timeout := time.NewTimer(routesWait)
	defer timeout.Stop()
	// Nor does it name routes before it has caught up with the cluster
	// since it started: until then its state is what it had when it went
	// down, and a tenant may have failed over or moved since. An edge
	// started with it, as after a reboot, holds its requests for a while
	// meanwhile; an edge that already had routes from it keeps them.
	if s.cluster != nil {
		select {
		case <-s.caughtUp:
		case <-timeout.C:
			writeError(w, statusError{http.StatusServiceUnavailable, errors.New("this node has not caught up with its cluster since it started: its routes may be out of date")})
			return
		case <-r.Context().Done():
			return
		case <-s.stopping:
			writeError(w, statusError{http.StatusServiceUnavailable, errors.New("the agent is stopping")})
			return
		}
	}
	routes := Routes{Store: s.store, RelayPort: s.relayPort, Replacing: s.reconciler.Replacing}
	after := r.URL.Query().Get("after")
	for {
		// Watch before reading, so no change between the two is missed.
		changed, replacing := s.store.Watch(), s.reconciler.WatchReplacing()
		table := routes.Table()
		if table.Version != after {
			writeJSON(w, http.StatusOK, table)
			return
		}
		select {
		case <-changed:
		case <-replacing:
		case <-timeout.C:
			writeJSON(w, http.StatusOK, table)
			return
		case <-r.Context().Done():
			return
		case <-s.stopping:
			writeJSON(w, http.StatusOK, table)
			return
		}
	}
}
