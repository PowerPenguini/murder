package main

import (
	"net/http"
	"sync/atomic"
	"time"

	"github.com/PowerPenguini/murder/internal/api"
	"github.com/PowerPenguini/murder/internal/edge"
	"github.com/PowerPenguini/murder/internal/metrics"
	"github.com/PowerPenguini/murder/internal/reconcile"
	"github.com/PowerPenguini/murder/internal/store"
)

// phases are the values murder_tenant_phase takes; "pending" is a tenant
// no pass has seen yet.
var phases = []string{"pending", reconcile.PhaseRunning, reconcile.PhasePulling, reconcile.PhaseRestarting, reconcile.PhaseFailed, reconcile.PhaseSuspended}

// agentMetrics serves /metrics: the edge's requests, each live tenant's
// state as of the last pass, and when the agent last reconciled, loaded
// the firewall and sent a backup, and when its certificate expires.
type agentMetrics struct {
	store      store.Store
	reconciler *reconcile.Reconciler
	api        *api.Server
	edge       *metrics.Edge
	// firewall is when the firewall was last loaded, in Unix nanoseconds;
	// nil when it is off.
	firewall     *atomic.Int64
	certificates atomic.Pointer[edge.Certificates]
}

func (m *agentMetrics) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	if r.URL.Path != "/metrics" {
		http.NotFound(w, r)
		return
	}
	w.Header().Set("Content-Type", "text/plain; version=0.0.4; charset=utf-8")
	out := metrics.NewWriter(w)
	m.edge.Write(out)

	type tenant struct {
		name   string
		status reconcile.Status
		seen   bool
	}
	var tenants []tenant
	for _, record := range m.store.List() {
		if !record.Archived() {
			status, seen := m.reconciler.Status(record.Spec.Name)
			tenants = append(tenants, tenant{record.Spec.Name, status, seen})
		}
	}
	out.Family("murder_tenant_phase", "gauge", "1 for the phase each live tenant is in, as of the last pass.")
	for _, t := range tenants {
		phase := t.status.Phase
		if !t.seen {
			phase = "pending"
		}
		for _, candidate := range phases {
			out.Sample("murder_tenant_phase", metrics.Bool(candidate == phase), "tenant", t.name, "phase", candidate)
		}
	}
	out.Family("murder_tenant_healthy", "gauge", "1 if the tenant answered its last health check.")
	for _, t := range tenants {
		out.Sample("murder_tenant_healthy", metrics.Bool(t.status.Healthy), "tenant", t.name)
	}
	out.Family("murder_tenant_restarts", "gauge", "Restarts of the tenant's container after its app exited.")
	for _, t := range tenants {
		out.Sample("murder_tenant_restarts", float64(t.status.Restarts), "tenant", t.name)
	}
	out.Family("murder_tenant_warning", "gauge", "1 if the tenant has a warning, such as a disk limit not applied.")
	for _, t := range tenants {
		out.Sample("murder_tenant_warning", metrics.Bool(t.status.Warning != ""), "tenant", t.name)
	}

	if at, errors := m.reconciler.LastPass(); !at.IsZero() {
		out.Family("murder_reconcile_last_pass_timestamp_seconds", "gauge", "When the last whole reconcile pass ended.")
		out.Sample("murder_reconcile_last_pass_timestamp_seconds", metrics.Timestamp(at))
		out.Family("murder_reconcile_errors", "gauge", "Errors in the last whole reconcile pass.")
		out.Sample("murder_reconcile_errors", float64(errors))
	}
	if m.firewall != nil {
		if at := m.firewall.Load(); at != 0 {
			out.Family("murder_firewall_loaded_timestamp_seconds", "gauge", "When the tenant firewall was last loaded.")
			out.Sample("murder_firewall_loaded_timestamp_seconds", metrics.Timestamp(time.Unix(0, at)))
		}
	}
	if at := m.api.LastBackup(); !at.IsZero() {
		out.Family("murder_backup_last_timestamp_seconds", "gauge", "When the agent last sent a whole backup of its database.")
		out.Sample("murder_backup_last_timestamp_seconds", metrics.Timestamp(at))
	}
	if certificates := m.certificates.Load(); certificates != nil {
		if at := certificates.NotAfter(); !at.IsZero() {
			out.Family("murder_tls_certificate_expiry_timestamp_seconds", "gauge", "When the first of the edge's certificates expires.")
			out.Sample("murder_tls_certificate_expiry_timestamp_seconds", metrics.Timestamp(at))
		}
	}
	_ = out.Err()
}
