package main

import (
	"maps"
	"net/http"
	"slices"
	"sync/atomic"
	"time"

	"github.com/PowerPenguini/murder/internal/apiserver"
	"github.com/PowerPenguini/murder/internal/cluster"
	"github.com/PowerPenguini/murder/internal/failover"
	"github.com/PowerPenguini/murder/internal/metrics"
	"github.com/PowerPenguini/murder/internal/reconcile"
	"github.com/PowerPenguini/murder/internal/replicate"
	"github.com/PowerPenguini/murder/internal/store"
	measure "github.com/PowerPenguini/murder/internal/usage"
)

// phases are the values murder_tenant_phase takes; "pending" is a tenant
// no pass has seen yet.
var phases = []string{"pending", reconcile.PhaseRunning, reconcile.PhasePulling, reconcile.PhaseRestarting, reconcile.PhaseFailed, reconcile.PhaseSuspended, reconcile.PhaseMoving}

// agentMetrics serves /metrics: each live tenant's state as of the last
// pass, and when the agent last reconciled, loaded the firewall and sent a
// backup. The edge serves its own.
type agentMetrics struct {
	store      store.Store
	reconciler *reconcile.Reconciler
	api        *apiserver.Server
	// firewall is when the firewall was last loaded, in Unix nanoseconds;
	// nil when it is off.
	firewall *atomic.Int64
	// backups is nil without backups set up.
	backups *agentBackups
	// replicator is nil outside a cluster of ZFS nodes.
	replicator *replicate.Replicator
	// monitor is nil outside a cluster.
	monitor *failover.Monitor
	// digests is nil outside a cluster.
	digests *cluster.Digests
	// usage is what the node's tenants use; nil in tests that measure none.
	usage *measure.Recorder
}

func (m *agentMetrics) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	if r.URL.Path != "/metrics" {
		http.NotFound(w, r)
		return
	}
	w.Header().Set("Content-Type", "text/plain; version=0.0.4; charset=utf-8")
	out := metrics.NewWriter(w)
	records := m.store.List()

	type tenant struct {
		name   string
		status reconcile.Status
		seen   bool
	}
	var tenants []tenant
	for _, record := range records {
		// Another node reports the tenants it runs.
		if !record.Archived() && record.Node == m.store.Node() {
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
	if m.backups != nil {
		m.backups.WriteMetrics(out)
	}
	if m.replicator != nil {
		m.replicator.WriteMetrics(out)
	}
	if m.monitor != nil {
		m.monitor.WriteMetrics(out)
	}
	if m.digests != nil {
		diverged := m.digests.Diverged()
		out.Family("murder_state_diverged", "gauge", "On the leader, 1 for each node whose replicated state differs from the leader's at the same version; nodes not compared are left out.")
		for _, node := range slices.Sorted(maps.Keys(diverged)) {
			out.Sample("murder_state_diverged", metrics.Bool(diverged[node]), "node", node)
		}
	}
	if m.usage != nil {
		writeUsage(out, m.usage.Report(time.Now()))
	}
	_ = out.Err()
}

// writeUsage writes what the node's tenants use: their containers'
// counters as they stand, and memory and data as last measured.
func writeUsage(out *metrics.Writer, report measure.Report) {
	names := slices.Sorted(maps.Keys(report.Tenants))
	families := []struct {
		name, kind, help string
		value            func(measure.Now) (float64, bool)
	}{
		{"murder_tenant_cpu_seconds_total", "counter", "Processor time the tenant's container used since it started.", func(n measure.Now) (float64, bool) { return float64(n.Counters.CPU) / 1e6, true }},
		{"murder_tenant_memory_bytes", "gauge", "Memory the tenant's container uses.", func(n measure.Now) (float64, bool) { return float64(n.Memory), true }},
		{"murder_tenant_memory_limit_bytes", "gauge", "The tenant's container's memory limit.", func(n measure.Now) (float64, bool) { return float64(n.MemoryLimit), n.MemoryLimit > 0 }},
		{"murder_tenant_network_receive_bytes_total", "counter", "Bytes the tenant's container received since it started.", func(n measure.Now) (float64, bool) { return float64(n.Counters.NetRx), true }},
		{"murder_tenant_network_transmit_bytes_total", "counter", "Bytes the tenant's container sent since it started.", func(n measure.Now) (float64, bool) { return float64(n.Counters.NetTx), true }},
		{"murder_tenant_disk_read_bytes_total", "counter", "Bytes the tenant's container read from disk since it started.", func(n measure.Now) (float64, bool) { return float64(n.Counters.DiskRead), true }},
		{"murder_tenant_disk_written_bytes_total", "counter", "Bytes the tenant's container wrote to disk since it started.", func(n measure.Now) (float64, bool) { return float64(n.Counters.DiskWrite), true }},
		{"murder_tenant_data_bytes", "gauge", "The size of the tenant's data, as its disk limit counts it.", func(n measure.Now) (float64, bool) { return float64(n.DiskUsed), n.DiskUsed >= 0 }},
	}
	for _, family := range families {
		out.Family(family.name, family.kind, family.help)
		for _, name := range names {
			if value, ok := family.value(report.Tenants[name]); ok {
				out.Sample(family.name, value, "tenant", name)
			}
		}
	}
}
