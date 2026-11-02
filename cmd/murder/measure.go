package main

import (
	"context"
	"log/slog"
	"time"

	"github.com/PowerPenguini/murder/internal/reconcile"
	"github.com/PowerPenguini/murder/internal/runtime"
	"github.com/PowerPenguini/murder/internal/spec"
	"github.com/PowerPenguini/murder/internal/store"
	measure "github.com/PowerPenguini/murder/internal/usage"
	"github.com/PowerPenguini/murder/internal/volume"
)

// newCollector measures the node's tenants: those the store places here
// and not archived, with their running container as the reconciler last
// saw it.
func newCollector(st store.Store, reconciler *reconcile.Reconciler, rt runtime.Runtime, volumes volume.Volumes, recorder *measure.Recorder, node string, defaults spec.Limits, log *slog.Logger) *measure.Collector {
	collector := &measure.Collector{
		Recorder: recorder,
		Log:      log.With("component", "usage"),
		Tenants: func() []measure.Tenant {
			var tenants []measure.Tenant
			for _, record := range st.List() {
				if record.Node != node || record.Archived() {
					continue
				}
				limits := record.Spec.Limits.Or(defaults)
				t := measure.Tenant{Name: record.Spec.Name, CPULimit: limits.CPUs, DiskLimit: int64(limits.DiskGB) << 30}
				if status, ok := reconciler.Status(record.Spec.Name); ok && status.Phase == reconcile.PhaseRunning {
					t.Container = status.ContainerID
				}
				tenants = append(tenants, t)
			}
			return tenants
		},
		Pid: func(ctx context.Context, container string) (int, error) {
			state, err := rt.Inspect(ctx, container)
			return state.Pid, err
		},
	}
	// The zfs driver tells a tenant's data's size; plain directories would
	// have to be walked.
	if sized, ok := volumes.(interface {
		Used(ctx context.Context, tenant string) (int64, error)
	}); ok {
		collector.Disk = func(ctx context.Context, tenant string) (int64, bool, error) {
			used, err := sized.Used(ctx, tenant)
			return used, err == nil, err
		}
	}
	return collector
}

// peerUsage serves the recorder to other nodes.
type peerUsage struct{ recorder *measure.Recorder }

func (u peerUsage) Report() any { return u.recorder.Report(time.Now()) }

func (u peerUsage) Series(tenant string, step time.Duration) (any, bool) {
	return u.recorder.Series(tenant, step, time.Now())
}
