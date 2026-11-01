package volume

import (
	"context"
	"fmt"
	"io"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
)

// Replicas keep a copy of each tenant's data on a second node, sent as ZFS
// snapshots: the node that runs a tenant snapshots its data and sends what
// changed since the copy's newest snapshot; the copy's node receives it
// into a dataset that is never mounted until the copy takes over.
type Replicas interface {
	// Snapshots lists the short names (after the "@") of the tenant's
	// snapshots taken for reason, oldest first; none if it has no data.
	Snapshots(ctx context.Context, tenant, reason string) ([]string, error)
	// Written is how many bytes the tenant's data took on since snapshot.
	Written(ctx context.Context, tenant, snapshot string) (int64, error)
	// SendSnapshot writes snapshot to as a stream, with the dataset's
	// properties: whole, or, with from set, what changed since from.
	SendSnapshot(ctx context.Context, tenant, from, to string, w io.Writer) error
	// ReceiveReplica takes such a stream into the tenant's copy on this
	// node: a whole one replaces any copy there, an incremental one goes on
	// top of it. The copy is never mounted; only the newest keep of its
	// snapshots stay.
	ReceiveReplica(ctx context.Context, tenant string, whole bool, r io.Reader, keep int) error
	// DestroySnapshot destroys one of the snapshots Snapshots lists.
	DestroySnapshot(ctx context.Context, tenant, snapshot string) error
	// Datasets lists the tenants that have data on this node.
	Datasets(ctx context.Context) ([]Dataset, error)
	// DropReplica destroys the tenant's copy on this node; live data, one
	// not marked as a copy, is refused.
	DropReplica(ctx context.Context, tenant string) error
	// PutAside renames the data of a tenant this node no longer runs out of
	// the way, unmounted and kept, and returns its new name.
	PutAside(ctx context.Context, tenant, why string) (string, error)
}

// Dataset is a tenant's data on this node.
type Dataset struct {
	Tenant string
	// Replica says it is a copy of a tenant another node runs.
	Replica bool
	Mounted bool
}

// replicaReason is the reason of the snapshots replication takes; the copy
// keeps them under the same names.
const replicaReason = "replica"

// ReplicaReason names the snapshots replication sends.
const ReplicaReason = replicaReason

// asidePrefix begins the names of datasets put aside: kept, never mounted,
// never a tenant's (tenant names cannot begin with "murder-").
const asidePrefix = "murder-failed-"

func (z ZFS) Snapshots(ctx context.Context, tenant, reason string) ([]string, error) {
	if err := checkTenant(tenant); err != nil {
		return nil, err
	}
	if !snapshotReason(reason) {
		return nil, fmt.Errorf("snapshot reason %q", reason)
	}
	dataset := z.dataset(tenant)
	output, err := z.Run(ctx, "zfs", "list", "-H", "-o", "name", "-t", "snapshot", "-d", "1", dataset)
	if err != nil {
		if strings.Contains(string(output), "does not exist") {
			return nil, nil
		}
		return nil, err
	}
	prefix := dataset + "@murder-" + reason + "-"
	var names []string
	for _, name := range strings.Split(string(output), "\n") {
		if strings.HasPrefix(name, prefix) {
			names = append(names, strings.TrimPrefix(name, dataset+"@"))
		}
	}
	sort.Strings(names)
	return names, nil
}

// checkSnapshot refuses a short snapshot name that could name anything but
// one of the tenant's own snapshots.
func checkSnapshot(name string) error {
	if !strings.HasPrefix(name, "murder-") || strings.ContainsAny(name, "/@ \t\n") {
		return fmt.Errorf("%q is not the name of a snapshot Murder took", name)
	}
	return nil
}

func (z ZFS) Written(ctx context.Context, tenant, snapshot string) (int64, error) {
	if err := checkTenant(tenant); err != nil {
		return 0, err
	}
	if err := checkSnapshot(snapshot); err != nil {
		return 0, err
	}
	output, err := z.Run(ctx, "zfs", "get", "-H", "-p", "-o", "value", "written@"+snapshot, z.dataset(tenant))
	if err != nil {
		return 0, err
	}
	written, err := strconv.ParseInt(strings.TrimSpace(string(output)), 10, 64)
	if err != nil {
		return 0, fmt.Errorf("zfs get written@%s: %q", snapshot, output)
	}
	return written, nil
}

func (z ZFS) DestroySnapshot(ctx context.Context, tenant, snapshot string) error {
	if err := checkTenant(tenant); err != nil {
		return err
	}
	if err := checkSnapshot(snapshot); err != nil {
		return err
	}
	_, err := z.Run(ctx, "zfs", "destroy", z.dataset(tenant)+"@"+snapshot)
	return err
}

func (z ZFS) SendSnapshot(ctx context.Context, tenant, from, to string, w io.Writer) error {
	if err := checkTenant(tenant); err != nil {
		return err
	}
	if err := checkSnapshot(to); err != nil {
		return err
	}
	args := []string{"send", "-p"}
	if from != "" {
		if err := checkSnapshot(from); err != nil {
			return err
		}
		args = append(args, "-i", "@"+from)
	}
	return z.pipe()(ctx, nil, w, "zfs", append(args, z.dataset(tenant)+"@"+to)...)
}

// ReceiveReplica receives unmounted, marked as a copy and never mounted at
// boot, where the data would be mounted if the copy took over. An
// incremental stream rolls back anything written to the copy since its
// newest snapshot (-F): nothing should be.
func (z ZFS) ReceiveReplica(ctx context.Context, tenant string, whole bool, r io.Reader, keep int) error {
	if err := checkTenant(tenant); err != nil {
		return err
	}
	dataset := z.dataset(tenant)
	current, exists, err := z.properties(ctx, dataset)
	if err != nil {
		return err
	}
	if exists && (current.owner != tenant || current.role != replicaRole) {
		return fmt.Errorf("dataset %s is not a copy of tenant %s: it is not replaced", dataset, tenant)
	}
	switch {
	case whole && exists:
		if _, err := z.Run(ctx, "zfs", "destroy", "-r", dataset); err != nil {
			return err
		}
	case !whole && !exists:
		return fmt.Errorf("tenant %s has no copy here to add changes to", tenant)
	}
	args := []string{"receive", "-u", "-o", "canmount=noauto", "-o", roleProperty + "=" + replicaRole,
		"-o", "mountpoint=" + filepath.Join(z.MountRoot, tenant)}
	if !whole {
		args = append(args, "-F")
	}
	if err := z.pipe()(ctx, r, io.Discard, "zfs", append(args, dataset)...); err != nil {
		return err
	}
	return z.Prune(ctx, tenant, replicaReason, keep)
}

func (z ZFS) Datasets(ctx context.Context) ([]Dataset, error) {
	output, err := z.Run(ctx, "zfs", "list", "-H", "-p", "-o", "name,"+ownerProperty+","+roleProperty+",mounted", "-d", "1", z.Parent)
	if err != nil {
		if strings.Contains(string(output), "does not exist") {
			return nil, nil
		}
		return nil, err
	}
	var datasets []Dataset
	for _, line := range strings.Split(strings.TrimRight(string(output), "\n"), "\n") {
		fields := strings.Split(line, "\t")
		if len(fields) != 4 {
			continue
		}
		tenant, ok := strings.CutPrefix(fields[0], z.Parent+"/")
		// Only datasets Murder made for the tenant they are named after;
		// copies, data put aside and anything else are not tenants'.
		if !ok || fields[1] != tenant || checkTenant(tenant) != nil || strings.HasPrefix(tenant, "murder-") {
			continue
		}
		datasets = append(datasets, Dataset{Tenant: tenant, Replica: fields[2] == replicaRole, Mounted: fields[3] == "yes"})
	}
	return datasets, nil
}

func (z ZFS) DropReplica(ctx context.Context, tenant string) error {
	if err := checkTenant(tenant); err != nil {
		return err
	}
	dataset := z.dataset(tenant)
	current, exists, err := z.properties(ctx, dataset)
	if err != nil || !exists {
		return err
	}
	if current.owner != tenant || current.role != replicaRole || current.mounted == "yes" {
		return fmt.Errorf("dataset %s is not a copy of tenant %s: it is not destroyed", dataset, tenant)
	}
	_, err = z.Run(ctx, "zfs", "destroy", "-r", dataset)
	return err
}

// PutAside unmounts the dataset and renames it murder-failed-<tenant>-<why>-
// <time>, with canmount off: nothing mounts or uses it, and Murder never
// destroys it; the operator looks at it, then destroys it.
func (z ZFS) PutAside(ctx context.Context, tenant, why string) (string, error) {
	if err := checkTenant(tenant); err != nil {
		return "", err
	}
	if !snapshotReason(why) {
		return "", fmt.Errorf("reason %q", why)
	}
	dataset := z.dataset(tenant)
	current, exists, err := z.properties(ctx, dataset)
	if err != nil {
		return "", err
	}
	if !exists {
		return "", fmt.Errorf("tenant %s has no data here", tenant)
	}
	if current.owner != tenant {
		return "", fmt.Errorf("dataset %s was not made by Murder for tenant %s", dataset, tenant)
	}
	if current.mounted == "yes" {
		if _, err := z.Run(ctx, "zfs", "unmount", dataset); err != nil {
			return "", err
		}
	}
	short := asidePrefix + tenant + "-" + why + "-" + strings.ToLower(z.now().UTC().Format("20060102T150405Z"))
	aside := z.Parent + "/" + short
	if _, err := z.Run(ctx, "zfs", "rename", dataset, aside); err != nil {
		return "", err
	}
	if _, err := z.Run(ctx, "zfs", "set", "canmount=off", aside); err != nil {
		return aside, err
	}
	_, err = z.Run(ctx, "zfs", "set", "mountpoint="+filepath.Join(z.MountRoot, ".failed", short), aside)
	return aside, err
}
