package volume

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"fmt"
	"io"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"sync"
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
	// IsReplica reports whether the tenant's data here is a copy, once a
	// whole copy's swap that stopped half way is finished.
	IsReplica(ctx context.Context, tenant string) (bool, error)
	// RollbackReplica takes the tenant's copy back to its newest replica
	// snapshot, dropping what a move added since; a copy without one goes.
	RollbackReplica(ctx context.Context, tenant string) error
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

// A whole copy is received as murder-incoming:<tenant>, and the copy it
// replaces waits as murder-previous:<tenant> until the new one is in its
// place. Tenant names cannot hold a colon, so no tenant's staging name is
// another's, nor a tenant's.
const (
	incomingPrefix = "murder-incoming:"
	previousPrefix = "murder-previous:"
)

// legacyStagingPrefix began both staging names before: murder-incoming-
// <tenant> for the copy received and murder-incoming-<tenant>-old for the
// one it replaced. The names of tenant "acme" and "acme-old" collide there,
// so such a dataset is only taken for a tenant's when it was made for it.
// Such datasets are destroyed with the tenant's other staging datasets,
// never put back: nothing in them says which swap, or which tenant of that
// name, they were part of.
const legacyStagingPrefix = "murder-incoming-"

// swapProperty ties together the two copies of one whole copy's swap: the
// copy received carries a token made for that receive, and the old copy is
// given the same token before it goes aside. Only an old copy whose token
// the copy received beside it carries is put back by restoreSwap: one left
// by a finished swap, or by an earlier tenant of the same name, never is.
const swapProperty = "io.murder:swap"

// swapToken makes the token of one whole copy's swap.
func swapToken() (string, error) {
	var b [12]byte
	if _, err := rand.Read(b[:]); err != nil {
		return "", err
	}
	return hex.EncodeToString(b[:]), nil
}

// datasetLocks serializes, per dataset, receiving a copy and making a copy
// the tenant's data: a copy never takes over half received, nor is a copy
// that took over written to.
var datasetLocks sync.Map

func lockDataset(dataset string) func() {
	lock, _ := datasetLocks.LoadOrStore(dataset, &sync.Mutex{})
	lock.(*sync.Mutex).Lock()
	return lock.(*sync.Mutex).Unlock
}

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
	defer lockDataset(dataset)()
	current, exists, err := z.properties(ctx, dataset)
	if err != nil {
		return err
	}
	if !exists {
		// A whole copy's swap that stopped half way: its old copy first
		// goes back where it belongs.
		if exists, err = z.restoreSwap(ctx, tenant); err != nil {
			return err
		} else if exists {
			if current, _, err = z.properties(ctx, dataset); err != nil {
				return err
			}
		}
	}
	if exists && (current.owner != tenant || current.role != replicaRole || current.mounted == "yes") {
		return fmt.Errorf("dataset %s is not a copy of tenant %s: it is not replaced", dataset, tenant)
	}
	if !whole && !exists {
		return fmt.Errorf("tenant %s has no copy here to add changes to", tenant)
	}
	args := []string{"receive", "-u", "-o", "canmount=noauto", "-o", roleProperty + "=" + replicaRole,
		"-o", "mountpoint=" + filepath.Join(z.MountRoot, tenant)}
	if !whole {
		if err := z.pipe()(ctx, r, io.Discard, "zfs", append(args, "-F", dataset)...); err != nil {
			return err
		}
		return z.Prune(ctx, tenant, replicaReason, keep)
	}
	// What an earlier whole receive left of the tenant's goes, never
	// another tenant's.
	for _, kind := range []stagingKind{incoming, previous} {
		if err := z.destroyStaged(ctx, tenant, kind); err != nil {
			return err
		}
	}
	// A whole copy goes in beside the old one, which it replaces once all
	// of it is there: the node keeps a copy all along.
	incomingName, previousName := z.Parent+"/"+incomingPrefix+tenant, z.Parent+"/"+previousPrefix+tenant
	// A dataset still there is not the tenant's copy (destroyStaged took
	// those): it is neither received over nor destroyed.
	if _, occupied, err := z.properties(ctx, incomingName); err != nil {
		return err
	} else if occupied {
		return fmt.Errorf("dataset %s, where tenant %s's whole copy is received, is not an unmounted copy of the tenant: nothing is received, and it is left alone", incomingName, tenant)
	}
	token, err := swapToken()
	if err != nil {
		return err
	}
	if err := z.pipe()(ctx, r, io.Discard, "zfs", append(args, "-o", swapProperty+"="+token, incomingName)...); err != nil {
		// incomingName was free: whatever is there now, this receive made.
		if _, cleanErr := z.Run(context.WithoutCancel(ctx), "zfs", "destroy", "-r", incomingName); cleanErr != nil && !strings.Contains(cleanErr.Error(), "does not exist") {
			return fmt.Errorf("%w; destroying what was received failed too (the next whole copy does): %w", err, cleanErr)
		}
		return err
	}
	// The old copy goes aside first and only then away: at every moment one
	// of the two holds a whole copy, and a swap that stops half way is
	// finished by restoreSwap. The old copy takes the received copy's token
	// first, which tells restoreSwap that the two are one swap's.
	if exists {
		if _, err := z.Run(ctx, "zfs", "set", swapProperty+"="+token, dataset); err != nil {
			return err
		}
		if _, err := z.Run(ctx, "zfs", "rename", dataset, previousName); err != nil {
			return err
		}
	}
	if _, err := z.Run(ctx, "zfs", "rename", incomingName, dataset); err != nil {
		if exists {
			if _, undoErr := z.Run(context.WithoutCancel(ctx), "zfs", "rename", previousName, dataset); undoErr != nil {
				return fmt.Errorf("%w; putting the old copy back from %s failed too (it is tried again before the tenant's data is next used): %w", err, previousName, undoErr)
			}
		}
		return err
	}
	// The swap is done: the old copy is never to be put back, even if
	// destroying it fails.
	if exists {
		if _, err := z.Run(ctx, "zfs", "inherit", swapProperty, previousName); err != nil {
			return err
		}
		if _, err := z.Run(ctx, "zfs", "destroy", "-r", previousName); err != nil {
			return err
		}
	}
	if _, err := z.Run(ctx, "zfs", "inherit", swapProperty, dataset); err != nil {
		return err
	}
	return z.Prune(ctx, tenant, replicaReason, keep)
}

// stagingKind is what a staging dataset holds: a whole copy received, or
// the copy it replaces.
type stagingKind int

const (
	incoming stagingKind = iota
	previous
)

// staged lists the tenant's staging datasets of kind: under today's name
// and under the name an older release gave. One counts only if it is a copy
// made for the tenant, unmounted; the old names of tenants "acme" and
// "acme-old" are told apart that way.
func (z ZFS) staged(ctx context.Context, tenant string, kind stagingKind) ([]string, error) {
	names := []string{z.Parent + "/" + incomingPrefix + tenant, z.Parent + "/" + legacyStagingPrefix + tenant}
	if kind == previous {
		names = []string{z.Parent + "/" + previousPrefix + tenant, z.Parent + "/" + legacyStagingPrefix + tenant + "-old"}
	}
	var found []string
	for _, name := range names {
		if _, ok, err := z.stagedCopy(ctx, tenant, name); err != nil {
			return nil, err
		} else if ok {
			found = append(found, name)
		}
	}
	return found, nil
}

// stagedCopy reads the staging dataset name, and reports whether it is
// there and an unmounted copy made for the tenant.
func (z ZFS) stagedCopy(ctx context.Context, tenant, name string) (properties, bool, error) {
	current, exists, err := z.properties(ctx, name)
	if err != nil || !exists {
		return properties{}, false, err
	}
	return current, current.owner == tenant && current.role == replicaRole && current.mounted != "yes", nil
}

// destroyAllStaged destroys every staging dataset of the tenant's, so that
// none is put back once the tenant's data is meant to be gone. The caller
// holds the tenant's dataset lock.
func (z ZFS) destroyAllStaged(ctx context.Context, tenant string) error {
	if err := z.destroyStaged(ctx, tenant, previous); err != nil {
		return err
	}
	return z.destroyStaged(ctx, tenant, incoming)
}

// destroyStaged destroys the tenant's staging datasets of kind. The
// caller holds the tenant's dataset lock.
func (z ZFS) destroyStaged(ctx context.Context, tenant string, kind stagingKind) error {
	names, err := z.staged(ctx, tenant, kind)
	if err != nil {
		return err
	}
	for _, name := range names {
		if _, err := z.Run(ctx, "zfs", "destroy", "-r", name); err != nil && !strings.Contains(err.Error(), "does not exist") {
			return err
		}
	}
	return nil
}

// restoreSwap finishes a whole copy's swap that stopped half way, after the
// old copy went aside and before the new one took its place, as a crash
// leaves it: the old copy, whole, goes back to the tenant's name, and the
// new one, which nothing vouched for yet, goes. That is only so while the
// copy received is still beside the old one and carries its token
// (swapProperty): an old copy alone, or one of another swap, is not put
// back. It reports whether the tenant has data now. The caller holds the
// tenant's dataset lock, and the tenant's dataset does not exist.
func (z ZFS) restoreSwap(ctx context.Context, tenant string) (bool, error) {
	previousName, incomingName := z.Parent+"/"+previousPrefix+tenant, z.Parent+"/"+incomingPrefix+tenant
	aside, ok, err := z.stagedCopy(ctx, tenant, previousName)
	if err != nil || !ok || aside.swap == "" {
		return false, err
	}
	received, ok, err := z.stagedCopy(ctx, tenant, incomingName)
	if err != nil || !ok || received.swap != aside.swap {
		return false, err
	}
	dataset := z.dataset(tenant)
	if _, err := z.Run(ctx, "zfs", "rename", previousName, dataset); err != nil {
		return false, fmt.Errorf("tenant %s's copy waits in %s, after a swap that stopped half way; putting it back failed: %w", tenant, previousName, err)
	}
	// The received copy goes before the token: until then the two still
	// tell they are one swap's, which no longer matters once the old copy
	// is back under the tenant's name.
	if err := z.destroyStaged(ctx, tenant, incoming); err != nil {
		return true, err
	}
	_, err = z.Run(ctx, "zfs", "inherit", swapProperty, dataset)
	return true, err
}

func (z ZFS) RollbackReplica(ctx context.Context, tenant string) error {
	if err := checkTenant(tenant); err != nil {
		return err
	}
	dataset := z.dataset(tenant)
	defer lockDataset(dataset)()
	// A copy whose swap stopped half way is rolled back, not dropped for
	// having no snapshots under the tenant's name.
	if _, exists, err := z.properties(ctx, dataset); err != nil {
		return err
	} else if !exists {
		if _, err := z.restoreSwap(ctx, tenant); err != nil {
			return err
		}
	}
	snapshots, err := z.Snapshots(ctx, tenant, replicaReason)
	if err != nil {
		return err
	}
	if len(snapshots) == 0 {
		return z.dropReplica(ctx, tenant)
	}
	current, exists, err := z.properties(ctx, dataset)
	if err != nil || !exists {
		return err
	}
	if current.owner != tenant || current.role != replicaRole || current.mounted == "yes" {
		return fmt.Errorf("dataset %s is not a copy of tenant %s: it is not rolled back", dataset, tenant)
	}
	_, err = z.Run(ctx, "zfs", "rollback", "-r", dataset+"@"+snapshots[len(snapshots)-1])
	return err
}

// IsReplica finishes first a whole copy's swap that stopped half way, as
// Exists does: the tenant's copy then waits under a staging name, and a
// caller told there is no copy would remove the tenant's data, staging
// copies and all, rather than keep the copy.
func (z ZFS) IsReplica(ctx context.Context, tenant string) (bool, error) {
	if err := checkTenant(tenant); err != nil {
		return false, err
	}
	dataset := z.dataset(tenant)
	current, exists, err := z.properties(ctx, dataset)
	if err != nil {
		return false, err
	}
	if !exists {
		defer lockDataset(dataset)()
		if current, exists, err = z.properties(ctx, dataset); err != nil {
			return false, err
		}
		if !exists {
			if restored, err := z.restoreSwap(ctx, tenant); err != nil || !restored {
				return false, err
			}
			if current, exists, err = z.properties(ctx, dataset); err != nil {
				return false, err
			}
		}
	}
	return exists && current.owner == tenant && current.role == replicaRole, nil
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

// DropReplica holds the dataset's lock from the check to the destroy: a
// copy that takes over meanwhile (Ensure mounts it under the same lock) is
// never destroyed.
func (z ZFS) DropReplica(ctx context.Context, tenant string) error {
	if err := checkTenant(tenant); err != nil {
		return err
	}
	defer lockDataset(z.dataset(tenant))()
	return z.dropReplica(ctx, tenant)
}

// dropReplica is DropReplica for a caller holding the dataset's lock. What
// a whole copy's receive left of the tenant's goes first, so that no later
// swap recovery puts a copy back that was meant to be gone, as it would
// for a new tenant of the same name.
func (z ZFS) dropReplica(ctx context.Context, tenant string) error {
	dataset := z.dataset(tenant)
	current, exists, err := z.properties(ctx, dataset)
	if err != nil {
		return err
	}
	if exists && (current.owner != tenant || current.role != replicaRole || current.mounted == "yes") {
		return fmt.Errorf("dataset %s is not a copy of tenant %s: it is not destroyed", dataset, tenant)
	}
	if err := z.destroyAllStaged(ctx, tenant); err != nil {
		return err
	}
	if !exists {
		return nil
	}
	_, err = z.Run(ctx, "zfs", "destroy", "-r", dataset)
	return err
}

// PutAside unmounts the dataset and renames it murder-failed-<tenant>-<why>-
// <time>, with canmount off: nothing mounts or uses it, and Murder never
// destroys it; the operator looks at it, then destroys it. It holds the
// dataset's lock, so no receive or promotion runs beside it.
func (z ZFS) PutAside(ctx context.Context, tenant, why string) (string, error) {
	if err := checkTenant(tenant); err != nil {
		return "", err
	}
	if !snapshotReason(why) {
		return "", fmt.Errorf("reason %q", why)
	}
	dataset := z.dataset(tenant)
	defer lockDataset(dataset)()
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
