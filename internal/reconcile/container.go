package reconcile

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"maps"
	"net"
	"slices"
	"strconv"

	"github.com/PowerPenguini/murder/internal/runtime"
	"github.com/PowerPenguini/murder/internal/secrets"
	"github.com/PowerPenguini/murder/internal/spec"
	"github.com/PowerPenguini/murder/internal/store"
)

// ContainerName and NetworkName name what Murder creates for a tenant.
func ContainerName(tenant string) string { return "murder-" + tenant }

func NetworkName(tenant string) string { return "murder-" + tenant }

// Backend is where a tenant listens: its fixed address in its network and
// the app's port. Only the host reaches it; other tenants cannot.
func Backend(record store.Record) (string, error) {
	_, address, err := runtime.Addresses(record.Subnet)
	if err != nil {
		return "", err
	}
	return net.JoinHostPort(address.String(), strconv.Itoa(record.Spec.Port)), nil
}

// Host and URL are a tenant's public address.
func (r *Reconciler) Host(tenant string) string { return tenant + "." + r.config.BaseDomain }

// BaseDomain is the domain tenants have their names below.
func (r *Reconciler) BaseDomain() string { return r.config.BaseDomain }

func (r *Reconciler) URL(tenant string) string { return r.config.Scheme + "://" + r.Host(tenant) }

func (r *Reconciler) containerSpec(record store.Record, dataPath string) (runtime.ContainerSpec, error) {
	tenant := record.Spec
	env := tenant.Render(spec.Values{Host: r.Host(tenant.Name), URL: r.URL(tenant.Name), Network: record.Subnet})
	digests := map[string]string{}
	for name := range tenant.Secrets {
		sealed, ok := record.Sealed[name]
		if !ok {
			return runtime.ContainerSpec{}, fmt.Errorf("secret %s has not been generated", name)
		}
		value, err := r.key.Open(secrets.Context(tenant.Name, name), sealed)
		if err != nil {
			return runtime.ContainerSpec{}, fmt.Errorf("secret %s: %w", name, err)
		}
		env[name] = value
		digests[name] = secrets.Digest(secrets.Context(tenant.Name, name), value)
	}
	pairs := make([]string, 0, len(env))
	for _, name := range slices.Sorted(maps.Keys(env)) {
		pairs = append(pairs, name+"="+env[name])
	}
	limits := tenant.Limits.Or(r.config.DefaultLimits)
	var tmpfs map[string]string
	if len(tenant.Tmpfs) > 0 {
		tmpfs = maps.Clone(tenant.Tmpfs)
	}
	_, address, err := runtime.Addresses(record.Subnet)
	if err != nil {
		return runtime.ContainerSpec{}, err
	}
	desired := runtime.ContainerSpec{
		Name:         ContainerName(tenant.Name),
		Image:        tenant.Image,
		Env:          pairs,
		User:         tenant.User,
		Network:      NetworkName(tenant.Name),
		Address:      address.String(),
		Mounts:       []runtime.Mount{{Source: dataPath, Target: tenant.DataPath}},
		MemoryBytes:  int64(limits.MemoryMB) << 20,
		NanoCPUs:     int64(limits.CPUs * 1e9),
		PIDs:         int64(limits.PIDs),
		ReadOnlyRoot: !tenant.WritableRoot,
		Tmpfs:        tmpfs,
		Labels: map[string]string{
			runtime.LabelTenant: tenant.Name,
			runtime.LabelImage:  tenant.Image,
		},
	}
	hash, err := r.hashInputs(record, dataPath, digests)
	if err != nil {
		return runtime.ContainerSpec{}, err
	}
	desired.Labels[runtime.LabelHash] = hash
	return desired, nil
}

// inputs are what a tenant's container is made from that the operator
// decides: the tenant's spec, its limits, address, names, secrets and data.
// How Murder turns them into a container is not among them, so a new
// release of Murder replaces no container; "murder tenant roll" applies
// what it changed. Fields only ever get added, omitted when empty, so a
// tenant whose inputs are as they were keeps its container across releases.
type inputs struct {
	Spec     spec.Tenant       `json:"spec"`
	Limits   spec.Limits       `json:"limits"`
	Subnet   string            `json:"subnet"`
	Host     string            `json:"host"`
	URL      string            `json:"url"`
	Sealed   map[string]string `json:"sealed,omitempty"`
	DataPath string            `json:"dataPath"`
	Rollout  int64             `json:"rollout,omitempty"`
	// Secrets are the digests of the secrets' values, in place of Sealed:
	// a master key rotation seals every value again, and replaces nothing.
	Secrets map[string]string `json:"secrets,omitempty"`
}

// inputsHash fingerprints a tenant's inputs as releases before master key
// rotations did, with the sealed values of its secrets: a container they
// made keeps it until it is replaced, or its secrets are sealed again.
func (r *Reconciler) inputsHash(record store.Record, dataPath string) (string, error) {
	return r.hashInputs(record, dataPath, nil)
}

// hashInputs fingerprints a tenant's inputs, with digests of its secrets'
// values; none, with their sealed values. A container whose label differs
// is replaced. JSON encodes maps in sorted key order, so equal inputs hash
// alike.
func (r *Reconciler) hashInputs(record store.Record, dataPath string, digests map[string]string) (string, error) {
	tenant := record.Spec
	tenant.Suspended = false // a resumed tenant gets a new container anyway
	// Health checks, routing and the disk limit (set on the volume as it
	// runs) use these; the container does not.
	tenant.HealthPath, tenant.Port, tenant.Limits.DiskGB = "", 0, 0
	limits := tenant.Limits.Or(r.config.DefaultLimits)
	limits.DiskGB = 0
	// No mounts, variables or secrets are none, however written.
	if len(tenant.Tmpfs) == 0 {
		tenant.Tmpfs = nil
	}
	if len(tenant.Env) == 0 {
		tenant.Env = nil
	}
	if len(tenant.Secrets) == 0 {
		tenant.Secrets = nil
	}
	sealed := map[string]string{}
	if digests == nil {
		for name := range tenant.Secrets {
			sealed[name] = record.Sealed[name]
		}
	}
	encoded, err := json.Marshal(inputs{
		Spec: tenant, Limits: limits, Subnet: record.Subnet,
		Host: r.Host(tenant.Name), URL: r.URL(tenant.Name), Sealed: sealed, DataPath: dataPath, Rollout: record.Rollout, Secrets: digests,
	})
	if err != nil {
		return "", err
	}
	sum := sha256.Sum256(encoded)
	return hex.EncodeToString(sum[:16]), nil
}

// legacyHash is the label releases before inputs hashing gave a container:
// the fingerprint of the container as made, before its hash label. Such a
// container keeps it until it is replaced.
func legacyHash(desired runtime.ContainerSpec) (string, error) {
	desired.Labels = maps.Clone(desired.Labels)
	delete(desired.Labels, runtime.LabelHash)
	encoded, err := json.Marshal(desired)
	if err != nil {
		return "", err
	}
	sum := sha256.Sum256(encoded)
	return hex.EncodeToString(sum[:16]), nil
}
