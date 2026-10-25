// Package store keeps the desired state of every tenant. Stage 2 keeps it in
// one local file; stage 3 replaces the implementation with a replicated log
// behind the same interface.
package store

import (
	"encoding/binary"
	"encoding/json"
	"errors"
	"fmt"
	"maps"
	"net/netip"
	"os"
	"path/filepath"
	"slices"
	"sync"
	"sync/atomic"
	"syscall"
	"time"

	"github.com/PowerPenguini/corral/internal/spec"
)

// ErrNotFound reports a tenant that does not exist.
var ErrNotFound = errors.New("tenant not found")

// ErrExists reports a tenant name that is already taken.
var ErrExists = errors.New("tenant already exists")

// Record is one tenant's desired state plus what Corral assigned to it.
type Record struct {
	Spec spec.Tenant `json:"spec"`
	// Sealed holds each generated secret, sealed with the master key.
	Sealed map[string]string `json:"sealed,omitempty"`
	// HostPort is the loopback port the edge proxy reaches the tenant on.
	HostPort int `json:"hostPort"`
	// Subnet is the tenant network's IPv4 range.
	Subnet string `json:"subnet"`
	// DataImage is the image whose container last owned the data. It is
	// observed state: the reconciler sets it and it does not bump Generation.
	DataImage  string    `json:"dataImage,omitempty"`
	Generation int64     `json:"generation"`
	CreatedAt  time.Time `json:"createdAt"`
	UpdatedAt  time.Time `json:"updatedAt"`
}

func (r Record) clone() Record {
	r.Spec.Env = maps.Clone(r.Spec.Env)
	r.Spec.Secrets = maps.Clone(r.Spec.Secrets)
	r.Spec.Tmpfs = maps.Clone(r.Spec.Tmpfs)
	r.Sealed = maps.Clone(r.Sealed)
	return r
}

// Store is the desired state of all tenants.
type Store interface {
	List() []Record
	Get(name string) (Record, error)
	// Create adds a tenant, assigning its host port and subnet.
	Create(record Record) (Record, error)
	// Update applies change to a copy of the tenant, stores the result and
	// bumps its generation.
	Update(name string, change func(*Record) error) (Record, error)
	// Observe records the image that now owns a tenant's data.
	Observe(name, dataImage string) error
	Delete(name string) error
	// Changed is signalled after every successful change of desired state.
	Changed() <-chan struct{}
}

// Options sets what a file store assigns to new tenants.
type Options struct {
	// PortFrom and PortTo bound the loopback ports tenants are published on.
	PortFrom, PortTo int
	// NetworkPool is split into one /28 per tenant network.
	NetworkPool netip.Prefix
}

const subnetBits = 28

type fileState struct {
	Version int               `json:"version"`
	Tenants map[string]Record `json:"tenants"`
}

// File stores the state as JSON in one file, rewritten atomically. Reads
// never wait for a write: they see the last committed state.
type File struct {
	path    string
	options Options
	now     func() time.Time
	lock    *os.File
	mu      sync.Mutex // serializes writers
	state   atomic.Pointer[fileState]
	changed chan struct{}
}

// InitFile writes an empty state at path, refusing to overwrite one.
func InitFile(path string) error {
	if err := os.MkdirAll(filepath.Dir(path), 0o700); err != nil {
		return err
	}
	if _, err := os.Stat(path); err == nil {
		return fmt.Errorf("state %s already exists", path)
	}
	f := &File{path: path}
	f.state.Store(&fileState{Version: 1, Tenants: map[string]Record{}})
	return f.save(f.state.Load())
}

// OpenFile loads the state at path and takes an exclusive lock on it, so a
// second agent cannot run on the same state. A missing file is an error, not
// an empty state: an empty state would remove every tenant's containers.
func OpenFile(path string, options Options) (*File, error) {
	if options.PortFrom < 1024 || options.PortTo > 65535 || options.PortFrom > options.PortTo {
		return nil, fmt.Errorf("host port range %d-%d is invalid", options.PortFrom, options.PortTo)
	}
	if !options.NetworkPool.IsValid() || !options.NetworkPool.Addr().Is4() || options.NetworkPool.Bits() > subnetBits-4 {
		return nil, fmt.Errorf("network pool %v must be an IPv4 prefix of /%d or larger", options.NetworkPool, subnetBits-4)
	}
	data, err := os.ReadFile(path)
	if errors.Is(err, os.ErrNotExist) {
		return nil, fmt.Errorf("state %s does not exist; run \"corral init\" on a new node, or restore the file", path)
	}
	if err != nil {
		return nil, err
	}
	state := &fileState{}
	if err := json.Unmarshal(data, state); err != nil {
		return nil, fmt.Errorf("parse state %s: %w", path, err)
	}
	if state.Version != 1 {
		return nil, fmt.Errorf("state %s has unsupported version %d", path, state.Version)
	}
	if state.Tenants == nil {
		state.Tenants = map[string]Record{}
	}
	lock, err := os.OpenFile(path+".lock", os.O_CREATE|os.O_RDWR, 0o600)
	if err != nil {
		return nil, err
	}
	if err := syscall.Flock(int(lock.Fd()), syscall.LOCK_EX|syscall.LOCK_NB); err != nil {
		_ = lock.Close()
		return nil, fmt.Errorf("state %s is in use by another agent", path)
	}
	f := &File{path: path, options: options, now: time.Now, lock: lock, changed: make(chan struct{}, 1)}
	f.state.Store(state)
	return f, nil
}

// Close releases the state lock.
func (f *File) Close() error { return f.lock.Close() }

func (f *File) List() []Record {
	state := f.state.Load()
	names := slices.Sorted(maps.Keys(state.Tenants))
	records := make([]Record, 0, len(names))
	for _, name := range names {
		records = append(records, state.Tenants[name].clone())
	}
	return records
}

func (f *File) Get(name string) (Record, error) {
	record, ok := f.state.Load().Tenants[name]
	if !ok {
		return Record{}, ErrNotFound
	}
	return record.clone(), nil
}

func (f *File) Create(record Record) (Record, error) {
	if err := record.Spec.Validate(); err != nil {
		return Record{}, err
	}
	f.mu.Lock()
	defer f.mu.Unlock()
	state := f.state.Load()
	if _, ok := state.Tenants[record.Spec.Name]; ok {
		return Record{}, ErrExists
	}
	port, err := f.freePort(state)
	if err != nil {
		return Record{}, err
	}
	subnet, err := f.freeSubnet(state)
	if err != nil {
		return Record{}, err
	}
	now := f.now().UTC()
	record = record.clone()
	record.HostPort, record.Subnet, record.DataImage = port, subnet, ""
	record.Generation = 1
	record.CreatedAt, record.UpdatedAt = now, now
	return record, f.commit(true, func(next *fileState) { next.Tenants[record.Spec.Name] = record })
}

func (f *File) Update(name string, change func(*Record) error) (Record, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	current, ok := f.state.Load().Tenants[name]
	if !ok {
		return Record{}, ErrNotFound
	}
	next := current.clone()
	if err := change(&next); err != nil {
		return Record{}, err
	}
	if next.Spec.Name != name || next.HostPort != current.HostPort || next.Subnet != current.Subnet || !next.CreatedAt.Equal(current.CreatedAt) || next.DataImage != current.DataImage {
		return Record{}, errors.New("a tenant's name, host port, subnet, creation time and data image cannot be changed by an update")
	}
	if err := next.Spec.Validate(); err != nil {
		return Record{}, err
	}
	next.Generation = current.Generation + 1
	next.UpdatedAt = f.now().UTC()
	return next, f.commit(true, func(state *fileState) { state.Tenants[name] = next })
}

func (f *File) Observe(name, dataImage string) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	current, ok := f.state.Load().Tenants[name]
	if !ok {
		return ErrNotFound
	}
	if current.DataImage == dataImage {
		return nil
	}
	current.DataImage = dataImage
	return f.commit(false, func(state *fileState) { state.Tenants[name] = current })
}

func (f *File) Delete(name string) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	if _, ok := f.state.Load().Tenants[name]; !ok {
		return ErrNotFound
	}
	return f.commit(true, func(state *fileState) { delete(state.Tenants, name) })
}

func (f *File) Changed() <-chan struct{} { return f.changed }

func (f *File) freePort(state *fileState) (int, error) {
	used := make(map[int]bool, len(state.Tenants))
	for _, record := range state.Tenants {
		used[record.HostPort] = true
	}
	for port := f.options.PortFrom; port <= f.options.PortTo; port++ {
		if !used[port] {
			return port, nil
		}
	}
	return 0, fmt.Errorf("no free host port in %d-%d", f.options.PortFrom, f.options.PortTo)
}

// freeSubnet returns the first /28 of the network pool no tenant uses.
func (f *File) freeSubnet(state *fileState) (string, error) {
	used := make(map[string]bool, len(state.Tenants))
	for _, record := range state.Tenants {
		used[record.Subnet] = true
	}
	base := f.options.NetworkPool.Masked().Addr().As4()
	start := binary.BigEndian.Uint32(base[:])
	count := uint32(1) << (subnetBits - f.options.NetworkPool.Bits())
	for i := range count {
		var addr [4]byte
		binary.BigEndian.PutUint32(addr[:], start+i<<(32-subnetBits))
		subnet := netip.PrefixFrom(netip.AddrFrom4(addr), subnetBits).String()
		if !used[subnet] {
			return subnet, nil
		}
	}
	return "", fmt.Errorf("network pool %v has no free /%d", f.options.NetworkPool, subnetBits)
}

// commit builds the next state from a copy, persists it and only then makes
// it current, so a failed write leaves memory and disk in agreement.
func (f *File) commit(signal bool, mutate func(*fileState)) error {
	current := f.state.Load()
	next := &fileState{Version: current.Version, Tenants: maps.Clone(current.Tenants)}
	mutate(next)
	if err := f.save(next); err != nil {
		return err
	}
	f.state.Store(next)
	if signal {
		select {
		case f.changed <- struct{}{}:
		default:
		}
	}
	return nil
}

func (f *File) save(state *fileState) error {
	data, err := json.MarshalIndent(state, "", "  ")
	if err != nil {
		return err
	}
	dir := filepath.Dir(f.path)
	temp, err := os.CreateTemp(dir, ".state-*")
	if err != nil {
		return err
	}
	defer os.Remove(temp.Name())
	if err := temp.Chmod(0o600); err != nil {
		_ = temp.Close()
		return err
	}
	if _, err := temp.Write(append(data, '\n')); err != nil {
		_ = temp.Close()
		return err
	}
	if err := temp.Sync(); err != nil {
		_ = temp.Close()
		return err
	}
	if err := temp.Close(); err != nil {
		return err
	}
	if err := os.Rename(temp.Name(), f.path); err != nil {
		return err
	}
	directory, err := os.Open(dir)
	if err != nil {
		return err
	}
	defer directory.Close()
	return directory.Sync()
}
