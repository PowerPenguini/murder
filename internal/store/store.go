// Package store keeps the desired state of every tenant. Stage 2 keeps it in
// one local file; stage 3 replaces the implementation with a replicated log
// behind the same interface.
package store

import (
	"encoding/json"
	"errors"
	"fmt"
	"maps"
	"os"
	"path/filepath"
	"slices"
	"sync"
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
	HostPort   int       `json:"hostPort"`
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
	// Create adds a tenant, assigning its host port.
	Create(record Record) (Record, error)
	// Update applies change to a copy of the tenant and stores the result.
	Update(name string, change func(*Record) error) (Record, error)
	Delete(name string) error
	// Changed is signalled after every successful write.
	Changed() <-chan struct{}
}

type fileState struct {
	Version int               `json:"version"`
	Tenants map[string]Record `json:"tenants"`
}

// File stores the state as JSON in one file, rewritten atomically.
type File struct {
	path     string
	portFrom int
	portTo   int
	now      func() time.Time
	mu       sync.Mutex
	state    fileState
	changed  chan struct{}
}

// OpenFile loads the state at path, creating an empty state if it is missing.
// Host ports are assigned from [portFrom, portTo].
func OpenFile(path string, portFrom, portTo int) (*File, error) {
	if portFrom < 1024 || portTo > 65535 || portFrom > portTo {
		return nil, fmt.Errorf("host port range %d-%d is invalid", portFrom, portTo)
	}
	f := &File{path: path, portFrom: portFrom, portTo: portTo, now: time.Now, changed: make(chan struct{}, 1), state: fileState{Version: 1, Tenants: map[string]Record{}}}
	data, err := os.ReadFile(path)
	switch {
	case errors.Is(err, os.ErrNotExist):
		if err := os.MkdirAll(filepath.Dir(path), 0o700); err != nil {
			return nil, err
		}
		return f, f.save()
	case err != nil:
		return nil, err
	}
	if err := json.Unmarshal(data, &f.state); err != nil {
		return nil, fmt.Errorf("parse state %s: %w", path, err)
	}
	if f.state.Version != 1 {
		return nil, fmt.Errorf("state %s has unsupported version %d", path, f.state.Version)
	}
	if f.state.Tenants == nil {
		f.state.Tenants = map[string]Record{}
	}
	return f, nil
}

func (f *File) List() []Record {
	f.mu.Lock()
	defer f.mu.Unlock()
	names := slices.Sorted(maps.Keys(f.state.Tenants))
	records := make([]Record, 0, len(names))
	for _, name := range names {
		records = append(records, f.state.Tenants[name].clone())
	}
	return records
}

func (f *File) Get(name string) (Record, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	record, ok := f.state.Tenants[name]
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
	if _, ok := f.state.Tenants[record.Spec.Name]; ok {
		return Record{}, ErrExists
	}
	port, err := f.freePort()
	if err != nil {
		return Record{}, err
	}
	now := f.now().UTC()
	record = record.clone()
	record.HostPort = port
	record.Generation = 1
	record.CreatedAt, record.UpdatedAt = now, now
	return record, f.commit(func(state *fileState) { state.Tenants[record.Spec.Name] = record })
}

func (f *File) Update(name string, change func(*Record) error) (Record, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	current, ok := f.state.Tenants[name]
	if !ok {
		return Record{}, ErrNotFound
	}
	next := current.clone()
	if err := change(&next); err != nil {
		return Record{}, err
	}
	if next.Spec.Name != name || next.HostPort != current.HostPort || !next.CreatedAt.Equal(current.CreatedAt) {
		return Record{}, errors.New("a tenant's name, host port and creation time cannot change")
	}
	if err := next.Spec.Validate(); err != nil {
		return Record{}, err
	}
	next.Generation = current.Generation + 1
	next.UpdatedAt = f.now().UTC()
	return next, f.commit(func(state *fileState) { state.Tenants[name] = next })
}

func (f *File) Delete(name string) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	if _, ok := f.state.Tenants[name]; !ok {
		return ErrNotFound
	}
	return f.commit(func(state *fileState) { delete(state.Tenants, name) })
}

func (f *File) Changed() <-chan struct{} { return f.changed }

func (f *File) freePort() (int, error) {
	used := make(map[int]bool, len(f.state.Tenants))
	for _, record := range f.state.Tenants {
		used[record.HostPort] = true
	}
	for port := f.portFrom; port <= f.portTo; port++ {
		if !used[port] {
			return port, nil
		}
	}
	return 0, fmt.Errorf("no free host port in %d-%d", f.portFrom, f.portTo)
}

// commit applies mutate to a copy of the state, persists it and only then
// makes it current, so a failed write leaves memory and disk in agreement.
func (f *File) commit(mutate func(*fileState)) error {
	next := fileState{Version: f.state.Version, Tenants: maps.Clone(f.state.Tenants)}
	mutate(&next)
	previous := f.state
	f.state = next
	if err := f.save(); err != nil {
		f.state = previous
		return err
	}
	select {
	case f.changed <- struct{}{}:
	default:
	}
	return nil
}

func (f *File) save() error {
	data, err := json.MarshalIndent(f.state, "", "  ")
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
