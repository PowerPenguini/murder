package panel

import (
	"context"
	"crypto/rand"
	"encoding/base64"
	"encoding/hex"
	"fmt"
	"maps"
	"slices"
	"sync"
	"time"

	"github.com/PowerPenguini/murder/internal/api"
	"github.com/PowerPenguini/murder/internal/store"
)

// MemoryAuth keeps the panel's people in memory, as the agent keeps them
// in the state: for the demo and tests.
type MemoryAuth struct {
	mu          sync.Mutex
	users       map[string]store.PanelUser
	passkeys    map[string]store.Passkey
	enrollments map[string]store.Enrollment
	sessions    map[string]store.PanelSession
	secret      string
}

func NewMemoryAuth() *MemoryAuth {
	return &MemoryAuth{users: map[string]store.PanelUser{}, passkeys: map[string]store.Passkey{}, enrollments: map[string]store.Enrollment{}, sessions: map[string]store.PanelSession{}}
}

// Invite adds the person if new and returns an invitation for them.
func (m *MemoryAuth) Invite(name string) string {
	m.mu.Lock()
	defer m.mu.Unlock()
	if _, ok := m.users[name]; !ok {
		handle := make([]byte, 32)
		_, _ = rand.Read(handle)
		m.users[name] = store.PanelUser{Name: name, Handle: base64.RawURLEncoding.EncodeToString(handle), CreatedAt: time.Now(), CreatedBy: "demo"}
	}
	token := Token(32)
	m.enrollments[TokenHash(token)] = store.Enrollment{Hash: TokenHash(token), User: name, Kind: store.EnrollInvite, ExpiresAt: time.Now().Add(InviteLength)}
	return token
}

func (m *MemoryAuth) user(name string) (store.PanelUser, error) {
	u, ok := m.users[name]
	if !ok {
		return u, fmt.Errorf("person %s: %w", name, store.ErrNotFound)
	}
	u.Passkeys = nil
	for _, id := range slices.Sorted(maps.Keys(m.passkeys)) {
		if k := m.passkeys[id]; k.User == name {
			u.Passkeys = append(u.Passkeys, k)
		}
	}
	for _, e := range m.enrollments {
		if e.User == name && e.Kind == store.EnrollRecovery {
			u.Recovery++
		}
	}
	return u, nil
}

func (m *MemoryAuth) PanelUser(_ context.Context, name string) (store.PanelUser, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.user(name)
}

func (m *MemoryAuth) PanelUserByHandle(_ context.Context, handle string) (store.PanelUser, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	for name, u := range m.users {
		if u.Handle == handle {
			return m.user(name)
		}
	}
	return store.PanelUser{}, store.ErrNotFound
}

func (m *MemoryAuth) Enrollment(_ context.Context, hash string) (store.Enrollment, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	e, ok := m.enrollments[hash]
	if !ok || (!e.ExpiresAt.IsZero() && time.Now().After(e.ExpiresAt)) {
		return e, store.ErrNotFound
	}
	return e, nil
}

func (m *MemoryAuth) AddPasskey(_ context.Context, request api.PasskeyRequest) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	e, ok := m.enrollments[request.Hash]
	if !ok || e.User != request.Passkey.User {
		return store.ErrNotFound
	}
	delete(m.enrollments, request.Hash)
	key := request.Passkey
	key.CreatedAt = time.Now()
	m.passkeys[key.ID] = key
	return nil
}

func (m *MemoryAuth) UsePasskey(_ context.Context, id, data string) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	key, ok := m.passkeys[id]
	if !ok {
		return store.ErrNotFound
	}
	key.Data, key.UsedAt = data, time.Now()
	m.passkeys[id] = key
	return nil
}

func (m *MemoryAuth) SetRecoveryCodes(_ context.Context, name string, hashes []string) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	for hash, e := range m.enrollments {
		if e.User == name && e.Kind == store.EnrollRecovery {
			delete(m.enrollments, hash)
		}
	}
	for _, hash := range hashes {
		m.enrollments[hash] = store.Enrollment{Hash: hash, User: name, Kind: store.EnrollRecovery}
	}
	return nil
}

func (m *MemoryAuth) StartPanelSession(_ context.Context, request api.SessionRequest) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.sessions[request.Hash] = store.PanelSession{Hash: request.Hash, User: request.User, ExpiresAt: request.Expires}
	return nil
}

func (m *MemoryAuth) PanelSession(_ context.Context, hash string) (store.PanelSession, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	session, ok := m.sessions[hash]
	if !ok || time.Now().After(session.ExpiresAt) {
		return session, store.ErrNotFound
	}
	return session, nil
}

func (m *MemoryAuth) EndPanelSession(_ context.Context, hash string) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	delete(m.sessions, hash)
	return nil
}

func (m *MemoryAuth) PanelSecret(context.Context, string) (string, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.secret == "" {
		raw := make([]byte, 32)
		_, _ = rand.Read(raw)
		m.secret = hex.EncodeToString(raw)
	}
	return m.secret, nil
}

// DemoAccess is the demo's people and devices, in memory.
type DemoAccess struct {
	Demo
	*MemoryAuth
	devices map[string]store.Device
}

func NewDemoAccess(people *MemoryAuth) *DemoAccess {
	return &DemoAccess{MemoryAuth: people, devices: map[string]store.Device{}}
}

func (d *DemoAccess) PanelUsers(context.Context) ([]store.PanelUser, error) {
	d.mu.Lock()
	defer d.mu.Unlock()
	var users []store.PanelUser
	for _, name := range slices.Sorted(maps.Keys(d.users)) {
		u, _ := d.user(name)
		users = append(users, u)
	}
	return users, nil
}

func (d *DemoAccess) InvitePanelUser(_ context.Context, name string, request api.InviteRequest) (store.PanelUser, error) {
	d.Invite(name)
	d.mu.Lock()
	defer d.mu.Unlock()
	d.enrollments[request.Hash] = store.Enrollment{Hash: request.Hash, User: name, Kind: store.EnrollInvite, ExpiresAt: request.Expires}
	return d.user(name)
}

func (d *DemoAccess) RemovePanelUser(_ context.Context, name string) error {
	d.mu.Lock()
	defer d.mu.Unlock()
	delete(d.users, name)
	for id, k := range d.passkeys {
		if k.User == name {
			delete(d.passkeys, id)
		}
	}
	return nil
}

func (d *DemoAccess) RemovePasskey(_ context.Context, id string) error {
	d.mu.Lock()
	defer d.mu.Unlock()
	delete(d.passkeys, id)
	return nil
}

func (d *DemoAccess) Devices(context.Context) ([]store.Device, error) {
	d.mu.Lock()
	defer d.mu.Unlock()
	var out []store.Device
	for _, name := range slices.Sorted(maps.Keys(d.devices)) {
		out = append(out, d.devices[name])
	}
	return out, nil
}

func (d *DemoAccess) AddDevice(_ context.Context, request api.DeviceRequest) (store.Device, error) {
	d.mu.Lock()
	defer d.mu.Unlock()
	address := request.Network.Addr()
	for range 100 + len(d.devices) {
		address = address.Next()
	}
	device := store.Device{Name: request.Name, Key: request.Key, Address: address, CreatedAt: time.Now(), CreatedBy: "demo"}
	d.devices[request.Name] = device
	return device, nil
}

func (d *DemoAccess) RemoveDevice(_ context.Context, name string) error {
	d.mu.Lock()
	defer d.mu.Unlock()
	delete(d.devices, name)
	return nil
}
