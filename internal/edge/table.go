package edge

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"maps"
	"os"
	"path/filepath"
	"slices"
	"sync"
	"sync/atomic"
	"time"
)

// Table is a set of routes with a version that changes with them. The agent
// makes it; the edge serves from its last copy.
type Table struct {
	Version string           `json:"version"`
	Routes  map[string]Route `json:"routes"`
	// Domains are tenants' own host names, by name; Challenges the ACME
	// HTTP-01 challenges every edge answers, by token.
	Domains    map[string]DomainRoute    `json:"domains,omitempty"`
	Challenges map[string]ChallengeRoute `json:"challenges,omitempty"`
}

// DomainRoute is a tenant's own host name: the tenant it leads to, and
// its certificate once it has one: the chain, PEM, and its private key,
// PEM, sealed with the master key.
type DomainRoute struct {
	Tenant      string `json:"tenant"`
	Certificate string `json:"certificate,omitempty"`
	Key         string `json:"key,omitempty"`
}

// ChallengeRoute is an ACME HTTP-01 challenge: the edge answers
// http://Domain/.well-known/acme-challenge/<token> with KeyAuth.
type ChallengeRoute struct {
	Domain  string `json:"domain"`
	KeyAuth string `json:"keyAuth"`
}

// NewTable versions routes by their content.
func NewTable(routes map[string]Route) Table {
	return NewTableWith(routes, nil, nil)
}

// NewTableWith versions routes, domains and challenges by their content.
func NewTableWith(routes map[string]Route, domains map[string]DomainRoute, challenges map[string]ChallengeRoute) Table {
	if routes == nil {
		routes = map[string]Route{}
	}
	sum := sha256.New()
	for _, tenant := range slices.Sorted(maps.Keys(routes)) {
		route := routes[tenant]
		fmt.Fprintf(sum, "%s\x00%s\x00%s\x00%t\x00%t\x00", tenant, route.Backend, route.Relay, route.Suspended, route.Moving)
		// A route without a canonical host keeps the version it had
		// before them.
		if route.Canonical != "" {
			fmt.Fprintf(sum, "canonical\x00%s\x00", route.Canonical)
		}
	}
	// A table without domains keeps the version it had before them.
	for _, name := range slices.Sorted(maps.Keys(domains)) {
		domain := domains[name]
		fmt.Fprintf(sum, "domain\x00%s\x00%s\x00%s\x00%s\x00", name, domain.Tenant, domain.Certificate, domain.Key)
	}
	for _, token := range slices.Sorted(maps.Keys(challenges)) {
		challenge := challenges[token]
		fmt.Fprintf(sum, "challenge\x00%s\x00%s\x00%s\x00", token, challenge.Domain, challenge.KeyAuth)
	}
	if len(domains) == 0 {
		domains = nil
	}
	if len(challenges) == 0 {
		challenges = nil
	}
	return Table{Version: hex.EncodeToString(sum.Sum(nil))[:16], Routes: routes, Domains: domains, Challenges: challenges}
}

// LiveTable is the table the edge serves from, replaced whole.
type LiveTable struct {
	table atomic.Pointer[Table]
	// ready is set once the table holds routes the agent gave, now or
	// before a restart.
	ready   atomic.Bool
	updated atomic.Int64
	// restored is when a saved table was restored, in Unix nanoseconds;
	// zero if none was.
	restored atomic.Int64
	// changed is closed, and replaced, whenever the table is replaced;
	// confirmed is closed once the agent first confirms a table.
	mu          sync.Mutex
	changed     chan struct{}
	confirmed   chan struct{}
	confirmOnce sync.Once
}

// Watch returns a channel closed when the table is next replaced.
func (l *LiveTable) Watch() <-chan struct{} {
	l.mu.Lock()
	defer l.mu.Unlock()
	if l.changed == nil {
		l.changed = make(chan struct{})
	}
	return l.changed
}

func (l *LiveTable) replaced() {
	l.mu.Lock()
	defer l.mu.Unlock()
	if l.changed != nil {
		close(l.changed)
		l.changed = nil
	}
}

// Route implements Routes.
func (l *LiveTable) Route(tenant string) (Route, bool) {
	table := l.table.Load()
	if table == nil {
		return Route{}, false
	}
	route, ok := table.Routes[tenant]
	return route, ok
}

// Domain implements Domains.
func (l *LiveTable) Domain(host string) (DomainRoute, bool) {
	table := l.table.Load()
	if table == nil {
		return DomainRoute{}, false
	}
	domain, ok := table.Domains[host]
	return domain, ok
}

// Challenge implements Challenges.
func (l *LiveTable) Challenge(token string) (ChallengeRoute, bool) {
	table := l.table.Load()
	if table == nil {
		return ChallengeRoute{}, false
	}
	challenge, ok := table.Challenges[token]
	return challenge, ok
}

// Load returns the table served now; zero before the first.
func (l *LiveTable) Load() Table {
	if table := l.table.Load(); table != nil {
		return *table
	}
	return Table{}
}

// Set serves table from now on.
func (l *LiveTable) Set(table Table) {
	l.table.Store(&table)
	l.ready.Store(true)
	l.confirm()
	l.replaced()
}

// Restore serves a saved table until the agent confirms one; it does not
// count as confirmed.
func (l *LiveTable) Restore(table Table) {
	l.table.Store(&table)
	l.ready.Store(true)
	l.restored.Store(time.Now().UnixNano())
	l.replaced()
}

// Unconfirmed reports whether the table is one restored from disk that
// the agent has not confirmed yet, since when, and a channel closed once
// it confirms one.
func (l *LiveTable) Unconfirmed() (since time.Time, confirmed <-chan struct{}, ok bool) {
	at := l.restored.Load()
	if at == 0 || l.updated.Load() != 0 {
		return time.Time{}, nil, false
	}
	return time.Unix(0, at), l.confirmedChan(), true
}

func (l *LiveTable) confirmedChan() chan struct{} {
	l.mu.Lock()
	defer l.mu.Unlock()
	if l.confirmed == nil {
		l.confirmed = make(chan struct{})
	}
	return l.confirmed
}

// confirm notes that the agent confirmed the table as it is now.
func (l *LiveTable) confirm() {
	l.updated.Store(time.Now().UnixNano())
	l.confirmOnce.Do(func() { close(l.confirmedChan()) })
}

// Ready reports whether the table holds routes the agent gave: until then,
// a tenant it does not know may simply not have reached it yet.
func (l *LiveTable) Ready() bool { return l.ready.Load() }

// Updated is when the agent last confirmed the table; zero before.
func (l *LiveTable) Updated() time.Time {
	if at := l.updated.Load(); at != 0 {
		return time.Unix(0, at)
	}
	return time.Time{}
}

// Touch notes that the agent confirmed the table as it is.
func (l *LiveTable) Touch() { l.confirm() }

// LoadTable reads a table SaveTable wrote; a missing file is no table.
func LoadTable(path string) (Table, bool, error) {
	data, err := os.ReadFile(path)
	if errors.Is(err, os.ErrNotExist) {
		return Table{}, false, nil
	}
	if err != nil {
		return Table{}, false, err
	}
	var table Table
	if err := json.Unmarshal(data, &table); err != nil {
		return Table{}, false, fmt.Errorf("routes %s: %w", path, err)
	}
	return NewTableWith(table.Routes, table.Domains, table.Challenges), true, nil
}

// SaveTable writes table to path whole, or not at all.
func SaveTable(path string, table Table) error {
	data, err := json.Marshal(table)
	if err != nil {
		return err
	}
	if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
		return err
	}
	temp, err := os.CreateTemp(filepath.Dir(path), filepath.Base(path)+".*")
	if err != nil {
		return err
	}
	defer os.Remove(temp.Name())
	if _, err := temp.Write(data); err != nil {
		_ = temp.Close()
		return err
	}
	if err := errors.Join(temp.Sync(), temp.Close()); err != nil {
		return err
	}
	return os.Rename(temp.Name(), path)
}
