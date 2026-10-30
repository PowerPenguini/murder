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
}

// NewTable versions routes by their content.
func NewTable(routes map[string]Route) Table {
	if routes == nil {
		routes = map[string]Route{}
	}
	sum := sha256.New()
	for _, tenant := range slices.Sorted(maps.Keys(routes)) {
		route := routes[tenant]
		fmt.Fprintf(sum, "%s\x00%s\x00%s\x00%t\x00%t\x00", tenant, route.Backend, route.Relay, route.Suspended, route.Moving)
	}
	return Table{Version: hex.EncodeToString(sum.Sum(nil))[:16], Routes: routes}
}

// LiveTable is the table the edge serves from, replaced whole.
type LiveTable struct {
	table   atomic.Pointer[Table]
	// ready is set once the table holds routes the agent gave, now or
	// before a restart.
	ready   atomic.Bool
	updated atomic.Int64
	// changed is closed, and replaced, whenever the table is replaced.
	mu      sync.Mutex
	changed chan struct{}
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
	l.updated.Store(time.Now().UnixNano())
	l.replaced()
}

// Restore serves a saved table until the agent confirms one; it does not
// count as confirmed.
func (l *LiveTable) Restore(table Table) {
	l.table.Store(&table)
	l.ready.Store(true)
	l.replaced()
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
func (l *LiveTable) Touch() { l.updated.Store(time.Now().UnixNano()) }

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
	return NewTable(table.Routes), true, nil
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
