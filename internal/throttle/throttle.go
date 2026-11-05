// Package throttle keeps a warning that may come every second, such as a
// peer refused each time it tries again, from filling the log: the first
// is logged at once, then at most one an interval for the same reason and
// source, saying how many were left out meanwhile.
package throttle

import (
	"log/slog"
	"sync"
	"time"
)

// Every is how often a warning for one reason and source is logged again
// while it keeps coming.
const Every = time.Minute

// kept bounds how many reasons and sources are remembered; past it, those
// not logged within the interval are forgotten, with what they left out.
const kept = 1024

// Log logs warnings, at most one an interval for each message and
// source. The zero value is ready to use.
type Log struct {
	// Every is the interval; zero is a minute.
	Every time.Duration
	// now is the clock, for tests.
	now func() time.Time

	mu   sync.Mutex
	seen map[key]*seen
}

type key struct{ message, source string }

type seen struct {
	logged time.Time
	// left is how many were left out since the last one logged.
	left int
}

// Warn logs message, with args, at Warn level, unless one for the same
// message and source was logged within the interval: then it counts it,
// and the next one logged says how many were left out ("suppressed").
// source names where it comes from, such as a node or an address without
// its port.
func (t *Log) Warn(log *slog.Logger, source, message string, args ...any) {
	if left, ok := t.allow(source, message); ok {
		if left > 0 {
			args = append(args, "suppressed", left)
		}
		log.Warn(message, args...)
	}
}

// allow reports whether a warning for message from source is logged now,
// and how many were left out before it.
func (t *Log) allow(source, message string) (int, bool) {
	now := time.Now()
	if t.now != nil {
		now = t.now()
	}
	every := t.Every
	if every <= 0 {
		every = Every
	}
	t.mu.Lock()
	defer t.mu.Unlock()
	if t.seen == nil {
		t.seen = map[key]*seen{}
	}
	k := key{message, source}
	entry, ok := t.seen[k]
	if ok && now.Sub(entry.logged) < every {
		entry.left++
		return 0, false
	}
	if !ok {
		if len(t.seen) >= kept {
			for old, entry := range t.seen {
				if now.Sub(entry.logged) >= every {
					delete(t.seen, old)
				}
			}
			// So many sources within one interval are a flood: the
			// ones beyond are not logged until it passes.
			if len(t.seen) >= kept {
				return 0, false
			}
		}
		entry = &seen{}
		t.seen[k] = entry
	}
	left := entry.left
	entry.logged, entry.left = now, 0
	return left, true
}
