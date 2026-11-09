package main

import (
	"bytes"
	"errors"
	"log/slog"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/PowerPenguini/murder/internal/setup"
	"github.com/PowerPenguini/murder/internal/store"
)

// awayStore is a node's state for marking it away: it names the node or
// not, and its write takes as long as block lets it.
type awayStore struct {
	member bool
	block  chan struct{}
	err    error
	marked atomic.Int32
}

func (s *awayStore) NodeKey(string) (string, bool) { return "", s.member }

func (s *awayStore) Away(string, time.Time) error {
	s.marked.Add(1)
	if s.block != nil {
		<-s.block
	}
	return s.err
}

func TestAStoppingAgentMarksItsNodeAwayWithoutWaitingLong(t *testing.T) {
	var out bytes.Buffer
	log := slog.New(slog.NewTextHandler(&out, nil))
	until := time.Now().Add(awayFor)

	// A member is marked.
	member := &awayStore{member: true}
	markAway(member, "n1", until, time.Second, log)
	if member.marked.Load() != 1 {
		t.Fatal("a member was not marked away")
	}

	// A node removed from the cluster is not: no leader would take it.
	removed := &awayStore{block: make(chan struct{})}
	defer close(removed.block)
	start := time.Now()
	markAway(removed, "n3", until, time.Minute, log)
	if removed.marked.Load() != 0 || time.Since(start) > 5*time.Second {
		t.Fatalf("a node no longer a member was marked away (%d), after %v", removed.marked.Load(), time.Since(start))
	}

	// A member cut off from the leader stops all the same, soon.
	cutOff := &awayStore{member: true, block: make(chan struct{})}
	defer close(cutOff.block)
	out.Reset()
	start = time.Now()
	markAway(cutOff, "n2", until, 200*time.Millisecond, log)
	if took := time.Since(start); took < 150*time.Millisecond || took > 5*time.Second {
		t.Fatalf("marking a cut-off node away took %v", took)
	}
	if !strings.Contains(out.String(), "level=WARN") || !strings.Contains(out.String(), "could not be marked away in time") {
		t.Fatalf("logged %q", out.String())
	}

	// A freeze is no fault.
	frozen := &awayStore{member: true, err: errors.Join(store.ErrFrozen)}
	out.Reset()
	markAway(frozen, "n1", until, time.Second, log)
	if strings.Contains(out.String(), "level=WARN") {
		t.Fatalf("a freeze logged %q", out.String())
	}
}

// The agent says which restart runs the edge under the units it replaced,
// and that, with the socket holding the ports, it refuses no connection.
func TestEdgeRestartAdvice(t *testing.T) {
	service, socket := setup.Target("murder-edge.service"), setup.Target("murder-edge.socket")
	for _, c := range []struct {
		replaced []string
		active   bool
		want     string
	}{
		{nil, true, ""},
		{[]string{setup.Target("murder-agent.service")}, true, ""},
		{[]string{service}, true, "no longer drops connections"},
		{[]string{setup.EdgePathsDropIn}, true, "no longer drops connections"},
		// The release that brings the socket: one restart starts it.
		{[]string{service, socket}, false, "systemctl stop murder-edge.service && systemctl start murder-edge.socket"},
		{[]string{setup.EdgeSocketDropIn}, true, "systemctl stop murder-edge.socket murder-edge.service; systemctl start murder-edge"},
		{[]string{service, setup.EdgeSocketDropIn}, true, "the ports close for that moment"},
	} {
		got := edgeRestartAdvice(c.replaced, c.active)
		if c.want == "" && got != "" || !strings.Contains(got, c.want) {
			t.Errorf("replaced %v, socket active %v: %q, want %q", c.replaced, c.active, got, c.want)
		}
	}
}
