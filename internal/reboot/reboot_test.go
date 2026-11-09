package reboot

import (
	"context"
	"errors"
	"slices"
	"strings"
	"sync"
	"testing"
	"time"
)

// fakeSystem answers systemctl as a host whose units are in states.
type fakeSystem struct {
	mu sync.Mutex
	// states are what "systemctl <verb> <unit>" prints, keyed "verb unit";
	// a unit not listed is "disabled" or "inactive".
	states map[string]string
	// rebootFails makes systemctl reboot fail.
	rebootFails bool
	ran         []string
}

func (f *fakeSystem) Run(_ context.Context, name string, args ...string) ([]byte, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	command := strings.Join(append([]string{name}, args...), " ")
	f.ran = append(f.ran, command)
	if name != "systemctl" || len(args) == 0 {
		return nil, errors.New("unexpected command " + command)
	}
	if args[0] == "reboot" {
		if f.rebootFails {
			return []byte("Access denied"), errors.New("exit status 1")
		}
		return nil, nil
	}
	state, ok := f.states[args[0]+" "+args[1]]
	switch {
	case ok && (state == "enabled" || state == "active"):
		return []byte(state + "\n"), nil
	case ok:
		return []byte(state + "\n"), errors.New("exit status 1")
	case args[0] == "is-active":
		return []byte("inactive\n"), errors.New("exit status 3")
	}
	return []byte("disabled\n"), errors.New("exit status 1")
}

func (f *fakeSystem) rebooted() bool {
	f.mu.Lock()
	defer f.mu.Unlock()
	return slices.Contains(f.ran, "systemctl reboot")
}

func healthyHost() *fakeSystem {
	return &fakeSystem{states: map[string]string{
		"is-enabled murder-agent.service":    "enabled",
		"is-enabled murder-edge.service":     "enabled",
		"is-enabled murder-firewall.service": "enabled",
		"is-enabled docker.service":          "enabled",
	}}
}

// now runs what the node schedules at once.
func now(_ time.Duration, f func()) { f() }

func TestANodeThatComesBackPassesTheCheck(t *testing.T) {
	host := healthyHost()
	node := &Node{System: host, Firewall: true, after: now}
	if err := node.Check(context.Background()); err != nil {
		t.Fatal(err)
	}
	if host.rebooted() {
		t.Fatal("a check rebooted the host")
	}
}

func TestAUnitThatDoesNotStartAtBootIsRefused(t *testing.T) {
	for _, tc := range []struct {
		name  string
		set   func(*fakeSystem)
		wants string
	}{
		{"agent", func(h *fakeSystem) { delete(h.states, "is-enabled murder-agent.service") }, "murder-agent.service is disabled"},
		{"edge masked", func(h *fakeSystem) { h.states["is-enabled murder-edge.service"] = "masked" }, "murder-edge.service is masked"},
		{"firewall", func(h *fakeSystem) { delete(h.states, "is-enabled murder-firewall.service") }, "murder-firewall.service"},
		{"docker", func(h *fakeSystem) { delete(h.states, "is-enabled docker.service") }, "tenants' containers do not start"},
		{"panel that runs", func(h *fakeSystem) { h.states["is-active murder-panel.service"] = "active" }, "murder-panel.service is disabled"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			host := healthyHost()
			tc.set(host)
			node := &Node{System: host, Firewall: true, after: now}
			err := node.Reboot(context.Background(), Request{})
			var notReady *NotReadyError
			if !errors.As(err, &notReady) || !strings.Contains(err.Error(), tc.wants) {
				t.Fatalf("reboot = %v, want it refused for %q", err, tc.wants)
			}
			if host.rebooted() {
				t.Fatal("the host rebooted all the same")
			}
		})
	}
}

func TestWhatTheNodeDoesNotRunNeedNotStartAtBoot(t *testing.T) {
	host := healthyHost()
	// No firewall here, no panel running, Docker from its socket.
	delete(host.states, "is-enabled murder-firewall.service")
	delete(host.states, "is-enabled docker.service")
	host.states["is-enabled docker.socket"] = "enabled"
	node := &Node{System: host, after: now}
	if err := node.Check(context.Background()); err != nil {
		t.Fatal(err)
	}
}

func TestAFreezeHoldsTheReboot(t *testing.T) {
	host := healthyHost()
	node := &Node{System: host, Frozen: func() (string, error) { return "upgrade to abc", nil }, after: now}
	err := node.Reboot(context.Background(), Request{})
	if !errors.As(err, new(*NotReadyError)) || !strings.Contains(err.Error(), "upgrade to abc") {
		t.Fatalf("reboot = %v, want it refused for the freeze", err)
	}
	if host.rebooted() {
		t.Fatal("the host rebooted under a freeze")
	}
}

func TestARebootMarksTheNodeAwayThenReboots(t *testing.T) {
	host := healthyHost()
	var marked time.Time
	var scheduled time.Duration
	node := &Node{System: host, Delay: 500 * time.Millisecond, Away: func(until time.Time) error { marked = until; return nil },
		after: func(d time.Duration, f func()) { scheduled = d; f() }}
	start := time.Now()
	if err := node.Reboot(context.Background(), Request{Check: true, Away: time.Hour}); err != nil || host.rebooted() || !marked.IsZero() {
		t.Fatalf("a checking request: %v, rebooted %v, marked %v", err, host.rebooted(), marked)
	}
	if err := node.Reboot(context.Background(), Request{Away: 20 * time.Minute}); err != nil {
		t.Fatal(err)
	}
	if !host.rebooted() || scheduled != 500*time.Millisecond {
		t.Fatalf("rebooted %v after %v", host.rebooted(), scheduled)
	}
	if marked.Before(start.Add(20*time.Minute)) || !node.AwayUntil().Equal(marked) {
		t.Fatalf("marked away until %v (kept %v), want 20 minutes on", marked, node.AwayUntil())
	}
	// A second request while it reboots is refused.
	if err := node.Reboot(context.Background(), Request{}); !errors.Is(err, ErrRebooting) {
		t.Fatalf("second reboot = %v, want ErrRebooting", err)
	}
}

func TestANodeThatCannotBeMarkedAwayDoesNotReboot(t *testing.T) {
	host := healthyHost()
	node := &Node{System: host, Away: func(time.Time) error { return errors.New("no leader") }, after: now}
	if err := node.Reboot(context.Background(), Request{Away: time.Minute}); err == nil || !strings.Contains(err.Error(), "no leader") {
		t.Fatalf("reboot = %v, want the mark's failure", err)
	}
	if host.rebooted() {
		t.Fatal("rebooted without being marked away")
	}
}

func TestARebootThatFailsCanBeAskedAgain(t *testing.T) {
	host := healthyHost()
	host.rebootFails = true
	node := &Node{System: host, after: now}
	if err := node.Reboot(context.Background(), Request{}); err != nil {
		t.Fatal(err)
	}
	host.rebootFails = false
	if err := node.Reboot(context.Background(), Request{}); err != nil {
		t.Fatalf("after a failed reboot: %v", err)
	}
}
