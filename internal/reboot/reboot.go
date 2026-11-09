// Package reboot is a node's part in rebooting its host, for the updates
// that wait for one (a kernel, the C library): it checks first that what
// runs the node starts again at boot, then has systemd reboot the host.
// Where the node's tenants run meanwhile is for its caller to decide:
// clusterops moves them to another node first.
package reboot

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"os/exec"
	"strings"
	"sync"
	"time"
)

// Request asks a node to reboot, or with Check only whether it could.
type Request struct {
	Check bool `json:"check,omitempty"`
	// Away is how long, from now, the node is not failed over for not
	// answering: a server can take minutes to boot.
	Away time.Duration `json:"away,omitempty"`
}

// ErrRebooting refuses a reboot of a node that reboots already.
var ErrRebooting = errors.New("the node reboots already")

// NotReadyError says why a node would not come back from a reboot as it
// is: it reboots nothing.
type NotReadyError struct{ Problems []string }

func (e *NotReadyError) Error() string {
	return "the node would not come back as it runs now, so it does not reboot: " + strings.Join(e.Problems, "; ")
}

// System runs a command on the host; Exec does, a test fakes it.
type System interface {
	Run(ctx context.Context, name string, args ...string) ([]byte, error)
}

// Exec runs commands on the host.
type Exec struct{}

func (Exec) Run(ctx context.Context, name string, args ...string) ([]byte, error) {
	return exec.CommandContext(ctx, name, args...).CombinedOutput()
}

// Node reboots the host it runs on.
type Node struct {
	System System
	// Firewall says the tenant firewall, murder-firewall.service, must
	// start at boot too.
	Firewall bool
	// Frozen is why changes are frozen, caught up with the cluster: "" if
	// they are not. An upgrade or a key rotation is not rebooted through.
	Frozen func() (string, error)
	// Away marks the node away until until; nil outside a cluster.
	Away func(until time.Time) error
	// Delay is how long after the answer the host reboots: the answer goes
	// out first.
	Delay time.Duration
	Log   *slog.Logger

	// after runs f after d; time.AfterFunc unless a test sets it.
	after func(d time.Duration, f func())

	mu        sync.Mutex
	rebooting bool
	awayUntil time.Time
}

// Units start the node at boot; Docker runs the tenants.
const (
	agentUnit    = "murder-agent.service"
	edgeUnit     = "murder-edge.service"
	firewallUnit = "murder-firewall.service"
	panelUnit    = "murder-panel.service"
	dockerUnit   = "docker.service"
	dockerSocket = "docker.socket"
)

// Check reports whether the node would come back from a reboot: changes
// are not frozen, and systemd starts at boot the agent, the edge, the
// firewall if the node runs it, the panel if it runs now, and Docker.
func (n *Node) Check(ctx context.Context) error {
	var problems []string
	if n.Frozen != nil {
		frozen, err := n.Frozen()
		switch {
		case err != nil:
			return fmt.Errorf("read the freeze: %w", err)
		case frozen != "":
			problems = append(problems, fmt.Sprintf("changes are frozen (%s): reboot once that is over", frozen))
		}
	}
	units := []string{agentUnit, edgeUnit}
	if n.Firewall {
		units = append(units, firewallUnit)
	}
	// The panel runs on some nodes only: one that runs now must come back.
	if n.state(ctx, "is-active", panelUnit) == "active" {
		units = append(units, panelUnit)
	}
	for _, unit := range units {
		if state := n.state(ctx, "is-enabled", unit); state != "enabled" {
			problems = append(problems, fmt.Sprintf("%s is %s, so it does not start at boot: systemctl enable %s", unit, describe(state), unit))
		}
	}
	// Docker may start from its socket, as the agent first reaches it.
	if state := n.state(ctx, "is-enabled", dockerUnit); state != "enabled" && n.state(ctx, "is-enabled", dockerSocket) != "enabled" {
		problems = append(problems, fmt.Sprintf("%s is %s and %s is not enabled, so the tenants' containers do not start at boot: systemctl enable docker", dockerUnit, describe(state), dockerSocket))
	}
	if len(problems) > 0 {
		return &NotReadyError{Problems: problems}
	}
	return nil
}

// state is what systemctl says of unit, or why it could not.
func (n *Node) state(ctx context.Context, verb, unit string) string {
	output, err := n.System.Run(ctx, "systemctl", verb, unit)
	if state := strings.TrimSpace(string(output)); state != "" {
		// systemctl exits non-zero for a unit that is not enabled or not
		// active, and says which.
		line, _, _ := strings.Cut(state, "\n")
		return line
	}
	if err != nil {
		return "unknown (" + err.Error() + ")"
	}
	return "unknown"
}

func describe(state string) string {
	if state == "" {
		return "unknown"
	}
	return state
}

// Reboot reboots the host once Check passes, after Delay, marking the node
// away for request.Away first; with request.Check it only checks. It
// returns before the host reboots: whether it came back is for the caller
// to see, as the node answers again.
func (n *Node) Reboot(ctx context.Context, request Request) error {
	if err := n.Check(ctx); err != nil {
		return err
	}
	if request.Check {
		return nil
	}
	n.mu.Lock()
	if n.rebooting {
		n.mu.Unlock()
		return ErrRebooting
	}
	if n.Away != nil && request.Away > 0 {
		until := time.Now().Add(request.Away)
		if err := n.Away(until); err != nil {
			n.mu.Unlock()
			return fmt.Errorf("mark the node away while it reboots, so it is not failed over: %w", err)
		}
		n.awayUntil = until
	}
	n.rebooting = true
	n.mu.Unlock()
	after := n.after
	if after == nil {
		after = func(d time.Duration, f func()) { time.AfterFunc(d, f) }
	}
	after(n.Delay, func() {
		n.log().Info("rebooting the host")
		output, err := n.System.Run(context.Background(), "systemctl", "reboot")
		if err != nil {
			n.log().Error("the host does not reboot", "error", err, "output", strings.TrimSpace(string(output)))
			n.mu.Lock()
			n.rebooting = false
			n.mu.Unlock()
		}
	})
	return nil
}

// AwayUntil is until when a reboot marked the node away: zero if none
// did. The agent's own stop keeps the later of it and its own.
func (n *Node) AwayUntil() time.Time {
	n.mu.Lock()
	defer n.mu.Unlock()
	return n.awayUntil
}

func (n *Node) log() *slog.Logger {
	if n.Log == nil {
		return slog.New(slog.DiscardHandler)
	}
	return n.Log
}
