package setup

import (
	"context"
	"fmt"
	"slices"
	"strings"

	"github.com/PowerPenguini/murder/internal/hostprobe"
)

// Listener is a port something besides Murder and sshd listens on, other
// than on loopback.
type Listener struct {
	Protocol string // tcp or udp
	Port     int
	Process  string
	// Private reports it listens on private addresses only, so the host
	// firewall opens it to private networks, not to everyone.
	Private bool
}

// Entry is the listener as hostFirewall lists it, such as "9100/tcp".
func (l Listener) Entry() string { return fmt.Sprintf("%d/%s", l.Port, l.Protocol) }

// listeners are what the host firewall would close: ports listened on
// beyond loopback by something other than Murder, sshd and Docker's
// published ports (which pass the host's input rules anyway), and other
// than the DHCP client's.
func listeners(ctx context.Context, host Host, ssh []int) []Listener {
	found := map[string]*Listener{}
	var order []string
	for _, s := range hostprobe.Sockets(ctx, host) {
		switch {
		case s.Address.IsLoopback() || s.Address.IsLinkLocalUnicast():
			continue
		case s.Process == "murder" || s.Process == "docker-proxy" || s.Process == "sshd" || s.Process == "sshd-session":
			continue
		case s.Protocol == "tcp" && slices.Contains(ssh, s.Port):
			continue // ssh started by systemd's socket
		case s.Protocol == "udp" && (s.Port == 68 || s.Port == 546):
			continue // DHCP's replies pass the firewall
		case s.Protocol == "udp" && (s.Port == 5353 || s.Port == 5355):
			continue // mDNS and LLMNR: names for a LAN, not a server's
		}
		private := s.Address.IsValid() && !s.Address.IsUnspecified() && (s.Address.IsPrivate() || hostprobe.CGNAT.Contains(s.Address))
		key := fmt.Sprintf("%d/%s", s.Port, s.Protocol)
		if l, ok := found[key]; ok {
			l.Private = l.Private && private
			continue
		}
		found[key] = &Listener{Protocol: s.Protocol, Port: s.Port, Process: s.Process, Private: private}
		order = append(order, key)
	}
	var out []Listener
	for _, key := range order {
		out = append(out, *found[key])
	}
	slices.SortFunc(out, func(a, b Listener) int {
		if a.Port != b.Port {
			return a.Port - b.Port
		}
		return strings.Compare(a.Protocol, b.Protocol)
	})
	return out
}
