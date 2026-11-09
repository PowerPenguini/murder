package setup

import (
	"context"
	"fmt"
	"net/netip"
	"slices"
	"strconv"
	"strings"
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

// sshPorts are the ports sshd listens on: its Port and ListenAddress
// settings, the sockets sshd holds, and those of systemd's ssh.socket for
// a socket-activated ssh; 22 when none says. A port is taken whatever the
// address: one the firewall opens needlessly costs less than SSH locked
// out.
func sshPorts(ctx context.Context, host Host) []int {
	var ports []int
	addPort := func(text string) {
		if port, err := strconv.Atoi(text); err == nil && port > 0 && port < 65536 {
			ports = append(ports, port)
		}
	}
	if settings, err := sshdSettingsAll(ctx, host); err == nil {
		for _, value := range settings["port"] {
			addPort(value)
		}
		for _, value := range settings["listenaddress"] {
			// "192.0.2.10:22", "[::]:22", with a "rdomain" after it.
			address, _, _ := strings.Cut(value, " ")
			if i := strings.LastIndex(address, ":"); i >= 0 && !strings.HasSuffix(address, "]") {
				addPort(address[i+1:])
			}
		}
	}
	for _, unit := range []string{"ssh.socket", "sshd.socket"} {
		// "Listen=[::]:22 (Stream)", one line for each.
		output, err := host.Run(ctx, "systemctl", "show", "-p", "Listen", "--value", unit)
		if err != nil {
			continue
		}
		for _, field := range strings.Fields(output) {
			if i := strings.LastIndex(field, ":"); i >= 0 {
				addPort(field[i+1:])
			} else {
				addPort(field)
			}
		}
	}
	for _, s := range sockets(ctx, host) {
		if s.Protocol == "tcp" && (s.Process == "sshd" || s.Process == "sshd-session") {
			ports = append(ports, s.Port)
		}
	}
	if len(ports) == 0 {
		ports = []int{22}
	}
	slices.Sort(ports)
	return slices.Compact(ports)
}

// sshdSettingsAll is sshd -T with every value of a setting it repeats,
// such as port.
func sshdSettingsAll(ctx context.Context, host Host) (map[string][]string, error) {
	output, err := host.Run(ctx, "sshd", "-T")
	if err != nil {
		if output, err = host.Run(ctx, "/usr/sbin/sshd", "-T"); err != nil {
			return nil, err
		}
	}
	settings := map[string][]string{}
	for line := range strings.Lines(output) {
		key, value, _ := strings.Cut(strings.TrimSpace(line), " ")
		settings[key] = append(settings[key], value)
	}
	return settings, nil
}

// listeners are what the host firewall would close: ports listened on
// beyond loopback by something other than Murder, sshd and Docker's
// published ports (which pass the host's input rules anyway), and other
// than the DHCP client's.
func listeners(ctx context.Context, host Host, ssh []int) []Listener {
	found := map[string]*Listener{}
	var order []string
	for _, s := range sockets(ctx, host) {
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
		private := s.Address.IsValid() && !s.Address.IsUnspecified() && (s.Address.IsPrivate() || cgnat.Contains(s.Address))
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

type socket struct {
	Protocol string
	Address  netip.Addr // invalid for any address ("*")
	Port     int
	Process  string
}

// sockets are the host's listening TCP and bound UDP sockets, from ss.
func sockets(ctx context.Context, host Host) []socket {
	output, err := host.Run(ctx, "ss", "-H", "-ltunp")
	if err != nil {
		return nil
	}
	var out []socket
	for line := range strings.Lines(output) {
		fields := strings.Fields(line)
		if len(fields) < 5 || (fields[0] != "tcp" && fields[0] != "udp") {
			continue
		}
		local := fields[4]
		i := strings.LastIndex(local, ":")
		if i < 0 {
			continue
		}
		port, err := strconv.Atoi(local[i+1:])
		if err != nil {
			continue
		}
		s := socket{Protocol: fields[0], Port: port}
		address := strings.Trim(local[:i], "[]")
		address, _, _ = strings.Cut(address, "%")
		if address != "*" {
			if s.Address, err = netip.ParseAddr(address); err != nil {
				continue
			}
			s.Address = s.Address.Unmap()
		}
		if _, rest, ok := strings.Cut(line, `(("`); ok {
			s.Process, _, _ = strings.Cut(rest, `"`)
		}
		out = append(out, s)
	}
	return out
}
