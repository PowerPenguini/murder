// Package hostprobe finds out what a node's settings would otherwise have
// to name, from the host: its addresses, its ZFS pools and the ports sshd
// listens on. Setup asks it before it writes a config, and the config
// asks it at every start for what the config leaves out, so the two find
// the same.
package hostprobe

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"net"
	"net/netip"
	"os"
	"os/exec"
	"path/filepath"
	"slices"
	"strconv"
	"strings"
	"time"

	"github.com/PowerPenguini/murder/internal/firewall"
)

// Runner runs a command and returns its standard output.
type Runner interface {
	Run(ctx context.Context, name string, args ...string) (string, error)
}

// Exec runs commands on this host.
type Exec struct{}

func (Exec) Run(ctx context.Context, name string, args ...string) (string, error) {
	var stdout, stderr bytes.Buffer
	command := exec.CommandContext(ctx, name, args...)
	command.Stdout, command.Stderr = &stdout, &stderr
	if err := command.Run(); err != nil {
		if message := strings.TrimSpace(stderr.String()); message != "" {
			return "", fmt.Errorf("%s %s: %w: %s", name, strings.Join(args, " "), err, message)
		}
		return "", fmt.Errorf("%s %s: %w", name, strings.Join(args, " "), err)
	}
	return stdout.String(), nil
}

// System answers from this host.
type System struct {
	// StateDir holds the records; /var/lib/murder if empty, for tests.
	StateDir string
}

// record is where System keeps the record at file (one of the *Record
// paths).
func (s System) record(file string) string {
	if s.StateDir == "" {
		return file
	}
	return filepath.Join(s.StateDir, filepath.Base(file))
}

func (System) Hostname() (string, error)        { return os.Hostname() }
func (System) Addresses() ([]netip.Addr, error) { return Addresses() }

// Pools are the host's ZFS pools; an error without ZFS.
func (System) Pools() ([]string, error) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	return Pools(ctx, Exec{})
}

func (System) SSHPorts() ([]int, error) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	return SSHPorts(ctx, Exec{})
}

// Records keep what the last complete detection found, so a start whose
// detection fails or finds too little, as early at boot before ss or an
// interface is there, takes nothing away: closing SSH's port locks the
// operator out, and dropping a node's public address from the cluster's
// DNS records is an outage. They are root's alone, beside the database.
const (
	SSHPortsRecord        = "/var/lib/murder/ssh-ports"
	PublicAddressesRecord = "/var/lib/murder/public-addresses"
)

// ErrNothingRecorded is a record's error when there is none to go by: it
// is missing, unreadable, corrupt or empty.
var ErrNothingRecorded = errors.New("nothing recorded")

// RecordedSSHPorts are the ports SSHPortsRecord keeps.
func (s System) RecordedSSHPorts() ([]int, error) {
	data, err := readRecord(s.record(SSHPortsRecord))
	if err != nil {
		return nil, err
	}
	return ParsePortsRecord(data)
}

// RecordSSHPorts keeps ports in SSHPortsRecord. It fails without root,
// as for the CLI run by a member of the socket's group: the agent and the
// firewall, which run as root, keep it.
func (s System) RecordSSHPorts(ports []int) error {
	return writeRecord(s.record(SSHPortsRecord), FormatPorts(ports))
}

// RecordedPublicAddresses are the addresses PublicAddressesRecord keeps.
func (s System) RecordedPublicAddresses() ([]string, error) {
	data, err := readRecord(s.record(PublicAddressesRecord))
	if err != nil {
		return nil, err
	}
	return ParseAddressesRecord(data)
}

// RecordPublicAddresses keeps addresses in PublicAddressesRecord.
func (s System) RecordPublicAddresses(addresses []string) error {
	return writeRecord(s.record(PublicAddressesRecord), strings.Join(addresses, " ")+"\n")
}

// OpenTCPPorts are the TCP ports the loaded host firewall opens to every
// address now; it needs root.
func (System) OpenTCPPorts() ([]int, error) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	listing, err := Exec{}.Run(ctx, "nft", "list", "table", "inet", "murder")
	if err != nil {
		return nil, err
	}
	return firewall.OpenTCP(listing), nil
}

func readRecord(path string) (string, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return "", fmt.Errorf("%w: %v", ErrNothingRecorded, err)
	}
	return string(data), nil
}

// writeRecord replaces path with text, readable by root alone.
func writeRecord(path, text string) error {
	if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
		return err
	}
	temp, err := os.CreateTemp(filepath.Dir(path), "."+filepath.Base(path)+".*")
	if err != nil {
		return err
	}
	defer os.Remove(temp.Name())
	// CreateTemp makes it 0600.
	if _, err := temp.WriteString(text); err != nil {
		temp.Close()
		return err
	}
	if err := temp.Close(); err != nil {
		return err
	}
	return os.Rename(temp.Name(), path)
}

// FormatPorts is ports as SSHPortsRecord keeps them: "22 2222\n".
func FormatPorts(ports []int) string {
	var texts []string
	for _, port := range ports {
		texts = append(texts, strconv.Itoa(port))
	}
	return strings.Join(texts, " ") + "\n"
}

// ParsePortsRecord reads FormatPorts' text strictly: one line of ports
// and nothing else. Anything else, or no port, is ErrNothingRecorded: a
// record half read must not pass for what was found.
func ParsePortsRecord(text string) ([]int, error) {
	line, ok := strings.CutSuffix(text, "\n")
	if !ok || strings.Contains(line, "\n") || strings.TrimSpace(line) == "" {
		return nil, fmt.Errorf("%w: %q is not one line of ports", ErrNothingRecorded, text)
	}
	var ports []int
	for _, field := range strings.Split(line, " ") {
		port, err := strconv.Atoi(field)
		if err != nil || port < 1 || port > 65535 || strconv.Itoa(port) != field {
			return nil, fmt.Errorf("%w: %q is not a port", ErrNothingRecorded, field)
		}
		ports = append(ports, port)
	}
	slices.Sort(ports)
	return slices.Compact(ports), nil
}

// ParseAddressesRecord reads a record of public addresses as strictly:
// one line of IPv4 addresses that are public.
func ParseAddressesRecord(text string) ([]string, error) {
	line, ok := strings.CutSuffix(text, "\n")
	if !ok || strings.Contains(line, "\n") || strings.TrimSpace(line) == "" {
		return nil, fmt.Errorf("%w: %q is not one line of addresses", ErrNothingRecorded, text)
	}
	var addresses []string
	for _, field := range strings.Split(line, " ") {
		address, err := netip.ParseAddr(field)
		if err != nil || address.String() != field {
			return nil, fmt.Errorf("%w: %q is not an address", ErrNothingRecorded, field)
		}
		if _, public := Classify([]netip.Addr{address}); len(public) != 1 {
			return nil, fmt.Errorf("%w: %s is not a public IPv4 address", ErrNothingRecorded, field)
		}
		addresses = append(addresses, field)
	}
	return addresses, nil
}

// Addresses are those of the host's own interfaces that are up: not
// Docker's bridges and their containers' ends, nor tunnels.
func Addresses() ([]netip.Addr, error) {
	interfaces, err := net.Interfaces()
	if err != nil {
		return nil, err
	}
	var addresses []netip.Addr
	for _, link := range interfaces {
		if link.Flags&net.FlagUp == 0 || link.Flags&net.FlagLoopback != 0 || Virtual(link.Name) {
			continue
		}
		own, err := link.Addrs()
		if err != nil {
			continue
		}
		for _, address := range own {
			if prefix, err := netip.ParsePrefix(address.String()); err == nil {
				addresses = append(addresses, prefix.Addr().Unmap())
			}
		}
	}
	return addresses, nil
}

// Virtual reports interfaces Docker, libvirt and VPNs make.
func Virtual(name string) bool {
	for _, prefix := range []string{"docker", "br-", "veth", "virbr", "vnet", "wg", "tun", "tap", "tailscale", "zt", "cni", "flannel", "cali", "vxlan"} {
		if strings.HasPrefix(name, prefix) {
			return true
		}
	}
	return false
}

// CGNAT is shared address space (RFC 6598), private to a provider's
// network: Tailscale and some WireGuard setups use it too.
var CGNAT = netip.MustParsePrefix("100.64.0.0/10")

// Classify parts a host's addresses into private and public ones, IPv4
// only: the cluster's address is private, and tenants' DNS records are A
// records. Loopback and link-local addresses are neither.
func Classify(addresses []netip.Addr) (private, public []netip.Addr) {
	for _, address := range addresses {
		switch {
		case !address.Is4() || address.IsLoopback() || address.IsLinkLocalUnicast():
		case address.IsPrivate() || CGNAT.Contains(address):
			private = append(private, address)
		case address.IsGlobalUnicast():
			public = append(public, address)
		}
	}
	return private, public
}

// ErrNoZFS is Pools' error on a host without ZFS.
var ErrNoZFS = errors.New("ZFS is missing")

// Pools are the host's ZFS pools.
func Pools(ctx context.Context, runner Runner) ([]string, error) {
	out, err := runner.Run(ctx, "zpool", "list", "-H", "-o", "name")
	if err != nil {
		return nil, fmt.Errorf("%w: %v", ErrNoZFS, err)
	}
	return strings.Fields(out), nil
}

// SSHPorts are the ports sshd listens on: its Port and ListenAddress
// settings, the sockets sshd holds, and those of systemd's ssh.socket for
// a socket-activated ssh. A port is taken whatever the address: one the
// firewall opens needlessly costs less than SSH locked out. The error says
// what could not be asked (sshd -T, or ss for the sockets): the ports are
// then only some of them, perhaps none, and the caller keeps open those it
// knew before.
func SSHPorts(ctx context.Context, runner Runner) ([]int, error) {
	var failed []error
	var ports []int
	addPort := func(text string) {
		if port, err := strconv.Atoi(text); err == nil && port > 0 && port < 65536 {
			ports = append(ports, port)
		}
	}
	if settings, err := SSHDSettings(ctx, runner); err != nil {
		failed = append(failed, err)
	} else {
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
	// A socket-activated ssh's ports are its socket unit's, whether or not
	// an sshd holds one yet: before the first connection none does.
	for _, unit := range []string{"ssh.socket", "sshd.socket"} {
		listen, err := socketListen(ctx, runner, unit)
		if err != nil {
			failed = append(failed, err)
			continue
		}
		for _, address := range listen {
			if i := strings.LastIndex(address, ":"); i >= 0 {
				addPort(address[i+1:])
			} else {
				addPort(address)
			}
		}
	}
	sockets, err := ListSockets(ctx, runner)
	if err != nil {
		failed = append(failed, err)
	}
	for _, s := range sockets {
		if s.Protocol == "tcp" && (s.Process == "sshd" || s.Process == "sshd-session") {
			ports = append(ports, s.Port)
		}
	}
	slices.Sort(ports)
	return slices.Compact(ports), errors.Join(failed...)
}

// socketListen is what a socket unit listens on: "[::]:22". A unit that
// does not exist has none, which is no failure; systemctl failing, or
// answering without the unit's state, is one: the unit may hold a port.
func socketListen(ctx context.Context, runner Runner, unit string) ([]string, error) {
	// "LoadState=loaded", then "Listen=[::]:22 (Stream)", one line each.
	output, err := runner.Run(ctx, "systemctl", "show", "-p", "LoadState", "-p", "Listen", unit)
	if err != nil {
		return nil, fmt.Errorf("ask systemd about %s: %w", unit, err)
	}
	var state string
	var listen []string
	for line := range strings.Lines(output) {
		key, value, _ := strings.Cut(strings.TrimSpace(line), "=")
		switch key {
		case "LoadState":
			state = value
		case "Listen":
			if address, _, _ := strings.Cut(value, " "); address != "" {
				listen = append(listen, address)
			}
		}
	}
	switch state {
	case "not-found", "masked":
		// No such unit, or one that cannot start: it holds no port.
		return nil, nil
	case "loaded":
		return listen, nil
	case "":
		return nil, fmt.Errorf("systemctl show %s: no LoadState", unit)
	default:
		// masked, error, bad-setting: what it would listen on is unknown.
		return nil, fmt.Errorf("systemctl show %s: LoadState=%s", unit, state)
	}
}

// SSHDSettings is sshd -T, by lowercase name, with every value of a
// setting it repeats, such as port.
func SSHDSettings(ctx context.Context, runner Runner) (map[string][]string, error) {
	output, err := runner.Run(ctx, "sshd", "-T")
	if err != nil {
		if output, err = runner.Run(ctx, "/usr/sbin/sshd", "-T"); err != nil {
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

// Socket is a listening TCP or a bound UDP socket.
type Socket struct {
	Protocol string
	Address  netip.Addr // invalid for any address ("*")
	Port     int
	Process  string
}

// Sockets are the host's listening TCP and bound UDP sockets, from ss;
// none if ss fails.
func Sockets(ctx context.Context, runner Runner) []Socket {
	sockets, _ := ListSockets(ctx, runner)
	return sockets
}

// ListSockets is Sockets, saying why ss failed.
func ListSockets(ctx context.Context, runner Runner) ([]Socket, error) {
	output, err := runner.Run(ctx, "ss", "-H", "-ltunp")
	if err != nil {
		return nil, err
	}
	var out []Socket
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
		s := Socket{Protocol: fields[0], Port: port}
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
	return out, nil
}
