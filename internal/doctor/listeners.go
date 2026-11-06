package doctor

import (
	"bufio"
	"encoding/hex"
	"fmt"
	"net/netip"
	"os"
	"path/filepath"
	"slices"
	"strconv"
	"strings"

	"github.com/PowerPenguini/murder/internal/config"
)

// socket is one listening socket from /proc/net.
type socket struct {
	protocol string // tcp or udp
	address  netip.Addr
	port     int
	inode    string
}

// listening reads the listening TCP and bound UDP sockets the kernel
// lists, IPv4 and IPv6.
func listening(h Host) []socket {
	var sockets []socket
	for _, file := range []struct{ name, protocol, state string }{
		{"tcp", "tcp", "0A"}, {"tcp6", "tcp", "0A"}, // LISTEN
		{"udp", "udp", "07"}, {"udp6", "udp", "07"}, // bound, unconnected
	} {
		data, err := os.ReadFile(h.path("/proc/net/" + file.name))
		if err != nil {
			continue
		}
		scanner := bufio.NewScanner(strings.NewReader(string(data)))
		scanner.Scan() // the header
		for scanner.Scan() {
			fields := strings.Fields(scanner.Text())
			if len(fields) < 10 || fields[3] != file.state {
				continue
			}
			address, port, ok := parseProcAddress(fields[1])
			if ok {
				sockets = append(sockets, socket{file.protocol, address, port, fields[9]})
			}
		}
	}
	return sockets
}

// parseProcAddress reads /proc/net's "0100007F:0016": the address in the
// kernel's byte order, 32-bit word by word, and the port in hex.
func parseProcAddress(text string) (netip.Addr, int, bool) {
	hexAddress, hexPort, ok := strings.Cut(text, ":")
	raw, err := hex.DecodeString(hexAddress)
	port, portErr := strconv.ParseUint(hexPort, 16, 16)
	if !ok || err != nil || portErr != nil || (len(raw) != 4 && len(raw) != 16) {
		return netip.Addr{}, 0, false
	}
	for word := 0; word < len(raw); word += 4 {
		slices.Reverse(raw[word : word+4])
	}
	address, _ := netip.AddrFromSlice(raw)
	return address.Unmap(), int(port), true
}

// public reports whether a socket on address can be reached from the
// internet: any address, or a public one.
func public(address netip.Addr) bool {
	if address.IsUnspecified() {
		return true
	}
	return !address.IsLoopback() && !address.IsPrivate() && !address.IsLinkLocalUnicast() && !sharedAddressSpace.Contains(address)
}

var sharedAddressSpace = netip.MustParsePrefix("100.64.0.0/10")

// owners maps socket inodes to the names of the processes holding them,
// as far as /proc lets this user see.
func owners(h Host) map[string]string {
	names := map[string]string{}
	processes, _ := filepath.Glob(h.path("/proc/[0-9]*"))
	for _, process := range processes {
		links, _ := filepath.Glob(filepath.Join(process, "fd", "*"))
		var comm string
		for _, link := range links {
			target, err := os.Readlink(link)
			inode, ok := strings.CutPrefix(target, "socket:[")
			if err != nil || !ok {
				continue
			}
			if comm == "" {
				data, _ := os.ReadFile(filepath.Join(process, "comm"))
				comm = strings.TrimSpace(string(data))
			}
			names[strings.TrimSuffix(inode, "]")] = comm
		}
	}
	return names
}

// dockerAPIPorts are where the Docker API listens over TCP: whoever
// reaches it is root on the host.
var dockerAPIPorts = []int{2375, 2376}

// checkListeners names what listens on public addresses. hostRules says
// whether the host firewall drops what reaches them.
func checkListeners(cfg config.Agent, h Host, sshPorts []int, hostRules bool) []Result {
	expected := map[string][]int{"tcp": sshPorts, "udp": {68, 546}} // DHCP clients
	if len(sshPorts) == 0 {
		expected["tcp"] = []int{22}
	}
	if ports, err := cfg.EdgePorts(); err == nil {
		expected["tcp"] = append(expected["tcp"], ports...)
	}
	if t := cfg.Panel.Tunnel; t != nil {
		expected["udp"] = append(expected["udp"], t.Port)
	}
	if rules, err := cfg.HostRules(); err == nil && rules != nil {
		expected["tcp"] = append(expected["tcp"], rules.PublicTCP...)
		expected["udp"] = append(expected["udp"], rules.PublicUDP...)
	}
	names := owners(h)
	sockets := listening(h)
	var results []Result
	// sshd -T names the Port settings only (checkSSH reports those): a
	// ListenAddress with a port of its own shows here, on any address, as
	// the firewall drops it from private networks too.
	if f := cfg.HostFirewall; f != nil {
		var closed []string
		for _, s := range sockets {
			if s.protocol == "tcp" && names[s.inode] == "sshd" && !s.address.IsLoopback() && !slices.Contains(f.SSH, s.port) && !slices.Contains(sshPorts, s.port) && !slices.Contains(closed, strconv.Itoa(s.port)) {
				closed = append(closed, strconv.Itoa(s.port))
			}
		}
		if len(closed) > 0 {
			results = append(results, Result{Failure, "listeners", "sshd listens on port " + strings.Join(closed, ", ") + ", which hostFirewall.ssh does not open: the next SSH connection is refused", "list every SSH port in hostFirewall.ssh"})
		}
	}
	seen := map[string]bool{}
	var unexpected, dropped []string
	for _, s := range sockets {
		key := fmt.Sprintf("%s/%d", s.protocol, s.port)
		if !public(s.address) || slices.Contains(expected[s.protocol], s.port) || seen[key] {
			continue
		}
		seen[key] = true
		label := key
		if name := names[s.inode]; name != "" {
			label += " (" + name + ")"
		}
		switch {
		case s.protocol == "tcp" && slices.Contains(dockerAPIPorts, s.port):
			results = append(results, Result{Failure, "listeners", "the Docker API listens on " + label + ": whoever reaches it is root here", "remove the tcp:// host from Docker's daemon.json or its unit, then restart Docker"})
		case names[s.inode] == "docker-proxy":
			// Docker sends a published port's traffic on to the container
			// before the host's input rules see it.
			results = append(results, Result{Warning, "listeners", "a container publishes " + label + " to the internet, past any host firewall", "publish it on 127.0.0.1 or a private address instead"})
		case cfg.HostFirewall != nil && names[s.inode] == "sshd":
			// Reported above if the firewall drops it.
		case hostRules:
			dropped = append(dropped, label)
		case s.port == 5355 || s.port == 5353:
			// systemd-resolved answers LLMNR (5355) and mDNS (5353) on
			// every interface by default: names for a LAN, not a server.
			results = append(results, Result{Warning, "listeners", label + " answers name lookups from anyone (LLMNR or mDNS)", "for systemd-resolved, LLMNR=no and MulticastDNS=no in /etc/systemd/resolved.conf, then systemctl restart systemd-resolved; otherwise stop the service that answers"})
		default:
			unexpected = append(unexpected, label)
		}
	}
	slices.Sort(unexpected)
	slices.Sort(dropped)
	if len(unexpected) > 0 {
		results = append(results, Result{Warning, "listeners", "open to the internet besides SSH and Murder: " + strings.Join(unexpected, ", "), "bind them to 127.0.0.1 or a private address, or set hostFirewall"})
	}
	if len(dropped) > 0 {
		results = append(results, Result{OK, "listeners", "the host firewall drops what reaches " + strings.Join(dropped, ", ") + " from outside", ""})
	}
	if len(results) == 0 {
		results = append(results, Result{OK, "listeners", "only SSH and Murder listen on the public addresses", ""})
	}
	if !h.Privileged {
		results = append(results, Result{Skipped, "listeners", "cannot name the processes behind the sockets", "run as root"})
	}
	return results
}
