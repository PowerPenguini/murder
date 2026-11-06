package doctor

import (
	"context"
	"errors"
	"net/netip"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/PowerPenguini/murder/internal/config"
	"github.com/PowerPenguini/murder/internal/runtime"
)

// fakeHost is a machine under dir, answering commands from outputs (a
// command missing from it is not installed).
func fakeHost(t *testing.T, outputs map[string]string) Host {
	t.Helper()
	return Host{
		Root:       t.TempDir(),
		Owner:      os.Getuid(),
		Privileged: true,
		Run: func(_ context.Context, name string, args ...string) ([]byte, error) {
			output, ok := outputs[strings.Join(append([]string{name}, args...), " ")]
			if !ok {
				return nil, exec.ErrNotFound
			}
			return []byte(output), nil
		},
		Engine: func(context.Context) (runtime.EngineInfo, error) {
			return runtime.EngineInfo{LiveRestoreEnabled: true, SecurityOptions: []string{"name=seccomp,profile=builtin"}}, nil
		},
		Now: func() time.Time { return time.Date(2026, 11, 5, 12, 0, 0, 0, time.UTC) },
	}
}

func write(t *testing.T, h Host, name, content string, mode os.FileMode) {
	t.Helper()
	path := h.path(name)
	if err := os.MkdirAll(filepath.Dir(path), 0o700); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(path, []byte(content), mode); err != nil {
		t.Fatal(err)
	}
	if err := os.Chmod(path, mode); err != nil {
		t.Fatal(err)
	}
}

func parse(t *testing.T, text string) config.Agent {
	t.Helper()
	cfg, err := config.Parse([]byte("baseDomain: example.com\n" + text))
	if err != nil {
		t.Fatal(err)
	}
	return cfg
}

// find returns the results of area, failing the test if there are none.
func find(t *testing.T, results []Result, area string) []Result {
	t.Helper()
	var found []Result
	for _, result := range results {
		if result.Area == area {
			found = append(found, result)
		}
	}
	if len(found) == 0 {
		t.Fatalf("no %s results in %+v", area, results)
	}
	return found
}

func has(results []Result, status Status, text string) bool {
	for _, result := range results {
		if result.Status == status && strings.Contains(result.Message, text) {
			return true
		}
	}
	return false
}

func TestParseProcAddress(t *testing.T) {
	for text, want := range map[string]netip.AddrPort{
		"0100007F:0016":                         netip.MustParseAddrPort("127.0.0.1:22"),
		"00000000:01BB":                         netip.MustParseAddrPort("0.0.0.0:443"),
		"0200000A:1CE8":                         netip.MustParseAddrPort("10.0.0.2:7400"),
		"00000000000000000000000000000000:0050": netip.MustParseAddrPort("[::]:80"),
		"00000000000000000000000001000000:0035": netip.MustParseAddrPort("[::1]:53"),
		"0000000000000000FFFF00000100007F:0016": netip.MustParseAddrPort("127.0.0.1:22"),
		"B80D0120000000000000000001000000:01BB": netip.MustParseAddrPort("[2001:db8::1]:443"),
	} {
		address, port, ok := parseProcAddress(text)
		if !ok || netip.AddrPortFrom(address, uint16(port)) != want {
			t.Errorf("%s = %v:%d, %v; want %v", text, address, port, ok, want)
		}
	}
	for _, bad := range []string{"", "0100007F", "0100007F:XYZ", "01007F:0016"} {
		if _, _, ok := parseProcAddress(bad); ok {
			t.Errorf("%q parsed", bad)
		}
	}
}

const tcpHeader = "  sl  local_address rem_address   st tx_queue rx_queue tr tm->when retrnsmt   uid  timeout inode\n"

func procLine(local, state, inode string) string {
	return "   0: " + local + " 00000000:0000 " + state + " 00000000:00000000 00:00000000 00000000     0        0 " + inode + " 1 0000000000000000 100 0 0 10 0\n"
}

// Listeners are judged by address and port: loopback and private ones,
// SSH and the edge pass; others are named by their process, and the Docker
// API and published ports are called out even behind the host firewall.
func TestListeners(t *testing.T) {
	h := fakeHost(t, nil)
	write(t, h, "/proc/net/tcp", tcpHeader+
		procLine("00000000:0016", "0A", "11")+ // ssh, any address
		procLine("00000000:01BB", "0A", "12")+ // the edge
		procLine("0100007F:2328", "0A", "13")+ // metrics on loopback
		procLine("0200000A:1CE8", "0A", "14")+ // the cluster, private
		procLine("00000000:238C", "0A", "15")+ // 9100, any address
		procLine("00000000:0947", "0A", "16")+ // 2375, the Docker API
		procLine("00000000:1F90", "0A", "17")+ // 8080, docker-proxy
		procLine("00000000:1F91", "01", "18")+ // an established connection, not a listener
		procLine("00000000:08AE", "0A", "20")+ // 2222, sshd
		procLine("00000000:14EB", "0A", "21"), // 5355, LLMNR
		0o644)
	write(t, h, "/proc/net/udp", tcpHeader+procLine("00000000:0044", "07", "19"), 0o644) // DHCP
	write(t, h, "/proc/321/comm", "node_exporter\n", 0o644)
	write(t, h, "/proc/322/comm", "docker-proxy\n", 0o644)
	write(t, h, "/proc/323/comm", "sshd\n", 0o644)
	for pid, inode := range map[string]string{"321": "15", "322": "17", "323": "20"} {
		if err := os.MkdirAll(h.path("/proc/"+pid+"/fd"), 0o755); err != nil {
			t.Fatal(err)
		}
		if err := os.Symlink("socket:["+inode+"]", h.path("/proc/"+pid+"/fd/3")); err != nil {
			t.Fatal(err)
		}
	}
	cfg := parse(t, "edge: {tls: {mode: internal}}\n")
	results := checkListeners(cfg, h, []int{22, 2222})
	if !has(results, Failure, "Docker API listens on tcp/2375") ||
		!has(results, Warning, "tcp/5355 answers name lookups") ||
		!has(results, Warning, "publishes tcp/8080 (docker-proxy)") ||
		!has(results, Warning, "besides SSH and Murder: tcp/9100 (node_exporter)") {
		t.Errorf("results = %+v", results)
	}
	if has(results, Failure, "sshd") {
		t.Errorf("sshd reported without the host firewall: %+v", results)
	}
	for _, result := range results {
		for _, quiet := range []string{"/22,", "/22 ", "/443", "/9000", "/7400", "/68", "8081"} {
			if strings.Contains(result.Message, quiet) {
				t.Errorf("%s reported: %+v", quiet, result)
			}
		}
	}
	// Behind the host firewall, 9100 is dropped from outside; the
	// published port is not. An sshd on a port hostFirewall.ssh leaves
	// out (a ListenAddress sshd -T does not report as a port) fails.
	results = checkListeners(parse(t, "edge: {tls: {mode: internal}}\nhostFirewall: {}\n"), h, []int{22})
	if !has(results, OK, "tcp/9100 (node_exporter)") || !has(results, Warning, "tcp/8080") || !has(results, Failure, "sshd listens on port 2222") {
		t.Errorf("with the host firewall: %+v", results)
	}
	// A port sshd -T named is checkSSH's to report, not reported twice.
	results = checkListeners(parse(t, "edge: {tls: {mode: internal}}\nhostFirewall: {}\n"), h, []int{22, 2222})
	if has(results, Failure, "sshd listens") {
		t.Errorf("sshd's port reported twice: %+v", results)
	}
}

func TestSSH(t *testing.T) {
	keysOnly := "port 22\npasswordauthentication no\nkbdinteractiveauthentication no\npermitrootlogin prohibit-password\nusepam yes\n"
	h := fakeHost(t, map[string]string{"sshd -T": keysOnly})
	results, ports := checkSSH(context.Background(), parse(t, ""), h)
	if !has(results, OK, "keys only") || len(ports) != 1 || ports[0] != 22 {
		t.Errorf("keys only: %+v, %v", results, ports)
	}
	h = fakeHost(t, map[string]string{"sshd -T": "port 22\nport 2222\npasswordauthentication yes\npermitrootlogin yes\n"})
	results, _ = checkSSH(context.Background(), parse(t, "hostFirewall: {}\n"), h)
	if !has(results, Failure, "takes passwords") || !has(results, Failure, "root may log in") || !has(results, Failure, "port 2222, which hostFirewall.ssh does not open") {
		t.Errorf("open: %+v", results)
	}
	// Without sshd, or without root, it is skipped.
	results, ports = checkSSH(context.Background(), parse(t, ""), fakeHost(t, nil))
	if !has(results, Skipped, "not installed") || ports != nil {
		t.Errorf("no sshd: %+v", results)
	}
	h = fakeHost(t, nil)
	h.Privileged = false
	h.Run = func(context.Context, string, ...string) ([]byte, error) {
		return []byte("no hostkeys"), errors.New("exit status 1")
	}
	if results, _ := checkSSH(context.Background(), parse(t, ""), h); !has(results, Skipped, "cannot read") {
		t.Errorf("not root: %+v", results)
	}
}

func TestFiles(t *testing.T) {
	h := fakeHost(t, nil)
	cfg := parse(t, "edge: {tls: {mode: acme, email: a@example.com, dnsProvider: hetzner, dnsTokenFile: /etc/murder/dns.token}}\n")
	write(t, h, "/etc/murder/master.key", "key", 0o600)
	write(t, h, "/etc/murder/dns.token", "token", 0o644)
	write(t, h, "/etc/murder/agent.yaml", "", 0o664)
	if err := os.MkdirAll(h.path("/var/lib/murder"), 0o755); err != nil {
		t.Fatal(err)
	}
	results := checkFiles(cfg, "/etc/murder/agent.yaml", h)
	if !has(results, Failure, "/etc/murder/dns.token (a secret) is 0644") || !has(results, Failure, "agent.yaml is 0664") || !has(results, Warning, "/var/lib/murder is 0755") {
		t.Errorf("results = %+v", results)
	}
	if has(results, Failure, "master.key") {
		t.Errorf("the master key is fine: %+v", results)
	}
	// A registry's token and the node's identity key are secrets too.
	h = fakeHost(t, nil)
	write(t, h, "/etc/murder/master.key", "key", 0o600)
	write(t, h, "/etc/murder/ghcr.token", "token", 0o640)
	write(t, h, "/var/lib/murder/cluster/node.key", "key", 0o644)
	results = checkFiles(parse(t, "registries: [{host: ghcr.io, username: u, tokenFile: /etc/murder/ghcr.token}]\ncluster: {address: 10.0.0.2:7400, dir: /var/lib/murder/cluster}\n"), "/etc/murder/agent.yaml", h)
	if !has(results, Failure, "/etc/murder/ghcr.token (a secret) is 0640") || !has(results, Failure, "/var/lib/murder/cluster/node.key (a secret) is 0644") {
		t.Errorf("registry token and identity key: %+v", results)
	}
	// A missing master key fails; a missing optional secret is no news.
	results = checkFiles(parse(t, "registryAuthFile: /etc/murder/registry.json\n"), "/etc/murder/agent.yaml", fakeHost(t, nil))
	if !has(results, Failure, "master.key is missing") || len(results) != 1 {
		t.Errorf("missing: %+v", results)
	}
}

func TestKernel(t *testing.T) {
	h := fakeHost(t, nil)
	for name, value := range map[string]string{
		"net/ipv4/conf/all/accept_redirects": "0",
		"net/ipv4/conf/all/send_redirects":   "1",
		"net/ipv4/conf/all/rp_filter":        "2",
		"kernel/kptr_restrict":               "0",
		"kernel/dmesg_restrict":              "1",
		"net/ipv4/tcp_slow_start_after_idle": "0",
	} {
		write(t, h, "/proc/sys/"+name, value+"\n", 0o644)
	}
	results := checkKernel(h)
	if len(results) != 1 || !has(results, Warning, "net.ipv4.conf.all.send_redirects is 1 (want 0); kernel.kptr_restrict is 0 (want 1)") {
		t.Errorf("results = %+v", results)
	}
	if strings.Contains(results[0].Message, "rp_filter") || strings.Contains(results[0].Message, "bpf") {
		t.Errorf("a good or missing setting reported: %+v", results)
	}
}

func TestUpdates(t *testing.T) {
	on := `APT::Periodic::Update-Package-Lists "1";` + "\n" + `APT::Periodic::Unattended-Upgrade "1";` + "\n"
	h := fakeHost(t, map[string]string{"apt-config dump": on})
	write(t, h, "/usr/bin/unattended-upgrade", "", 0o755)
	write(t, h, "/run/reboot-required", "", 0o644)
	results := checkUpdates(context.Background(), h)
	if !has(results, OK, "install themselves") || !has(results, Warning, "reboot") {
		t.Errorf("on: %+v", results)
	}
	h = fakeHost(t, map[string]string{"apt-config dump": strings.Replace(on, `Upgrade "1"`, `Upgrade "0"`, 1)})
	write(t, h, "/usr/bin/unattended-upgrade", "", 0o755)
	if results := checkUpdates(context.Background(), h); !has(results, Warning, "do not install themselves") {
		t.Errorf("off: %+v", results)
	}
	if results := checkUpdates(context.Background(), fakeHost(t, nil)); !has(results, Skipped, "not Debian") {
		t.Errorf("not Debian: %+v", results)
	}
}

func TestDocker(t *testing.T) {
	h := fakeHost(t, nil)
	write(t, h, "/etc/group", "root:x:0:\ndocker:x:999:alice,bob\n", 0o644)
	h.Engine = func(context.Context) (runtime.EngineInfo, error) { return runtime.EngineInfo{}, nil }
	results := checkDocker(context.Background(), h)
	if !has(results, Warning, "stops every tenant") || !has(results, Warning, "seccomp") || !has(results, Warning, "alice, bob") {
		t.Errorf("results = %+v", results)
	}
	h.Engine = func(context.Context) (runtime.EngineInfo, error) {
		return runtime.EngineInfo{}, errors.New("no socket")
	}
	if results := checkDocker(context.Background(), h); !has(results, Skipped, "does not answer") {
		t.Errorf("down: %+v", results)
	}
}

// On a machine with none of the tools and none of the files, every check
// still answers, and the firewall's absence counts.
func TestCheckOnABareMachine(t *testing.T) {
	results := Check(context.Background(), parse(t, ""), "/etc/murder/agent.yaml", fakeHost(t, nil))
	for _, area := range []string{"files", "firewall", "services", "backups", "ssh", "listeners", "docker", "updates", "kernel", "clock"} {
		find(t, results, area)
	}
	if !has(results, Failure, "nft is not installed") || !has(results, Warning, "host firewall is off") {
		t.Errorf("results = %+v", results)
	}
	results = Check(context.Background(), parse(t, "firewall: off\n"), "/etc/murder/agent.yaml", fakeHost(t, nil))
	if !has(results, Failure, "tenant firewall is off") {
		t.Errorf("firewall off: %+v", results)
	}
}

func TestFirewallLoaded(t *testing.T) {
	loaded := "table inet murder {\n\tchain input {\n\t\tiif \"lo\" accept\n\t}\n}\n"
	h := fakeHost(t, map[string]string{"nft list table inet murder": loaded})
	if results := checkFirewall(context.Background(), parse(t, "hostFirewall: {}\n"), h); len(results) != 1 || !has(results, OK, "loaded") {
		t.Errorf("loaded: %+v", results)
	}
	h = fakeHost(t, map[string]string{"nft list table inet murder": "table inet murder {\n}\n"})
	if results := checkFirewall(context.Background(), parse(t, "hostFirewall: {}\n"), h); !has(results, Warning, "not loaded yet") {
		t.Errorf("host rules missing: %+v", results)
	}
}

func TestBackups(t *testing.T) {
	h := fakeHost(t, nil)
	write(t, h, BackupDir+"/murder-20261105T100000Z.db", "", 0o600)
	old := h.Now().Add(-5 * time.Hour)
	if err := os.Chtimes(h.path(BackupDir+"/murder-20261105T100000Z.db"), old, old); err != nil {
		t.Fatal(err)
	}
	if results := checkBackups(parse(t, ""), h); !has(results, Warning, "5h0m0s old") || !has(results, Warning, "not backed up off the node") {
		t.Errorf("old: %+v", results)
	}
	recent := h.Now().Add(-20 * time.Minute)
	if err := os.Chtimes(h.path(BackupDir+"/murder-20261105T100000Z.db"), recent, recent); err != nil {
		t.Fatal(err)
	}
	if results := checkBackups(parse(t, ""), h); !has(results, OK, "20m0s ago") {
		t.Errorf("recent: %+v", results)
	}
}

// The running edge's process says whether it is confined, whatever its
// unit file says now.
func TestEdgeConfined(t *testing.T) {
	h := fakeHost(t, map[string]string{"systemctl show -p MainPID --value murder-edge.service": "4242\n"})
	write(t, h, "/proc/4242/status", "Name:\tmurder\nNoNewPrivs:\t0\nSeccomp:\t0\n", 0o644)
	if results := checkEdgeConfined(context.Background(), h); !has(results, Warning, "unconfined") {
		t.Errorf("unconfined: %+v", results)
	}
	write(t, h, "/proc/4242/status", "Name:\tmurder\nNoNewPrivs:\t1\nSeccomp:\t2\n", 0o644)
	if results := checkEdgeConfined(context.Background(), h); !has(results, OK, "confined") {
		t.Errorf("confined: %+v", results)
	}
	stopped := fakeHost(t, map[string]string{"systemctl show -p MainPID --value murder-edge.service": "0\n"})
	if results := checkEdgeConfined(context.Background(), stopped); results != nil {
		t.Errorf("stopped: %+v", results)
	}
}
