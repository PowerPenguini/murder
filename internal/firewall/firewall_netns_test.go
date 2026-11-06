//go:build netns

// The test runs the rules in network namespaces of its own (no root, no
// Docker; the host's network is untouched):
//
//	go test -tags netns ./internal/firewall -v
//
// A namespace stands for the host, with the tenant network 10.249.0.0/28
// and a private network 10.77.0.0/24, each behind a veth pair to a namespace
// of its own. The private network holds a DNS server and other services.
package firewall

import (
	"context"
	"fmt"
	"net"
	"net/netip"
	"os"
	"os/exec"
	"testing"
	"time"
)

func TestRulesInNamespaces(t *testing.T) {
	if os.Getenv("MURDER_NETNS") == "" {
		// Run this test again as root of a user and network namespace.
		if _, err := exec.LookPath("unshare"); err != nil {
			t.Skip("unshare is not installed")
		}
		self, err := os.Executable()
		if err != nil {
			t.Fatal(err)
		}
		command := exec.Command("unshare", "--user", "--map-root-user", "--net", "--fork", self, "-test.run=^TestRulesInNamespaces$", "-test.v")
		command.Env = append(os.Environ(), "MURDER_NETNS=1")
		output, err := command.CombinedOutput()
		t.Logf("in namespaces:\n%s", output)
		if err != nil {
			t.Fatal(err)
		}
		return
	}
	run := func(args ...string) string {
		t.Helper()
		output, err := exec.Command(args[0], args[1:]...).CombinedOutput()
		if err != nil {
			t.Fatalf("%v: %v\n%s", args, err, output)
		}
		return string(output)
	}
	// Namespaces for the tenant and the private network: a process each.
	spawn := func() string {
		t.Helper()
		command := exec.Command("unshare", "--net", "sleep", "600")
		if err := command.Start(); err != nil {
			t.Fatal(err)
		}
		t.Cleanup(func() { _ = command.Process.Kill() })
		return fmt.Sprint(command.Process.Pid)
	}
	tenant, lan := spawn(), spawn()
	time.Sleep(200 * time.Millisecond)
	in := func(pid string, args ...string) string {
		t.Helper()
		return run(append([]string{"nsenter", "--target", pid, "--net"}, args...)...)
	}
	run("ip", "link", "set", "lo", "up")
	run("ip", "link", "add", "host-t", "type", "veth", "peer", "name", "eth0", "netns", tenant)
	run("ip", "link", "add", "host-l", "type", "veth", "peer", "name", "eth0", "netns", lan)
	run("ip", "address", "add", "10.249.0.1/28", "dev", "host-t")
	run("ip", "address", "add", "10.77.0.1/24", "dev", "host-l")
	run("ip", "link", "set", "host-t", "up")
	run("ip", "link", "set", "host-l", "up")
	for pid, addresses := range map[string][2]string{tenant: {"10.249.0.2/28", "10.249.0.1"}, lan: {"10.77.0.2/24", "10.77.0.1"}} {
		address, gateway := addresses[0], addresses[1]
		in(pid, "ip", "link", "set", "lo", "up")
		in(pid, "ip", "address", "add", address, "dev", "eth0")
		in(pid, "ip", "link", "set", "eth0", "up")
		in(pid, "ip", "route", "add", "default", "via", gateway)
	}
	if err := os.WriteFile("/proc/sys/net/ipv4/ip_forward", []byte("1\n"), 0o644); err != nil {
		t.Fatal(err)
	}
	// Services: the host's (an edge port, another port) and the private
	// network's (DNS, SSH).
	for _, address := range []string{"0.0.0.0:8098", "0.0.0.0:8099"} {
		listener, err := net.Listen("tcp", address)
		if err != nil {
			t.Fatal(err)
		}
		t.Cleanup(func() { listener.Close() })
		go acceptAll(listener)
	}
	for _, port := range []string{"53", "22"} {
		server := exec.Command("nsenter", "--target", lan, "--net", "python3", "-c",
			"import socket\ns=socket.socket()\ns.setsockopt(socket.SOL_SOCKET,socket.SO_REUSEADDR,1)\ns.bind(('0.0.0.0',"+port+"))\ns.listen()\nwhile True: s.accept()[0].close()")
		if err := server.Start(); err != nil {
			t.Fatal(err)
		}
		t.Cleanup(func() { _ = server.Process.Kill() })
	}
	tenantServer := exec.Command("nsenter", "--target", tenant, "--net", "python3", "-c",
		"import socket\ns=socket.socket()\ns.bind(('0.0.0.0',8080))\ns.listen()\nwhile True: s.accept()[0].close()")
	if err := tenantServer.Start(); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = tenantServer.Process.Kill() })
	time.Sleep(500 * time.Millisecond)

	reaches := func(address string) bool {
		host, port, _ := net.SplitHostPort(address)
		err := exec.Command("nsenter", "--target", tenant, "--net", "python3", "-c",
			"import socket,sys\ntry:\n socket.create_connection(('"+host+"',"+port+"),timeout=2).close()\nexcept OSError:\n sys.exit(1)").Run()
		return err == nil
	}
	hostReaches := func(address string) bool {
		conn, err := net.DialTimeout("tcp", address, 2*time.Second)
		if err == nil {
			conn.Close()
		}
		return err == nil
	}
	// Without rules everything is reachable: what the rules change.
	for _, address := range []string{"10.249.0.1:8098", "10.77.0.2:22", "10.77.0.2:53"} {
		if !reaches(address) {
			t.Fatalf("without rules the tenant cannot reach %s; the test proves nothing", address)
		}
	}
	if err := Apply(context.Background(), []netip.Prefix{netip.MustParsePrefix("10.249.0.0/16")}, []int{8099}, []netip.Addr{netip.MustParseAddr("10.77.0.2")}, nil); err != nil {
		t.Fatal(err)
	}
	for address, want := range map[string]bool{
		"10.249.0.1:8098": false, // a host service
		"10.77.0.1:8098":  false, // the same, at another host address
		"10.249.0.1:8099": true,  // an edge port
		"10.77.0.2:22":    false, // a private server
		"10.77.0.2:53":    true,  // the host's DNS server
	} {
		if got := reaches(address); got != want {
			t.Errorf("tenant reaches %s: %v, want %v", address, got, want)
		}
	}
	// The host still reaches the tenant, as the edge and health checks do.
	if !hostReaches("10.249.0.2:8080") {
		t.Error("the host cannot reach the tenant")
	}
	t.Log(run("nft", "list", "table", "inet", Table))

	// With the host firewall, the outside (the private network stands in
	// for the internet, the tenant network for itself) reaches the host
	// only on the open ports, and private ports only from a private address.
	// 8098 is the public port, 8099 the edge, 8097 a private one, 8096
	// closed.
	for _, address := range []string{"0.0.0.0:8097", "0.0.0.0:8096"} {
		listener, err := net.Listen("tcp", address)
		if err != nil {
			t.Fatal(err)
		}
		t.Cleanup(func() { listener.Close() })
		go acceptAll(listener)
	}
	lanReaches := func(address string) bool {
		host, port, _ := net.SplitHostPort(address)
		err := exec.Command("nsenter", "--target", lan, "--net", "python3", "-c",
			"import socket,sys\ntry:\n socket.create_connection(('"+host+"',"+port+"),timeout=2).close()\nexcept OSError:\n sys.exit(1)").Run()
		return err == nil
	}
	pings := func(pid, address string) bool {
		return exec.Command("nsenter", "--target", pid, "--net", "ping", "-c", "1", "-W", "2", address).Run() == nil
	}
	host := &Host{PublicTCP: []int{8098, 8099}, PublicUDP: []int{51820}, PrivateTCP: []int{8097}, PrivateUDP: []int{8125}}
	if err := Apply(context.Background(), []netip.Prefix{netip.MustParsePrefix("10.249.0.0/16")}, []int{8099}, []netip.Addr{netip.MustParseAddr("10.77.0.2")}, host); err != nil {
		t.Fatal(err)
	}
	for address, want := range map[string]bool{
		"10.77.0.1:8098": true,  // open to all
		"10.77.0.1:8099": true,  // the edge
		"10.77.0.1:8097": true,  // private, from a private address
		"10.77.0.1:8096": false, // closed
	} {
		if got := lanReaches(address); got != want {
			t.Errorf("the private network reaches %s: %v, want %v", address, got, want)
		}
	}
	// The tenant rules still decide the tenant's traffic.
	for address, want := range map[string]bool{
		"10.249.0.1:8099": true,
		"10.249.0.1:8098": false,
		"10.249.0.1:8097": false,
		"10.77.0.2:53":    true,
	} {
		if got := reaches(address); got != want {
			t.Errorf("with the host firewall, the tenant reaches %s: %v, want %v", address, got, want)
		}
	}
	if !hostReaches("10.249.0.2:8080") || !hostReaches("127.0.0.1:8096") {
		t.Error("the host cannot reach the tenant or itself")
	}
	if _, err := exec.LookPath("ping"); err == nil && !pings(lan, "10.77.0.1") {
		t.Error("the host does not answer ping")
	}
	t.Log(run("nft", "list", "table", "inet", Table))
}

func acceptAll(listener net.Listener) {
	for {
		conn, err := listener.Accept()
		if err != nil {
			return
		}
		conn.Close()
	}
}
