//go:build firewall

// The test changes the host firewall, so it runs as root in the test VM,
// which has Docker and nft:
//
//	GO_TEST_TAGS=firewall test/vm/go-test.sh ./internal/firewall -v
package firewall

import (
	"context"
	"net"
	"net/http"
	"net/netip"
	"os"
	"os/exec"
	"strings"
	"testing"
	"time"

	"github.com/PowerPenguini/murder/internal/runtime"
)

const probeImage = "busybox:1.37"

func TestTenantReachesOnlyTheInternetAndTheEdge(t *testing.T) {
	if os.Getenv("MURDER_TEST_VM") == "" {
		t.Skip("changes the host firewall; run it in the test VM")
	}
	ctx := context.Background()
	if output, err := exec.Command("docker", "pull", "-q", probeImage).CombinedOutput(); err != nil {
		t.Fatalf("pull %s: %v\n%s", probeImage, err, output)
	}
	docker := runtime.NewDocker(runtime.DockerOptions{Socket: "/var/run/docker.sock", Node: "firewall-test"})
	pool := netip.MustParsePrefix("10.249.0.0/16")
	const network, subnet = "murder-firewall-test", "10.249.0.0/28"
	if err := docker.EnsureNetwork(ctx, network, subnet, map[string]string{runtime.LabelTenant: "firewall-test"}); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		_ = exec.Command("docker", "rm", "-f", "murder-firewall-listener").Run()
		_ = docker.RemoveNetwork(context.Background(), network)
		_ = exec.Command("nft", "delete", "table", "inet", Table).Run()
	})
	gateway, address, _ := runtime.Addresses(subnet)
	for _, port := range []string{"8098", "8099"} {
		listener, err := net.Listen("tcp", "0.0.0.0:"+port)
		if err != nil {
			t.Fatal(err)
		}
		server := &http.Server{Handler: http.NotFoundHandler()}
		go server.Serve(listener)
		t.Cleanup(func() { _ = server.Close() })
	}
	reaches := func(host, port string) bool {
		t.Helper()
		err := exec.Command("docker", "run", "--rm", "--network", network, probeImage, "nc", "-z", "-w", "3", host, port).Run()
		return err == nil
	}
	hostReaches := func(host, port string) bool {
		conn, err := net.DialTimeout("tcp", net.JoinHostPort(host, port), 3*time.Second)
		if err == nil {
			conn.Close()
		}
		return err == nil
	}

	// Without the rules a tenant reaches the host: what the rules change.
	if !reaches(gateway.String(), "8098") {
		t.Fatal("a tenant cannot reach the host even without the rules; the test proves nothing")
	}
	for range 2 { // loading twice replaces the table
		if err := Apply(ctx, []netip.Prefix{pool}, []int{8099}, Resolvers(), nil); err != nil {
			t.Fatal(err)
		}
	}
	if output, _ := exec.Command("nft", "list", "tables").CombinedOutput(); strings.Count(string(output), "inet "+Table) != 1 {
		t.Fatalf("tables:\n%s", output)
	}

	if reaches(gateway.String(), "8098") || reaches(gateway.String(), "22") {
		t.Error("a tenant reaches host services")
	}
	if !reaches(gateway.String(), "8099") {
		t.Error("a tenant cannot reach the edge port")
	}
	// 10.0.2.2 is the VM's NAT gateway, a private address the VM reaches.
	if hostReaches("10.0.2.2", "22") && reaches("10.0.2.2", "22") {
		t.Error("a tenant reaches a private address")
	}
	if hostReaches("1.1.1.1", "443") && !reaches("1.1.1.1", "443") {
		t.Error("a tenant cannot reach the internet")
	}
	if output, err := exec.Command("docker", "run", "--rm", "--network", network, probeImage, "nslookup", "example.com").CombinedOutput(); err != nil {
		t.Errorf("a tenant cannot resolve names: %v\n%s", err, output)
	}
	// The host still reaches the tenant, as the edge and health checks do.
	if output, err := exec.Command("docker", "run", "-d", "--name", "murder-firewall-listener", "--network", network, "--ip", address.String(),
		probeImage, "httpd", "-f", "-p", "8080", "-h", "/").CombinedOutput(); err != nil {
		t.Fatalf("start a tenant listener: %v\n%s", err, output)
	}
	deadline := time.Now().Add(10 * time.Second)
	for !hostReaches(address.String(), "8080") {
		if time.Now().After(deadline) {
			t.Fatal("the host cannot reach a tenant")
		}
		time.Sleep(200 * time.Millisecond)
	}
}
