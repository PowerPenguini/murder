package setup

import (
	"context"
	"slices"
	"strings"
	"testing"

	"github.com/PowerPenguini/murder/internal/config"
)

// What ss says on a cloud host: socket-activated ssh on 22 and 2222,
// Murder's edge, a monitoring agent on everything, a database on the
// private network, and what the firewall need not open.
const ssListening = `udp   UNCONN 0      0           127.0.0.53%lo:53         0.0.0.0:*    users:(("systemd-resolve",pid=600,fd=13))
udp   UNCONN 0      0       10.0.0.2%enp7s0:68           0.0.0.0:*    users:(("systemd-network",pid=550,fd=22))
udp   UNCONN 0      0               0.0.0.0:5355         0.0.0.0:*    users:(("systemd-resolve",pid=600,fd=11))
udp   UNCONN 0      0                 [::1]:323             [::]:*    users:(("chronyd",pid=700,fd=6))
tcp   LISTEN 0      4096            0.0.0.0:22           0.0.0.0:*    users:(("systemd",pid=1,fd=50))
tcp   LISTEN 0      128             0.0.0.0:2222         0.0.0.0:*    users:(("sshd",pid=900,fd=3))
tcp   LISTEN 0      4096                  *:443                *:*    users:(("murder",pid=1200,fd=8))
tcp   LISTEN 0      4096                  *:9100               *:*    users:(("node_exporter",pid=1300,fd=3))
tcp   LISTEN 0      4096               [::]:9100            [::]:*    users:(("node_exporter",pid=1300,fd=4))
tcp   LISTEN 0      244            10.0.0.2:5432         0.0.0.0:*    users:(("postgres",pid=1400,fd=7))
tcp   LISTEN 0      4096            0.0.0.0:8080         0.0.0.0:*    users:(("docker-proxy",pid=1500,fd=4))
tcp   LISTEN 0      4096          127.0.0.1:7480         0.0.0.0:*    users:(("murder",pid=1200,fd=9))
`

func firewallHost() *fakeHost {
	host := newFakeHost()
	host.commands["ss -H -ltunp"] = ssListening
	host.commands["sshd -T"] = "port 22\nport 2222\npasswordauthentication no\n"
	return host
}

func TestSetupFindsWhatTheHostFirewallWouldClose(t *testing.T) {
	facts := Inspect(context.Background(), firewallHost())
	if !slices.Equal(facts.SSHPorts, []int{22, 2222}) {
		t.Fatalf("ssh ports %v", facts.SSHPorts)
	}
	var found []string
	for _, l := range facts.Listeners {
		found = append(found, l.Entry()+" "+l.Process+" "+map[bool]string{true: "private", false: "public"}[l.Private])
	}
	if want := []string{"5432/tcp postgres private", "9100/tcp node_exporter public"}; !slices.Equal(found, want) {
		t.Fatalf("listeners %q, want %q", found, want)
	}

	// Without ss or sshd, SSH is taken to be on 22.
	if ports := Inspect(context.Background(), newFakeHost()).SSHPorts; !slices.Equal(ports, []int{22}) {
		t.Fatalf("ssh ports on a bare host %v", ports)
	}
}

func TestSetupTurnsTheHostFirewallOn(t *testing.T) {
	facts := Inspect(context.Background(), firewallHost())
	s, problems := Settle(Options{Domain: "example.com", ClusterAddress: "none", ZFSPool: "none"}, facts)
	if len(problems) > 0 {
		t.Fatal(problems)
	}
	text := string(Config(s))
	agent, err := config.Parse([]byte(text))
	if err != nil {
		t.Fatalf("%v\n%s", err, text)
	}
	h := agent.HostFirewall
	if h == nil || !slices.Equal(h.SSH, []int{22, 2222}) || !slices.Equal(h.Open, []string{"9100/tcp"}) || !slices.Equal(h.Private, []string{"5432/tcp"}) {
		t.Fatalf("hostFirewall %+v in\n%s", h, text)
	}
	if !strings.Contains(text, "9100/tcp  # node_exporter") {
		t.Fatalf("the config does not say what listens:\n%s", text)
	}
	if _, err := agent.HostRules(); err != nil {
		t.Fatal(err)
	}

	s, _ = Settle(Options{Domain: "example.com", ClusterAddress: "none", ZFSPool: "none", NoHostFirewall: true}, facts)
	if agent, err := config.Parse(Config(s)); err != nil || agent.HostFirewall != nil {
		t.Fatalf("-no-host-firewall gave %+v, %v", agent.HostFirewall, err)
	}
}
