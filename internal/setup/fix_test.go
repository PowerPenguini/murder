package setup

import (
	"context"
	"slices"
	"strings"
	"testing"

	"github.com/PowerPenguini/murder/internal/config"
)

const aloneConfig = "node: n1\nbaseDomain: example.com\nvolumes: {driver: dir}\n"

func fixHost() *sshHost {
	host := newSSHHost()
	host.files["/home/ubuntu/.ssh/authorized_keys"] = []byte("ssh-ed25519 AAAA\n")
	host.files["/etc/murder/agent.yaml"] = []byte(aloneConfig)
	host.commands["ss -H -ltunp"] = "tcp LISTEN 0 4096 0.0.0.0:22 0.0.0.0:* users:((\"sshd\",pid=9,fd=3))\ntcp LISTEN 0 4096 *:9100 *:* users:((\"node_exporter\",pid=8,fd=3))\n"
	host.commands["systemctl restart murder-agent.service murder-firewall.service"] = ""
	return host
}

func TestDoctorFixesANodeAlone(t *testing.T) {
	host := fixHost()
	cfg, err := config.Parse([]byte(aloneConfig))
	if err != nil {
		t.Fatal(err)
	}
	steps, notes := Fixes(context.Background(), host, cfg, "/etc/murder/agent.yaml")
	if len(notes) != 0 || len(steps) != 3 {
		t.Fatalf("steps %v, notes %v", steps, notes)
	}
	for _, step := range steps {
		if err := step.Do(context.Background()); err != nil {
			t.Fatalf("%s: %v", step.Say, err)
		}
	}
	if !strings.Contains(string(host.files[SSHDropIn]), "PasswordAuthentication no") || !strings.Contains(string(host.files[DaemonConfig]), `"live-restore": true`) {
		t.Fatal("SSH or live-restore not fixed")
	}
	written := host.files["/etc/murder/agent.yaml"]
	agent, err := config.Parse(written)
	if err != nil || agent.HostFirewall == nil || !slices.Equal(agent.HostFirewall.SSH, []int{22}) || !slices.Equal(agent.HostFirewall.Open, []string{"9100/tcp"}) {
		t.Fatalf("config %s: %+v, %v", written, agent.HostFirewall, err)
	}
	if !strings.HasPrefix(string(written), aloneConfig) || string(host.files["/etc/murder/agent.yaml.before-host-firewall"]) != aloneConfig {
		t.Fatalf("the config was not kept as it was:\n%s", written)
	}
	if !slices.Contains(host.ran, "systemctl restart murder-agent.service murder-firewall.service") {
		t.Fatalf("not restarted: %v", host.ran)
	}

	// Fixed, there is nothing left to do.
	cfg = agent
	host.commands["docker info --format {{.LiveRestoreEnabled}} {{.Swarm.LocalNodeState}}"] = "true inactive"
	if steps, notes := Fixes(context.Background(), host, cfg, "/etc/murder/agent.yaml"); len(steps) != 0 || len(notes) != 0 {
		t.Fatalf("again: steps %v, notes %v", steps, notes)
	}
}

func TestDoctorNamesTheFirewallForAClusterToAdd(t *testing.T) {
	host := fixHost()
	delete(host.files, "/home/ubuntu/.ssh/authorized_keys")
	cfg, err := config.Parse([]byte(aloneConfig + "cluster: {address: \"10.0.0.2:7400\"}\n"))
	if err != nil {
		t.Fatal(err)
	}
	steps, notes := Fixes(context.Background(), host, cfg, "/etc/murder/agent.yaml")
	for _, step := range steps {
		if strings.Contains(step.Say, "SSH") || strings.Contains(step.Say, "firewall") {
			t.Fatalf("step %q", step.Say)
		}
	}
	joined := strings.Join(notes, "\n")
	if !strings.Contains(joined, "has no key") || !strings.Contains(joined, "  ssh: [22]") || !strings.Contains(joined, "9100/tcp") {
		t.Fatalf("notes:\n%s", joined)
	}
}
