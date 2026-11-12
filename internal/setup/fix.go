package setup

import (
	"bytes"
	"context"
	"fmt"
	"strings"

	"github.com/PowerPenguini/murder/internal/config"
)

// Fixes are what "murder doctor -fix" does on a node set up already: what
// setup now does on a new one, where it is safe on a running node.
//   - SSH stops taking passwords, if the account running it logs in with
//     a key (as setup does).
//   - Docker's live-restore is turned on (a reload: no container stops).
//   - On a node alone, the host firewall is turned on with what listens
//     now left open; in a cluster the nodes share a config, so it is
//     named for the operator to add.
//
// The notes are what it leaves to the operator, and why.
func Fixes(ctx context.Context, host Host, cfg config.Agent, configPath string) (steps []Step, notes []string) {
	facts := Inspect(ctx, host)
	add := func(say string, do func(ctx context.Context) error) { steps = append(steps, Step{say, do}) }
	switch s := facts.SSH; {
	case s.Harden():
		add("have SSH take keys only, no passwords ("+SSHDropIn+"; you log in with the key in "+s.Keys+")", func(ctx context.Context) error {
			return hardenSSH(ctx, host)
		})
	case s.Passwords:
		for _, warning := range facts.Warnings() {
			if strings.Contains(warning, "SSH") {
				notes = append(notes, warning)
			}
		}
	}
	if facts.WantsLiveRestore() {
		add("have Docker keep containers running while it restarts (live-restore; a reload, no container stops)", func(ctx context.Context) error {
			return liveRestore(ctx, host)
		})
	}
	if cfg.HostFirewall == nil && cfg.Firewall == config.FirewallNftables {
		block := hostFirewallBlock(facts)
		if cfg.Cluster != nil {
			notes = append(notes, "the host firewall is off; the nodes of a cluster share their config, so add to it on each node (then systemctl restart murder-agent murder-firewall):\n"+block)
		} else {
			add("turn the host firewall on in "+configPath+", with SSH and what listens now left open, then restart murder-agent and murder-firewall (tenants keep running)", func(ctx context.Context) error {
				return addHostFirewall(ctx, host, configPath, block)
			})
		}
	}
	return steps, notes
}

// hostFirewallBlock is the hostFirewall setup would write for the facts.
func hostFirewallBlock(facts Facts) string {
	s, _ := Settle(Options{Domain: "example.com", Node: "node", ZFSPool: "none", ClusterAddress: "none"}, facts)
	text := string(Config(s))
	start := strings.Index(text, "\nhostFirewall:")
	end := strings.Index(text, "\nedge:")
	if start < 0 || end < start {
		panic("the config template has no hostFirewall before edge") // fixed text
	}
	return strings.TrimSpace(text[start:end]) + "\n"
}

// addHostFirewall adds block to the config at configPath, which has no
// hostFirewall, checks the agent takes it, and restarts the agent and the
// firewall to load it. The config is put back if the agent refuses it.
func addHostFirewall(ctx context.Context, host Host, configPath, block string) error {
	old, err := host.ReadFile(configPath)
	if err != nil {
		return err
	}
	// The block starts on a line of its own, though the file ends in a
	// comment with no newline after it.
	data := bytes.Clone(old)
	if len(data) > 0 && !bytes.HasSuffix(data, []byte("\n")) {
		data = append(data, '\n')
	}
	data = append(data, []byte("\n"+block)...)
	agent, err := config.Parse(data)
	switch {
	case err != nil:
		return Skipped{fmt.Sprintf("the agent would refuse %s with hostFirewall added (%v): doctor left it as it was; add it by hand:\n%s", configPath, err, block)}
	case agent.HostFirewall == nil:
		return Skipped{fmt.Sprintf("hostFirewall added to the end of %s does not take: doctor left it as it was; add it by hand:\n%s", configPath, block)}
	}
	if err := host.WriteFile(configPath+".before-host-firewall", old, 0o644); err != nil {
		return err
	}
	if err := host.WriteFile(configPath, data, 0o644); err != nil {
		return err
	}
	if _, err := host.Run(ctx, "systemctl", "restart", "murder-agent.service", "murder-firewall.service"); err != nil {
		return fmt.Errorf("restart murder-agent and murder-firewall to load the host firewall (%s.before-host-firewall is the config before): %w", configPath, err)
	}
	return nil
}
