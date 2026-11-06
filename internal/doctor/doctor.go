// Package doctor checks how safely a node is set up, and changes nothing:
// Murder's own files, firewall and services, what listens on the public
// addresses, SSH, Docker, updates, kernel settings and the clock. Each
// check says what it found and, when something is amiss, how to fix it.
// A check whose tool is missing, or that needs root, is skipped, not
// failed: the doctor runs on any Linux and reports what it could not see.
package doctor

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"slices"
	"strconv"
	"strings"
	"syscall"
	"time"

	"github.com/PowerPenguini/murder/internal/cluster"
	"github.com/PowerPenguini/murder/internal/config"
	"github.com/PowerPenguini/murder/internal/runtime"
)

// Status is how a check came out, from best to worst.
type Status int

const (
	OK Status = iota
	Skipped
	Warning
	Failure
)

func (s Status) String() string {
	return [...]string{"ok", "skip", "warn", "FAIL"}[s]
}

// Result is one finding.
type Result struct {
	Status Status
	// Area groups findings: files, firewall, ssh and so on.
	Area    string
	Message string
	// Fix says what to do; empty when there is nothing to.
	Fix string
}

// Host is how the checks see the machine; tests replace each part.
type Host struct {
	// Root prefixes every path read, such as /proc and /etc; empty on a
	// node.
	Root string
	// Owner is the user secrets must belong to: root.
	Owner int
	// Privileged says whether the doctor runs as root, which some
	// checks need.
	Privileged bool
	// Run runs a command and returns its output.
	Run func(ctx context.Context, name string, args ...string) ([]byte, error)
	// Engine asks Docker about itself.
	Engine func(ctx context.Context) (runtime.EngineInfo, error)
	Now    func() time.Time
}

// System is the machine the doctor runs on, with Docker at the socket cfg
// names.
func System(cfg config.Agent) Host {
	docker := runtime.NewDocker(runtime.DockerOptions{Socket: cfg.DockerSocket})
	return Host{
		Owner:      0,
		Privileged: os.Geteuid() == 0,
		Run: func(ctx context.Context, name string, args ...string) ([]byte, error) {
			ctx, cancel := context.WithTimeout(ctx, 10*time.Second)
			defer cancel()
			command := exec.CommandContext(ctx, name, args...)
			command.Env = append(os.Environ(), "LC_ALL=C")
			return command.CombinedOutput()
		},
		Engine: docker.Info,
		Now:    time.Now,
	}
}

func (h Host) path(name string) string { return filepath.Join(h.Root, name) }

// BackupDir is where murder-backup.timer keeps copies of the database.
const BackupDir = "/var/backups/murder"

// Check runs every check against the node cfg configures, read from
// configPath.
func Check(ctx context.Context, cfg config.Agent, configPath string, h Host) []Result {
	var results []Result
	results = append(results, checkFiles(cfg, configPath, h)...)
	results = append(results, checkFirewall(ctx, cfg, h)...)
	results = append(results, checkUnits(ctx, cfg, h)...)
	results = append(results, checkBackups(cfg, h)...)
	sshResults, sshPorts := checkSSH(ctx, cfg, h)
	results = append(results, sshResults...)
	results = append(results, checkListeners(cfg, h, sshPorts)...)
	results = append(results, checkDocker(ctx, h)...)
	results = append(results, checkUpdates(ctx, h)...)
	results = append(results, checkKernel(h)...)
	results = append(results, checkClock(ctx, h)...)
	return results
}

// missingTool reports whether err says the command is not installed.
func missingTool(err error) bool {
	return errors.Is(err, exec.ErrNotFound) || errors.Is(err, os.ErrNotExist)
}

// secret is a file only its owner may read.
type secret struct {
	path     string
	required bool
}

func secrets(cfg config.Agent) []secret {
	list := []secret{{cfg.MasterKeyFile, true}}
	optional := func(path string) {
		if path != "" {
			list = append(list, secret{path, false})
		}
	}
	optional(cfg.Edge.TLS.DNSTokenFile)
	optional(cfg.RegistryAuthFile)
	for _, r := range cfg.Registries {
		optional(r.TokenFile)
	}
	if b := cfg.Backups; b != nil {
		optional(b.AccessKeyFile)
		optional(b.SecretKeyFile)
		optional(b.IdentityFile)
	}
	if c := cfg.Cluster; c != nil {
		if c.Dir != "" {
			optional(cluster.IdentityPath(c.Dir))
		}
		if c.DNS != nil {
			optional(c.DNS.TokenFile)
		}
		if c.Failover != nil && c.Failover.Hetzner != nil {
			optional(c.Failover.Hetzner.TokenFile)
		}
	}
	if t := cfg.Panel.Tunnel; t != nil {
		optional(t.KeyFile)
	}
	return list
}

// owner is the user that owns info's file, or -1 off Linux.
func owner(info os.FileInfo) int {
	if stat, ok := info.Sys().(*syscall.Stat_t); ok {
		return int(stat.Uid)
	}
	return -1
}

func checkFiles(cfg config.Agent, configPath string, h Host) []Result {
	var results []Result
	checked := 0
	for _, s := range secrets(cfg) {
		info, err := os.Stat(h.path(s.path))
		switch {
		case errors.Is(err, os.ErrNotExist) && s.required:
			results = append(results, Result{Failure, "files", s.path + " is missing", `"murder init" creates the master key; restore it from the offline copy otherwise`})
			continue
		case errors.Is(err, os.ErrPermission):
			results = append(results, Result{Skipped, "files", "cannot read " + filepath.Dir(s.path), "run as root"})
			continue
		case err != nil:
			continue
		}
		checked++
		mode := info.Mode().Perm()
		if mode&0o077 != 0 || owner(info) != h.Owner {
			results = append(results, Result{Failure, "files", fmt.Sprintf("%s (a secret) is %04o and user %d's", s.path, mode, owner(info)), fmt.Sprintf("chown root:root %s && chmod 600 %s", s.path, s.path)})
		}
	}
	if info, err := os.Stat(h.path(configPath)); err == nil && (info.Mode().Perm()&0o022 != 0 || owner(info) != h.Owner) {
		results = append(results, Result{Failure, "files", fmt.Sprintf("%s is %04o and user %d's: whoever may write it runs code as root", configPath, info.Mode().Perm(), owner(info)), "chown root:root " + configPath + " && chmod 644 " + configPath})
	}
	for _, dir := range []string{filepath.Dir(cfg.Database), BackupDir} {
		if info, err := os.Stat(h.path(dir)); err == nil && info.Mode().Perm()&0o077 != 0 {
			results = append(results, Result{Warning, "files", fmt.Sprintf("%s is %04o: it holds the database, whose audit log and tenants others may then read", dir, info.Mode().Perm()), "chmod 700 " + dir})
		}
	}
	if len(results) == 0 {
		results = append(results, Result{OK, "files", fmt.Sprintf("%d secrets, the config and the database are root's alone", checked), ""})
	}
	return results
}

func checkFirewall(ctx context.Context, cfg config.Agent, h Host) []Result {
	if cfg.Firewall != config.FirewallNftables {
		return []Result{{Failure, "firewall", "the tenant firewall is off: tenants reach the host's services, private networks and the cloud's metadata service", "set firewall: nftables"}}
	}
	var results []Result
	output, err := h.Run(ctx, "nft", "list", "table", "inet", "murder")
	switch {
	case missingTool(err):
		results = append(results, Result{Failure, "firewall", "nft is not installed: the agent cannot load the tenant firewall", "apt install nftables"})
	case err != nil && !h.Privileged:
		results = append(results, Result{Skipped, "firewall", "cannot list the loaded rules", "run as root"})
	case err != nil:
		results = append(results, Result{Failure, "firewall", "the tenant firewall is not loaded: " + firstLine(output), "systemctl restart murder-firewall murder-agent"})
	case cfg.HostFirewall != nil && !bytes.Contains(output, []byte("iif \"lo\" accept")):
		results = append(results, Result{Warning, "firewall", "the host firewall is configured but not loaded yet", "systemctl restart murder-agent murder-firewall: the agent reads the config at start and loads its rules every 15 s"})
	default:
		results = append(results, Result{OK, "firewall", "the tenant firewall is loaded", ""})
	}
	if cfg.HostFirewall == nil {
		results = append(results, Result{Warning, "firewall", "the host firewall is off: every port the host listens on is open to the internet", "set hostFirewall in the config (see README), or filter in the provider's firewall"})
	}
	return results
}

func firstLine(output []byte) string {
	line, _, _ := strings.Cut(strings.TrimSpace(string(output)), "\n")
	return line
}

func checkUnits(ctx context.Context, cfg config.Agent, h Host) []Result {
	units := []string{"murder-agent.service", "murder-edge.service", "murder-backup.timer"}
	if cfg.Firewall == config.FirewallNftables {
		units = append([]string{"murder-firewall.service"}, units...)
	}
	var off []string
	for _, unit := range units {
		output, err := h.Run(ctx, "systemctl", "is-enabled", unit)
		if missingTool(err) {
			return []Result{{Skipped, "services", "systemctl is not installed", ""}}
		}
		if state := strings.TrimSpace(string(output)); state != "enabled" {
			off = append(off, unit+" ("+state+")")
		}
	}
	var results []Result
	if len(off) > 0 {
		results = append(results, Result{Warning, "services", "not started at boot: " + strings.Join(off, ", "), "install the units from deploy/ and systemctl enable --now them"})
	} else {
		results = append(results, Result{OK, "services", "Murder's services start at boot", ""})
	}
	return append(results, checkEdgeConfined(ctx, h)...)
}

// checkEdgeConfined looks at the running edge, not its unit file: a unit
// installed anew is confined only once the edge restarts (a reload hands
// over to a child of the old, unconfined process).
func checkEdgeConfined(ctx context.Context, h Host) []Result {
	output, err := h.Run(ctx, "systemctl", "show", "-p", "MainPID", "--value", "murder-edge.service")
	pid := strings.TrimSpace(string(output))
	if err != nil || pid == "" || pid == "0" {
		return nil
	}
	status, err := os.ReadFile(h.path("/proc/" + pid + "/status"))
	if err != nil {
		return nil
	}
	for line := range strings.Lines(string(status)) {
		if value, ok := strings.CutPrefix(line, "NoNewPrivs:"); ok {
			if strings.TrimSpace(value) == "1" {
				return []Result{{OK, "services", "the edge runs confined", ""}}
			}
			return []Result{{Warning, "services", "the edge runs unconfined: its unit predates deploy/murder-edge.service, or it has not restarted since", "install deploy/murder-edge.service, systemctl daemon-reload, systemctl restart murder-edge"}}
		}
	}
	return nil
}

func checkBackups(cfg config.Agent, h Host) []Result {
	var results []Result
	entries, err := os.ReadDir(h.path(BackupDir))
	var newest time.Time
	for _, entry := range entries {
		if !strings.HasPrefix(entry.Name(), "murder-") || !strings.HasSuffix(entry.Name(), ".db") {
			continue
		}
		if info, err := entry.Info(); err == nil && info.ModTime().After(newest) {
			newest = info.ModTime()
		}
	}
	switch {
	case errors.Is(err, os.ErrPermission):
		results = append(results, Result{Skipped, "backups", "cannot read " + BackupDir, "run as root"})
	case newest.IsZero():
		results = append(results, Result{Warning, "backups", "no copy of the database in " + BackupDir, "systemctl enable --now murder-backup.timer"})
	case h.Now().Sub(newest) > 3*time.Hour:
		results = append(results, Result{Warning, "backups", fmt.Sprintf("the newest copy of the database is %s old", h.Now().Sub(newest).Round(time.Minute)), "systemctl status murder-backup.service"})
	default:
		results = append(results, Result{OK, "backups", "the database was copied " + h.Now().Sub(newest).Round(time.Minute).String() + " ago", ""})
	}
	if cfg.Backups == nil {
		results = append(results, Result{Warning, "backups", "tenants' data is not backed up off the node: a lost disk loses it", "set backups in the config (see README)"})
	}
	return results
}

// checkSSH reads sshd's effective configuration, and returns the ports
// it listens on (nil if unknown).
func checkSSH(ctx context.Context, cfg config.Agent, h Host) ([]Result, []int) {
	output, err := h.Run(ctx, "sshd", "-T")
	if missingTool(err) {
		output, err = h.Run(ctx, "/usr/sbin/sshd", "-T")
	}
	switch {
	case missingTool(err):
		return []Result{{Skipped, "ssh", "sshd is not installed", ""}}, nil
	case err != nil && !h.Privileged:
		return []Result{{Skipped, "ssh", "cannot read sshd's configuration", "run as root"}}, nil
	case err != nil:
		return []Result{{Skipped, "ssh", "sshd -T failed: " + firstLine(output), ""}}, nil
	}
	settings := map[string]string{}
	var ports []int
	for line := range strings.Lines(string(output)) {
		key, value, _ := strings.Cut(strings.TrimSpace(line), " ")
		if key == "port" {
			if port, err := strconv.Atoi(value); err == nil {
				ports = append(ports, port)
			}
		}
		settings[key] = value
	}
	var results []Result
	if settings["passwordauthentication"] == "yes" {
		results = append(results, Result{Failure, "ssh", "SSH takes passwords, which bots guess around the clock", "PasswordAuthentication no in /etc/ssh/sshd_config.d/murder.conf, then systemctl reload ssh"})
	}
	if settings["kbdinteractiveauthentication"] == "yes" && settings["usepam"] == "yes" {
		results = append(results, Result{Warning, "ssh", "SSH takes keyboard-interactive logins, which PAM answers with a password", "KbdInteractiveAuthentication no in /etc/ssh/sshd_config.d/murder.conf"})
	}
	if settings["permitrootlogin"] == "yes" {
		results = append(results, Result{Failure, "ssh", "root may log in over SSH with a password", "PermitRootLogin prohibit-password in /etc/ssh/sshd_config.d/murder.conf"})
	}
	if f := cfg.HostFirewall; f != nil {
		var closed []string
		for _, port := range ports {
			if !slices.Contains(f.SSH, port) {
				closed = append(closed, strconv.Itoa(port))
			}
		}
		if len(closed) > 0 {
			results = append(results, Result{Failure, "ssh", "sshd listens on port " + strings.Join(closed, ", ") + ", which hostFirewall.ssh does not open: the next SSH connection is refused", "list every SSH port in hostFirewall.ssh"})
		}
	}
	if len(results) == 0 {
		results = append(results, Result{OK, "ssh", "SSH takes keys only", ""})
	}
	return results, ports
}

func checkDocker(ctx context.Context, h Host) []Result {
	info, err := h.Engine(ctx)
	if err != nil {
		return []Result{{Skipped, "docker", "Docker does not answer: " + err.Error(), ""}}
	}
	var results []Result
	if !info.LiveRestoreEnabled {
		results = append(results, Result{Warning, "docker", "restarting Docker, as an upgrade of it does, stops every tenant until it is back", `"live-restore": true in /etc/docker/daemon.json, then systemctl reload docker`})
	}
	if !slices.ContainsFunc(info.SecurityOptions, func(option string) bool { return strings.HasPrefix(option, "name=seccomp") }) {
		results = append(results, Result{Warning, "docker", "containers run without a seccomp profile", "remove seccomp-profile: unconfined from /etc/docker/daemon.json"})
	}
	if members := groupMembers(h, "docker"); len(members) > 0 {
		results = append(results, Result{Warning, "docker", "members of the docker group control Docker, and so the host as root: " + strings.Join(members, ", "), "gpasswd -d <user> docker, unless they should be root"})
	}
	if len(results) == 0 {
		results = append(results, Result{OK, "docker", "Docker keeps tenants running across its restarts and filters their system calls", ""})
	}
	return results
}

// groupMembers lists the users /etc/group names in group.
func groupMembers(h Host, group string) []string {
	data, err := os.ReadFile(h.path("/etc/group"))
	if err != nil {
		return nil
	}
	for line := range strings.Lines(string(data)) {
		fields := strings.Split(strings.TrimSpace(line), ":")
		if len(fields) == 4 && fields[0] == group && fields[3] != "" {
			return strings.Split(fields[3], ",")
		}
	}
	return nil
}

func checkUpdates(ctx context.Context, h Host) []Result {
	var results []Result
	output, err := h.Run(ctx, "apt-config", "dump")
	switch {
	case missingTool(err):
		results = append(results, Result{Skipped, "updates", "not Debian or Ubuntu: check that security updates install themselves", ""})
	case err != nil:
		results = append(results, Result{Skipped, "updates", "apt-config failed: " + firstLine(output), ""})
	default:
		enabled := false
		for line := range strings.Lines(string(output)) {
			if value, ok := strings.CutPrefix(strings.TrimSpace(line), "APT::Periodic::Unattended-Upgrade "); ok {
				enabled = strings.Trim(value, `";`) != "0"
			}
		}
		if _, err := os.Stat(h.path("/usr/bin/unattended-upgrade")); err != nil || !enabled {
			results = append(results, Result{Warning, "updates", "security updates do not install themselves", "apt install unattended-upgrades && dpkg-reconfigure -plow unattended-upgrades"})
		} else {
			results = append(results, Result{OK, "updates", "security updates install themselves", ""})
		}
	}
	if _, err := os.Stat(h.path("/run/reboot-required")); err == nil {
		results = append(results, Result{Warning, "updates", "an update waits for a reboot to take effect", "reboot when it suits (tenants restart with Docker)"})
	}
	return results
}

// kernelSetting is a sysctl and the values that pass.
type kernelSetting struct {
	name string
	good func(int) bool
	want string
}

func atLeast(n int) func(int) bool { return func(v int) bool { return v >= n } }
func equal(n int) func(int) bool   { return func(v int) bool { return v == n } }

// kernelSettings are what deploy/murder-hardening.conf sets. Docker needs
// net.ipv4.ip_forward on, so it is not here.
var kernelSettings = []kernelSetting{
	{"net.ipv4.conf.all.accept_redirects", equal(0), "0"},
	{"net.ipv6.conf.all.accept_redirects", equal(0), "0"},
	{"net.ipv4.conf.all.send_redirects", equal(0), "0"},
	{"net.ipv4.conf.all.accept_source_route", equal(0), "0"},
	{"net.ipv4.conf.all.rp_filter", atLeast(1), "1 or 2"},
	{"net.ipv4.tcp_syncookies", equal(1), "1"},
	{"kernel.kptr_restrict", atLeast(1), "1"},
	{"kernel.dmesg_restrict", equal(1), "1"},
	{"kernel.unprivileged_bpf_disabled", atLeast(1), "1"},
}

func checkKernel(h Host) []Result {
	read := func(name string) (int, bool) {
		data, err := os.ReadFile(h.path("/proc/sys/" + strings.ReplaceAll(name, ".", "/")))
		if err != nil {
			return 0, false
		}
		value, err := strconv.Atoi(strings.TrimSpace(string(data)))
		return value, err == nil
	}
	var off []string
	for _, setting := range kernelSettings {
		if value, ok := read(setting.name); ok && !setting.good(value) {
			off = append(off, fmt.Sprintf("%s is %d (want %s)", setting.name, value, setting.want))
		}
	}
	var results []Result
	if len(off) > 0 {
		results = append(results, Result{Warning, "kernel", strings.Join(off, "; "), "install deploy/murder-hardening.conf as /etc/sysctl.d/91-murder-hardening.conf, then sysctl --system"})
	} else {
		results = append(results, Result{OK, "kernel", "the kernel's network and information settings are hardened", ""})
	}
	if value, ok := read("net.ipv4.tcp_slow_start_after_idle"); ok && value != 0 {
		results = append(results, Result{Warning, "kernel", "TCP starts each connection's sending rate over after a pause, slowing the first response after a click", "install deploy/murder-sysctl.conf as /etc/sysctl.d/90-murder.conf"})
	}
	return results
}

func checkClock(ctx context.Context, h Host) []Result {
	output, err := h.Run(ctx, "timedatectl", "show", "-p", "NTPSynchronized", "--value")
	switch {
	case missingTool(err):
		return []Result{{Skipped, "clock", "timedatectl is not installed", ""}}
	case err != nil:
		return []Result{{Skipped, "clock", "timedatectl failed: " + firstLine(output), ""}}
	case strings.TrimSpace(string(output)) != "yes":
		return []Result{{Warning, "clock", "the clock is not synchronised: certificates, sign-ins and the cluster need the right time", "timedatectl set-ntp true"}}
	}
	return []Result{{OK, "clock", "the clock is synchronised", ""}}
}
