package setup

import (
	"context"
	"errors"
	"io/fs"
	"net/netip"
	"slices"
	"strings"
	"testing"

	"github.com/PowerPenguini/murder/internal/config"
)

// fakeHost answers commands from a table and keeps written files.
type fakeHost struct {
	commands map[string]string // "name args" -> output; missing ones fail
	files    map[string][]byte
	modes    map[string]fs.FileMode
	ran      []string
	exe      string
	hostname string
	addrs    []string
	root     bool
}

func newFakeHost() *fakeHost {
	return &fakeHost{
		commands: map[string]string{
			"systemctl --version":                            "systemd 255",
			"docker version --format {{.Server.APIVersion}}": "1.51\n",
			"nft --version":                                  "nftables v1.0.9",
			"zpool list -H -o name":                          "tank\n",
			"sysctl -q -p " + SysctlFile:                     "",
			"systemctl daemon-reload":                        "",
			"zfs create -p tank/murder/tenants":              "",
		},
		files:    map[string][]byte{"/sys/fs/cgroup/cgroup.controllers": []byte("cpu memory")},
		modes:    map[string]fs.FileMode{},
		exe:      "/tmp/murder",
		hostname: "fsn-1.example.com",
		addrs:    []string{"203.0.113.10", "10.0.0.2", "2001:db8::1"},
		root:     true,
	}
}

func (f *fakeHost) Run(_ context.Context, name string, args ...string) (string, error) {
	line := strings.TrimSpace(name + " " + strings.Join(args, " "))
	f.ran = append(f.ran, line)
	if strings.HasPrefix(line, "systemctl enable --now") || strings.HasPrefix(line, "systemctl try-") || strings.HasPrefix(line, "ss ") {
		if out, ok := f.commands[line]; ok {
			return out, nil
		}
		return "", nil
	}
	out, ok := f.commands[line]
	if !ok {
		return "", errors.New("no such command: " + line)
	}
	return out, nil
}

func (f *fakeHost) ReadFile(path string) ([]byte, error) {
	data, ok := f.files[path]
	if !ok {
		return nil, fs.ErrNotExist
	}
	return data, nil
}

func (f *fakeHost) WriteFile(path string, data []byte, perm fs.FileMode) error {
	f.files[path], f.modes[path] = data, perm
	return nil
}

func (f *fakeHost) MkdirAll(string, fs.FileMode) error { return nil }
func (f *fakeHost) Exists(path string) bool            { _, ok := f.files[path]; return ok }
func (f *fakeHost) Executable() (string, error)        { return f.exe, nil }
func (f *fakeHost) Hostname() (string, error)          { return f.hostname, nil }
func (f *fakeHost) Root() bool                         { return f.root }
func (f *fakeHost) Addresses() ([]netip.Addr, error) {
	var addrs []netip.Addr
	for _, a := range f.addrs {
		addrs = append(addrs, netip.MustParseAddr(a))
	}
	return addrs, nil
}

func TestInspectFindsWhatTheHostHas(t *testing.T) {
	facts := Inspect(context.Background(), newFakeHost())
	if !facts.Root || !facts.Systemd || facts.Docker != "1.51" || !facts.Nftables || !facts.ZFS || !slices.Equal(facts.Pools, []string{"tank"}) || !facts.CgroupV2 || facts.Installed {
		t.Fatalf("facts %+v", facts)
	}
	if len(facts.Private) != 1 || facts.Private[0].String() != "10.0.0.2" || len(facts.Public) != 1 || facts.Public[0].String() != "203.0.113.10" {
		t.Fatalf("addresses: private %v, public %v", facts.Private, facts.Public)
	}
	if problems := facts.Problems(); len(problems) != 0 {
		t.Fatalf("problems on a ready host: %v", problems)
	}
	bare := newFakeHost()
	bare.root = false
	delete(bare.commands, "nft --version")
	bare.commands["docker version --format {{.Server.APIVersion}}"] = "1.43"
	delete(bare.files, "/sys/fs/cgroup/cgroup.controllers")
	bare.commands["ss -H -ltnp ( sport = :80 or sport = :443 )"] = "LISTEN 0 511 0.0.0.0:80 0.0.0.0:* users:((\"nginx\",pid=1,fd=6))\nLISTEN 0 4096 *:443 *:* users:((\"murder\",pid=2,fd=8))\n"
	bareFacts := Inspect(context.Background(), bare)
	problems := strings.Join(bareFacts.Problems(), "\n")
	for _, want := range []string{"as root", "Docker's API is 1.43", "apt install nftables", "something else listens on 0.0.0.0:80"} {
		if !strings.Contains(problems, want) {
			t.Errorf("problems lack %q:\n%s", want, problems)
		}
	}
	if strings.Contains(problems, "*:443") || strings.Contains(problems, "cgroup") {
		t.Errorf("problems name Murder's own edge or cgroups:\n%s", problems)
	}
	if warnings := bareFacts.Warnings(); len(warnings) != 1 || !strings.Contains(warnings[0], "cgroup v2") {
		t.Errorf("warnings %v", warnings)
	}
}

// Every config setup writes is one the agent accepts, as setup meant it.
func TestTheConfigIsOneTheAgentAccepts(t *testing.T) {
	facts := Inspect(context.Background(), newFakeHost())
	for name, opts := range map[string]Options{
		"alone":     {Domain: "example.com", ZFSPool: "none", ClusterAddress: "none", TLS: "off"},
		"internal":  {Domain: "example.com"},
		"acme":      {Domain: "example.com", DNSProvider: "hetzner", HetznerToken: "tok", Email: "ops@example.com", ManageDNS: true},
		"namecheap": {Domain: "example.com", DNSProvider: "namecheap", DNSToken: "user:key", Email: "ops@example.com", ClusterAddress: "10.0.0.2:7500"},
	} {
		t.Run(name, func(t *testing.T) {
			s, problems := Settle(opts, facts)
			if len(problems) > 0 {
				t.Fatalf("problems: %v", problems)
			}
			agent, err := config.Parse(Config(s))
			if err != nil {
				t.Fatalf("%v\n%s", err, Config(s))
			}
			if agent.Node != "fsn-1" || agent.BaseDomain != "example.com" || agent.Edge.TLS.Mode != s.TLS {
				t.Fatalf("agent %+v", agent)
			}
			if (s.ZFSPool != "") != (agent.Volumes.Driver == "zfs") || (s.ClusterAddress != "") != (agent.Cluster != nil) || s.Panel != (agent.Panel.Tunnel != nil) {
				t.Fatalf("settings %+v gave volumes %+v, cluster %+v, panel %+v", s, agent.Volumes, agent.Cluster, agent.Panel)
			}
			if s.Failover != (agent.Cluster != nil && agent.Cluster.Failover != nil) || s.ManageDNS != (agent.Cluster != nil && agent.Cluster.DNS != nil) {
				t.Fatalf("failover %v, dns %v in %s", s.Failover, s.ManageDNS, Config(s))
			}
		})
	}
}

func TestSettleTakesWhatTheHostHas(t *testing.T) {
	facts := Inspect(context.Background(), newFakeHost())
	s, problems := Settle(Options{Domain: "Example.COM", DNSProvider: "hetzner", HetznerToken: " tok\n", Email: "ops@example.com"}, facts)
	if len(problems) > 0 {
		t.Fatal(problems)
	}
	want := Settings{Domain: "example.com", Node: "fsn-1", TLS: "acme", Email: "ops@example.com", DNSProvider: "hetzner", DNSToken: "tok",
		ZFSPool: "tank", ClusterAddress: "10.0.0.2:7400", Public: []string{"203.0.113.10"}, HetznerToken: "tok", Failover: true, Panel: true, TunnelAddress: "10.77.0.1"}
	if !equalSettings(s, want) {
		t.Fatalf("settings\n%+v, want\n%+v", s, want)
	}
	// Without a DNS token, certificates come from a local CA; without a
	// private network there is no cluster, so no failover and no panel.
	alone := facts
	alone.Private = nil
	s, _ = Settle(Options{Domain: "example.com", HetznerToken: "tok"}, alone)
	if s.TLS != "internal" || s.ClusterAddress != "" || s.Failover || s.Panel {
		t.Fatalf("alone: %+v", s)
	}
}

func equalSettings(a, b Settings) bool {
	return a.Domain == b.Domain && a.Node == b.Node && a.TLS == b.TLS && a.Email == b.Email && a.DNSProvider == b.DNSProvider &&
		a.DNSToken == b.DNSToken && a.ZFSPool == b.ZFSPool && a.ClusterAddress == b.ClusterAddress && slices.Equal(a.Public, b.Public) &&
		a.HetznerToken == b.HetznerToken && a.Failover == b.Failover && a.ManageDNS == b.ManageDNS && a.Panel == b.Panel && a.TunnelAddress == b.TunnelAddress
}

func TestSettleSaysWhatItCannotChoose(t *testing.T) {
	facts := Inspect(context.Background(), newFakeHost())
	many := facts
	many.Pools = []string{"tank", "fast"}
	many.Private = append(many.Private, netip.MustParseAddr("10.1.0.2"))
	noZFS := facts
	noZFS.ZFS, noZFS.Pools = false, nil
	for name, c := range map[string]struct {
		opts  Options
		facts Facts
		want  string
	}{
		"domain":          {Options{Domain: "not a domain"}, facts, "not a domain name"},
		"pools":           {Options{Domain: "example.com"}, many, "several ZFS pools"},
		"addresses":       {Options{Domain: "example.com", ZFSPool: "tank"}, many, "several private addresses"},
		"no zfs":          {Options{Domain: "example.com"}, noZFS, "apt install zfsutils-linux"},
		"unknown pool":    {Options{Domain: "example.com", ZFSPool: "fast"}, facts, `no ZFS pool "fast"`},
		"acme no dns":     {Options{Domain: "example.com", TLS: "acme"}, facts, "-dns-provider"},
		"acme no email":   {Options{Domain: "example.com", DNSProvider: "hetzner", HetznerToken: "t"}, facts, "-email"},
		"dns alone":       {Options{Domain: "example.com", ClusterAddress: "none", ManageDNS: true, DNSProvider: "hetzner", HetznerToken: "t", Email: "a@b.c"}, facts, "needs a cluster"},
		"cluster address": {Options{Domain: "example.com", ClusterAddress: "here"}, facts, "an IP address"},
		"tls":             {Options{Domain: "example.com", TLS: "maybe"}, facts, "acme, internal or off"},
	} {
		_, problems := Settle(c.opts, c.facts)
		if !strings.Contains(strings.Join(problems, "\n"), c.want) {
			t.Errorf("%s: problems %v lack %q", name, problems, c.want)
		}
	}
}

func TestStepsInstallEverything(t *testing.T) {
	host := newFakeHost()
	host.files["/tmp/murder"] = []byte("binary")
	s, _ := Settle(Options{Domain: "example.com", DNSProvider: "hetzner", HetznerToken: "hcloud-s3cret", Email: "ops@example.com"}, Inspect(context.Background(), host))
	initialized := false
	steps, err := Steps(host, s.Install(), false, func() error { initialized = true; return nil })
	if err != nil {
		t.Fatal(err)
	}
	for _, step := range steps {
		if err := step.Do(context.Background()); err != nil {
			t.Fatalf("%s: %v", step.Say, err)
		}
	}
	if !initialized {
		t.Fatal("the master key and database were not made")
	}
	for path, mode := range map[string]fs.FileMode{
		Binary: 0o755, ConfigFile: 0o644, HetznerTokenFile: 0o600, SysctlFile: 0o644, DockerDropIn: 0o644,
		UnitDir + "/murder-agent.service": 0o644, UnitDir + "/murder-panel.service": 0o644, UnitDir + "/murder-backup.timer": 0o644,
	} {
		if _, ok := host.files[path]; !ok || host.modes[path] != mode {
			t.Errorf("%s: written %v, mode %v, want %v", path, ok, host.modes[path], mode)
		}
	}
	if string(host.files[HetznerTokenFile]) != "hcloud-s3cret\n" || strings.Contains(string(host.files[ConfigFile]), "s3cret") {
		t.Fatal("the token is not kept in its own file")
	}
	ran := strings.Join(host.ran, "\n")
	for _, want := range []string{"zfs create -p tank/murder/tenants", "systemctl daemon-reload", "systemctl enable --now murder-firewall.service murder-agent.service murder-edge.service murder-backup.timer murder-panel.service"} {
		if !strings.Contains(ran, want) {
			t.Errorf("did not run %q:\n%s", want, ran)
		}
	}
	// Run again, it keeps the config it wrote.
	again, _ := Steps(host, s.Install(), false, func() error { return nil })
	for _, step := range again {
		if err := step.Do(context.Background()); err != nil {
			if !strings.Contains(err.Error(), "set up already") {
				t.Fatalf("%s: %v", step.Say, err)
			}
			return
		}
	}
	t.Fatal("setup wrote over an existing config")
}

func TestForceRestartsWhatRuns(t *testing.T) {
	host := newFakeHost()
	host.exe = Binary
	s, _ := Settle(Options{Domain: "example.com", ZFSPool: "none", ClusterAddress: "none"}, Inspect(context.Background(), host))
	host.files[ConfigFile] = []byte("old")
	steps, _ := Steps(host, s.Install(), true, func() error { return nil })
	for _, step := range steps {
		if err := step.Do(context.Background()); err != nil {
			t.Fatalf("%s: %v", step.Say, err)
		}
	}
	ran := strings.Join(host.ran, "\n")
	if !strings.Contains(ran, "systemctl try-reload-or-restart murder-edge.service") || !strings.Contains(ran, "systemctl try-restart murder-firewall.service murder-agent.service") {
		t.Fatalf("ran:\n%s", ran)
	}
	if string(host.files[ConfigFile]) == "old" {
		t.Fatal("-force kept the old config")
	}
}

func TestPlainDirectoriesNeedNoZFS(t *testing.T) {
	host := newFakeHost()
	host.exe = Binary
	s, problems := Settle(Options{Domain: "example.com", ZFSPool: "none", ClusterAddress: "none"}, Inspect(context.Background(), host))
	if len(problems) > 0 {
		t.Fatal(problems)
	}
	steps, _ := Steps(host, s.Install(), false, func() error { return nil })
	for _, step := range steps {
		if strings.Contains(step.Say, "install /") || strings.Contains(step.Say, "ZFS") {
			t.Errorf("step %q", step.Say)
		}
		if err := step.Do(context.Background()); err != nil {
			t.Fatalf("%s: %v", step.Say, err)
		}
	}
	if _, ok := host.files[DockerDropIn]; ok {
		t.Fatal("the ZFS drop-in was installed without ZFS")
	}
	if strings.Contains(strings.Join(host.ran, "\n"), "murder-panel") {
		t.Fatal("the panel was started without a cluster")
	}
}
