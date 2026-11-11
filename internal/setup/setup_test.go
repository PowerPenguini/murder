package setup

import (
	"context"
	"encoding/json"
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
	// connection is SSH_CONNECTION.
	connection string
}

func newFakeHost() *fakeHost {
	return &fakeHost{
		commands: map[string]string{
			"systemctl --version":                                                    "systemd 255",
			"docker version --format {{.Server.APIVersion}}":                         "1.51\n",
			"docker info --format {{.LiveRestoreEnabled}} {{.Swarm.LocalNodeState}}": "false inactive\n",
			"systemctl reload docker":                                                "",
			"nft --version":                                                          "nftables v1.0.9",
			"zpool list -H -o name":                                                  "tank\n",
			"sysctl -q -p " + SysctlFile:                                             "",
			"sysctl -q -e -p " + HardeningFile:                                       "",
			"sysctl -q -e -p " + SysctlFile:                                          "",
			"systemctl daemon-reload":                                                "",
			"zfs create -p tank/murder/tenants":                                      "",
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

func (f *fakeHost) Remove(path string) error {
	if _, ok := f.files[path]; !ok {
		return fs.ErrNotExist
	}
	delete(f.files, path)
	return nil
}

func (f *fakeHost) MkdirAll(string, fs.FileMode) error { return nil }
func (f *fakeHost) Exists(path string) bool            { _, ok := f.files[path]; return ok }
func (f *fakeHost) Executable() (string, error)        { return f.exe, nil }
func (f *fakeHost) Hostname() (string, error)          { return f.hostname, nil }
func (f *fakeHost) Root() bool                         { return f.root }
func (f *fakeHost) Invoker() string                    { return "ubuntu" }
func (f *fakeHost) Connection() string                 { return f.connection }
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
			// The agent finds the node's own settings on the host: the
			// config names none of them, and node.yaml only one it would
			// not find.
			agent, err := config.ParseOn(Config(s), s.NodeConfig, factsHost{facts})
			if err != nil {
				t.Fatalf("%v\n%s\n%s", err, Config(s), s.NodeConfig)
			}
			for _, own := range []string{"node:", "address:", "publicAddresses", "parent:", "ssh:"} {
				if strings.Contains(string(Config(s)), own) {
					t.Fatalf("the shared config names %s:\n%s", own, Config(s))
				}
			}
			if wantOwn := opts.ClusterAddress == "10.0.0.2:7500"; wantOwn != (s.NodeConfig != nil) || wantOwn && !strings.HasSuffix(string(s.NodeConfig), "\ncluster:\n  address: 10.0.0.2:7500\n") {
				t.Fatalf("node.yaml:\n%s", s.NodeConfig)
			}
			if agent.Node != "fsn-1" || agent.BaseDomain != "example.com" || agent.Edge.TLS.Mode != s.TLS {
				t.Fatalf("agent %+v", agent)
			}
			if (s.ZFSPool != "") != (agent.Volumes.Driver == "zfs") || (s.ClusterAddress != "") != (agent.Cluster != nil) || s.Panel != (agent.Panel.Tunnel != nil) {
				t.Fatalf("settings %+v gave volumes %+v, cluster %+v, panel %+v", s, agent.Volumes, agent.Cluster, agent.Panel)
			}
			// A node alone keeps its public addresses for its tenants'
			// domains; a node of a cluster keeps them under cluster.
			if alone := agent.Cluster == nil; alone != slices.Equal(agent.PublicAddresses, s.Public) || !alone && !slices.Equal(agent.Cluster.PublicAddresses, s.Public) {
				t.Fatalf("public %v gave %s", s.Public, Config(s))
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
		ZFSPool: "tank", ClusterAddress: "10.0.0.2:7400", Public: []string{"203.0.113.10"}, HetznerToken: "tok", Failover: true, Panel: true}
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
		a.HetznerToken == b.HetznerToken && a.Failover == b.Failover && a.ManageDNS == b.ManageDNS && a.Panel == b.Panel
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
		Binary: 0o755, ConfigFile: 0o644, HetznerTokenFile: 0o600, SysctlFile: 0o644, HardeningFile: 0o644, DockerDropIn: 0o644,
		UnitDir + "/murder-agent.service": 0o644, UnitDir + "/murder-panel.service": 0o644, UnitDir + "/murder-backup.timer": 0o644,
		UnitDir + "/murder-edge.socket": 0o644, EdgeSocketDropIn: 0o644,
	} {
		if _, ok := host.files[path]; !ok || host.modes[path] != mode {
			t.Errorf("%s: written %v, mode %v, want %v", path, ok, host.modes[path], mode)
		}
	}
	// The cluster's relay is held by the socket too, from its first start.
	if dropIn := string(host.files[EdgeSocketDropIn]); !strings.HasSuffix(dropIn, "ListenStream=\nListenStream=80\nListenStream=443\nListenStream=10.0.0.2:7402\n") {
		t.Errorf("socket drop-in %q", dropIn)
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
	host.files[NodeFile] = []byte("node: old\n")
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
	// A node.yaml of the earlier setup would be read over the new config.
	if _, ok := host.files[NodeFile]; ok {
		t.Fatal("-force kept the old node.yaml")
	}
}

// A host whose own settings the agent would not find all by itself gets
// node.yaml beside the shared config, with only those.
func TestSetupWritesWhatTheHostCannotTell(t *testing.T) {
	host := newFakeHost()
	host.exe = Binary
	host.addrs = append(host.addrs, "192.168.5.2")
	host.commands["zpool list -H -o name"] = "tank\nfast\n"
	host.commands["zfs create -p fast/murder/tenants"] = ""
	s, problems := Settle(Options{Domain: "example.com", Node: "edge-1", ZFSPool: "fast", ClusterAddress: "192.168.5.2"}, Inspect(context.Background(), host))
	if len(problems) > 0 {
		t.Fatal(problems)
	}
	want := "node: edge-1\ncluster:\n  address: 192.168.5.2:7400\nvolumes:\n  parent: fast/murder/tenants\n"
	if !strings.HasSuffix(string(s.NodeConfig), want) {
		t.Fatalf("node.yaml\n%s\nwant it to end in\n%s", s.NodeConfig, want)
	}
	steps, _ := Steps(host, s.Install(), false, func() error { return nil })
	for _, step := range steps {
		if err := step.Do(context.Background()); err != nil {
			t.Fatalf("%s: %v", step.Say, err)
		}
	}
	if string(host.files[NodeFile]) != string(s.NodeConfig) || host.modes[NodeFile] != 0o644 || strings.Contains(string(host.files[ConfigFile]), "edge-1") {
		t.Fatalf("node.yaml %q, config\n%s", host.files[NodeFile], host.files[ConfigFile])
	}
	// The relay listens on the address node.yaml names.
	if dropIn := string(host.files[EdgeSocketDropIn]); !strings.Contains(dropIn, "ListenStream=192.168.5.2:7402") {
		t.Fatalf("socket drop-in %q", dropIn)
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
	if _, ok := host.files[EdgeSocketDropIn]; ok {
		t.Fatal("the socket got a drop-in for its own addresses")
	}
	if strings.Contains(strings.Join(host.ran, "\n"), "murder-panel") {
		t.Fatal("the panel was started without a cluster")
	}
}

func TestSetupTurnsLiveRestoreOn(t *testing.T) {
	host := newFakeHost()
	facts := Inspect(context.Background(), host)
	if !facts.WantsLiveRestore() {
		t.Fatalf("facts %+v want no live-restore", facts)
	}
	host.files[DaemonConfig] = []byte(`{"log-driver": "json-file", "log-opts": {"max-size": "10m"}}`)
	if err := liveRestore(context.Background(), host); err != nil {
		t.Fatal(err)
	}
	var settings map[string]any
	if err := json.Unmarshal(host.files[DaemonConfig], &settings); err != nil || settings["live-restore"] != true || settings["log-driver"] != "json-file" || settings["log-opts"] == nil {
		t.Fatalf("daemon.json %s, %v", host.files[DaemonConfig], err)
	}
	if !strings.Contains(string(host.files[DaemonConfig+".before-murder"]), "max-size") || !slices.Contains(host.ran, "systemctl reload docker") {
		t.Fatalf("backup %q, ran %v", host.files[DaemonConfig+".before-murder"], host.ran)
	}
	// Root alone reads them: they may hold credentials.
	if host.modes[DaemonConfig] != 0o600 || host.modes[DaemonConfig+".before-murder"] != 0o600 {
		t.Fatalf("modes %v", host.modes)
	}
	// A file setup cannot read is left as it is, and setup goes on.
	var skipped Skipped
	for _, broken := range []string{"{ not json", "[1, 2]"} {
		host.files[DaemonConfig] = []byte(broken)
		if err := liveRestore(context.Background(), host); !errors.As(err, &skipped) || !strings.Contains(err.Error(), "by hand") || string(host.files[DaemonConfig]) != broken {
			t.Fatalf("daemon.json %q: %v", broken, err)
		}
	}
	// "null" is no settings.
	host.files[DaemonConfig] = []byte("null")
	if err := liveRestore(context.Background(), host); err != nil || !strings.Contains(string(host.files[DaemonConfig]), `"live-restore": true`) {
		t.Fatalf("a null daemon.json %q, %v", host.files[DaemonConfig], err)
	}
	// No file yet: one is written.
	delete(host.files, DaemonConfig)
	if err := liveRestore(context.Background(), host); err != nil || !strings.Contains(string(host.files[DaemonConfig]), `"live-restore": true`) {
		t.Fatalf("a new daemon.json %q, %v", host.files[DaemonConfig], err)
	}
	// On already, or in swarm mode, setup leaves Docker alone.
	host.commands["docker info --format {{.LiveRestoreEnabled}} {{.Swarm.LocalNodeState}}"] = "true inactive"
	if Inspect(context.Background(), host).WantsLiveRestore() {
		t.Fatal("live-restore is on already")
	}
	host.commands["docker info --format {{.LiveRestoreEnabled}} {{.Swarm.LocalNodeState}}"] = "false active"
	swarm := Inspect(context.Background(), host)
	if swarm.WantsLiveRestore() || !strings.Contains(strings.Join(swarm.Warnings(), "\n"), "swarm mode") {
		t.Fatalf("swarm: wants %v, warnings %v", swarm.WantsLiveRestore(), swarm.Warnings())
	}
}
