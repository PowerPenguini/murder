package setup

import (
	"context"
	"errors"
	"io/fs"
	"slices"
	"strings"
	"testing"

	"github.com/PowerPenguini/murder/deploy"
	"github.com/PowerPenguini/murder/internal/config"
	"github.com/PowerPenguini/murder/internal/edge"
)

// A node installed by an older release has its units and sysctl settings
// brought to this one's; files it never had stay out, and a node already
// up to date is left alone.
func TestRefreshBringsTheInstallFilesUpToThisRelease(t *testing.T) {
	host := newFakeHost()
	host.commands["systemctl show --property NeedDaemonReload --value murder-agent.service murder-edge.service murder-edge.socket"] = "no\nno\nno\n"
	edge, agent := Target("murder-edge.service"), Target("murder-agent.service")
	current, err := deploy.Files.ReadFile("murder-agent.service")
	if err != nil {
		t.Fatal(err)
	}
	host.files[edge] = []byte("[Service]\nExecStart=/usr/local/bin/murder edge\n") // an older unit
	host.files[agent] = current
	host.files[SysctlFile] = []byte("net.ipv4.tcp_slow_start_after_idle = 1\n")
	replaced, err := Refresh(context.Background(), host, Edge{})
	if err != nil {
		t.Fatal(err)
	}
	// murder-edge.socket came with this release, beside the edge's unit.
	if !slices.Equal(replaced, []string{edge, Target("murder-edge.socket"), SysctlFile}) {
		t.Fatalf("replaced %v", replaced)
	}
	want, _ := deploy.Files.ReadFile("murder-edge.service")
	if string(host.files[edge]) != string(want) || host.modes[edge] != 0o644 || !strings.Contains(string(host.files[edge]), "ProtectSystem=strict") {
		t.Fatalf("edge unit not replaced: %s", host.files[edge])
	}
	for _, missing := range []string{HardeningFile, DockerDropIn, Target("murder-panel.service"), EdgePathsDropIn} {
		if host.Exists(missing) {
			t.Errorf("%s installed, though the node had none", missing)
		}
	}
	if ran := strings.Join(host.ran, "\n"); ran != "sysctl -q -p "+SysctlFile+"\nsystemctl daemon-reload" {
		t.Fatalf("ran:\n%s", ran)
	}
	// Up to date, nothing is written; systemd is asked whether a reload is
	// still due, and it is not.
	host.ran = nil
	if replaced, err := Refresh(context.Background(), host, Edge{}); err != nil || len(replaced) != 0 || len(host.ran) != 1 || !strings.HasPrefix(host.ran[0], "systemctl show") {
		t.Fatalf("again: replaced %v, ran %v, %v", replaced, host.ran, err)
	}
	// A daemon-reload that failed at an earlier start is done now.
	host.ran = nil
	host.commands["systemctl show --property NeedDaemonReload --value murder-agent.service murder-edge.service murder-edge.socket"] = "no\nyes\nno\n"
	if _, err := Refresh(context.Background(), host, Edge{}); err != nil || !slices.Contains(host.ran, "systemctl daemon-reload") {
		t.Fatalf("due reload: ran %v, %v", host.ran, err)
	}
}

// A file that cannot be written does not keep the others from being
// written and taken.
func TestRefreshGoesOnPastAFileItCannotWrite(t *testing.T) {
	host := &failingHost{fakeHost: newFakeHost(), fail: Target("murder-agent.service")}
	host.files[Target("murder-agent.service")] = []byte("old")
	host.files[Target("murder-edge.service")] = []byte("old")
	replaced, err := Refresh(context.Background(), host, Edge{})
	if err == nil || !slices.Equal(replaced, []string{Target("murder-edge.service"), Target("murder-edge.socket")}) || !slices.Contains(host.ran, "systemctl daemon-reload") {
		t.Fatalf("replaced %v, ran %v, %v", replaced, host.ran, err)
	}
}

type failingHost struct {
	*fakeHost
	fail string
}

func (f *failingHost) WriteFile(path string, data []byte, perm fs.FileMode) error {
	if path == f.fail {
		return errors.New("read-only")
	}
	return f.fakeHost.WriteFile(path, data, perm)
}

// The confined edge gets a drop-in for the directories its config moves
// out of its own, and it follows the config back.
func TestRefreshLetsTheEdgeWriteWhereItsConfigSays(t *testing.T) {
	if dirs := EdgeWritable("/var/lib/murder", "/var/lib/murder/certs", "/run/murder-edge", "/srv/certs", "/srv/certs/", "relative", "/var/lib/murderous"); !slices.Equal(dirs, []string{"/srv/certs", "/var/lib/murderous"}) {
		t.Fatalf("writable %v", dirs)
	}
	host := newFakeHost()
	host.files[Target("murder-edge.service")], _ = deploy.Files.ReadFile("murder-edge.service")
	host.files[Target("murder-edge.socket")], _ = deploy.Files.ReadFile("murder-edge.socket")
	host.commands["systemctl show --property NeedDaemonReload --value murder-edge.service murder-edge.socket"] = "no\nno\n"
	replaced, err := Refresh(context.Background(), host, Edge{Writable: []string{"/srv/certs"}})
	if err != nil || !slices.Equal(replaced, []string{EdgePathsDropIn}) || !strings.Contains(string(host.files[EdgePathsDropIn]), "\nReadWritePaths=/srv/certs\n") || !slices.Contains(host.ran, "systemctl daemon-reload") {
		t.Fatalf("replaced %v, ran %v, drop-in %q, %v", replaced, host.ran, host.files[EdgePathsDropIn], err)
	}
	// Back in /var/lib/murder, the drop-in names none.
	if _, err := Refresh(context.Background(), host, Edge{}); err != nil || strings.Contains(string(host.files[EdgePathsDropIn]), "ReadWritePaths") {
		t.Fatalf("drop-in %q, %v", host.files[EdgePathsDropIn], err)
	}
}

// murder-edge.socket holds the addresses the edge's config names: its own
// (80 and 443 on all addresses) need no drop-in; others, as TLS off or a
// cluster's relay, get one, which follows the config back.
func TestRefreshHasTheEdgeSocketHoldTheConfigsAddresses(t *testing.T) {
	cfg, err := config.Parse([]byte("node: a\nbaseDomain: example.com\n"))
	if err != nil {
		t.Fatal(err)
	}
	cfg.Edge.TLS.Mode = edge.TLSACME
	if listen := EdgeListen(cfg); !slices.Equal(listen, edgeSocketListen()) || !slices.Equal(listen, []string{"80", "443"}) {
		t.Fatalf("default listen %v, unit %v", listen, edgeSocketListen())
	}
	cfg.Edge.HTTP, cfg.Edge.HTTPS = "203.0.113.10:http", "[2001:db8::1]:8443"
	cfg.Cluster = &config.Cluster{Address: "10.0.0.2:7400"}
	cfg.Edge.Metrics = "127.0.0.1:9101"
	if listen := EdgeListen(cfg); !slices.Equal(listen, []string{"203.0.113.10:80", "[2001:db8::1]:8443", "10.0.0.2:7402"}) {
		t.Fatalf("listen %v", listen)
	}
	cfg, _ = config.Parse([]byte("node: a\nbaseDomain: example.com\n"))
	cfg.Edge.HTTP = "edge.example.com:80" // a name, left to the edge
	if listen := EdgeListen(cfg); len(listen) != 0 {
		t.Fatalf("listen %v", listen)
	}

	host := newFakeHost()
	host.files[Target("murder-edge.service")], _ = deploy.Files.ReadFile("murder-edge.service")
	host.files[Target("murder-edge.socket")], _ = deploy.Files.ReadFile("murder-edge.socket")
	host.commands["systemctl show --property NeedDaemonReload --value murder-edge.service murder-edge.socket"] = "no\nno\n"
	if replaced, err := Refresh(context.Background(), host, Edge{Listen: []string{"80", "443"}}); err != nil || len(replaced) != 0 || host.Exists(EdgeSocketDropIn) {
		t.Fatalf("the unit's own addresses: replaced %v, %v", replaced, err)
	}
	replaced, err := Refresh(context.Background(), host, Edge{Listen: []string{"80"}}) // TLS off
	if err != nil || !slices.Equal(replaced, []string{EdgeSocketDropIn}) || !slices.Contains(host.ran, "systemctl daemon-reload") {
		t.Fatalf("replaced %v, ran %v, %v", replaced, host.ran, err)
	}
	if dropIn := string(host.files[EdgeSocketDropIn]); !strings.Contains(dropIn, "[Socket]\nListenStream=\nListenStream=80\n") || strings.Contains(dropIn, "443") {
		t.Fatalf("drop-in %q", dropIn)
	}
	// Back to the unit's own, the drop-in names them, rather than staying.
	if _, err := Refresh(context.Background(), host, Edge{Listen: []string{"80", "443"}}); err != nil || !strings.HasSuffix(string(host.files[EdgeSocketDropIn]), "ListenStream=\nListenStream=80\nListenStream=443\n") {
		t.Fatalf("drop-in %q, %v", host.files[EdgeSocketDropIn], err)
	}
	// Nil leaves it as it is.
	before := string(host.files[EdgeSocketDropIn])
	if replaced, err := Refresh(context.Background(), host, Edge{}); err != nil || len(replaced) != 0 || string(host.files[EdgeSocketDropIn]) != before {
		t.Fatalf("nil listen: replaced %v, %v", replaced, err)
	}
}
