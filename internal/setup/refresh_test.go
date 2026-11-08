package setup

import (
	"context"
	"errors"
	"io/fs"
	"slices"
	"strings"
	"testing"

	"github.com/PowerPenguini/murder/deploy"
)

// A node installed by an older release has its units and sysctl settings
// brought to this one's; files it never had stay out, and a node already
// up to date is left alone.
func TestRefreshBringsTheInstallFilesUpToThisRelease(t *testing.T) {
	host := newFakeHost()
	host.commands["systemctl show --property NeedDaemonReload --value murder-agent.service murder-edge.service"] = "no\nno\n"
	edge, agent := Target("murder-edge.service"), Target("murder-agent.service")
	current, err := deploy.Files.ReadFile("murder-agent.service")
	if err != nil {
		t.Fatal(err)
	}
	host.files[edge] = []byte("[Service]\nExecStart=/usr/local/bin/murder edge\n") // an older unit
	host.files[agent] = current
	host.files[SysctlFile] = []byte("net.ipv4.tcp_slow_start_after_idle = 1\n")
	replaced, err := Refresh(context.Background(), host, nil)
	if err != nil {
		t.Fatal(err)
	}
	if !slices.Equal(replaced, []string{edge, SysctlFile}) {
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
	if replaced, err := Refresh(context.Background(), host, nil); err != nil || len(replaced) != 0 || len(host.ran) != 1 || !strings.HasPrefix(host.ran[0], "systemctl show") {
		t.Fatalf("again: replaced %v, ran %v, %v", replaced, host.ran, err)
	}
	// A daemon-reload that failed at an earlier start is done now.
	host.ran = nil
	host.commands["systemctl show --property NeedDaemonReload --value murder-agent.service murder-edge.service"] = "no\nyes\n"
	if _, err := Refresh(context.Background(), host, nil); err != nil || !slices.Contains(host.ran, "systemctl daemon-reload") {
		t.Fatalf("due reload: ran %v, %v", host.ran, err)
	}
}

// A file that cannot be written does not keep the others from being
// written and taken.
func TestRefreshGoesOnPastAFileItCannotWrite(t *testing.T) {
	host := &failingHost{fakeHost: newFakeHost(), fail: Target("murder-agent.service")}
	host.files[Target("murder-agent.service")] = []byte("old")
	host.files[Target("murder-edge.service")] = []byte("old")
	replaced, err := Refresh(context.Background(), host, nil)
	if err == nil || !slices.Equal(replaced, []string{Target("murder-edge.service")}) || !slices.Contains(host.ran, "systemctl daemon-reload") {
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
	host.commands["systemctl show --property NeedDaemonReload --value murder-edge.service"] = "no\n"
	replaced, err := Refresh(context.Background(), host, []string{"/srv/certs"})
	if err != nil || !slices.Equal(replaced, []string{EdgePathsDropIn}) || !strings.Contains(string(host.files[EdgePathsDropIn]), "\nReadWritePaths=/srv/certs\n") || !slices.Contains(host.ran, "systemctl daemon-reload") {
		t.Fatalf("replaced %v, ran %v, drop-in %q, %v", replaced, host.ran, host.files[EdgePathsDropIn], err)
	}
	// Back in /var/lib/murder, the drop-in names none.
	if _, err := Refresh(context.Background(), host, nil); err != nil || strings.Contains(string(host.files[EdgePathsDropIn]), "ReadWritePaths") {
		t.Fatalf("drop-in %q, %v", host.files[EdgePathsDropIn], err)
	}
}
