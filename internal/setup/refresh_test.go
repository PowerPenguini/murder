package setup

import (
	"context"
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
	edge, agent := Target("murder-edge.service"), Target("murder-agent.service")
	current, err := deploy.Files.ReadFile("murder-agent.service")
	if err != nil {
		t.Fatal(err)
	}
	host.files[edge] = []byte("[Service]\nExecStart=/usr/local/bin/murder edge\n") // an older unit
	host.files[agent] = current
	host.files[SysctlFile] = []byte("net.ipv4.tcp_slow_start_after_idle = 1\n")
	replaced, err := Refresh(context.Background(), host)
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
	for _, missing := range []string{HardeningFile, DockerDropIn, Target("murder-panel.service")} {
		if host.Exists(missing) {
			t.Errorf("%s installed, though the node had none", missing)
		}
	}
	if ran := strings.Join(host.ran, "\n"); ran != "systemctl daemon-reload\nsysctl -q -e -p "+SysctlFile {
		t.Fatalf("ran:\n%s", ran)
	}
	// Up to date, nothing is written or run.
	host.ran = nil
	if replaced, err := Refresh(context.Background(), host); err != nil || len(replaced) != 0 || len(host.ran) != 0 {
		t.Fatalf("again: replaced %v, ran %v, %v", replaced, host.ran, err)
	}
}
