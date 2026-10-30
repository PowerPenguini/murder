package upgrade

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"strings"
	"testing"
	"time"
)

// fakeBinary is a script that answers "version -json" like murder would.
func fakeBinary(schema int, tag string) []byte {
	return []byte(fmt.Sprintf("#!/bin/sh\n# %s\necho '{\"binary\":\"%s\",\"schema\":%d}'\n", tag, tag, schema))
}

func sha(data []byte) string {
	sum := sha256.Sum256(data)
	return hex.EncodeToString(sum[:])
}

type fixture struct {
	node       *Node
	dir        string
	restarted  int
	snapshotOf string
}

func newFixture(t *testing.T) *fixture {
	t.Helper()
	dir := t.TempDir()
	f := &fixture{dir: dir}
	executable := filepath.Join(dir, "murder")
	if err := os.WriteFile(executable, fakeBinary(5, "old"), 0o755); err != nil {
		t.Fatal(err)
	}
	database := filepath.Join(dir, "murder.db")
	f.node = &Node{
		Executable: executable, Current: Version{Binary: "old", Schema: 5},
		EdgePIDFile: filepath.Join(dir, "edge.pid"), Database: database,
		Snapshot: func() (string, error) {
			path := filepath.Join(dir, "copy")
			return path, os.WriteFile(path, []byte("database copy"), 0o600)
		},
		Restart:  func() { f.restarted++ },
		Log:      slog.New(slog.NewTextHandler(io.Discard, nil)),
		edgeWait: 300 * time.Millisecond,
	}
	return f
}

func TestInstallKeepsWhatARollbackNeeds(t *testing.T) {
	f := newFixture(t)
	binary := fakeBinary(6, "new")
	// No edge runs here: nothing to hand over.
	if err := f.node.Install(context.Background(), bytes.NewReader(binary), sha(binary)); err != nil {
		t.Fatal(err)
	}
	installed, _ := os.ReadFile(f.node.Executable)
	previous, _ := os.ReadFile(f.node.Executable + ".prev")
	copied, _ := os.ReadFile(f.node.Database + ".before-upgrade")
	if !bytes.Equal(installed, binary) || !bytes.Equal(previous, fakeBinary(5, "old")) || string(copied) != "database copy" || f.restarted != 1 {
		t.Fatalf("installed %q, kept %q, copied %q, restarted %d", installed, previous, copied, f.restarted)
	}
	if info, err := os.Stat(f.node.Executable); err != nil || info.Mode().Perm() != 0o755 {
		t.Fatalf("mode %v, %v", info.Mode(), err)
	}
}

func TestInstallRefusesWhatItCannotRun(t *testing.T) {
	for _, test := range []struct {
		name   string
		binary []byte
		sha    string
		want   string
	}{
		{"another hash", fakeBinary(6, "new"), sha([]byte("something else")), "arrived with SHA-256"},
		{"an older schema", fakeBinary(4, "older"), "", "older schema"},
		{"not murder", []byte("#!/bin/sh\necho hello\n"), "", "not a murder binary"},
		{"not runnable", []byte("\x7fELF garbage"), "", "version"},
	} {
		t.Run(test.name, func(t *testing.T) {
			f := newFixture(t)
			sum := test.sha
			if sum == "" {
				sum = sha(test.binary)
			}
			err := f.node.Install(context.Background(), bytes.NewReader(test.binary), sum)
			if err == nil || !strings.Contains(err.Error(), test.want) {
				t.Fatalf("install: %v", err)
			}
			installed, _ := os.ReadFile(f.node.Executable)
			if !bytes.Equal(installed, fakeBinary(5, "old")) || f.restarted != 0 {
				t.Fatal("a refused binary replaced the running one")
			}
			if errors.Is(err, ErrDowngrade) != (test.name == "an older schema") {
				t.Fatalf("downgrade error: %v", err)
			}
		})
	}
}

func TestAnEdgeThatDoesNotTakeOverLeavesTheOldBinary(t *testing.T) {
	f := newFixture(t)
	// An "edge" that dies on SIGHUP and never names a successor.
	edge := exec.Command("sleep", "30")
	if err := edge.Start(); err != nil {
		t.Fatal(err)
	}
	defer edge.Process.Kill()
	go func() { _ = edge.Wait() }()
	if err := os.WriteFile(f.node.EdgePIDFile, []byte(strconv.Itoa(edge.Process.Pid)), 0o644); err != nil {
		t.Fatal(err)
	}
	binary := fakeBinary(6, "new")
	err := f.node.Install(context.Background(), bytes.NewReader(binary), sha(binary))
	if err == nil || !strings.Contains(err.Error(), "did not hand over") {
		t.Fatalf("install: %v", err)
	}
	installed, _ := os.ReadFile(f.node.Executable)
	if !bytes.Equal(installed, fakeBinary(5, "old")) || f.restarted != 0 {
		t.Fatal("the new binary stayed though the edge did not take it")
	}
}

func TestAnEdgeThatTakesOverLetsTheAgentRestart(t *testing.T) {
	f := newFixture(t)
	// An "edge" that, on SIGHUP, writes another PID, as a new edge would.
	script := filepath.Join(f.dir, "edge.sh")
	if err := os.WriteFile(script, []byte("#!/bin/sh\ntrap 'echo 1 > "+f.node.EdgePIDFile+"' HUP\necho $$ > "+f.node.EdgePIDFile+"\nwhile true; do sleep 0.05; done\n"), 0o755); err != nil {
		t.Fatal(err)
	}
	edge := exec.Command(script)
	if err := edge.Start(); err != nil {
		t.Fatal(err)
	}
	defer edge.Process.Kill()
	for range 100 {
		if pid, err := readPID(f.node.EdgePIDFile); err == nil && pid == edge.Process.Pid {
			break
		}
		time.Sleep(20 * time.Millisecond)
	}
	f.node.edgeWait = 5 * time.Second
	binary := fakeBinary(6, "new")
	if err := f.node.Install(context.Background(), bytes.NewReader(binary), sha(binary)); err != nil {
		t.Fatal(err)
	}
	if f.restarted != 1 {
		t.Fatal("the agent was not restarted")
	}
}
