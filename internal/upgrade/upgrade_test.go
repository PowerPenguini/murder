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
	"os/signal"
	"path/filepath"
	"strconv"
	"strings"
	"syscall"
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
		Executable: executable, Current: Version{Binary: sha(fakeBinary(5, "old")), Schema: 5},
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
	edge := exec.Command("/bin/sh", "-c", "sleep 30; true", "edge")
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
	if _, err := Rollback(f.node.Executable); !errors.Is(err, ErrNothingToRollBack) {
		t.Fatalf("a refused install left something to roll back: %v", err)
	}
}

func TestRollbackUndoesTheLastInstallOnly(t *testing.T) {
	f := newFixture(t)
	f.node.Current.Binary = sha(fakeBinary(5, "old"))
	if _, err := Rollback(f.node.Executable); !errors.Is(err, ErrNothingToRollBack) {
		t.Fatalf("rollback before any install: %v", err)
	}
	binary := fakeBinary(6, "new")
	if err := f.node.Install(context.Background(), bytes.NewReader(binary), sha(binary)); err != nil {
		t.Fatal(err)
	}
	record, err := Rollback(f.node.Executable)
	if err != nil {
		t.Fatal(err)
	}
	inPlace, _ := os.ReadFile(f.node.Executable)
	failed, _ := os.ReadFile(f.node.Executable + ".failed")
	if !bytes.Equal(inPlace, fakeBinary(5, "old")) || !bytes.Equal(failed, binary) || record.From.Schema != 5 || record.To.Schema != 6 {
		t.Fatalf("after rollback: %q in place, %q failed, record %+v", inPlace, failed, record)
	}
	// Done once: a second rollback has nothing to undo.
	if _, err := Rollback(f.node.Executable); !errors.Is(err, ErrNothingToRollBack) {
		t.Fatalf("a second rollback: %v", err)
	}
}

// A new binary may not run at all: the old one, kept as murder.prev, rolls
// back the one in place.
func TestTheOldBinaryRollsBackTheNewOne(t *testing.T) {
	f := newFixture(t)
	f.node.Current.Binary = sha(fakeBinary(5, "old"))
	previous := f.node.Executable + ".prev"
	if got := RollbackTarget(previous); got != previous {
		t.Fatalf("before any install, murder.prev rolls back %s", got)
	}
	binary := fakeBinary(6, "new")
	if err := f.node.Install(context.Background(), bytes.NewReader(binary), sha(binary)); err != nil {
		t.Fatal(err)
	}
	if got := RollbackTarget(previous); got != f.node.Executable {
		t.Fatalf("murder.prev rolls back %s, want %s", got, f.node.Executable)
	}
	if got := RollbackTarget(f.node.Executable); got != f.node.Executable {
		t.Fatalf("murder rolls back %s", got)
	}
	if _, err := Rollback(RollbackTarget(previous)); err != nil {
		t.Fatal(err)
	}
	if inPlace, _ := os.ReadFile(f.node.Executable); !bytes.Equal(inPlace, fakeBinary(5, "old")) {
		t.Fatalf("after rollback from murder.prev: %q in place", inPlace)
	}
}

func TestInstallRefusesWhenTheBinaryInPlaceIsNotTheOneRunning(t *testing.T) {
	f := newFixture(t)
	f.node.Current.Binary = "something-else"
	binary := fakeBinary(6, "new")
	if err := f.node.Install(context.Background(), bytes.NewReader(binary), sha(binary)); err == nil || !strings.Contains(err.Error(), "not the binary running") {
		t.Fatalf("install over a binary that is not running: %v", err)
	}
}

func TestAnEdgeThatTakesOverLetsTheAgentRestart(t *testing.T) {
	f := newFixture(t)
	// An "edge" that, on SIGHUP, writes another PID, as a new edge would.
	script := filepath.Join(f.dir, "edge.sh")
	if err := os.WriteFile(script, []byte("#!/bin/sh\ntrap 'echo 1 > "+f.node.EdgePIDFile+"' HUP\necho $$ > "+f.node.EdgePIDFile+"\nwhile true; do sleep 0.05; done\n"), 0o755); err != nil {
		t.Fatal(err)
	}
	// A shell cannot trap a signal it was started with ignored, as under
	// nohup; handling SIGHUP here makes the script start with the default.
	hup := make(chan os.Signal, 1)
	signal.Notify(hup, syscall.SIGHUP)
	defer signal.Stop(hup)
	edge := exec.Command(script, "edge")
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
