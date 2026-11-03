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
	// Until it is finished, a rollback that stopped after the binary
	// finds it back, and goes on with the database.
	again, err := Rollback(f.node.Executable)
	if err != nil || again != record {
		t.Fatalf("a rollback again before it finished: %+v, %v", again, err)
	}
	if inPlace, _ := os.ReadFile(f.node.Executable); !bytes.Equal(inPlace, fakeBinary(5, "old")) {
		t.Fatalf("a rollback again changed the binary: %q in place", inPlace)
	}
	if err := FinishRollback(f.node.Executable); err != nil {
		t.Fatal(err)
	}
	// Done once: a rollback after it has nothing to undo.
	if _, err := Rollback(f.node.Executable); !errors.Is(err, ErrNothingToRollBack) {
		t.Fatalf("a second rollback: %v", err)
	}
}

// Installs on one node go one at a time, from the first check to the
// restart, and none follows one that installed until the agent restarted:
// two at once would each keep the other's binary as the old one, and leave
// nothing to roll back to.
func TestInstallsGoOneAtATime(t *testing.T) {
	f := newFixture(t)
	entered, release := make(chan struct{}), make(chan struct{})
	snapshots := 0
	snapshot := f.node.Snapshot
	f.node.Snapshot = func() (string, error) {
		if snapshots++; snapshots == 1 {
			close(entered)
			<-release
		}
		return snapshot()
	}
	first, second := fakeBinary(6, "first"), fakeBinary(6, "second")
	firstDone, secondDone := make(chan error, 1), make(chan error, 1)
	go func() { firstDone <- f.node.Install(context.Background(), bytes.NewReader(first), sha(first)) }()
	<-entered
	go func() { secondDone <- f.node.Install(context.Background(), bytes.NewReader(second), sha(second)) }()
	select {
	case err := <-secondDone:
		t.Fatalf("a second install went on beside the first: %v", err)
	case <-time.After(300 * time.Millisecond):
	}
	close(release)
	if err := <-firstDone; err != nil {
		t.Fatal(err)
	}
	if err := <-secondDone; !errors.Is(err, ErrRestarting) {
		t.Fatalf("an install while the agent restarts into another: %v", err)
	}
	// The same binary again, as an upgrade that resumes sends it, is no
	// refusal: it is installed already.
	if err := f.node.Install(context.Background(), bytes.NewReader(first), sha(first)); err != nil {
		t.Fatalf("the installed binary again: %v", err)
	}
	if snapshots != 1 || f.restarted != 1 {
		t.Fatalf("%d snapshots, %d restarts", snapshots, f.restarted)
	}
	if previous, _ := os.ReadFile(f.node.Executable + ".prev"); !bytes.Equal(previous, fakeBinary(5, "old")) {
		t.Fatalf("murder.prev holds %q, not the binary that ran", previous)
	}
	record, err := Rollback(f.node.Executable)
	if err != nil || record.From != f.node.Current {
		t.Fatalf("rollback: %+v, %v", record, err)
	}
	if inPlace, _ := os.ReadFile(f.node.Executable); !bytes.Equal(inPlace, fakeBinary(5, "old")) {
		t.Fatalf("after rollback: %q in place", inPlace)
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

// startEdge runs script as the edge, its PID in the PID file.
func startEdge(t *testing.T, f *fixture, script string) *exec.Cmd {
	t.Helper()
	path := filepath.Join(f.dir, "edge.sh")
	if err := os.WriteFile(path, []byte(script), 0o755); err != nil {
		t.Fatal(err)
	}
	hup := make(chan os.Signal, 1)
	signal.Notify(hup, syscall.SIGHUP)
	t.Cleanup(func() { signal.Stop(hup) })
	edge := exec.Command(path, "edge")
	if err := edge.Start(); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = edge.Process.Kill(); _ = edge.Wait() })
	for range 200 {
		if pid, err := readPID(f.node.EdgePIDFile); err == nil && pid == edge.Process.Pid {
			return edge
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Fatal("the edge did not start")
	return nil
}

// An edge whose new binary does not start says so, and the install gives
// up at once instead of waiting out the handover's limit.
func TestAFailedHandoverIsReportedAtOnce(t *testing.T) {
	f := newFixture(t)
	failed := HandoverFailedPath(f.node.EdgePIDFile)
	// A failure left by an earlier install counts for nothing.
	if err := os.WriteFile(failed, []byte("long ago\n"), 0o644); err != nil {
		t.Fatal(err)
	}
	past := time.Now().Add(-time.Hour)
	_ = os.Chtimes(failed, past, past)
	startEdge(t, f, "#!/bin/sh\ntrap 'echo \"child 42 exited: this release does not start\" > "+failed+"' HUP\necho $$ > "+f.node.EdgePIDFile+"\nwhile true; do sleep 0.05; done\n")
	f.node.edgeWait = 30 * time.Second
	f.node.lateEdgeWait = time.Second
	binary := fakeBinary(6, "new")
	start := time.Now()
	err := f.node.Install(context.Background(), bytes.NewReader(binary), sha(binary))
	if err == nil || !strings.Contains(err.Error(), "this release does not start") {
		t.Fatalf("install: %v", err)
	}
	if took := time.Since(start); took > 5*time.Second {
		t.Fatalf("the failure was noticed after %s", took)
	}
	if installed, _ := os.ReadFile(f.node.Executable); !bytes.Equal(installed, fakeBinary(5, "old")) || f.restarted != 0 {
		t.Fatal("the new binary stayed though its edge did not start")
	}
}

// A new edge that took over late, from a binary that is no longer in
// place, is handed back, asked again while it refuses, as an edge whose
// predecessor still drains does; one running another binary is left alone,
// and so is any edge once a new install began.
func TestALateEdgeIsHandedBack(t *testing.T) {
	f := newFixture(t)
	f.node.lateEdgeWait = 4 * time.Second
	shell, err := os.ReadFile("/bin/sh")
	if err != nil {
		t.Skip("no /bin/sh")
	}
	late := filepath.Join(f.dir, "murder-new")
	if err := os.WriteFile(late, shell, 0o755); err != nil {
		t.Fatal(err)
	}
	marker := filepath.Join(f.dir, "handed-back")
	hup := make(chan os.Signal, 1)
	signal.Notify(hup, syscall.SIGHUP)
	defer signal.Stop(hup)
	start := func() *exec.Cmd {
		// It refuses the first handover and takes the second.
		edge := exec.Command(late, "-c", "n=0; trap 'n=$((n+1)); if [ $n -ge 2 ]; then echo $n > "+marker+"; exit; fi' HUP; while true; do sleep 0.05; done", "edge")
		if err := edge.Start(); err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(f.node.EdgePIDFile, []byte(strconv.Itoa(edge.Process.Pid)), 0o644); err != nil {
			t.Fatal(err)
		}
		return edge
	}
	edge := start()
	f.node.revertLateEdge(context.Background(), 1, sha(shell))
	_ = edge.Wait()
	if data, err := os.ReadFile(marker); err != nil || string(data) != "2\n" {
		t.Fatalf("the late edge was not handed back after it refused once: %q, %v", data, err)
	}
	// An edge of another binary is not touched.
	_ = os.Remove(marker)
	edge = start()
	defer func() { _ = edge.Process.Kill(); _ = edge.Wait() }()
	f.node.revertLateEdge(context.Background(), 1, sha([]byte("another binary")))
	// Nor any edge once the watch is stopped, as a new install stops it.
	stopped, stop := context.WithCancel(context.Background())
	stop()
	f.node.revertLateEdge(stopped, 1, sha(shell))
	time.Sleep(200 * time.Millisecond)
	if _, err := os.Stat(marker); !os.IsNotExist(err) {
		t.Fatal("an edge was signalled that should not be")
	}
}
