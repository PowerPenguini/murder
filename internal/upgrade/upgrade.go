// Package upgrade replaces the murder binary on a node without stopping a
// tenant or dropping a request: it checks the new binary, keeps the old one
// and a copy of the database for a rollback, hands the edge over to the new
// binary, and has the agent restart into it.
package upgrade

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"os"
	"os/exec"
	"path/filepath"
	"slices"
	"strconv"
	"strings"
	"sync"
	"syscall"
	"time"
)

// Version describes a murder binary.
type Version struct {
	// Binary is the SHA-256 of the binary file.
	Binary string `json:"binary"`
	Schema int    `json:"schema"`
}

// Info is what a node reports while an upgrade waits for it.
type Info struct {
	Node    string  `json:"node"`
	Version Version `json:"version"`
	// Started is when the agent started; Applied the last log entry it
	// applied; LastPass when its last whole reconcile pass ended.
	Started  time.Time `json:"started"`
	Applied  uint64    `json:"applied"`
	LastPass time.Time `json:"lastPass,omitzero"`
}

// Hash returns the SHA-256 of the file at path.
func Hash(path string) (string, error) {
	file, err := os.Open(path)
	if err != nil {
		return "", err
	}
	defer file.Close()
	sum := sha256.New()
	if _, err := io.Copy(sum, file); err != nil {
		return "", err
	}
	return hex.EncodeToString(sum.Sum(nil)), nil
}

// Probe runs the binary at path, so a binary that does not run on this
// node is refused before it replaces anything, and returns its version.
func Probe(ctx context.Context, path string) (Version, error) {
	ctx, cancel := context.WithTimeout(ctx, 30*time.Second)
	defer cancel()
	command := exec.CommandContext(ctx, path, "version", "-json")
	// A child that keeps the output open must not hold the probe past ctx.
	command.WaitDelay = 5 * time.Second
	output, err := command.Output()
	if err != nil {
		return Version{}, fmt.Errorf("run %s version: %w", path, err)
	}
	var version Version
	if err := json.Unmarshal(output, &version); err != nil || version.Binary == "" || version.Schema == 0 {
		return Version{}, fmt.Errorf("%s version said %q: not a murder binary", path, strings.TrimSpace(string(output)))
	}
	return version, nil
}

// Node installs new binaries on this node.
type Node struct {
	// Executable is where the running binary is, which a new one replaces;
	// the old one stays beside it as Executable+".prev".
	Executable string
	Current    Version
	// EdgePIDFile names the edge's process, which an install hands over.
	EdgePIDFile string
	// Snapshot writes a copy of the database to a new file and returns its
	// name: the database a rollback goes back to if the new release
	// changed it.
	Snapshot func() (string, error)
	// Database is the database's path; its copy is kept beside it.
	Database string
	// Restart has the agent stop, so that its service starts the new
	// binary.
	Restart func()
	Log     *slog.Logger
	// edgeWait bounds the edge's handover, lateEdgeWait the watch for a
	// late one; tests shorten them.
	edgeWait, lateEdgeWait time.Duration
	// stopRevert ends the watch for a late edge of an earlier install.
	revertMu   sync.Mutex
	stopRevert context.CancelFunc
	// installMu holds one install at a time, from its first check to its
	// restart: two at once would each keep the other's binary as the one
	// to roll back to. restarting is the binary an install put in place,
	// which the agent restarts into; no other is taken until then.
	installMu  sync.Mutex
	restarting string
}

// ErrRestarting refuses an install while the agent restarts into the
// binary an earlier one put in place.
var ErrRestarting = errors.New("the agent restarts into the binary installed just before")

// ErrNotOwner refuses an install for an upgrade whose freeze is gone: a
// person thawed, or another upgrade took the freeze over.
var ErrNotOwner = errors.New("the upgrade no longer holds the freeze")

// ErrDowngrade refuses a binary of an older schema: the database, once the
// new release has run, would not open with it.
var ErrDowngrade = errors.New("the binary has an older schema version")

// Install puts the binary read from r in place, if its hash is sha, then
// hands the edge over to it and has the agent restart into it. Without an
// edge running, there is nothing to hand over. If the edge does not take
// the new binary, the old one is put back and nothing else changes.
func (n *Node) Install(ctx context.Context, r io.Reader, sha string) error {
	return n.InstallHolding(ctx, r, sha, nil)
}

// InstallHolding installs as Install does, while holds, if set, says the
// upgrade that sends the binary still holds the freeze. The sender may lose
// it while the binary arrives, or while the edge hands over: holds is asked
// again, under the install lock, once the binary has arrived and been
// checked, before anything is replaced, and again before the agent
// restarts. If it fails before the restart, the old binary goes back, and
// the edge to it, and nothing else changes.
func (n *Node) InstallHolding(ctx context.Context, r io.Reader, sha string, holds func() error) error {
	if holds == nil {
		holds = func() error { return nil }
	}
	n.installMu.Lock()
	defer n.installMu.Unlock()
	switch n.restarting {
	case "":
	case sha:
		return nil // installed already: the agent restarts into it
	default:
		return fmt.Errorf("%w (%s): wait until it runs it", ErrRestarting, short(n.restarting))
	}
	// An earlier install's watch for a late edge would hand this one's
	// edge back.
	n.revertMu.Lock()
	if n.stopRevert != nil {
		n.stopRevert()
		n.stopRevert = nil
	}
	n.revertMu.Unlock()
	dir := filepath.Dir(n.Executable)
	temp, err := os.CreateTemp(dir, "."+filepath.Base(n.Executable)+".new-*")
	if err != nil {
		return err
	}
	defer os.Remove(temp.Name())
	sum := sha256.New()
	_, err = io.Copy(io.MultiWriter(temp, sum), r)
	if err = errors.Join(err, temp.Chmod(0o755), temp.Sync(), temp.Close()); err != nil {
		return err
	}
	if got := hex.EncodeToString(sum.Sum(nil)); got != sha {
		return fmt.Errorf("the binary arrived with SHA-256 %s, not %s", got, sha)
	}
	if sha == n.Current.Binary {
		return nil
	}
	// The binary in place must be the one running: its copy is what a
	// rollback goes back to.
	if inPlace, err := Hash(n.Executable); err != nil {
		return err
	} else if inPlace != n.Current.Binary {
		return fmt.Errorf("%s holds %s, not the binary running (%s): restart the agent, or put its binary back, first", n.Executable, short(inPlace), short(n.Current.Binary))
	}
	version, err := Probe(ctx, temp.Name())
	if err != nil {
		return err
	}
	if version.Schema < n.Current.Schema {
		return fmt.Errorf("%w: %d, this node runs %d", ErrDowngrade, version.Schema, n.Current.Schema)
	}
	// The binary has arrived and runs: is it still the sender's to install?
	if err := holds(); err != nil {
		return err
	}
	// The database as it is, for a rollback after the new release changed
	// it. Changes are frozen during an upgrade: nothing is lost going back.
	copied, err := n.Snapshot()
	if err != nil {
		return fmt.Errorf("copy the database: %w", err)
	}
	if err := os.Rename(copied, n.Database+".before-upgrade"); err != nil {
		_ = os.Remove(copied)
		return err
	}
	previous := n.Executable + ".prev"
	if err := copyFile(n.Executable, previous); err != nil {
		return fmt.Errorf("keep the current binary: %w", err)
	}
	// What a rollback checks it undoes this install, and nothing older.
	record := Record{From: n.Current, To: Version{Binary: sha, Schema: version.Schema}, At: time.Now().UTC()}
	if err := writeRecord(n.Executable, record); err != nil {
		return err
	}
	if err := os.Rename(temp.Name(), n.Executable); err != nil {
		return err
	}
	if err := n.handOverEdge(ctx); err != nil {
		// Put the old binary back; the old edge serves on.
		if restoreErr := os.Rename(previous, n.Executable); restoreErr != nil {
			return errors.Join(err, fmt.Errorf("put the old binary back: %w", restoreErr))
		}
		_ = os.Remove(recordPath(n.Executable))
		// A new edge may yet take over, late, from the new binary: it is
		// handed back to the old one, now in place again.
		if pid, pidErr := readPID(n.EdgePIDFile); pidErr == nil {
			revert, stop := context.WithCancel(context.Background())
			n.revertMu.Lock()
			n.stopRevert = stop
			n.revertMu.Unlock()
			go n.revertLateEdge(revert, pid, sha)
		}
		return err
	}
	// The handover takes a while: the freeze may have gone meanwhile.
	if err := holds(); err != nil {
		return errors.Join(err, n.putBack(ctx, previous, sha))
	}
	n.Log.Info("installed a new murder binary; the agent restarts into it", "binary", sha, "schema", version.Schema)
	n.restarting = sha
	n.Restart()
	return nil
}

// putBack undoes an install whose edge handed over, before the agent
// restarts: the old binary goes back in place, and the edge, if one runs,
// hands over to it. An edge that does not at once is watched, and asked
// again, as after a handover that failed.
func (n *Node) putBack(ctx context.Context, previous, sha string) error {
	if err := os.Rename(previous, n.Executable); err != nil {
		return fmt.Errorf("put the old binary back: %w", err)
	}
	_ = os.Remove(recordPath(n.Executable))
	// The sender may be gone: the edge goes back all the same.
	if err := n.handOverEdge(context.WithoutCancel(ctx)); err != nil {
		revert, stop := context.WithCancel(context.Background())
		n.revertMu.Lock()
		n.stopRevert = stop
		n.revertMu.Unlock()
		// Any edge still running the new binary is handed back.
		go n.revertLateEdge(revert, 0, sha)
		return fmt.Errorf("hand the edge back to the old binary: %w", err)
	}
	n.Log.Warn("put the old binary back: the upgrade lost the freeze while the edge handed over", "binary", sha)
	return nil
}

// handOverEdge has the edge start the binary now in place and waits until
// the new edge serves. The edge's PID file names the process serving. An
// edge still finishing an earlier handover refuses another, so the signal
// is sent again until the edge takes it.
func (n *Node) handOverEdge(ctx context.Context) error {
	pid, err := readPID(n.EdgePIDFile)
	if err != nil || !isEdge(pid) {
		n.Log.Warn("no edge runs to hand over", "pidFile", n.EdgePIDFile)
		return nil
	}
	wait := n.edgeWait
	if wait == 0 {
		wait = 90 * time.Second
	}
	deadline := time.Now().Add(wait)
	var signalled, first time.Time
	failed := HandoverFailedPath(n.EdgePIDFile)
	// What an earlier handover left says nothing of this one.
	_ = os.Remove(failed)
	for {
		if next, err := readPID(n.EdgePIDFile); err == nil && next != pid {
			return nil
		}
		// The edge says at once when its new binary did not start.
		// File times come from a coarser clock: a few milliseconds' slack.
		if info, err := os.Stat(failed); err == nil && !first.IsZero() && !info.ModTime().Before(first.Add(-50*time.Millisecond)) {
			why, _ := os.ReadFile(failed)
			return fmt.Errorf("the edge's new binary did not take over (%s); the old edge serves on", strings.TrimSpace(string(why)))
		}
		if time.Now().After(deadline) {
			return fmt.Errorf("the edge did not hand over to the new binary within %v; the old edge serves on", wait)
		}
		if time.Since(signalled) >= resignal {
			if first.IsZero() {
				first = time.Now()
			}
			if err := syscall.Kill(pid, syscall.SIGHUP); err != nil {
				return fmt.Errorf("signal the edge: %w", err)
			}
			signalled = time.Now()
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(100 * time.Millisecond):
		}
	}
}

// resignal is how long a handover gets before the edge is asked again.
const resignal = 10 * time.Second

// HandoverFailedPath is where, beside its PID file, an edge whose new
// binary did not take over says why.
func HandoverFailedPath(pidFile string) string {
	return filepath.Join(filepath.Dir(pidFile), "handover-failed")
}

// RecordHandoverFailure says, for an install waiting on the handover, that
// the new binary did not take over, and why.
func RecordHandoverFailure(pidFile string, why error) error {
	path := HandoverFailedPath(pidFile)
	temp := path + ".new"
	if err := os.WriteFile(temp, []byte(why.Error()+"\n"), 0o644); err != nil {
		return err
	}
	return os.Rename(temp, path)
}

// lateEdge is how long after a failed handover a new edge may still take
// over: tableflip gives a new process a minute to become ready, and a
// signal sent again starts another.
const lateEdge = 75 * time.Second

// revertLateEdge watches, after a failed handover, for a new edge that took
// over late from the new binary, which no longer is in place, and hands it
// over again, to the old binary, so edge and agent run one release. Such an
// edge refuses a handover while its own predecessor still drains, so it is
// asked again until it runs another binary, or the watch ends.
func (n *Node) revertLateEdge(ctx context.Context, oldPID int, newBinary string) {
	wait := n.lateEdgeWait
	if wait == 0 {
		wait = lateEdge
	}
	ctx, cancel := context.WithTimeout(ctx, wait)
	defer cancel()
	var signalled time.Time
	for {
		select {
		case <-ctx.Done():
			return
		case <-time.After(time.Second):
		}
		pid, err := readPID(n.EdgePIDFile)
		if err != nil || pid == oldPID || !isEdge(pid) {
			continue
		}
		running, err := Hash(fmt.Sprintf("/proc/%d/exe", pid))
		if err != nil {
			continue
		}
		if running != newBinary {
			return // handed back, or never late
		}
		if time.Since(signalled) >= n.resignalEvery() {
			n.Log.Warn("a new edge took over late from the binary that was not installed; handing it back to the old one", "pid", pid)
			if err := syscall.Kill(pid, syscall.SIGHUP); err != nil {
				n.Log.Error("hand the late edge back to the old binary", "error", err)
			}
			signalled = time.Now()
		}
	}
}

func (n *Node) resignalEvery() time.Duration {
	if n.lateEdgeWait != 0 && n.lateEdgeWait < resignal {
		return n.lateEdgeWait / 5
	}
	return resignal
}

// isEdge reports whether pid is a murder edge, not a process that took
// the PID of one that is gone.
func isEdge(pid int) bool {
	cmdline, err := os.ReadFile(fmt.Sprintf("/proc/%d/cmdline", pid))
	if err != nil {
		return false
	}
	args := strings.Split(strings.TrimRight(string(cmdline), "\x00"), "\x00")
	return len(args) >= 2 && slices.Contains(args[1:], "edge")
}

// Record says what an install replaced, for its rollback.
type Record struct {
	From Version   `json:"from"`
	To   Version   `json:"to"`
	At   time.Time `json:"at"`
}

func recordPath(executable string) string { return executable + ".upgrade" }

// RollbackTarget is the binary a rollback run as executable puts the old
// one back for. The new binary, in place, rolls itself back. The old one,
// kept as murder.prev, rolls back the one in place, so a node whose new
// binary does not even run can still be rolled back. A copy of the new
// binary, kept as murder.failed once the old one is back, finishes a
// rollback that stopped after that: the old binary in place may be of a
// release that cannot.
func RollbackTarget(executable string) string {
	for _, suffix := range []string{".prev", ".failed"} {
		if installed, ok := strings.CutSuffix(executable, suffix); ok {
			if _, err := os.Stat(recordPath(installed)); err == nil {
				return installed
			}
		}
	}
	return executable
}

func writeRecord(executable string, record Record) error {
	data, err := json.Marshal(record)
	if err != nil {
		return err
	}
	return os.WriteFile(recordPath(executable), data, 0o644)
}

// LastInstall is the record of the last install over executable, or
// ErrNothingToRollBack.
func LastInstall(executable string) (Record, error) {
	data, err := os.ReadFile(recordPath(executable))
	if errors.Is(err, os.ErrNotExist) {
		return Record{}, ErrNothingToRollBack
	}
	if err != nil {
		return Record{}, err
	}
	var record Record
	if err := json.Unmarshal(data, &record); err != nil {
		return Record{}, fmt.Errorf("%s: %w", recordPath(executable), err)
	}
	return record, nil
}

// ErrNothingToRollBack reports a node with no install to undo.
var ErrNothingToRollBack = errors.New("no upgrade of this node to roll back")

// CheckRollback returns the record of the last install over executable
// if a rollback can put its binary back, changing nothing: the binary in
// place is the one it installed and murder.prev the one it replaced, or
// the old binary is back already. The record's From schema says which
// database the old binary opens.
func CheckRollback(executable string) (Record, error) {
	record, err := LastInstall(executable)
	if err != nil {
		return Record{}, err
	}
	inPlace, err := Hash(executable)
	if err != nil {
		return Record{}, err
	}
	if inPlace == record.From.Binary {
		return record, nil // put back already
	}
	previous, err := Hash(executable + ".prev")
	if err != nil {
		return Record{}, fmt.Errorf("%w: %w", ErrNothingToRollBack, err)
	}
	if inPlace != record.To.Binary || previous != record.From.Binary {
		return Record{}, fmt.Errorf("%w: %s holds %s and its .prev %s, not what the last install left (%s replacing %s)",
			ErrNothingToRollBack, executable, short(inPlace), short(previous), short(record.To.Binary), short(record.From.Binary))
	}
	return record, nil
}

// Rollback puts back the binary the last install replaced, if CheckRollback
// allows it, and returns the record. The new binary is copied to
// murder.failed first and the old one then renamed over it, so a binary is
// always in place and the new one, whose code finishes a rollback, is
// kept. It keeps the record: a rollback that also puts the database back
// is over only once both are, and then FinishRollback removes it. A
// rollback that stopped after putting the binary back finds it in place,
// and returns the record again.
func Rollback(executable string) (Record, error) {
	record, err := CheckRollback(executable)
	if err != nil {
		return Record{}, err
	}
	if inPlace, err := Hash(executable); err != nil {
		return Record{}, err
	} else if inPlace == record.From.Binary {
		return record, nil
	}
	if err := copyFile(executable, executable+".failed"); err != nil {
		return Record{}, fmt.Errorf("keep the new binary: %w", err)
	}
	if err := os.Rename(executable+".prev", executable); err != nil {
		return Record{}, err
	}
	return record, nil
}

// FinishRollback says a rollback is over: binary and database are back.
func FinishRollback(executable string) error {
	err := os.Remove(recordPath(executable))
	if errors.Is(err, os.ErrNotExist) {
		return nil
	}
	return err
}

func short(sha string) string {
	if len(sha) > 12 {
		return sha[:12]
	}
	return sha
}

func readPID(path string) (int, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return 0, err
	}
	return strconv.Atoi(strings.TrimSpace(string(data)))
}

func copyFile(from, to string) error {
	source, err := os.Open(from)
	if err != nil {
		return err
	}
	defer source.Close()
	info, err := source.Stat()
	if err != nil {
		return err
	}
	temp, err := os.CreateTemp(filepath.Dir(to), "."+filepath.Base(to)+"-*")
	if err != nil {
		return err
	}
	defer os.Remove(temp.Name())
	_, err = io.Copy(temp, source)
	if err = errors.Join(err, temp.Chmod(info.Mode().Perm()), temp.Sync(), temp.Close()); err != nil {
		return err
	}
	return os.Rename(temp.Name(), to)
}
