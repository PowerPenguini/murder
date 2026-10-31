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
	output, err := exec.CommandContext(ctx, path, "version", "-json").Output()
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
	// edgeWait bounds the edge's handover; tests shorten it.
	edgeWait time.Duration
}

// ErrDowngrade refuses a binary of an older schema: the database, once the
// new release has run, would not open with it.
var ErrDowngrade = errors.New("the binary has an older schema version")

// Install puts the binary read from r in place, if its hash is sha, then
// hands the edge over to it and has the agent restart into it. Without an
// edge running, there is nothing to hand over. If the edge does not take
// the new binary, the old one is put back and nothing else changes.
func (n *Node) Install(ctx context.Context, r io.Reader, sha string) error {
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
			err = errors.Join(err, fmt.Errorf("put the old binary back: %w", restoreErr))
		} else {
			_ = os.Remove(recordPath(n.Executable))
		}
		return err
	}
	n.Log.Info("installed a new murder binary; the agent restarts into it", "binary", sha, "schema", version.Schema)
	n.Restart()
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
	var signalled time.Time
	for {
		if next, err := readPID(n.EdgePIDFile); err == nil && next != pid {
			return nil
		}
		if time.Now().After(deadline) {
			return fmt.Errorf("the edge did not hand over to the new binary within %v; the old edge serves on", wait)
		}
		if time.Since(signalled) >= resignal {
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
// one back for: the old binary, kept as murder.prev, rolls back the one in
// place, so a node whose new binary does not even run can still be rolled
// back.
func RollbackTarget(executable string) string {
	if installed, ok := strings.CutSuffix(executable, ".prev"); ok {
		if _, err := os.Stat(recordPath(installed)); err == nil {
			return installed
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

// ErrNothingToRollBack reports a node with no install to undo.
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

var ErrNothingToRollBack = errors.New("no upgrade of this node to roll back")

// Rollback puts back the binary the last install replaced, if the binary
// in place is the one it installed. It returns the record, whose From
// schema says which database the old binary opens.
func Rollback(executable string) (Record, error) {
	record, err := LastInstall(executable)
	if err != nil {
		return Record{}, err
	}
	inPlace, err := Hash(executable)
	if err != nil {
		return Record{}, err
	}
	previous, err := Hash(executable + ".prev")
	if err != nil {
		return Record{}, fmt.Errorf("%w: %w", ErrNothingToRollBack, err)
	}
	if inPlace != record.To.Binary || previous != record.From.Binary {
		return Record{}, fmt.Errorf("%w: %s holds %s and its .prev %s, not what the last install left (%s replacing %s)",
			ErrNothingToRollBack, executable, short(inPlace), short(previous), short(record.To.Binary), short(record.From.Binary))
	}
	if err := os.Rename(executable, executable+".failed"); err != nil {
		return Record{}, err
	}
	if err := os.Rename(executable+".prev", executable); err != nil {
		return Record{}, err
	}
	return record, os.Remove(recordPath(executable))
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
