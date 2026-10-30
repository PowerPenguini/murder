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
	if err := os.Rename(temp.Name(), n.Executable); err != nil {
		return err
	}
	if err := n.handOverEdge(ctx); err != nil {
		// Put the old binary back; the old edge serves on.
		if restoreErr := os.Rename(previous, n.Executable); restoreErr != nil {
			err = errors.Join(err, fmt.Errorf("put the old binary back: %w", restoreErr))
		}
		return err
	}
	n.Log.Info("installed a new murder binary; the agent restarts into it", "binary", sha, "schema", version.Schema)
	n.Restart()
	return nil
}

// handOverEdge has the edge start the binary now in place and waits until
// the new edge serves. The edge's PID file names the process serving.
func (n *Node) handOverEdge(ctx context.Context) error {
	pid, err := readPID(n.EdgePIDFile)
	if err != nil || syscall.Kill(pid, 0) != nil {
		n.Log.Warn("no edge runs to hand over", "pidFile", n.EdgePIDFile)
		return nil
	}
	if err := syscall.Kill(pid, syscall.SIGHUP); err != nil {
		return fmt.Errorf("signal the edge: %w", err)
	}
	wait := n.edgeWait
	if wait == 0 {
		wait = 90 * time.Second
	}
	deadline := time.Now().Add(wait)
	for {
		if next, err := readPID(n.EdgePIDFile); err == nil && next != pid {
			return nil
		}
		if time.Now().After(deadline) {
			return fmt.Errorf("the edge did not hand over to the new binary within %v; the old edge serves on", wait)
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(100 * time.Millisecond):
		}
	}
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
