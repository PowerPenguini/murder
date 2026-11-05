package reconcile

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io/fs"
	"os"
	"path/filepath"
	"strings"
	"time"

	"github.com/PowerPenguini/murder/internal/runtime"
	"github.com/PowerPenguini/murder/internal/spec"
)

// The last lines of a tenant's previous container, kept so that the lines
// written before a crash or a replacement can still be read once a new
// container runs. One per tenant, until the next exit or replacement, in
// the node's own state beside its database; never in the cluster's.
//
// They are kept when:
//   - the app exits and the agent starts its container again: its lines up
//     to the exit (the engine keeps them too, in the same container, but
//     mixed with the new run's);
//   - the container is replaced (a roll, a change of image, spec, limits
//     or secrets, a data mount found missing) or removed on suspension:
//     its lines, read after it stopped and before it is removed.
//
// They are not kept when the node itself stopped (a power loss, a kernel
// panic, a failover: the containers went with the node), when a tenant
// moves to another node (the lines stay on the old one, which removes
// them as the tenant leaves), for exits while the agent was down, or for
// a container an older release left restarting under the engine's own
// restart policy.

// PreviousBytes and PreviousLines bound what is kept of a container's
// output: its last lines, whichever bound comes first.
const (
	PreviousBytes = 64 << 10
	PreviousLines = 1000
)

// Previous is what was kept of a tenant's container before it was
// replaced or exited.
type Previous struct {
	Tenant    string `json:"tenant"`
	Container string `json:"container"`
	Image     string `json:"image,omitempty"`
	// Why is "exited" (the app exited and the container was started
	// again), "replaced" (a new container took its place) or "stopped"
	// (removed as the tenant was suspended).
	Why string `json:"why"`
	// The exit, when the app exited: when, whether its code is known, the
	// code, and whether the kernel killed it for its memory.
	ExitedAt  time.Time `json:"exitedAt,omitzero"`
	ExitKnown bool      `json:"exitKnown,omitempty"`
	ExitCode  int       `json:"exitCode,omitempty"`
	OOMKilled bool      `json:"oomKilled,omitempty"`
	// MemoryMB is the limit the container ran with.
	MemoryMB int `json:"memoryMB,omitempty"`
	// At is when the lines were kept; Cut says older lines were dropped to
	// keep within the bounds.
	At    time.Time `json:"at"`
	Cut   bool      `json:"cut,omitempty"`
	Lines []byte    `json:"lines"`
}

// ErrNoPrevious is returned when nothing is kept for a tenant.
var ErrNoPrevious = errors.New("no previous container's lines are kept")

// bound keeps the last PreviousLines lines of out, within PreviousBytes,
// and says whether anything was dropped. A line cut by the byte bound is
// dropped whole.
func bound(out []byte) ([]byte, bool) {
	cut := false
	if len(out) > PreviousBytes {
		out, cut = out[len(out)-PreviousBytes:], true
		if i := bytes.IndexByte(out, '\n'); i >= 0 {
			out = out[i+1:]
		}
	}
	body := bytes.TrimSuffix(out, []byte("\n"))
	if n := bytes.Count(body, []byte("\n")) + 1; len(body) > 0 && n > PreviousLines {
		for drop := n - PreviousLines; drop > 0; drop-- {
			out = out[bytes.IndexByte(out, '\n')+1:]
		}
		cut = true
	}
	return bytes.Clone(out), cut
}

func (r *Reconciler) previousPath(tenant string) string {
	if r.config.PreviousDir == "" || spec.ValidName(tenant) != nil {
		return ""
	}
	return filepath.Join(r.config.PreviousDir, tenant+".json")
}

// keepPrevious reads a container's last lines and keeps them for its
// tenant, in place of what was kept before. A failure is logged: losing
// these lines must never hold up starting or replacing a container.
func (r *Reconciler) keepPrevious(ctx context.Context, tenant string, container runtime.Container, kept Previous) {
	path := r.previousPath(tenant)
	if path == "" {
		return
	}
	// Twice the line bound read, so the line bound decides, not the tail.
	out, err := r.runtime.Logs(ctx, container.ID, 2*PreviousLines)
	if err != nil {
		r.log.Warn("keep the previous container's lines", "tenant", tenant, "container", container.ID, "error", err)
		return
	}
	kept.Tenant, kept.Container, kept.At = tenant, container.ID, time.Now().UTC()
	if kept.Image == "" {
		kept.Image = container.Labels[runtime.LabelImage]
	}
	kept.Lines, kept.Cut = bound(out)
	if err := writePrevious(path, kept); err != nil {
		r.log.Warn("keep the previous container's lines", "tenant", tenant, "container", container.ID, "error", err)
	}
}

func writePrevious(path string, kept Previous) error {
	data, err := json.Marshal(kept)
	if err != nil {
		return err
	}
	if err := os.MkdirAll(filepath.Dir(path), 0o700); err != nil {
		return err
	}
	temp, err := os.CreateTemp(filepath.Dir(path), ".previous-*")
	if err != nil {
		return err
	}
	defer os.Remove(temp.Name())
	if _, err := temp.Write(data); err != nil {
		temp.Close()
		return err
	}
	if err := temp.Chmod(0o600); err != nil {
		temp.Close()
		return err
	}
	if err := temp.Close(); err != nil {
		return err
	}
	return os.Rename(temp.Name(), path)
}

// Previous is what is kept of a tenant's previous container:
// ErrNoPrevious when nothing is.
func (r *Reconciler) Previous(tenant string) (Previous, error) {
	path := r.previousPath(tenant)
	if path == "" {
		return Previous{}, ErrNoPrevious
	}
	data, err := os.ReadFile(path)
	if errors.Is(err, fs.ErrNotExist) {
		return Previous{}, ErrNoPrevious
	}
	if err != nil {
		return Previous{}, err
	}
	var kept Previous
	if err := json.Unmarshal(data, &kept); err != nil {
		return Previous{}, fmt.Errorf("the previous container's lines: %w", err)
	}
	return kept, nil
}

// LinesSince is when the current run of a tenant's container began, as far
// as its lines go: after the exit kept for it, when the container was
// started again in place. Zero when its lines are all its own.
func (r *Reconciler) LinesSince(tenant, container string) time.Time {
	kept, err := r.Previous(tenant)
	if err != nil || kept.Why != "exited" || kept.Container != container {
		return time.Time{}
	}
	return kept.ExitedAt
}

// forgetPrevious drops what is kept for tenants this node no longer has.
func (r *Reconciler) forgetPrevious(keep map[string]bool) {
	if r.config.PreviousDir == "" {
		return
	}
	entries, err := os.ReadDir(r.config.PreviousDir)
	if err != nil {
		return
	}
	for _, entry := range entries {
		tenant, ok := strings.CutSuffix(entry.Name(), ".json")
		if !ok || keep[tenant] {
			continue
		}
		if err := os.Remove(filepath.Join(r.config.PreviousDir, entry.Name())); err != nil {
			r.log.Warn("forget the previous container's lines", "tenant", tenant, "error", err)
		}
	}
}
