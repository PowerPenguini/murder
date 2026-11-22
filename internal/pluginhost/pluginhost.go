// Package pluginhost runs the containers of plugins of kind container on
// a node, for its edge: one for each plugin, of the image it was
// installed with, with no network, its socket in a directory of its own
// below plugins.SocketRoot, on a tmpfs of its own the container cannot
// fill past a few kilobytes. The agent keeps them so: it pulls, starts
// and restarts them, and stops a plugin's old container a while after its
// image changed, once the edge's requests to it are done.
package pluginhost

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"maps"
	"os"
	"path/filepath"
	"slices"
	"time"

	"github.com/PowerPenguini/murder/internal/plugins"
	"github.com/PowerPenguini/murder/internal/runtime"
	"github.com/PowerPenguini/murder/internal/store"
)

// LabelID marks a plugin's container with its plugins.ContainerID.
const LabelID = "io.murder.plugin-id"

// The bounds of a plugin's container: no plugin needs more to read
// answers as they pass.
const (
	Memory = 256 << 20
	CPUs   = 1
	PIDs   = 128
	// User is who the plugin runs as: nobody, whatever its image says.
	User = "65534:65534"
	uid  = 65534
)

// GoMemoryLimit has a plugin written in Go collect its garbage harder
// as it nears Memory, rather than be stopped past it.
const GoMemoryLimit = "GOMEMLIMIT=224MiB"

// Retire is how long a plugin's container is kept once its plugin went,
// or runs another image, for the edge's requests to it to end: one still
// under way then is cut.
const Retire = 2 * time.Minute

// Runtime is what the host needs of the container engine.
type Runtime interface {
	HasImage(ctx context.Context, ref string) (bool, error)
	PullImage(ctx context.Context, ref string) error
	ListPlugins(ctx context.Context) ([]runtime.Container, error)
	Create(ctx context.Context, spec runtime.ContainerSpec) (string, error)
	Start(ctx context.Context, id string) error
	Stop(ctx context.Context, id string, timeout time.Duration) error
	Remove(ctx context.Context, id string) error
}

// Host keeps the containers of a node's plugins.
type Host struct {
	Runtime Runtime
	// Plugins are the cluster's plugins.
	Plugins func() []store.Plugin
	// Root holds the sockets' directories: plugins.SocketRoot.
	Root string
	// Tmpfs mounts a small tmpfs on each socket's directory. Without, as
	// in tests, the directory is one any user may write.
	Tmpfs bool
	Log   *slog.Logger

	now func() time.Time
	// retired is when each container no plugin runs any more was first
	// found so.
	retired map[string]time.Time
}

// Run keeps the containers every interval, until ctx ends.
func (h *Host) Run(ctx context.Context, interval time.Duration) {
	ticker := time.NewTicker(interval)
	defer ticker.Stop()
	for {
		if err := h.Sync(ctx); err != nil && ctx.Err() == nil {
			h.Log.Warn("keep the plugins' containers", "error", err)
		}
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
		}
	}
}

// want is a plugin's container this node is to run.
type want struct {
	plugin, image string
}

// Sync makes the node run what its plugins need, once.
func (h *Host) Sync(ctx context.Context) error {
	if h.now == nil {
		h.now = time.Now
	}
	if h.retired == nil {
		h.retired = map[string]time.Time{}
	}
	wanted := map[string]want{}
	for _, plugin := range h.Plugins() {
		if plugin.Spec.Kind == plugins.KindContainer && plugin.Manifest != nil {
			wanted[plugins.ContainerID(plugin.Name, plugin.Spec.Image)] = want{plugin: plugin.Name, image: plugin.Spec.Image}
		}
	}
	listed, err := h.Runtime.ListPlugins(ctx)
	if err != nil {
		return fmt.Errorf("list the plugins' containers: %w", err)
	}
	byID := map[string][]runtime.Container{}
	for _, container := range listed {
		byID[container.Labels[LabelID]] = append(byID[container.Labels[LabelID]], container)
	}
	var errs []error
	for _, id := range slices.Sorted(maps.Keys(wanted)) {
		delete(h.retired, id)
		if err := h.keep(ctx, id, wanted[id], byID[id]); err != nil {
			errs = append(errs, fmt.Errorf("plugin %s: %w", wanted[id].plugin, err))
		}
	}
	now := h.now()
	for id, containers := range byID {
		if _, ok := wanted[id]; ok {
			continue
		}
		first, seen := h.retired[id]
		if !seen {
			h.retired[id] = now
			continue
		}
		if now.Sub(first) < Retire {
			continue
		}
		if err := h.remove(ctx, id, containers); err != nil {
			errs = append(errs, err)
			continue
		}
		delete(h.retired, id)
	}
	// A directory of no container left, as one a crash left behind.
	if entries, err := os.ReadDir(h.Root); err == nil {
		for _, entry := range entries {
			if _, ok := wanted[entry.Name()]; ok || byID[entry.Name()] != nil {
				continue
			}
			if err := h.removeDir(filepath.Join(h.Root, entry.Name())); err != nil {
				errs = append(errs, err)
			}
		}
	}
	return errors.Join(errs...)
}

// keep has one container of a plugin's image run: started if it stopped,
// made anew if it cannot be.
func (h *Host) keep(ctx context.Context, id string, w want, containers []runtime.Container) error {
	dir := filepath.Join(h.Root, id)
	if err := h.ensureDir(dir); err != nil {
		return fmt.Errorf("its socket's directory: %w", err)
	}
	for i, container := range containers {
		if i > 0 {
			// Two of one image: one goes.
			if err := h.removeContainer(ctx, container); err != nil {
				return err
			}
			continue
		}
		if container.Running {
			continue
		}
		if err := h.Runtime.Start(ctx, container.ID); err == nil {
			h.Log.Info("started a plugin's container", "plugin", w.plugin, "container", container.Name)
			continue
		} else {
			h.Log.Warn("a plugin's container does not start; it is made anew", "plugin", w.plugin, "container", container.Name, "error", err)
		}
		if err := h.Runtime.Remove(ctx, container.ID); err != nil && !errors.Is(err, runtime.ErrNotFound) {
			return err
		}
		containers = nil
	}
	if len(containers) > 0 {
		return nil
	}
	present, err := h.Runtime.HasImage(ctx, w.image)
	if err != nil {
		return err
	}
	if !present {
		h.Log.Info("pulling a plugin's image", "plugin", w.plugin, "image", w.image)
		if err := h.Runtime.PullImage(ctx, w.image); err != nil {
			return fmt.Errorf("pull %s: %w", w.image, err)
		}
	}
	created, err := h.Runtime.Create(ctx, Spec(id, w.plugin, w.image, dir))
	if err != nil {
		return err
	}
	if err := h.Runtime.Start(ctx, created); err != nil {
		return err
	}
	h.Log.Info("started a plugin's container", "plugin", w.plugin, "image", w.image)
	return nil
}

// Spec is the container of a plugin's image: no network, no capability,
// nothing written but its socket's directory and a small /tmp, and
// started again by the engine when it stops, as at boot.
func Spec(id, plugin, image, dir string) runtime.ContainerSpec {
	return runtime.ContainerSpec{
		Name:          "murder-plugin-" + id,
		Image:         image,
		Env:           []string{"MURDER_PLUGIN_SOCKET=" + plugins.SocketDir + "/" + plugins.SocketName, GoMemoryLimit},
		User:          User,
		Labels:        map[string]string{runtime.LabelPlugin: plugin, LabelID: id, runtime.LabelImage: image},
		Network:       "none",
		Mounts:        []runtime.Mount{{Source: dir, Target: plugins.SocketDir}},
		MemoryBytes:   Memory,
		NanoCPUs:      CPUs * 1e9,
		PIDs:          PIDs,
		ReadOnlyRoot:  true,
		Tmpfs:         map[string]string{"/tmp": "rw,noexec,nosuid,size=64m"},
		RestartPolicy: "unless-stopped",
	}
}

func (h *Host) remove(ctx context.Context, id string, containers []runtime.Container) error {
	for _, container := range containers {
		if err := h.removeContainer(ctx, container); err != nil {
			return err
		}
		h.Log.Info("removed a plugin's container no plugin runs", "plugin", container.Labels[runtime.LabelPlugin], "container", container.Name)
	}
	return h.removeDir(filepath.Join(h.Root, id))
}

func (h *Host) removeContainer(ctx context.Context, container runtime.Container) error {
	if container.Running {
		if err := h.Runtime.Stop(ctx, container.ID, 30*time.Second); err != nil && !errors.Is(err, runtime.ErrNotFound) {
			return fmt.Errorf("stop %s: %w", container.Name, err)
		}
	}
	if err := h.Runtime.Remove(ctx, container.ID); err != nil && !errors.Is(err, runtime.ErrNotFound) {
		return fmt.Errorf("remove %s: %w", container.Name, err)
	}
	return nil
}

// ensureDir makes a socket's directory: a tmpfs of its own, the
// container's user's, that others may only pass through, in a root only
// its owner reaches (the edge, as root, without overriding permissions;
// the engine mounts the directory itself).
func (h *Host) ensureDir(dir string) error {
	if err := os.MkdirAll(h.Root, 0o700); err != nil {
		return err
	}
	if err := os.Chmod(h.Root, 0o700); err != nil {
		return err
	}
	if err := os.Mkdir(dir, 0o711); err != nil && !errors.Is(err, os.ErrExist) {
		return err
	}
	if !h.Tmpfs {
		return os.Chmod(dir, 0o777)
	}
	return mountTmpfs(dir, uid)
}

func (h *Host) removeDir(dir string) error {
	if h.Tmpfs {
		if err := unmountTmpfs(dir); err != nil {
			return fmt.Errorf("unmount %s: %w", dir, err)
		}
		if err := os.Remove(dir); err != nil && !errors.Is(err, os.ErrNotExist) {
			return err
		}
		return nil
	}
	return os.RemoveAll(dir)
}
