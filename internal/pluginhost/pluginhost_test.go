package pluginhost

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/PowerPenguini/murder/internal/plugins"
	"github.com/PowerPenguini/murder/internal/runtime"
	"github.com/PowerPenguini/murder/internal/store"
)

// engine is a container engine in memory.
type engine struct {
	mu         sync.Mutex
	images     map[string]bool
	pullFails  error
	containers map[string]*made
	next       int
	pulled     []string
}

type made struct {
	spec    runtime.ContainerSpec
	running bool
}

func (e *engine) HasImage(_ context.Context, ref string) (bool, error) {
	e.mu.Lock()
	defer e.mu.Unlock()
	return e.images[ref], nil
}

func (e *engine) PullImage(_ context.Context, ref string) error {
	e.mu.Lock()
	defer e.mu.Unlock()
	if e.pullFails != nil {
		return e.pullFails
	}
	e.images[ref] = true
	e.pulled = append(e.pulled, ref)
	return nil
}

func (e *engine) ListPlugins(context.Context) ([]runtime.Container, error) {
	e.mu.Lock()
	defer e.mu.Unlock()
	var list []runtime.Container
	for id, c := range e.containers {
		list = append(list, runtime.Container{ID: id, Name: c.spec.Name, Running: c.running, Labels: c.spec.Labels})
	}
	return list, nil
}

func (e *engine) Create(_ context.Context, spec runtime.ContainerSpec) (string, error) {
	e.mu.Lock()
	defer e.mu.Unlock()
	if !e.images[spec.Image] {
		return "", fmt.Errorf("no image %s", spec.Image)
	}
	e.next++
	id := fmt.Sprintf("c%d", e.next)
	e.containers[id] = &made{spec: spec}
	return id, nil
}

func (e *engine) Start(_ context.Context, id string) error {
	e.mu.Lock()
	defer e.mu.Unlock()
	e.containers[id].running = true
	return nil
}

func (e *engine) Stop(_ context.Context, id string, _ time.Duration) error {
	e.mu.Lock()
	defer e.mu.Unlock()
	e.containers[id].running = false
	return nil
}

func (e *engine) Remove(_ context.Context, id string) error {
	e.mu.Lock()
	defer e.mu.Unlock()
	if _, ok := e.containers[id]; !ok {
		return runtime.ErrNotFound
	}
	delete(e.containers, id)
	return nil
}

func (e *engine) running() []*made {
	e.mu.Lock()
	defer e.mu.Unlock()
	var list []*made
	for _, c := range e.containers {
		if c.running {
			list = append(list, c)
		}
	}
	return list
}

var (
	imageV1 = "ghcr.io/acme/plugin@sha256:" + strings.Repeat("1", 64)
	imageV2 = "ghcr.io/acme/plugin@sha256:" + strings.Repeat("2", 64)
)

type hostFixture struct {
	host    *Host
	engine  *engine
	plugins []store.Plugin
	now     time.Time
}

func newHost(t *testing.T) *hostFixture {
	t.Helper()
	f := &hostFixture{engine: &engine{images: map[string]bool{}, containers: map[string]*made{}}, now: time.Date(2026, 11, 21, 12, 0, 0, 0, time.UTC)}
	f.host = &Host{Runtime: f.engine, Plugins: func() []store.Plugin { return f.plugins }, Root: t.TempDir(), Log: slog.New(slog.DiscardHandler)}
	f.host.now = func() time.Time { return f.now }
	return f
}

func (f *hostFixture) install(image string) {
	manifest := plugins.Manifest{ABI: plugins.ABI, Name: "mine"}
	f.plugins = []store.Plugin{
		{Name: "openai", Spec: plugins.Spec{Kind: plugins.KindOpenAI}},
		{Name: "mine", Spec: plugins.Spec{Kind: plugins.KindContainer, Image: image}, Manifest: &manifest},
	}
}

func (f *hostFixture) sync(t *testing.T) {
	t.Helper()
	if err := f.host.Sync(context.Background()); err != nil {
		t.Fatal(err)
	}
}

// A plugin of kind container gets one container on the node: of its
// image, pulled, with no network, as nobody, its socket's directory
// mounted, and started again by the engine; a plugin of Murder's own
// kind gets none.
func TestAPluginGetsAContainerOfItsImage(t *testing.T) {
	f := newHost(t)
	f.install(imageV1)
	f.sync(t)
	f.sync(t)
	running := f.engine.running()
	if len(running) != 1 || len(f.engine.pulled) != 1 {
		t.Fatalf("running %d, pulled %v", len(running), f.engine.pulled)
	}
	spec := running[0].spec
	id := plugins.ContainerID("mine", imageV1)
	dir := filepath.Join(f.host.Root, id)
	if spec.Image != imageV1 || spec.Network != "none" || spec.User != User || !spec.ReadOnlyRoot || spec.RestartPolicy != "unless-stopped" ||
		spec.Labels[runtime.LabelPlugin] != "mine" || spec.Labels[LabelID] != id || spec.MemoryBytes != Memory ||
		len(spec.Mounts) != 1 || spec.Mounts[0].Source != dir || spec.Mounts[0].Target != plugins.SocketDir ||
		spec.Env[0] != "MURDER_PLUGIN_SOCKET=/run/murder-plugin/plugin.sock" {
		t.Fatalf("spec %+v", spec)
	}
	if info, err := os.Stat(dir); err != nil || !info.IsDir() {
		t.Fatalf("its socket's directory: %v", err)
	}
	// No user of the host but the root's owner reaches the sockets.
	if info, err := os.Stat(f.host.Root); err != nil || info.Mode().Perm() != 0o700 {
		t.Fatalf("the sockets' root: %v %v", info.Mode(), err)
	}
	// One that stopped is started again.
	for _, c := range f.engine.containers {
		c.running = false
	}
	f.sync(t)
	if len(f.engine.running()) != 1 || len(f.engine.containers) != 1 {
		t.Fatalf("after it stopped: %d running of %d", len(f.engine.running()), len(f.engine.containers))
	}
}

// A new image starts a new container at once; the old one serves the
// edge's requests under way, and goes once Retire passed, with its
// socket's directory. So does a plugin's that went.
func TestAnOldImagesContainerGoesAWhileAfter(t *testing.T) {
	f := newHost(t)
	f.install(imageV1)
	f.sync(t)
	f.install(imageV2)
	f.sync(t)
	if len(f.engine.running()) != 2 {
		t.Fatalf("running %d: the old one went at once", len(f.engine.running()))
	}
	f.now = f.now.Add(Retire + time.Second)
	f.sync(t)
	running := f.engine.running()
	if len(running) != 1 || running[0].spec.Image != imageV2 || len(f.engine.containers) != 1 {
		t.Fatalf("after Retire: %d running, of %d", len(running), len(f.engine.containers))
	}
	if _, err := os.Stat(filepath.Join(f.host.Root, plugins.ContainerID("mine", imageV1))); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("the old socket's directory: %v", err)
	}
	f.plugins = nil
	f.sync(t)
	f.now = f.now.Add(Retire + time.Second)
	f.sync(t)
	if len(f.engine.containers) != 0 {
		t.Fatalf("a plugin gone keeps %d containers", len(f.engine.containers))
	}
	if entries, _ := os.ReadDir(f.host.Root); len(entries) != 0 {
		t.Fatalf("directories left: %v", entries)
	}
}

// An image that cannot be pulled is tried again at the next pass.
func TestAnImageNotPulledIsTriedAgain(t *testing.T) {
	f := newHost(t)
	f.install(imageV1)
	f.engine.pullFails = errors.New("registry down")
	if err := f.host.Sync(context.Background()); err == nil || !strings.Contains(err.Error(), "registry down") {
		t.Fatalf("sync: %v", err)
	}
	f.engine.pullFails = nil
	f.sync(t)
	if len(f.engine.running()) != 1 {
		t.Fatal("the image pulled later runs no container")
	}
}
