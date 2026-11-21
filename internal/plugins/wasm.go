package plugins

import (
	"bytes"
	"context"
	"crypto/rand"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"maps"
	"net/http"
	"net/textproto"
	"net/url"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"sync"
	"time"

	"github.com/PowerPenguini/murder/internal/metrics"
	"github.com/tetratelabs/wazero"
	"github.com/tetratelabs/wazero/api"
	"github.com/tetratelabs/wazero/imports/wasi_snapshot_preview1"
)

// The functions a module exports, by ABI version 1 (see docs/plugins.md).
// Each that answers gives a pointer and a length, the pointer in the high
// 32 bits; what it points at holds until the next call into the module.
// Memory murder_alloc gives the edge for an input is the module's again
// once the call it was for returns.
var moduleExports = map[string]struct{ params, results []api.ValueType }{
	"murder_alloc":        {[]api.ValueType{api.ValueTypeI32}, []api.ValueType{api.ValueTypeI32}},
	"murder_manifest":     {nil, []api.ValueType{api.ValueTypeI64}},
	"murder_request":      {[]api.ValueType{api.ValueTypeI32, api.ValueTypeI32, api.ValueTypeI32, api.ValueTypeI32}, []api.ValueType{api.ValueTypeI64}},
	"murder_request_body": {nil, []api.ValueType{api.ValueTypeI64}},
	"murder_response":     {[]api.ValueType{api.ValueTypeI32, api.ValueTypeI32}, []api.ValueType{api.ValueTypeI64}},
	"murder_body":         {[]api.ValueType{api.ValueTypeI32, api.ValueTypeI32}, []api.ValueType{api.ValueTypeI32}},
	"murder_end":          {[]api.ValueType{api.ValueTypeI32, api.ValueTypeI32}, []api.ValueType{api.ValueTypeI64}},
}

// How long a call into a module may take before the instance is stopped:
// a request's may read a body of MaxModuleBody, which a module built with
// Go reads as JSON in about 5 seconds, interpreted.
var callRequest = 10 * time.Second

const (
	callManifest = 5 * time.Second
	callResponse = time.Second
	callBody     = time.Second
	callEnd      = 10 * time.Second
	// maxAnswer is the most a module answers to one call, but for a
	// request's body.
	maxAnswer = 1 << 20
	// maxCounts is the most counts one answer holds.
	maxCounts = 64
)

// The bounds of the instances an edge runs: at most maxInstances at once,
// maxIdle of them idle for each tenant and module, each kept for at most
// maxUses requests, and not once it grew past idleMemory.
const (
	maxInstances = 256
	maxIdle      = 4
	maxUses      = 1000
	idleMemory   = 16 << 20
	// retryLoad is how soon a module that could not be loaded is tried
	// again; waitLoad how long a request waits for one being loaded.
	retryLoad = 30 * time.Second
	waitLoad  = 30 * time.Second
)

// runtimeConfig runs modules interpreted: compiled, they would need memory
// both written and run, which the edge's unit denies it
// (MemoryDenyWriteExecute), and the native code of a module would run in
// the process that holds every key.
func runtimeConfig() wazero.RuntimeConfig {
	return wazero.NewRuntimeConfigInterpreter().WithMemoryLimitPages(moduleMemory).WithCloseOnContextDone(true)
}

// checkModule says what is wrong with a compiled module: it may import
// only WASI's functions, and must export the functions of the ABI, with
// their signatures, and its memory.
func checkModule(compiled wazero.CompiledModule) error {
	var problems []string
	for _, function := range compiled.ImportedFunctions() {
		if module, name, _ := function.Import(); module != wasi_snapshot_preview1.ModuleName {
			problems = append(problems, fmt.Sprintf("it imports %s.%s: a module may import only WASI's functions (%s)", module, name, wasi_snapshot_preview1.ModuleName))
		}
	}
	if len(compiled.ImportedMemories()) > 0 {
		problems = append(problems, "it imports memory: a module has its own")
	}
	if _, ok := compiled.ExportedMemories()["memory"]; !ok {
		problems = append(problems, "it exports no memory")
	}
	exported := compiled.ExportedFunctions()
	for _, name := range slices.Sorted(maps.Keys(moduleExports)) {
		want := moduleExports[name]
		function, ok := exported[name]
		switch {
		case !ok:
			problems = append(problems, "it exports no "+name)
		case !slices.Equal(function.ParamTypes(), want.params) || !slices.Equal(function.ResultTypes(), want.results):
			problems = append(problems, fmt.Sprintf("%s takes %s and gives %s, not %s and %s", name, typesOf(function.ParamTypes()), typesOf(function.ResultTypes()), typesOf(want.params), typesOf(want.results)))
		}
	}
	if len(problems) > 0 {
		return errors.New("the module is not a plugin's: " + strings.Join(problems, "; "))
	}
	return nil
}

func typesOf(types []api.ValueType) string {
	if len(types) == 0 {
		return "nothing"
	}
	names := make([]string, len(types))
	for i, t := range types {
		names[i] = api.ValueTypeName(t)
	}
	return "(" + strings.Join(names, ", ") + ")"
}

// Inspect checks a module and reads its manifest, running it once, with
// nothing of the edge's around it: the agent does so as it is installed.
func Inspect(ctx context.Context, wasm []byte) (Manifest, error) {
	if len(wasm) > MaxModule {
		return Manifest{}, fmt.Errorf("a module is at most %d MB", MaxModule>>20)
	}
	runtime := wazero.NewRuntimeWithConfig(ctx, runtimeConfig())
	defer runtime.Close(context.WithoutCancel(ctx))
	if _, err := wasi_snapshot_preview1.Instantiate(ctx, runtime); err != nil {
		return Manifest{}, err
	}
	compiled, err := runtime.CompileModule(ctx, wasm)
	if err != nil {
		return Manifest{}, fmt.Errorf("the module is not WebAssembly the edge runs: %w", err)
	}
	if err := checkModule(compiled); err != nil {
		return Manifest{}, err
	}
	output := &tail{}
	instance, err := instantiate(ctx, runtime, compiled, output)
	if err != nil {
		return Manifest{}, fmt.Errorf("the module does not start: %w%s", err, output.said())
	}
	defer instance.mod.Close(context.WithoutCancel(ctx))
	answer, err := instance.call(callManifest, "murder_manifest")
	if err != nil {
		return Manifest{}, fmt.Errorf("the module says no manifest: %w%s", err, output.said())
	}
	var manifest Manifest
	decoder := json.NewDecoder(bytes.NewReader(answer))
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(&manifest); err != nil {
		return Manifest{}, fmt.Errorf("the module's manifest: %w", err)
	}
	if err := manifest.Validate(); err != nil {
		return Manifest{}, fmt.Errorf("the module's manifest: %w", err)
	}
	return manifest, nil
}

// tail keeps the last of what a module writes, to log with its failures.
type tail struct {
	mu   sync.Mutex
	kept []byte
}

const tailKeptOutput = 2 << 10

func (t *tail) Write(p []byte) (int, error) {
	t.mu.Lock()
	defer t.mu.Unlock()
	t.kept = append(t.kept, p...)
	if len(t.kept) > tailKeptOutput {
		t.kept = append(t.kept[:0], t.kept[len(t.kept)-tailKeptOutput:]...)
	}
	return len(p), nil
}

// said is what the module wrote, for an error: empty for nothing.
func (t *tail) said() string {
	t.mu.Lock()
	defer t.mu.Unlock()
	if text := strings.TrimSpace(string(t.kept)); text != "" {
		return "; it wrote: " + text
	}
	return ""
}

func (t *tail) reset() {
	t.mu.Lock()
	defer t.mu.Unlock()
	t.kept = t.kept[:0]
}

// instance is one instance of a module, serving one request at a time,
// of one tenant.
type instance struct {
	module *module
	tenant string
	mod    api.Module
	output *tail
	uses   int
}

// instantiate starts an instance: no files, no environment, no arguments
// and no sockets; a clock, randomness, and what it writes kept for logs.
// Its sleeps return at once: one would hold the instance past its call's
// deadline, which a host function does not heed.
func instantiate(ctx context.Context, runtime wazero.Runtime, compiled wazero.CompiledModule, output *tail) (*instance, error) {
	config := wazero.NewModuleConfig().WithName("").WithStartFunctions("_initialize").
		WithStdout(output).WithStderr(output).WithRandSource(rand.Reader).
		WithSysWalltime().WithSysNanotime()
	started, cancel := context.WithTimeout(context.WithoutCancel(ctx), callRequest)
	defer cancel()
	mod, err := runtime.InstantiateModule(started, compiled, config)
	if err != nil {
		return nil, err
	}
	return &instance{mod: mod, output: output}, nil
}

// call calls a function of the instance with inputs, each written into
// its memory and given as a pointer and a length, and reads its answer.
// It runs for at most timeout, whatever becomes of the request: what a
// tenant stops waiting for is counted all the same.
func (i *instance) call(timeout time.Duration, name string, inputs ...[]byte) ([]byte, error) {
	results, err := i.invoke(timeout, name, inputs...)
	if err != nil {
		return nil, err
	}
	pointer, length := uint32(results[0]>>32), uint32(results[0])
	limit := uint32(maxAnswer)
	if name == "murder_request_body" {
		limit = MaxModuleBody
	}
	if length > limit {
		return nil, fmt.Errorf("%s answered %d bytes, more than %d", name, length, limit)
	}
	answer, ok := i.mod.Memory().Read(pointer, length)
	if !ok {
		return nil, fmt.Errorf("%s answered outside its memory", name)
	}
	return bytes.Clone(answer), nil
}

// invoke calls a function of the instance with inputs, and gives its
// results as they are.
func (i *instance) invoke(timeout time.Duration, name string, inputs ...[]byte) ([]uint64, error) {
	ctx, cancel := context.WithTimeout(context.Background(), timeout)
	defer cancel()
	args := make([]uint64, 0, 2*len(inputs))
	for _, input := range inputs {
		if len(input) == 0 {
			args = append(args, 0, 0)
			continue
		}
		pointer, err := i.mod.ExportedFunction("murder_alloc").Call(ctx, uint64(len(input)))
		if err != nil {
			return nil, fmt.Errorf("murder_alloc: %w", err)
		}
		if !i.mod.Memory().Write(uint32(pointer[0]), input) {
			return nil, errors.New("murder_alloc gave memory outside its memory")
		}
		args = append(args, pointer[0], uint64(len(input)))
	}
	results, err := i.mod.ExportedFunction(name).Call(ctx, args...)
	if err != nil {
		if ctx.Err() != nil {
			return nil, fmt.Errorf("%s took more than %s", name, timeout)
		}
		return nil, fmt.Errorf("%s: %w", name, err)
	}
	return results, nil
}

// Modules runs plugins' modules for the edge: it fetches each by its
// digest, keeps it on disk, compiles it, and keeps instances of it ready
// for each tenant. An instance serves one tenant only, one request at a
// time: what a module keeps between requests is never another tenant's.
type Modules struct {
	runtime wazero.Runtime
	fetch   func(ctx context.Context, digest string) ([]byte, error)
	dir     string
	log     *slog.Logger

	mu      sync.Mutex
	modules map[string]*module
	live    int
}

// module is a module the edge loads, or loaded.
type module struct {
	digest string
	// ready is closed once the module is compiled, or could not be.
	ready    chan struct{}
	compiled wazero.CompiledModule
	err      error
	failedAt time.Time
	idle     map[string][]*instance
	active   int
	dropped  bool
}

// NewModules runs modules, fetched with fetch and kept in dir, the edge's
// own. Without dir, modules are fetched each time the edge starts.
func NewModules(ctx context.Context, dir string, fetch func(ctx context.Context, digest string) ([]byte, error), log *slog.Logger) (*Modules, error) {
	if dir != "" {
		if err := os.MkdirAll(dir, 0o700); err != nil {
			return nil, err
		}
		if err := os.Chmod(dir, 0o700); err != nil {
			return nil, err
		}
	}
	runtime := wazero.NewRuntimeWithConfig(ctx, runtimeConfig())
	if _, err := wasi_snapshot_preview1.Instantiate(ctx, runtime); err != nil {
		return nil, err
	}
	return &Modules{runtime: runtime, fetch: fetch, dir: dir, log: log, modules: map[string]*module{}}, nil
}

// Close stops every instance.
func (m *Modules) Close(ctx context.Context) error { return m.runtime.Close(ctx) }

// Prepare loads the modules config's plugins run, ahead of their
// requests, and lets go of those it no longer runs, and of instances
// kept for tenants gone.
func (m *Modules) Prepare(config Config) {
	wanted := config.Modules()
	m.mu.Lock()
	defer m.mu.Unlock()
	for _, digest := range wanted {
		if existing := m.modules[digest]; existing == nil || existing.dropped || m.retry(existing) {
			m.load(digest)
		}
	}
	for digest, mod := range m.modules {
		if !slices.Contains(wanted, digest) {
			mod.dropped = true
		}
		for tenant, idle := range mod.idle {
			if _, ok := config.Tenants[tenant]; !ok || mod.dropped {
				for _, instance := range idle {
					m.stop(instance)
				}
				delete(mod.idle, tenant)
			}
		}
		m.forget(mod)
	}
}

// retry reports whether a module that could not be loaded is tried
// again.
func (m *Modules) retry(mod *module) bool {
	select {
	case <-mod.ready:
		return mod.err != nil && time.Since(mod.failedAt) > retryLoad
	default:
		return false
	}
}

// load loads a module, in the background; m.mu is held.
func (m *Modules) load(digest string) *module {
	mod := &module{digest: digest, ready: make(chan struct{}), idle: map[string][]*instance{}}
	if previous := m.modules[digest]; previous != nil {
		// Its instances, if any, finish their requests.
		previous.dropped = true
	}
	m.modules[digest] = mod
	go func() {
		compiled, err := m.compile(digest)
		m.mu.Lock()
		defer m.mu.Unlock()
		mod.compiled, mod.err = compiled, err
		if err != nil {
			mod.failedAt = time.Now()
			m.log.Error("a plugin's module cannot be loaded; its plugins answer 503", "module", digest, "error", err)
		}
		close(mod.ready)
		m.forget(mod)
	}()
	return mod
}

// forget lets go of a module dropped once nothing of it runs; m.mu is
// held.
func (m *Modules) forget(mod *module) {
	select {
	case <-mod.ready:
	default:
		return
	}
	if !mod.dropped || mod.active > 0 || len(mod.idle) > 0 {
		return
	}
	if mod.compiled != nil {
		_ = mod.compiled.Close(context.Background())
	}
	if m.modules[mod.digest] == mod {
		delete(m.modules, mod.digest)
	}
}

// compile reads a module, from disk or else fetched, and compiles it.
func (m *Modules) compile(digest string) (wazero.CompiledModule, error) {
	wasm, err := m.read(digest)
	if err != nil {
		return nil, err
	}
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	defer cancel()
	compiled, err := m.runtime.CompileModule(ctx, wasm)
	if err != nil {
		return nil, err
	}
	if err := checkModule(compiled); err != nil {
		_ = compiled.Close(ctx)
		return nil, err
	}
	return compiled, nil
}

// read reads a module kept on disk, or fetches it and keeps it; either
// must be of its digest.
func (m *Modules) read(digest string) ([]byte, error) {
	sum, ok := strings.CutPrefix(digest, "sha256:")
	if decoded, err := hex.DecodeString(sum); !ok || err != nil || len(decoded) != sha256.Size {
		return nil, fmt.Errorf("%q is not a module's digest", digest)
	}
	path := ""
	if m.dir != "" {
		path = filepath.Join(m.dir, sum+".wasm")
		if wasm, err := os.ReadFile(path); err == nil && ModuleDigest(sha256.Sum256(wasm)) == digest {
			return wasm, nil
		}
	}
	if m.fetch == nil {
		return nil, errors.New("the edge has no agent to fetch it from")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()
	wasm, err := m.fetch(ctx, digest)
	if err != nil {
		return nil, fmt.Errorf("fetch it from the agent: %w", err)
	}
	if ModuleDigest(sha256.Sum256(wasm)) != digest {
		return nil, errors.New("the agent sent a module of another digest")
	}
	if path != "" {
		if err := writeFile(path, wasm); err != nil {
			m.log.Warn("keep a plugin's module on disk", "module", digest, "error", err)
		}
	}
	return wasm, nil
}

func writeFile(path string, data []byte) error {
	file, err := os.CreateTemp(filepath.Dir(path), ".module-*")
	if err != nil {
		return err
	}
	defer os.Remove(file.Name())
	if _, err := file.Write(data); err != nil {
		file.Close()
		return err
	}
	if err := file.Close(); err != nil {
		return err
	}
	return os.Rename(file.Name(), path)
}

// errModuleBusy refuses a request when the edge runs as many instances as
// it may.
var errModuleBusy = errors.New("the edge runs as many plugin modules at once as it may; try again")

// acquire takes an instance of a module for a tenant's request: one kept
// idle for the tenant, or a new one.
func (m *Modules) acquire(ctx context.Context, digest, tenant string) (*instance, error) {
	m.mu.Lock()
	mod := m.modules[digest]
	if mod == nil || mod.dropped || m.retry(mod) {
		mod = m.load(digest)
	}
	m.mu.Unlock()
	wait := time.NewTimer(waitLoad)
	defer wait.Stop()
	select {
	case <-mod.ready:
	case <-wait.C:
		return nil, errors.New("the plugin's module is still being loaded; try again")
	case <-ctx.Done():
		return nil, ctx.Err()
	}
	if mod.err != nil {
		return nil, fmt.Errorf("the plugin's module cannot be loaded: %w", mod.err)
	}
	m.mu.Lock()
	if idle := mod.idle[tenant]; len(idle) > 0 {
		instance := idle[len(idle)-1]
		if mod.idle[tenant] = idle[:len(idle)-1]; len(mod.idle[tenant]) == 0 {
			delete(mod.idle, tenant)
		}
		mod.active++
		m.mu.Unlock()
		return instance, nil
	}
	if m.live >= maxInstances && !m.evict() {
		m.mu.Unlock()
		return nil, errModuleBusy
	}
	m.live++
	mod.active++
	m.mu.Unlock()
	output := &tail{}
	instance, err := instantiate(ctx, m.runtime, mod.compiled, output)
	if err != nil {
		m.mu.Lock()
		m.live--
		mod.active--
		m.forget(mod)
		m.mu.Unlock()
		return nil, fmt.Errorf("the plugin's module does not start: %w%s", err, output.said())
	}
	instance.module, instance.tenant = mod, tenant
	return instance, nil
}

// evict stops an idle instance, to make room for another; m.mu is held.
func (m *Modules) evict() bool {
	for _, mod := range m.modules {
		for tenant, idle := range mod.idle {
			m.stop(idle[0])
			if mod.idle[tenant] = idle[1:]; len(mod.idle[tenant]) == 0 {
				delete(mod.idle, tenant)
			}
			return true
		}
	}
	return false
}

// stop stops an instance; m.mu is held.
func (m *Modules) stop(instance *instance) {
	_ = instance.mod.Close(context.Background())
	m.live--
}

// release gives an instance back once its request is done: kept for the
// tenant's next if it served well, stopped otherwise.
func (m *Modules) release(instance *instance, ok bool) {
	instance.uses++
	instance.output.reset()
	keep := ok && instance.uses < maxUses && !instance.mod.IsClosed() && instance.mod.Memory().Size() <= idleMemory
	m.mu.Lock()
	defer m.mu.Unlock()
	mod := instance.module
	mod.active--
	if keep && !mod.dropped && len(mod.idle[instance.tenant]) < maxIdle {
		mod.idle[instance.tenant] = append(mod.idle[instance.tenant], instance)
		return
	}
	m.stop(instance)
	m.forget(mod)
}

// The JSON a module takes and gives (see docs/plugins.md).
type (
	moduleRequest struct {
		Tenant string              `json:"tenant"`
		Plugin string              `json:"plugin"`
		Method string              `json:"method"`
		Path   string              `json:"path"`
		Query  string              `json:"query,omitempty"`
		Header map[string][]string `json:"header"`
		Config map[string]any      `json:"config,omitempty"`
	}
	moduleForward struct {
		URL    string              `json:"url"`
		Header map[string][]string `json:"header"`
		Drop   []string            `json:"drop"`
		Body   bool                `json:"body"`
		Detail string              `json:"detail"`
		Meter  bool                `json:"meter"`
		Refuse *struct {
			Status  int    `json:"status"`
			Message string `json:"message"`
		} `json:"refuse"`
		Error string `json:"error"`
	}
	moduleResponse struct {
		Status int                 `json:"status"`
		Header map[string][]string `json:"header"`
	}
	moduleMetered struct {
		Counts []moduleCount `json:"counts"`
		Body   bool          `json:"body"`
		Error  string        `json:"error"`
	}
	moduleCount struct {
		Metric string `json:"metric"`
		Detail string `json:"detail"`
		Amount int64  `json:"amount"`
	}
	moduleEnd struct {
		Complete bool `json:"complete"`
	}
)

// WithModules runs plugins of kind wasm with modules.
func (p *Proxy) WithModules(modules *Modules) *Proxy {
	p.modules = modules
	return p
}

// wasmHandler passes a request on as a plugin's module says, and counts
// what the module reads in its answer.
type wasmHandler struct {
	x        *exchange
	manifest Manifest
	hosts    []string
	modules  *Modules

	mu       sync.Mutex
	instance *instance
	failed   bool
	meter    bool
	reading  bool
	ended    bool
}

func (p *Proxy) wasmHandler(ctx context.Context, x *exchange, plugin Plugin) (handler, error) {
	if p.modules == nil || plugin.Manifest == nil {
		return nil, statusError{code: http.StatusServiceUnavailable, message: fmt.Sprintf("this edge runs no modules for plugin %s", x.plugin), kind: "murder_plugin_unavailable"}
	}
	instance, err := p.modules.acquire(ctx, plugin.Spec.Module, x.tenant)
	if err != nil {
		p.log.Warn("a plugin's module serves no request", "plugin", x.plugin, "tenant", x.tenant, "error", err)
		return nil, statusError{code: http.StatusServiceUnavailable, message: fmt.Sprintf("plugin %s: %v", x.plugin, err), kind: "murder_plugin_unavailable"}
	}
	return &wasmHandler{x: x, manifest: *plugin.Manifest, hosts: hostsOf(plugin.Spec, *plugin.Manifest), modules: p.modules, instance: instance}, nil
}

// failure is what a request is answered when the module failed before it
// went on.
var failure = statusError{code: http.StatusBadGateway, message: "the plugin's module failed; the edge's log says how", kind: "murder_plugin_failed"}

// fail stops the instance, which may have been left anyhow, and says why.
func (h *wasmHandler) fail(stage string, err error) {
	h.failed = true
	h.x.proxy.moduleFailed(h.x.plugin)
	said := ""
	if h.instance != nil {
		said = h.instance.output.said()
	}
	h.x.proxy.log.Error("a plugin's module failed; what it was to count after is not counted", "plugin", h.x.plugin, "tenant", h.x.tenant, "stage", stage,
		"error", h.scrub(err.Error()+said))
}

// scrub takes the key out of what a module says.
func (h *wasmHandler) scrub(text string) string {
	if h.x.key == "" {
		return text
	}
	return strings.ReplaceAll(strings.ReplaceAll(text, h.x.key, "[key]"), url.QueryEscape(h.x.key), "[key]")
}

// credentials are the headers a tenant's credentials come in, never shown
// to a module.
var credentials = []string{"Authorization", "Api-Key", "X-Api-Key", "Cookie", "Proxy-Authorization", "Openai-Organization", "Openai-Project", "X-Forwarded-For"}

func (h *wasmHandler) request(r *http.Request, rest string) (*url.URL, error) {
	h.mu.Lock()
	defer h.mu.Unlock()
	// A path that climbs, written as it is or escaped, would get past the
	// endpoints a module passes on.
	if !plainPath(rest) {
		return nil, statusError{code: http.StatusBadRequest, message: "the path is not one to pass on"}
	}
	var body []byte
	if h.manifest.RequestBody > 0 && r.Body != nil && r.Body != http.NoBody {
		var err error
		if body, err = h.x.readBodyUpTo(r, int(h.manifest.RequestBody)); err != nil {
			return nil, err
		}
	}
	header := r.Header.Clone()
	for _, name := range append(slices.Clip(credentials), h.manifest.Key.Header) {
		if name != "" {
			header.Del(name)
		}
	}
	query := r.URL.RawQuery
	if h.manifest.Key.Query != "" {
		query = withoutKey(query, h.manifest.Key.Query)
	}
	input, err := json.Marshal(moduleRequest{Tenant: h.x.tenant, Plugin: h.x.plugin, Method: r.Method, Path: "/" + rest, Query: query, Header: header, Config: h.x.spec.Config})
	if err != nil {
		return nil, err
	}
	answer, err := h.instance.call(callRequest, "murder_request", input, body)
	if err != nil {
		h.fail("request", err)
		return nil, failure
	}
	var forward moduleForward
	if err := json.Unmarshal(answer, &forward); err != nil {
		h.fail("request", fmt.Errorf("murder_request answered no JSON of a request: %w", err))
		return nil, failure
	}
	if forward.Error != "" {
		h.fail("request", errors.New(forward.Error))
		return nil, failure
	}
	if refusal := forward.Refuse; refusal != nil {
		status := refusal.Status
		if status < 400 || status > 599 {
			status = http.StatusBadRequest
		}
		message := refusal.Message
		if len(message) > 1024 {
			message = message[:1024]
		}
		return nil, statusError{code: status, message: h.scrub(message), kind: "murder_plugin_refused"}
	}
	target, err := h.target(forward.URL)
	if err == nil {
		err = h.headers(r.Header, forward)
	}
	if err == nil && forward.Body {
		var replaced []byte
		if replaced, err = h.instance.call(callRequest, "murder_request_body"); err == nil {
			r.Body = io.NopCloser(bytes.NewReader(replaced))
			r.ContentLength = int64(len(replaced))
			r.Header.Set("Content-Length", fmt.Sprint(len(replaced)))
			r.GetBody = func() (io.ReadCloser, error) { return io.NopCloser(bytes.NewReader(replaced)), nil }
		}
	}
	if err != nil {
		h.fail("request", err)
		return nil, failure
	}
	h.x.detail = cleanDetail(forward.Detail)
	h.meter = forward.Meter
	return target, nil
}

// target checks where a module sends a request: over HTTPS, to a host of
// the plugin's, which the key may go to.
func (h *wasmHandler) target(raw string) (*url.URL, error) {
	target, err := url.Parse(raw)
	if err != nil {
		return nil, fmt.Errorf("the module sends the request to %q, not a URL", raw)
	}
	host := target.Hostname()
	switch {
	case target.Scheme != "https":
		return nil, fmt.Errorf("the module sends the request over %q: only https carries the key", target.Scheme)
	case target.User != nil || target.Opaque != "" || target.Fragment != "":
		return nil, errors.New("the module sends the request to a URL with a user, or a fragment")
	case target.Port() != "" && target.Port() != "443":
		return nil, fmt.Errorf("the module sends the request to port %s: only 443 carries the key", target.Port())
	case host != strings.ToLower(host) || hostRuleIn(h.hosts, host) == "":
		return nil, fmt.Errorf("the module sends the request to %s, which is not one of the plugin's hosts (%s)", host, strings.Join(h.hosts, ", "))
	}
	if !plainPath(target.EscapedPath()) {
		return nil, fmt.Errorf("the module sends the request to a path that climbs: %s", target.EscapedPath())
	}
	return &url.URL{Scheme: "https", Host: target.Host, Path: target.Path, RawPath: target.RawPath, RawQuery: target.RawQuery}, nil
}

// plainPath reports whether an escaped path neither climbs nor stays, as
// in /../ or /./, written as it is or escaped, and escapes no slash.
func plainPath(escaped string) bool {
	lower := strings.ToLower(escaped)
	if strings.Contains(lower, "%2f") || strings.Contains(lower, "%5c") || strings.Contains(escaped, "\\") {
		return false
	}
	unescaped, err := url.PathUnescape(escaped)
	if err != nil {
		return false
	}
	padded := "/" + unescaped + "/"
	return !strings.Contains(padded, "/../") && !strings.Contains(padded, "/./")
}

// headers changes the request's headers as the module says: never those
// that say how it is carried, nor the key's.
func (h *wasmHandler) headers(header http.Header, forward moduleForward) error {
	check := func(name string) error {
		if !headerToken.MatchString(name) || reservedHeader(name) || strings.EqualFold(name, h.manifest.Key.Header) {
			return fmt.Errorf("the module may not set header %q", name)
		}
		return nil
	}
	for _, name := range forward.Drop {
		if err := check(name); err != nil {
			return err
		}
		header.Del(name)
	}
	for name, values := range forward.Header {
		if err := check(name); err != nil {
			return err
		}
		for _, value := range values {
			if strings.ContainsAny(value, "\r\n\x00") {
				return fmt.Errorf("the module sets header %s to a value with a line break", name)
			}
		}
		header[textproto.CanonicalMIMEHeaderKey(name)] = slices.Clone(values)
	}
	return nil
}

// authorize puts the key where the module's manifest says.
func (h *wasmHandler) authorize(out *http.Request) {
	if key := h.manifest.Key; key.Header != "" {
		out.Header.Set(key.Header, key.Prefix+h.x.key)
	} else {
		out.URL.RawQuery = withKey(out.URL.RawQuery, key.Query, h.x.key)
	}
	if h.meter || h.manifest.Key.Query != "" {
		// The module reads the answer uncompressed, and an error is read
		// so to take a key in the query out of it.
		out.Header.Set("Accept-Encoding", "identity")
	}
}

func (h *wasmHandler) response(response *http.Response) {
	h.mu.Lock()
	defer h.mu.Unlock()
	if h.manifest.Key.Query != "" {
		scrubErrorBody(response, h.x.key)
	}
	if !h.meter || h.failed {
		return
	}
	input, err := json.Marshal(moduleResponse{Status: response.StatusCode, Header: response.Header})
	if err != nil {
		h.fail("response", err)
		return
	}
	metered, err := h.metered("murder_response", callResponse, input)
	if err != nil {
		h.fail("response", err)
		return
	}
	if metered.Body {
		h.reading = true
		response.Body = &moduleBody{h: h, body: response.Body}
	}
}

// metered calls a function that answers counts, and counts them.
func (h *wasmHandler) metered(name string, timeout time.Duration, input []byte) (moduleMetered, error) {
	answer, err := h.instance.call(timeout, name, input)
	if err != nil {
		return moduleMetered{}, err
	}
	var metered moduleMetered
	if err := json.Unmarshal(answer, &metered); err != nil {
		return moduleMetered{}, fmt.Errorf("%s answered no JSON of counts: %w", name, err)
	}
	if metered.Error != "" {
		return moduleMetered{}, errors.New(metered.Error)
	}
	if len(metered.Counts) > maxCounts {
		return moduleMetered{}, fmt.Errorf("%s answered %d counts, more than %d", name, len(metered.Counts), maxCounts)
	}
	metrics := MetricsOf(Spec{Kind: KindWasm}, &h.manifest)
	for _, count := range metered.Counts {
		switch {
		case count.Metric == MetricRequests || !slices.Contains(metrics, count.Metric):
			return metered, fmt.Errorf("%s counts %q, which is not one of the metrics its manifest says (%s)", name, count.Metric, strings.Join(h.manifest.Metrics, ", "))
		case count.Amount < 0 || count.Amount > MaxCount:
			return metered, fmt.Errorf("%s counts %d of %s, not an amount from 0 to 2^40", name, count.Amount, count.Metric)
		}
	}
	for _, count := range metered.Counts {
		if count.Amount > 0 {
			h.x.count(count.Metric, cleanDetail(count.Detail), count.Amount)
		}
	}
	return metered, nil
}

// chunk shows the module a chunk of the answer's body; it reports
// whether the module wants more.
func (h *wasmHandler) chunk(data []byte) bool {
	h.mu.Lock()
	defer h.mu.Unlock()
	if h.failed || h.ended {
		return false
	}
	results, err := h.instance.invoke(callBody, "murder_body", data)
	if err != nil {
		h.fail("body", err)
		return false
	}
	switch code := uint32(results[0]); code {
	case 0:
		return true
	case 1:
		return false
	default:
		h.fail("body", fmt.Errorf("murder_body answered %d: it failed", code))
		return false
	}
}

// end tells the module the answer's body ended, read whole or not, and
// counts what it says.
func (h *wasmHandler) end(complete bool) {
	h.mu.Lock()
	defer h.mu.Unlock()
	if h.ended || h.failed || !h.reading {
		return
	}
	h.ended = true
	input, _ := json.Marshal(moduleEnd{Complete: complete})
	if _, err := h.metered("murder_end", callEnd, input); err != nil {
		h.fail("end", err)
	}
}

func (h *wasmHandler) close() {
	h.end(false)
	h.mu.Lock()
	defer h.mu.Unlock()
	if h.instance != nil {
		h.modules.release(h.instance, !h.failed)
		h.instance = nil
	}
}

// moduleBody shows a module the answer's body as it passes.
type moduleBody struct {
	h      *wasmHandler
	body   io.ReadCloser
	done   bool
	enough bool
}

func (b *moduleBody) Read(buffer []byte) (int, error) {
	n, err := b.body.Read(buffer)
	if n > 0 && !b.enough {
		b.enough = !b.h.chunk(buffer[:n])
	}
	if err != nil && !b.done {
		b.done = true
		b.h.end(err == io.EOF)
	}
	return n, err
}

func (b *moduleBody) Close() error {
	if !b.done {
		b.done = true
		b.h.end(false)
	}
	return b.body.Close()
}

// moduleFailed counts a failure of a plugin's module.
func (p *Proxy) moduleFailed(plugin string) {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.failures[plugin]++
}

// WriteMetrics writes how often each plugin's module failed.
func (p *Proxy) WriteMetrics(out *metrics.Writer) {
	p.mu.Lock()
	failures := maps.Clone(p.failures)
	p.mu.Unlock()
	if len(failures) == 0 {
		return
	}
	out.Family("murder_plugin_module_failures_total", "counter", "How often a plugin's module failed, since the edge started: a request it failed before is answered 502, one after passes without what was left to count.")
	for _, plugin := range slices.Sorted(maps.Keys(failures)) {
		out.Sample("murder_plugin_module_failures_total", float64(failures[plugin]), "plugin", plugin)
	}
}
