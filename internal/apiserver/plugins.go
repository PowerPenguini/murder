package apiserver

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"slices"
	"sync"
	"time"

	"github.com/PowerPenguini/murder/internal/api"
	"github.com/PowerPenguini/murder/internal/plugins"
	"github.com/PowerPenguini/murder/internal/spec"
	"github.com/PowerPenguini/murder/internal/store"
)

// pluginRoutes serve plugins, tenants' settings of them, and what tenants
// used of them.
func (s *Server) pluginRoutes(mux *http.ServeMux) {
	mux.HandleFunc("GET /v1/plugins", s.listPlugins)
	mux.HandleFunc("GET /v1/plugins/{name}", s.getPlugin)
	mux.HandleFunc("PUT /v1/plugins/{name}", s.putPlugin)
	mux.HandleFunc("DELETE /v1/plugins/{name}", s.deletePlugin)
	mux.HandleFunc("PUT /v1/plugins/{name}/key", s.setPluginKey)
	mux.HandleFunc("PUT /v1/plugins/{name}/tenants/{tenant}/limits", s.setPluginLimits)
	mux.HandleFunc("GET /v1/tenants/{name}/plugins", s.tenantPlugins)
	mux.HandleFunc("GET /v1/plugin-uses", s.allPluginUses)
	mux.HandleFunc("GET /v1/plugin-usage", s.pluginUsage)
	// What the edge counted: only the edge, as root, reports it.
	mux.HandleFunc("POST /v1/edge/plugin-usage", rootOnly(s.edgePluginUsage))
}

// pluginGate says whether every node of the cluster applies the writes of
// plugin usage, asking the nodes at most once a minute while they do.
type pluginGate struct {
	mu    sync.Mutex
	ready func(context.Context) error
	until time.Time
}

func (g *pluginGate) check(ctx context.Context) error {
	if g == nil || g.ready == nil {
		return nil
	}
	g.mu.Lock()
	defer g.mu.Unlock()
	if time.Now().Before(g.until) {
		return nil
	}
	ctx, cancel := context.WithTimeout(ctx, 10*time.Second)
	defer cancel()
	if err := g.ready(ctx); err != nil {
		return err
	}
	g.until = time.Now().Add(time.Minute)
	return nil
}

// WithPluginsReady counts plugin usage only once ready says that every
// node of the cluster applies it.
func (s *Server) WithPluginsReady(ready func(context.Context) error) *Server {
	s.pluginGate = &pluginGate{ready: ready}
	return s
}

func (s *Server) pluginView(plugin store.Plugin) api.Plugin {
	view := api.Plugin{Name: plugin.Name, Spec: plugin.Spec, Manifest: plugin.Manifest, HasKey: plugin.Key != "", CreatedAt: plugin.CreatedAt, UpdatedAt: plugin.UpdatedAt}
	for _, app := range s.store.Apps() {
		if slices.Contains(app.Spec.Plugins, plugin.Name) {
			view.Apps = append(view.Apps, app.Name)
		}
	}
	return view
}

func (s *Server) listPlugins(w http.ResponseWriter, r *http.Request) {
	views := []api.Plugin{}
	for _, plugin := range s.store.Plugins() {
		views = append(views, s.pluginView(plugin))
	}
	writeJSON(w, http.StatusOK, views)
}

func (s *Server) getPlugin(w http.ResponseWriter, r *http.Request) {
	plugin, err := s.store.Plugin(r.PathValue("name"))
	if err != nil {
		writeError(w, err)
		return
	}
	writeJSON(w, http.StatusOK, s.pluginView(plugin))
}

func (s *Server) putPlugin(w http.ResponseWriter, r *http.Request) {
	var request api.PluginRequest
	if !readJSON(w, r, &request) {
		return
	}
	name := r.PathValue("name")
	pluginSpec, manifest, note, err := s.pluginImage(r.Context(), request.Spec)
	if err != nil {
		writeError(w, err)
		return
	}
	plugin, changed, err := s.store.PutPlugin(name, pluginSpec, manifest, actor(r))
	if err != nil {
		writeError(w, err)
		return
	}
	writeJSON(w, http.StatusOK, api.PluginChange{Plugin: s.pluginView(plugin), Changed: changed, Note: note})
}

// labeledRuntime reads images' labels.
type labeledRuntime interface {
	ImageLabels(ctx context.Context, ref string) (map[string]string, error)
}

// pluginImage pins the image of a plugin of kind container, and reads its
// manifest from its label (plugins.ManifestLabel): the image is pulled
// here, never run. An image a plugin runs already is not read again.
func (s *Server) pluginImage(ctx context.Context, pluginSpec plugins.Spec) (plugins.Spec, *plugins.Manifest, string, error) {
	if pluginSpec.Kind != plugins.KindContainer || pluginSpec.Image == "" {
		return pluginSpec, nil, "", nil
	}
	if err := spec.ValidReference(pluginSpec.Image); err != nil {
		return pluginSpec, nil, "", badRequest(err)
	}
	for _, plugin := range s.store.Plugins() {
		if plugin.Spec.Kind == plugins.KindContainer && plugin.Spec.Image == pluginSpec.Image && plugin.Manifest != nil {
			return pluginSpec, plugin.Manifest, "", nil
		}
	}
	pinned, note, err := s.resolve(ctx, pluginSpec.Image)
	if err != nil {
		return pluginSpec, nil, "", err
	}
	// A reference pinned already is not pulled to be pinned.
	present, err := s.runtime.HasImage(ctx, pinned)
	if err == nil && !present {
		err = s.runtime.PullImage(ctx, pinned)
	}
	if err != nil {
		return pluginSpec, nil, "", statusError{http.StatusUnprocessableEntity, fmt.Errorf("pull image %s: %w", pinned, err)}
	}
	labeled, ok := s.runtime.(labeledRuntime)
	if !ok {
		return pluginSpec, nil, "", errors.New("this agent's container engine reads no image's labels")
	}
	labels, err := labeled.ImageLabels(ctx, pinned)
	if err != nil {
		return pluginSpec, nil, "", err
	}
	text, ok := labels[plugins.ManifestLabel]
	if !ok {
		return pluginSpec, nil, "", badRequest(fmt.Errorf("image %s has no label %s: it is not a plugin's, or was built without its manifest (docs/plugins.md)", pluginSpec.Image, plugins.ManifestLabel))
	}
	manifest, err := plugins.ParseManifest(text)
	if err != nil {
		return pluginSpec, nil, "", badRequest(fmt.Errorf("image %s: %w", pluginSpec.Image, err))
	}
	pluginSpec.Image = pinned
	return pluginSpec, &manifest, note, nil
}

func (s *Server) deletePlugin(w http.ResponseWriter, r *http.Request) {
	if err := s.store.DeletePlugin(r.PathValue("name"), actor(r)); err != nil {
		writeError(w, err)
		return
	}
	w.WriteHeader(http.StatusNoContent)
}

// setPluginKey seals a key with the master key, bound to the plugin and,
// for a tenant's own, to the tenant.
func (s *Server) setPluginKey(w http.ResponseWriter, r *http.Request) {
	var request api.PluginKeyRequest
	if !readJSON(w, r, &request) {
		return
	}
	name := r.PathValue("name")
	if len(request.Key) > 4096 {
		writeError(w, badRequest(errors.New("a key is at most 4096 bytes")))
		return
	}
	sealed := ""
	if request.Key != "" {
		if s.key == nil {
			writeError(w, errors.New("this agent has no master key to seal the key with"))
			return
		}
		context := plugins.KeyContext(name)
		if request.Tenant != "" {
			context = plugins.TenantKeyContext(name, request.Tenant)
		}
		var err error
		if sealed, err = s.key.Seal(context, request.Key); err != nil {
			writeError(w, err)
			return
		}
	}
	if err := s.store.SetPluginKey(name, request.Tenant, sealed, actor(r)); err != nil {
		writeError(w, err)
		return
	}
	w.WriteHeader(http.StatusNoContent)
}

func (s *Server) setPluginLimits(w http.ResponseWriter, r *http.Request) {
	var request api.PluginLimitsRequest
	if !readJSON(w, r, &request) {
		return
	}
	if err := s.store.SetPluginLimits(r.PathValue("name"), r.PathValue("tenant"), request.Limits, request.Set, actor(r)); err != nil {
		writeError(w, err)
		return
	}
	w.WriteHeader(http.StatusNoContent)
}

// tenantPlugins shows a tenant's use of each plugin its app lists or it
// has settings of: the limits that hold, and what it used today and this
// month.
func (s *Server) tenantPlugins(w http.ResponseWriter, r *http.Request) {
	record, err := s.store.Get(r.PathValue("name"))
	if err != nil {
		writeError(w, err)
		return
	}
	uses, err := s.pluginUses([]store.Record{record}, time.Now())
	if err != nil {
		writeError(w, err)
		return
	}
	writeJSON(w, http.StatusOK, uses)
}

// allPluginUses shows every live tenant's use of plugins, as
// tenantPlugins shows one's: for the panel's view of them all.
func (s *Server) allPluginUses(w http.ResponseWriter, r *http.Request) {
	var records []store.Record
	for _, record := range s.store.List() {
		if !record.Archived() {
			records = append(records, record)
		}
	}
	uses, err := s.pluginUses(records, time.Now())
	if err != nil {
		writeError(w, err)
		return
	}
	writeJSON(w, http.StatusOK, uses)
}

// pluginUses are tenants' uses of the plugins their apps list or they
// have settings of, by tenant and plugin.
func (s *Server) pluginUses(records []store.Record, now time.Time) ([]api.PluginUse, error) {
	uses := []api.PluginUse{}
	installed := s.store.Plugins()
	for _, record := range records {
		use, err := s.pluginUsesOf(record, installed, now)
		if err != nil {
			return nil, err
		}
		uses = append(uses, use...)
	}
	return uses, nil
}

func (s *Server) pluginUsesOf(record store.Record, installed []store.Plugin, now time.Time) ([]api.PluginUse, error) {
	name := record.Spec.Name
	settings := map[string]store.PluginTenant{}
	for _, own := range s.store.PluginTenants(name, "") {
		settings[own.Plugin] = own
	}
	if len(record.Spec.Plugins) == 0 && len(settings) == 0 {
		return nil, nil
	}
	totals, err := s.store.PluginTotals([]string{name}, now)
	if err != nil {
		return nil, err
	}
	used := map[[3]string]int64{}
	for _, total := range totals {
		used[[3]string{total.Plugin, total.Period, total.Metric}] = total.Amount
	}
	var uses []api.PluginUse
	for _, plugin := range installed {
		own, hasOwn := settings[plugin.Name]
		granted := slices.Contains(record.Spec.Plugins, plugin.Name)
		if !granted && !hasOwn {
			continue
		}
		use := api.PluginUse{Tenant: name, Plugin: plugin.Name, Granted: granted, OwnKey: own.Key != "", OwnLimits: own.HasLimits}
		for _, limit := range store.PluginLimits(plugin, own) {
			period := plugins.PeriodOf(limit.Per, now)
			amount := used[[3]string{plugin.Name, period, limit.Metric}]
			use.Limits = append(use.Limits, api.LimitUse{Limit: limit, Period: period, Used: amount, Reached: amount >= limit.Amount})
		}
		for _, metric := range plugin.Metrics() {
			if amount := used[[3]string{plugin.Name, plugins.Day(now), metric}]; amount > 0 {
				use.Today = append(use.Today, api.MetricUsed{Metric: metric, Amount: amount})
			}
			if amount := used[[3]string{plugin.Name, plugins.Month(now), metric}]; amount > 0 {
				use.Month = append(use.Month, api.MetricUsed{Metric: metric, Amount: amount})
			}
		}
		uses = append(uses, use)
	}
	return uses, nil
}

// pluginUsage lists what tenants used, from and to days of the query;
// this month so far by default.
func (s *Server) pluginUsage(w http.ResponseWriter, r *http.Request) {
	query := r.URL.Query()
	now := time.Now()
	from, to := query.Get("from"), query.Get("to")
	if from == "" {
		from = plugins.Month(now) + "-01"
	}
	if to == "" {
		to = plugins.Day(now)
	}
	counts, err := s.store.PluginUsage(store.UsageQuery{Tenant: query.Get("tenant"), Plugin: query.Get("plugin"), From: from, To: to})
	if err != nil {
		writeError(w, err)
		return
	}
	if counts == nil {
		counts = []plugins.Count{}
	}
	writeJSON(w, http.StatusOK, api.PluginUsage{From: from, To: to, Counts: counts})
}

// edgePluginUsage counts a report of what the node's edge counted, and
// answers what the node's tenants used, cluster wide, for the edge to
// hold their limits by. A report numbered 0 only asks.
func (s *Server) edgePluginUsage(w http.ResponseWriter, r *http.Request) {
	var report plugins.Report
	if !readJSONUpTo(w, r, &report, 8<<20) {
		return
	}
	node := s.store.Node()
	if report.Seq > 0 {
		if err := s.pluginGate.check(r.Context()); err != nil {
			writeError(w, statusError{http.StatusServiceUnavailable, fmt.Errorf("plugin usage is counted once every node runs a release that keeps it: %w", err)})
			return
		}
		if _, err := s.store.AddPluginUsage(node, report); err != nil {
			writeError(w, err)
			return
		}
	}
	var tenants []string
	for _, record := range s.store.List() {
		if record.Node == node && !record.Archived() && len(record.Spec.Plugins) > 0 {
			tenants = append(tenants, record.Spec.Name)
		}
	}
	totals, err := s.store.PluginTotals(tenants, time.Now())
	if err != nil {
		writeError(w, err)
		return
	}
	writeJSON(w, http.StatusOK, plugins.Totals{Edge: report.Edge, Seq: s.store.PluginReported(node, report.Edge), Totals: totals})
}
