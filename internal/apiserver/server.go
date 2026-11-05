// Package apiserver serves the agent's local HTTP API on a unix socket,
// used by the CLI and the panel through internal/api.
package apiserver

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"maps"
	"net"
	"net/http"
	"slices"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/PowerPenguini/murder/internal/api"
	"github.com/PowerPenguini/murder/internal/cluster"
	"github.com/PowerPenguini/murder/internal/edge"
	"github.com/PowerPenguini/murder/internal/reconcile"
	"github.com/PowerPenguini/murder/internal/runtime"
	"github.com/PowerPenguini/murder/internal/secrets"
	"github.com/PowerPenguini/murder/internal/spec"
	"github.com/PowerPenguini/murder/internal/store"
)

// Server serves the API.
type Server struct {
	store      store.Store
	reconciler *reconcile.Reconciler
	// placing keeps automatic placements one at a time.
	placing sync.Mutex
	runtime runtime.Runtime
	key     *secrets.Key
	log     *slog.Logger
	// cluster is this node's part of a cluster; nil outside one.
	cluster Cluster
	// stopping is closed when the agent stops, so requests that wait, such
	// as the edge's for new routes, end at once.
	stopping chan struct{}
	stopOnce sync.Once
	// upgrader installs new binaries on this node; nil if it takes none.
	upgrader Upgrader
	// relayPort is where the edges of a cluster pass requests on.
	relayPort int
	// moves moves tenants between nodes; nil if this node moves none.
	moves Moves
	// replicas reports on the copies of this node's tenants; nil when it
	// keeps none.
	replicas Replicas
	// backups backs this node's tenants up; nil without backups set up.
	backups Backups
	// usage is what this node measures of its tenants; nil if nothing.
	usage Usage
	// invites makes invitations to join the cluster; nil makes none.
	invites Invites
	// keys rotates the master key; nil does not.
	keys Keys
	// lastBackup is when a backup was last sent whole, in Unix nanoseconds.
	lastBackup atomic.Int64
}

// NewServer returns the API server.
func NewServer(st store.Store, reconciler *reconcile.Reconciler, rt runtime.Runtime, key *secrets.Key, log *slog.Logger) *Server {
	return &Server{store: st, reconciler: reconciler, runtime: rt, key: key, log: log, stopping: make(chan struct{})}
}

// WithRelayPort routes to tenants of other nodes through their edges'
// relay port.
func (s *Server) WithRelayPort(port int) *Server {
	s.relayPort = port
	return s
}

// Stop ends the requests that wait, before the server shuts down.
func (s *Server) Stop() { s.stopOnce.Do(func() { close(s.stopping) }) }

// Handler routes API requests.
func (s *Server) Handler() http.Handler {
	mux := http.NewServeMux()
	s.clusterRoutes(mux)
	s.keyRoutes(mux)
	s.upgradeRoutes(mux)
	s.usageRoutes(mux)
	mux.HandleFunc("GET /v1/routes", s.routes)
	mux.HandleFunc("GET /v1/apps", s.listApps)
	mux.HandleFunc("GET /v1/apps/{name}", s.getApp)
	mux.HandleFunc("PUT /v1/apps/{name}", s.putApp)
	mux.HandleFunc("DELETE /v1/apps/{name}", s.deleteApp)
	mux.HandleFunc("POST /v1/apps/{name}/check", s.checkApp)
	mux.HandleFunc("POST /v1/apps/{name}/update", s.updateApp)
	mux.HandleFunc("GET /v1/apps/{name}/revisions", s.appRevisions)
	mux.HandleFunc("POST /v1/apps/{name}/revisions/{revision}/restore", s.restoreRevision)
	mux.HandleFunc("POST /v1/apps/{name}/rollout/{action}", s.rolloutAction)
	mux.HandleFunc("PUT /v1/tenants/{name}/flags", s.setFlags)
	mux.HandleFunc("GET /v1/tenants", s.list)
	mux.HandleFunc("POST /v1/tenants", s.create)
	mux.HandleFunc("GET /v1/tenants/{name}", s.get)
	mux.HandleFunc("PUT /v1/tenants/{name}", s.configure)
	mux.HandleFunc("POST /v1/tenants/{name}/suspend", s.suspend(true))
	mux.HandleFunc("POST /v1/tenants/{name}/resume", s.suspend(false))
	mux.HandleFunc("POST /v1/tenants/{name}/archive", s.archive)
	mux.HandleFunc("POST /v1/tenants/{name}/restore", s.restore)
	mux.HandleFunc("DELETE /v1/tenants/{name}", s.purge)
	mux.HandleFunc("GET /v1/tenants/{name}/secrets/{key}", s.secret)
	mux.HandleFunc("POST /v1/tenants/{name}/secrets/{key}/rotate", s.rotate)
	mux.HandleFunc("POST /v1/tenants/{name}/roll", s.roll)
	mux.HandleFunc("POST /v1/tenants/{name}/move", s.move)
	mux.HandleFunc("GET /v1/tenants/{name}/logs", s.logs)
	mux.HandleFunc("GET /v1/events", s.events)
	mux.HandleFunc("GET /v1/nodes", s.nodes)
	s.panelRoutes(mux)
	s.panelAuthRoutes(mux)
	mux.HandleFunc("POST /v1/reconcile", s.reconcileNow)
	// The whole database: panel secrets and sign-in state too, which only
	// root reads.
	mux.HandleFunc("GET /v1/backup", rootOnly(s.backup))
	mux.HandleFunc("POST /v1/backups/run", rootOnly(s.runBackups))
	mux.HandleFunc("POST /v1/backups/replace", rootOnly(s.replaceTenant))
	return mux
}

func (s *Server) view(record store.Record) api.Tenant {
	view := api.Tenant{
		Spec: record.Spec, App: record.App, Overrides: record.Overrides, Node: record.Node, Replica: record.Replica, ReplicaReady: record.ReplicaReady, ReplicaNext: record.ReplicaNext,
		Host: s.reconciler.Host(record.Spec.Name), URL: s.reconciler.URL(record.Spec.Name),
		Generation: record.Generation, CreatedAt: record.CreatedAt, UpdatedAt: record.UpdatedAt, ArchivedAt: record.ArchivedAt,
		AppRevision: record.AppRevision, BeforeRevision: record.BeforeRevision, Canary: record.Canary, Hold: record.Hold, Restoring: record.Restore != "",
	}
	view.KeptSecrets = slices.Sorted(maps.Keys(record.Sealed))
	view.Elsewhere = record.Node != s.store.Node()
	if !view.Elsewhere && s.replicas != nil {
		view.ReplicaSyncedAt = s.replicas.Synced(record.Spec.Name)
	}
	if record.Archived() || view.Elsewhere {
		return view
	}
	if address, err := reconcile.Backend(record); err == nil {
		view.Address = address
	}
	if status, ok := s.reconciler.Status(record.Spec.Name); ok {
		view.Status = &status
	}
	return view
}

func (s *Server) appView(app store.App) api.App {
	view := api.App{Name: app.Name, Spec: app.Spec, Tag: app.Tag, Revision: app.Revision, Latest: app.Latest, LatestAt: app.LatestAt, Tenants: []string{}, CreatedAt: app.CreatedAt, UpdatedAt: app.UpdatedAt}
	if rollout, err := s.store.Rollout(app.Name); err == nil {
		view.Rollout = &rollout
	}
	for _, record := range s.store.List() {
		if record.App == app.Name && !record.Archived() {
			view.Tenants = append(view.Tenants, record.Spec.Name)
		}
	}
	return view
}

func (s *Server) listApps(w http.ResponseWriter, _ *http.Request) {
	apps := s.store.Apps()
	views := make([]api.App, 0, len(apps))
	for _, app := range apps {
		views = append(views, s.appView(app))
	}
	writeJSON(w, http.StatusOK, views)
}

func (s *Server) getApp(w http.ResponseWriter, r *http.Request) {
	app, err := s.store.App(r.PathValue("name"))
	if err != nil {
		writeError(w, err)
		return
	}
	writeJSON(w, http.StatusOK, s.appView(app))
}

// putApp stores an app and rolls it out to its tenants. Everything but the
// image pin is checked first, so a broken spec never waits for a pull; the
// pull runs on the request's context, so a client that gives up cancels it.
// An image given as the tag the app already has keeps the image it is
// pinned to, unless update is asked for: a change of the spec takes no
// newer image by the way.
func (s *Server) putApp(w http.ResponseWriter, r *http.Request) {
	name := r.PathValue("name")
	cond, err := appCondition(r)
	if err != nil {
		writeError(w, err)
		return
	}
	var app spec.Tenant
	if !readJSON(w, r, &app) {
		return
	}
	update := false
	if value := r.URL.Query().Get("update"); value != "" {
		var err error
		if update, err = strconv.ParseBool(value); err != nil {
			writeError(w, badRequest(errors.New("update must be true or false")))
			return
		}
	}
	s.storeApp(w, r, name, app, cond, update)
}

// appCondition reads the condition a change of an app is made on.
func appCondition(r *http.Request) (store.AppIf, error) {
	var cond store.AppIf
	switch value := r.URL.Query().Get("if"); {
	case value == "absent":
		cond.Absent = true
	case value != "":
		updated, err := time.Parse(time.RFC3339Nano, value)
		if err != nil {
			return cond, badRequest(errors.New(`if must be "absent" or the app's updatedAt`))
		}
		cond.UpdatedAt = updated
	}
	return cond, nil
}

// storeApp pins app's image and stores it. A tag the app already has keeps
// the image it is pinned to, unless update asks for the tag's newest one;
// update takes nothing but the registry's answer.
func (s *Server) storeApp(w http.ResponseWriter, r *http.Request, name string, app spec.Tenant, cond store.AppIf, update bool) {
	if err := s.store.CheckApp(store.App{Name: name, Spec: app}); err != nil {
		writeError(w, err)
		return
	}
	// The app as the cluster has it, not as a node behind it last heard.
	if err := s.catchUp(); err != nil {
		writeError(w, err)
		return
	}
	tag, note, latest := "", "", ""
	kept := false
	if !spec.Pinned(app.Image) {
		tag = app.Image
		stored, err := s.store.App(name)
		follows := err == nil && stored.Tag == tag
		if kept = follows && !update; kept {
			app.Image = stored.Spec.Image
			// What the pin was kept on must not change meanwhile.
			if cond == (store.AppIf{}) {
				cond.UpdatedAt = stored.UpdatedAt
			}
			// The registry is asked now, briefly: what it holds is newer
			// than any earlier answer. No answer leaves what was found.
			ask, cancel := context.WithTimeout(r.Context(), askTimeout)
			latest, err = s.runtime.Latest(ask, tag)
			cancel()
			if err != nil {
				latest = ""
				s.log.Info("ask the registry about a kept tag", "app", name, "tag", tag, "error", err)
			}
		} else {
			// Only an update of the tag the app follows insists on the
			// registry's answer; a new tag may be a local image.
			pinned, pinNote, err := s.pinTag(r.Context(), tag, update && follows)
			if err != nil {
				writeError(w, err)
				return
			}
			app.Image, note = pinned, pinNote
			// A local image cannot be asked about; one pinned from the
			// registry is what it names now.
			if spec.RegistryPinned(pinned) && pinNote == "" {
				latest = pinned
			} else if !spec.RegistryPinned(pinned) {
				tag = ""
			}
		}
	}
	if err := r.Context().Err(); err != nil {
		writeError(w, clientGone(fmt.Errorf("the client gave up while waiting; nothing was changed: %w", err)))
		return
	}
	change, err := s.store.PutAppIf(store.App{Name: name, Spec: app, Tag: tag, Latest: latest, AllAtOnce: atOnce(r), First: first(r)}, cond, actor(r))
	if err != nil {
		writeError(w, err)
		return
	}
	// An app stored as it was takes no new revision for what the registry
	// holds: that is recorded on its own.
	if !change.Changed && latest != "" {
		if _, err := s.store.SetLatest(name, tag, latest, change.App.Revision, actor(r)); err != nil {
			s.log.Warn("record what the registry holds", "app", name, "error", err)
		} else if current, err := s.store.App(name); err == nil {
			change.App = current
		}
	}
	if kept && change.App.Latest != "" {
		note = fmt.Sprintf("kept the image %s is pinned to; a newer one is in its registry: \"murder app update %s\" takes it", tag, name)
	}
	s.writeAppChange(w, change, note)
}

// askTimeout bounds asking a registry about a kept tag while a client
// waits; the change waits for it, and goes on without its answer.
const askTimeout = 5 * time.Second

// catchUp applies here what the cluster has applied; nothing outside one.
func (s *Server) catchUp() error {
	if s.cluster == nil || s.store.Cluster() == "" {
		return nil
	}
	return s.cluster.CatchUp()
}

// pinTag pins tag to the image its registry names now, pulled here. If the
// registry cannot be asked, strict refuses; otherwise the tag is resolved
// as before, a local image being taken when the registry has none.
func (s *Server) pinTag(ctx context.Context, tag string, strict bool) (string, string, error) {
	latest, err := s.runtime.Latest(ctx, tag)
	if err == nil {
		if err := s.runtime.PullImage(ctx, latest); err != nil {
			return "", "", statusError{http.StatusUnprocessableEntity, fmt.Errorf("pull %s, the image of %s: %w", latest, tag, err)}
		}
		return latest, "", nil
	}
	if strict {
		return "", "", statusError{http.StatusBadGateway, fmt.Errorf("the newest image of %s is unknown; nothing was changed: %w", tag, err)}
	}
	return s.resolve(ctx, tag)
}

// atOnce reports whether a request moves every tenant to an app's change
// now (?rollout=now), not in waves.
func atOnce(r *http.Request) bool { return r.URL.Query().Get("rollout") == "now" }

// first names the tenants an app change in waves moves at once, as its
// first wave: ?first=a,b.
func first(r *http.Request) []string {
	var names []string
	for _, name := range strings.Split(r.URL.Query().Get("first"), ",") {
		if name = strings.TrimSpace(name); name != "" {
			names = append(names, name)
		}
	}
	return names
}

// writeAppChange answers with a stored app and the tenants it rolls out to.
func (s *Server) writeAppChange(w http.ResponseWriter, change store.AppChange, note string) {
	result := api.AppApplied{App: s.appView(change.App), Created: change.Created, Changed: change.Changed, Rollout: change.Rollout, Tenants: []api.Tenant{}, Note: note}
	for _, tenant := range change.Tenants {
		if record, err := s.store.Get(tenant); err == nil {
			result.Tenants = append(result.Tenants, s.view(record))
		}
	}
	if change.Changed {
		s.log.Info("app stored", "app", change.App.Name, "revision", change.App.Revision, "image", change.App.Spec.Image, "tenants", change.Tenants)
	}
	status := http.StatusOK
	if change.Created {
		status = http.StatusCreated
	}
	writeJSON(w, status, result)
}

func (s *Server) deleteApp(w http.ResponseWriter, r *http.Request) {
	name := r.PathValue("name")
	if err := s.store.DeleteApp(name, actor(r)); err != nil {
		writeError(w, err)
		return
	}
	s.log.Info("app deleted", "app", name)
	w.WriteHeader(http.StatusNoContent)
}

func (s *Server) list(w http.ResponseWriter, r *http.Request) {
	records := s.store.List()
	views := make([]api.Tenant, 0, len(records))
	for _, record := range records {
		views = append(views, s.view(record))
	}
	s.remoteStatus(r.Context(), views)
	writeJSON(w, http.StatusOK, views)
}

func (s *Server) get(w http.ResponseWriter, r *http.Request) {
	record, err := s.store.Get(r.PathValue("name"))
	if err != nil {
		writeError(w, err)
		return
	}
	views := []api.Tenant{s.view(record)}
	s.remoteStatus(r.Context(), views)
	writeJSON(w, http.StatusOK, views[0])
}

// remoteStatusTimeout bounds asking other nodes for their tenants' status:
// a node that is down leaves its tenants' status unknown instead of
// holding the answer.
const remoteStatusTimeout = 2 * time.Second

// remoteStatus fills in the status of tenants other nodes run, asking for
// all of them side by side, so that waiting for a tenant, or listing them,
// works from any node.
func (s *Server) remoteStatus(ctx context.Context, views []api.Tenant) {
	if s.cluster == nil {
		return
	}
	ctx, cancel := context.WithTimeout(ctx, remoteStatusTimeout)
	defer cancel()
	asking := make(chan struct{}, 16)
	var group sync.WaitGroup
	for i := range views {
		if !views[i].Elsewhere || views[i].Archived() {
			continue
		}
		group.Go(func() {
			asking <- struct{}{}
			defer func() { <-asking }()
			var status reconcile.Status
			if err := s.cluster.StatusOn(ctx, views[i].Node, views[i].Spec.Name, &status); err == nil {
				views[i].Status = &status
			}
		})
	}
	group.Wait()
}

func (s *Server) resolve(ctx context.Context, image string) (string, string, error) {
	pinned, note, err := s.runtime.Resolve(ctx, image)
	if err != nil {
		return "", "", statusError{http.StatusUnprocessableEntity, fmt.Errorf("pin image %s: %w", image, err)}
	}
	return pinned, note, nil
}

// prepareOverrides checks what a tenant of app with overrides would run and
// pins an image override, before anything is stored.
func (s *Server) prepareOverrides(r *http.Request, name, appName string, overrides spec.Overrides) (spec.Overrides, string, error) {
	app, err := s.store.App(appName)
	if err != nil {
		return spec.Overrides{}, "", err
	}
	if overrides.Image != "" {
		if err := spec.ValidReference(overrides.Image); err != nil {
			return spec.Overrides{}, "", badRequest(err)
		}
	}
	if err := spec.Compose(app.Spec, name, overrides).ValidateUnpinned(); err != nil {
		return spec.Overrides{}, "", badRequest(err)
	}
	note := ""
	if overrides.Image != "" {
		overrides.Image, note, err = s.resolve(r.Context(), overrides.Image)
		if err != nil {
			return spec.Overrides{}, "", err
		}
	}
	return overrides, note, nil
}

func (s *Server) create(w http.ResponseWriter, r *http.Request) {
	var request api.TenantRequest
	if !readJSON(w, r, &request) {
		return
	}
	if err := spec.ValidName(request.Name); err != nil {
		writeError(w, badRequest(err))
		return
	}
	overrides, note, err := s.prepareOverrides(r, request.Name, request.App, request.Overrides)
	if err != nil {
		writeError(w, err)
		return
	}
	ctx, cancel := detached(r)
	defer cancel()
	// Placed one at a time, so creates at once spread over the nodes
	// instead of all counting the same tenants.
	if request.Node == store.Auto {
		s.placing.Lock()
		defer s.placing.Unlock()
		// What the leader took last, through this node or not, counts:
		// this node may not have applied it yet.
		if s.cluster != nil {
			if err := s.cluster.CatchUp(); err != nil {
				writeError(w, err)
				return
			}
		}
	}
	node, err := s.placeNew(request.Node)
	if err != nil {
		writeError(w, err)
		return
	}
	tenant := store.NewTenant{Name: request.Name, App: request.App, Overrides: overrides, Node: node}
	var record store.Record
	if node == s.store.Node() {
		record, err = CreateHere(ctx, r.Context(), s.store, s.reconciler, tenant, actor(r))
	} else if s.cluster == nil {
		err = errNoCluster
	} else {
		// The node it runs on creates it: the data there is that node's
		// to check, under its own lock. The client's own context: one that
		// gives up while that node waits for its lock creates nothing there
		// either (it finishes, detached, what it began).
		record, err = s.cluster.CreateOn(r.Context(), node, tenant, actor(r))
	}
	if err != nil {
		writeError(w, err)
		return
	}
	s.log.Info("tenant created", "tenant", request.Name, "app", request.App, "image", record.Spec.Image)
	writeJSON(w, http.StatusCreated, api.Applied{Tenant: s.view(record), Changed: true, Note: note})
}

// CreateHere creates tenant on this node, under the reconciler's lock, so
// a purge cannot interleave with the check for data an earlier tenant of
// its name left here, which the new one would start on. Nothing is created
// once asked, the request's context, ends while it waits for the lock; ctx
// bounds the work itself.
func CreateHere(ctx, asked context.Context, st store.Store, reconciler *reconcile.Reconciler, tenant store.NewTenant, actor string) (store.Record, error) {
	var record store.Record
	// Waited for only while the client waits.
	err := reconciler.ExclusiveContext(asked, func() error {
		if err := asked.Err(); err != nil {
			return clientGone(fmt.Errorf("the client gave up while waiting; nothing was created: %w", err))
		}
		if existing, err := st.Get(tenant.Name); err == nil && existing.Archived() {
			return store.ErrArchived
		} else if err == nil {
			return store.ErrExists
		}
		exists, err := reconciler.DataExists(ctx, tenant.Name)
		if err != nil {
			return err
		}
		if exists {
			return fmt.Errorf("%w: data of an earlier tenant %s is on node %s but not in the archive; purge it there first, or create the tenant on another node", store.ErrConflict, tenant.Name, st.Node())
		}
		// The look for data may have taken long.
		if err := asked.Err(); err != nil {
			return clientGone(fmt.Errorf("the client gave up while waiting; nothing was created: %w", err))
		}
		tenant.Node = st.Node()
		record, err = st.Create(tenant, actor)
		return err
	})
	if gaveUp := asked.Err(); gaveUp != nil && errors.Is(err, gaveUp) {
		return record, clientGone(fmt.Errorf("the client gave up while waiting; nothing was created: %w", err))
	}
	return record, err
}

// placeNew is the node a new tenant runs on, as asked: a node's name,
// "auto" for the least loaded, or this node.
func (s *Server) placeNew(asked string) (string, error) {
	switch asked {
	case "":
		return s.store.Node(), nil
	case store.Auto:
		return store.LeastLoaded(s.store.Nodes(), s.store.List(), s.store.Node()), nil
	}
	if asked == s.store.Node() {
		return asked, nil
	}
	for _, node := range s.store.Nodes() {
		if node.Name != asked {
			continue
		}
		if node.Failed() {
			return "", statusError{http.StatusConflict, fmt.Errorf("node %s failed and has not answered since", asked)}
		}
		return asked, nil
	}
	return "", badRequest(fmt.Errorf("node %s is not in the cluster", asked))
}

// configure gives a tenant, live or archived, another app or other
// overrides. The request replaces both; omitted overrides are dropped. With
// a generation it is refused if the tenant changed since the client read
// it. Suspension is operational state and stays as it is. A request equal
// to what is stored changes nothing, not even the generation.
func (s *Server) configure(w http.ResponseWriter, r *http.Request) {
	name := r.PathValue("name")
	var request api.TenantRequest
	if !readJSON(w, r, &request) {
		return
	}
	if request.Name != "" && request.Name != name {
		writeError(w, badRequest(fmt.Errorf("request names tenant %q, not %q", request.Name, name)))
		return
	}
	if _, err := s.store.Get(name); err != nil {
		writeError(w, err)
		return
	}
	// Checked against what exists now; the store checks again when it
	// writes.
	overrides, note, err := s.prepareOverrides(r, name, request.App, request.Overrides)
	if err != nil {
		writeError(w, err)
		return
	}
	if err := gaveUp(r, "changed"); err != nil {
		writeError(w, err)
		return
	}
	record, changed, err := s.store.Configure(name, request.App, overrides, request.Generation, actor(r))
	if err != nil {
		writeError(w, err)
		return
	}
	if changed {
		s.log.Info("tenant updated", "tenant", name, "app", request.App, "generation", record.Generation, "image", record.Spec.Image)
	}
	writeJSON(w, http.StatusOK, api.Applied{Tenant: s.view(record), Changed: changed, Note: note})
}

func (s *Server) suspend(suspended bool) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		if err := gaveUp(r, "changed"); err != nil {
			writeError(w, err)
			return
		}
		record, err := s.store.SetSuspended(r.PathValue("name"), suspended, actor(r))
		if err != nil {
			writeError(w, err)
			return
		}
		s.log.Info("tenant suspension changed", "tenant", record.Spec.Name, "suspended", suspended)
		writeJSON(w, http.StatusOK, s.view(record))
	}
}

// archive takes a tenant out of service and keeps everything needed to
// restore it. The edge stops routing to it at once; the next reconcile
// pass, which the change starts, removes its container and network.
func (s *Server) archive(w http.ResponseWriter, r *http.Request) {
	name := r.PathValue("name")
	if err := gaveUp(r, "archived"); err != nil {
		writeError(w, err)
		return
	}
	if err := s.store.Archive(name, actor(r)); err != nil {
		writeError(w, err)
		return
	}
	s.log.Info("tenant archived", "tenant", name)
	w.WriteHeader(http.StatusNoContent)
}

func (s *Server) restore(w http.ResponseWriter, r *http.Request) {
	name := r.PathValue("name")
	var record store.Record
	// A client that gave up while waiting for a reconcile pass restores
	// nothing.
	err := s.exclusive(r, "restored", func() error {
		if err := gaveUp(r, "restored"); err != nil {
			return err
		}
		var err error
		record, err = s.store.Restore(name, actor(r))
		return err
	})
	if err != nil {
		writeError(w, err)
		return
	}
	s.log.Info("tenant restored", "tenant", name)
	writeJSON(w, http.StatusOK, s.view(record))
}

// purge deletes an archived tenant, or data no archive entry accounts for.
func (s *Server) purge(w http.ResponseWriter, r *http.Request) {
	name := r.PathValue("name")
	if err := spec.ValidLabel(name); err != nil {
		writeError(w, badRequest(err))
		return
	}
	// Given up while it waits for a pass, or before it begins, the purge
	// deletes nothing; once begun it runs to its end (Purge detaches it).
	if err := s.reconciler.Purge(r.Context(), name, actor(r)); err != nil {
		writeError(w, gone(r, "purged", err))
		return
	}
	s.log.Info("tenant purged", "tenant", name)
	w.WriteHeader(http.StatusNoContent)
}

// A client that gives up is not changed for: once a handler holds what it
// waits for (the reconciler's lock), it checks, right before the change is
// committed, that its client still waits. The store's writes take no
// context, so a client that gives up while a write waits for the writer's
// lock, or for this node to catch up with the cluster, still has it made.

// gaveUp refuses a change for a client that gave up; nothing names what
// was then not done.
func gaveUp(r *http.Request, nothing string) error {
	if err := r.Context().Err(); err != nil {
		return clientGone(fmt.Errorf("the client gave up while waiting; nothing was %s: %w", nothing, err))
	}
	return nil
}

// gone reports err, the client's giving up as such.
func gone(r *http.Request, nothing string, err error) error {
	if gaveUp := r.Context().Err(); gaveUp != nil && errors.Is(err, gaveUp) {
		return clientGone(fmt.Errorf("the client gave up while waiting; nothing was %s: %w", nothing, err))
	}
	return err
}

// exclusive runs fn under the reconciler's lock, waited for only while the
// client waits.
func (s *Server) exclusive(r *http.Request, nothing string, fn func() error) error {
	return gone(r, nothing, s.reconciler.ExclusiveContext(r.Context(), fn))
}

// detached gives work that changes the runtime its own deadline, so a client
// that disconnects cannot leave it half done.
func detached(r *http.Request) (context.Context, context.CancelFunc) {
	return context.WithTimeout(context.WithoutCancel(r.Context()), 10*time.Minute)
}

func (s *Server) secret(w http.ResponseWriter, r *http.Request) {
	name, key := r.PathValue("name"), r.PathValue("key")
	record, err := s.store.Live(name)
	if err != nil {
		writeError(w, err)
		return
	}
	sealed, ok := record.Sealed[key]
	if !ok {
		writeError(w, notFound(fmt.Errorf("tenant %s has no secret %s", name, key)))
		return
	}
	value, err := s.key.Open(secrets.Context(name, key), sealed)
	if err != nil {
		writeError(w, err)
		return
	}
	s.log.Info("secret read", "tenant", name, "secret", key, "actor", actor(r))
	writeJSON(w, http.StatusOK, api.SecretValue{Value: value})
}

// rotate gives one secret of the spec a new value; the container is
// replaced with it. Values are otherwise generated once and kept, even when
// a secret leaves the spec and comes back.
// roll replaces a tenant's container as it is, made as this release makes
// containers.
func (s *Server) roll(w http.ResponseWriter, r *http.Request) {
	record, err := s.store.Roll(r.PathValue("name"), actor(r))
	if err != nil {
		writeError(w, err)
		return
	}
	writeJSON(w, http.StatusOK, s.view(record))
}

func (s *Server) rotate(w http.ResponseWriter, r *http.Request) {
	name, key := r.PathValue("name"), r.PathValue("key")
	record, err := s.store.Rotate(name, key, actor(r))
	if err != nil {
		writeError(w, err)
		return
	}
	s.log.Info("secret rotated", "tenant", name, "secret", key, "generation", record.Generation)
	writeJSON(w, http.StatusOK, s.view(record))
}

func (s *Server) logs(w http.ResponseWriter, r *http.Request) {
	name := r.PathValue("name")
	record, err := s.store.Live(name)
	if err != nil {
		writeError(w, err)
		return
	}
	if record.Node != s.store.Node() {
		writeError(w, fmt.Errorf("%w: tenant %s runs on node %s: read its logs there", reconcile.ErrElsewhere, name, record.Node))
		return
	}
	if r.URL.Query().Get("previous") != "" {
		kept, err := s.reconciler.Previous(name)
		if errors.Is(err, reconcile.ErrNoPrevious) {
			writeError(w, notFound(fmt.Errorf("tenant %s: no previous container's lines are kept on node %s", name, record.Node)))
			return
		}
		if err != nil {
			writeError(w, err)
			return
		}
		writeJSON(w, http.StatusOK, api.PreviousLogs{Container: kept.Container, Image: kept.Image, Why: kept.Why,
			ExitedAt: kept.ExitedAt, ExitKnown: kept.ExitKnown, ExitCode: kept.ExitCode, OOMKilled: kept.OOMKilled, MemoryMB: kept.MemoryMB,
			At: kept.At, Cut: kept.Cut, Lines: string(kept.Lines)})
		return
	}
	tail := 200
	if value := r.URL.Query().Get("tail"); value != "" {
		parsed, err := strconv.Atoi(value)
		if err != nil || parsed < 1 || parsed > 10000 {
			writeError(w, badRequest(errors.New("tail must be 1-10000")))
			return
		}
		tail = parsed
	}
	status, ok := s.reconciler.Status(name)
	if !ok || status.ContainerID == "" {
		writeError(w, notFound(fmt.Errorf("tenant %s has no container", name)))
		return
	}
	// A container started again in place after its app exited holds the
	// lines of its runs before; those are the previous container's, kept
	// apart. Its current run's are those after that exit.
	var output []byte
	if since := s.reconciler.LinesSince(name, status.ContainerID); !since.IsZero() {
		if reader, ok := s.runtime.(runtime.SinceLogs); ok {
			output, err = reader.LogsSince(r.Context(), status.ContainerID, tail, since)
		} else {
			output, err = s.runtime.Logs(r.Context(), status.ContainerID, tail)
		}
	} else {
		output, err = s.runtime.Logs(r.Context(), status.ContainerID, tail)
	}
	if err != nil {
		writeError(w, err)
		return
	}
	w.Header().Set("Content-Type", "text/plain; charset=utf-8")
	_, _ = w.Write(output)
}

// events returns the audit log, newest first: ?tenant=, ?app=, ?before=ID
// for the next page and ?limit= (at most 1000).
func (s *Server) events(w http.ResponseWriter, r *http.Request) {
	query := store.EventQuery{Tenant: r.URL.Query().Get("tenant"), App: r.URL.Query().Get("app")}
	if value := r.URL.Query().Get("before"); value != "" {
		parsed, err := strconv.ParseInt(value, 10, 64)
		if err != nil || parsed < 1 {
			writeError(w, badRequest(errors.New("before must be a positive event ID")))
			return
		}
		query.Before = parsed
	}
	if value := r.URL.Query().Get("limit"); value != "" {
		parsed, err := strconv.Atoi(value)
		if err != nil || parsed < 1 || parsed > 1000 {
			writeError(w, badRequest(errors.New("limit must be 1-1000")))
			return
		}
		query.Limit = parsed
	}
	events, err := s.store.Events(query)
	if err != nil {
		writeError(w, err)
		return
	}
	writeJSON(w, http.StatusOK, events)
}

// reconcileNow runs a pass at once, retrying failed pulls without waiting
// for their delay and waiting for the pulls it starts.
func (s *Server) reconcileNow(w http.ResponseWriter, r *http.Request) {
	ctx, cancel := detached(r)
	defer cancel()
	if err := s.reconciler.ReconcileNow(ctx); err != nil {
		writeError(w, err)
		return
	}
	w.WriteHeader(http.StatusNoContent)
}

// backup streams a copy of the database. A failure before the first byte is
// an error response; one after it cuts the connection, so the client never
// takes a partial copy for a whole one.
func (s *Server) backup(w http.ResponseWriter, _ *http.Request) {
	w.Header().Set("Content-Type", "application/vnd.sqlite3")
	out := &countingWriter{w: w}
	if err := s.store.Backup(out); err != nil {
		if out.n == 0 {
			w.Header().Del("Content-Type")
			writeError(w, err)
			return
		}
		s.log.Error("back up the database", "error", err)
		panic(http.ErrAbortHandler)
	}
	s.lastBackup.Store(time.Now().UnixNano())
}

// LastBackup returns when a backup was last sent whole since the agent
// started; zero if none was.
func (s *Server) LastBackup() time.Time {
	if at := s.lastBackup.Load(); at != 0 {
		return time.Unix(0, at)
	}
	return time.Time{}
}

type countingWriter struct {
	w io.Writer
	n int64
}

func (c *countingWriter) Write(p []byte) (int, error) {
	n, err := c.w.Write(p)
	c.n += int64(n)
	return n, err
}

// RouteState is what routes are made from: the tenants, which node runs
// each, and where the nodes are.
type RouteState interface {
	Node() string
	Nodes() []store.Node
	List() []store.Record
	// Live refuses an archived tenant with store.ErrArchived: it has no
	// route.
	Live(name string) (store.Record, error)
}

var _ RouteState = (*store.SQLite)(nil)

// Routes adapts the store for the edge proxy.
type Routes struct {
	Store RouteState
	// RelayPort is where every node's edge takes requests other nodes pass
	// on; 0 outside a cluster.
	RelayPort int
	// Replacing reports a tenant of this node whose container is being
	// replaced; nil if none ever is.
	Replacing func(tenant string) bool
}

func (r Routes) Route(tenant string) (edge.Route, bool) {
	record, err := r.Store.Live(tenant)
	if err != nil {
		return edge.Route{}, false
	}
	// Moving, it runs nowhere for a moment: requests wait for it.
	if record.MoveHeld {
		return edge.Route{Moving: true, Suspended: record.Spec.Suspended}, true
	}
	if record.Node != r.Store.Node() {
		return r.relay(record)
	}
	// So does a tenant whose container is being replaced, and one whose
	// data goes back to before its rollout moved it, until it has: nothing
	// is written to data about to be replaced. Other nodes' edges pass its
	// requests on to this one, which holds them.
	if r.Replacing != nil && r.Replacing(tenant) || record.RestorePending() {
		return edge.Route{Moving: true, Suspended: record.Spec.Suspended}, true
	}
	backend, err := reconcile.Backend(record)
	if err != nil {
		return edge.Route{}, false
	}
	return edge.Route{Backend: backend, Suspended: record.Spec.Suspended}, true
}

// relay routes to the edge of the node that runs the tenant, on the private
// network, at the host of its cluster address.
func (r Routes) relay(record store.Record) (edge.Route, bool) {
	if r.RelayPort == 0 {
		return edge.Route{}, false
	}
	for _, node := range r.Store.Nodes() {
		if node.Name != record.Node {
			continue
		}
		host, _, err := net.SplitHostPort(node.Address)
		if err != nil {
			return edge.Route{}, false
		}
		return edge.Route{Relay: net.JoinHostPort(host, strconv.Itoa(r.RelayPort)), Suspended: record.Spec.Suspended}, true
	}
	return edge.Route{}, false
}

type statusError struct {
	status int
	err    error
}

func (e statusError) Error() string { return e.err.Error() }

func (e statusError) Unwrap() error { return e.err }

func badRequest(err error) error { return statusError{http.StatusBadRequest, err} }

func notFound(err error) error { return statusError{http.StatusNotFound, err} }

// clientGone marks work refused because the client disconnected: nginx's 499,
// not a server failure.
func clientGone(err error) error { return statusError{499, err} }

func writeError(w http.ResponseWriter, err error) {
	status := http.StatusInternalServerError
	var typed statusError
	var peer *cluster.PeerError
	switch {
	case errors.As(err, &typed):
		status = typed.status
	case errors.As(err, &peer) && (peer.Status >= 400 && peer.Status < 500 || peer.Status == http.StatusServiceUnavailable):
		// What another node answered, a refusal or "try again", stays
		// that for the client.
		status = peer.Status
	case errors.Is(err, store.ErrNotFound):
		status = http.StatusNotFound
	case errors.Is(err, store.ErrExists), errors.Is(err, store.ErrArchived), errors.Is(err, store.ErrNotArchived), errors.Is(err, store.ErrInUse), errors.Is(err, store.ErrConflict):
		status = http.StatusConflict
	case errors.As(err, new(*spec.ValidationError)), errors.Is(err, store.ErrInvalid):
		status = http.StatusBadRequest
	case errors.Is(err, reconcile.ErrElsewhere):
		status = http.StatusConflict
	case errors.Is(err, reconcile.ErrStopping), errors.Is(err, cluster.ErrNoLeader), errors.Is(err, cluster.ErrNotLeader):
		status = http.StatusServiceUnavailable
	case errors.Is(err, store.ErrStale), errors.Is(err, store.ErrFrozen), errors.Is(err, cluster.ErrNotJoined), errors.Is(err, cluster.ErrMember):
		status = http.StatusConflict
	case errors.Is(err, context.Canceled):
		status = 499
	}
	writeJSON(w, status, api.ErrorBody{Error: err.Error()})
}

// readJSON reads one JSON value of at most 1 MiB, and nothing after it.
func readJSON(w http.ResponseWriter, r *http.Request, out any) bool {
	decoder := json.NewDecoder(http.MaxBytesReader(w, r.Body, 1<<20))
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(out); err != nil {
		writeError(w, badRequest(fmt.Errorf("request body: %w", err)))
		return false
	}
	if _, err := decoder.Token(); !errors.Is(err, io.EOF) {
		writeError(w, badRequest(errors.New("request body: more than one JSON value")))
		return false
	}
	return true
}

func writeJSON(w http.ResponseWriter, status int, value any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(value)
}
