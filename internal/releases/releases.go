// Package releases asks apps' registries, from the cluster's leader, which
// image each app's tag names now. A newer image than the app runs is
// recorded with the app, so the CLI and the panel offer it as an update;
// nothing is pulled or rolled out until someone takes it.
package releases

import (
	"context"
	"errors"
	"log/slog"
	"maps"
	"slices"
	"strings"
	"sync"
	"time"

	"github.com/PowerPenguini/murder/internal/metrics"
	"github.com/PowerPenguini/murder/internal/store"
)

// Actor is who the audit log names for an image found.
const Actor = "murder"

// askTimeout bounds one question to a registry.
const askTimeout = 30 * time.Second

// hubEvery is how often a tag on Docker Hub is asked about at most: Hub
// counts every question as a pull against its rate limits, which the
// node's own pulls share.
const hubEvery = time.Hour

// firstCheck is how long after the agent starts the first check waits, so
// a restart does not ask every registry at once with everything else.
const firstCheck = 30 * time.Second

// Store is what the watcher needs of the store.
type Store interface {
	Apps() []store.App
	SetLatest(app, tag, latest, actor string) (bool, error)
}

// Registry asks a registry, without pulling, which image a tag names.
type Registry interface {
	Latest(ctx context.Context, tag string) (string, error)
}

// Watcher asks, every Every, about the tag of every app given one.
type Watcher struct {
	Store    Store
	Registry Registry
	// Leader reports whether this node leads the cluster: only the leader
	// asks. Nil outside a cluster, where the node asks.
	Leader func() bool
	Every  time.Duration
	Log    *slog.Logger
	// Now tells the time; time.Now if nil.
	Now func() time.Time

	// hubAsked is when each Docker Hub tag was last asked about.
	hubAsked map[string]time.Time

	mu sync.Mutex
	// failing maps each app whose registry could not be asked at the last
	// check to why.
	failing map[string]string
}

// Run checks until ctx ends.
func (w *Watcher) Run(ctx context.Context) {
	wait := min(firstCheck, w.Every)
	for {
		select {
		case <-ctx.Done():
			return
		case <-time.After(wait):
		}
		wait = w.Every
		w.Check(ctx)
	}
}

// Check asks once about the tag of every app given one, on the leader; a
// tag several apps share is asked about once.
func (w *Watcher) Check(ctx context.Context) {
	if w.Leader != nil && !w.Leader() {
		w.setFailing(nil)
		return
	}
	type answer struct {
		image string
		err   error
	}
	now := time.Now
	if w.Now != nil {
		now = w.Now
	}
	if w.hubAsked == nil {
		w.hubAsked = map[string]time.Time{}
	}
	answers := map[string]answer{}
	failing := map[string]string{}
	for _, app := range w.Store.Apps() {
		if app.Tag == "" {
			continue
		}
		got, asked := answers[app.Tag]
		if !asked && OnDockerHub(app.Tag) {
			if last, ok := w.hubAsked[app.Tag]; ok && now().Sub(last) < hubEvery {
				// What failed last time is still what is known.
				if why := w.failingFor(app.Name); why != "" {
					failing[app.Name] = why
				}
				continue
			}
			w.hubAsked[app.Tag] = now()
		}
		if !asked {
			ask, cancel := context.WithTimeout(ctx, askTimeout)
			got.image, got.err = w.Registry.Latest(ask, app.Tag)
			cancel()
			answers[app.Tag] = got
		}
		if ctx.Err() != nil {
			return
		}
		if got.err != nil {
			failing[app.Name] = got.err.Error()
			if w.failingFor(app.Name) != got.err.Error() {
				w.Log.Warn("ask the registry about an app's tag", "app", app.Name, "tag", app.Tag, "error", got.err)
			}
			continue
		}
		changed, err := w.Store.SetLatest(app.Name, app.Tag, got.image, Actor)
		switch {
		case errors.Is(err, store.ErrFrozen), errors.Is(err, store.ErrNotFound):
			// Asked again at the next check.
		case err != nil:
			w.Log.Warn("record what the registry holds", "app", app.Name, "error", err)
		case changed && !store.SameImage(got.image, app.Spec.Image):
			w.Log.Info("a newer image of an app's tag is in its registry", "app", app.Name, "tag", app.Tag, "image", got.image)
		}
	}
	w.setFailing(failing)
}

// OnDockerHub reports whether tag names an image on Docker Hub: one
// without a registry host, or with Hub's own.
func OnDockerHub(tag string) bool {
	first, _, ok := strings.Cut(tag, "/")
	if !ok || !(strings.ContainsAny(first, ".:") || first == "localhost") {
		return true
	}
	return first == "docker.io" || first == "index.docker.io" || first == "registry-1.docker.io"
}

func (w *Watcher) failingFor(app string) string {
	w.mu.Lock()
	defer w.mu.Unlock()
	return w.failing[app]
}

func (w *Watcher) setFailing(failing map[string]string) {
	w.mu.Lock()
	defer w.mu.Unlock()
	w.failing = failing
}

// Failing maps each app whose registry could not be asked at the last
// check to why; empty on a node that does not lead.
func (w *Watcher) Failing() map[string]string {
	w.mu.Lock()
	defer w.mu.Unlock()
	return maps.Clone(w.failing)
}

// WriteMetrics reports, for every app given a tag, whether a newer image
// is in its registry, and on the leader whether asking failed.
func (w *Watcher) WriteMetrics(out *metrics.Writer) {
	apps := w.Store.Apps()
	out.Family("murder_app_newer_image", "gauge", "1 if a newer image of the app's tag is in its registry than the app runs.")
	for _, app := range apps {
		if app.Tag != "" {
			out.Sample("murder_app_newer_image", metrics.Bool(app.Latest != ""), "app", app.Name)
		}
	}
	if w.Leader != nil && !w.Leader() {
		return
	}
	failing := w.Failing()
	out.Family("murder_app_image_check_failing", "gauge", "On the leader, 1 if the app's registry could not be asked about its tag at the last check.")
	for _, name := range slices.Sorted(maps.Keys(appNames(apps))) {
		_, failed := failing[name]
		out.Sample("murder_app_image_check_failing", metrics.Bool(failed), "app", name)
	}
}

func appNames(apps []store.App) map[string]bool {
	names := map[string]bool{}
	for _, app := range apps {
		if app.Tag != "" {
			names[app.Name] = true
		}
	}
	return names
}
