package apiserver

import (
	"errors"
	"fmt"
	"net/http"
	"strconv"

	"github.com/PowerPenguini/murder/internal/api"
	"github.com/PowerPenguini/murder/internal/store"
)

// errNoTag refuses to ask the registry about an app given a pinned image.
func errNoTag(app string) error {
	return statusError{http.StatusUnprocessableEntity, fmt.Errorf("app %s was given a pinned image, not a tag: there is nothing to ask its registry about", app)}
}

// checkApp asks the app's registry now which image its tag names, and
// records it as the leader's checks do.
func (s *Server) checkApp(w http.ResponseWriter, r *http.Request) {
	name := r.PathValue("name")
	app, err := s.store.App(name)
	if err != nil {
		writeError(w, err)
		return
	}
	if app.Tag == "" {
		writeError(w, errNoTag(name))
		return
	}
	latest, err := s.runtime.Latest(r.Context(), app.Tag)
	if err != nil {
		writeError(w, statusError{http.StatusBadGateway, err})
		return
	}
	if _, err := s.store.SetLatest(name, app.Tag, latest, actor(r)); err != nil {
		writeError(w, err)
		return
	}
	if app, err = s.store.App(name); err != nil {
		writeError(w, err)
		return
	}
	writeJSON(w, http.StatusOK, api.AppCheck{App: s.appView(app), Image: latest, Newer: !store.SameImage(latest, app.Spec.Image)})
}

// updateApp pins the app's tag to its newest image and rolls it out, the
// rest of the spec as it is. Unless the request names the version it read,
// the app as read now is the one updated: a change made meanwhile refuses
// the update.
func (s *Server) updateApp(w http.ResponseWriter, r *http.Request) {
	name := r.PathValue("name")
	cond, err := appCondition(r)
	if err != nil {
		writeError(w, err)
		return
	}
	if cond.Absent {
		writeError(w, badRequest(errors.New("an update needs an app that exists")))
		return
	}
	if err := s.catchUp(); err != nil {
		writeError(w, err)
		return
	}
	app, err := s.store.App(name)
	if err != nil {
		writeError(w, err)
		return
	}
	if app.Tag == "" {
		writeError(w, errNoTag(name))
		return
	}
	if cond.UpdatedAt.IsZero() {
		cond.UpdatedAt = app.UpdatedAt
	}
	next := app.Spec
	next.Image = app.Tag
	s.storeApp(w, r, name, next, cond, true)
}

func (s *Server) appRevisions(w http.ResponseWriter, r *http.Request) {
	revisions, err := s.store.Revisions(r.PathValue("name"))
	if err != nil {
		writeError(w, err)
		return
	}
	writeJSON(w, http.StatusOK, revisions)
}

// restoreRevision stores one of the app's revisions again, as its newest,
// and rolls it out.
func (s *Server) restoreRevision(w http.ResponseWriter, r *http.Request) {
	revision, err := strconv.ParseInt(r.PathValue("revision"), 10, 64)
	if err != nil || revision < 1 {
		writeError(w, badRequest(fmt.Errorf("revision %q is not a revision number", r.PathValue("revision"))))
		return
	}
	cond, err := appCondition(r)
	if err != nil {
		writeError(w, err)
		return
	}
	if cond.Absent {
		writeError(w, badRequest(errors.New("a revision restores an app that exists")))
		return
	}
	if err := s.catchUp(); err != nil {
		writeError(w, err)
		return
	}
	change, err := s.store.RestoreRevision(r.PathValue("name"), revision, cond, actor(r))
	if err != nil {
		writeError(w, err)
		return
	}
	s.writeAppChange(w, change, "")
}
