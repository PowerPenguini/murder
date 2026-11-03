package apiserver

import (
	"errors"
	"fmt"
	"net/http"

	"github.com/PowerPenguini/murder/internal/api"
	"github.com/PowerPenguini/murder/internal/store"
)

// rolloutAction pauses, resumes, aborts or promotes an app's rollout.
// Promote moves every tenant still waiting at once: the rollout still
// waits for them to be healthy before it is done.
func (s *Server) rolloutAction(w http.ResponseWriter, r *http.Request) {
	app := r.PathValue("name")
	if err := s.catchUp(); err != nil {
		writeError(w, err)
		return
	}
	var err error
	switch r.PathValue("action") {
	case "pause":
		err = s.store.PauseRollout(app, "paused by "+actor(r), actor(r))
	case "resume":
		err = s.store.ResumeRollout(app, actor(r))
	case "abort":
		_, err = s.store.AbortRollout(app, "aborted by "+actor(r), actor(r))
	case "promote":
		err = s.promote(app, actor(r))
	default:
		err = badRequest(errors.New("a rollout is paused, resumed, aborted or promoted"))
	}
	if err != nil {
		writeError(w, err)
		return
	}
	stored, err := s.store.App(app)
	if err != nil {
		writeError(w, err)
		return
	}
	writeJSON(w, http.StatusOK, s.appView(stored))
}

// promote moves every waiting tenant of app's running rollout now.
func (s *Server) promote(app, actor string) error {
	rollout, err := s.store.Rollout(app)
	if err != nil {
		return err
	}
	if rollout.State != store.RolloutRunning {
		return statusError{http.StatusConflict, fmt.Errorf("the rollout of app %s is %s: resume it first", app, rollout.State)}
	}
	var waiting []string
	for _, record := range s.store.List() {
		if record.App == app && !record.Archived() && !record.Hold && record.AppRevision != 0 {
			waiting = append(waiting, record.Spec.Name)
		}
	}
	if len(waiting) == 0 {
		return nil
	}
	return s.store.AdvanceRollout(app, rollout.Wave, waiting, actor)
}

// setFlags makes a tenant a canary or held, or not.
func (s *Server) setFlags(w http.ResponseWriter, r *http.Request) {
	var request api.TenantFlags
	if !readJSON(w, r, &request) {
		return
	}
	record, err := s.store.SetFlags(r.PathValue("name"), request.Canary, request.Hold, actor(r))
	if err != nil {
		writeError(w, err)
		return
	}
	writeJSON(w, http.StatusOK, s.view(record))
}
