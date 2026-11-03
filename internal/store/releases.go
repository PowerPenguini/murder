package store

import (
	"encoding/json"
	"errors"
	"fmt"
	"slices"
	"strings"
	"time"
)

// revisionsKept is how many of an app's revisions are kept to go back to.
const revisionsKept = 20

// SameImage reports whether two pinned images are the same content: the
// same digest, however the repository is written.
func SameImage(a, b string) bool {
	return contentOf(a) == contentOf(b)
}

// contentOf is the content a pinned image names: its digest, or a local
// image's ID.
func contentOf(image string) string {
	if _, digest, ok := strings.Cut(image, "@"); ok {
		return digest
	}
	return image
}

// actorOf names who made a change, also when no one is known.
func actorOf(actor string) string {
	if actor == "" {
		return "unknown"
	}
	return actor
}

func loadRevisions(q querier, state *snapshot) error {
	state.revisions = map[string][]Revision{}
	rows, err := q.Query(`SELECT app, revision, spec, tag, created_at, actor FROM app_revisions ORDER BY app, revision`)
	if err != nil {
		return err
	}
	for rows.Next() {
		var app, specJSON, created string
		var revision Revision
		if err := rows.Scan(&app, &revision.Revision, &specJSON, &revision.Tag, &created, &revision.Actor); err != nil {
			rows.Close()
			return err
		}
		if err := json.Unmarshal([]byte(specJSON), &revision.Spec); err != nil {
			rows.Close()
			return fmt.Errorf("app %s, revision %d: %w", app, revision.Revision, err)
		}
		revision.CreatedAt = parseTime(created)
		state.revisions[app] = append(state.revisions[app], revision)
	}
	return closeRows(rows)
}

func (s *SQLite) Revisions(app string) ([]Revision, error) {
	state := s.state.Load()
	if _, ok := state.apps[app]; !ok {
		return nil, fmt.Errorf("app %s: %w", app, ErrNotFound)
	}
	revisions := make([]Revision, 0, len(state.revisions[app]))
	for _, revision := range slices.Backward(state.revisions[app]) {
		revision.Spec = cloneApp(App{Spec: revision.Spec}).Spec
		revisions = append(revisions, revision)
	}
	return revisions, nil
}

func (s *SQLite) RestoreRevision(app string, revision int64, cond AppIf, actor string) (AppChange, error) {
	revisions, err := s.Revisions(app)
	if err != nil {
		return AppChange{}, err
	}
	for _, kept := range revisions {
		if kept.Revision == revision {
			return s.putApp(App{Name: app, Spec: kept.Spec, Tag: kept.Tag}, cond, revision, actor)
		}
	}
	return AppChange{}, fmt.Errorf("app %s keeps no revision %d: %w", app, revision, ErrNotFound)
}

func (s *SQLite) SetLatest(app, tag, latest, actor string) (bool, error) {
	_, err := s.write(false, func(tx *recorder, current *snapshot, now time.Time) error {
		existing, ok := current.apps[app]
		if !ok {
			return fmt.Errorf("app %s: %w", app, ErrNotFound)
		}
		if existing.Tag == "" || existing.Tag != tag {
			return errUnchanged
		}
		found := latest
		if found != "" && SameImage(found, existing.Spec.Image) {
			found = ""
		}
		if found == existing.Latest {
			return errUnchanged
		}
		if found == "" {
			return tx.Exec(`UPDATE apps SET latest = '', latest_at = NULL WHERE name = ?`, app)
		}
		if err := tx.Exec(`UPDATE apps SET latest = ?, latest_at = ? WHERE name = ?`, found, formatTime(now), app); err != nil {
			return err
		}
		return event(tx, now, actor, "app.image-found", "", app, "image "+found+" of "+tag)
	})
	if errors.Is(err, errUnchanged) {
		return false, nil
	}
	return err == nil, err
}
