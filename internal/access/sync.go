package access

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"time"

	"github.com/PowerPenguini/murder/internal/store"
)

// Register is where an agent records accesses: its store, whose writes go
// to the cluster.
type Register interface {
	Access(tenant, action, detail, actor string, once bool) (store.Event, error)
}

// Synced is what a sync of the offline file did.
type Synced struct {
	// Recorded is how many records are in the register now and left the
	// file; Left how many stay, Damaged how many lines are no record.
	Recorded, Left, Damaged int
}

// SyncOffline records the records of the offline file at path in the
// register, and keeps in the file only those it could not record and the
// damaged lines. Each is recorded once, however many syncs run, on this
// node or another, or stop halfway: the register finds a record recorded
// before by its ID, which the event's detail holds. A record the register
// refuses stays, for the next sync or a person.
func SyncOffline(path string, register Register) (Synced, error) {
	records, damaged, err := ReadOffline(path)
	if err != nil {
		return Synced{}, err
	}
	result := Synced{Damaged: len(damaged)}
	if len(records) == 0 {
		return result, nil
	}
	synced := map[string]bool{}
	var failed error
	for _, record := range records {
		request := record.Synced()
		if err := request.Normalize(); err != nil {
			failed = errors.Join(failed, fmt.Errorf("record %s: %w", record.ID, err))
			continue
		}
		// Who made the access, as the agent would have named them then.
		actor := record.Actor
		if actor == "" {
			actor = "unknown"
		}
		if _, err := register.Access(request.Tenant, request.Kind.Action(), request.Detail(), actor, true); err != nil {
			failed = errors.Join(failed, fmt.Errorf("record %s: %w", record.ID, err))
			continue
		}
		synced[record.ID] = true
	}
	// Records written meanwhile stay: KeepOffline reads the file again
	// under its lock.
	if err := KeepOffline(path, synced); err != nil {
		return result, errors.Join(failed, err)
	}
	result.Recorded, result.Left = len(synced), len(records)-len(synced)
	return result, failed
}

// SyncEvery is how often the agent syncs the offline file while it holds
// records: an access written there is in the register within about this
// long once the cluster answers.
const SyncEvery = time.Minute

// Syncer has the agent record the offline file's accesses itself: once
// it starts, and then every Every while the file holds any. It writes
// nothing to the register while the file is empty, and costs a stat then.
type Syncer struct {
	Path     string
	Register Register
	Every    time.Duration
	Log      *slog.Logger
}

// Run syncs until ctx is done.
func (s *Syncer) Run(ctx context.Context) {
	failing := ""
	warned := 0
	for {
		failing, warned = s.once(failing, warned)
		select {
		case <-ctx.Done():
			return
		case <-time.After(s.Every):
		}
	}
}

// once syncs if the file holds anything. It says a failure when it begins
// or changes, not every minute it lasts, and when it ends; and damaged
// lines when their count changes. It returns the failure, and the damaged
// lines said, for the next.
func (s *Syncer) once(failing string, warned int) (string, int) {
	records, damaged, err := ReadOffline(s.Path)
	if err == nil && len(records) == 0 {
		s.damaged(len(damaged), &warned)
		return "", warned
	}
	var result Synced
	if err == nil {
		result, err = SyncOffline(s.Path, s.Register)
		s.damaged(result.Damaged, &warned)
	}
	if result.Recorded > 0 {
		s.Log.Info("recorded the accesses written to the offline file in the register", "file", s.Path, "recorded", result.Recorded, "left", result.Left)
	}
	if err != nil {
		if err.Error() != failing {
			s.Log.Warn("the accesses written to the offline file are not all in the register yet; trying again in a minute", "file", s.Path, "left", result.Left, "error", err)
		}
		return err.Error(), warned
	}
	if failing != "" {
		s.Log.Info("the offline file's accesses are in the register", "file", s.Path)
	}
	return "", warned
}

func (s *Syncer) damaged(count int, warned *int) {
	if count > 0 && count != *warned {
		s.Log.Warn("lines of the offline file of accesses are no record, and stay there: read them, record each access with murder access log, then remove them", "file", s.Path, "lines", count)
	}
	*warned = count
}
