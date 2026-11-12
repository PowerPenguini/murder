package access

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"log/slog"
	"os"
	"path/filepath"
	"sync"
	"testing"
	"time"

	"github.com/PowerPenguini/murder/internal/store"
)

// fakeRegister is the store's register: with once, an access of the same
// action, tenant and detail recorded before is returned instead of a
// second. Until down is cleared, it cannot record, as with no quorum.
type fakeRegister struct {
	mu     sync.Mutex
	events []store.Event
	down   error
	// during runs inside a write, as another writer of the file would.
	during func()
}

func (r *fakeRegister) Access(tenant, action, detail, actor string, once bool) (store.Event, error) {
	if r.during != nil {
		r.during()
		r.during = nil
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.down != nil {
		return store.Event{}, r.down
	}
	if once {
		for _, e := range r.events {
			if e.Tenant == tenant && e.Action == action && e.Detail == detail {
				return e, nil
			}
		}
	}
	e := store.Event{ID: int64(len(r.events) + 1), Tenant: tenant, Action: action, Detail: detail, Actor: actor}
	r.events = append(r.events, e)
	return e, nil
}

func (r *fakeRegister) recorded() []store.Event {
	r.mu.Lock()
	defer r.mu.Unlock()
	return append([]store.Event(nil), r.events...)
}

func writeOffline(t *testing.T, path, tenant string) OfflineRecord {
	t.Helper()
	record, err := WriteOffline(path, Request{Tenant: tenant, Kind: Manual, What: "read its files", Purpose: "the cluster is down", Ticket: "incident"},
		"jan via root", "jan", time.Date(2026, 11, 10, 9, 0, 0, 0, time.UTC))
	if err != nil {
		t.Fatal(err)
	}
	return record
}

// A sync records every record once, as of when and by whom it was made,
// and empties the file; a sync that stopped before it cut the file
// records none twice.
func TestASyncRecordsEachOnce(t *testing.T) {
	path := filepath.Join(t.TempDir(), "access-offline.jsonl")
	first, second := writeOffline(t, path, "acme"), writeOffline(t, path, "beta")
	register := &fakeRegister{}
	synced, err := SyncOffline(path, register)
	if err != nil || synced.Recorded != 2 || synced.Left != 0 {
		t.Fatalf("synced %+v, %v", synced, err)
	}
	if _, err := os.Stat(path); !os.IsNotExist(err) {
		t.Fatal("the file stays after a sync")
	}
	events := register.recorded()
	if len(events) != 2 || events[0].Actor != "jan via root" || events[0].Action != "access.manual" {
		t.Fatalf("recorded %+v", events)
	}
	for i, record := range []OfflineRecord{first, second} {
		entry, ok := FromEvent(events[i])
		if !ok || entry.Offline != record.ID || !entry.When().Equal(record.At) || entry.Tenant != record.Tenant {
			t.Fatalf("entry %+v of %+v", entry, record)
		}
	}
	// As if the sync stopped before it cut the file: the records are
	// there again, and are found recorded.
	for _, record := range []OfflineRecord{first, second} {
		appendRecord(t, path, record)
	}
	if synced, err := SyncOffline(path, register); err != nil || synced.Recorded != 2 || len(register.recorded()) != 2 {
		t.Fatalf("synced again %+v, %v: %+v", synced, err, register.recorded())
	}
}

// appendRecord writes record to the file again, as it was written once.
func appendRecord(t *testing.T, path string, record OfflineRecord) {
	t.Helper()
	line, err := json.Marshal(record)
	if err != nil {
		t.Fatal(err)
	}
	file, err := os.OpenFile(path, os.O_WRONLY|os.O_APPEND|os.O_CREATE, 0o600)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := file.Write(append(line, '\n')); err != nil {
		t.Fatal(err)
	}
	file.Close()
}

// A register that cannot record keeps every record in the file; a record
// written during a sync, and a damaged line, stay too.
func TestASyncLosesNothing(t *testing.T) {
	path := filepath.Join(t.TempDir(), "access-offline.jsonl")
	first := writeOffline(t, path, "acme")
	register := &fakeRegister{down: errors.New("no quorum")}
	if synced, err := SyncOffline(path, register); err == nil || synced.Recorded != 0 || synced.Left != 1 {
		t.Fatalf("synced with no quorum %+v, %v", synced, err)
	}
	if records, _, _ := ReadOffline(path); len(records) != 1 || records[0].ID != first.ID {
		t.Fatalf("left %+v", records)
	}
	// A line cut short, and a record written while the sync records.
	file, err := os.OpenFile(path, os.O_WRONLY|os.O_APPEND, 0o600)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := file.WriteString(`{"id":"TORN","purp`); err != nil {
		t.Fatal(err)
	}
	file.Close()
	var during OfflineRecord
	register.down = nil
	register.during = func() { during = writeOffline(t, path, "beta") }
	synced, err := SyncOffline(path, register)
	if err != nil || synced.Recorded != 1 || synced.Damaged != 1 {
		t.Fatalf("synced %+v, %v", synced, err)
	}
	records, damaged, err := ReadOffline(path)
	if err != nil || len(records) != 1 || records[0].ID != during.ID || len(damaged) != 1 || string(damaged[0]) != `{"id":"TORN","purp` {
		t.Fatalf("left %+v %q %v", records, damaged, err)
	}
	if synced, err := SyncOffline(path, register); err != nil || synced.Recorded != 1 || len(register.recorded()) != 2 {
		t.Fatalf("the one written meanwhile: %+v, %v", synced, err)
	}
}

// The agent syncs at once when it starts, and again while records wait,
// as soon as the register records again.
func TestTheAgentSyncsByItself(t *testing.T) {
	path := filepath.Join(t.TempDir(), "access-offline.jsonl")
	writeOffline(t, path, "acme")
	register := &fakeRegister{}
	syncer := &Syncer{Path: path, Register: register, Every: 10 * time.Millisecond, Log: slog.New(slog.NewTextHandler(io.Discard, nil))}
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() {
		syncer.Run(ctx)
		close(done)
	}()
	defer func() {
		cancel()
		<-done
	}()
	waitFor(t, func() bool { return len(register.recorded()) == 1 })
	register.mu.Lock()
	register.down = errors.New("no quorum")
	register.mu.Unlock()
	writeOffline(t, path, "beta")
	time.Sleep(50 * time.Millisecond)
	if records, _, _ := ReadOffline(path); len(records) != 1 || len(register.recorded()) != 1 {
		t.Fatalf("with no quorum: %+v", records)
	}
	register.mu.Lock()
	register.down = nil
	register.mu.Unlock()
	waitFor(t, func() bool {
		_, err := os.Stat(path)
		return os.IsNotExist(err)
	})
	if got := register.recorded(); len(got) != 2 || got[1].Tenant != "beta" {
		t.Fatalf("recorded %+v", got)
	}
}

func waitFor(t *testing.T, ok func() bool) {
	t.Helper()
	for deadline := time.Now().Add(5 * time.Second); time.Now().Before(deadline); time.Sleep(5 * time.Millisecond) {
		if ok() {
			return
		}
	}
	t.Fatal("timed out")
}
