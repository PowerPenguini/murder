package cluster

import (
	"errors"
	"fmt"
	"io"
	"os"
	"sync/atomic"

	"github.com/hashicorp/raft"

	"github.com/PowerPenguini/murder/internal/store"
)

// fsm applies the log's batches to the node's store. A failure that is the
// node's own, such as a full disk, stops it: applying later entries to a
// state that missed one would make it differ from every other node. A
// restart applies the entry again.
type fsm struct {
	store  *store.SQLite
	fatal  func(error)
	broken atomic.Bool
}

func (f *fsm) Apply(entry *raft.Log) any {
	if entry.Type != raft.LogCommand {
		return nil
	}
	if f.broken.Load() {
		return errBroken
	}
	batch, err := store.DecodeBatch(entry.Data)
	if err != nil {
		// Every node refuses it alike; the leader checks what it appends.
		return err
	}
	err = f.store.ApplyEntry(entry.Index, batch)
	if err == nil || store.Refused(err) {
		return err
	}
	f.broken.Store(true)
	err = fmt.Errorf("apply log entry %d: %w", entry.Index, err)
	f.fatal(err)
	return err
}

var errBroken = errors.New("this node stopped applying the log; restart it")

// Snapshot copies the database while Raft applies nothing; Persist then
// streams the copy.
func (f *fsm) Snapshot() (raft.FSMSnapshot, error) {
	// Raft counts the entries a stopped node did not apply as applied: a
	// snapshot would claim them.
	if f.broken.Load() {
		return nil, errBroken
	}
	name, err := f.store.SnapshotFile()
	if err != nil {
		return nil, err
	}
	return fileSnapshot(name), nil
}

// Restore installs a snapshot from the leader, or, at start, the node's own
// last snapshot, which the database usually holds already.
func (f *fsm) Restore(snapshot io.ReadCloser) error {
	defer snapshot.Close()
	_, err := f.store.InstallSnapshot(snapshot)
	return err
}

type fileSnapshot string

func (s fileSnapshot) Persist(sink raft.SnapshotSink) error {
	file, err := os.Open(string(s))
	if err != nil {
		_ = sink.Cancel()
		return err
	}
	defer file.Close()
	if _, err := io.Copy(sink, file); err != nil {
		_ = sink.Cancel()
		return err
	}
	return sink.Close()
}

func (s fileSnapshot) Release() { _ = os.Remove(string(s)) }
