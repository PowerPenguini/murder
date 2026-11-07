package access

import (
	"bytes"
	"crypto/rand"
	"encoding/json"
	"errors"
	"fmt"
	"io/fs"
	"os"
	"path/filepath"
	"syscall"
	"time"
)

// OfflineFile is where a node keeps the accesses it could not have the
// agent record: during a disaster recovery, with no agent or no quorum.
// "murder access sync" records them once the agent can.
const OfflineFile = "/var/lib/murder/access-offline.jsonl"

// OfflineRecord is an access written to a node's offline file.
type OfflineRecord struct {
	// ID tells the record apart; the event it is synced as names it, so a
	// sync that stops halfway records none twice.
	ID string `json:"id"`
	// At is when it was written: when the access was.
	At time.Time `json:"at"`
	// Actor names who made it as the agent would ("jan via root");
	// Operator is the person sudo named, if any, whom a sync names to the
	// agent in turn.
	Actor    string `json:"actor"`
	Operator string `json:"operator,omitempty"`
	Request
}

// WriteOffline appends an access to the offline file at path, on disk
// before it returns: the access goes ahead only once it is written.
func WriteOffline(path string, request Request, actor, operator string, now time.Time) (OfflineRecord, error) {
	if err := request.Normalize(); err != nil {
		return OfflineRecord{}, err
	}
	request.AccessedAt, request.Offline = time.Time{}, ""
	record := OfflineRecord{ID: rand.Text(), At: now.UTC(), Actor: actor, Operator: operator, Request: request}
	line, err := json.Marshal(record)
	if err != nil {
		return OfflineRecord{}, err
	}
	if err := os.MkdirAll(filepath.Dir(path), 0o700); err != nil {
		return OfflineRecord{}, err
	}
	unlock, err := lock(path)
	if err != nil {
		return OfflineRecord{}, err
	}
	defer unlock()
	file, err := os.OpenFile(path, os.O_RDWR|os.O_APPEND|os.O_CREATE, 0o600)
	if err != nil {
		return OfflineRecord{}, err
	}
	// A write cut short before (a full disk) left a line unended: this
	// one starts on a line of its own, and that one stays damaged.
	if info, statErr := file.Stat(); statErr == nil && info.Size() > 0 {
		last := make([]byte, 1)
		if _, err := file.ReadAt(last, info.Size()-1); err == nil && last[0] != '\n' {
			line = append([]byte{'\n'}, line...)
		}
	}
	_, err = file.Write(append(line, '\n'))
	if err = errors.Join(err, file.Sync(), file.Close()); err != nil {
		return OfflineRecord{}, fmt.Errorf("write the access to %s: %w", path, err)
	}
	return record, syncDir(filepath.Dir(path))
}

// ReadOffline reads the offline file at path; none is no records. Lines
// that are no record (one cut short by a full disk, say) are damaged: they
// are returned apart, as they are, never dropped.
func ReadOffline(path string) (records []OfflineRecord, damaged [][]byte, err error) {
	data, err := os.ReadFile(path)
	if errors.Is(err, fs.ErrNotExist) {
		return nil, nil, nil
	}
	if err != nil {
		return nil, nil, err
	}
	for _, line := range bytes.Split(data, []byte{'\n'}) {
		if len(bytes.TrimSpace(line)) == 0 {
			continue
		}
		var record OfflineRecord
		if err := json.Unmarshal(line, &record); err != nil || record.ID == "" || record.Request.Check() != nil {
			damaged = append(damaged, bytes.Clone(line))
			continue
		}
		records = append(records, record)
	}
	return records, damaged, nil
}

// KeepOffline replaces the offline file at path with its records not
// synced yet, and its damaged lines as they are; none removes it. A record written meanwhile, past those
// read, is kept too: writers and this take the file's lock.
func KeepOffline(path string, synced map[string]bool) error {
	unlock, err := lock(path)
	if err != nil {
		return err
	}
	defer unlock()
	records, damaged, err := ReadOffline(path)
	if err != nil {
		return err
	}
	var kept bytes.Buffer
	for _, record := range records {
		if synced[record.ID] {
			continue
		}
		line, err := json.Marshal(record)
		if err != nil {
			return err
		}
		kept.Write(append(line, '\n'))
	}
	for _, line := range damaged {
		kept.Write(append(line, '\n'))
	}
	if kept.Len() == 0 {
		if err := os.Remove(path); err != nil && !errors.Is(err, fs.ErrNotExist) {
			return err
		}
		return syncDir(filepath.Dir(path))
	}
	temp, err := os.CreateTemp(filepath.Dir(path), ".access-offline-*")
	if err != nil {
		return err
	}
	defer os.Remove(temp.Name())
	_, err = temp.Write(kept.Bytes())
	if err = errors.Join(err, temp.Chmod(0o600), temp.Sync(), temp.Close()); err != nil {
		return err
	}
	if err := os.Rename(temp.Name(), path); err != nil {
		return err
	}
	return syncDir(filepath.Dir(path))
}

// Synced is the request an offline record is recorded as.
func (o OfflineRecord) Synced() Request {
	request := o.Request
	request.AccessedAt, request.Offline = o.At, o.ID
	return request
}

// lock takes the lock of the offline file at path, beside it, for as long
// as one process appends to it or rewrites it.
func lock(path string) (func(), error) {
	file, err := os.OpenFile(path+".lock", os.O_RDWR|os.O_CREATE, 0o600)
	if err != nil {
		return nil, err
	}
	if err := syscall.Flock(int(file.Fd()), syscall.LOCK_EX); err != nil {
		file.Close()
		return nil, fmt.Errorf("lock %s: %w", path, err)
	}
	return func() { file.Close() }, nil
}

func syncDir(dir string) error {
	d, err := os.Open(dir)
	if err != nil {
		return err
	}
	return errors.Join(d.Sync(), d.Close())
}
