package secrets

import (
	"crypto/rand"
	"encoding/base64"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"sync"
)

// ringMu keeps this process's changes of key files one at a time.
var ringMu sync.Mutex

// NewRaw makes a new random key.
func NewRaw() ([]byte, error) {
	raw := make([]byte, keySize)
	if _, err := rand.Read(raw); err != nil {
		return nil, err
	}
	return raw, nil
}

// AddNext adds raw to the ring in path as the next key: trusted and
// opening, not used yet. A ring that has it already is left alone.
func AddNext(path string, raw []byte) error {
	added, err := newEntry(raw, true)
	if err != nil {
		return err
	}
	return changeRing(path, func(keys []entry) ([]entry, error) {
		if slices.ContainsFunc(keys, func(e entry) bool { return e.id == added.id }) {
			return keys, nil
		}
		return append(keys, added), nil
	})
}

// Use makes the key id, which the ring in path must have, the key in use;
// the one in use before stays as an older key.
func Use(path, id string) error {
	return changeRing(path, func(keys []entry) ([]entry, error) {
		at := slices.IndexFunc(keys, func(e entry) bool { return e.id == id })
		if at < 0 {
			return nil, fmt.Errorf("%w: key %s", errNoKey, id)
		}
		used := keys[at]
		used.next = false
		return append([]entry{used}, slices.Delete(slices.Clone(keys), at, at+1)...), nil
	})
}

// Drop takes the key id out of the ring in path; not the one in use. A
// ring without it is left alone.
func Drop(path, id string) error {
	return changeRing(path, func(keys []entry) ([]entry, error) {
		at := slices.IndexFunc(keys, func(e entry) bool { return e.id == id })
		switch {
		case at < 0:
			return keys, nil
		case at == 0:
			return nil, fmt.Errorf("key %s is the one in use: use another first", id)
		}
		return slices.Delete(slices.Clone(keys), at, at+1), nil
	})
}

var errNoKey = errors.New("the master key file does not have it")

// IsNoKey reports whether err says a ring lacks a key.
func IsNoKey(err error) bool { return errors.Is(err, errNoKey) }

// changeRing rewrites the ring in path as change makes it: a new file in
// its place, so a reader sees the old ring or the new one, never half.
func changeRing(path string, change func([]entry) ([]entry, error)) error {
	ringMu.Lock()
	defer ringMu.Unlock()
	current, err := readRing(path)
	if err != nil {
		return err
	}
	keys, err := change(current.keys)
	if err != nil {
		return err
	}
	var out strings.Builder
	for _, e := range keys {
		if e.next {
			out.WriteString("next ")
		}
		out.WriteString(base64.StdEncoding.EncodeToString(e.raw) + "\n")
	}
	if out.String() == current.file {
		return nil
	}
	if _, err := parseRing(out.String()); err != nil {
		return err
	}
	return replaceFile(path, out.String())
}

func replaceFile(path, data string) error {
	file, err := os.CreateTemp(filepath.Dir(path), ".master.key-*")
	if err != nil {
		return err
	}
	defer os.Remove(file.Name())
	if err := file.Chmod(0o600); err != nil {
		_ = file.Close()
		return err
	}
	if _, err := file.WriteString(data); err != nil {
		_ = file.Close()
		return err
	}
	if err := file.Sync(); err != nil {
		_ = file.Close()
		return err
	}
	if err := file.Close(); err != nil {
		return err
	}
	if err := os.Rename(file.Name(), path); err != nil {
		return err
	}
	dir, err := os.Open(filepath.Dir(path))
	if err != nil {
		return err
	}
	defer dir.Close()
	return dir.Sync()
}
