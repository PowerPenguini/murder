// Package secrets generates tenant secrets and seals them with the node's
// master key, so the state file never holds a secret in the clear.
package secrets

import (
	"bytes"
	"context"
	"crypto/aes"
	"crypto/cipher"
	"crypto/hkdf"
	"crypto/rand"
	"crypto/sha256"
	"encoding/base64"
	"encoding/hex"
	"errors"
	"fmt"
	"log/slog"
	"math/big"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"sync/atomic"
	"time"

	"github.com/PowerPenguini/murder/internal/spec"
)

const keySize = 32

// Key seals and opens secrets with AES-256-GCM. It is a ring of master
// keys, as the master key file lists them, one base64 key a line:
//
//   - the first is the key in use: it seals, and the cluster's authority
//     is derived from it;
//   - each line after it is an older key, still opening what it sealed and
//     still trusted between nodes;
//   - a line "next KEY" is a key on its way in, during a rotation: trusted,
//     opening, not used yet.
//
// A file of one key, as every node had before rotations, is a ring of one.
// Reload reads the file again, so every holder of the Key sees a rotation.
type Key struct {
	path string
	ring atomic.Pointer[ring]
}

type ring struct {
	keys []entry
	// file is what the ring was read from, to tell a changed file.
	file string
}

type entry struct {
	raw  []byte
	aead cipher.AEAD
	id   string
	next bool
}

// Entry describes one key of a ring, without the key.
type Entry struct {
	ID string `json:"id"`
	// State is current, old or next.
	State string `json:"state"`
}

// States of a key in a ring.
const (
	StateCurrent = "current"
	StateOld     = "old"
	StateNext    = "next"
)

// LoadKey reads a master key ring from path. The file must not be readable
// by group or others.
func LoadKey(path string) (*Key, error) {
	k := &Key{path: path}
	if _, err := k.Reload(); err != nil {
		return nil, err
	}
	return k, nil
}

// readRing reads and checks the ring in path.
func readRing(path string) (*ring, error) {
	info, err := os.Stat(path)
	if err != nil {
		return nil, fmt.Errorf("read master key: %w", err)
	}
	if info.Mode().Perm()&0o077 != 0 {
		return nil, fmt.Errorf("master key %s must not be accessible to group or others (mode %v)", path, info.Mode().Perm())
	}
	data, err := os.ReadFile(path)
	if err != nil {
		return nil, fmt.Errorf("read master key: %w", err)
	}
	r, err := parseRing(string(data))
	if err != nil {
		return nil, fmt.Errorf("master key %s: %w", path, err)
	}
	return r, nil
}

func parseRing(data string) (*ring, error) {
	r := &ring{file: data}
	for line := range strings.Lines(data) {
		line = strings.TrimSpace(line)
		if line == "" || strings.HasPrefix(line, "#") {
			continue
		}
		encoded, next := strings.CutPrefix(line, "next ")
		raw, err := base64.StdEncoding.DecodeString(strings.TrimSpace(encoded))
		if err != nil || len(raw) != keySize {
			return nil, fmt.Errorf("each key must be %d base64-encoded bytes", keySize)
		}
		e, err := newEntry(raw, next)
		if err != nil {
			return nil, err
		}
		if slices.ContainsFunc(r.keys, func(have entry) bool { return have.id == e.id }) {
			return nil, fmt.Errorf("key %s is there twice", e.id)
		}
		r.keys = append(r.keys, e)
	}
	switch {
	case len(r.keys) == 0:
		return nil, errors.New("no key in it")
	case r.keys[0].next:
		return nil, errors.New("its first key is the one in use, so it cannot be a next one")
	}
	return r, nil
}

func newEntry(raw []byte, next bool) (entry, error) {
	if len(raw) != keySize {
		return entry{}, fmt.Errorf("master key must be %d bytes", keySize)
	}
	block, err := aes.NewCipher(raw)
	if err != nil {
		return entry{}, err
	}
	aead, err := cipher.NewGCM(block)
	if err != nil {
		return entry{}, err
	}
	return entry{raw: bytes.Clone(raw), aead: aead, id: KeyID(raw), next: next}, nil
}

// KeyID names a key without revealing it.
func KeyID(raw []byte) string {
	sum := sha256.Sum256(append([]byte("murder key id\x00"), raw...))
	return hex.EncodeToString(sum[:4])
}

// NewKey builds a Key of one key from raw key bytes.
func NewKey(raw []byte) (*Key, error) {
	e, err := newEntry(raw, false)
	if err != nil {
		return nil, err
	}
	k := &Key{}
	k.ring.Store(&ring{keys: []entry{e}})
	return k, nil
}

// Reload reads the key file again, reporting whether the ring changed. A
// file that does not read keeps the ring as it was.
func (k *Key) Reload() (bool, error) {
	if k.path == "" {
		return false, nil
	}
	r, err := readRing(k.path)
	if err != nil {
		return false, err
	}
	if old := k.ring.Load(); old != nil && old.file == r.file {
		return false, nil
	}
	k.ring.Store(r)
	return true, nil
}

// Watch reloads the key file every so often until ctx ends, calling
// changed after each change.
func (k *Key) Watch(ctx context.Context, every time.Duration, changed func(), log *slog.Logger) {
	ticker := time.NewTicker(every)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
		}
		switch ok, err := k.Reload(); {
		case err != nil:
			log.Warn("read the master key again", "error", err)
		case ok:
			changed()
		}
	}
}

// Ring describes the keys, the one in use first.
func (k *Key) Ring() []Entry {
	var out []Entry
	for i, e := range k.ring.Load().keys {
		state := StateOld
		switch {
		case i == 0:
			state = StateCurrent
		case e.next:
			state = StateNext
		}
		out = append(out, Entry{ID: e.id, State: state})
	}
	return out
}

// Current is the ID of the key in use.
func (k *Key) Current() string { return k.ring.Load().keys[0].id }

// Raw is the key named id, if the ring has it.
func (k *Key) Raw(id string) ([]byte, bool) {
	for _, e := range k.ring.Load().keys {
		if e.id == id {
			return bytes.Clone(e.raw), true
		}
	}
	return nil, false
}

// CreateKey writes a new random master key to path, refusing to overwrite one.
func CreateKey(path string) error {
	raw := make([]byte, keySize)
	if _, err := rand.Read(raw); err != nil {
		return err
	}
	if err := os.MkdirAll(filepath.Dir(path), 0o700); err != nil {
		return err
	}
	file, err := os.OpenFile(path, os.O_WRONLY|os.O_CREATE|os.O_EXCL, 0o600)
	if err != nil {
		if errors.Is(err, os.ErrExist) {
			return fmt.Errorf("master key %s already exists", path)
		}
		return err
	}
	if _, err := file.WriteString(base64.StdEncoding.EncodeToString(raw) + "\n"); err != nil {
		_ = file.Close()
		return err
	}
	if err := file.Sync(); err != nil {
		_ = file.Close()
		return err
	}
	return file.Close()
}

// Seal encrypts plaintext with the key in use, bound to context (tenant
// and secret name), so a sealed value cannot be moved to another tenant or
// key unnoticed.
func (k *Key) Seal(context, plaintext string) (string, error) {
	return seal(k.ring.Load().keys[0], context, plaintext)
}

func seal(e entry, context, plaintext string) (string, error) {
	nonce := make([]byte, e.aead.NonceSize())
	if _, err := rand.Read(nonce); err != nil {
		return "", err
	}
	sealed := e.aead.Seal(nonce, nonce, []byte(plaintext), []byte(context))
	return base64.StdEncoding.EncodeToString(sealed), nil
}

// Open decrypts a value sealed with any key of the ring and the same
// context.
func (k *Key) Open(context, sealed string) (string, error) {
	plaintext, _, err := k.open(context, sealed)
	return plaintext, err
}

// open is Open, saying whether the key in use opened it.
func (k *Key) open(context, sealed string) (string, bool, error) {
	raw, err := base64.StdEncoding.DecodeString(sealed)
	if err != nil {
		return "", false, errors.New("sealed secret is malformed")
	}
	for i, e := range k.ring.Load().keys {
		if len(raw) < e.aead.NonceSize() {
			return "", false, errors.New("sealed secret is malformed")
		}
		nonce, ciphertext := raw[:e.aead.NonceSize()], raw[e.aead.NonceSize():]
		if plaintext, err := e.aead.Open(nil, nonce, ciphertext, []byte(context)); err == nil {
			return string(plaintext), i == 0, nil
		}
	}
	return "", false, errors.New("sealed secret does not open with this master key")
}

// Reseal seals a value again with the key in use, if another key sealed
// it; changed is false for a value the key in use sealed already.
func (k *Key) Reseal(context, sealed string) (resealed string, changed bool, err error) {
	plaintext, current, err := k.open(context, sealed)
	if err != nil || current {
		return sealed, false, err
	}
	resealed, err = k.Seal(context, plaintext)
	return resealed, err == nil, err
}

// Digest names a secret's value without revealing it or depending on the
// key that seals it: it changes when the value does, and only then.
func Digest(context, plaintext string) string {
	sum := sha256.Sum256([]byte("murder secret\x00" + context + "\x00" + plaintext))
	return hex.EncodeToString(sum[:])
}

// Context names the sealing context of one tenant secret.
func Context(tenant, name string) string { return tenant + "/" + name }

// NewSealed generates a value for one secret of tenant and seals it with k.
func (k *Key) NewSealed(tenant, name string, secret spec.Secret) (string, error) {
	plain, err := Generate(secret)
	if err != nil {
		return "", err
	}
	return k.Seal(Context(tenant, name), plain)
}

// passwordAlphabet leaves out characters that are easy to misread.
const passwordAlphabet = "abcdefghjkmnpqrstuvwxyzABCDEFGHJKLMNPQRSTUVWXYZ23456789"

// Generate returns a fresh value for secret.
func Generate(secret spec.Secret) (string, error) {
	switch secret.Kind {
	case "hex", "base64":
		raw := make([]byte, secret.Bytes)
		if _, err := rand.Read(raw); err != nil {
			return "", err
		}
		if secret.Kind == "hex" {
			return hex.EncodeToString(raw), nil
		}
		return base64.StdEncoding.EncodeToString(raw), nil
	case "password":
		var builder strings.Builder
		limit := big.NewInt(int64(len(passwordAlphabet)))
		for range secret.Bytes {
			index, err := rand.Int(rand.Reader, limit)
			if err != nil {
				return "", err
			}
			builder.WriteByte(passwordAlphabet[index.Int64()])
		}
		return builder.String(), nil
	default:
		return "", fmt.Errorf("unknown secret kind %q", secret.Kind)
	}
}

// Derive returns 32 bytes for purpose, derived from the key in use: every
// node with the same master key derives the same bytes, and they reveal
// nothing of the key or of what it derives for other purposes.
func (k *Key) Derive(purpose string) []byte {
	return derive(k.ring.Load().keys[0].raw, purpose)
}

// DeriveAll is Derive for every key of the ring, the one in use first.
func (k *Key) DeriveAll(purpose string) [][]byte {
	var out [][]byte
	for _, e := range k.ring.Load().keys {
		out = append(out, derive(e.raw, purpose))
	}
	return out
}

func derive(raw []byte, purpose string) []byte {
	derived, err := hkdf.Key(sha256.New, raw, nil, "murder "+purpose, keySize)
	if err != nil {
		panic(err) // only for lengths SHA-256 cannot reach
	}
	return derived
}
