package cluster

import (
	"crypto/ed25519"
	"crypto/rand"
	"crypto/tls"
	"crypto/x509"
	"encoding/base64"
	"encoding/pem"
	"errors"
	"fmt"
	"io/fs"
	"os"
	"path/filepath"
)

// A node proves who it is with an identity key of its own, which never
// leaves it: its cluster certificate is issued over the key, and the
// cluster's state pins the key's public half to the node's name. The
// master key, which every node shares, only proves that a certificate is
// the cluster's: whoever holds it can name any node in one, but cannot
// present the key the state holds for that node.

// IdentityFile is the name of the file, in a node's cluster directory, that
// holds its identity key.
const IdentityFile = "node.key"

// IdentityPath is where a node whose cluster directory is dir keeps its
// identity key.
func IdentityPath(dir string) string { return filepath.Join(dir, IdentityFile) }

// NewIdentity makes an identity key.
func NewIdentity() (ed25519.PrivateKey, error) {
	_, key, err := ed25519.GenerateKey(rand.Reader)
	return key, err
}

// EncodeIdentity is an identity key as its file holds it: PKCS #8, in PEM.
func EncodeIdentity(key ed25519.PrivateKey) ([]byte, error) {
	der, err := x509.MarshalPKCS8PrivateKey(key)
	if err != nil {
		return nil, err
	}
	return pem.EncodeToMemory(&pem.Block{Type: "PRIVATE KEY", Bytes: der}), nil
}

// LoadIdentity reads the identity key in dir, and makes one the first time.
func LoadIdentity(dir string) (ed25519.PrivateKey, error) {
	path := IdentityPath(dir)
	data, err := os.ReadFile(path)
	if errors.Is(err, fs.ErrNotExist) {
		return makeIdentity(path)
	}
	if err != nil {
		return nil, fmt.Errorf("read the node's identity key: %w", err)
	}
	block, _ := pem.Decode(data)
	if block == nil || block.Type != "PRIVATE KEY" {
		return nil, fmt.Errorf("%s holds no identity key: move it aside, and the node makes a new one (it must then join again)", path)
	}
	parsed, err := x509.ParsePKCS8PrivateKey(block.Bytes)
	key, ok := parsed.(ed25519.PrivateKey)
	if err != nil || !ok {
		return nil, fmt.Errorf("%s holds no ed25519 identity key: move it aside, and the node makes a new one (it must then join again)", path)
	}
	return key, nil
}

// ReadIdentity reads the identity key in dir, making none: for showing a
// node's key, the agent made it.
func ReadIdentity(dir string) (ed25519.PrivateKey, error) {
	if _, err := os.Stat(IdentityPath(dir)); err != nil {
		return nil, fmt.Errorf("this node has no identity key yet: start its agent, which makes one (%w)", err)
	}
	return LoadIdentity(dir)
}

// makeIdentity writes a new identity key to path, whole or not at all: a
// key another start wrote meanwhile is the one kept.
func makeIdentity(path string) (ed25519.PrivateKey, error) {
	key, err := NewIdentity()
	if err != nil {
		return nil, err
	}
	data, err := EncodeIdentity(key)
	if err != nil {
		return nil, err
	}
	if err := os.MkdirAll(filepath.Dir(path), 0o700); err != nil {
		return nil, err
	}
	temp, err := os.CreateTemp(filepath.Dir(path), IdentityFile+".new-*")
	if err != nil {
		return nil, err
	}
	defer os.Remove(temp.Name())
	_, err = temp.Write(data)
	if err = errors.Join(err, temp.Sync(), temp.Close()); err != nil {
		return nil, fmt.Errorf("write the node's identity key: %w", err)
	}
	// A link fails if the file is there: it never replaces a key.
	if err := os.Link(temp.Name(), path); err != nil {
		if errors.Is(err, fs.ErrExist) {
			return LoadIdentity(filepath.Dir(path))
		}
		return nil, fmt.Errorf("write the node's identity key: %w", err)
	}
	if dir, err := os.Open(filepath.Dir(path)); err == nil {
		_ = dir.Sync()
		_ = dir.Close()
	}
	return key, nil
}

// rebuildFile, in a node's cluster directory, notes a bootstrap that
// rebuilds a cluster from a restored state: it holds the name of the
// cluster the state belonged to. It is the node's own, outside the
// database, so no batch reads it.
const rebuildFile = "rebuilding"

// rebuilding is the name a rebuild leaves, while one is under way in dir;
// empty otherwise.
func rebuilding(dir string) (string, error) {
	data, err := os.ReadFile(filepath.Join(dir, rebuildFile))
	if errors.Is(err, fs.ErrNotExist) {
		return "", nil
	}
	return string(data), err
}

// beginRebuild notes, whole and on disk before anything else changes, that
// a bootstrap in dir rebuilds a cluster from the state of cluster name.
func beginRebuild(dir, name string) error {
	temp, err := os.CreateTemp(dir, rebuildFile+".new-*")
	if err != nil {
		return err
	}
	defer os.Remove(temp.Name())
	_, err = temp.WriteString(name)
	if err = errors.Join(err, temp.Sync(), temp.Close()); err != nil {
		return err
	}
	if err := os.Rename(temp.Name(), filepath.Join(dir, rebuildFile)); err != nil {
		return err
	}
	return syncDir(dir)
}

// endRebuild notes that the rebuild in dir is done.
func endRebuild(dir string) error {
	if err := os.Remove(filepath.Join(dir, rebuildFile)); err != nil && !errors.Is(err, fs.ErrNotExist) {
		return err
	}
	return syncDir(dir)
}

func syncDir(dir string) error {
	handle, err := os.Open(dir)
	if err != nil {
		return err
	}
	defer handle.Close()
	return handle.Sync()
}

// EncodeKey is a public identity key as the state holds it.
func EncodeKey(key ed25519.PublicKey) string {
	if len(key) != ed25519.PublicKeySize {
		return ""
	}
	return base64.StdEncoding.EncodeToString(key)
}

// peerIdentity is the node name and identity key in the certificate a
// peer presented, checked against the cluster's authorities. A node of a
// release before identity keys presents another kind of key: none is
// returned for it.
func peerIdentity(state *tls.ConnectionState) (string, ed25519.PublicKey, error) {
	if state == nil || len(state.VerifiedChains) == 0 || len(state.VerifiedChains[0]) == 0 {
		return "", nil, errors.New("no verified cluster certificate")
	}
	leaf := state.VerifiedChains[0][0]
	key, _ := leaf.PublicKey.(ed25519.PublicKey)
	return leaf.Subject.CommonName, key, nil
}

// peerName is the node name in the certificate a peer presented.
func peerName(state *tls.ConnectionState) (string, error) {
	name, _, err := peerIdentity(state)
	return name, err
}
