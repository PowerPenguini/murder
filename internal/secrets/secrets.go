// Package secrets generates tenant secrets and seals them with the node's
// master key, so the state file never holds a secret in the clear.
package secrets

import (
	"crypto/aes"
	"crypto/cipher"
	"crypto/rand"
	"encoding/base64"
	"encoding/hex"
	"errors"
	"fmt"
	"math/big"
	"os"
	"path/filepath"
	"strings"

	"github.com/PowerPenguini/corral/internal/spec"
)

const keySize = 32

// Key seals and opens secrets with AES-256-GCM.
type Key struct {
	aead cipher.AEAD
}

// LoadKey reads a base64 master key from path. The file must not be readable
// by group or others.
func LoadKey(path string) (*Key, error) {
	info, err := os.Stat(path)
	if err != nil {
		return nil, fmt.Errorf("read master key: %w", err)
	}
	if info.Mode().Perm()&0o077 != 0 {
		return nil, fmt.Errorf("master key %s must not be accessible to group or others (mode %v)", path, info.Mode().Perm())
	}
	encoded, err := os.ReadFile(path)
	if err != nil {
		return nil, fmt.Errorf("read master key: %w", err)
	}
	raw, err := base64.StdEncoding.DecodeString(strings.TrimSpace(string(encoded)))
	if err != nil || len(raw) != keySize {
		return nil, fmt.Errorf("master key %s must be %d base64-encoded bytes", path, keySize)
	}
	return NewKey(raw)
}

// NewKey builds a Key from raw key bytes.
func NewKey(raw []byte) (*Key, error) {
	if len(raw) != keySize {
		return nil, fmt.Errorf("master key must be %d bytes", keySize)
	}
	block, err := aes.NewCipher(raw)
	if err != nil {
		return nil, err
	}
	aead, err := cipher.NewGCM(block)
	if err != nil {
		return nil, err
	}
	return &Key{aead: aead}, nil
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

// Seal encrypts plaintext bound to context (tenant and secret name), so a
// sealed value cannot be moved to another tenant or key unnoticed.
func (k *Key) Seal(context, plaintext string) (string, error) {
	nonce := make([]byte, k.aead.NonceSize())
	if _, err := rand.Read(nonce); err != nil {
		return "", err
	}
	sealed := k.aead.Seal(nonce, nonce, []byte(plaintext), []byte(context))
	return base64.StdEncoding.EncodeToString(sealed), nil
}

// Open decrypts a value sealed with the same key and context.
func (k *Key) Open(context, sealed string) (string, error) {
	raw, err := base64.StdEncoding.DecodeString(sealed)
	if err != nil || len(raw) < k.aead.NonceSize() {
		return "", errors.New("sealed secret is malformed")
	}
	nonce, ciphertext := raw[:k.aead.NonceSize()], raw[k.aead.NonceSize():]
	plaintext, err := k.aead.Open(nil, nonce, ciphertext, []byte(context))
	if err != nil {
		return "", errors.New("sealed secret does not open with this master key")
	}
	return string(plaintext), nil
}

// Context names the sealing context of one tenant secret.
func Context(tenant, name string) string { return tenant + "/" + name }

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
