package secrets

import (
	"encoding/base64"
	"encoding/hex"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/PowerPenguini/corral/internal/spec"
)

func newTestKey(t *testing.T) (*Key, string) {
	t.Helper()
	path := filepath.Join(t.TempDir(), "keys", "master.key")
	if err := CreateKey(path); err != nil {
		t.Fatal(err)
	}
	key, err := LoadKey(path)
	if err != nil {
		t.Fatal(err)
	}
	return key, path
}

func TestSealOpenRoundTripBoundToContext(t *testing.T) {
	key, _ := newTestKey(t)
	sealed, err := key.Seal(Context("acme", "TOKEN"), "s3cret")
	if err != nil {
		t.Fatal(err)
	}
	if strings.Contains(sealed, "s3cret") {
		t.Fatal("sealed value contains the plaintext")
	}
	if plain, err := key.Open(Context("acme", "TOKEN"), sealed); err != nil || plain != "s3cret" {
		t.Fatalf("Open = %q, %v", plain, err)
	}
	if _, err := key.Open(Context("other", "TOKEN"), sealed); err == nil {
		t.Fatal("a value sealed for one tenant opened for another")
	}
	other, _ := newTestKey(t)
	if _, err := other.Open(Context("acme", "TOKEN"), sealed); err == nil {
		t.Fatal("a value opened with a different master key")
	}
	again, _ := key.Seal(Context("acme", "TOKEN"), "s3cret")
	if again == sealed {
		t.Fatal("sealing twice produced the same ciphertext")
	}
}

func TestKeyFileRules(t *testing.T) {
	_, path := newTestKey(t)
	if err := CreateKey(path); err == nil {
		t.Fatal("CreateKey overwrote an existing key")
	}
	if err := os.Chmod(path, 0o640); err != nil {
		t.Fatal(err)
	}
	if _, err := LoadKey(path); err == nil {
		t.Fatal("LoadKey accepted a group-readable key")
	}
	short := filepath.Join(t.TempDir(), "short.key")
	if err := os.WriteFile(short, []byte(base64.StdEncoding.EncodeToString([]byte("too short"))), 0o600); err != nil {
		t.Fatal(err)
	}
	if _, err := LoadKey(short); err == nil {
		t.Fatal("LoadKey accepted a short key")
	}
}

func TestGenerate(t *testing.T) {
	hexValue, err := Generate(spec.Secret{Kind: "hex", Bytes: 32})
	if err != nil {
		t.Fatal(err)
	}
	if raw, err := hex.DecodeString(hexValue); err != nil || len(raw) != 32 {
		t.Fatalf("hex secret %q decodes to %d bytes, %v", hexValue, len(raw), err)
	}
	b64, err := Generate(spec.Secret{Kind: "base64", Bytes: 32})
	if err != nil {
		t.Fatal(err)
	}
	if raw, err := base64.StdEncoding.DecodeString(b64); err != nil || len(raw) != 32 {
		t.Fatalf("base64 secret decodes to %d bytes, %v", len(raw), err)
	}
	password, err := Generate(spec.Secret{Kind: "password", Bytes: 24})
	if err != nil {
		t.Fatal(err)
	}
	if len(password) != 24 || strings.Trim(password, passwordAlphabet) != "" {
		t.Fatalf("password %q is not 24 characters from the alphabet", password)
	}
	second, _ := Generate(spec.Secret{Kind: "password", Bytes: 24})
	if second == password {
		t.Fatal("two generated passwords are equal")
	}
	if _, err := Generate(spec.Secret{Kind: "rot13", Bytes: 24}); err == nil {
		t.Fatal("unknown kind accepted")
	}
}
