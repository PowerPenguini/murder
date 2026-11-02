package panel

import (
	"bytes"
	"context"
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/sha256"
	"encoding/base64"
	"encoding/binary"
	"encoding/json"
	"io"
	"log/slog"
	"net/http"
	"net/http/cookiejar"
	"net/http/httptest"
	"net/url"
	"strings"
	"testing"

	"github.com/fxamacker/cbor/v2"
)

// softKey is a passkey in software: what an authenticator does, for tests.
type softKey struct {
	key     *ecdsa.PrivateKey
	id      []byte
	handle  []byte
	counter uint32
}

func b64(data []byte) string { return base64.RawURLEncoding.EncodeToString(data) }

func unb64(t *testing.T, value string) []byte {
	t.Helper()
	data, err := base64.RawURLEncoding.DecodeString(value)
	if err != nil {
		t.Fatal(err)
	}
	return data
}

func (k *softKey) authData(rp string, attested bool) []byte {
	sum := sha256.Sum256([]byte(rp))
	flags := byte(0x01 | 0x04) // user present, verified
	if attested {
		flags |= 0x40
	}
	data := append(sum[:], flags)
	data = binary.BigEndian.AppendUint32(data, k.counter)
	if attested {
		x, y := k.key.X.FillBytes(make([]byte, 32)), k.key.Y.FillBytes(make([]byte, 32))
		cose, _ := cbor.Marshal(map[int]any{1: 2, 3: -7, -1: 1, -2: x, -3: y})
		data = append(data, make([]byte, 16)...) // AAGUID
		data = binary.BigEndian.AppendUint16(data, uint16(len(k.id)))
		data = append(data, k.id...)
		data = append(data, cose...)
	}
	return data
}

func clientData(kind, challenge, origin string) []byte {
	data, _ := json.Marshal(map[string]string{"type": kind, "challenge": challenge, "origin": origin})
	return data
}

// create answers a registration's options as a browser would send it.
func (k *softKey) create(t *testing.T, options map[string]any, origin string) map[string]any {
	t.Helper()
	public := options["publicKey"].(map[string]any)
	rp := public["rp"].(map[string]any)["id"].(string)
	k.handle = unb64(t, public["user"].(map[string]any)["id"].(string))
	data := clientData("webauthn.create", public["challenge"].(string), origin)
	attestation, _ := cbor.Marshal(map[string]any{"fmt": "none", "attStmt": map[string]any{}, "authData": k.authData(rp, true)})
	return map[string]any{"id": b64(k.id), "rawId": b64(k.id), "type": "public-key",
		"response": map[string]any{"clientDataJSON": b64(data), "attestationObject": b64(attestation), "transports": []string{"internal"}}}
}

// get answers a sign-in's options.
func (k *softKey) get(t *testing.T, options map[string]any, rp, origin string) map[string]any {
	t.Helper()
	public := options["publicKey"].(map[string]any)
	k.counter++
	data := clientData("webauthn.get", public["challenge"].(string), origin)
	auth := k.authData(rp, false)
	sum := sha256.Sum256(data)
	digest := sha256.Sum256(append(append([]byte(nil), auth...), sum[:]...))
	signature, err := ecdsa.SignASN1(rand.Reader, k.key, digest[:])
	if err != nil {
		t.Fatal(err)
	}
	return map[string]any{"id": b64(k.id), "rawId": b64(k.id), "type": "public-key",
		"response": map[string]any{"clientDataJSON": b64(data), "authenticatorData": b64(auth), "signature": b64(signature), "userHandle": b64(k.handle)}}
}

func newSoftKey() *softKey {
	key, _ := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	id := make([]byte, 16)
	_, _ = rand.Read(id)
	return &softKey{key: key, id: id}
}

type browser struct {
	t      *testing.T
	client *http.Client
	base   string
	origin string
}

func (b *browser) post(path string, body any) (int, map[string]any) {
	b.t.Helper()
	var reader io.Reader
	if body != nil {
		data, _ := json.Marshal(body)
		reader = bytes.NewReader(data)
	}
	request, _ := http.NewRequest(http.MethodPost, b.base+path, reader)
	request.Header.Set("Origin", b.origin)
	request.Header.Set("Content-Type", "application/json")
	response, err := b.client.Do(request)
	if err != nil {
		b.t.Fatal(err)
	}
	defer response.Body.Close()
	var answer map[string]any
	_ = json.NewDecoder(response.Body).Decode(&answer)
	return response.StatusCode, answer
}

func (b *browser) get(path string) (int, string) {
	b.t.Helper()
	response, err := b.client.Get(b.base + path)
	if err != nil {
		b.t.Fatal(err)
	}
	defer response.Body.Close()
	body, _ := io.ReadAll(response.Body)
	return response.StatusCode, string(body)
}

// A person invited adds a passkey and gets recovery codes; signed out,
// they sign in with the passkey alone; a lost passkey is replaced with a
// recovery code, once. Nothing is taken from another origin, nor twice.
func TestSigningInWithPasskeys(t *testing.T) {
	log := slog.New(slog.NewTextHandler(io.Discard, nil))
	people := NewMemoryAuth()
	s, err := New(Demo{Now: demoNow}, log)
	if err != nil {
		t.Fatal(err)
	}
	const origin = "http://localhost"
	if s.Auth, err = NewAuthAt(people, "localhost", origin, log); err != nil {
		t.Fatal(err)
	}
	server := httptest.NewServer(s.Handler())
	defer server.Close()
	jar, _ := cookiejar.New(nil)
	b := &browser{t: t, client: &http.Client{Jar: jar, CheckRedirect: func(*http.Request, []*http.Request) error { return http.ErrUseLastResponse }}, base: server.URL, origin: origin}

	// Not signed in: sent to sign in.
	if status, _ := b.get("/"); status != http.StatusSeeOther {
		t.Fatalf("not signed in: %d", status)
	}
	invite := people.Invite("jan")
	laptop := newSoftKey()
	status, begun := b.post("/auth/enroll/begin", map[string]string{"token": strings.ToLower(invite), "label": "laptop"})
	if status != http.StatusOK || begun["user"] != "jan" {
		t.Fatalf("enroll begin: %d %v", status, begun)
	}
	status, finished := b.post("/auth/enroll/finish", laptop.create(t, begun["options"].(map[string]any), origin))
	if status != http.StatusOK || len(finished["codes"].([]any)) != recoveryCodes {
		t.Fatalf("enroll finish: %d %v", status, finished)
	}
	codes := finished["codes"].([]any)
	if status, body := b.get("/"); status != http.StatusOK || !strings.Contains(body, ">jan<") {
		t.Fatalf("signed in: %d", status)
	}
	// The invitation is used up.
	if status, _ := b.post("/auth/enroll/begin", map[string]string{"token": invite, "label": "again"}); status != http.StatusForbidden {
		t.Fatalf("an invitation used twice: %d", status)
	}

	// A change asked from another origin is refused.
	b.origin = "https://evil.example"
	if status, _ := b.post("/auth/logout", nil); status != http.StatusForbidden {
		t.Fatalf("a sign-out from elsewhere: %d", status)
	}
	b.origin = origin
	b.post("/auth/logout", nil)
	if status, _ := b.get("/"); status != http.StatusSeeOther {
		t.Fatal("signed out, still signed in")
	}

	// Signing in: the passkey alone says who.
	_, options := b.post("/auth/login/begin", nil)
	serverURL, _ := url.Parse(server.URL)
	var captured *http.Cookie
	for _, cookie := range jar.Cookies(serverURL) {
		if cookie.Name == ceremonyCookie {
			captured = cookie
		}
	}
	answer := laptop.get(t, options, "localhost", origin)
	if status, signed := b.post("/auth/login/finish", answer); status != http.StatusOK || signed["user"] != "jan" {
		t.Fatalf("sign in: %d %v", status, signed)
	}
	// The same answer again, with the ceremony's cookie as captured: the
	// state has its challenge answered already.
	replay := &browser{t: t, client: &http.Client{}, base: server.URL, origin: origin}
	replayJar, _ := cookiejar.New(nil)
	replayJar.SetCookies(serverURL, []*http.Cookie{captured})
	replay.client.Jar = replayJar
	if status, _ := replay.post("/auth/login/finish", answer); status != http.StatusUnauthorized {
		t.Fatalf("a sign-in replayed: %d", status)
	}
	b.post("/auth/logout", nil)
	// A copied passkey, its counter behind, is refused.
	_, options = b.post("/auth/login/begin", nil)
	laptop.counter = 0
	if status, _ := b.post("/auth/login/finish", laptop.get(t, options, "localhost", origin)); status != http.StatusUnauthorized {
		t.Fatalf("a passkey whose counter went back: %d", status)
	}
	// A passkey that is not jan's signs no one in.
	_, options = b.post("/auth/login/begin", nil)
	stranger := newSoftKey()
	stranger.handle = laptop.handle
	if status, _ := b.post("/auth/login/finish", stranger.get(t, options, "localhost", origin)); status != http.StatusUnauthorized {
		t.Fatalf("a stranger's passkey: %d", status)
	}

	// The laptop lost: a recovery code adds the phone's passkey.
	phone := newSoftKey()
	status, begun = b.post("/auth/enroll/begin", map[string]string{"token": codes[0].(string), "label": "phone"})
	if status != http.StatusOK {
		t.Fatalf("recover: %d %v", status, begun)
	}
	if status, finished := b.post("/auth/enroll/finish", phone.create(t, begun["options"].(map[string]any), origin)); status != http.StatusOK || finished["codes"] != nil {
		t.Fatalf("recovered: %d %v", status, finished)
	}
	if status, _ := b.get("/"); status != http.StatusOK {
		t.Fatal("recovered, not signed in")
	}
	if u, _ := people.PanelUser(context.Background(), "jan"); len(u.Passkeys) != 2 || u.Recovery != recoveryCodes-1 {
		t.Fatalf("jan: %d passkeys, %d codes", len(u.Passkeys), u.Recovery)
	}
}

func TestTokens(t *testing.T) {
	token := Token(10)
	if len(strings.ReplaceAll(token, "-", "")) != 16 || TokenHash(token) != TokenHash(strings.ToLower(" "+token+" ")) || TokenHash(token) == TokenHash(Token(10)) {
		t.Fatalf("token %q", token)
	}
}
