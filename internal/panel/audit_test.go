package panel

import (
	"bufio"
	"context"
	"io"
	"log/slog"
	"net"
	"net/http"
	"net/http/cookiejar"
	"net/http/httptest"
	"net/netip"
	"net/url"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/PowerPenguini/murder/internal/api"
	"github.com/PowerPenguini/murder/internal/spec"
	"github.com/PowerPenguini/murder/internal/store"
)

// sqliteAuth is the panel's agent over a real store, as the agent's API
// serves it.
type sqliteAuth struct{ st *store.SQLite }

func (a sqliteAuth) PanelUser(_ context.Context, n string) (store.PanelUser, error) {
	return a.st.PanelUser(n)
}
func (a sqliteAuth) PanelUserByHandle(_ context.Context, n string) (store.PanelUser, error) {
	return a.st.PanelUserByHandle(n)
}
func (a sqliteAuth) Enrollment(_ context.Context, n string) (store.Enrollment, error) {
	return a.st.Enrollment(n)
}
func (a sqliteAuth) AddPasskey(_ context.Context, r api.PasskeyRequest) error {
	return a.st.AddPasskey(r.Hash, r.Passkey)
}
func (a sqliteAuth) UsePasskey(_ context.Context, id, previous, data string) error {
	return a.st.UsePasskeyIf(id, previous, data)
}
func (a sqliteAuth) SetRecoveryCodes(_ context.Context, n string, h []string) error {
	return a.st.SetRecoveryCodes(n, h, "test")
}
func (a sqliteAuth) StartPanelSession(_ context.Context, r api.SessionRequest) error {
	return a.st.StartPanelSession(r)
}
func (a sqliteAuth) PanelSession(_ context.Context, h string, _ bool) (store.PanelSession, error) {
	return a.st.PanelSession(h)
}
func (a sqliteAuth) EndPanelSession(_ context.Context, h string) error {
	return a.st.EndPanelSession(h)
}
func (a sqliteAuth) PanelSecret(_ context.Context, n string) (string, error) {
	return a.st.PanelSecret(n)
}

func testStore(t *testing.T) *store.SQLite {
	t.Helper()
	path := filepath.Join(t.TempDir(), "murder.db")
	if err := store.Init(path, "node"); err != nil {
		t.Fatal(err)
	}
	st, err := store.Open(path, store.Options{NetworkPool: netip.MustParsePrefix("10.240.0.0/16"), Sealer: func(string, string, spec.Secret) (string, error) { return "sealed", nil }})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = st.Close() })
	return st
}

// signInServer serves a panel signing people in through agent.
func signInServer(t *testing.T, agent AuthAgent) (*browser, *cookiejar.Jar, *httptest.Server) {
	t.Helper()
	log := slog.New(slog.NewTextHandler(io.Discard, nil))
	s, err := New(Demo{Now: demoNow}, log)
	if err != nil {
		t.Fatal(err)
	}
	const origin = "http://localhost"
	if s.Auth, err = NewAuthAt(agent, "localhost", origin, log); err != nil {
		t.Fatal(err)
	}
	server := httptest.NewServer(s.Handler())
	t.Cleanup(server.Close)
	jar, _ := cookiejar.New(nil)
	return &browser{t: t, client: &http.Client{Jar: jar, CheckRedirect: func(*http.Request, []*http.Request) error { return http.ErrUseLastResponse }}, base: server.URL, origin: origin}, jar, server
}

// enroll adds key with the invitation token.
func enroll(t *testing.T, b *browser, key *softKey, token string) {
	t.Helper()
	status, begun := b.post("/auth/enroll/begin", map[string]string{"token": token, "label": "key"})
	if status != 200 {
		t.Fatal(status, begun)
	}
	if status, answer := b.post("/auth/enroll/finish", key.create(t, begun["options"].(map[string]any), b.origin)); status != 200 {
		t.Fatal(status, answer)
	}
}

// A sign-in, signed out of, cannot be sent again: not even by an
// authenticator whose counter stays at zero.
func TestASignInIsNotTakenTwiceAfterSigningOut(t *testing.T) {
	st := testStore(t)
	token := Token(32)
	if _, err := st.InvitePanelUser("jan", TokenHash(token), time.Now().Add(time.Hour), "test"); err != nil {
		t.Fatal(err)
	}
	replayAfterSigningOut(t, sqliteAuth{st}, token)
}

// The demo's people in memory keep the mark too.
func TestASignInIsNotTakenTwiceAfterSigningOutOfMemory(t *testing.T) {
	people := NewMemoryAuth()
	replayAfterSigningOut(t, people, people.Invite("jan"))
}

func replayAfterSigningOut(t *testing.T, agent AuthAgent, token string) {
	t.Helper()
	b, jar, server := signInServer(t, agent)
	key := newSoftKey()
	enroll(t, b, key, token)
	b.post("/auth/logout", nil)
	_, options := b.post("/auth/login/begin", nil)
	u, _ := url.Parse(server.URL)
	var ceremony *http.Cookie
	for _, c := range jar.Cookies(u) {
		if c.Name == ceremonyCookie {
			ceremony = c
		}
	}
	key.counter = ^uint32(0) // the next is zero: a key without a counter
	answer := key.get(t, options, "localhost", b.origin)
	if status, result := b.post("/auth/login/finish", answer); status != 200 {
		t.Fatal(status, result)
	}
	b.post("/auth/logout", nil)
	jar.SetCookies(u, []*http.Cookie{ceremony})
	if status, _ := b.post("/auth/login/finish", answer); status == 200 {
		t.Fatal("the same sign-in was taken again after signing out")
	}
	if status, _ := b.get("/"); status == 200 {
		t.Fatal("signed in by a replay")
	}
}

// usedMeanwhile is an agent with which a sign-in with a passkey, recording
// a further counter, comes between the passkey's being added and its
// enrollment's recording its use.
type usedMeanwhile struct{ sqliteAuth }

func (a usedMeanwhile) AddPasskey(ctx context.Context, r api.PasskeyRequest) error {
	if err := a.sqliteAuth.AddPasskey(ctx, r); err != nil {
		return err
	}
	return a.st.UsePasskeyIf(r.Passkey.ID, r.Passkey.Data, "signed in since")
}

// Enrolling records the new passkey's use only over the credential it
// added: a sign-in meanwhile keeps its counter.
func TestEnrollingDoesNotTakeAPasskeysCounterBack(t *testing.T) {
	st := testStore(t)
	token := Token(32)
	if _, err := st.InvitePanelUser("jan", TokenHash(token), time.Now().Add(time.Hour), "test"); err != nil {
		t.Fatal(err)
	}
	b, _, _ := signInServer(t, usedMeanwhile{sqliteAuth{st}})
	enroll(t, b, newSoftKey(), token)
	u, err := st.PanelUser("jan")
	if err != nil || len(u.Passkeys) != 1 {
		t.Fatal(u, err)
	}
	if u.Passkeys[0].Data != "signed in since" {
		t.Fatalf("the enrollment wrote its credential back over a later sign-in's: %.60s", u.Passkeys[0].Data)
	}
}

// A request refused before its body is read (here as from elsewhere, or
// for another name) is still held to its body's time: the server drains
// what is left of the body after answering, and a client that never sends
// it holds the connection only that long.
func TestARefusedRequestsWithheldBodyIsHeldToItsTime(t *testing.T) {
	log := slog.New(slog.NewTextHandler(io.Discard, nil))
	s, err := New(Demo{Now: demoNow}, log)
	if err != nil {
		t.Fatal(err)
	}
	if s.Auth, err = NewAuthAt(NewMemoryAuth(), "localhost", "http://localhost", log); err != nil {
		t.Fatal(err)
	}
	s.bodyTime = 200 * time.Millisecond
	server := httptest.NewServer(s.Handler())
	defer server.Close()
	for _, head := range []string{
		"POST /auth/login/finish HTTP/1.1\r\nHost: localhost\r\nOrigin: http://elsewhere.example\r\n",
		"POST /tenants HTTP/1.1\r\nHost: elsewhere.example\r\nOrigin: http://elsewhere.example\r\n",
	} {
		conn, err := net.Dial("tcp", server.Listener.Addr().String())
		if err != nil {
			t.Fatal(err)
		}
		defer conn.Close()
		_ = conn.SetDeadline(time.Now().Add(5 * time.Second))
		if _, err := io.WriteString(conn, head+"Content-Type: application/json\r\nContent-Length: 1000\r\n\r\n"); err != nil {
			t.Fatal(err)
		}
		response, err := http.ReadResponse(bufio.NewReader(conn), nil)
		if err != nil {
			t.Fatalf("no answer while the body was withheld: %v", err)
		}
		if response.StatusCode < 400 {
			t.Fatalf("taken: %d", response.StatusCode)
		}
	}
}

// Once a body is read to its end its time no longer runs, and a request
// without a body never had one: neither's handler is cut short by it.
func TestARequestsTimeEndsWithItsBody(t *testing.T) {
	s := &Server{bodyTime: 100 * time.Millisecond}
	handler := s.bounded(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if _, err := io.ReadAll(r.Body); err != nil {
			http.Error(w, err.Error(), http.StatusBadRequest)
			return
		}
		select {
		case <-r.Context().Done():
			http.Error(w, "cut short", http.StatusServiceUnavailable)
		case <-time.After(400 * time.Millisecond):
			w.WriteHeader(http.StatusNoContent)
		}
	}))
	server := httptest.NewServer(handler)
	defer server.Close()
	for _, method := range []string{"GET", "POST"} {
		var body io.Reader
		if method == "POST" {
			body = strings.NewReader("name=acme")
		}
		r, _ := http.NewRequest(method, server.URL+"/tenants", body)
		response, err := server.Client().Do(r)
		if err != nil {
			t.Fatal(err)
		}
		response.Body.Close()
		if response.StatusCode != http.StatusNoContent {
			t.Errorf("%s: %d", method, response.StatusCode)
		}
	}
}

type countingReader struct {
	io.Reader
	n int
}

func (c *countingReader) Read(p []byte) (int, error) {
	n, err := c.Reader.Read(p)
	c.n += n
	return n, err
}

// Before anyone signs in, the panel reads a small credential at most.
func TestASignInsBodyIsBounded(t *testing.T) {
	log := slog.New(slog.NewTextHandler(io.Discard, nil))
	s, err := New(Demo{Now: demoNow}, log)
	if err != nil {
		t.Fatal(err)
	}
	if s.Auth, err = NewAuthAt(NewMemoryAuth(), "localhost", "http://localhost", log); err != nil {
		t.Fatal(err)
	}
	handler := s.Handler()
	begin := httptest.NewRequest("POST", "http://localhost/auth/login/begin", nil)
	begin.Header.Set("Origin", "http://localhost")
	begun := httptest.NewRecorder()
	handler.ServeHTTP(begun, begin)
	body := &countingReader{Reader: strings.NewReader(`{"padding":"` + strings.Repeat("x", 2<<20) + `"}`)}
	r := httptest.NewRequest("POST", "http://localhost/auth/login/finish", body)
	r.Header.Set("Origin", "http://localhost")
	for _, c := range begun.Result().Cookies() {
		r.AddCookie(c)
	}
	w := httptest.NewRecorder()
	handler.ServeHTTP(w, r)
	if body.n == 0 {
		t.Fatalf("the sign-in never read its body: %d %s", w.Code, w.Body)
	}
	if body.n > authBody+64<<10 || w.Code == 200 {
		t.Fatalf("read %d bytes, answered %d", body.n, w.Code)
	}
}
