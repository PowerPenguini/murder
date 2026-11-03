package panel

import (
	"context"
	"io"
	"log/slog"
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

// A sign-in, signed out of, cannot be sent again: not even by an
// authenticator whose counter stays at zero.
func TestASignInIsNotTakenTwiceAfterSigningOut(t *testing.T) {
	path := filepath.Join(t.TempDir(), "murder.db")
	if err := store.Init(path, "node"); err != nil {
		t.Fatal(err)
	}
	st, err := store.Open(path, store.Options{NetworkPool: netip.MustParsePrefix("10.240.0.0/16"), Sealer: func(string, string, spec.Secret) (string, error) { return "sealed", nil }})
	if err != nil {
		t.Fatal(err)
	}
	defer st.Close()
	log := slog.New(slog.NewTextHandler(io.Discard, nil))
	s, err := New(Demo{Now: demoNow}, log)
	if err != nil {
		t.Fatal(err)
	}
	const origin = "http://localhost"
	if s.Auth, err = NewAuthAt(sqliteAuth{st}, "localhost", origin, log); err != nil {
		t.Fatal(err)
	}
	server := httptest.NewServer(s.Handler())
	defer server.Close()
	jar, _ := cookiejar.New(nil)
	b := &browser{t: t, client: &http.Client{Jar: jar, CheckRedirect: func(*http.Request, []*http.Request) error { return http.ErrUseLastResponse }}, base: server.URL, origin: origin}
	token := Token(32)
	if _, err := st.InvitePanelUser("jan", TokenHash(token), time.Now().Add(time.Hour), "test"); err != nil {
		t.Fatal(err)
	}
	key := newSoftKey()
	status, begun := b.post("/auth/enroll/begin", map[string]string{"token": token, "label": "key"})
	if status != 200 {
		t.Fatal(status, begun)
	}
	if status, answer := b.post("/auth/enroll/finish", key.create(t, begun["options"].(map[string]any), origin)); status != 200 {
		t.Fatal(status, answer)
	}
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
	answer := key.get(t, options, "localhost", origin)
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
