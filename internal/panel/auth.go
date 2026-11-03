package panel

import (
	"context"
	"crypto/hmac"
	"crypto/rand"
	"crypto/sha256"
	"encoding/base32"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"net/http"
	"slices"
	"strings"
	"sync"
	"time"

	"github.com/PowerPenguini/murder/internal/api"
	"github.com/PowerPenguini/murder/internal/store"
	"github.com/go-webauthn/webauthn/protocol"
	"github.com/go-webauthn/webauthn/webauthn"
)

// AuthAgent is what signing in asks of the agent.
type AuthAgent interface {
	PanelUser(ctx context.Context, name string) (store.PanelUser, error)
	PanelUserByHandle(ctx context.Context, handle string) (store.PanelUser, error)
	Enrollment(ctx context.Context, hash string) (store.Enrollment, error)
	AddPasskey(ctx context.Context, request api.PasskeyRequest) error
	// UsePasskey records a sign-in, refused if the passkey was used since
	// its credential read previous (empty: whatever it is).
	UsePasskey(ctx context.Context, id, previous, data string) error
	SetRecoveryCodes(ctx context.Context, name string, hashes []string) error
	StartPanelSession(ctx context.Context, request api.SessionRequest) error
	PanelSession(ctx context.Context, hash string, fresh bool) (store.PanelSession, error)
	EndPanelSession(ctx context.Context, hash string) error
	PanelSecret(ctx context.Context, name string) (string, error)
}

// The cookies are __Host-: only the panel's own name sets them, not a
// tenant's page under the same base domain.
const (
	sessionCookie  = "__Host-murder_session"
	ceremonyCookie = "__Host-murder_ceremony"
	// SessionLength is how long a sign-in lasts.
	SessionLength  = 12 * time.Hour
	ceremonyLength = 5 * time.Minute
	// InviteLength is how long an invitation may be used.
	InviteLength = 24 * time.Hour
	// recoveryCodes is how many recovery codes a person gets.
	recoveryCodes = 10
)

// Auth signs people in with passkeys: the panel is the WebAuthn relying
// party, as its own name, over HTTPS. Ceremonies in flight are kept in a
// cookie signed with a secret every node shares, so any node may finish
// what another began.
type Auth struct {
	agent AuthAgent
	name  string
	// origin is where the panel's pages are: https://name; origins are
	// those taken too, as murder admin connect's port.
	origin  string
	origins []string
	web     *webauthn.WebAuthn
	log     *slog.Logger
	now     func() time.Time

	mu     sync.Mutex
	secret []byte
}

// ConnectPort is where murder admin connect serves the panel by default.
const ConnectPort = 8443

// NewAuth signs people in to the panel served as name, over HTTPS, and
// through murder admin connect's local port.
func NewAuth(agent AuthAgent, name string, log *slog.Logger) (*Auth, error) {
	return NewAuthAt(agent, name, "https://"+name, log, fmt.Sprintf("https://%s:%d", name, ConnectPort))
}

// NewAuthAt is NewAuth with the pages at origin, and others, such as
// http://localhost:7480 for the demo: browsers allow passkeys there too.
func NewAuthAt(agent AuthAgent, name, origin string, log *slog.Logger, others ...string) (*Auth, error) {
	origins := append([]string{origin}, others...)
	web, err := webauthn.New(&webauthn.Config{RPID: name, RPDisplayName: "Murder", RPOrigins: origins})
	if err != nil {
		return nil, err
	}
	return &Auth{agent: agent, name: name, origin: origin, origins: origins, web: web, log: log, now: time.Now}, nil
}

// person is a panel user as WebAuthn sees them.
type person struct {
	store.PanelUser
	handle      []byte
	credentials []webauthn.Credential
}

func newPerson(u store.PanelUser) (*person, error) {
	handle, err := base64.RawURLEncoding.DecodeString(u.Handle)
	if err != nil {
		return nil, err
	}
	p := &person{PanelUser: u, handle: handle}
	for _, key := range u.Passkeys {
		var credential webauthn.Credential
		if err := json.Unmarshal([]byte(key.Data), &credential); err != nil {
			return nil, fmt.Errorf("passkey %s: %w", key.Label, err)
		}
		p.credentials = append(p.credentials, credential)
	}
	return p, nil
}

func (p *person) WebAuthnID() []byte                         { return p.handle }
func (p *person) WebAuthnName() string                       { return p.Name }
func (p *person) WebAuthnDisplayName() string                { return p.Name }
func (p *person) WebAuthnCredentials() []webauthn.Credential { return p.credentials }

// Token is a random one-time token: an invitation or a recovery code, in
// base32, grouped by four for reading.
func Token(bytes int) string {
	raw := make([]byte, bytes)
	if _, err := rand.Read(raw); err != nil {
		panic(err)
	}
	text := base32.StdEncoding.WithPadding(base32.NoPadding).EncodeToString(raw)
	var groups []string
	for len(text) > 4 {
		groups, text = append(groups, text[:4]), text[4:]
	}
	return strings.Join(append(groups, text), "-")
}

// TokenHash is what the state keeps of a token: the SHA-256 of it as
// typed, any case, dashes and spaces left out.
func TokenHash(token string) string {
	normal := strings.Map(func(r rune) rune {
		if r == '-' || r == ' ' || r == '\t' || r == '\n' {
			return -1
		}
		return r
	}, strings.ToUpper(token))
	sum := sha256.Sum256([]byte(normal))
	return hex.EncodeToString(sum[:])
}

// ceremony is a sign-in or a passkey being added, as its cookie keeps it.
type ceremony struct {
	Kind    string               `json:"kind"`
	Session webauthn.SessionData `json:"session"`
	// For a passkey being added: the token's hash, the person and the
	// passkey's label.
	Hash  string `json:"hash,omitempty"`
	User  string `json:"user,omitempty"`
	Label string `json:"label,omitempty"`
	// Enrollment is the kind of token used: an invitation or a code.
	Enrollment string    `json:"enrollment,omitempty"`
	Expires    time.Time `json:"expires"`
}

func (a *Auth) key(ctx context.Context) ([]byte, error) {
	a.mu.Lock()
	defer a.mu.Unlock()
	if a.secret != nil {
		return a.secret, nil
	}
	value, err := a.agent.PanelSecret(ctx, "ceremonies")
	if err != nil {
		return nil, err
	}
	secret, err := hex.DecodeString(value)
	if err != nil || len(secret) < 32 {
		return nil, errors.New("the panel's shared secret is malformed")
	}
	a.secret = secret
	return secret, nil
}

func (a *Auth) seal(ctx context.Context, c ceremony) (string, error) {
	key, err := a.key(ctx)
	if err != nil {
		return "", err
	}
	data, err := json.Marshal(c)
	if err != nil {
		return "", err
	}
	mac := hmac.New(sha256.New, key)
	mac.Write(data)
	return base64.RawURLEncoding.EncodeToString(data) + "." + base64.RawURLEncoding.EncodeToString(mac.Sum(nil)), nil
}

func (a *Auth) open(ctx context.Context, r *http.Request, kind string) (ceremony, error) {
	var c ceremony
	cookie, err := r.Cookie(ceremonyCookie)
	if err != nil {
		return c, errors.New("no ceremony under way: begin again")
	}
	data64, sum64, ok := strings.Cut(cookie.Value, ".")
	data, dataErr := base64.RawURLEncoding.DecodeString(data64)
	sum, sumErr := base64.RawURLEncoding.DecodeString(sum64)
	if !ok || dataErr != nil || sumErr != nil {
		return c, errors.New("a malformed ceremony")
	}
	key, err := a.key(ctx)
	if err != nil {
		return c, err
	}
	mac := hmac.New(sha256.New, key)
	mac.Write(data)
	if !hmac.Equal(sum, mac.Sum(nil)) {
		return c, errors.New("a ceremony not signed by the panel")
	}
	if err := json.Unmarshal(data, &c); err != nil {
		return c, err
	}
	if c.Kind != kind || a.now().After(c.Expires) {
		return c, errors.New("the ceremony expired: begin again")
	}
	return c, nil
}

func (a *Auth) setCookie(w http.ResponseWriter, name, value string, maxAge time.Duration) {
	http.SetCookie(w, &http.Cookie{Name: name, Value: value, Path: "/", MaxAge: int(maxAge.Seconds()),
		Secure: true, HttpOnly: true, SameSite: http.SameSiteStrictMode})
}

// User is the person r is signed in as, if any.
func (a *Auth) User(r *http.Request) (string, bool) {
	cookie, err := r.Cookie(sessionCookie)
	if err != nil || cookie.Value == "" {
		return "", false
	}
	// A change is checked against the cluster's sessions, not a lagging
	// node's.
	fresh := r.Method != http.MethodGet && r.Method != http.MethodHead
	session, err := a.agent.PanelSession(r.Context(), TokenHash(cookie.Value), fresh)
	if err != nil {
		return "", false
	}
	return session.User, true
}

// startSession signs user in, by passkey, having answered challenge: the
// state refuses a challenge answered before, so a sign-in replayed signs
// no one in.
func (a *Auth) startSession(ctx context.Context, w http.ResponseWriter, user, passkey, challenge string) error {
	token := Token(32)
	if err := a.agent.StartPanelSession(ctx, api.SessionRequest{Hash: TokenHash(token), User: user, Passkey: passkey, Challenge: challenge, ExpiresAt: a.now().Add(SessionLength)}); err != nil {
		return err
	}
	a.setCookie(w, sessionCookie, token, SessionLength)
	a.setCookie(w, ceremonyCookie, "", -time.Second)
	return nil
}

func (a *Auth) loginBegin(w http.ResponseWriter, r *http.Request) {
	// The person is verified, by PIN or biometrics, not only present: a
	// stolen key alone signs no one in.
	options, session, err := a.web.BeginDiscoverableLogin(webauthn.WithUserVerification(protocol.VerificationRequired))
	if err != nil {
		a.fail(w, err, http.StatusInternalServerError)
		return
	}
	sealed, err := a.seal(r.Context(), ceremony{Kind: "login", Session: *session, Expires: a.now().Add(ceremonyLength)})
	if err != nil {
		a.fail(w, err, http.StatusBadGateway)
		return
	}
	a.setCookie(w, ceremonyCookie, sealed, ceremonyLength)
	writeJSON(w, options)
}

func (a *Auth) loginFinish(w http.ResponseWriter, r *http.Request) {
	c, err := a.open(r.Context(), r, "login")
	if err != nil {
		a.fail(w, err, http.StatusBadRequest)
		return
	}
	var signedIn *person
	user, credential, err := a.web.FinishPasskeyLogin(func(_, handle []byte) (webauthn.User, error) {
		u, err := a.agent.PanelUserByHandle(r.Context(), base64.RawURLEncoding.EncodeToString(handle))
		if err != nil {
			return nil, err
		}
		signedIn, err = newPerson(u)
		return signedIn, err
	}, c.Session, r)
	if err != nil {
		a.log.Warn("a sign-in was refused", "error", err)
		a.fail(w, errors.New("this passkey does not sign anyone in"), http.StatusUnauthorized)
		return
	}
	// A counter that went back: the passkey may have been copied.
	if credential.Authenticator.CloneWarning {
		a.log.Warn("a sign-in was refused: the passkey's counter went back, it may have been copied", "user", user.WebAuthnName())
		a.fail(w, errors.New("this passkey may have been copied: remove it, and add another"), http.StatusUnauthorized)
		return
	}
	data, err := json.Marshal(credential)
	if err != nil {
		a.fail(w, err, http.StatusInternalServerError)
		return
	}
	id := base64.RawURLEncoding.EncodeToString(credential.ID)
	// Its counter is recorded first, only over the credential this
	// sign-in checked: two sign-ins at once cannot take its counter back.
	previous := ""
	for _, key := range signedIn.Passkeys {
		if key.ID == id {
			previous = key.Data
		}
	}
	if previous == "" {
		a.fail(w, errors.New("this passkey does not sign anyone in"), http.StatusUnauthorized)
		return
	}
	if err := a.agent.UsePasskey(r.Context(), id, previous, string(data)); err != nil {
		a.log.Warn("a sign-in was refused: its passkey was used meanwhile", "user", signedIn.Name, "error", err)
		a.fail(w, errors.New("this sign-in was not taken: begin again"), http.StatusUnauthorized)
		return
	}
	if err := a.startSession(r.Context(), w, signedIn.Name, id, c.Session.Challenge); err != nil {
		a.fail(w, errors.New("this sign-in was not taken: begin again"), http.StatusUnauthorized)
		return
	}
	writeJSON(w, map[string]string{"user": signedIn.Name})
}

type enrollRequest struct {
	Token string `json:"token"`
	Label string `json:"label"`
}

func (a *Auth) enrollBegin(w http.ResponseWriter, r *http.Request) {
	var request enrollRequest
	if err := json.NewDecoder(http.MaxBytesReader(w, r.Body, 4096)).Decode(&request); err != nil {
		a.fail(w, err, http.StatusBadRequest)
		return
	}
	label := strings.TrimSpace(request.Label)
	if label == "" || len(label) > 40 {
		a.fail(w, errors.New("name the passkey, as the device it is on: 1 to 40 characters"), http.StatusBadRequest)
		return
	}
	hash := TokenHash(request.Token)
	enrollment, err := a.agent.Enrollment(r.Context(), hash)
	if err != nil {
		a.fail(w, errors.New("this invitation or recovery code is not valid: it may have expired or been used"), http.StatusForbidden)
		return
	}
	u, err := a.agent.PanelUser(r.Context(), enrollment.User)
	if err != nil {
		a.fail(w, err, http.StatusBadGateway)
		return
	}
	p, err := newPerson(u)
	if err != nil {
		a.fail(w, err, http.StatusInternalServerError)
		return
	}
	var exclude []protocol.CredentialDescriptor
	for _, credential := range p.credentials {
		exclude = append(exclude, credential.Descriptor())
	}
	options, session, err := a.web.BeginRegistration(p,
		webauthn.WithAuthenticatorSelection(protocol.AuthenticatorSelection{ResidentKey: protocol.ResidentKeyRequirementRequired, UserVerification: protocol.VerificationRequired}),
		webauthn.WithExclusions(exclude))
	if err != nil {
		a.fail(w, err, http.StatusInternalServerError)
		return
	}
	sealed, err := a.seal(r.Context(), ceremony{Kind: "enroll", Session: *session, Hash: hash, User: u.Name, Label: label, Enrollment: enrollment.Kind, Expires: a.now().Add(ceremonyLength)})
	if err != nil {
		a.fail(w, err, http.StatusBadGateway)
		return
	}
	a.setCookie(w, ceremonyCookie, sealed, ceremonyLength)
	writeJSON(w, map[string]any{"user": u.Name, "options": options})
}

func (a *Auth) enrollFinish(w http.ResponseWriter, r *http.Request) {
	c, err := a.open(r.Context(), r, "enroll")
	if err != nil {
		a.fail(w, err, http.StatusBadRequest)
		return
	}
	u, err := a.agent.PanelUser(r.Context(), c.User)
	if err != nil {
		a.fail(w, err, http.StatusBadGateway)
		return
	}
	p, err := newPerson(u)
	if err != nil {
		a.fail(w, err, http.StatusInternalServerError)
		return
	}
	credential, err := a.web.FinishRegistration(p, c.Session, r)
	if err != nil {
		a.log.Warn("a passkey was not added", "user", u.Name, "error", err)
		a.fail(w, errors.New("the passkey was not added: "+err.Error()), http.StatusBadRequest)
		return
	}
	data, err := json.Marshal(credential)
	if err != nil {
		a.fail(w, err, http.StatusInternalServerError)
		return
	}
	key := store.Passkey{ID: base64.RawURLEncoding.EncodeToString(credential.ID), User: u.Name, Data: string(data), Label: c.Label}
	if err := a.agent.AddPasskey(r.Context(), api.PasskeyRequest{Hash: c.Hash, Passkey: key}); err != nil {
		a.fail(w, err, http.StatusConflict)
		return
	}
	answer := map[string]any{"user": u.Name}
	// A first passkey comes with recovery codes, shown once; so does one
	// added with the last code left.
	left := u.Recovery
	if c.Enrollment == store.EnrollRecovery {
		left--
	}
	if len(u.Passkeys) == 0 || left <= 0 {
		codes, err := a.newRecoveryCodes(r.Context(), u.Name)
		if err != nil {
			a.fail(w, err, http.StatusBadGateway)
			return
		}
		answer["codes"] = codes
	}
	if err := a.startSession(r.Context(), w, u.Name, key.ID, c.Session.Challenge); err != nil {
		a.fail(w, err, http.StatusBadGateway)
		return
	}
	// It signed its person in already.
	if err := a.agent.UsePasskey(r.Context(), key.ID, "", key.Data); err != nil {
		a.log.Warn("record a passkey's use", "error", err)
	}
	writeJSON(w, answer)
}

// newRecoveryCodes gives name new recovery codes, in place of any left.
func (a *Auth) newRecoveryCodes(ctx context.Context, name string) ([]string, error) {
	codes := make([]string, recoveryCodes)
	hashes := make([]string, recoveryCodes)
	for i := range codes {
		codes[i] = Token(10)
		hashes[i] = TokenHash(codes[i])
	}
	if err := a.agent.SetRecoveryCodes(ctx, name, hashes); err != nil {
		return nil, err
	}
	return codes, nil
}

func (a *Auth) logout(w http.ResponseWriter, r *http.Request) {
	if cookie, err := r.Cookie(sessionCookie); err == nil && cookie.Value != "" {
		if err := a.agent.EndPanelSession(r.Context(), TokenHash(cookie.Value)); err != nil {
			a.log.Warn("end a session", "error", err)
		}
	}
	a.setCookie(w, sessionCookie, "", -time.Second)
	http.Redirect(w, r, "/login", http.StatusSeeOther)
}

func (a *Auth) fail(w http.ResponseWriter, err error, status int) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(map[string]string{"error": err.Error()})
}

func writeJSON(w http.ResponseWriter, value any) {
	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(value)
}

// sameOrigin reports whether a request that changes something comes from
// the panel's own pages: the browser says where it comes from.
func (a *Auth) sameOrigin(r *http.Request) bool {
	return sameOrigin(r, a.origins)
}

// sameOrigin reports whether r comes from one of origins, by the browser's
// Origin header, or, when it sends "null", its Sec-Fetch-Site.
func sameOrigin(r *http.Request, origins []string) bool {
	if origin := r.Header.Get("Origin"); origin != "" && origin != "null" {
		return slices.Contains(origins, origin)
	}
	return r.Header.Get("Sec-Fetch-Site") == "same-origin"
}
