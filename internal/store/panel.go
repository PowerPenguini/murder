package store

import (
	"crypto/rand"
	"database/sql"
	"encoding/base64"
	"encoding/hex"
	"errors"
	"fmt"
	"maps"
	"regexp"
	"slices"
	"time"
)

// PanelUser is a person who signs in to the panel.
type PanelUser struct {
	Name string `json:"name"`
	// Handle is the WebAuthn user handle, random, in base64url.
	Handle    string    `json:"handle"`
	CreatedAt time.Time `json:"createdAt"`
	CreatedBy string    `json:"createdBy"`
	Passkeys  []Passkey `json:"passkeys"`
	// Recovery counts the recovery codes left unused.
	Recovery int `json:"recovery"`
}

// Passkey is one of a person's WebAuthn credentials.
type Passkey struct {
	// ID is the credential's ID, in base64url.
	ID   string `json:"id"`
	User string `json:"user"`
	// Data is the webauthn.Credential as JSON: its public key and counter.
	Data      string    `json:"data"`
	Label     string    `json:"label"`
	CreatedAt time.Time `json:"createdAt"`
	UsedAt    time.Time `json:"usedAt,omitzero"`
}

// Enrollment lets a person add a passkey, once: an invitation, which
// expires, or a recovery code.
type Enrollment struct {
	Hash      string    `json:"hash"`
	User      string    `json:"user"`
	Kind      string    `json:"kind"`
	ExpiresAt time.Time `json:"expiresAt,omitzero"`
}

// PanelSession is a person signed in.
type PanelSession struct {
	Hash string `json:"hash"`
	User string `json:"user"`
	// Passkey began the session; Challenge is the one it answered.
	Passkey   string    `json:"passkey"`
	Challenge string    `json:"challenge"`
	ExpiresAt time.Time `json:"expiresAt"`
}

// panelSchema is the schema version that added the panel's people.
// Signing in and out works the same in every release since, so it is
// marked as of it and taken while an upgrade freezes changes: the panel
// is most needed then.
const panelSchema = 13

// Enrollment kinds.
const (
	EnrollInvite   = "invite"
	EnrollRecovery = "recovery"
)

var (
	panelUserName = regexp.MustCompile(`^[a-z0-9]([a-z0-9._-]{0,30}[a-z0-9])?$`)
	tokenHash     = regexp.MustCompile(`^[0-9a-f]{64}$`)
)

// panelState is the panel's part of a snapshot.
type panelState struct {
	users       map[string]PanelUser
	passkeys    map[string]Passkey
	enrollments map[string]Enrollment
	sessions    map[string]PanelSession
	secrets     map[string]string
}

func loadPanel(q querier, state *snapshot) error {
	p := panelState{users: map[string]PanelUser{}, passkeys: map[string]Passkey{}, enrollments: map[string]Enrollment{}, sessions: map[string]PanelSession{}, secrets: map[string]string{}}
	scan := func(query string, row func(*sql.Rows) error) error {
		rows, err := q.Query(query)
		if err != nil {
			return err
		}
		for rows.Next() {
			if err := row(rows); err != nil {
				rows.Close()
				return err
			}
		}
		return closeRows(rows)
	}
	if err := scan(`SELECT name, handle, created_at, created_by FROM panel_users`, func(rows *sql.Rows) error {
		var u PanelUser
		var created string
		if err := rows.Scan(&u.Name, &u.Handle, &created, &u.CreatedBy); err != nil {
			return err
		}
		u.CreatedAt = parseTime(created)
		p.users[u.Name] = u
		return nil
	}); err != nil {
		return err
	}
	if err := scan(`SELECT id, user, data, label, created_at, used_at FROM panel_passkeys`, func(rows *sql.Rows) error {
		var k Passkey
		var created string
		var used sql.NullString
		if err := rows.Scan(&k.ID, &k.User, &k.Data, &k.Label, &created, &used); err != nil {
			return err
		}
		k.CreatedAt = parseTime(created)
		if used.Valid {
			k.UsedAt = parseTime(used.String)
		}
		p.passkeys[k.ID] = k
		return nil
	}); err != nil {
		return err
	}
	if err := scan(`SELECT hash, user, kind, expires_at FROM panel_enrollments`, func(rows *sql.Rows) error {
		var e Enrollment
		var expires sql.NullString
		if err := rows.Scan(&e.Hash, &e.User, &e.Kind, &expires); err != nil {
			return err
		}
		if expires.Valid {
			e.ExpiresAt = parseTime(expires.String)
		}
		p.enrollments[e.Hash] = e
		return nil
	}); err != nil {
		return err
	}
	if err := scan(`SELECT hash, user, passkey, challenge, expires_at FROM panel_sessions`, func(rows *sql.Rows) error {
		var s PanelSession
		var expires string
		if err := rows.Scan(&s.Hash, &s.User, &s.Passkey, &s.Challenge, &expires); err != nil {
			return err
		}
		s.ExpiresAt = parseTime(expires)
		p.sessions[s.Hash] = s
		return nil
	}); err != nil {
		return err
	}
	if err := scan(`SELECT name, value FROM panel_secrets`, func(rows *sql.Rows) error {
		var name, value string
		if err := rows.Scan(&name, &value); err != nil {
			return err
		}
		p.secrets[name] = value
		return nil
	}); err != nil {
		return err
	}
	state.panel = p
	return nil
}

// panelUser is a person with their passkeys and recovery codes left.
func (p panelState) user(name string, now time.Time) (PanelUser, bool) {
	u, ok := p.users[name]
	if !ok {
		return u, false
	}
	u.Passkeys = []Passkey{}
	for _, id := range slices.Sorted(maps.Keys(p.passkeys)) {
		if k := p.passkeys[id]; k.User == name {
			u.Passkeys = append(u.Passkeys, k)
		}
	}
	for _, e := range p.enrollments {
		if e.User == name && e.Kind == EnrollRecovery {
			u.Recovery++
		}
	}
	return u, true
}

func (s *SQLite) PanelUsers() []PanelUser {
	current := s.state.Load()
	users := make([]PanelUser, 0, len(current.panel.users))
	for _, name := range slices.Sorted(maps.Keys(current.panel.users)) {
		u, _ := current.panel.user(name, time.Now())
		users = append(users, u)
	}
	return users
}

func (s *SQLite) PanelUser(name string) (PanelUser, error) {
	if u, ok := s.state.Load().panel.user(name, time.Now()); ok {
		return u, nil
	}
	return PanelUser{}, fmt.Errorf("person %s: %w", name, ErrNotFound)
}

func (s *SQLite) PanelUserByHandle(handle string) (PanelUser, error) {
	current := s.state.Load()
	for name, u := range current.panel.users {
		if u.Handle == handle {
			found, _ := current.panel.user(name, time.Now())
			return found, nil
		}
	}
	return PanelUser{}, fmt.Errorf("a person with that handle: %w", ErrNotFound)
}

// InvitePanelUser lets name add a passkey with the token whose SHA-256 is
// hash, until expires; it adds the person if new.
func (s *SQLite) InvitePanelUser(name, hash string, expires time.Time, actor string) (PanelUser, error) {
	if !panelUserName.MatchString(name) {
		return PanelUser{}, fmt.Errorf("%w: a person's name is 1 to 32 lowercase letters, digits, dots, dashes and underscores", ErrInvalid)
	}
	if !tokenHash.MatchString(hash) {
		return PanelUser{}, fmt.Errorf("%w: a token's hash is SHA-256 in hex", ErrInvalid)
	}
	handle := make([]byte, 32)
	if _, err := rand.Read(handle); err != nil {
		return PanelUser{}, err
	}
	next, err := s.write(false, func(tx *recorder, current *snapshot, now time.Time) error {
		if _, ok := current.panel.users[name]; !ok {
			if err := tx.Exec(`INSERT INTO panel_users (name, handle, created_at, created_by) VALUES (?, ?, ?, ?)`,
				name, base64.RawURLEncoding.EncodeToString(handle), formatTime(now), actor); err != nil {
				return err
			}
			if err := event(tx, now, actor, "panel.user-added", "", "", name); err != nil {
				return err
			}
		}
		// Invitations of the past expire as new ones are made; a person's
		// newest invitation is their only one.
		for _, old := range slices.Sorted(maps.Keys(current.panel.enrollments)) {
			e := current.panel.enrollments[old]
			if e.Kind == "invite" && (e.ExpiresAt.Before(now) || e.User == name) {
				if err := tx.Exec(`DELETE FROM panel_enrollments WHERE hash = ?`, old); err != nil {
					return err
				}
			}
		}
		if err := tx.Exec(`INSERT INTO panel_enrollments (hash, user, kind, expires_at, created_at, created_by) VALUES (?, ?, 'invite', ?, ?, ?)`,
			hash, name, formatTime(expires), formatTime(now), actor); err != nil {
			return err
		}
		return event(tx, now, actor, "panel.invited", "", "", name+" may add a passkey until "+formatTime(expires))
	})
	if err != nil {
		return PanelUser{}, err
	}
	u, _ := next.panel.user(name, time.Now())
	return u, nil
}

// Enrollment is the invitation or recovery code whose token's SHA-256 is
// hash, while it is valid.
func (s *SQLite) Enrollment(hash string) (Enrollment, error) {
	e, ok := s.state.Load().panel.enrollments[hash]
	if !ok || (!e.ExpiresAt.IsZero() && time.Now().After(e.ExpiresAt)) {
		return Enrollment{}, fmt.Errorf("this invitation or recovery code: %w, or expired, or used", ErrNotFound)
	}
	return e, nil
}

// AddPasskey adds key to the person the enrollment hash is for, using the
// enrollment up, in one change: a token adds one passkey only.
func (s *SQLite) AddPasskey(hash string, key Passkey) error {
	if key.ID == "" || key.Data == "" {
		return fmt.Errorf("%w: a passkey needs its ID and its credential", ErrInvalid)
	}
	_, err := s.write(false, func(tx *recorder, current *snapshot, now time.Time) error {
		e, ok := current.panel.enrollments[hash]
		if !ok || (!e.ExpiresAt.IsZero() && now.After(e.ExpiresAt)) || e.User != key.User {
			return fmt.Errorf("this invitation or recovery code: %w, or expired, or used", ErrNotFound)
		}
		if _, taken := current.panel.passkeys[key.ID]; taken {
			return fmt.Errorf("%w: that passkey is added already", ErrConflict)
		}
		if err := tx.Exec(`DELETE FROM panel_enrollments WHERE hash = ?`, hash); err != nil {
			return err
		}
		if err := tx.Exec(`INSERT INTO panel_passkeys (id, user, data, label, created_at) VALUES (?, ?, ?, ?, ?)`,
			key.ID, key.User, key.Data, key.Label, formatTime(now)); err != nil {
			return err
		}
		how := "an invitation"
		if e.Kind == EnrollRecovery {
			how = "a recovery code"
		}
		return event(tx, now, key.User+" (panel)", "panel.passkey-added", "", "", fmt.Sprintf("%q, with %s", key.Label, how))
	})
	return err
}

// SetRecoveryCodes replaces the person's recovery codes with those whose
// tokens' SHA-256 are hashes.
func (s *SQLite) SetRecoveryCodes(name string, hashes []string, actor string) error {
	for _, hash := range hashes {
		if !tokenHash.MatchString(hash) {
			return fmt.Errorf("%w: a code's hash is SHA-256 in hex", ErrInvalid)
		}
	}
	_, err := s.write(false, func(tx *recorder, current *snapshot, now time.Time) error {
		if _, ok := current.panel.users[name]; !ok {
			return fmt.Errorf("person %s: %w", name, ErrNotFound)
		}
		if err := tx.Exec(`DELETE FROM panel_enrollments WHERE user = ? AND kind = 'recovery'`, name); err != nil {
			return err
		}
		for _, hash := range hashes {
			if err := tx.Exec(`INSERT INTO panel_enrollments (hash, user, kind, created_at, created_by) VALUES (?, ?, 'recovery', ?, ?)`,
				hash, name, formatTime(now), actor); err != nil {
				return err
			}
		}
		return event(tx, now, actor, "panel.recovery-codes", "", "", fmt.Sprintf("%s has %d new recovery codes", name, len(hashes)))
	})
	return err
}

// UsePasskey records a sign-in with a passkey: its credential as it is
// now (its counter), and when.
// sessionsKeptEnded is how long an ended session stays: longer than a
// sign-in ceremony lasts, so its challenge cannot be used again.
const sessionsKeptEnded = 15 * time.Minute

func (s *SQLite) UsePasskey(id, data string) error {
	return s.UsePasskeyIf(id, "", data)
}

// UsePasskeyIf is UsePasskey only while the passkey's credential is still
// previous, as the sign-in checked it: one that another sign-in recorded
// meanwhile, with a counter as far or further, is not written back over.
func (s *SQLite) UsePasskeyIf(id, previous, data string) error {
	_, err := s.writeAs(panelSchema, false, false, func(tx *recorder, current *snapshot, now time.Time) error {
		key, ok := current.panel.passkeys[id]
		if !ok {
			return fmt.Errorf("passkey: %w", ErrNotFound)
		}
		if previous != "" && key.Data != previous {
			return fmt.Errorf("%w: the passkey was used meanwhile", ErrConflict)
		}
		return tx.Exec(`UPDATE panel_passkeys SET data = ?, used_at = ? WHERE id = ?`, data, formatTime(now), id)
	})
	return err
}

func (s *SQLite) RemovePasskey(id, actor string) error {
	_, err := s.write(false, func(tx *recorder, current *snapshot, now time.Time) error {
		k, ok := current.panel.passkeys[id]
		if !ok {
			return fmt.Errorf("passkey: %w", ErrNotFound)
		}
		// The sessions it began end with it: it may have been copied.
		if err := tx.Exec(`DELETE FROM panel_sessions WHERE passkey = ?`, id); err != nil {
			return err
		}
		if err := tx.Exec(`DELETE FROM panel_passkeys WHERE id = ?`, id); err != nil {
			return err
		}
		return event(tx, now, actor, "panel.passkey-removed", "", "", fmt.Sprintf("%s's %q", k.User, k.Label))
	})
	return err
}

// RemovePanelUser removes a person, their passkeys, codes and sessions.
func (s *SQLite) RemovePanelUser(name, actor string) error {
	_, err := s.write(false, func(tx *recorder, current *snapshot, now time.Time) error {
		if _, ok := current.panel.users[name]; !ok {
			return fmt.Errorf("person %s: %w", name, ErrNotFound)
		}
		for _, table := range []string{"panel_sessions", "panel_enrollments", "panel_passkeys"} {
			if err := tx.Exec(`DELETE FROM `+table+` WHERE user = ?`, name); err != nil {
				return err
			}
		}
		if err := tx.Exec(`DELETE FROM panel_users WHERE name = ?`, name); err != nil {
			return err
		}
		return event(tx, now, actor, "panel.user-removed", "", "", name)
	})
	return err
}

// StartPanelSession signs session.User in, with the cookie whose SHA-256
// is session.Hash, by the passkey that answered session.Challenge; sessions
// that ended go. A challenge answered once signs no one in again.
func (s *SQLite) StartPanelSession(session PanelSession) error {
	if !tokenHash.MatchString(session.Hash) || session.Challenge == "" || session.Passkey == "" {
		return fmt.Errorf("%w: a session needs its hash, the passkey and the challenge", ErrInvalid)
	}
	name := session.User
	_, err := s.writeAs(panelSchema, false, false, func(tx *recorder, current *snapshot, now time.Time) error {
		if _, ok := current.panel.users[name]; !ok {
			return fmt.Errorf("person %s: %w", name, ErrNotFound)
		}
		if key, ok := current.panel.passkeys[session.Passkey]; !ok || key.User != name {
			return fmt.Errorf("%s's passkey: %w", name, ErrNotFound)
		}
		for _, other := range current.panel.sessions {
			if other.Challenge == session.Challenge {
				return fmt.Errorf("%w: this sign-in was used already", ErrConflict)
			}
		}
		// Sessions ended a while ago go; ended ones stay as long as their
		// sign-in's challenge could be replayed, as a mark it was used.
		for _, hash := range slices.Sorted(maps.Keys(current.panel.sessions)) {
			if current.panel.sessions[hash].ExpiresAt.Before(now.Add(-sessionsKeptEnded)) {
				if err := tx.Exec(`DELETE FROM panel_sessions WHERE hash = ?`, hash); err != nil {
					return err
				}
			}
		}
		if err := tx.Exec(`INSERT INTO panel_sessions (hash, user, passkey, challenge, created_at, expires_at) VALUES (?, ?, ?, ?, ?, ?)`,
			session.Hash, name, session.Passkey, session.Challenge, formatTime(now), formatTime(session.ExpiresAt)); err != nil {
			return err
		}
		expires := session.ExpiresAt
		return event(tx, now, name+" (panel)", "panel.signed-in", "", "", "until "+formatTime(expires))
	})
	return err
}

// PanelSession is the session whose cookie's SHA-256 is hash, while it
// lasts.
func (s *SQLite) PanelSession(hash string) (PanelSession, error) {
	session, ok := s.state.Load().panel.sessions[hash]
	if !ok || time.Now().After(session.ExpiresAt) {
		return PanelSession{}, fmt.Errorf("session: %w, or ended", ErrNotFound)
	}
	return session, nil
}

func (s *SQLite) EndPanelSession(hash string) error {
	_, err := s.writeAs(panelSchema, false, false, func(tx *recorder, current *snapshot, now time.Time) error {
		session, ok := current.panel.sessions[hash]
		if !ok || !session.ExpiresAt.After(now) {
			return errUnchanged
		}
		// Ended, not deleted: its challenge stays a mark that it was used.
		if err := tx.Exec(`UPDATE panel_sessions SET expires_at = ? WHERE hash = ?`, formatTime(now), hash); err != nil {
			return err
		}
		return event(tx, now, session.User+" (panel)", "panel.signed-out", "", "", "")
	})
	if errors.Is(err, errUnchanged) {
		return nil
	}
	return err
}

// PanelSecret is the secret every node's panel shares under name, made
// once, at random.
func (s *SQLite) PanelSecret(name string) (string, error) {
	if value, ok := s.state.Load().panel.secrets[name]; ok {
		return value, nil
	}
	raw := make([]byte, 32)
	if _, err := rand.Read(raw); err != nil {
		return "", err
	}
	next, err := s.writeAs(panelSchema, false, false, func(tx *recorder, current *snapshot, now time.Time) error {
		if _, ok := current.panel.secrets[name]; ok {
			return errUnchanged
		}
		return tx.Exec(`INSERT INTO panel_secrets (name, value) VALUES (?, ?)`, name, hex.EncodeToString(raw))
	})
	if errors.Is(err, errUnchanged) {
		return s.state.Load().panel.secrets[name], nil
	}
	if err != nil {
		return "", err
	}
	return next.panel.secrets[name], nil
}
