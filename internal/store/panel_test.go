package store

import (
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"testing"
	"time"
)

func hashOf(token string) string {
	sum := sha256.Sum256([]byte(token))
	return hex.EncodeToString(sum[:])
}

// A person is invited, adds a passkey with the invitation once, gets
// recovery codes, signs in and out, and goes with everything of theirs.
func TestThePanelsPeople(t *testing.T) {
	st, _ := withApp(t, options())
	expires := time.Now().Add(24 * time.Hour)
	jan, err := st.InvitePanelUser("jan", hashOf("invite-1"), expires, "root")
	if err != nil || jan.Handle == "" || len(jan.Passkeys) != 0 {
		t.Fatalf("invited %+v, %v", jan, err)
	}
	if _, err := st.InvitePanelUser("Jan Kowalski", hashOf("x"), expires, "root"); !errors.Is(err, ErrInvalid) {
		t.Fatalf("a bad name: %v", err)
	}
	if e, err := st.Enrollment(hashOf("invite-1")); err != nil || e.User != "jan" || e.Kind != EnrollInvite {
		t.Fatalf("enrollment %+v, %v", e, err)
	}
	key := Passkey{ID: "cred-1", User: "jan", Data: `{"id":"x"}`, Label: "laptop"}
	if err := st.AddPasskey(hashOf("invite-1"), key); err != nil {
		t.Fatal(err)
	}
	// An invitation adds one passkey only.
	if err := st.AddPasskey(hashOf("invite-1"), Passkey{ID: "cred-2", User: "jan", Data: "{}", Label: "again"}); !errors.Is(err, ErrNotFound) {
		t.Fatalf("an invitation used twice: %v", err)
	}
	// Nor for another person.
	if _, err := st.InvitePanelUser("anna", hashOf("invite-2"), expires, "jan (panel)"); err != nil {
		t.Fatal(err)
	}
	if err := st.AddPasskey(hashOf("invite-2"), Passkey{ID: "cred-3", User: "jan", Data: "{}", Label: "x"}); !errors.Is(err, ErrNotFound) {
		t.Fatalf("anna's invitation used by jan: %v", err)
	}
	// An expired invitation adds nothing.
	if _, err := st.InvitePanelUser("anna", hashOf("old"), time.Now().Add(-time.Minute), "root"); err != nil {
		t.Fatal(err)
	}
	if _, err := st.Enrollment(hashOf("old")); !errors.Is(err, ErrNotFound) {
		t.Fatalf("an expired invitation: %v", err)
	}
	if err := st.SetRecoveryCodes("jan", []string{hashOf("code-1"), hashOf("code-2")}, "jan (panel)"); err != nil {
		t.Fatal(err)
	}
	if u, _ := st.PanelUser("jan"); u.Recovery != 2 || len(u.Passkeys) != 1 || u.Passkeys[0].Label != "laptop" {
		t.Fatalf("jan: %+v", u)
	}
	if err := st.AddPasskey(hashOf("code-1"), Passkey{ID: "cred-4", User: "jan", Data: "{}", Label: "phone"}); err != nil {
		t.Fatal(err)
	}
	if u, _ := st.PanelUserByHandle(jan.Handle); u.Name != "jan" || u.Recovery != 1 || len(u.Passkeys) != 2 {
		t.Fatalf("by handle: %+v", u)
	}
	if err := st.UsePasskey("cred-1", `{"id":"x","counter":5}`); err != nil {
		t.Fatal(err)
	}
	if u, _ := st.PanelUser("jan"); u.Passkeys[0].UsedAt.IsZero() || u.Passkeys[0].Data != `{"id":"x","counter":5}` {
		t.Fatalf("used: %+v", u.Passkeys[0])
	}
	session := func(cookie, passkey, challenge string, expires time.Time) PanelSession {
		return PanelSession{Hash: hashOf(cookie), User: "jan", Passkey: passkey, Challenge: challenge, ExpiresAt: expires}
	}
	if err := st.StartPanelSession(session("cookie", "cred-1", "ch-1", time.Now().Add(time.Hour))); err != nil {
		t.Fatal(err)
	}
	// A challenge answered once signs no one in again.
	if err := st.StartPanelSession(session("cookie-x", "cred-1", "ch-1", time.Now().Add(time.Hour))); !errors.Is(err, ErrConflict) {
		t.Fatalf("a sign-in replayed: %v", err)
	}
	// Nor with a passkey not jan's.
	if err := st.StartPanelSession(session("cookie-y", "nobody's", "ch-y", time.Now().Add(time.Hour))); !errors.Is(err, ErrNotFound) {
		t.Fatalf("a session by a stranger's passkey: %v", err)
	}
	if session, err := st.PanelSession(hashOf("cookie")); err != nil || session.User != "jan" {
		t.Fatalf("session %+v, %v", session, err)
	}
	if err := st.StartPanelSession(session("ended", "cred-1", "ch-2", time.Now().Add(-time.Second))); err != nil {
		t.Fatal(err)
	}
	if _, err := st.PanelSession(hashOf("ended")); !errors.Is(err, ErrNotFound) {
		t.Fatalf("an ended session: %v", err)
	}
	if err := st.EndPanelSession(hashOf("cookie")); err != nil {
		t.Fatal(err)
	}
	if _, err := st.PanelSession(hashOf("cookie")); !errors.Is(err, ErrNotFound) {
		t.Fatal("signed out, still signed in")
	}
	// One secret, the same for every node, made once.
	first, err := st.PanelSecret("ceremonies")
	if again, _ := st.PanelSecret("ceremonies"); err != nil || len(first) != 64 || again != first {
		t.Fatalf("secret %q then %q, %v", first, again, err)
	}
	// Removing a passkey ends the sessions it began.
	if err := st.StartPanelSession(session("by-phone", "cred-4", "ch-3", time.Now().Add(time.Hour))); err != nil {
		t.Fatal(err)
	}
	if err := st.RemovePasskey("cred-4", "jan (panel)"); err != nil {
		t.Fatal(err)
	}
	if _, err := st.PanelSession(hashOf("by-phone")); !errors.Is(err, ErrNotFound) {
		t.Fatal("a removed passkey's session goes on")
	}
	// A new invitation is a person's only one.
	if _, err := st.InvitePanelUser("anna", hashOf("invite-3"), expires, "root"); err != nil {
		t.Fatal(err)
	}
	if _, err := st.Enrollment(hashOf("invite-2")); !errors.Is(err, ErrNotFound) {
		t.Fatal("an older invitation still works")
	}
	if err := st.StartPanelSession(session("cookie-2", "cred-1", "ch-4", time.Now().Add(time.Hour))); err != nil {
		t.Fatal(err)
	}
	if err := st.RemovePanelUser("jan", "anna (panel)"); err != nil {
		t.Fatal(err)
	}
	if _, err := st.PanelSession(hashOf("cookie-2")); !errors.Is(err, ErrNotFound) {
		t.Fatal("a removed person stays signed in")
	}
	if len(st.PanelUsers()) != 1 {
		t.Fatalf("people %+v", st.PanelUsers())
	}
}
