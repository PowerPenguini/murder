package main

import (
	"bytes"
	"crypto/rand"
	"errors"
	"sync"
	"time"

	"github.com/PowerPenguini/murder/pluginsdk"
)

// A tenant never holds a session of BIR1's: opened with the operator's
// key, it would let the tenant call BIR1 itself, past what the edge counts
// and limits. A login's answer gives it a session of the plugin's in
// place of BIR1's, and the plugin puts BIR1's back on each call of the
// tenant's that names it. Sessions are the container's: one the plugin
// does not know, as after it was started again, or on another node, is
// none, as BIR1's that ran out are, and the client logs in again.
type sessions struct {
	mu      sync.Mutex
	byToken map[string]*session
	now     func() time.Time
}

type session struct {
	sid, tenant string
	used        time.Time
}

// How long a session is kept without a call, as BIR1 keeps its own, and
// how many are kept, in all and of one tenant.
const (
	sessionIdle      = time.Hour
	maxSessions      = 10000
	maxTenantSession = 64
)

var known = &sessions{byToken: map[string]*session{}, now: time.Now}

// open keeps BIR1's session sid of a tenant, and gives the session the
// tenant gets in its place.
func (s *sessions) open(tenant, sid string) string {
	s.mu.Lock()
	defer s.mu.Unlock()
	now := s.now()
	var oldest, tenantsOldest string
	tenants := 0
	for token, kept := range s.byToken {
		if now.Sub(kept.used) > sessionIdle {
			delete(s.byToken, token)
			continue
		}
		if oldest == "" || kept.used.Before(s.byToken[oldest].used) {
			oldest = token
		}
		if kept.tenant == tenant {
			tenants++
			if tenantsOldest == "" || kept.used.Before(s.byToken[tenantsOldest].used) {
				tenantsOldest = token
			}
		}
	}
	if tenants >= maxTenantSession {
		delete(s.byToken, tenantsOldest)
	} else if len(s.byToken) >= maxSessions {
		delete(s.byToken, oldest)
	}
	token := newToken()
	s.byToken[token] = &session{sid: sid, tenant: tenant, used: now}
	return token
}

// find is BIR1's session of a tenant's token.
func (s *sessions) find(tenant, token string) (string, bool) {
	s.mu.Lock()
	defer s.mu.Unlock()
	kept, ok := s.byToken[token]
	if !ok || kept.tenant != tenant || s.now().Sub(kept.used) > sessionIdle {
		return "", false
	}
	kept.used = s.now()
	return kept.sid, true
}

// close forgets a tenant's session, as it logs out.
func (s *sessions) close(tenant, token string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if kept, ok := s.byToken[token]; ok && kept.tenant == tenant {
		delete(s.byToken, token)
	}
}

// newToken is a session of the plugin's: 20 lowercase letters and digits,
// as BIR1's are.
func newToken() string {
	const letters = "abcdefghijklmnopqrstuvwxyz0123456789"
	var b [20]byte
	_, _ = rand.Read(b[:])
	for i := range b {
		b[i] = letters[int(b[i])%len(letters)]
	}
	return string(b[:])
}

// login replaces BIR1's session in a login's answer with one of the
// plugin's, for the tenant that logged in.
type login struct{ tenant string }

func (l *login) Response(*pluginsdk.Response, *pluginsdk.Counter) bool { return true }
func (l *login) Body([]byte, *pluginsdk.Counter) bool                  { return false }
func (l *login) End(bool, *pluginsdk.Counter)                          {}

func (l *login) Replace(r *pluginsdk.Response, body []byte, _ *pluginsdk.Counter) ([]byte, error) {
	start := bytes.Index(body, []byte("<ZalogujResult>"))
	end := bytes.Index(body, []byte("</ZalogujResult>"))
	if r.Status != 200 || start < 0 || end < start {
		// A fault, or a key BIR1 did not take (an empty result): no
		// session to hide.
		return body, nil
	}
	sid := string(body[start+len("<ZalogujResult>") : end])
	if !sessionID.MatchString(sid) {
		return nil, errors.New("BIR1's login answered a session the plugin cannot read")
	}
	return bytes.ReplaceAll(body, []byte(sid), []byte(known.open(l.tenant, sid))), nil
}
