package main

import (
	"bytes"
	"crypto/rand"
	"encoding/xml"
	"errors"
	"fmt"
	"io"
	"mime"
	"mime/multipart"
	"strings"
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
	token := newToken()
	s.keep(token, tenant, sid)
	return token
}

// keep keeps BIR1's session sid of a tenant, as the session token.
func (s *sessions) keep(token, tenant, sid string) {
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
	s.byToken[token] = &session{sid: sid, tenant: tenant, used: now}
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
	// Anything the plugin cannot read for sure withholds the answer: it
	// may hold BIR1's session.
	results, err := loginResults(r.Get("Content-Type"), body)
	switch {
	case err != nil:
		return nil, fmt.Errorf("BIR1's login answered what the plugin cannot read: %w", err)
	case len(results) > 1:
		return nil, errors.New("BIR1's login answered more than one result")
	case len(results) == 0 && r.Status == 200:
		return nil, errors.New("BIR1's login answered success, and no result")
	case len(results) == 0:
		// A fault: no session to hide.
		return body, nil
	}
	sid := strings.TrimSpace(results[0])
	if sid == "" {
		// A key BIR1 did not take: an empty result.
		return body, nil
	}
	if !sessionID.MatchString(sid) {
		return nil, errors.New("BIR1's login answered a session the plugin cannot read")
	}
	// The session as written goes: one written otherwise, as of escapes,
	// would stay, and the answer is withheld.
	token := newToken()
	replaced := bytes.ReplaceAll(body, []byte(sid), []byte(token))
	if again, err := loginResults(r.Get("Content-Type"), replaced); err != nil || len(again) != 1 || strings.TrimSpace(again[0]) != token {
		return nil, errors.New("BIR1's login answered its session written in a way the plugin cannot replace")
	}
	known.keep(token, l.tenant, sid)
	return replaced, nil
}

// loginResults reads what each ZalogujResult of a login's answer holds,
// its element of any prefix: the answer is XML, or the XOP package BIR1
// answers in, its envelope the package's root part.
func loginResults(contentType string, body []byte) ([]string, error) {
	envelope, err := envelopeOf(contentType, body)
	if err != nil {
		return nil, err
	}
	// A login's package holds the envelope alone: a part beside it is
	// not read, and could hold the session.
	if media, params, _ := mime.ParseMediaType(contentType); strings.EqualFold(media, "multipart/related") {
		parts := multipart.NewReader(bytes.NewReader(body), params["boundary"])
		for count := 0; ; count++ {
			_, err := parts.NextRawPart()
			if err == io.EOF && count == 1 {
				break
			}
			if err != nil || count > 0 {
				return nil, errors.New("a package of other than one part")
			}
		}
	}
	decoder := xml.NewDecoder(bytes.NewReader(envelope))
	// One document: a root, and nothing but space outside it.
	var results []string
	depth, roots := 0, 0
	for {
		token, err := decoder.Token()
		if err == io.EOF {
			return results, nil
		}
		if err != nil {
			return nil, err
		}
		switch token := token.(type) {
		case xml.CharData:
			if depth == 0 && len(bytes.TrimSpace(token)) > 0 {
				return nil, errors.New("text outside the envelope")
			}
			continue
		case xml.EndElement:
			depth--
			continue
		case xml.StartElement:
			if depth == 0 {
				if roots++; roots > 1 {
					return nil, errors.New("more than one envelope")
				}
			}
			if token.Name.Local != "ZalogujResult" {
				depth++
				continue
			}
		default:
			continue
		}
		var text strings.Builder
		for ended := false; !ended; {
			token, err := decoder.Token()
			if err != nil {
				return nil, fmt.Errorf("a result that does not end: %w", err)
			}
			switch token := token.(type) {
			case xml.StartElement:
				return nil, errors.New("a result that holds an element")
			case xml.EndElement:
				ended = true
			case xml.CharData:
				text.Write(token)
			}
		}
		results = append(results, text.String())
	}
}
