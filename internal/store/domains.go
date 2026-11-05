package store

import (
	"database/sql"
	"errors"
	"fmt"
	"maps"
	"regexp"
	"slices"
	"strings"
	"time"
)

// Domain is a host name of a tenant's own, outside the base domain, such
// as app.customer.com: every edge routes it to the tenant once it was seen
// to point at the cluster, and serves it with a certificate the cluster's
// leader orders and renews.
type Domain struct {
	// Name is the host name, lowercase ASCII (IDNA's A-labels).
	Name      string    `json:"name"`
	Tenant    string    `json:"tenant"`
	CreatedAt time.Time `json:"createdAt"`
	CreatedBy string    `json:"createdBy"`
	// State says where getting its certificate is (see the Domain*
	// states); Detail says why, such as what its DNS shows or what the CA
	// answered; Attempts counts the failures in a row, which space the
	// next attempt out; NextAt is when the leader looks at it again.
	State    string    `json:"state"`
	Detail   string    `json:"detail,omitempty"`
	Attempts int       `json:"attempts,omitempty"`
	NextAt   time.Time `json:"nextAt,omitzero"`
	// CheckedAt is when its DNS was last checked; VerifiedAt when it was
	// first seen to point at the cluster: only then do the edges route it.
	CheckedAt  time.Time `json:"checkedAt,omitzero"`
	VerifiedAt time.Time `json:"verifiedAt,omitzero"`
	// Certificate is its certificate chain, PEM; Key the certificate's
	// private key, PEM, sealed with the master key (see DomainKeyContext).
	Certificate string    `json:"-"`
	Key         string    `json:"-"`
	IssuedAt    time.Time `json:"issuedAt,omitzero"`
	NotAfter    time.Time `json:"notAfter,omitzero"`
}

// The states of a domain's certificate.
const (
	// DomainVerifying: added, its DNS not checked yet.
	DomainVerifying = "verifying"
	// DomainPending: its DNS does not point at the cluster (yet).
	DomainPending = "pending"
	// DomainIssuing: the leader orders its first certificate.
	DomainIssuing = "issuing"
	// DomainActive: it has a certificate, not due for renewal.
	DomainActive = "active"
	// DomainRenewing: its certificate is due for renewal, which the
	// leader orders; it is served until it expires.
	DomainRenewing = "renewing"
	// DomainFailed: ordering a certificate failed, and it has no valid
	// one; the leader tries again at NextAt.
	DomainFailed = "failed"
)

// DomainStates are the states a domain may be in.
var DomainStates = []string{DomainVerifying, DomainPending, DomainIssuing, DomainActive, DomainRenewing, DomainFailed}

// Verified reports whether the domain was seen to point at the cluster:
// the edges route it.
func (d Domain) Verified() bool { return !d.VerifiedAt.IsZero() }

// MaxDomains is how many domains a tenant may have.
const MaxDomains = 20

// Challenge is an HTTP-01 challenge of an order the leader waits on: every
// edge answers http://Domain/.well-known/acme-challenge/Token with KeyAuth
// until ExpiresAt.
type Challenge struct {
	Token     string    `json:"token"`
	Domain    string    `json:"domain"`
	KeyAuth   string    `json:"keyAuth"`
	ExpiresAt time.Time `json:"expiresAt"`
}

// ACMEAccount is the cluster's account with an ACME CA: its URL there and
// its private key, PEM, sealed with the master key (see
// AccountKeyContext).
type ACMEAccount struct {
	CA        string    `json:"ca"`
	Email     string    `json:"email"`
	Location  string    `json:"location"`
	Key       string    `json:"-"`
	CreatedAt time.Time `json:"createdAt"`
}

// SecretContext names the sealing context of a tenant's secret, as
// secrets.Context does: the store seals nothing, but seals values again.
func SecretContext(tenant, name string) string { return tenant + "/" + name }

// DomainKeyContext names the sealing context of a domain certificate's
// private key. No tenant secret's context is like it: tenant names have no
// spaces.
func DomainKeyContext(domain string) string { return "domain key " + domain }

// AccountKeyContext names the sealing context of the private key of the
// cluster's account with an ACME CA.
func AccountKeyContext(ca string) string { return "acme account " + ca }

// domainName is a host name as stored: lowercase letters, digits and
// hyphens in labels of 1 to 63, two labels at least. The API normalises
// and checks names further (see internal/domains).
var domainName = regexp.MustCompile(`^(?:[a-z0-9](?:[a-z0-9-]{0,61}[a-z0-9])?\.)+[a-z0-9](?:[a-z0-9-]{0,61}[a-z0-9])?$`)

// challengeToken is an ACME token: URL-safe base64 (RFC 8555 §8.1).
var challengeToken = regexp.MustCompile(`^[A-Za-z0-9_-]{1,256}$`)

func validDomain(name string) error {
	if len(name) > 253 || !domainName.MatchString(name) {
		return fmt.Errorf("%w: domain %q is not a host name in lowercase ASCII", ErrInvalid, name)
	}
	return nil
}

func loadDomains(q querier, state *snapshot) error {
	state.domains = map[string]Domain{}
	rows, err := q.Query(`SELECT name, tenant, created_at, created_by, state, detail, attempts, checked_at, next_at, verified_at, certificate, key, issued_at, not_after FROM domains`)
	if err != nil {
		return err
	}
	for rows.Next() {
		var d Domain
		var created string
		var checked, next, verified, issued, notAfter sql.NullString
		if err := rows.Scan(&d.Name, &d.Tenant, &created, &d.CreatedBy, &d.State, &d.Detail, &d.Attempts, &checked, &next, &verified, &d.Certificate, &d.Key, &issued, &notAfter); err != nil {
			rows.Close()
			return err
		}
		d.CreatedAt = parseTime(created)
		d.CheckedAt, d.NextAt, d.VerifiedAt, d.IssuedAt, d.NotAfter = nullTime(checked), nullTime(next), nullTime(verified), nullTime(issued), nullTime(notAfter)
		state.domains[d.Name] = d
	}
	return closeRows(rows)
}

// loadACME reads the cluster's ACME accounts and the challenges under way.
func loadACME(q querier, state *snapshot) error {
	state.accounts = map[string]ACMEAccount{}
	rows, err := q.Query(`SELECT ca, email, location, key, created_at FROM acme_accounts`)
	if err != nil {
		return err
	}
	for rows.Next() {
		var a ACMEAccount
		var created string
		if err := rows.Scan(&a.CA, &a.Email, &a.Location, &a.Key, &created); err != nil {
			rows.Close()
			return err
		}
		a.CreatedAt = parseTime(created)
		state.accounts[a.CA] = a
	}
	if err := closeRows(rows); err != nil {
		return err
	}
	state.challenges = map[string]Challenge{}
	rows, err = q.Query(`SELECT token, domain, key_auth, expires_at FROM acme_challenges`)
	if err != nil {
		return err
	}
	for rows.Next() {
		var c Challenge
		var expires string
		if err := rows.Scan(&c.Token, &c.Domain, &c.KeyAuth, &expires); err != nil {
			rows.Close()
			return err
		}
		c.ExpiresAt = parseTime(expires)
		state.challenges[c.Token] = c
	}
	return closeRows(rows)
}

func nullTime(text sql.NullString) time.Time {
	if !text.Valid {
		return time.Time{}
	}
	return parseTime(text.String)
}

// timeArg is a time as a statement's argument: NULL for none.
func timeArg(t time.Time) any {
	if t.IsZero() {
		return nil
	}
	return formatTime(t)
}

// Domains lists every tenant's domains, by name.
func (s *SQLite) Domains() []Domain {
	state := s.state.Load()
	domains := make([]Domain, 0, len(state.domains))
	for _, name := range slices.Sorted(maps.Keys(state.domains)) {
		domains = append(domains, state.domains[name])
	}
	return domains
}

// Domain returns one domain.
func (s *SQLite) Domain(name string) (Domain, error) {
	domain, ok := s.state.Load().domains[name]
	if !ok {
		return Domain{}, fmt.Errorf("domain %s: %w", name, ErrNotFound)
	}
	return domain, nil
}

// TenantDomains lists a tenant's domains, by name.
func (s *SQLite) TenantDomains(tenant string) []Domain {
	var domains []Domain
	for _, domain := range s.state.Load().domains {
		if domain.Tenant == tenant {
			domains = append(domains, domain)
		}
	}
	slices.SortFunc(domains, func(a, b Domain) int { return strings.Compare(a.Name, b.Name) })
	return domains
}

// AddDomain gives a live tenant a domain of its own, name as normalised
// and checked by the caller (see internal/domains): the leader checks its
// DNS at once. A domain is one tenant's: another's is refused with
// ErrConflict; the tenant's own is left as it is. A tenant has at most
// MaxDomains.
func (s *SQLite) AddDomain(tenant, name, actor string) (Domain, bool, error) {
	if err := validDomain(name); err != nil {
		return Domain{}, false, err
	}
	var existing Domain
	next, err := s.write(false, func(tx *recorder, current *snapshot, now time.Time) error {
		record, err := liveTenant(current, tenant)
		if err != nil {
			return err
		}
		if domain, ok := current.domains[name]; ok {
			if domain.Tenant != tenant {
				return fmt.Errorf("%w: domain %s belongs to tenant %s; remove it there first", ErrConflict, name, domain.Tenant)
			}
			existing = domain
			return errUnchanged
		}
		count := 0
		for _, domain := range current.domains {
			if domain.Tenant == tenant {
				count++
			}
		}
		if count >= MaxDomains {
			return fmt.Errorf("%w: tenant %s has %d domains, the most a tenant may have", ErrConflict, tenant, count)
		}
		stamp := formatTime(now)
		if err := tx.Exec(`INSERT INTO domains (name, tenant, created_at, created_by, state, next_at) VALUES (?, ?, ?, ?, ?, ?)`,
			name, tenant, stamp, actorOf(actor), DomainVerifying, stamp); err != nil {
			return err
		}
		return event(tx, now, actor, "domain.added", tenant, record.App, name)
	})
	if errors.Is(err, errUnchanged) {
		return existing, false, nil
	}
	if err != nil {
		return Domain{}, false, err
	}
	return next.domains[name], true, nil
}

// RemoveDomain takes a domain from its tenant, live or archived, with its
// certificate: the edges stop routing it at once.
func (s *SQLite) RemoveDomain(tenant, name, actor string) error {
	_, err := s.write(false, func(tx *recorder, current *snapshot, now time.Time) error {
		domain, ok := current.domains[name]
		if !ok || domain.Tenant != tenant {
			return fmt.Errorf("tenant %s has no domain %s: %w", tenant, name, ErrNotFound)
		}
		if err := tx.Exec(`DELETE FROM domains WHERE name = ?`, name); err != nil {
			return err
		}
		app := ""
		if record, ok := current.tenants[tenant]; ok {
			app = record.App
		}
		return event(tx, now, actor, "domain.removed", tenant, app, name)
	})
	return err
}

// DomainEvent is what UpdateDomain records in the audit log; an empty
// Action records nothing.
type DomainEvent struct {
	Action string
	Detail string
}

// UpdateDomain stores what the leader found of a domain, as read before:
// its state, detail, attempts and times, and its certificate. Nothing
// changes, with ErrConflict, when the domain was removed meanwhile, or
// removed and added again.
func (s *SQLite) UpdateDomain(update Domain, said DomainEvent, actor string) error {
	if !slices.Contains(DomainStates, update.State) {
		return fmt.Errorf("%w: domain state %q", ErrInvalid, update.State)
	}
	_, err := s.write(false, func(tx *recorder, current *snapshot, now time.Time) error {
		domain, ok := current.domains[update.Name]
		if !ok || domain.Tenant != update.Tenant || !domain.CreatedAt.Equal(update.CreatedAt) {
			return fmt.Errorf("%w: domain %s was removed meanwhile", ErrConflict, update.Name)
		}
		if err := tx.Exec(`UPDATE domains SET state = ?, detail = ?, attempts = ?, checked_at = ?, next_at = ?, verified_at = ?, certificate = ?, key = ?, issued_at = ?, not_after = ? WHERE name = ?`,
			update.State, update.Detail, update.Attempts, timeArg(update.CheckedAt), timeArg(update.NextAt), timeArg(update.VerifiedAt),
			update.Certificate, update.Key, timeArg(update.IssuedAt), timeArg(update.NotAfter), update.Name); err != nil {
			return err
		}
		if said.Action == "" {
			return nil
		}
		app := ""
		if record, ok := current.tenants[domain.Tenant]; ok {
			app = record.App
		}
		return event(tx, now, actor, said.Action, domain.Tenant, app, said.Detail)
	})
	return err
}

// CheckDomainNow has the leader look at a domain at its next pass, as if
// its next attempt were due: say once its DNS was changed.
func (s *SQLite) CheckDomainNow(tenant, name, actor string) error {
	_, err := s.write(false, func(tx *recorder, current *snapshot, now time.Time) error {
		domain, ok := current.domains[name]
		if !ok || domain.Tenant != tenant {
			return fmt.Errorf("tenant %s has no domain %s: %w", tenant, name, ErrNotFound)
		}
		return tx.Exec(`UPDATE domains SET next_at = ? WHERE name = ?`, formatTime(now), name)
	})
	return err
}

// Challenges lists the HTTP-01 challenges under way, by token.
func (s *SQLite) Challenges() []Challenge {
	state := s.state.Load()
	challenges := make([]Challenge, 0, len(state.challenges))
	for _, token := range slices.Sorted(maps.Keys(state.challenges)) {
		challenges = append(challenges, state.challenges[token])
	}
	return challenges
}

// PutChallenge has every edge answer an HTTP-01 challenge for one of the
// domains, and forgets the challenges that expired.
func (s *SQLite) PutChallenge(challenge Challenge) error {
	if !challengeToken.MatchString(challenge.Token) {
		return fmt.Errorf("%w: challenge token %q", ErrInvalid, challenge.Token)
	}
	_, err := s.write(false, func(tx *recorder, current *snapshot, now time.Time) error {
		if _, ok := current.domains[challenge.Domain]; !ok {
			return fmt.Errorf("domain %s: %w", challenge.Domain, ErrNotFound)
		}
		for _, token := range slices.Sorted(maps.Keys(current.challenges)) {
			if token != challenge.Token && !current.challenges[token].ExpiresAt.After(now) {
				if err := tx.Exec(`DELETE FROM acme_challenges WHERE token = ?`, token); err != nil {
					return err
				}
			}
		}
		return tx.Exec(`INSERT INTO acme_challenges (token, domain, key_auth, expires_at) VALUES (?, ?, ?, ?)
			ON CONFLICT (token) DO UPDATE SET domain = excluded.domain, key_auth = excluded.key_auth, expires_at = excluded.expires_at`,
			challenge.Token, challenge.Domain, challenge.KeyAuth, formatTime(challenge.ExpiresAt))
	})
	return err
}

// DeleteChallenge stops the edges answering a challenge.
func (s *SQLite) DeleteChallenge(token string) error {
	if _, ok := s.state.Load().challenges[token]; !ok {
		return nil
	}
	_, err := s.write(false, func(tx *recorder, current *snapshot, _ time.Time) error {
		if _, ok := current.challenges[token]; !ok {
			return errUnchanged
		}
		return tx.Exec(`DELETE FROM acme_challenges WHERE token = ?`, token)
	})
	if errors.Is(err, errUnchanged) {
		return nil
	}
	return err
}

// ACMEAccount returns the cluster's account with the CA of directory URL
// ca, if it has one.
func (s *SQLite) ACMEAccount(ca string) (ACMEAccount, bool) {
	account, ok := s.state.Load().accounts[ca]
	return account, ok
}

// PutACMEAccount keeps the cluster's account with a CA, unless it has one
// already: then it returns that one, which the leader uses instead, so
// leaders that made one each at once end up with the same.
func (s *SQLite) PutACMEAccount(account ACMEAccount, actor string) (ACMEAccount, error) {
	if account.CA == "" || account.Location == "" || account.Key == "" {
		return ACMEAccount{}, fmt.Errorf("%w: an ACME account needs its CA, URL and key", ErrInvalid)
	}
	var kept ACMEAccount
	next, err := s.write(false, func(tx *recorder, current *snapshot, now time.Time) error {
		if existing, ok := current.accounts[account.CA]; ok {
			kept = existing
			return errUnchanged
		}
		if err := tx.Exec(`INSERT INTO acme_accounts (ca, email, location, key, created_at) VALUES (?, ?, ?, ?, ?)`,
			account.CA, account.Email, account.Location, account.Key, formatTime(now)); err != nil {
			return err
		}
		return event(tx, now, actor, "acme.account-created", "", "", account.CA+": "+account.Location)
	})
	if errors.Is(err, errUnchanged) {
		return kept, nil
	}
	if err != nil {
		return ACMEAccount{}, err
	}
	return next.accounts[account.CA], nil
}
