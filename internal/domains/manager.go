package domains

import (
	"context"
	"crypto"
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/tls"
	"crypto/x509"
	"encoding/pem"
	"errors"
	"fmt"
	"log/slog"
	"net/http"
	"net/netip"
	"slices"
	"strings"
	"sync"
	"time"

	"github.com/mholt/acmez/v3"
	"github.com/mholt/acmez/v3/acme"

	"github.com/PowerPenguini/murder/internal/metrics"
	"github.com/PowerPenguini/murder/internal/store"
)

// Actor is who the audit log names for what the leader does with domains.
const Actor = "murder"

// Store is what the manager needs of the replicated state.
type Store interface {
	Domains() []store.Domain
	Get(name string) (store.Record, error)
	Nodes() []store.Node
	Frozen() string
	UpdateDomain(update store.Domain, said store.DomainEvent, actor string) error
	PutChallenge(challenge store.Challenge) error
	DeleteChallenge(token string) error
	ACMEAccount(ca string) (store.ACMEAccount, bool)
	PutACMEAccount(account store.ACMEAccount, actor string) (store.ACMEAccount, error)
	Watch() <-chan struct{}
}

// Sealer seals private keys with the master key, and opens them.
type Sealer interface {
	Seal(context, plaintext string) (string, error)
	Open(context, sealed string) (string, error)
}

// Manager, on the cluster's leader, checks that tenants' domains point at
// the cluster, orders their certificates and renews them. Everything it
// knows is in the replicated state, so a new leader goes on where the old
// one stopped.
type Manager struct {
	Store Store
	Key   Sealer
	// BaseDomain is where tenants have their own names: a domain may lead
	// to <tenant>.<BaseDomain> or to BaseDomain with a CNAME.
	BaseDomain string
	// CA is the ACME directory certificates are ordered from, Email the
	// account's contact. Without a CA nothing is ordered: NoCA says why.
	CA    string
	Email string
	NoCA  string
	// HTTPClient talks to the CA; http.DefaultClient's way if nil.
	HTTPClient *http.Client
	// Resolver checks domains' DNS; net.DefaultResolver if nil.
	Resolver Resolver
	// Public are the addresses the nodes are reached at; the nodes'
	// public addresses in the state if nil.
	Public func() []netip.Addr
	// Probe, if set, returns once the edges answer a challenge (or it
	// gave up waiting), before the CA is asked to check it.
	Probe func(ctx context.Context, challenge store.Challenge) error
	// Leader reports whether this node leads the cluster: only the leader
	// acts. Nil outside a cluster.
	Leader func() bool
	// Ready reports whether every node runs a release that applies what
	// the manager writes; nil if they all do.
	Ready func(ctx context.Context) error
	Log   *slog.Logger
	// Now tells the time; time.Now if nil.
	Now func() time.Time
	// Every is the longest between two passes; Run's default if zero.
	Every time.Duration

	// obtain, if set, orders certificates in place of the CA: for tests.
	obtain func(ctx context.Context, name string) (chain, key string, leaf *x509.Certificate, err error)

	mu      sync.Mutex
	client  *acmez.Client
	account *acme.Account
	// ready is until when every node was found to run a release that
	// applies domain writes.
	ready  time.Time
	notice string
}

// Backoffs: a domain whose DNS does not point at the cluster is checked
// again after a minute, then less and less often, an hour apart at most;
// an order that failed is tried again after five minutes, then less and
// less often, six hours apart at most, well within a CA's limits on
// failed validations (Let's Encrypt: five an hour for a name).
const (
	dnsFirst   = time.Minute
	dnsMost    = time.Hour
	orderFirst = 5 * time.Minute
	orderMost  = 6 * time.Hour
	// orderTimeout bounds one order; a leader that stopped during one is
	// followed by the next once it is over.
	orderTimeout = 5 * time.Minute
	// renewBefore is how long before it expires a certificate is renewed,
	// at the most: a third of its lifetime for a short one.
	renewBefore = 30 * 24 * time.Hour
	// challengeValid is how long the edges answer a challenge at most.
	challengeValid = 15 * time.Minute
	// readyFor is how long every node is taken to run a release that
	// applies domain writes, once seen to.
	readyFor = 10 * time.Minute
)

// backoff is the wait before the next attempt after attempts failures in
// a row: first, doubling, most at the most.
func backoff(attempts int, first, most time.Duration) time.Duration {
	wait := first
	for i := 1; i < attempts && wait < most; i++ {
		wait *= 2
	}
	return min(wait, most)
}

// RenewAt is when a certificate valid from issued to notAfter is renewed.
func RenewAt(issued, notAfter time.Time) time.Time {
	before := renewBefore
	if lifetime := notAfter.Sub(issued); !issued.IsZero() && lifetime > 0 {
		before = min(before, lifetime/3)
	}
	return notAfter.Add(-before)
}

func (m *Manager) now() time.Time {
	if m.Now != nil {
		return m.Now()
	}
	return time.Now()
}

func (m *Manager) log() *slog.Logger {
	if m.Log != nil {
		return m.Log
	}
	return slog.Default()
}

// Run makes passes until ctx ends: every Every, and soon after the state
// changes, say once a domain was added.
func (m *Manager) Run(ctx context.Context) {
	every := m.Every
	if every <= 0 {
		every = 30 * time.Second
	}
	for {
		changed := m.Store.Watch()
		m.Step(ctx)
		select {
		case <-ctx.Done():
			return
		case <-changed:
		case <-time.After(every):
		}
		// Changes come in bursts (a pass's own writes among them).
		select {
		case <-ctx.Done():
			return
		case <-time.After(time.Second):
		}
	}
}

// Step looks once at every domain that is due, on the leader.
func (m *Manager) Step(ctx context.Context) {
	if m.Leader != nil && !m.Leader() {
		return
	}
	if frozen := m.Store.Frozen(); frozen != "" {
		return
	}
	due := m.due()
	if len(due) == 0 {
		return
	}
	if !m.everyNodeReady(ctx) {
		return
	}
	for _, domain := range due {
		if ctx.Err() != nil {
			return
		}
		if m.Leader != nil && !m.Leader() {
			return
		}
		m.handle(ctx, domain)
	}
}

// due lists the domains of live tenants whose next look is due.
func (m *Manager) due() []store.Domain {
	now := m.now()
	var due []store.Domain
	for _, domain := range m.Store.Domains() {
		record, err := m.Store.Get(domain.Tenant)
		// An archived tenant's domains wait for it: nothing is ordered
		// for a name that leads nowhere.
		if err != nil || record.Archived() {
			continue
		}
		if domain.NextAt.After(now) {
			continue
		}
		due = append(due, domain)
	}
	return due
}

// everyNodeReady reports whether every node runs a release that applies
// domain writes; a release before them would stop at the first.
func (m *Manager) everyNodeReady(ctx context.Context) bool {
	if m.Ready == nil {
		return true
	}
	m.mu.Lock()
	ready := m.now().Before(m.ready)
	m.mu.Unlock()
	if ready {
		return true
	}
	err := m.Ready(ctx)
	m.mu.Lock()
	defer m.mu.Unlock()
	if err != nil {
		if m.notice != err.Error() {
			m.log().Info("tenants' domains wait for every node to run this release", "error", err)
			m.notice = err.Error()
		}
		return false
	}
	m.ready, m.notice = m.now().Add(readyFor), ""
	return true
}

// target is what d must lead to in DNS.
func (m *Manager) target(d store.Domain) Target {
	target := Target{Names: []string{d.Tenant + "." + m.BaseDomain, m.BaseDomain}}
	if m.Public != nil {
		target.Addresses = m.Public()
	} else {
		target.Addresses = PublicAddresses(m.Store.Nodes())
	}
	return target
}

// PublicAddresses are the nodes' public addresses, which the domains may
// point at.
func PublicAddresses(nodes []store.Node) []netip.Addr {
	var addresses []netip.Addr
	for _, node := range nodes {
		for _, address := range node.Public {
			if !slices.Contains(addresses, address.Unmap()) {
				addresses = append(addresses, address.Unmap())
			}
		}
	}
	slices.SortFunc(addresses, func(a, b netip.Addr) int { return a.Compare(b) })
	return addresses
}

func (m *Manager) resolver() Resolver {
	if m.Resolver != nil {
		return m.Resolver
	}
	return defaultResolver{}
}

// valid reports whether d has a certificate that has not expired.
func valid(d store.Domain, now time.Time) bool {
	return d.Certificate != "" && d.Key != "" && now.Before(d.NotAfter)
}

// handle moves one due domain on: it checks its DNS, and orders its first
// certificate or renews it once the name points at the cluster.
func (m *Manager) handle(ctx context.Context, d store.Domain) {
	now := m.now()
	next := d
	have := valid(d, now)
	// A certificate not due for renewal waits for its time.
	if have && now.Before(RenewAt(d.IssuedAt, d.NotAfter)) {
		at := RenewAt(d.IssuedAt, d.NotAfter)
		if d.State == store.DomainActive && d.NextAt.Equal(at) {
			return
		}
		next.State, next.Detail, next.Attempts, next.NextAt = store.DomainActive, "", 0, at
		m.update(next, store.DomainEvent{})
		return
	}
	check := Check(ctx, m.resolver(), d.Name, m.target(d))
	if ctx.Err() != nil {
		return
	}
	next.CheckedAt = now
	if !check.OK {
		next.Attempts = d.Attempts + 1
		next.NextAt = now.Add(backoff(next.Attempts, dnsFirst, dnsMost))
		next.State = store.DomainPending
		if have {
			next.State = store.DomainRenewing
		}
		said := store.DomainEvent{}
		detail := check.Said
		if have {
			detail = "renewal waits: " + check.Said
		}
		// Said once while it stays the same: a name not set up yet is
		// checked again and again.
		if detail != d.Detail || next.State != d.State {
			said = store.DomainEvent{Action: "domain.pending", Detail: d.Name + ": " + detail}
		}
		next.Detail = detail
		m.update(next, said)
		return
	}
	if !d.Verified() {
		next.VerifiedAt = now
		if err := m.update(next, store.DomainEvent{Action: "domain.verified", Detail: d.Name + ": " + check.Said}); err != nil {
			return
		}
		d = next
	}
	// Failures of the DNS check are not the CA's: an order begins anew.
	if d.State == store.DomainPending || d.State == store.DomainVerifying {
		next.Attempts = 0
	}
	if m.CA == "" {
		next.State, next.Detail = store.DomainPending, m.NoCA
		if have {
			next.State = store.DomainRenewing
		}
		next.NextAt = now.Add(dnsMost)
		said := store.DomainEvent{}
		if next.Detail != d.Detail {
			said = store.DomainEvent{Action: "domain.pending", Detail: d.Name + ": " + m.NoCA}
		}
		m.update(next, said)
		return
	}
	m.order(ctx, next, have)
}

// order gets a certificate for d and keeps it, or says why it could not.
func (m *Manager) order(ctx context.Context, d store.Domain, renewal bool) {
	now := m.now()
	ordering := d
	ordering.State = store.DomainIssuing
	if renewal {
		ordering.State = store.DomainRenewing
	}
	ordering.Detail = "ordering a certificate from " + m.CA
	// A leader that stops meanwhile leaves it to the next, once the order
	// would be over.
	ordering.NextAt = now.Add(orderTimeout + time.Minute)
	if err := m.update(ordering, store.DomainEvent{}); err != nil {
		return
	}
	ctx, cancel := context.WithTimeout(ctx, orderTimeout)
	defer cancel()
	obtain := m.fromCA
	if m.obtain != nil {
		obtain = m.obtain
	}
	chain, key, leaf, err := obtain(ctx, d.Name)
	now = m.now()
	if err != nil {
		failed := ordering
		failed.Attempts = d.Attempts + 1
		failed.NextAt = now.Add(backoff(failed.Attempts, orderFirst, orderMost))
		failed.Detail = "ordering its certificate failed: " + oneLine(err.Error())
		if !renewal {
			failed.State = store.DomainFailed
		}
		m.log().Warn("order a certificate for a tenant's domain", "domain", d.Name, "tenant", d.Tenant, "attempts", failed.Attempts, "next", failed.NextAt, "error", err)
		m.update(failed, store.DomainEvent{Action: "domain.failed", Detail: d.Name + ": " + failed.Detail})
		return
	}
	sealed, err := m.Key.Seal(store.DomainKeyContext(d.Name), key)
	if err != nil {
		m.log().Error("seal a domain certificate's key", "domain", d.Name, "error", err)
		return
	}
	issued := ordering
	issued.State, issued.Detail, issued.Attempts = store.DomainActive, "", 0
	issued.Certificate, issued.Key = chain, sealed
	issued.IssuedAt, issued.NotAfter = now, leaf.NotAfter
	issued.NextAt = RenewAt(issued.IssuedAt, issued.NotAfter)
	action := "domain.issued"
	if renewal {
		action = "domain.renewed"
	}
	m.log().Info("a tenant's domain has a certificate", "domain", d.Name, "tenant", d.Tenant, "notAfter", leaf.NotAfter, "renewal", renewal)
	m.update(issued, store.DomainEvent{Action: action, Detail: fmt.Sprintf("%s: valid until %s", d.Name, leaf.NotAfter.UTC().Format(time.DateOnly))})
}

// update stores d, saying why it could not.
func (m *Manager) update(d store.Domain, said store.DomainEvent) error {
	err := m.Store.UpdateDomain(d, said, Actor)
	switch {
	case err == nil:
	case errors.Is(err, store.ErrConflict), errors.Is(err, store.ErrNotFound), errors.Is(err, store.ErrFrozen):
		// Removed meanwhile, or changes stopped: looked at again later.
	default:
		m.log().Warn("store a domain's state", "domain", d.Name, "error", err)
	}
	return err
}

func oneLine(text string) string {
	text = strings.Join(strings.Fields(text), " ")
	if len(text) > 300 {
		text = text[:300] + "…"
	}
	return text
}

// fromCA orders a certificate for name from the CA: its chain, PEM, its
// private key, PEM, and its leaf.
func (m *Manager) fromCA(ctx context.Context, name string) (string, string, *x509.Certificate, error) {
	client, account, err := m.accountFor(ctx)
	if err != nil {
		return "", "", nil, err
	}
	key, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		return "", "", nil, err
	}
	certificates, err := client.ObtainCertificateForSANs(ctx, account, key, []string{name})
	if err != nil {
		return "", "", nil, err
	}
	if len(certificates) == 0 {
		return "", "", nil, errors.New("the CA sent no certificate")
	}
	keyDER, err := x509.MarshalPKCS8PrivateKey(key)
	if err != nil {
		return "", "", nil, err
	}
	keyPEM := string(pem.EncodeToMemory(&pem.Block{Type: "PRIVATE KEY", Bytes: keyDER}))
	chain := string(certificates[0].ChainPEM)
	pair, err := tls.X509KeyPair([]byte(chain), []byte(keyPEM))
	if err != nil {
		return "", "", nil, fmt.Errorf("the certificate the CA sent does not go with its key: %w", err)
	}
	if err := pair.Leaf.VerifyHostname(name); err != nil {
		return "", "", nil, fmt.Errorf("the certificate the CA sent is not for %s: %w", name, err)
	}
	return chain, keyPEM, pair.Leaf, nil
}

// accountFor returns the client for the CA and the cluster's account with
// it, made and kept in the state the first time.
func (m *Manager) accountFor(ctx context.Context) (*acmez.Client, acme.Account, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.client == nil {
		m.client = &acmez.Client{
			Client: &acme.Client{
				Directory:    m.CA,
				HTTPClient:   m.HTTPClient,
				UserAgent:    "murder",
				PollTimeout:  2 * time.Minute,
				PollInterval: time.Second,
				Logger:       m.log().With("component", "acme"),
			},
			ChallengeSolvers: map[string]acmez.Solver{acme.ChallengeTypeHTTP01: &solver{m: m}},
		}
	}
	stored, ok := m.Store.ACMEAccount(m.CA)
	if m.account != nil && ok && m.account.Location == stored.Location {
		return m.client, *m.account, nil
	}
	if ok {
		account, err := m.openAccount(stored)
		if err != nil {
			return nil, acme.Account{}, err
		}
		m.account = &account
		return m.client, account, nil
	}
	key, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		return nil, acme.Account{}, err
	}
	account := acme.Account{TermsOfServiceAgreed: true, PrivateKey: key}
	if m.Email != "" {
		account.Contact = []string{"mailto:" + m.Email}
	}
	account, err = m.client.NewAccount(ctx, account)
	if err != nil {
		return nil, acme.Account{}, fmt.Errorf("make an account with %s: %w", m.CA, err)
	}
	der, err := x509.MarshalPKCS8PrivateKey(key)
	if err != nil {
		return nil, acme.Account{}, err
	}
	sealed, err := m.Key.Seal(store.AccountKeyContext(m.CA), string(pem.EncodeToMemory(&pem.Block{Type: "PRIVATE KEY", Bytes: der})))
	if err != nil {
		return nil, acme.Account{}, err
	}
	kept, err := m.Store.PutACMEAccount(store.ACMEAccount{CA: m.CA, Email: m.Email, Location: account.Location, Key: sealed}, Actor)
	if err != nil {
		return nil, acme.Account{}, fmt.Errorf("keep the account with %s: %w", m.CA, err)
	}
	// Another leader kept one first: that one is the cluster's.
	if kept.Location != account.Location {
		if account, err = m.openAccount(kept); err != nil {
			return nil, acme.Account{}, err
		}
	}
	m.account = &account
	return m.client, account, nil
}

// openAccount makes a stored account usable.
func (m *Manager) openAccount(stored store.ACMEAccount) (acme.Account, error) {
	keyPEM, err := m.Key.Open(store.AccountKeyContext(stored.CA), stored.Key)
	if err != nil {
		return acme.Account{}, fmt.Errorf("open the key of the account with %s: %w", stored.CA, err)
	}
	key, err := parseKey(keyPEM)
	if err != nil {
		return acme.Account{}, fmt.Errorf("the key of the account with %s: %w", stored.CA, err)
	}
	account := acme.Account{Status: acme.StatusValid, Location: stored.Location, PrivateKey: key, TermsOfServiceAgreed: true}
	if stored.Email != "" {
		account.Contact = []string{"mailto:" + stored.Email}
	}
	return account, nil
}

// parseKey reads a PKCS #8 private key, PEM.
func parseKey(text string) (crypto.Signer, error) {
	block, _ := pem.Decode([]byte(text))
	if block == nil {
		return nil, errors.New("no PEM block")
	}
	key, err := x509.ParsePKCS8PrivateKey(block.Bytes)
	if err != nil {
		return nil, err
	}
	signer, ok := key.(crypto.Signer)
	if !ok {
		return nil, fmt.Errorf("a key of type %T cannot sign", key)
	}
	return signer, nil
}

// solver answers HTTP-01 challenges from every edge: it puts them in the
// replicated state, which every edge serves from.
type solver struct{ m *Manager }

func (s *solver) Present(_ context.Context, challenge acme.Challenge) error {
	return s.m.Store.PutChallenge(store.Challenge{
		Token: challenge.Token, Domain: challenge.Identifier.Value, KeyAuth: challenge.KeyAuthorization,
		ExpiresAt: s.m.now().Add(challengeValid),
	})
}

// Wait returns once the edges answer the challenge, so the CA, which may
// ask any node, finds it on every one.
func (s *solver) Wait(ctx context.Context, challenge acme.Challenge) error {
	if s.m.Probe == nil {
		return nil
	}
	return s.m.Probe(ctx, store.Challenge{Token: challenge.Token, Domain: challenge.Identifier.Value, KeyAuth: challenge.KeyAuthorization})
}

func (s *solver) CleanUp(ctx context.Context, challenge acme.Challenge) error {
	return s.m.Store.DeleteChallenge(challenge.Token)
}

type defaultResolver struct{}

func (defaultResolver) LookupCNAME(ctx context.Context, host string) (string, error) {
	return netResolver.LookupCNAME(ctx, host)
}

func (defaultResolver) LookupNetIP(ctx context.Context, network, host string) ([]netip.Addr, error) {
	return netResolver.LookupNetIP(ctx, network, host)
}

// WriteMetrics reports, on the leader, each domain's state and, for those
// with one, when its certificate expires, and whether getting one fails.
func (m *Manager) WriteMetrics(out *metrics.Writer) {
	if m.Leader != nil && !m.Leader() {
		return
	}
	writeMetrics(out, m.Store.Domains(), m.now())
}

func writeMetrics(out *metrics.Writer, domains []store.Domain, now time.Time) {
	out.Family("murder_domain_state", "gauge", "1 for the state each tenant's domain is in.")
	for _, d := range domains {
		for _, state := range store.DomainStates {
			out.Sample("murder_domain_state", metrics.Bool(state == d.State), "domain", d.Name, "tenant", d.Tenant, "state", state)
		}
	}
	out.Family("murder_domain_certificate_expiry_timestamp_seconds", "gauge", "When the certificate of a tenant's domain expires.")
	for _, d := range domains {
		if d.Certificate != "" {
			out.Sample("murder_domain_certificate_expiry_timestamp_seconds", metrics.Timestamp(d.NotAfter), "domain", d.Name, "tenant", d.Tenant)
		}
	}
	out.Family("murder_domain_failing", "gauge", "1 if ordering or renewing the certificate of a tenant's domain failed at the last attempt.")
	for _, d := range domains {
		failing := d.State == store.DomainFailed || d.State == store.DomainRenewing && d.Attempts > 0 || !valid(d, now) && d.Certificate != ""
		out.Sample("murder_domain_failing", metrics.Bool(failing), "domain", d.Name, "tenant", d.Tenant)
	}
}
