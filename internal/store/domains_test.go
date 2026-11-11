package store

import (
	"database/sql"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/PowerPenguini/murder/internal/spec"
)

func withTenant(t *testing.T, names ...string) *SQLite {
	t.Helper()
	st, _ := withApp(t, options())
	for _, name := range names {
		if _, err := st.Create(NewTenant{Name: name, App: "web"}, "test"); err != nil {
			t.Fatal(err)
		}
	}
	return st
}

// A domain is one tenant's, of a form the store checks, and a tenant has
// at most MaxDomains.
func TestADomainIsOneTenants(t *testing.T) {
	st := withTenant(t, "acme", "beta")
	domain, added, err := st.AddDomain("acme", "app.customer.com", "alice")
	if err != nil || !added || domain.State != DomainVerifying || domain.Tenant != "acme" || domain.NextAt.IsZero() || domain.Verified() {
		t.Fatalf("add: %+v %v %v", domain, added, err)
	}
	// Again, the same: nothing changes.
	version := st.state.Load().version
	if again, added, err := st.AddDomain("acme", "app.customer.com", "alice"); err != nil || added || again.Name != domain.Name {
		t.Fatalf("add again: %+v %v %v", again, added, err)
	}
	if st.state.Load().version != version {
		t.Error("adding a domain the tenant has changed the state")
	}
	if _, _, err := st.AddDomain("beta", "app.customer.com", "bob"); !errors.Is(err, ErrConflict) || !strings.Contains(err.Error(), "belongs to tenant acme") {
		t.Fatalf("another tenant's domain: %v", err)
	}
	for _, bad := range []string{"App.customer.com", "app.customer.com.", "customer", "-a.example.com", "a..example.com", "https://a.example.com", strings.Repeat("a", 64) + ".example.com"} {
		if _, _, err := st.AddDomain("acme", bad, "alice"); !errors.Is(err, ErrInvalid) {
			t.Errorf("%q: %v", bad, err)
		}
	}
	if _, _, err := st.AddDomain("nobody", "x.example.com", "alice"); !errors.Is(err, ErrNotFound) {
		t.Fatalf("no such tenant: %v", err)
	}
	for i := 1; i < MaxDomains; i++ {
		if _, _, err := st.AddDomain("acme", fmt.Sprintf("d%d.customer.com", i), "alice"); err != nil {
			t.Fatal(err)
		}
	}
	if _, _, err := st.AddDomain("acme", "one-more.customer.com", "alice"); !errors.Is(err, ErrConflict) || !strings.Contains(err.Error(), "the most a tenant may have") {
		t.Fatalf("past the cap: %v", err)
	}
	if got := len(st.TenantDomains("acme")); got != MaxDomains {
		t.Fatalf("acme has %d domains", got)
	}
	if err := st.RemoveDomain("beta", "app.customer.com", "bob"); !errors.Is(err, ErrNotFound) {
		t.Fatalf("remove another tenant's domain: %v", err)
	}
	if err := st.RemoveDomain("acme", "app.customer.com", "alice"); err != nil {
		t.Fatal(err)
	}
	if _, err := st.Domain("app.customer.com"); !errors.Is(err, ErrNotFound) {
		t.Fatalf("a removed domain: %v", err)
	}
	// Free again, for another tenant.
	if _, _, err := st.AddDomain("beta", "app.customer.com", "bob"); err != nil {
		t.Fatal(err)
	}
	events, _ := st.Events(EventQuery{Limit: 3})
	if events[0].Action != "domain.added" || events[0].Tenant != "beta" || events[0].Detail != "app.customer.com" || events[1].Action != "domain.removed" {
		t.Fatalf("events %+v", events)
	}
}

// An archived tenant gets no new domain but keeps its own; a purge
// releases them, with their challenges.
func TestAPurgeReleasesATenantsDomains(t *testing.T) {
	st := withTenant(t, "acme")
	domain, _, err := st.AddDomain("acme", "app.customer.com", "alice")
	if err != nil {
		t.Fatal(err)
	}
	if err := st.PutChallenge(Challenge{Token: "tok_en-1", Domain: domain.Name, KeyAuth: "tok_en-1.thumb", ExpiresAt: time.Now().Add(time.Minute)}); err != nil {
		t.Fatal(err)
	}
	if err := st.Archive("acme", "alice"); err != nil {
		t.Fatal(err)
	}
	if _, _, err := st.AddDomain("acme", "other.customer.com", "alice"); !errors.Is(err, ErrArchived) {
		t.Fatalf("add to an archived tenant: %v", err)
	}
	if len(st.TenantDomains("acme")) != 1 {
		t.Fatal("archiving dropped the domain")
	}
	if _, err := st.Restore("acme", "alice"); err != nil {
		t.Fatal(err)
	}
	if len(st.TenantDomains("acme")) != 1 {
		t.Fatal("restoring dropped the domain")
	}
	if err := st.Archive("acme", "alice"); err != nil {
		t.Fatal(err)
	}
	if err := st.Forget("acme", "alice"); err != nil {
		t.Fatal(err)
	}
	if len(st.Domains()) != 0 || len(st.Challenges()) != 0 {
		t.Fatalf("a purge left %v and %v", st.Domains(), st.Challenges())
	}
}

// The leader's writes: a domain removed and added again meanwhile is not
// written over.
func TestUpdateDomainKeepsToTheDomainRead(t *testing.T) {
	st := withTenant(t, "acme")
	read, _, err := st.AddDomain("acme", "app.customer.com", "alice")
	if err != nil {
		t.Fatal(err)
	}
	now := time.Now().UTC().Truncate(time.Second)
	next := read
	next.State, next.Detail, next.Attempts, next.CheckedAt, next.VerifiedAt = DomainIssuing, "ordering", 2, now, now
	next.Certificate, next.Key, next.IssuedAt, next.NotAfter, next.NextAt = "CERT", "sealed", now, now.Add(90*24*time.Hour), now.Add(time.Hour)
	if err := st.UpdateDomain(next, DomainEvent{Action: "domain.verified", Detail: "app.customer.com: a CNAME leads it to acme.example.com"}, "murder"); err != nil {
		t.Fatal(err)
	}
	stored, _ := st.Domain("app.customer.com")
	if !reflect.DeepEqual(stored, next) {
		t.Fatalf("stored %+v\nwant   %+v", stored, next)
	}
	events, _ := st.Events(EventQuery{Limit: 1})
	if events[0].Action != "domain.verified" || events[0].Tenant != "acme" || events[0].App != "web" {
		t.Fatalf("event %+v", events[0])
	}
	if err := st.UpdateDomain(Domain{Name: next.Name, Tenant: "acme", CreatedAt: next.CreatedAt, State: "lost"}, DomainEvent{}, "murder"); !errors.Is(err, ErrInvalid) {
		t.Fatalf("an unknown state: %v", err)
	}
	if err := st.RemoveDomain("acme", read.Name, "alice"); err != nil {
		t.Fatal(err)
	}
	st.now = func() time.Time { return time.Now().Add(time.Second) }
	if _, _, err := st.AddDomain("acme", read.Name, "alice"); err != nil {
		t.Fatal(err)
	}
	if err := st.UpdateDomain(next, DomainEvent{}, "murder"); !errors.Is(err, ErrConflict) {
		t.Fatalf("an update of the domain as it was before: %v", err)
	}
	if again, _ := st.Domain(read.Name); again.State != DomainVerifying || again.Certificate != "" {
		t.Fatalf("the domain added again was written over: %+v", again)
	}
	if err := st.CheckDomainNow("acme", read.Name, "alice"); err != nil {
		t.Fatal(err)
	}
	if err := st.CheckDomainNow("acme", "else.customer.com", "alice"); !errors.Is(err, ErrNotFound) {
		t.Fatalf("check another: %v", err)
	}
}

// Challenges: one per token, the expired ones forgotten with the next.
func TestChallengesComeAndGo(t *testing.T) {
	st := withTenant(t, "acme")
	if _, _, err := st.AddDomain("acme", "app.customer.com", "alice"); err != nil {
		t.Fatal(err)
	}
	old := Challenge{Token: "old", Domain: "app.customer.com", KeyAuth: "old.x", ExpiresAt: time.Now().Add(-time.Minute)}
	if err := st.PutChallenge(old); err != nil {
		t.Fatal(err)
	}
	if err := st.PutChallenge(Challenge{Token: "new", Domain: "app.customer.com", KeyAuth: "new.x", ExpiresAt: time.Now().Add(time.Minute)}); err != nil {
		t.Fatal(err)
	}
	if challenges := st.Challenges(); len(challenges) != 1 || challenges[0].Token != "new" {
		t.Fatalf("challenges %+v", challenges)
	}
	if err := st.PutChallenge(Challenge{Token: "a/b", Domain: "app.customer.com", KeyAuth: "x", ExpiresAt: time.Now()}); !errors.Is(err, ErrInvalid) {
		t.Fatalf("a token with a slash: %v", err)
	}
	if err := st.PutChallenge(Challenge{Token: "c", Domain: "else.customer.com", KeyAuth: "x", ExpiresAt: time.Now()}); !errors.Is(err, ErrNotFound) {
		t.Fatalf("a challenge of no domain: %v", err)
	}
	if err := st.DeleteChallenge("new"); err != nil || len(st.Challenges()) != 0 {
		t.Fatalf("delete: %v %v", err, st.Challenges())
	}
	if err := st.DeleteChallenge("new"); err != nil {
		t.Fatalf("delete again: %v", err)
	}
}

// The first account kept for a CA is the cluster's.
func TestTheFirstACMEAccountStays(t *testing.T) {
	st := withTenant(t)
	ca := "https://acme.example/directory"
	if _, ok := st.ACMEAccount(ca); ok {
		t.Fatal("an account from nowhere")
	}
	first, err := st.PutACMEAccount(ACMEAccount{CA: ca, Email: "ops@example.com", Location: "https://acme.example/acct/1", Key: "sealed-1"}, "murder")
	if err != nil || first.Location != "https://acme.example/acct/1" {
		t.Fatalf("first: %+v %v", first, err)
	}
	kept, err := st.PutACMEAccount(ACMEAccount{CA: ca, Location: "https://acme.example/acct/2", Key: "sealed-2"}, "murder")
	if err != nil || !reflect.DeepEqual(kept, first) {
		t.Fatalf("second: %+v %v", kept, err)
	}
	if _, err := st.PutACMEAccount(ACMEAccount{CA: ca}, "murder"); !errors.Is(err, ErrInvalid) {
		t.Fatalf("an account without a key: %v", err)
	}
}

// Domains, their certificates, challenges and the ACME account reach every
// node through the log, and the keys go through a master key rotation.
func TestDomainsReachEveryNodeAndAreSealedAgain(t *testing.T) {
	origin, _ := openNew(t, options())
	replicaPath := filepath.Join(t.TempDir(), "murder.db")
	if err := Init(replicaPath, "node-2"); err != nil {
		t.Fatal(err)
	}
	replica, err := Open(replicaPath, Options{NetworkPool: options().NetworkPool, Sealer: func(string, string, spec.Secret) (string, error) {
		return "", errors.New("a replica never seals")
	}})
	if err != nil {
		t.Fatal(err)
	}
	defer replica.Close()
	replicate(t, origin, replica)
	if _, err := origin.PutApp(App{Name: "web", Spec: appSpec()}, "test"); err != nil {
		t.Fatal(err)
	}
	if _, err := origin.Create(NewTenant{Name: "acme", App: "web"}, "test"); err != nil {
		t.Fatal(err)
	}

	domain, _, err := origin.AddDomain("acme", "app.customer.com", "alice")
	if err != nil {
		t.Fatal(err)
	}
	domain.State, domain.Certificate, domain.Key, domain.VerifiedAt, domain.NotAfter = DomainActive, "CERT", "sealed:key", time.Now(), time.Now().Add(time.Hour)
	if err := origin.UpdateDomain(domain, DomainEvent{Action: "domain.issued", Detail: "app.customer.com"}, "murder"); err != nil {
		t.Fatal(err)
	}
	if err := origin.PutChallenge(Challenge{Token: "t1", Domain: domain.Name, KeyAuth: "t1.x", ExpiresAt: time.Now().Add(time.Minute)}); err != nil {
		t.Fatal(err)
	}
	if _, err := origin.PutACMEAccount(ACMEAccount{CA: "https://ca", Location: "https://ca/acct/1", Key: "sealed:account"}, "murder"); err != nil {
		t.Fatal(err)
	}
	// Sealed again, as a rotation does: tenants' secrets, the domain's key
	// and the account's, each under its own context.
	contexts := map[string]bool{}
	count, err := origin.ResealSecrets(func(context, sealed string) (string, bool, error) {
		contexts[context] = true
		return "again:" + sealed, true, nil
	}, "root")
	if err != nil || count != 3 {
		t.Fatalf("resealed %d, %v", count, err)
	}
	for _, context := range []string{SecretContext("acme", "SESSION"), DomainKeyContext("app.customer.com"), AccountKeyContext("https://ca")} {
		if !contexts[context] {
			t.Errorf("not sealed again: %s (of %v)", context, contexts)
		}
	}
	for _, st := range []*SQLite{origin, replica} {
		stored, err := st.Domain("app.customer.com")
		account, _ := st.ACMEAccount("https://ca")
		if err != nil || stored.Key != "again:sealed:key" || stored.Certificate != "CERT" || account.Key != "again:sealed:account" || len(st.Challenges()) != 1 {
			t.Fatalf("%s: %+v %+v %v %v", st.Node(), stored, account, st.Challenges(), err)
		}
	}
	if !reflect.DeepEqual(origin.Domains(), replica.Domains()) || !reflect.DeepEqual(origin.Challenges(), replica.Challenges()) {
		t.Fatalf("the replica differs:\n%+v\n%+v", origin.Domains(), replica.Domains())
	}
	digests := func(st *SQLite) Digest {
		digest, err := st.Digest(t.Context())
		if err != nil {
			t.Fatal(err)
		}
		return digest
	}
	if a, b := digests(origin), digests(replica); !a.Comparable(b) || a.Hash != b.Hash {
		t.Fatalf("digests differ: %+v %+v", a, b)
	}
}

// A schema 17 database upgrades in place: it has no domains, then takes
// them.
func TestOpenUpgradesSchemaVersion17(t *testing.T) {
	path := filepath.Join(t.TempDir(), "murder.db")
	db, err := sql.Open("sqlite", initDSN(path))
	if err != nil {
		t.Fatal(err)
	}
	for _, statement := range []string{
		schema,
		`DROP TABLE acme_challenges`, `DROP TABLE acme_accounts`, `DROP TABLE domains`, `ALTER TABLE tenants DROP COLUMN canonical`,
		`INSERT INTO meta (key, value) VALUES ('node', 'n1'), ('version', '3'), ('applied', '0')`,
		`INSERT INTO apps (name, spec, created_at, updated_at, revision) VALUES ('web', '` + marshal(normalizeApp(appSpec())) + `', '2026-10-01T00:00:00Z', '2026-10-01T00:00:00Z', 1)`,
		`INSERT INTO tenants (name, app, overrides, suspended, subnet, data_image, previous_image, generation, created_at, updated_at, node)
			VALUES ('acme', 'web', '{}', 0, '10.240.0.0/28', '', '', 1, '2026-10-01T00:00:00Z', '2026-10-01T00:00:00Z', 'n1')`,
		`PRAGMA user_version = 17`,
	} {
		if _, err := db.Exec(statement); err != nil {
			t.Fatal(err)
		}
	}
	if err := db.Close(); err != nil {
		t.Fatal(err)
	}
	st, err := Open(path, options())
	if err != nil {
		t.Fatal(err)
	}
	defer st.Close()
	var version int
	if err := st.db.QueryRow(`PRAGMA user_version`).Scan(&version); err != nil || version != schemaVersion {
		t.Fatalf("user_version = %d, %v", version, err)
	}
	if len(st.Domains()) != 0 {
		t.Fatal("domains from nowhere")
	}
	if _, _, err := st.AddDomain("acme", "app.customer.com", "alice"); err != nil {
		t.Fatal(err)
	}
	if err := st.SetCanonical("acme", "app.customer.com", "acme.example.com", "alice"); err != nil {
		t.Fatal(err)
	}
	if record, _ := st.Get("acme"); record.Canonical != "app.customer.com" {
		t.Fatalf("canonical %q", record.Canonical)
	}
	// A batch of the release before still applies.
	recorded := &recorder{}
	if err := event(recorded, time.Now(), "murder", "tenant.restarted", "acme", "web", "old release"); err != nil {
		t.Fatal(err)
	}
	if err := st.Apply(Batch{Schema: 17, Version: st.state.Load().version, Statements: recorded.statements}); err != nil {
		t.Fatalf("a schema 17 batch: %v", err)
	}
}

// A snapshot of the release before, without domains, installs on a node of
// this one: the node then has none.
func TestASnapshotFromBeforeDomainsInstalls(t *testing.T) {
	older := withTenant(t, "acme")
	path, err := older.SnapshotFile()
	if err != nil {
		t.Fatal(err)
	}
	defer removeDatabase(path)
	db, err := sql.Open("sqlite", uri(path, ""))
	if err != nil {
		t.Fatal(err)
	}
	for _, statement := range []string{`DROP TABLE acme_challenges`, `DROP TABLE acme_accounts`, `DROP TABLE domains`, `ALTER TABLE tenants DROP COLUMN canonical`, `UPDATE meta SET value = '10' WHERE key = 'applied'`, `PRAGMA user_version = 17`} {
		if _, err := db.Exec(statement); err != nil {
			t.Fatal(err)
		}
	}
	db.Close()
	node := withTenant(t, "beta")
	if _, _, err := node.AddDomain("beta", "app.customer.com", "alice"); err != nil {
		t.Fatal(err)
	}
	file, err := os.Open(path)
	if err != nil {
		t.Fatal(err)
	}
	defer file.Close()
	if ok, err := node.InstallSnapshot(file); err != nil || !ok {
		t.Fatalf("install: %v %v", ok, err)
	}
	if len(node.Domains()) != 0 || len(node.List()) != 1 || node.List()[0].Spec.Name != "acme" {
		t.Fatalf("after the install: %v %v", node.Domains(), node.List())
	}
}

// A tenant's canonical host is one of its domains or its own name; it
// goes with the domain it names, and a tenant that is not live gets none.
func TestACanonicalHostIsOneOfTheTenantsHosts(t *testing.T) {
	st := withTenant(t, "acme", "beta")
	for _, name := range []string{"app.customer.com", "www.customer.com"} {
		if _, _, err := st.AddDomain("acme", name, "alice"); err != nil {
			t.Fatal(err)
		}
	}
	if _, _, err := st.AddDomain("beta", "beta.customer.com", "bob"); err != nil {
		t.Fatal(err)
	}
	for _, bad := range []string{"beta.customer.com", "else.customer.com", "beta.example.com"} {
		if err := st.SetCanonical("acme", bad, "acme.example.com", "alice"); !errors.Is(err, ErrInvalid) {
			t.Errorf("%s: %v", bad, err)
		}
	}
	if err := st.SetCanonical("nobody", "", "nobody.example.com", "alice"); !errors.Is(err, ErrNotFound) {
		t.Fatalf("no such tenant: %v", err)
	}
	canonical := func() string {
		t.Helper()
		record, err := st.Get("acme")
		if err != nil {
			t.Fatal(err)
		}
		return record.Canonical
	}
	if err := st.SetCanonical("acme", "acme.example.com", "acme.example.com", "alice"); err != nil || canonical() != "acme.example.com" {
		t.Fatalf("its own name: %q %v", canonical(), err)
	}
	if err := st.SetCanonical("acme", "app.customer.com", "acme.example.com", "alice"); err != nil || canonical() != "app.customer.com" {
		t.Fatalf("a domain: %q %v", canonical(), err)
	}
	// The same again changes nothing, and says nothing.
	version := st.state.Load().version
	if err := st.SetCanonical("acme", "app.customer.com", "acme.example.com", "alice"); err != nil || st.state.Load().version != version {
		t.Fatalf("the same again: %v", err)
	}
	events, _ := st.Events(EventQuery{Tenant: "acme", Limit: 1})
	if events[0].Action != "tenant.canonical" || !strings.Contains(events[0].Detail, "app.customer.com") {
		t.Fatalf("events %+v", events)
	}
	// Another domain removed leaves it; its own removed takes it.
	if err := st.RemoveDomain("acme", "www.customer.com", "alice"); err != nil || canonical() != "app.customer.com" {
		t.Fatalf("another domain removed: %q %v", canonical(), err)
	}
	if err := st.RemoveDomain("acme", "app.customer.com", "alice"); err != nil || canonical() != "" {
		t.Fatalf("the canonical domain removed: %q %v", canonical(), err)
	}
	if err := st.SetCanonical("acme", "acme.example.com", "acme.example.com", "alice"); err != nil {
		t.Fatal(err)
	}
	if err := st.SetCanonical("acme", "", "acme.example.com", "alice"); err != nil || canonical() != "" {
		t.Fatalf("none: %q %v", canonical(), err)
	}
	if err := st.Archive("beta", "bob"); err != nil {
		t.Fatal(err)
	}
	if err := st.SetCanonical("beta", "beta.customer.com", "beta.example.com", "bob"); !errors.Is(err, ErrArchived) {
		t.Fatalf("an archived tenant: %v", err)
	}
}
