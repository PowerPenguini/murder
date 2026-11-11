package store

import (
	"database/sql"
	"errors"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

// One operation holds the cluster at a time; another is refused, told
// whose it is, until the first lets go.
func TestOneOperationHoldsTheClusterAtATime(t *testing.T) {
	st, _ := openNew(t, options())
	now := time.Date(2026, 11, 11, 12, 0, 0, 0, time.UTC)
	st.now = func() time.Time { return now }
	if lease := st.Lease(); lease.Held(now) || lease.ID != "" {
		t.Fatalf("a new state holds a lease: %+v", lease)
	}
	drain, err := st.TakeLease("drain", "node n2 to n3", "alice")
	if err != nil {
		t.Fatal(err)
	}
	if drain.ID == "" || drain.Node != "node-1" || drain.Actor != "alice" || !drain.Expires.Equal(now.Add(LeaseTTL)) || st.Lease() != drain {
		t.Fatalf("lease %+v, state %+v", drain, st.Lease())
	}
	_, err = st.TakeLease("balance", "", "bob")
	if !errors.Is(err, ErrLeased) || !errors.Is(err, ErrConflict) || !strings.Contains(err.Error(), "drain node n2 to n3, begun by alice on node node-1") || !strings.Contains(err.Error(), drain.ID) {
		t.Fatalf("a second lease: %v", err)
	}
	// Releasing another lease than the one held changes nothing.
	if err := st.ReleaseLease("not-it", "bob"); err != nil || st.Lease() != drain {
		t.Fatalf("release of another: %v, %+v", err, st.Lease())
	}
	if err := st.ReleaseLease(drain.ID, "alice"); err != nil {
		t.Fatal(err)
	}
	if lease := st.Lease(); lease.ID != "" {
		t.Fatalf("released, the state still holds %+v", lease)
	}
	if _, err := st.TakeLease("balance", "", "bob"); err != nil {
		t.Fatal(err)
	}
	events, _ := st.Events(EventQuery{})
	if len(events) != 3 || events[0].Action != "operation.started" || events[1].Action != "operation.ended" || events[2].Action != "operation.started" || !strings.Contains(events[2].Detail, "drain node n2 to n3") {
		t.Fatalf("events = %+v", events)
	}
}

// A lease expires unless renewed: a holder that died lets go by itself,
// and the next operation takes its place, the audit log saying whose it
// was. Renewed, it holds on.
func TestALeaseExpiresUnlessRenewed(t *testing.T) {
	st, _ := openNew(t, options())
	now := time.Date(2026, 11, 11, 12, 0, 0, 0, time.UTC)
	st.now = func() time.Time { return now }
	reboot, err := st.TakeLease("reboot", "node n2", "alice")
	if err != nil {
		t.Fatal(err)
	}
	now = now.Add(LeaseTTL - time.Second)
	renewed, err := st.RenewLease(reboot.ID)
	if err != nil || !renewed.Expires.Equal(now.Add(LeaseTTL)) || !renewed.Started.Equal(reboot.Started) {
		t.Fatalf("renewal: %+v, %v", renewed, err)
	}
	now = now.Add(LeaseTTL - time.Second)
	if _, err := st.TakeLease("drain", "", "bob"); !errors.Is(err, ErrLeased) {
		t.Fatalf("a renewed lease was taken: %v", err)
	}
	now = now.Add(2 * time.Second)
	if st.Lease().Held(now) {
		t.Fatal("an expired lease holds")
	}
	drain, err := st.TakeLease("drain", "node n1 to n3", "bob")
	if err != nil {
		t.Fatal(err)
	}
	// The holder that came back has lost it.
	if _, err := st.RenewLease(reboot.ID); !errors.Is(err, ErrLeaseLost) {
		t.Fatalf("a renewal of a lease taken over: %v", err)
	}
	if st.Lease() != drain {
		t.Fatalf("lease %+v", st.Lease())
	}
	events, _ := st.Events(EventQuery{Limit: 2})
	if len(events) != 2 || events[1].Action != "operation.expired" || !strings.Contains(events[1].Detail, "reboot node n2, begun by alice") {
		t.Fatalf("events = %+v", events)
	}
	// One that expired, but that no one took, is renewed: nothing ran
	// meanwhile.
	now = now.Add(LeaseTTL + time.Minute)
	if _, err := st.RenewLease(drain.ID); err != nil {
		t.Fatalf("renewal of a lease no one took: %v", err)
	}
}

// A lease and a freeze exclude each other: no upgrade or rotation runs
// beside a drain, and no drain beside them. A lease that expired holds
// nothing: a freeze goes on, and the holder that comes back has lost it.
func TestALeaseAndAFreezeExcludeEachOther(t *testing.T) {
	st, _ := openNew(t, options())
	now := time.Date(2026, 11, 11, 12, 0, 0, 0, time.UTC)
	st.now = func() time.Time { return now }
	lease, err := st.TakeLease("drain", "node n2 to n3", "alice")
	if err != nil {
		t.Fatal(err)
	}
	for _, reason := range []string{UpgradeFreeze + "abc from node n1, operation 01", RotationFreeze + "n1"} {
		if err := st.Freeze(reason, "bob"); !errors.Is(err, ErrLeased) || !strings.Contains(err.Error(), "drain node n2 to n3") {
			t.Fatalf("a freeze beside a lease: %v", err)
		}
	}
	if st.Frozen() != "" {
		t.Fatalf("frozen %q", st.Frozen())
	}
	now = now.Add(LeaseTTL)
	if err := st.Freeze(UpgradeFreeze+"abc", "bob"); err != nil {
		t.Fatalf("a freeze once the lease expired: %v", err)
	}
	if _, err := st.RenewLease(lease.ID); !errors.Is(err, ErrLeaseLost) {
		t.Fatalf("renewal under a freeze: %v", err)
	}
	if _, err := st.TakeLease("balance", "", "carol"); !errors.Is(err, ErrFrozen) || !strings.Contains(err.Error(), UpgradeFreeze+"abc") {
		t.Fatalf("a lease while frozen: %v", err)
	}
	// Released while frozen, a lease goes: its operation ended.
	if err := st.ReleaseLease(lease.ID, "alice"); err != nil || st.Lease().ID != "" {
		t.Fatalf("release while frozen: %v, %+v", err, st.Lease())
	}
}

// A person breaks a lease by its ID, and only that one: one taken since
// stays. Its holder then loses it. The audit log says who broke whose.
func TestALeaseIsBrokenOnlyAsSeen(t *testing.T) {
	st, _ := openNew(t, options())
	if _, err := st.BreakLease("nothing", "root"); !errors.Is(err, ErrNotFound) {
		t.Fatalf("break of no lease: %v", err)
	}
	lease, err := st.TakeLease("reboot", "node n2", "alice")
	if err != nil {
		t.Fatal(err)
	}
	if _, err := st.BreakLease("an-older-one", "root"); !errors.Is(err, ErrConflict) || !strings.Contains(err.Error(), "reboot node n2") {
		t.Fatalf("break of another lease: %v", err)
	}
	broken, err := st.BreakLease(lease.ID, "root")
	if err != nil || broken.ID != lease.ID || st.Lease().ID != "" {
		t.Fatalf("break: %+v, %v, held %+v", broken, err, st.Lease())
	}
	if _, err := st.RenewLease(lease.ID); !errors.Is(err, ErrLeaseLost) {
		t.Fatalf("renewal of a broken lease: %v", err)
	}
	events, _ := st.Events(EventQuery{Limit: 1})
	if len(events) != 1 || events[0].Action != "operation.unlocked" || events[0].Actor != "root" || !strings.Contains(events[0].Detail, "reboot node n2, begun by alice") {
		t.Fatalf("events = %+v", events)
	}
}

// The lease is part of the replicated state: every node holds it alike,
// and it is written as of its own schema version, which only nodes that
// keep it apply.
func TestTheLeaseIsReplicated(t *testing.T) {
	origin, _ := openNew(t, options())
	replicaPath := filepath.Join(t.TempDir(), "murder.db")
	if err := Init(replicaPath, "node-2"); err != nil {
		t.Fatal(err)
	}
	replica, err := Open(replicaPath, options())
	if err != nil {
		t.Fatal(err)
	}
	defer replica.Close()
	replicate(t, origin, replica)
	lease, err := origin.TakeLease("balance", "", "alice")
	if err != nil {
		t.Fatal(err)
	}
	if got := replica.Lease(); got != lease {
		t.Fatalf("replica holds %+v, origin %+v", got, lease)
	}
	if ours, theirs := digestOf(t, origin), digestOf(t, replica); ours.Hash != theirs.Hash {
		t.Fatal("the lease hashes differently on two nodes")
	}
	if _, err := replica.TakeLease("drain", "", "bob"); !errors.Is(err, ErrLeased) {
		t.Fatalf("the replica took a lease beside the origin's: %v", err)
	}
	entries := logged(t, origin)
	if err := origin.ReleaseLease(lease.ID, "alice"); err != nil {
		t.Fatal(err)
	}
	if last := (*entries)[len(*entries)-1]; last.Schema != LeaseSchema {
		t.Fatalf("a lease's batch is of schema %d", last.Schema)
	}
}

// A schema 19 database upgrades in place, with no lease held.
func TestOpenUpgradesSchemaVersion19(t *testing.T) {
	path := filepath.Join(t.TempDir(), "murder.db")
	db, err := sql.Open("sqlite", initDSN(path))
	if err != nil {
		t.Fatal(err)
	}
	for _, statement := range []string{
		schema,
		`DROP TABLE operation_lease`,
		`INSERT INTO meta (key, value) VALUES ('node', 'n1'), ('version', '3'), ('applied', '0')`,
		`PRAGMA user_version = 19`,
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
	if lease := st.Lease(); lease.ID != "" {
		t.Fatalf("lease after the upgrade: %+v", lease)
	}
	if _, err := st.TakeLease("drain", "", "alice"); err != nil {
		t.Fatal(err)
	}
	var version int
	if err := st.db.QueryRow(`PRAGMA user_version`).Scan(&version); err != nil || version != schemaVersion {
		t.Fatalf("user_version = %d, %v", version, err)
	}
}
